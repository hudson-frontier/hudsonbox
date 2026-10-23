import hashlib
import io
from pathlib import Path
import tarfile
import tempfile
import unittest
from unittest.mock import patch

import prepare_runtime_artifacts as artifacts


def archive(kind='files', architecture='aarch64'):
    data = io.BytesIO()
    with tarfile.open(fileobj=data, mode='w:gz') as output:
        for name in ('firecracker', 'jailer'):
            member = tarfile.TarInfo(f'release-v1.17.0-{architecture}/{name}-v1.17.0-{architecture}')
            if kind == 'symlink':
                member.type = tarfile.SYMTYPE
                member.linkname = '/etc/passwd'
                output.addfile(member)
            else:
                member.size = 3
                output.addfile(member, io.BytesIO(b'abc'))
        member = tarfile.TarInfo('../../outside')
        member.size = 3
        output.addfile(member, io.BytesIO(b'bad'))
    return data.getvalue()


class PreparationTests(unittest.TestCase):
    def setUp(self):
        self.pins = patch.object(artifacts, 'binary_pins', return_value={name: hashlib.sha256(b'abc').hexdigest() for name in ('firecracker', 'jailer')})
        self.pins.start()
        self.addCleanup(self.pins.stop)

    def test_initial_http_never_opens_network(self):
        with patch('urllib.request.build_opener') as opener:
            with self.assertRaises(ValueError):
                artifacts.download({'url': 'http://example.test/private?secret=hidden'})
            opener.assert_not_called()

    def test_native_arm64_names_prepare_the_pinned_aarch64_artifacts(self):
        with tempfile.TemporaryDirectory() as root:
            for index, architecture in enumerate(('arm64', 'ARM64')):
                destination = Path(root).resolve() / f'artifacts-{index}'
                artifacts.prepare(
                    destination, architecture,
                    lambda spec: archive() if 'tgz' in spec['url'] else b'kernel',
                )
                self.assertEqual((destination / 'vmlinux-6.1.186').read_bytes(), b'kernel')
                for name in ('firecracker', 'jailer'):
                    binary = destination / f'release-v1.17.0-aarch64/{name}-v1.17.0-aarch64'
                    self.assertEqual(binary.read_bytes(), b'abc')

    def test_downgrade_redirect_is_rejected(self):
        with self.assertRaises(ValueError):
            artifacts.HTTPSRedirects().redirect_request(None, None, 302, '', {}, 'http://example.test/')

    def test_native_x86_names_select_only_the_x86_release_and_kernel(self):
        with tempfile.TemporaryDirectory() as root:
            for index, architecture in enumerate(('x86_64', 'amd64', 'AMD64')):
                requested = []
                def fetch(spec):
                    requested.append(spec['url'])
                    return archive(architecture='x86_64') if 'tgz' in spec['url'] else b'kernel'
                destination = Path(root).resolve() / f'artifacts-{index}'
                artifacts.prepare(destination, architecture, fetch)
                self.assertTrue(all('x86_64' in url for url in requested))
                self.assertEqual((destination / 'vmlinux-6.1.186').read_bytes(), b'kernel')
                self.assertEqual(len(list(destination.rglob('*x86_64'))), 3)
                self.assertFalse(any(destination.rglob('*aarch64*')))

    def test_wrong_architecture_archive_fails_before_publication(self):
        with tempfile.TemporaryDirectory() as root:
            destination = Path(root).resolve() / 'artifacts'
            with self.assertRaises(ValueError):
                artifacts.prepare(destination, 'x86_64', lambda _: archive())
            self.assertFalse(destination.exists())

    def test_oversized_download(self):
        response = io.BytesIO(b'12345')
        response.geturl = lambda: 'https://example.test/artifact'
        with patch.object(artifacts, 'MAX_BYTES', 4), patch('urllib.request.build_opener') as opener:
            opener.return_value.open.return_value = response
            with self.assertRaises(ValueError):
                artifacts.download({'url': 'https://example.test/artifact', 'sha256': hashlib.sha256(b'12345').hexdigest()})

    def test_partial_publication_does_not_delete_swapped_directory(self):
        with tempfile.TemporaryDirectory() as root:
            destination = Path(root).resolve() / 'artifacts'
            def fail_move(*args):
                destination.rmdir()
                destination.mkdir()
                (destination / 'unrelated').write_text('keep')
                raise OSError('failure')
            with patch.object(artifacts.shutil, 'move', side_effect=fail_move):
                with self.assertRaises(ValueError):
                    artifacts.prepare(destination, 'aarch64', lambda spec: archive() if 'tgz' in spec['url'] else b'kernel')
            self.assertEqual((destination / 'unrelated').read_text(), 'keep')

    def test_missing_and_duplicate_members(self):
        for names in (['firecracker'], ['firecracker', 'firecracker', 'jailer']):
            data = io.BytesIO()
            with tarfile.open(fileobj=data, mode='w:gz') as output:
                for name in names:
                    member = tarfile.TarInfo(f'release-v1.17.0-aarch64/{name}-v1.17.0-aarch64')
                    member.size = 3
                    output.addfile(member, io.BytesIO(b'abc'))
            with self.assertRaises(ValueError):
                artifacts.select_binaries(data.getvalue())

    def test_corrupt_extracted_binary(self):
        with patch.object(artifacts, 'binary_pins', return_value={'firecracker': '0' * 64, 'jailer': '0' * 64}):
            with self.assertRaises(ValueError):
                artifacts.select_binaries(archive())

    def test_only_exact_regular_members_are_selected(self):
        self.assertEqual(len(artifacts.select_binaries(archive())), 2)
        with self.assertRaises(ValueError):
            artifacts.select_binaries(archive('symlink'))

    def test_new_directory_and_no_overwrite(self):
        with tempfile.TemporaryDirectory() as root:
            destination = Path(root).resolve() / 'artifacts'
            artifacts.prepare(destination, 'aarch64', lambda spec: archive() if 'tgz' in spec['url'] else b'kernel')
            self.assertEqual((destination / 'vmlinux-6.1.186').read_bytes(), b'kernel')
            self.assertFalse((destination.parent / 'outside').exists())
            with self.assertRaises(ValueError):
                artifacts.prepare(destination, 'aarch64')

    def test_unsupported_architecture_fails_before_download(self):
        with self.assertRaises(ValueError):
            artifacts.prepare('/tmp/unused', 'riscv64', lambda _: self.fail('download'))

    def test_corrupt_download_fails(self):
        response = io.BytesIO(b'corrupt')
        response.geturl = lambda: 'https://example.test/artifact'
        with patch('urllib.request.build_opener') as opener:
            opener.return_value.open.return_value = response
            with self.assertRaises(ValueError):
                artifacts.download({'url': 'https://example.test/artifact', 'sha256': '0' * 64})

    def test_missing_binary_fails(self):
        with self.assertRaises(tarfile.ReadError):
            artifacts.select_binaries(b'not an archive')


if __name__ == '__main__':
    unittest.main()
