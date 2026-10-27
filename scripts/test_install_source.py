"""Source/install boundaries with a tiny committed repository and no compiler or services."""
import hashlib
import io
import json
import os
from pathlib import Path
import struct
import subprocess
import tarfile
import tempfile
import unittest
from unittest.mock import patch

import install_source as installer


class BuildProfileTests(unittest.TestCase):
    def test_profiles_build_services_and_offline_tools_without_public_cli(self):
        binaries = {
            'sandbox-api': ('sandbox-api',),
            'sandbox-controller': ('sandbox-controller', 'sandbox-host-manager', 'sandbox-local-pki'),
            'sandbox-cleanup': ('sandbox-cleanup', 'sandbox-storage-audit'),
            'sandbox-supervisor': ('sandbox-supervisor', 'sandbox-host'),
            'sandbox-guest': ('sandbox-guest',),
            'sandbox-cli': ('sandbox-api-test-client',),
        }
        expected = {
            'control-plane': {'sandbox-api', 'sandbox-controller', 'sandbox-host-manager',
                              'sandbox-local-pki', 'sandbox-cleanup', 'sandbox-storage-audit'},
            'linux-runtime': {'sandbox-supervisor', 'sandbox-host', 'sandbox-guest'},
        }
        metadata = {'packages': [
            {'id': name, 'name': name,
             'targets': [{'name': binary, 'kind': ['bin']} for binary in names]}
            for name, names in binaries.items()
        ]}
        for profile, names in expected.items():
            with self.subTest(profile=profile), tempfile.TemporaryDirectory() as temporary:
                work = Path(temporary)
                target = work / 'target'

                def build(command, **kwargs):
                    packages = [command[index + 1] for index, arg in enumerate(command) if arg == '-p']
                    self.assertNotIn('sandbox-cli', packages)
                    self.assertIn('--locked', command)
                    for package in packages:
                        for binary in binaries[package]:
                            event = {'reason': 'compiler-artifact', 'package_id': package,
                                     'target': {'name': binary}, 'executable': str(target / binary)}
                            kwargs['stdout'].write((json.dumps(event) + '\n').encode())

                with patch.object(installer.subprocess, 'check_output', return_value=json.dumps(metadata)), \
                     patch.object(installer.subprocess, 'run', side_effect=build):
                    result = installer.build_binaries(work, profile, target, '1.92.0', work)
                self.assertEqual(set(result), names)


@unittest.skipUnless(os.name == 'posix', 'native Unix installer')
class InstallTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.parent = Path(self.temporary.name).resolve()
        self.parent.chmod(0o700)
        self.root = self.parent / 'repository'
        self.root.mkdir()
        self.git_environment = {**os.environ, 'GIT_CONFIG_NOSYSTEM': '1',
                                'GIT_CONFIG_GLOBAL': os.devnull, 'GIT_TEMPLATE_DIR': ''}
        values = {'Cargo.toml': '[workspace]\nmembers=[]\n', 'Cargo.lock': '# fixture\n',
                  'rust-toolchain.toml': '[toolchain]\nchannel="1.92.0"\n',
                  'crates/fixture/src/main.rs': 'fn main() {}\n', 'proto/fixture.proto': 'syntax="proto3";\n',
                  'migrations/0001_fixture.sql': 'SELECT 1;\n', 'dev/runtime-artifacts.json': '{}\n',
                  '.env': 'fixture-secret-not-an-actual-credential\n'}
        for name, value in values.items():
            target = self.root / name
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(value)
        subprocess.run(['git', 'init', '-q', str(self.root)], check=True, env=self.git_environment)
        self.commit()

    def commit(self):
        subprocess.run(['git', 'add', '.'], cwd=self.root, check=True, env=self.git_environment)
        subprocess.run(['git', '-c', 'user.name=Fixture', '-c', 'user.email=fixture@example.test',
                        'commit', '-qm', 'fixture'], cwd=self.root, check=True, env=self.git_environment)

    def binary(self, target, arch=None):
        target.mkdir(parents=True, exist_ok=True)
        path = target / 'hudsonbox'
        header = bytearray(64)
        if installer.platform.system() == 'Linux':
            header[:6] = b'\x7fELF\x02\x01'
            struct.pack_into('<H', header, 16, 3)
            struct.pack_into('<H', header, 18, {'x86_64': 62, 'aarch64': 183}[arch or installer.architecture()])
        else:
            header[:4] = b'\xcf\xfa\xed\xfe'
            struct.pack_into('<I', header, 12, 2)
            struct.pack_into('<I', header, 4, {'x86_64': 0x01000007, 'aarch64': 0x0100000C}[arch or installer.architecture()])
        path.write_bytes(header + b'fixture-binary')
        return path

    def build(self, source, profile, target, toolchain, work):
        self.assertFalse((source / '.git').exists())
        self.assertFalse((source / '.env').exists())
        self.assertEqual(toolchain, '1.92.0')
        self.assertEqual((source / 'Cargo.toml').read_text(), '[workspace]\nmembers=[]\n')
        return {'hudsonbox': self.binary(target)}

    def test_committed_allowlist_receipt_and_no_overwrite(self):
        # Working-tree changes must not be attributed to the captured commit.
        (self.root / 'Cargo.toml').write_text('uncommitted\n')
        prefix = self.parent / 'installed'
        receipt = installer.install(prefix, 'control-plane', self.root, self.build)
        stored = json.loads((prefix / 'install-receipt.json').read_text())
        self.assertEqual(receipt, stored)
        self.assertEqual(receipt['source_revision'], subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=self.root).decode().strip())
        data = (prefix / 'bin/hudsonbox').read_bytes()
        self.assertEqual(receipt['binaries'][0]['sha256'], hashlib.sha256(data).hexdigest())
        self.assertEqual((prefix / 'bin/hudsonbox').stat().st_mode & 0o777, 0o755)
        self.assertEqual((prefix / 'install-receipt.json').stat().st_mode & 0o777, 0o600)
        self.assertFalse(list(self.parent.glob('.hudsonbox-build-*')))
        with self.assertRaises(installer.Failure):
            installer.install(prefix, 'control-plane', self.root, self.build)
        self.assertEqual((prefix / 'bin/hudsonbox').read_bytes(), data)

    def test_public_or_symlink_parent_never_starts_builder(self):
        public = self.parent / 'public'
        public.mkdir(mode=0o755)
        link = self.parent / 'linked'
        link.symlink_to(self.parent, target_is_directory=True)
        for prefix in (public / 'installed', link / 'installed', Path('relative')):
            with patch.object(installer, 'build_binaries') as build:
                with self.assertRaises(installer.Failure):
                    installer.install(prefix, 'control-plane', self.root, build)
                build.assert_not_called()

    def test_foreign_architecture_or_outside_artifact_never_publishes(self):
        foreign = 'aarch64' if installer.architecture() == 'x86_64' else 'x86_64'
        for kind in ('foreign', 'outside'):
            prefix = self.parent / kind
            def build(source, profile, target, toolchain, work):
                return {'hudsonbox': self.binary(target if kind == 'foreign' else self.parent / 'outside-build',
                                                 foreign if kind == 'foreign' else None)}
            with self.assertRaises(installer.Failure):
                installer.install(prefix, 'control-plane', self.root, build)
            self.assertFalse(prefix.exists())

    def test_concurrent_prefix_creation_preserves_unrelated_data(self):
        prefix = self.parent / 'raced'
        def build(*arguments):
            prefix.mkdir()
            (prefix / 'unrelated').write_text('keep')
            return self.build(*arguments)
        with self.assertRaises(installer.Failure):
            installer.install(prefix, 'control-plane', self.root, build)
        self.assertEqual((prefix / 'unrelated').read_text(), 'keep')

    def test_symlinked_target_cannot_redirect_artifact_validation(self):
        prefix = self.parent / 'redirected'
        def build(source, profile, target, toolchain, work):
            outside = self.parent / 'other-target'
            binary = self.binary(outside)
            target.symlink_to(outside, target_is_directory=True)
            return {'hudsonbox': binary}
        with self.assertRaises(installer.Failure):
            installer.install(prefix, 'control-plane', self.root, build)
        self.assertFalse(prefix.exists())

    def test_archived_symlink_is_never_followed(self):
        (self.root / 'crates/fixture/link').symlink_to('/etc/passwd')
        self.commit()
        prefix = self.parent / 'bad-source'
        with self.assertRaises(installer.Failure):
            installer.install(prefix, 'control-plane', self.root, self.build)
        self.assertFalse(prefix.exists())

    def test_archive_traversal_devices_links_and_unlisted_files(self):
        for name, kind in (('../outside', tarfile.REGTYPE), ('/outside', tarfile.REGTYPE),
                           ('crates/link', tarfile.SYMTYPE), ('crates/device', tarfile.CHRTYPE),
                           ('private/config', tarfile.REGTYPE)):
            with self.subTest(name=name), tempfile.TemporaryDirectory(dir=self.parent) as directory:
                root = Path(directory)
                archive = root / 'input.tar'
                with tarfile.open(archive, mode='w') as output:
                    member = tarfile.TarInfo(name)
                    member.type = kind
                    member.size = 1 if kind == tarfile.REGTYPE else 0
                    output.addfile(member, io.BytesIO(b'x') if member.size else None)
                destination = root / 'source'
                destination.mkdir()
                with self.assertRaises(installer.Failure):
                    installer.extract_source(archive, destination)


if __name__ == '__main__':
    unittest.main()
