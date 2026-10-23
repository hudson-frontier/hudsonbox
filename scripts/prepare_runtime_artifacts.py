"""Prepare pinned development artifacts without sudo, archive extraction or overwrite."""
import argparse
import hashlib
import io
import json
from pathlib import Path
import platform
import shutil
import tarfile
import tempfile
import urllib.request
from urllib.parse import urlsplit

PINS = Path(__file__).resolve().parents[1] / 'dev/runtime-artifacts.json'
MAX_BYTES = 128 * 1024 * 1024


class HTTPSRedirects(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        if not newurl.startswith("https://"):
            raise ValueError("download redirected outside HTTPS")
        return super().redirect_request(req, fp, code, msg, headers, newurl)


def download(spec):
    if urlsplit(spec['url']).scheme != 'https':
        raise ValueError('artifact URL must use HTTPS')
    request = urllib.request.Request(spec['url'])
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), HTTPSRedirects())
    with opener.open(request, timeout=30) as response:
        if not response.geturl().startswith('https://'):
            raise ValueError('download redirected outside HTTPS')
        data = response.read(MAX_BYTES + 1)
    if len(data) > MAX_BYTES or hashlib.sha256(data).hexdigest() != spec['sha256']:
        raise ValueError('artifact size or SHA-256 mismatch')
    return data


def architecture_name(architecture):
    aliases = {'arm64': 'aarch64', 'ARM64': 'aarch64',
               'amd64': 'x86_64', 'AMD64': 'x86_64'}
    architecture = aliases.get(architecture, architecture)
    if architecture not in ('aarch64', 'x86_64'):
        raise ValueError('unsupported development architecture')
    return architecture


def artifact_pins(architecture):
    architecture = architecture_name(architecture)
    pins = json.loads(PINS.read_text())
    if pins['version'] != 1:
        raise ValueError('unsupported runtime artifact pins')
    return pins, pins['architectures'][architecture]


def binary_pins(architecture='aarch64'):
    return artifact_pins(architecture)[1]['binary_sha256']


def select_binaries(data, architecture='aarch64'):
    architecture = architecture_name(architecture)
    version = artifact_pins(architecture)[0]['firecracker_version']
    names = {f'release-v{version}-{architecture}/{kind}-v{version}-{architecture}': kind
             for kind in ('firecracker', 'jailer')}
    pins = binary_pins(architecture)
    result = {}
    with tarfile.open(fileobj=io.BytesIO(data), mode='r:gz') as archive:
        for member in archive:
            if member.name not in names:
                continue
            if member.name in result or not member.isfile() or not 0 < member.size <= MAX_BYTES:
                raise ValueError('invalid or duplicate binary member')
            content = archive.extractfile(member).read()
            if hashlib.sha256(content).hexdigest() != pins[names[member.name]]:
                raise ValueError('extracted binary SHA-256 mismatch')
            result[member.name] = content
    if set(result) != set(names):
        raise ValueError('archive lacks expected binaries')
    return result


def prepare(destination, architecture, fetch=download):
    architecture = architecture_name(architecture)
    destination = Path(destination)
    if not destination.is_absolute() or destination.exists() or destination.is_symlink():
        raise ValueError('destination must be a new absolute path')
    parent = destination.parent.resolve(strict=True)
    if parent != destination.parent:
        raise ValueError('destination parent must be canonical')
    pins, evidence = artifact_pins(architecture)
    binaries = select_binaries(fetch(evidence['firecracker_archive']), architecture)
    kernel = fetch(evidence['guest_kernel'])
    with tempfile.TemporaryDirectory(prefix='.hudson-artifacts-', dir=parent) as temporary:
        stage = Path(temporary) / 'artifacts'
        stage.mkdir(mode=0o700)
        for name, data in {**binaries, f"vmlinux-{pins['kernel_version']}": kernel}.items():
            path = stage / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(data)
            path.chmod(0o755 if name in binaries else 0o644)
        # Reserve destination exclusively; never replace any pre-existing file/directory.
        destination.mkdir(mode=0o700)
        try:
            for path in stage.iterdir():
                shutil.move(str(path), destination)
        except OSError:
            raise ValueError("partial publication retained; inspect destination manually before retry") from None


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--destination', required=True)
    parser.add_argument('--architecture', default=platform.machine())
    args = parser.parse_args()
    try:
        prepare(args.destination, args.architecture)
    except (OSError, ValueError, tarfile.TarError) as error:
        parser.exit(1, 'Artifact preparation failed; inspect trusted configuration and any partial destination.\n')
