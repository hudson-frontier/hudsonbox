#!/usr/bin/env python3
"""Build committed native tools into a new private prefix; never start services."""
import argparse
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import platform
import re
import shutil
import stat
import struct
import subprocess
import sys
import tarfile
import tempfile
import tomllib

ROOT = Path(__file__).resolve().parents[1]
SOURCE_PATHS = ('Cargo.toml', 'Cargo.lock', 'rust-toolchain.toml', 'crates', 'proto',
                'migrations', 'dev/runtime-artifacts.json')
PACKAGES = {
    'control-plane': ('sandbox-api', 'sandbox-controller', 'sandbox-cleanup'),
    'linux-runtime': ('sandbox-supervisor', 'sandbox-guest'),
}


class Failure(Exception):
    pass


def require(condition, code):
    if not condition:
        raise Failure(code)


def architecture():
    return {'arm64': 'aarch64', 'ARM64': 'aarch64', 'AMD64': 'x86_64',
            'amd64': 'x86_64'}.get(platform.machine(), platform.machine())


def private_parent(prefix):
    require(prefix.is_absolute() and prefix.name not in ('', '.', '..'), 'absolute_prefix_required')
    parent = prefix.parent
    require(parent.resolve(strict=True) == parent, 'canonical_parent_required')
    meta = parent.stat()
    require(stat.S_ISDIR(meta.st_mode) and meta.st_uid == os.geteuid()
            and meta.st_mode & 0o077 == 0, 'private_owned_parent_required')
    require(not prefix.exists() and not prefix.is_symlink(), 'new_prefix_required')
    return parent


def extract_source(archive, destination):
    require(archive.stat().st_size <= 64 * 1024 * 1024, 'source_archive_too_large')
    seen = set()
    total = 0
    with tarfile.open(archive, mode='r:') as source:
        for member in source:
            name = PurePosixPath(member.name)
            require(not name.is_absolute() and '..' not in name.parts
                    and '\\' not in member.name and name.parts, 'invalid_source_path')
            require(member.name not in seen and len(seen) < 4096, 'duplicate_or_excess_source')
            seen.add(member.name)
            allowed = member.name in SOURCE_PATHS or name.parts[0] in ('crates', 'proto', 'migrations') or (
                member.isdir() and member.name.rstrip('/') == 'dev')
            require(allowed and (member.isdir() or member.isfile()), 'invalid_source_member')
            require(0 <= member.size <= 64 * 1024 * 1024, 'invalid_source_size')
            total += member.size
            require(total <= 64 * 1024 * 1024, 'source_content_too_large')
            target = destination.joinpath(*name.parts)
            if member.isdir():
                target.mkdir(parents=True, exist_ok=True)
            else:
                target.parent.mkdir(parents=True, exist_ok=True)
                with target.open('xb') as output, source.extractfile(member) as content:
                    shutil.copyfileobj(content, output)


def build_binaries(source, profile, target, toolchain, work):
    environment = {**os.environ, 'CARGO_TARGET_DIR': str(target), 'RUSTUP_AUTO_INSTALL': '0'}
    cargo = ['cargo', '+' + toolchain]
    metadata = json.loads(subprocess.check_output(
        [*cargo, 'metadata', '--format-version', '1', '--no-deps', '--locked'],
        cwd=source, env=environment, stderr=subprocess.DEVNULL, timeout=60))
    selected = {item['id']: item for item in metadata['packages'] if item['name'] in PACKAGES[profile]}
    require(len(selected) == len(PACKAGES[profile]), 'required_packages_missing')
    expected = [entry['name'] for package in selected.values() for entry in package['targets'] if 'bin' in entry['kind']]
    require(expected and len(expected) == len(set(expected)), 'ambiguous_binary_names')
    command = [*cargo, 'build', '--release', '--locked', '--bins', '--message-format=json']
    for package in PACKAGES[profile]:
        command.extend(['-p', package])
    log_path = work / 'cargo.jsonl'
    with log_path.open('xb') as output, (work / 'cargo.stderr').open('xb') as errors:
        subprocess.run(command, cwd=source, env=environment, stdout=output, stderr=errors,
                       check=True, timeout=3600)
    require(log_path.stat().st_size <= 32 * 1024 * 1024, 'build_report_too_large')
    binaries = {}
    with log_path.open() as lines:
        for line in lines:
            event = json.loads(line)
            if event.get('reason') == 'compiler-artifact' and event.get('package_id') in selected and event.get('executable'):
                name = event['target']['name']
                require(name in expected and name not in binaries, 'unexpected_binary')
                binaries[name] = Path(event['executable'])
    require(set(binaries) == set(expected), 'build_artifacts_incomplete')
    return binaries


def native_binary(path, system, arch):
    require(path.is_file() and not path.is_symlink() and 0 < path.stat().st_size <= 256 * 1024 * 1024,
            'invalid_binary_file')
    with path.open('rb') as source:
        header = source.read(64)
    if system == 'Linux':
        require(len(header) >= 20 and header[:6] == b'\x7fELF\x02\x01'
                and struct.unpack_from('<H', header, 16)[0] in (2, 3)
                and struct.unpack_from('<H', header, 18)[0] == {'x86_64': 62, 'aarch64': 183}[arch],
                'native_binary_required')
    else:
        require(len(header) >= 32 and header[:4] == b'\xcf\xfa\xed\xfe'
                and struct.unpack_from('<I', header, 12)[0] == 2
                and struct.unpack_from('<I', header, 4)[0] == {'x86_64': 0x01000007, 'aarch64': 0x0100000C}[arch],
                'native_binary_required')


def install(prefix, profile, root=ROOT, builder=build_binaries):
    system, arch = platform.system(), architecture()
    require(system in ('Linux', 'Darwin') and arch in ('x86_64', 'aarch64'), 'native_unix_required')
    require(profile in PACKAGES and (profile != 'linux-runtime' or system == 'Linux'), 'unsupported_profile')
    parent = private_parent(prefix)
    work = Path(tempfile.mkdtemp(prefix='.hudsonbox-build-', dir=parent)).resolve()
    work.chmod(0o700)
    complete = False
    try:
        revision = subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=root, timeout=15).decode().strip()
        require(re.fullmatch(r'[0-9a-f]{40}', revision) is not None, 'immutable_revision_required')
        archive = work / 'source.tar'
        subprocess.run(['git', 'archive', '--format=tar', '--output', str(archive), revision, '--', *SOURCE_PATHS],
                       cwd=root, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=30)
        source = work / 'source'
        source.mkdir()
        extract_source(archive, source)
        for required in ('Cargo.toml', 'Cargo.lock', 'rust-toolchain.toml'):
            require((source / required).is_file(), 'required_source_missing')
        toolchain = tomllib.loads((source / 'rust-toolchain.toml').read_text())['toolchain']['channel']
        require(re.fullmatch(r'\d+\.\d+\.\d+', toolchain) is not None, 'pinned_toolchain_required')
        target = work / 'target'
        binaries = builder(source, profile, target, toolchain, work)
        require(binaries, 'build_artifacts_incomplete')
        require(target.resolve(strict=True) == target and target.is_relative_to(work),
                'target_outside_build')
        for name, path in binaries.items():
            require(re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9._-]{0,127}', name) is not None,
                    'invalid_binary_name')
            require(path.resolve(strict=True).is_relative_to(target.resolve()), 'binary_outside_build')
            native_binary(path, system, arch)
        # Exclusive publication: a concurrent install or any existing prefix wins.
        prefix.mkdir(mode=0o700)
        prefix.chmod(0o700)
        binary_dir = prefix / 'bin'
        binary_dir.mkdir(mode=0o700)
        installed = []
        for name, path in sorted(binaries.items()):
            destination = binary_dir / name
            with path.open('rb') as source_file, destination.open('xb') as output:
                shutil.copyfileobj(source_file, output)
                os.fchmod(output.fileno(), 0o755)
                output.flush()
                os.fsync(output.fileno())
            with destination.open('rb') as output:
                digest = hashlib.file_digest(output, 'sha256').hexdigest()
            installed.append({'name': name, 'sha256': digest, 'size': destination.stat().st_size})
        with archive.open('rb') as source_file:
            archive_digest = hashlib.file_digest(source_file, 'sha256').hexdigest()
        receipt = {'version': 1, 'profile': profile, 'system': system, 'architecture': arch,
                   'source_revision': revision, 'source_archive_sha256': archive_digest,
                   'toolchain': toolchain, 'prefix': str(prefix), 'binaries': installed,
                   'scope': 'native application binaries; services, credentials and runtime qualification are separate'}
        with (prefix / 'install-receipt.json').open('x') as output:
            os.fchmod(output.fileno(), 0o600)
            json.dump(receipt, output, indent=2)
            output.write('\n')
            output.flush()
            os.fsync(output.fileno())
        for directory in (binary_dir, prefix, parent):
            descriptor = os.open(directory, os.O_RDONLY | os.O_DIRECTORY)
            try:
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
        complete = True
        return receipt
    except (Failure, OSError, ValueError, KeyError, tarfile.TarError, subprocess.SubprocessError) as error:
        code = str(error) if isinstance(error, Failure) else 'source_install_failed'
        raise Failure(code + '; private build retained at ' + str(work)) from error
    finally:
        if complete:
            require(work.parent == parent and work.name.startswith('.hudsonbox-build-')
                    and work.resolve() == work, 'owned_build_cleanup_required')
            shutil.rmtree(work)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--prefix', type=Path, required=True)
    parser.add_argument('--profile', choices=PACKAGES, default='control-plane')
    args = parser.parse_args()
    try:
        print(json.dumps(install(args.prefix, args.profile)))
    except (Failure, OSError) as error:
        print('Native installation failed: ' + str(error), file=sys.stderr)
        return 1
    return 0


if __name__ == '__main__':
    sys.exit(main())
