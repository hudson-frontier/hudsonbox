#!/usr/bin/env python3
"""Pinned build stages and offline verification; never mount or run guest bytes."""

import argparse
import hashlib
import json
import os
from pathlib import Path
from pathlib import PurePosixPath
import re
import shutil
import stat
import struct
import subprocess
import tarfile
import tempfile
import urllib.request
import uuid

ROOT = Path(__file__).resolve().parents[1]
PINS_PATH = ROOT / "dev/guest-image/pins.json"
# Guest and supervisor versions must equal the product version (docs/versioning.md).
GUEST_VERSION = (ROOT / "VERSION").read_text(encoding="utf-8").strip()
MAX_METADATA = 65536
DISK_MIB = 256
ARCHES = {"x86_64": 62, "aarch64": 183}
REQUIRED = {
    "CONFIG_MODULES": "n",
    "CONFIG_KEXEC": "n",
    "CONFIG_KEXEC_FILE": "n",
    "CONFIG_SECURITY": "y",
    "CONFIG_SECURITYFS": "y",
    "CONFIG_SECURITY_LOCKDOWN_LSM": "y",
    "CONFIG_SECURITY_LOCKDOWN_LSM_EARLY": "y",
    "CONFIG_LOCK_DOWN_KERNEL_FORCE_CONFIDENTIALITY": "y",
    "CONFIG_CGROUPS": "y",
    "CONFIG_CGROUP_PIDS": "y",
    "CONFIG_NAMESPACES": "y",
    "CONFIG_PID_NS": "y",
    "CONFIG_PROC_FS": "y",
    "CONFIG_SYSFS": "y",
    "CONFIG_DEVTMPFS": "y",
    "CONFIG_EXT4_FS": "y",
    "CONFIG_VIRTIO_BLK": "y",
    "CONFIG_VSOCKETS": "y",
    "CONFIG_VIRTIO_VSOCKETS": "y",
}
ARTIFACTS = {
    "kernel.bin",
    "kernel.config",
    "rootfs.ext4",
    "sandbox-guest",
    "Cargo.lock",
    "linux-source.tar.xz",
    "pins.json",
    "hardening.config",
}


class ImageError(Exception):
    pass


def require(condition, message):
    if not condition:
        raise ImageError(message)


def sha256(path, cap=512 * 1024 * 1024):
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_NONBLOCK", 0)
    fd = os.open(path, flags)
    with os.fdopen(fd, "rb") as file:
        meta = os.fstat(file.fileno())
        require(
            stat.S_ISREG(meta.st_mode) and 0 < meta.st_size <= cap,
            "invalid artifact file or size",
        )
        digest = hashlib.sha256()
        total = 0
        while chunk := file.read(1024 * 1024):
            total += len(chunk)
            require(total <= cap, "artifact byte limit exceeded")
            digest.update(chunk)
        require(total == meta.st_size, "artifact size changed while reading")
        return digest.hexdigest(), total


def recipe_bytes(path):
    """Canonical text inputs across LF/CRLF checkouts; artifact hashes stay exact."""
    fd = os.open(
        path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_NONBLOCK", 0)
    )
    with os.fdopen(fd, "rb") as source:
        meta = os.fstat(source.fileno())
        require(
            stat.S_ISREG(meta.st_mode) and 0 < meta.st_size <= MAX_METADATA,
            "invalid recipe file or size",
        )
        raw = source.read(MAX_METADATA + 1)
        require(
            len(raw) == meta.st_size and len(raw) <= MAX_METADATA,
            "recipe byte limit or concurrent change",
        )
    return raw.replace(b"\r\n", b"\n")


def read_json(path):
    fd = os.open(
        path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_NONBLOCK", 0)
    )
    with os.fdopen(fd, "rb") as source:
        meta = os.fstat(source.fileno())
        require(
            stat.S_ISREG(meta.st_mode) and meta.st_size <= MAX_METADATA,
            "invalid metadata file",
        )
        raw = source.read(MAX_METADATA + 1)
        require(len(raw) <= MAX_METADATA, "metadata byte limit exceeded")

    def unique(pairs):
        value = {}
        for key, item in pairs:
            require(key not in value, "duplicate metadata key")
            value[key] = item
        return value

    return json.loads(
        raw,
        object_pairs_hook=unique,
        parse_constant=lambda _: require(False, "nonfinite metadata"),
    )


def kernel_config(path):
    require(path.stat().st_size <= 1024 * 1024, "kernel config too large")
    values = {}
    for line in path.read_text().splitlines():
        match = re.fullmatch(r"(CONFIG_[A-Z0-9_]+)=(.*)", line)
        disabled = re.fullmatch(r"# (CONFIG_[A-Z0-9_]+) is not set", line)
        if match or disabled:
            key, value = (match[1], match[2]) if match else (disabled[1], "n")
            require(key not in values, "duplicate kernel setting")
            values[key] = value
    require(
        all(values.get(k, "n") == v for k, v in REQUIRED.items()),
        "required kernel setting missing or incompatible",
    )
    require(
        not any(v == "m" for v in values.values()),
        "kernel must contain no loadable modules",
    )
    require(
        values.get("CONFIG_LSM") == '"lockdown"', "kernel lockdown selection mismatch"
    )
    return values


def elf(path, arch):
    with path.open("rb") as file:
        head = file.read(64)
    require(
        len(head) == 64
        and head[:6] == b"\x7fELF\x02\x01"
        and struct.unpack_from("<H", head, 18)[0] == ARCHES[arch],
        "guest ELF architecture mismatch",
    )


def kernel_binary(path, arch):
    if arch == "x86_64":
        elf(path, arch)
    else:
        with path.open("rb") as file:
            head = file.read(64)
        require(
            len(head) == 64 and head[56:60] == b"ARM\x64",
            "guest kernel architecture mismatch",
        )


def fetch(url, expected, destination, cap):
    require(url.startswith("https://"), "source must use HTTPS")
    digest = hashlib.sha256()
    with (
        urllib.request.urlopen(url, timeout=60) as response,
        destination.open("xb") as file,
    ):
        require(response.url.startswith("https://"), "source redirect must use HTTPS")
        total = 0
        while chunk := response.read(1024 * 1024):
            total += len(chunk)
            require(total <= cap, "source byte limit exceeded")
            digest.update(chunk)
            file.write(chunk)
    require(digest.hexdigest() == expected, "source digest mismatch")


def extract_source(
    archive, destination, prefix=None, allow_links=False, max_bytes=64 * 1024 * 1024
):
    """Python 3.11-compatible extraction into a new owned tree; links come last."""
    with tarfile.open(archive) as source:
        members, total = [], 0
        for member in source:
            total += member.size
            require(
                len(members) < 200000
                and 0 <= member.size <= max_bytes
                and total <= max_bytes,
                "source archive limit exceeded",
            )
            members.append(member)
        names, links = set(), []
        for member in members:
            path = PurePosixPath(member.name)
            require(
                member.name
                and not path.is_absolute()
                and not any(p in (".", "..") for p in member.name.split("/"))
                and "\\" not in member.name
                and not any(ord(c) < 32 for c in member.name)
                and member.name not in names,
                "unsafe or duplicate source archive path",
            )
            require(
                prefix is None or path.parts[0] == prefix,
                "unexpected source archive root",
            )
            names.add(member.name)
            require(
                member.isfile()
                or member.isdir()
                or (allow_links and (member.issym() or member.islnk())),
                "unsupported source archive member",
            )
            if member.issym() or member.islnk():
                links.append(member)
        link_names = {m.name for m in links}
        for member in members:
            require(
                not any(
                    parent.as_posix() in link_names
                    for parent in PurePosixPath(member.name).parents
                ),
                "source path traverses a link",
            )
            if member in links:
                continue
            target = destination / member.name
            require(
                target.resolve().is_relative_to(destination.resolve()),
                "source extraction leaves destination",
            )
            if member.isdir():
                target.mkdir(parents=True, exist_ok=True)
            else:
                target.parent.mkdir(parents=True, exist_ok=True)
                with (
                    source.extractfile(member) as incoming,
                    target.open("xb") as outgoing,
                ):
                    shutil.copyfileobj(incoming, outgoing)
                target.chmod(member.mode & 0o755)
                os.utime(target, (member.mtime, member.mtime))
        for member in links:
            target = destination / member.name
            require(
                not PurePosixPath(member.linkname).is_absolute()
                and not any(ord(c) < 32 for c in member.linkname),
                "unsafe source link",
            )
            pointed = (
                target.parent / member.linkname
                if member.issym()
                else destination / member.linkname
            )
            root = destination / prefix if prefix else destination
            require(
                pointed.resolve().is_relative_to(root.resolve()),
                "source link leaves source root",
            )
            target.parent.mkdir(parents=True, exist_ok=True)
            if member.issym():
                target.symlink_to(member.linkname)
            else:
                require(
                    pointed.is_file() and not pointed.is_symlink(),
                    "source hardlink target must be a regular file",
                )
                os.link(pointed, target)


def fetch_kernel(arch, directory):
    pins = read_json(PINS_PATH)
    require(not directory.exists(), "kernel build directory already exists")
    archive = directory.parent / "linux.tar.xz"
    fetch(pins["kernel_url"], pins["kernel_sha256"], archive, 200 * 1024 * 1024)
    prefix = "linux-" + pins["kernel_version"]
    unpacked = directory.parent / prefix
    require(not unpacked.exists(), "kernel source root already exists")
    extract_source(
        archive,
        directory.parent,
        prefix=prefix,
        allow_links=True,
        max_bytes=3 * 1024 * 1024 * 1024,
    )
    unpacked.rename(directory)
    fetch(
        pins["kernel_configs"][arch]["url"],
        pins["kernel_configs"][arch]["sha256"],
        directory / ".config",
        1024 * 1024,
    )


def run(*args, **kwargs):
    return subprocess.run(args, check=True, **kwargs)


def rootfs_image(rootfs, output, epoch, identity):
    require(
        rootfs.is_dir() and not rootfs.is_symlink(),
        "rootfs staging must be a directory",
    )
    paths = sorted(rootfs.rglob("*"))
    for path in [rootfs, *paths]:
        require(
            not any(ord(c) < 32 for c in path.as_posix()), "unsupported rootfs filename"
        )
        os.utime(path, (epoch, epoch), follow_symlinks=False)
    fs_uuid = str(uuid.UUID(hashlib.sha256(identity.encode()).hexdigest()[:32]))
    with output.open("xb") as file:
        file.truncate(DISK_MIB << 20)
    env = {**os.environ, "E2FSPROGS_FAKE_TIME": str(epoch)}
    run(
        "mke2fs",
        "-q",
        "-t",
        "ext4",
        "-b",
        "4096",
        "-I",
        "256",
        "-m",
        "0",
        "-L",
        "hudson-rootfs",
        "-U",
        fs_uuid,
        "-E",
        "lazy_itable_init=0,lazy_journal_init=0,root_owner=0:0,hash_seed=" + fs_uuid,
        "-d",
        str(rootfs),
        str(output),
        env=env,
    )
    # Population copies source ctimes, which Unix cannot set. Normalize all four
    # inode timestamps after population rather than relying on Docker layer time.
    with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8") as commands:
        for name in [
            "/",
            "/lost+found",
            *["/" + p.relative_to(rootfs).as_posix() for p in paths],
        ]:
            escaped = name.replace("\\", "\\\\").replace('"', '\\"')
            for field in ("atime", "mtime", "ctime", "crtime"):
                commands.write(
                    'set_inode_field "'
                    + escaped
                    + '" '
                    + field
                    + " @"
                    + str(epoch)
                    + "\n"
                )
        commands.flush()
        run(
            "debugfs",
            "-w",
            "-f",
            commands.name,
            str(output),
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            env=env,
        )


def assemble(arch, revision, rootfs, output):
    pins = read_json(PINS_PATH)
    require(
        re.fullmatch(r"[0-9a-f]{40}", revision) is not None,
        "source revision must be immutable",
    )
    agent_sha, _ = sha256(output / "sandbox-guest", 64 * 1024 * 1024)
    elf(output / "sandbox-guest", arch)
    kernel_binary(output / "kernel.bin", arch)
    kernel_config(output / "kernel.config")
    for name in ("init", "usr/local/libexec/hudson/sandbox-guest"):
        path = rootfs / name
        require(
            not path.is_symlink()
            and stat.S_IMODE(path.stat().st_mode) == 0o755
            and sha256(path, 64 * 1024 * 1024)[0] == agent_sha,
            "required guest init/agent missing or changed",
        )
    for name in ("proc", "sys", "dev", "run", "tmp", "root", "workspace"):
        (rootfs / name).mkdir(exist_ok=True)
    (rootfs / "tmp").chmod(0o1777)
    # Fresh installations receive no reusable machine identity or credential state.
    for name, data in {
        "machine-id": b"",
        "hostname": b"hudsonbox\n",
        "hosts": b"127.0.0.1 localhost\n",
        "resolv.conf": b"",
    }.items():
        path = rootfs / "etc" / name
        require(
            not path.is_symlink(), "image identity/network file must not be a symlink"
        )
        path.write_bytes(data)
    require(
        not (rootfs / "run/hudson").exists(),
        "bootstrap/agent state must not be baked into image",
    )
    shutil.copyfile(PINS_PATH, output / "pins.json")
    (output / "hardening.config").write_bytes(
        recipe_bytes(ROOT / "dev/guest-image/hardening.config")
    )
    identity = arch + revision + agent_sha + pins["userland_image"]
    rootfs_image(rootfs, output / "rootfs.ext4", pins["source_date_epoch"], identity)
    artifacts = {
        name: dict(zip(("sha256", "size"), sha256(output / name)))
        for name in sorted(ARTIFACTS)
    }
    manifest = {
        "version": 1,
        "architecture": arch,
        "source_revision": revision,
        "guest_version": GUEST_VERSION,
        "bootstrap_version": 1,
        "guest_protocol_version": 1,
        "kernel_version": pins["kernel_version"],
        "firecracker_version": pins["firecracker_version"],
        "minimum_disk_mib": DISK_MIB,
        "source_date_epoch": pins["source_date_epoch"],
        "required_files": {
            "/init": agent_sha,
            "/usr/local/libexec/hudson/sandbox-guest": agent_sha,
        },
        "artifacts": artifacts,
    }
    with (output / "manifest.json").open("x") as file:
        json.dump(manifest, file, sort_keys=True, indent=2)
        file.write("\n")


def verify(directory, expected, arch):
    require(
        directory.is_absolute()
        and directory.resolve() == directory
        and directory.is_dir(),
        "image directory must be existing, absolute and canonical",
    )
    require(
        re.fullmatch(r"[0-9a-f]{64}", expected) is not None,
        "expected manifest digest required",
    )
    require(
        sha256(directory / "manifest.json", MAX_METADATA)[0] == expected,
        "manifest digest mismatch",
    )
    manifest = read_json(directory / "manifest.json")
    require(isinstance(manifest, dict), "manifest must be an object")
    require(
        isinstance(manifest.get("source_revision"), str)
        and re.fullmatch(r"[0-9a-f]{40}", manifest["source_revision"]) is not None,
        "source revision must be immutable",
    )
    require(
        type(manifest.get("source_date_epoch")) is int, "image epoch must be an integer"
    )
    require(
        set(manifest)
        == {
            "version",
            "architecture",
            "source_revision",
            "guest_version",
            "bootstrap_version",
            "guest_protocol_version",
            "kernel_version",
            "firecracker_version",
            "minimum_disk_mib",
            "source_date_epoch",
            "required_files",
            "artifacts",
        },
        "unsupported manifest fields",
    )
    require(
        type(manifest["version"]) is int
        and manifest["version"] == 1
        and manifest["architecture"] == arch,
        "unsupported image format/architecture",
    )
    require(
        manifest["guest_version"] == GUEST_VERSION
        and type(manifest["bootstrap_version"]) is int
        and manifest["bootstrap_version"] == 1
        and type(manifest["guest_protocol_version"]) is int
        and manifest["guest_protocol_version"] == 1,
        "guest protocol compatibility mismatch",
    )
    require(
        type(manifest["minimum_disk_mib"]) is int
        and manifest["minimum_disk_mib"] == DISK_MIB,
        "image disk limit mismatch",
    )
    require(set(manifest["artifacts"]) == ARTIFACTS, "missing or unsupported artifact")
    for name, spec in manifest["artifacts"].items():
        require(
            set(spec) == {"sha256", "size"}
            and type(spec["size"]) is int
            and re.fullmatch(r"[0-9a-f]{64}", spec["sha256"]) is not None,
            "invalid artifact descriptor",
        )
        require(
            sha256(directory / name) == (spec["sha256"], spec["size"]),
            "artifact digest/size mismatch",
        )
    require(
        manifest["artifacts"]["rootfs.ext4"]["size"] == DISK_MIB << 20,
        "rootfs size mismatch",
    )
    pins = read_json(directory / "pins.json")
    require(
        pins == read_json(PINS_PATH)
        and manifest["kernel_version"] == pins["kernel_version"]
        and manifest["firecracker_version"] == pins["firecracker_version"]
        and manifest["source_date_epoch"] == pins["source_date_epoch"],
        "image source/compatibility pin mismatch",
    )
    require(
        manifest["artifacts"]["linux-source.tar.xz"]["sha256"] == pins["kernel_sha256"],
        "kernel source digest mismatch",
    )
    require(
        recipe_bytes(directory / "hardening.config")
        == recipe_bytes(ROOT / "dev/guest-image/hardening.config"),
        "kernel hardening recipe mismatch",
    )
    elf(directory / "sandbox-guest", arch)
    kernel_binary(directory / "kernel.bin", arch)
    kernel_config(directory / "kernel.config")
    require(
        manifest["required_files"]
        == {
            "/init": manifest["artifacts"]["sandbox-guest"]["sha256"],
            "/usr/local/libexec/hudson/sandbox-guest": manifest["artifacts"][
                "sandbox-guest"
            ]["sha256"],
        },
        "required guest files mismatch",
    )
    with tempfile.TemporaryDirectory(prefix="hudson-image-verify-") as stage:
        for number, path in enumerate(manifest["required_files"]):
            inspected = run(
                "debugfs",
                "-R",
                "stat " + path,
                str(directory / "rootfs.ext4"),
                capture_output=True,
                timeout=30,
            )
            info = inspected.stdout.decode("utf-8", errors="strict")
            require(
                len(info) < MAX_METADATA
                and "Type: regular" in info
                and re.search(r"Mode:\s+0?755\b", info) is not None,
                "required image binary missing or non-executable",
            )
            size = re.search(r"Size:\s+(\d+)\b", info)
            require(
                size is not None
                and 0 < int(size[1]) <= 64 * 1024 * 1024
                and int(size[1]) == manifest["artifacts"]["sandbox-guest"]["size"],
                "embedded guest binary size mismatch",
            )
            target = Path(stage) / str(number)
            run(
                "debugfs",
                "-R",
                "dump " + path + " " + str(target),
                str(directory / "rootfs.ext4"),
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                timeout=30,
            )
            require(
                sha256(target, 64 * 1024 * 1024)[0] == manifest["required_files"][path],
                "embedded init/agent digest mismatch",
            )
    return manifest


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    fetch_parser = sub.add_parser("fetch-kernel")
    fetch_parser.add_argument("--architecture", choices=ARCHES, required=True)
    fetch_parser.add_argument("--directory", type=Path, required=True)
    config_parser = sub.add_parser("check-kernel-config")
    config_parser.add_argument("--config", type=Path, required=True)
    assemble_parser = sub.add_parser("assemble")
    assemble_parser.add_argument("--architecture", choices=ARCHES, required=True)
    assemble_parser.add_argument("--source-revision", required=True)
    assemble_parser.add_argument("--rootfs", type=Path, required=True)
    assemble_parser.add_argument("--output", type=Path, required=True)
    verify_parser = sub.add_parser("verify")
    verify_parser.add_argument("--architecture", choices=ARCHES, required=True)
    verify_parser.add_argument("--directory", type=Path, required=True)
    verify_parser.add_argument("--manifest-sha256", required=True)
    args = parser.parse_args()
    try:
        if args.command == "fetch-kernel":
            fetch_kernel(args.architecture, args.directory)
        elif args.command == "check-kernel-config":
            kernel_config(args.config)
        elif args.command == "assemble":
            assemble(args.architecture, args.source_revision, args.rootfs, args.output)
        else:
            manifest = verify(args.directory, args.manifest_sha256, args.architecture)
            print(
                json.dumps(
                    {
                        "verified": True,
                        "architecture": args.architecture,
                        "image_digest": "sha256:"
                        + manifest["artifacts"]["rootfs.ext4"]["sha256"],
                        "minimum_disk_mib": manifest["minimum_disk_mib"],
                    }
                )
            )
    except ImageError as error:
        parser.exit(1, str(error) + ".\n")
    except (
        OSError,
        ValueError,
        TypeError,
        KeyError,
        subprocess.SubprocessError,
        tarfile.TarError,
    ):
        parser.exit(
            1,
            "Guest image build/verification failed; inspect pinned inputs and owned build logs. No guest binary was executed.\n",
        )


if __name__ == "__main__":
    main()
