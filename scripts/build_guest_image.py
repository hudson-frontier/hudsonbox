#!/usr/bin/env python3
"""Native Linux Docker build with an allowlisted context and owned artifact output."""

import argparse
import hashlib
import json
from pathlib import Path
import platform
import re
import subprocess
import tarfile
import tempfile

import guest_image

ROOT = Path(__file__).resolve().parents[1]


def git(*args):
    return subprocess.check_output(["git", *args], cwd=ROOT).decode().strip()


def build(destination, arch, no_cache=False):
    guest_image.require(
        platform.system() == "Linux" and platform.machine() == arch,
        "native Linux builder architecture required; emulation does not qualify an image",
    )
    guest_image.require(
        destination.is_absolute()
        and destination.parent.resolve() / destination.name == destination
        and destination.parent.is_dir()
        and not destination.exists()
        and not destination.is_symlink(),
        "destination must be a new canonical absolute directory",
    )
    revision = git("rev-parse", "HEAD")
    guest_image.require(
        re.fullmatch(r"[0-9a-f]{40}", revision) is not None,
        "immutable source revision required",
    )
    pins = guest_image.read_json(guest_image.PINS_PATH)
    destination.mkdir(mode=0o700)
    container = None
    with tempfile.TemporaryDirectory(prefix="hudson-image-build-") as temporary:
        stage = Path(temporary)
        context = stage / "context"
        context.mkdir()
        archive = stage / "source.tar"
        subprocess.run(
            [
                "git",
                "archive",
                "--format=tar",
                "--output",
                str(archive),
                revision,
                "--",
                "Cargo.toml",
                "Cargo.lock",
                "rust-toolchain.toml",
                "VERSION",
                "crates",
                "proto",
            ],
            cwd=ROOT,
            check=True,
        )
        guest_image.require(
            archive.stat().st_size <= 64 * 1024 * 1024,
            "source context byte limit exceeded",
        )
        guest_image.extract_source(archive, context)
        guest_image.require(
            all(
                (context / name).is_file() and (context / name).stat().st_size > 0
                for name in (
                    "Cargo.lock",
                    "Cargo.toml",
                    "rust-toolchain.toml",
                    "VERSION",
                )
            ),
            "committed build inputs are missing after extraction",
        )
        for name in [
            "dev/guest-image/Dockerfile",
            "dev/guest-image/pins.json",
            "dev/guest-image/hardening.config",
            "scripts/guest_image.py",
        ]:
            source = ROOT / name
            guest_image.require(
                source.is_file()
                and not source.is_symlink()
                and source.resolve().is_relative_to(ROOT),
                "invalid build context member",
            )
            target = context / name
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(guest_image.recipe_bytes(source))
        digest = hashlib.sha256()
        for path in sorted(context.rglob("*")):
            if path.is_file():
                digest.update(path.relative_to(context).as_posix().encode() + b"\0")
                digest.update(path.read_bytes())
        iid = stage / "image-id"
        command = [
            "docker",
            "build",
            "--iidfile",
            str(iid),
            "--platform",
            "linux/arm64" if arch == "aarch64" else "linux/amd64",
            "--build-arg",
            "RUST_IMAGE=" + pins["rust_image"],
            "--build-arg",
            "USERLAND_IMAGE=" + pins["userland_image"],
            "--build-arg",
            "HUDSON_ARCH=" + arch,
            "--build-arg",
            "HUDSON_SOURCE_REVISION=" + revision,
            "-f",
            str(context / "dev/guest-image/Dockerfile"),
        ]
        if no_cache:
            command.append("--no-cache")
        command.append(str(context))
        try:
            subprocess.run(command, check=True)
            image_id = iid.read_text().strip()
            guest_image.require(
                re.fullmatch(r"sha256:[0-9a-f]{64}", image_id) is not None,
                "unexpected build image ID",
            )
            container = subprocess.check_output(
                ["docker", "create", image_id, "/never-run"], text=True
            ).strip()
            guest_image.require(
                re.fullmatch(r"[0-9a-f]{64}", container) is not None,
                "unexpected owned container ID",
            )
            subprocess.run(
                ["docker", "cp", container + ":/.", str(destination)], check=True
            )
            expected = guest_image.sha256(
                destination / "manifest.json", guest_image.MAX_METADATA
            )[0]
            manifest = guest_image.verify(destination, expected, arch)
            provenance = {
                "version": 1,
                "source_revision": revision,
                "build_context_sha256": digest.hexdigest(),
                "manifest_sha256": expected,
                "image_digest": "sha256:"
                + manifest["artifacts"]["rootfs.ext4"]["sha256"],
                "architecture": arch,
                "docker_image_id": image_id,
            }
            with (destination / "build-receipt.json").open("x") as file:
                json.dump(provenance, file, sort_keys=True, indent=2)
                file.write("\n")
            print(json.dumps(provenance))
        finally:
            if container is not None:
                subprocess.run(["docker", "rm", container], check=True)
    # Failed/partial output is retained for diagnosis, never overwritten on retry.


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--architecture", choices=guest_image.ARCHES, required=True)
    parser.add_argument("--destination", type=Path, required=True)
    parser.add_argument("--no-cache", action="store_true")
    args = parser.parse_args()
    try:
        build(args.destination, args.architecture, args.no_cache)
    except guest_image.ImageError as error:
        parser.exit(1, str(error) + ". Partial artifact output is retained.\n")
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
            "Guest image build failed. Only an invocation-owned container is removed; partial artifact output is retained.\n",
        )


if __name__ == "__main__":
    main()
