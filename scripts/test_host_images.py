import argparse
from contextlib import redirect_stdout
import io
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import guest_image
import launch_verified_host as launch


@unittest.skipUnless(hasattr(os, "geteuid"), "Unix operator file permissions")
class HostImageTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name).resolve()
        self.image = self.root / "image"
        self.image.mkdir(mode=0o700)
        for name in guest_image.ARTIFACTS | {"manifest.json"}:
            (self.image / name).write_bytes(b"fixture")
            (self.image / name).chmod(0o600)
        self.digest = "sha256:" + "a" * 64
        self.manifest = {
            "artifacts": {
                "rootfs.ext4": {"sha256": "a" * 64},
                "kernel.bin": {"sha256": "b" * 64},
            },
            "minimum_disk_mib": 256,
            "guest_version": guest_image.GUEST_VERSION,
            "bootstrap_version": 1,
            "guest_protocol_version": 1,
            "firecracker_version": "1.17.0",
        }
        self.config = {
            "state_root": "/operator-private-state",
            "images": {
                self.digest: {
                    "kernel": {
                        "path": str(self.image / "kernel.bin"),
                        "sha256": "b" * 64,
                    },
                    "rootfs": {
                        "path": str(self.image / "rootfs.ext4"),
                        "sha256": "a" * 64,
                    },
                }
            },
        }
        self.policy = {
            "version": 1,
            "images": [{"directory": str(self.image), "manifest_sha256": "c" * 64}],
        }
        self.config_path = self.root / "host.json"
        self.policy_path = self.root / "policy.json"
        self.write(self.config_path, self.config)
        self.write(self.policy_path, self.policy)

    def tearDown(self):
        self.temporary.cleanup()

    def write(self, path, value):
        path.write_text(json.dumps(value))
        path.chmod(0o600)

    def args(self, **changes):
        values = dict(
            config=self.config_path,
            image_policy=self.policy_path,
            architecture="x86_64",
            check_only=True,
            host_binary=None,
            host_sha256=None,
            host_arguments=[],
        )
        return argparse.Namespace(**(values | changes))

    def test_every_image_is_verified_and_check_only_exposes_no_configuration(self):
        output = io.StringIO()
        with patch.object(guest_image, "verify", return_value=self.manifest) as verify:
            with redirect_stdout(output), patch.object(launch, "run_host") as start:
                self.assertEqual(launch.run(self.args()), 0)
                start.assert_not_called()
        verify.assert_called_once_with(self.image, "c" * 64, "x86_64")
        self.assertEqual(
            json.loads(output.getvalue()),
            {
                "guest_images_verified": True,
                "architecture": "x86_64",
                "image_digests": [self.digest],
            },
        )
        self.assertNotIn("operator-private", output.getvalue())

    def test_source_configuration_change_cannot_change_launched_snapshot(self):
        original = self.config_path.read_bytes()

        def mutate(*_):
            self.write(self.config_path, self.config | {"state_root": "/changed"})
            return self.manifest

        with patch.object(guest_image, "verify", side_effect=mutate):
            with launch.verified_snapshot(
                self.config_path, self.policy_path, "x86_64"
            ) as (snapshot, images):
                self.assertEqual(snapshot.read_bytes(), original)
                self.assertEqual(snapshot.stat().st_mode & 0o777, 0o600)
                self.assertEqual(images, [self.digest])
                temporary = snapshot.parent
        self.assertFalse(temporary.exists())
        self.assertNotEqual(self.config_path.read_bytes(), original)

    def test_missing_corrupt_or_incomplete_image_prevents_host_start(self):
        with patch.object(
            guest_image,
            "verify",
            side_effect=guest_image.ImageError("required image binary missing"),
        ):
            with patch.object(launch, "run_host") as start:
                with self.assertRaises(guest_image.ImageError):
                    launch.run(self.args(check_only=False))
                start.assert_not_called()

    def test_mapping_alias_or_unverified_extra_image_fails_closed(self):
        for changed in (
            {"sha256:" + "d" * 64: self.config["images"][self.digest]},
            self.config["images"]
            | {"sha256:" + "d" * 64: self.config["images"][self.digest]},
            {
                self.digest: self.config["images"][self.digest]
                | {"rootfs": {"path": "/other", "sha256": "a" * 64}}
            },
        ):
            with self.subTest(changed=changed):
                self.write(self.config_path, self.config | {"images": changed})
                with patch.object(guest_image, "verify", return_value=self.manifest):
                    with self.assertRaises(launch.LaunchError):
                        launch.run(self.args())

    def test_capability_floor_protocol_and_architecture_must_match_verified_manifest(
        self,
    ):
        capabilities = {
            "architecture": "x86_64",
            "firecracker_version": "1.17.0",
            "images": [
                {
                    "rootfs_digest": self.digest,
                    "kernel_digest": "sha256:" + "b" * 64,
                    "minimum_disk_mib": 256,
                    "guest_version": guest_image.GUEST_VERSION,
                    "bootstrap_version": 1,
                    "guest_protocol_version": 1,
                }
            ],
        }
        for change in (
            {"minimum_disk_mib": 64},
            {"guest_protocol_version": 2},
            {"bootstrap_version": True},
        ):
            changed = capabilities | {"images": [capabilities["images"][0] | change]}
            self.write(self.config_path, self.config | {"capabilities": changed})
            with patch.object(guest_image, "verify", return_value=self.manifest):
                with self.assertRaises(launch.LaunchError):
                    launch.run(self.args())
        self.write(self.config_path, self.config | {"capabilities": capabilities})
        with (
            patch.object(guest_image, "verify", return_value=self.manifest),
            redirect_stdout(io.StringIO()),
        ):
            self.assertEqual(launch.run(self.args()), 0)

    def test_unsafe_config_artifact_permissions_and_duplicate_policy_are_rejected(self):
        self.config_path.chmod(0o644)
        with self.assertRaises(launch.LaunchError):
            launch.private_json(self.config_path)
        self.config_path.chmod(0o600)
        alias = self.root / "alias"
        alias.symlink_to(self.config_path)
        with self.assertRaises(launch.LaunchError):
            launch.private_json(alias)
        self.policy_path.write_text('{"version":1,"version":1,"images":[]}')
        with self.assertRaises(launch.LaunchError):
            launch.private_json(self.policy_path)
        self.write(self.policy_path, self.policy)
        (self.image / "rootfs.ext4").chmod(0o666)
        with (
            self.assertRaises(launch.LaunchError),
            patch.object(guest_image, "verify") as verify,
        ):
            launch.run(self.args())
        verify.assert_not_called()

    def test_forwarded_arguments_cannot_replace_verified_configuration(self):
        for arguments in (["--", "--config", "/other"], ["--config=/other"]):
            with patch.object(guest_image, "verify") as verify:
                with self.assertRaises(launch.LaunchError):
                    launch.run(self.args(host_arguments=arguments))
                verify.assert_not_called()

    def test_unprotected_temporary_parent_cannot_receive_verified_snapshot(self):
        unsafe = self.root / "unsafe"
        unsafe.mkdir(mode=0o777)
        unsafe.chmod(0o777)
        with patch.object(guest_image, "verify", return_value=self.manifest):
            with patch.object(launch.tempfile, "gettempdir", return_value=str(unsafe)):
                with self.assertRaises(launch.LaunchError):
                    with launch.verified_snapshot(
                        self.config_path, self.policy_path, "x86_64"
                    ):
                        self.fail("unsafe snapshot created")
        self.assertEqual(list(unsafe.iterdir()), [])


if __name__ == "__main__":
    unittest.main()
