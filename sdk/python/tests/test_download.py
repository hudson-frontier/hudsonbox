import hashlib
import os
import tempfile
import unittest
from pathlib import Path

from hudsonbox import Client, ClientError, models
from hudsonbox._wire import RangeChunk


class CaptureClient(Client):
    def __init__(self):
        self.data = b"\x00download\xff\n"
        self.digest = hashlib.sha256(self.data).hexdigest()
        self.calls = []

    async def capture_file(self, **_):
        self.calls.append("capture")
        return models.FileCaptureResponse(
            capture="fixture-capture",
            size=len(self.data),
            sha256=self.digest,
            expires_unix_ms=4102444800000,
            chunk_size=32768,
            simulated=True,
            guest_reported=True,
        )

    async def read_captured_file(self, **_):
        self.calls.append("read")
        return RangeChunk(
            data=self.data,
            offset=0,
            next_offset=len(self.data),
            size=len(self.data),
            eof=True,
            simulated=True,
            sha256=self.digest,
            guest_reported=True,
        )

    async def release_file_capture(self, **_):
        self.calls.append("release")


class DownloadTests(unittest.IsolatedAsyncioTestCase):
    async def test_directory_syntax_is_rejected_before_capture(self):
        for suffix in (os.sep, os.sep + ".", os.sep + ".."):
            with self.subTest(suffix=suffix):
                with tempfile.TemporaryDirectory() as directory:
                    root = Path(directory)
                    client = CaptureClient()
                    with self.assertRaises(ClientError) as error:
                        await client.download(
                            sandbox_id="sbx_fixture",
                            path="payload",
                            destination=str(root / "missing") + suffix,
                        )
                    self.assertEqual(error.exception.kind, "file")
                    self.assertEqual(client.calls, [])
                    self.assertEqual(list(root.iterdir()), [])

    async def test_invalid_destination_is_a_request_error_before_capture(self):
        for destination in (
            "",
            "invalid\x00name",
            "invalid\ud800name",
            None,
            1,
            b"bytes",
        ):
            with self.subTest(destination=repr(destination)):
                client = CaptureClient()
                with self.assertRaises(ClientError) as error:
                    await client.download(
                        sandbox_id="sbx_fixture",
                        path="payload",
                        destination=destination,
                    )
                self.assertEqual(error.exception.kind, "request")
                self.assertEqual(client.calls, [])

    async def test_string_and_path_destinations_publish_private_verified_bytes(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for convert in (str, Path):
                with self.subTest(convert=convert.__name__):
                    client = CaptureClient()
                    destination = root / convert.__name__
                    result = await client.download(
                        sandbox_id="sbx_fixture",
                        path="payload",
                        destination=convert(destination),
                    )
                    self.assertEqual(destination.read_bytes(), client.data)
                    self.assertEqual(destination.stat().st_mode & 0o077, 0)
                    self.assertEqual(
                        result,
                        dict(
                            size=len(client.data),
                            sha256=client.digest,
                            simulated=True,
                            guest_reported=True,
                            release_confirmed=True,
                        ),
                    )
                    self.assertEqual(client.calls, ["capture", "read", "release"])
            self.assertEqual({item.name for item in root.iterdir()}, {"str", "Path"})

    async def test_existing_file_and_symlink_are_not_replaced(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            existing = root / "existing"
            existing.write_bytes(b"preserve original")
            link = root / "link"
            link.symlink_to(existing)
            for destination in (existing, link):
                with self.subTest(destination=destination.name):
                    client = CaptureClient()
                    with self.assertRaises(ClientError) as error:
                        await client.download(
                            sandbox_id="sbx_fixture",
                            path="payload",
                            destination=destination,
                        )
                    self.assertEqual(error.exception.kind, "file")
                    self.assertEqual(existing.read_bytes(), b"preserve original")
                    self.assertTrue(link.is_symlink())
                    self.assertEqual(client.calls, ["capture", "read", "release"])
                    self.assertEqual(
                        {item.name for item in root.iterdir()}, {"existing", "link"}
                    )


if __name__ == "__main__":
    unittest.main()
