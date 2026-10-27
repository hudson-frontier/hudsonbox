"""Async project client. Retain mutation keys and payloads before submitting."""

import asyncio
import hashlib
import os
from pathlib import Path
import re
import tempfile
import uuid
from ._requests import Requests
from ._wire import ClientError
from .models import FileCaptureRequest


def new_idempotency_key() -> str:
    """Generate once, persist outside this call, and reuse after uncertainty."""
    return str(uuid.uuid4())


class Client(Requests):
    async def wait(self, operation_id: str, seconds: float = 60):
        if type(seconds) not in (float, int) or not 0 < seconds <= 86400:
            raise ClientError("request")
        try:
            async with asyncio.timeout(seconds):
                while True:
                    operation = await self.get_operation(operation_id=operation_id)
                    if operation.operation_id != operation_id:
                        raise ClientError("protocol")
                    if operation.status in (
                        "succeeded",
                        "failed",
                        "cancelled",
                        "unknown",
                    ):
                        return operation
                    if operation.status not in ("queued", "running"):
                        raise ClientError("protocol")
                    await asyncio.sleep(0.5)
        except TimeoutError:
            raise ClientError("wait_timeout") from None

    async def upload(
        self,
        *,
        sandbox_id: str,
        path: str,
        data: bytes,
        idempotency_key: str,
        mode: str = "0644",
    ):
        if (
            not isinstance(data, bytes)
            or len(data) > 8388608
            or mode not in ("0644", "0755")
        ):
            raise ClientError("request")
        return await self.upload_file(
            sandbox_id=sandbox_id,
            path=path,
            body=data,
            idempotency_key=idempotency_key,
            x_file_size=len(data),
            x_file_sha256=hashlib.sha256(data).hexdigest(),
            x_file_mode=mode,
        )

    async def download(self, *, sandbox_id: str, path: str, destination: str | Path):
        try:
            raw_destination = os.fspath(destination)
            if (
                not isinstance(raw_destination, str)
                or not raw_destination
                or "\x00" in raw_destination
            ):
                raise ClientError("request")
            os.fsencode(raw_destination)
        except (TypeError, ValueError):
            raise ClientError("request") from None
        # Path normalizes away trailing separators and '.', changing a directory
        # request into a file name. Validate that syntax before normalization.
        if os.path.basename(raw_destination) in ("", ".", ".."):
            raise ClientError("file")
        try:
            destination = Path(raw_destination)
            with tempfile.NamedTemporaryFile(dir=destination.parent) as file:
                capture = await self.capture_file(
                    sandbox_id=sandbox_id, body=FileCaptureRequest(path=path)
                )
                released = False
                try:
                    if (
                        capture.size > 8388608
                        or capture.chunk_size != 32768
                        or not capture.guest_reported
                        or not re.fullmatch("[0-9a-f]{64}", capture.sha256)
                    ):
                        raise ClientError("protocol")
                    offset = 0
                    digest = hashlib.sha256()
                    async with asyncio.timeout(60):
                        while True:
                            chunk = await self.read_captured_file(
                                sandbox_id=sandbox_id,
                                x_file_capture=capture.capture,
                                offset=offset,
                                limit=32768,
                            )
                            if (
                                chunk.size != capture.size
                                or chunk.sha256 != capture.sha256
                                or chunk.simulated != capture.simulated
                            ):
                                raise ClientError("integrity")
                            file.write(chunk.data)
                            digest.update(chunk.data)
                            offset = chunk.next_offset
                            if chunk.eof:
                                break
                    if offset != capture.size or digest.hexdigest() != capture.sha256:
                        raise ClientError("integrity")
                except TimeoutError:
                    raise ClientError("transport") from None
                finally:
                    try:
                        async with asyncio.timeout(2):
                            await self.release_file_capture(
                                sandbox_id=sandbox_id, x_file_capture=capture.capture
                            )
                            released = True
                    except (ClientError, TimeoutError):
                        pass
                file.flush()
                os.fsync(file.fileno())
                os.link(
                    file.name, destination
                )  # Same directory, atomic and never replaces an existing path.
                return {
                    "size": capture.size,
                    "sha256": capture.sha256,
                    "simulated": capture.simulated,
                    "guest_reported": capture.guest_reported,
                    "release_confirmed": released,
                }
        except OSError:
            raise ClientError("file") from None
