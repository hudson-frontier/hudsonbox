import asyncio
import base64
import hashlib
import importlib.util
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

import httpx
import hudsonbox
from hudsonbox import Client, ClientError

ROOT = Path(__file__).resolve().parents[3]
EXAMPLES = ROOT / "sdk/python/examples"
sys.path.insert(0, str(EXAMPLES))
spec = importlib.util.spec_from_file_location(
    "recover_operation", EXAMPLES / "recover_operation.py"
)
example = importlib.util.module_from_spec(spec)
spec.loader.exec_module(example)
CORPUS = json.loads((ROOT / "api/conformance/workflow.json").read_text())
SB, OP = CORPUS["sandbox_id"], CORPUS["operation_id"]
OTHER_OP = OP[:-1] + "3"


def reply(status, value):
    return httpx.Response(
        status,
        headers={"cache-control": "no-store", "content-type": "application/json"},
        stream=httpx.ByteStream(json.dumps(value).encode()),
    )


def admitted(op=OP):
    return {
        "sandbox_id": SB,
        "operation_id": op,
        "status": "queued",
        "status_url": "/v1/operations/" + op,
    }


def operation(kind, **extra):
    return {
        "sandbox_id": SB,
        "operation_id": OP,
        "kind": CORPUS["operation_kinds"][kind],
        "status": "succeeded",
        "created_at": "2026-10-14T00:00:00Z",
        **extra,
    }


class WorkflowTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        credential = {
            "version": 1,
            "project_id": "prj_fixture",
            "name": "workflow",
            "token": "private-workflow-token",
            "created_at": 1,
            "expires_at": 4102444800,
        }
        profile = {
            "version": 1,
            "endpoint": "https://sandbox.test",
            "credential_file": "credential.json",
        }
        for name, value in (("credential.json", credential), ("client.json", profile)):
            path = self.root / name
            path.write_text(json.dumps(value))
            path.chmod(0o600)
        self.config = self.root / "client.json"
        self.binding = example.state.binding(self.config)
        self.requests = []
        self.handler = lambda request: reply(202, admitted())
        self.client = Client(self.config)
        await self.client._http.aclose()

        def route(request):
            self.requests.append(request)
            self.assertEqual(
                request.headers["authorization"], "Bearer private-workflow-token"
            )
            return self.handler(request)

        self.client._http = httpx.AsyncClient(transport=httpx.MockTransport(route))

    async def asyncTearDown(self):
        await self.client.aclose()
        self.temporary.cleanup()

    def prepare(self, kind):
        directory = self.root / kind
        directory.mkdir(mode=0o700)
        with example.Journal(directory) as journal:
            example.prepare(
                journal,
                self.binding,
                CORPUS["logical_run_id"],
                kind,
                CORPUS["requests"][kind],
            )
        return directory

    async def call(self, directory, action="submit", seconds=10):
        with example.Journal(directory) as journal:
            return await example.recover(
                self.client, journal, self.binding, action, seconds
            )

    async def test_versioned_corpus_all_mutations_keep_keys_payloads_and_poll_existing_handles(
        self,
    ):
        self.assertEqual(CORPUS["version"], 1)
        self.assertEqual(CORPUS["api_version"], hudsonbox.__version__)
        for kind in example.KINDS:
            with self.subTest(kind=kind):
                directory = self.prepare(kind)
                original = (directory / "request.json").read_bytes()
                self.handler = lambda request: reply(202, admitted())
                admitted_reply = await self.call(directory)
                self.assertTrue(admitted_reply["admission_only"])
                mutation = self.requests[-1]
                saved = json.loads(original)
                self.assertEqual(
                    mutation.headers["idempotency-key"], saved["idempotency_key"]
                )
                self.assertEqual(
                    admitted_reply["logical_run_id"], CORPUS["logical_run_id"]
                )
                if kind == "upload":
                    expected = base64.b64decode(CORPUS["requests"][kind]["data_base64"])
                    self.assertEqual(mutation.content, expected)
                    self.assertEqual(
                        mutation.headers["x-file-sha256"],
                        hashlib.sha256(expected).hexdigest(),
                    )
                self.handler = lambda request: reply(
                    200,
                    operation(
                        kind, target_operation_id=OP if kind == "cancel" else None
                    ),
                )
                count = len(self.requests)
                report = await self.call(
                    directory
                )  # Restart-style submit with saved handle.
                self.assertEqual(len(self.requests), count + 1)
                self.assertEqual(self.requests[-1].method, "GET")
                self.assertEqual(report["operation_id"], OP)
                self.assertEqual((directory / "request.json").read_bytes(), original)

    async def test_lost_replies_across_all_mutations_require_explicit_identical_retry(
        self,
    ):
        for kind in example.KINDS:
            directory = self.prepare(kind)
            original = (directory / "request.json").read_bytes()
            before = len(self.requests)

            def lost(request):
                raise httpx.ReadError("private-workflow-token", request=request)

            self.handler = lost
            with self.assertRaises(ClientError) as error:
                await self.call(directory)
            self.assertEqual(error.exception.kind, "transport")
            self.assertEqual(len(self.requests), before + 1)
            self.assertFalse((directory / "receipt.json").exists())
            first = self.requests[-1]
            self.handler = lambda request: reply(202, admitted())
            await self.call(directory)
            last = self.requests[-1]
            self.assertEqual(first.method, last.method)
            self.assertEqual(first.content, last.content)
            self.assertEqual(
                first.headers["idempotency-key"], last.headers["idempotency-key"]
            )
            self.assertEqual((directory / "request.json").read_bytes(), original)

    async def test_changed_payload_or_profile_and_state_clobber_fail_closed(self):
        directory = self.prepare("execute")
        with example.Journal(directory) as journal:
            with self.assertRaises(FileExistsError):
                example.prepare(
                    journal,
                    self.binding,
                    "changed-run",
                    "execute",
                    CORPUS["requests"]["execute"],
                )
            with self.assertRaises(example.state.StateError):
                await example.recover(self.client, journal, "changed-binding", "submit")
        await self.call(directory)
        value = json.loads((directory / "request.json").read_bytes())
        value["request"]["body"]["argv"] = ["different"]
        (directory / "request.json").write_bytes(example.state.encode(value))
        before = len(self.requests)
        with self.assertRaises(example.state.StateError):
            await self.call(directory)
        self.assertEqual(len(self.requests), before)

    async def test_directory_durability_failure_and_poll_without_handle_make_no_request(
        self,
    ):
        directory = self.prepare("create")
        with patch.object(example.os, "fsync", side_effect=OSError("sync")):
            with self.assertRaises(OSError):
                await self.call(directory)
        with self.assertRaises(example.state.StateError):
            await self.call(directory, "poll")
        self.assertEqual(self.requests, [])

    async def test_response_expiry_retains_create_handle_and_reports_explicit_outcome(
        self,
    ):
        directory = self.prepare("create")
        self.handler = lambda request: reply(
            410,
            {
                "title": "private-workflow-token",
                "status": 410,
                "code": "response_expired",
                "operation_id": OP,
            },
        )
        with self.assertRaises(ClientError):
            await self.call(directory)
        self.assertTrue((directory / "receipt.json").exists())
        original = (directory / "receipt.json").read_bytes()
        self.handler = lambda request: reply(
            200, operation("create", response_expired=True)
        )
        report = await self.call(directory)
        self.assertEqual(self.requests[-1].method, "GET")
        self.assertEqual(report["outcome"], "response_expired")
        self.assertEqual(report["sandbox_id"], SB)
        with example.Journal(directory) as journal:
            _, digest = example.load(journal, self.binding)
            self.assertEqual(example.receipt(journal, digest)["sandbox_id"], SB)
        await self.call(directory, "poll")
        self.assertEqual((directory / "receipt.json").read_bytes(), original)
        self.assertEqual([r.method for r in self.requests], ["POST", "GET", "GET"])

    async def test_expired_handle_rejects_wrong_kind_and_mismatched_resolved_identity(
        self,
    ):
        directory = self.prepare("create")
        with example.Journal(directory) as journal:
            _, digest = example.load(journal, self.binding)
            example.save_receipt(journal, digest, OP, None)
        self.handler = lambda request: reply(200, operation("execute"))
        with self.assertRaises(example.state.StateError):
            await self.call(directory, "poll")
        self.assertFalse((directory / "resolved-receipt.json").exists())
        with example.Journal(directory) as journal:
            original = example.receipt(journal, digest)
            journal.write_new(
                "resolved-receipt.json",
                example.state.encode(
                    {**original, "operation_id": OTHER_OP, "sandbox_id": SB}
                ),
            )
        count = len(self.requests)
        with self.assertRaises(example.state.StateError):
            await self.call(directory, "poll")
        self.assertEqual(len(self.requests), count)

    async def test_wait_timeout_and_disconnect_never_cancel_or_destroy(self):
        directory = self.prepare("execute")
        await self.call(directory)
        self.requests.clear()
        self.handler = lambda request: reply(
            200, operation("execute", status="running")
        )
        with self.assertRaises(ClientError) as error:
            await self.call(directory, "wait", seconds=0.05)
        self.assertEqual(error.exception.kind, "wait_timeout")
        self.assertEqual(error.exception.operation_id, OP)
        task = asyncio.create_task(self.call(directory, "wait", seconds=10))
        await asyncio.sleep(0.05)
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertTrue(all(r.method == "GET" for r in self.requests))

    async def test_unknown_failure_process_nonzero_simulation_and_expiry_classification(
        self,
    ):
        directory = self.prepare("execute")
        await self.call(directory)
        results = [
            ({"status": "unknown"}, "unknown_effects"),
            (
                {"status": "failed", "error": {"code": "host_lost"}},
                "infrastructure_failure",
            ),
            (
                {
                    "status": "failed",
                    "result": {
                        "exit_code": 7,
                        "signal": None,
                        "guest_reported": True,
                        "simulated": False,
                    },
                },
                "process_result",
            ),
            (
                {
                    "result": {
                        "exit_code": 0,
                        "signal": None,
                        "guest_reported": True,
                        "simulated": True,
                    }
                },
                "simulated_process_result",
            ),
            ({"response_expired": True}, "response_expired"),
            ({"result": {"exit_code": 0, "signal": None}}, "result_unavailable"),
        ]
        for extra, expected in results:
            self.handler = lambda request: reply(200, operation("execute", **extra))
            report = await self.call(directory)
            self.assertEqual(report["outcome"], expected)
            self.assertEqual(self.requests[-1].method, "GET")

    async def test_cancel_preserves_separate_handle_and_validates_target(self):
        directory = self.prepare("cancel")
        self.handler = lambda request: reply(202, admitted(OTHER_OP))
        report = await self.call(directory)
        self.assertEqual(report["operation_id"], OTHER_OP)
        self.handler = lambda request: reply(
            200,
            operation("cancel", operation_id=OTHER_OP, target_operation_id=OTHER_OP),
        )
        with self.assertRaises(example.state.StateError):
            await self.call(directory)
        self.handler = lambda request: reply(
            200, operation("cancel", operation_id=OTHER_OP, target_operation_id=OP)
        )
        report = await self.call(directory)
        self.assertEqual(report["outcome"], "completed")

    async def test_denial_revocation_and_unavailable_output_are_typed_without_empty_success(
        self,
    ):
        directory = self.prepare("execute")
        await self.call(directory)
        for status, code in (
            (404, "not_found"),
            (401, "unauthenticated"),
            (410, "response_expired"),
        ):
            self.handler = lambda request: reply(
                status,
                {"status": status, "code": code, "title": "private-workflow-token"},
            )
            with self.assertRaises(ClientError) as error:
                await self.call(directory)
            self.assertEqual(error.exception.status, status)
            self.assertEqual(error.exception.operation_id, OP)
            self.assertNotIn("private-workflow-token", str(error.exception))
        self.handler = lambda request: reply(
            200, operation("execute", output_status="expired")
        )
        report = await self.call(directory)
        self.assertEqual(report["output_status"], "expired")


if __name__ == "__main__":
    unittest.main()
