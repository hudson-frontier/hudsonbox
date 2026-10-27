import json
import copy
import dataclasses
import tempfile
import unittest
from pathlib import Path
from hudsonbox import Client, ClientError, models, new_idempotency_key
from hudsonbox._wire import parse_json, decode_model, encode_model, json_bytes
from hudsonbox._stream import Parser


class ClientTests(unittest.TestCase):
    def test_exact_integer_bounds(self):
        for number in (0, 2**53 + 1, 2**64 - 1):
            value = decode_model(
                "OutputStats",
                parse_json('{"seen":' + str(number) + ',"stored":0,"truncated":true}'),
            )
            self.assertEqual(value.seen, number)
            self.assertIn(str(number).encode(), json_bytes(value.to_wire()))
        for raw in ("-1", "18446744073709551616", "1.0", "true"):
            with self.subTest(raw=raw), self.assertRaises(ClientError):
                decode_model(
                    "OutputStats",
                    parse_json('{"seen":' + raw + ',"stored":0,"truncated":true}'),
                )

    def test_defaults_null_and_closed_models(self):
        self.assertEqual(
            models.CommandInput(argv=["true"], deadline_unix_ms=1).to_wire(),
            dict(
                argv=["true"], env={}, cwd="/", deadline_unix_ms=1, output_limit=1048576
            ),
        )
        self.assertEqual(encode_model("DestroyRequest", {}), {"correlation_id": None})
        for value in (
            {"argv": [], "deadline_unix_ms": 1, "env": None},
            {"argv": [], "deadline_unix_ms": 1, "unknown": True},
        ):
            with self.assertRaises(ClientError):
                encode_model("CommandInput", value)
        with self.assertRaises(ClientError):
            decode_model("SandboxList", {"items": []})
        self.assertEqual(
            decode_model("SandboxList", {"items": [], "next_cursor": None}).to_wire(),
            {"items": [], "next_cursor": None},
        )

    def test_bad_json(self):
        for raw in (
            b"\xff",
            '{"x":1,"x":2}',
            "NaN",
            "1e999",
            '"\\ud800"',
            "[" * 130 + "0" + "]" * 130,
        ):
            with self.subTest(raw=repr(raw)[:30]), self.assertRaises(ClientError):
                parse_json(raw)

    def test_optional_generation_rejects_explicit_null_without_losing_omission(self):
        body = {"argv": ["true"], "deadline_unix_ms": 1, "expected_generation": None}
        for operation, kind in ((encode_model, "request"), (decode_model, "protocol")):
            for value in (body, models.CommandInput(**body)):
                with self.subTest(
                    kind=kind, model=isinstance(value, models.CommandInput)
                ):
                    with self.assertRaises(ClientError) as error:
                        operation("CommandInput", value)
                    self.assertEqual(error.exception.kind, kind)
        command = models.CommandInput(argv=["true"], deadline_unix_ms=1)
        self.assertNotIn("expected_generation", command.to_wire())
        self.assertEqual(copy.deepcopy(command).to_wire(), command.to_wire())
        self.assertEqual(
            encode_model("CommandInput", dataclasses.asdict(command)), command.to_wire()
        )
        self.assertEqual(
            decode_model("CommandInput", command.to_wire()).to_wire(), command.to_wire()
        )
        for generation in (0, 3):
            command.expected_generation = generation
            self.assertEqual(command.to_wire()["expected_generation"], generation)
            self.assertEqual(
                encode_model("CommandInput", dataclasses.asdict(command))[
                    "expected_generation"
                ],
                generation,
            )
        with self.assertRaises(ClientError):
            encode_model(
                "CommandInput", dataclasses.asdict(models.CommandInput(**body))
            )
        problem = decode_model(
            "ProblemBody",
            {
                "title": "fixture",
                "status": 409,
                "code": "conflict",
                "operation_id": None,
            },
        )
        self.assertIsNone(problem.operation_id)

    def test_key_and_redaction(self):
        self.assertNotEqual(new_idempotency_key(), new_idempotency_key())
        secret = "private-backend-text"
        e = ClientError("http", 503, secret, secret)
        self.assertNotIn(secret, str(e) + repr(e))
        self.assertEqual(e.code, "unrecognized_problem_code")
        self.assertIsNone(e.operation_id)
        self.assertNotIn(
            secret, repr(models.CommandInput(argv=[secret], deadline_unix_ms=1))
        )

    def test_stream_bounds_and_incomplete_frames(self):
        parser = Parser()
        for b in b'event: gap\ndata: {"code":"output_expired"}\n':
            self.assertIsNone(parser.byte(b))
        self.assertEqual(parser.byte(10).event, "gap")
        parser = Parser()
        with self.assertRaises(ClientError):
            for b in b"x" * 65537:
                parser.byte(b)

    def test_private_config(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            config = root / "config"
            credential = root / "credential"

            def write(path, data):
                path.write_text(json.dumps(data))
                path.chmod(0o600)

            creds = dict(
                version=1,
                project_id="prj_fixture",
                name="fixture",
                token="test-private-token",
                created_at=1,
                expires_at=4102444800,
            )
            cfg = dict(
                version=1, endpoint="https://localhost", credential_file="credential"
            )
            write(credential, creds)
            write(config, cfg)
            for endpoint in (
                "http://localhost",
                "https://user:secret@localhost",
                "https://localhost/path",
                "https://localhost/?",
                "https://localhost/#",
            ):
                write(config, dict(cfg, endpoint=endpoint))
                with self.assertRaises(ClientError):
                    Client(config)
            write(config, cfg)
            credential.chmod(0o644)
            with self.assertRaises(ClientError):
                Client(config)
            credential.chmod(0o600)
            (root / "link").symlink_to(credential)
            write(config, dict(cfg, credential_file="link"))
            with self.assertRaises(ClientError):
                Client(config)
            write(config, cfg)
            write(credential, dict(creds, expires_at=2**63))
            with self.assertRaises(ClientError):
                Client(config)
            write(credential, creds)
            config.write_bytes(b"x" * 65537)
            with self.assertRaises(ClientError):
                Client(config)


if __name__ == "__main__":
    unittest.main()
