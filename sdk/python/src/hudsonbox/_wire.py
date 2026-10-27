"""Wire types and bounded decoding. Semantic admission remains server-owned."""

from __future__ import annotations
import dataclasses
from enum import Enum
import json
import math
import re
from typing import Any
from ._schema import SCHEMA

_MESSAGES = {
    "configuration": "invalid private client configuration or credential file",
    "request": "invalid or oversized request",
    "transport": "transport failed; a sent mutation may have been admitted; retain its key and payload",
    "protocol": "invalid, oversized or inconsistent server response",
    "wait_timeout": "wait deadline reached; poll the same operation",
    "file": "local file operation failed; destination must not already exist",
    "integrity": "download integrity verification failed",
}


class _Omitted(Enum):
    VALUE = 0


_OMITTED = _Omitted.VALUE


class ClientError(Exception):
    def __init__(
        self,
        kind: str,
        status: int | None = None,
        code: str | None = None,
        operation_id: str | None = None,
    ):
        self.kind, self.status = kind, status
        self.raw_code = code
        self.code = (
            (code if code in SCHEMA["codes"] else "unrecognized_problem_code")
            if code is not None
            else None
        )
        self.operation_id = (
            operation_id
            if isinstance(operation_id, str)
            and re.fullmatch(
                r"op_[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}",
                operation_id,
            )
            else None
        )
        super().__init__(
            f"HTTP request failed with status {status}"
            if kind == "http"
            else _MESSAGES[kind]
        )


class Model:
    def __repr__(self):
        return f"{type(self).__name__}(...)"

    def to_wire(self) -> dict[str, Any]:
        return encode_model(type(self).__name__, self)


def _fail(kind: str):
    raise ClientError(kind)


def parse_json(data: bytes | str):
    def pairs(items):
        result = {}
        for key, value in items:
            if key in result:
                _fail("protocol")
            result[key] = value
        return result

    try:
        text = data.decode("utf-8") if isinstance(data, bytes) else data
        value = json.loads(
            text, object_pairs_hook=pairs, parse_constant=lambda _: _fail("protocol")
        )

        def check(value, depth=0):
            if depth > 128:
                _fail("protocol")
            if isinstance(value, str):
                value.encode("utf-8")
            if isinstance(value, float) and not math.isfinite(value):
                _fail("protocol")
            if isinstance(value, list):
                for item in value:
                    check(item, depth + 1)
            if isinstance(value, dict):
                for key, item in value.items():
                    check(key, depth + 1)
                    check(item, depth + 1)

        check(value)
        return value
    except (ValueError, UnicodeError, RecursionError):
        raise ClientError("protocol") from None


def json_bytes(value, cap=2 * 1024 * 1024):
    try:
        body = json.dumps(
            value, ensure_ascii=False, allow_nan=False, separators=(",", ":")
        ).encode("utf-8")
        if len(body) > cap:
            _fail("request")
        return body
    except (TypeError, ValueError, UnicodeError, RecursionError):
        raise ClientError("request") from None


def _value(p, value, decode, depth):
    if depth > 128:
        _fail("protocol" if decode else "request")
    error = "protocol" if decode else "request"
    kind = p["kind"]
    if kind == "nullable":
        return None if value is None else _value(p["inner"], value, decode, depth + 1)
    if kind == "ref":
        return _model(p["name"], value, decode, depth + 1)
    if kind == "string":
        if not isinstance(value, str):
            _fail(error)
        try:
            value.encode("utf-8")
        except UnicodeError:
            raise ClientError(error) from None
    elif kind == "boolean":
        if type(value) is not bool:
            _fail(error)
    elif kind == "integer":
        if type(value) is not int or not int(p["min"]) <= value <= int(p["max"]):
            _fail(error)
    elif kind == "array":
        if not isinstance(value, list):
            _fail(error)
        return [_value(p["inner"], v, decode, depth + 1) for v in value]
    elif kind == "map":
        if not isinstance(value, dict) or any(not isinstance(k, str) for k in value):
            _fail(error)
        return {
            k: _value(p["inner"], value[k], decode, depth + 1) for k in sorted(value)
        }
    elif kind == "json":
        if value is None or type(value) in (str, bool):
            return value
        if type(value) is int:
            if not -(2**63) <= value <= 2**64 - 1:
                _fail(error)
        elif type(value) is float:
            if not math.isfinite(value):
                _fail(error)
        elif isinstance(value, list):
            return [_value(p, v, decode, depth + 1) for v in value]
        elif isinstance(value, dict):
            if any(not isinstance(k, str) for k in value):
                _fail(error)
            return {k: _value(p, v, decode, depth + 1) for k, v in value.items()}
        else:
            _fail(error)
    return value


def _model(name, value, decode, depth=0):
    error = "protocol" if decode else "request"
    if depth > 128:
        _fail(error)
    if isinstance(value, Model):
        if type(value).__name__ != name:
            _fail(error)
        value = {f.name: getattr(value, f.name) for f in dataclasses.fields(value)}
    if not isinstance(value, dict):
        _fail(error)
    spec = SCHEMA["models"][name]
    if spec["closed"] and value.keys() - spec["fields"].keys():
        _fail(error)
    result = {}
    for field, rule in spec["fields"].items():
        if field not in value or (rule.get("reject_null") and value[field] is _OMITTED):
            if rule["required"]:
                _fail(error)
            if rule.get("reject_null"):
                continue
            item = rule.get("default")
        else:
            item = value[field]
        if item is None and rule.get("reject_null"):
            _fail(error)
        if item is None and not rule["required"] and "default" not in rule:
            result[field] = None
            continue
        result[field] = _value(rule["type"], item, decode, depth + 1)
    if decode:
        from .models import MODELS

        return MODELS[name](**result)
    return {
        k: v
        for k, v in result.items()
        if not (
            v is None
            and not spec["fields"][k]["nullable"]
            and not spec["fields"][k]["required"]
        )
        and not (v is False and spec["fields"][k]["omit_false"])
    }


def decode_model(name: str, value):
    return _model(name, value, True)


def encode_model(name: str, value):
    return _model(name, value, False)


@dataclasses.dataclass(repr=False)
class RangeChunk:
    data: bytes
    offset: int
    next_offset: int
    size: int
    eof: bool
    simulated: bool
    seen: int | None = None
    truncated: bool | None = None
    sha256: str | None = None
    guest_reported: bool | None = None

    def __repr__(self):
        return f"RangeChunk(bytes={len(self.data)}, offset={self.offset})"


def one(headers, name):
    values = headers.get_list(name)
    if len(values) != 1:
        _fail("protocol")
    return values[0]


def uint(value: str):
    if not re.fullmatch(r"[0-9]+", value) or len(value) > 20 or int(value) > 2**64 - 1:
        _fail("protocol")
    return int(value)


def boolean(value: str):
    if value not in ("true", "false"):
        _fail("protocol")
    return value == "true"


def range_chunk(headers, data, prefix, offset, limit):
    def read(field):
        return one(headers, f"x-{prefix}-{field}")

    chunk = RangeChunk(
        data,
        uint(read("offset")),
        uint(read("next-offset")),
        uint(read("size")),
        boolean(read("eof")),
        boolean(read("simulated")),
    )
    if prefix == "output":
        chunk.seen, chunk.truncated = uint(read("seen")), boolean(read("truncated"))
        if (
            chunk.size > 10485760
            or chunk.seen < chunk.size
            or chunk.truncated != (chunk.seen > chunk.size)
        ):
            _fail("protocol")
    else:
        chunk.sha256, chunk.guest_reported = (
            read("sha256"),
            boolean(read("guest-reported")),
        )
        if (
            chunk.size > 8388608
            or not re.fullmatch("[0-9a-f]{64}", chunk.sha256)
            or not chunk.guest_reported
        ):
            _fail("protocol")
    if (
        chunk.offset != offset
        or chunk.next_offset != offset + len(data)
        or chunk.next_offset > chunk.size
        or chunk.eof != (chunk.next_offset == chunk.size)
        or len(data) > limit
        or (not chunk.eof and not data)
    ):
        _fail("protocol")
    return chunk
