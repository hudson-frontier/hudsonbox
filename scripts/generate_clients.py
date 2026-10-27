"""Finite Python/TypeScript model and request profile, owned by OpenAPI."""

import json
from pathlib import Path
from generate_api import nullable, rust_type, snake

ROOT = Path(__file__).resolve().parents[1]


def profile(schema, schemas):
    # Reuse the existing generator's refusal of unsupported compositions.
    rust_type(schema, schemas)
    if schema.get("x-rust-json"):
        return {"kind": "json"}
    inner = nullable(schema)
    if inner is not None:
        return {"kind": "nullable", "inner": profile(inner, schemas)}
    if "$ref" in schema:
        name = schema["$ref"].removeprefix("#/components/schemas/")
        return (
            {"kind": "ref", "name": name}
            if schemas[name].get("x-rust-model")
            else profile(schemas[name], schemas)
        )
    kind = schema["type"]
    if kind == "integer":
        width = schema.get("x-rust-integer", "i64")
        bits = int(width[1:])
        return {
            "kind": kind,
            "wide": bits == 64,
            "min": str(0 if width[0] == "u" else -(2 ** (bits - 1))),
            "max": str(2**bits - 1 if width[0] == "u" else 2 ** (bits - 1) - 1),
        }
    if kind == "array":
        return {"kind": kind, "inner": profile(schema["items"], schemas)}
    if kind == "object":
        if schema.get("additionalProperties") is True:
            return {"kind": "json"}
        return {
            "kind": "map",
            "inner": profile(schema["additionalProperties"], schemas),
        }
    return {"kind": kind}


def profiles(doc):
    schemas = doc["components"]["schemas"]
    result = {}
    for name, schema in schemas.items():
        if not schema.get("x-rust-model"):
            continue
        fields = {}
        for field, prop in schema["properties"].items():
            entry = {
                "type": profile(prop, schemas),
                "required": field in schema.get("required", []),
                "nullable": nullable(prop) is not None,
                "omit_false": prop.get("x-rust-omit-false", False),
            }
            if prop.get("x-rust-default"):
                entry["default"] = prop["default"]
            if prop.get("x-rust-nonnull-option"):
                entry["reject_null"] = True
            fields[field] = entry
        result[name] = {
            "fields": fields,
            "closed": schema.get("additionalProperties") is False,
        }
    return result


def py_type(p):
    k = p["kind"]
    if k == "ref":
        return p["name"]
    if k == "nullable":
        return py_type(p["inner"]) + " | None"
    if k == "array":
        return f"list[{py_type(p['inner'])}]"
    if k == "map":
        return f"dict[str, {py_type(p['inner'])}]"
    return {"string": "str", "integer": "int", "boolean": "bool", "json": "Any"}[k]


def ts_type(p):
    k = p["kind"]
    if k == "ref":
        return p["name"]
    if k == "nullable":
        return ts_type(p["inner"]) + " | null"
    if k == "array":
        return f"Array<{ts_type(p['inner'])}>"
    if k == "map":
        return f"Record<string, {ts_type(p['inner'])}>"
    return {
        "string": "string",
        "integer": "bigint" if p.get("wide") else "number",
        "boolean": "boolean",
        "json": "JsonValue",
    }[k]


def operations(doc):
    def resolve(value):
        if "$ref" not in value:
            return value
        node = doc
        for key in value["$ref"].removeprefix("#/").split("/"):
            node = node[key]
        return node

    for path, methods in doc["paths"].items():
        for method, op in methods.items():
            params = [resolve(p) for p in op.get("parameters", [])]
            body = op.get("requestBody", {}).get("content", {})
            body_type = (
                next(iter(body.values()))["schema"]["$ref"].split("/")[-1]
                if "application/json" in body
                else "bytes"
                if body
                else None
            )
            status, response = next(
                (int(code), resolve(r))
                for code, r in op["responses"].items()
                if code.startswith("2")
            )
            content = response.get("content", {})
            result = (
                next(iter(content.values()))["schema"]["$ref"].split("/")[-1]
                if "application/json" in content
                else "binary"
                if "application/octet-stream" in content
                else "stream"
                if content
                else "empty"
            )
            yield dict(
                name=op["operationId"],
                method=method.upper(),
                path=path,
                params=params,
                body_type=body_type,
                status=status,
                result=result,
                prefix="output"
                if "X-Output-Offset" in response.get("headers", {})
                else "file",
            )


def render_python(doc, ps):
    lines = [
        "# Generated from api/openapi.json. Do not edit.",
        "from __future__ import annotations",
        "from dataclasses import dataclass, field",
        "from typing import Any",
        "from ._wire import Model, _Omitted, _OMITTED",
        "",
    ]
    for name, model in ps.items():
        lines += ["@dataclass(kw_only=True, repr=False)", f"class {name}(Model):"]
        for field_name, field_data in model["fields"].items():
            ty = py_type(field_data["type"])
            default = ""
            if "default" in field_data:
                value = field_data["default"]
                default = (
                    " = field(default_factory=dict)"
                    if value == {}
                    else " = " + repr(value)
                )
            elif not field_data["required"]:
                if field_data.get("reject_null"):
                    ty += " | _Omitted"
                    default = " = _OMITTED"
                elif not field_data["nullable"]:
                    ty += " | None"
                    default = " = None"
                else:
                    default = " = None"
            lines.append(f"    {field_name}: {ty}{default}")
        if not model["fields"]:
            lines.append("    pass")
        lines.append("")
    lines.append("MODELS = {" + ", ".join(f"{name!r}: {name}" for name in ps) + "}")
    model_text = "\n".join(lines) + "\n"
    lines = [
        "# Generated from api/openapi.json. Do not edit.",
        "from __future__ import annotations",
        "from typing import TYPE_CHECKING",
        "from .models import *  # noqa: F403",
        "from ._transport import Transport",
        "if TYPE_CHECKING:",
        "    from ._stream import EventStream",
        "    from ._wire import RangeChunk",
        "",
        "class Requests(Transport):",
    ]
    for op in operations(doc):
        args, paths, queries, headers = [], [], [], []
        for p in op["params"]:
            name = snake(p["name"])
            ty = (
                "int"
                if profile(p["schema"], doc["components"]["schemas"])["kind"]
                == "integer"
                else "str"
            )
            args.append(
                f"{name}: {ty}" + ("" if p.get("required") else " | None = None")
            )
            pair = f"({p['name']!r}, {name})"
            if p["in"] == "query":
                queries.append(pair)
            if p["in"] == "header":
                headers.append(pair)
        if op["body_type"]:
            args.append(
                "body: " + ("bytes" if op["body_type"] == "bytes" else op["body_type"])
            )
        for part in op["path"].lstrip("/").split("/"):
            paths.append(part[1:-1] if part.startswith("{") else repr(part))
        ret = {"empty": "None", "binary": "RangeChunk", "stream": "EventStream"}.get(
            op["result"], op["result"]
        )
        lines += [
            f"    async def {snake(op['name'])}(self, *, {', '.join(args)}) -> {ret}:",
            f"        return await self._request({op['method']!r}, [{', '.join(paths)}],",
            f"            [{', '.join(queries)}], [{', '.join(headers)}],",
            f"            {op['body_type']!r}, {'body' if op['body_type'] else 'None'}, {op['status']}, {op['result']!r},",
            f"            {op['prefix']!r}, {'offset or 0, limit if limit is not None else 32768' if op['result'] == 'binary' else '0, 32768'})",
            "",
        ]
    return model_text, "\n".join(lines)


def render_typescript(doc, ps):
    lines = [
        "// Generated from api/openapi.json. Do not edit.",
        'import type { JsonValue } from "./wire.js";',
        "export type Input<T> = T extends bigint ? bigint | number : T extends Array<infer U> ? Input<U>[] : T extends object ? { [K in keyof T]: Input<T[K]> } : T;",
        "",
    ]
    for name, model in ps.items():
        lines.append(f"export interface {name} {{")
        for field_name, data in model["fields"].items():
            lines.append(
                f"  {field_name}{'' if data['required'] else '?'}: {ts_type(data['type'])};"
            )
        lines.append("}")
    model_text = "\n".join(lines) + "\n"
    lines = [
        "// Generated from api/openapi.json. Do not edit.",
        'import { Transport, type RequestOptions } from "./transport.js";',
        'import type { RangeChunk } from "./wire.js";',
        'import type { EventStream } from "./stream.js";',
        'import type * as M from "./models.js";',
        "export class Requests extends Transport {",
    ]
    for op in operations(doc):
        args, paths, queries, headers = [], [], [], []
        for p in op["params"]:
            name = snake(p["name"])
            ty = (
                "bigint | number"
                if profile(p["schema"], doc["components"]["schemas"])["kind"]
                == "integer"
                else "string"
            )
            args.append(f"{name}{'' if p.get('required') else '?'}: {ty}")
            pair = f"[{json.dumps(p['name'])}, args.{name}]"
            if p["in"] == "query":
                queries.append(pair)
            if p["in"] == "header":
                headers.append(pair)
        if op["body_type"]:
            args.append(
                "body: "
                + (
                    "Uint8Array"
                    if op["body_type"] == "bytes"
                    else f"M.Input<M.{op['body_type']}>"
                )
            )
        for part in op["path"].lstrip("/").split("/"):
            paths.append(
                "args." + part[1:-1] if part.startswith("{") else json.dumps(part)
            )
        ret = {"empty": "void", "binary": "RangeChunk", "stream": "EventStream"}.get(
            op["result"], "M." + op["result"]
        )
        lines += [
            f"  async {op['name']}(args: {{ {'; '.join(args)} }}, options: RequestOptions = {{}}): Promise<{ret}> {{",
            f"    return await this.request({json.dumps(op['method'])}, [{', '.join(paths)}],",
            f"      [{', '.join(queries)}], [{', '.join(headers)}],",
            f"      {json.dumps(op['body_type'])}, {'args.body' if op['body_type'] else 'undefined'}, {op['status']}, {json.dumps(op['result'])},",
            f"      {json.dumps(op['prefix'])}, {'args.offset ?? 0n, args.limit ?? 32768n' if op['result'] == 'binary' else '0n, 32768n'}, options) as {ret};",
            "  }",
        ]
    lines.append("}")
    return model_text, "\n".join(lines) + "\n"


def outputs(doc):
    ps = profiles(doc)
    py_models, py_requests = render_python(doc, ps)
    ts_models, ts_requests = render_typescript(doc, ps)
    codes = doc["components"]["schemas"]["ProblemBody"]["properties"]["code"]["enum"]
    metadata = {"models": ps, "codes": codes, "version": doc["info"]["version"]}
    py = ROOT / "sdk/python/src/hudsonbox"
    ts = ROOT / "sdk/typescript/src"
    return {
        py / "models.py": py_models,
        py / "_requests.py": py_requests,
        py / "_schema.py": "# Generated from api/openapi.json. Do not edit.\nSCHEMA = "
        + repr(metadata)
        + "\n",
        ts / "models.ts": ts_models,
        ts / "requests.ts": ts_requests,
        ts
        / "schema.ts": '// Generated from api/openapi.json. Do not edit.\nimport type { Schema } from "./wire.js";\nexport const schema: Schema = '
        + json.dumps(metadata, indent=2)
        + ";\n",
    }
