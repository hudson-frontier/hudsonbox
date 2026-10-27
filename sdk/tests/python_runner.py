"""Invoked by the Rust TLS harness; uses only the public Python client API."""

import asyncio
import base64
import dataclasses
import json
import re
import sys
from hudsonbox import Client, ClientError, RangeChunk, EventStream, models


def normalize(value):
    if isinstance(value, RangeChunk):
        result = dataclasses.asdict(value)
        result["data_base64"] = base64.b64encode(result.pop("data")).decode()
        return result
    return value.to_wire() if hasattr(value, "to_wire") else value


async def run():
    case = json.load(open(sys.argv[2]))
    args = case["args"].copy()
    if "body_base64" in args:
        args["body"] = base64.b64decode(args.pop("body_base64"))
    try:
        if model := case.get("python_body_model"):
            args["body"] = models.MODELS[model](**args["body"])
        async with Client(sys.argv[1]) as client:
            action = re.sub(r"(?<!^)(?=[A-Z])", "_", case["action"]).lower()
            result = await getattr(client, action)(**args)
            if isinstance(result, EventStream):
                async with result:
                    result = [event.to_wire() async for event in result]
            return {"ok": normalize(result)}
    except ClientError as e:
        assert "private-backend-text" not in repr(e) + str(e)
        result = {"error": e.kind}
        if e.kind == "http":
            result.update(status=e.status, code=e.code, operation_id=e.operation_id)
        return result


print(json.dumps(asyncio.run(run()), ensure_ascii=False, allow_nan=False))
