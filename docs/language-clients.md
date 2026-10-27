# Python and TypeScript clients

Status: development clients cover the 14 implemented Project API operations. Models and request methods are generated from the same [OpenAPI document](../api/openapi.json) as the [Rust client and CLI](client-cli.md). Version `0.0.0` matches the API/workspace. Python wheels and npm tarballs are built and installed in tests; no package has been published. Supported-host installation, signed distribution and hostile-workload release gates remain unfinished.

## Build and configure

Use Python 3.11 or newer, Node 24.9.0 or newer, and a Unix account. These are server-side clients: do not embed project credentials in a browser bundle. The clients require an existing HTTPS API installation and a provisioned project credential. Use the same [private configuration file](client-cli.md#install-and-configure) as the Rust client. Pass its trusted path directly to the constructor; credentials and endpoint selection belong outside workload/model arguments.

From the repository root:

```sh
make sdk-setup
make sdk-check
# Optional installation into your application's own Python environment:
python -m pip install ./sdk/python
# Optional npm tarball for installation into your application's Node project:
cd sdk/typescript
npm pack --ignore-scripts
```

`make sdk-check` builds TypeScript, runs language tests, builds the Python wheel and npm tarball, installs them in temporary directories and checks imports/types. Its Python smoke test uses the pinned runtime dependencies already installed by `sdk-setup`; it does not publish or test a package registry. Use an ordinary Node TypeScript project with Node type definitions when consuming declarations. The npm package remains `private: true` to prevent accidental publication.

Configuration, credential and explicit CA files must be private regular files owned by the current effective user; symlinks and group/other access fail. Explicit CA files replace platform roots. TLS verifies the hostname and certificate. Redirects, ambient proxy configuration and implicit retries are disabled. Node uses its own HTTPS agent with certificate verification enabled, including when the process has opted into an environment proxy or disabled verification for other clients. Python uses an explicit [HTTPX async transport](https://www.python-httpx.org/advanced/transports/).

## Submit once and retain recovery information

Persist a key and exact payload before calling a mutation. The examples assume the application has already saved a logical execute request as `saved-execute.json`, with `sandbox_id`, `idempotency_key` and `body` fields. Keep its original absolute `deadline_unix_ms` on every retry; do not recompute it. Request-file storage and permissions are owned by the calling application.

Python uses async snake_case methods and generated dataclasses:

```python
import asyncio
import json
from hudsonbox import Client, models

async def main():
    with open("saved-execute.json") as source:
        saved = json.load(source)
    async with Client("/absolute/private/path/client.json") as client:
        admitted = await client.execute_command(
            sandbox_id=saved["sandbox_id"],
            idempotency_key=saved["idempotency_key"],
            body=models.CommandInput(**saved["body"]),
        )
        print(admitted.operation_id)  # Admission, not completion.

asyncio.run(main())
```

TypeScript uses camelCase methods with snake_case wire fields. Input integer fields accept safe JavaScript integers or `bigint`:

```typescript
import { readFile } from "node:fs/promises";
import { Client, type Input, type CommandInput } from "@hudson-frontier/hudsonbox-client";

// This saved request uses safe numeric values (including a millisecond deadline).
const saved: { sandbox_id: string; idempotency_key: string; body: Input<CommandInput> } =
  JSON.parse(await readFile("saved-execute.json", "utf8"));
const client = new Client("/absolute/private/path/client.json");
try {
  const admitted = await client.executeCommand(saved);
  console.log(admitted.operation_id);
} finally {
  client.close();
}
```

`new_idempotency_key()` / `newIdempotencyKey()` generates a UUID key, without persisting or submitting it. `wait(operation_id, seconds)` / `wait(operationId, seconds)` polls that exact operation. A timeout leaves the server operation running. A terminal `unknown` result is returned as unknown; it never triggers another execute. The generated cancel method admits a separate cancel operation using its own explicit key.

`ClientError.kind` distinguishes configuration, request, transport, HTTP, protocol, wait timeout, file and integrity failures. HTTP errors expose `status`, a safe known `code`, and a validated original handle (`operation_id` in Python, `operationId` in TypeScript) when supplied. Bounded unknown raw codes are available explicitly as `raw_code` / `rawCode`. Error messages and normal debug inspection omit backend titles, credentials and URLs. Response objects remain workload-controlled data; handle them explicitly. A transport/protocol failure after sending a mutation does not prove non-admission. Recover with its saved key and payload.

To bind an execute request to the sandbox generation you inspected, include `expected_generation` in its saved command body: a positive Python `int`, TypeScript `bigint` (for example `1n`), or Rust `Some(1_i64)`. A new stale request returns HTTP `409` with code `sandbox_generation_mismatch`. Keep the precondition unchanged when reconciling a lost admission reply; an exact retry preserves its original operation. See the [generation contract](api-contract.md#execution-generation-precondition).

## Integer, streaming and file behavior

The [executable Python recovery example](../sdk/python/examples/README.md) saves
one private immutable execute request before submission, retains the operation
handle and reconnects through read-only status. It demonstrates explicit same-key
admission reconciliation without a harness or new API surface, including the
limits of its conservative client-configuration binding.

Python preserves integer fields as arbitrary-precision `int`, checked against the schema's wire widths. TypeScript returns every typed 64-bit integer as `bigint`, including timestamps, generation, offsets and counters; typed 32-bit values remain `number`. Unsafe numeric input integers are rejected. Decoding preserves integer JSON tokens before type conversion. Use the exported `stringify` function for SDK results; native `JSON.stringify` cannot serialize `bigint`. Untyped result/error JSON also preserves large integer tokens. Non-finite numbers, lone Unicode surrogates and duplicate JSON keys fail; TypeScript additionally rejects floating-point tokens that evaluate to unsafe integer-valued numbers rather than silently rounding them. This stricter case is outside the shared supported numeric profile.

Both clients use the [same byte bounds](client-cli.md#results-bounds-and-exit-status) as Rust: 64 KiB JSON requests, 2 MiB JSON success responses, 64 KiB problems and SSE frames, 32 KiB range chunks, and 8 MiB file transfers. Request deadlines include the response body. Streams have a 100-second total limit and a 20-second idle limit. Parsing is depth-bounded. Semantic admission limits, authorization and lifecycle state remain server-owned.

`stream_output` / `streamOutput` returns an async iterator. Only complete validated frames yield events; an unfinished frame at EOF never advances the cursor. Save the opaque cursor after processing an event, then explicitly reconnect to the same operation using `cursor` or `last_event_id`. Reconnect does not re-execute. An `end` event means output completion, not command success; `gap` and `error` are terminal stream events. Use Python `async with stream` to close on early exit, or TypeScript `for await` (which closes on `break`), `stream.close()` or async disposal. Close clients when finished.

`upload` calculates the digest and size for caller-supplied bytes and still requires an explicit mutation key. `download` captures once, verifies every range against the capture, hashes all bytes and checks the final size/digest before publishing a private destination file. It refuses overwrite, removes temporary files on ordinary failure, and never recaptures silently. Release is best effort within two seconds; `release_confirmed: false` reports uncertainty while preserving verified bytes. Cancellation or process death can leave a remote capture until its bounded expiry. Filesystem crash durability and cleanup after uncatchable process termination are not claimed.

The download destination must name a file. Python rejects directory-shaped strings such as `result/` and `result/.` before creating a remote capture; pass a file name such as `result/output.bin` instead.

TypeScript generated methods accept optional `{ signal: AbortSignal }` as their second argument. Aborting an HTTP call stops client I/O; it does not cancel the server operation. Python supports ordinary task cancellation. On either language, cancelling a sent mutation leaves its admission outcome uncertain.

## Validation boundaries

The [shared data corpus](../api/conformance/clients.json) runs through the public Rust, Python and TypeScript clients against one [controlled HTTPS harness](../crates/sandbox-client/tests/conformance.rs). It checks all 14 routes, payload defaults, exact integers, byte framing, opaque cursors, expired handles, TLS rejection, proxy bypass, redirects, lost replies without retry, bounded waits, and verified/no-clobber downloads. Corpus expectations are independent of generated code. Missing SDK tools fail the test rather than silently skipping it.

[API integration tests](../crates/sandbox-cli/tests/sdks.rs) run both language clients through HTTPS and the real API/PostgreSQL across every route, stable-key retries, conflict/cross-project responses, streaming resume and verified file transfer. Guest, file and output adapters are synthetic. These checks establish client behavior; they do not establish VM isolation, deployment readiness or production performance. `make api` checks generated drift and the Rust CI job runs package tests, shared conformance and API integration on the proposed commit.
