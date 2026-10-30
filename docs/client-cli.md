# Rust client and Hudson CLI

The reusable [Rust client](../crates/sandbox-client/src/lib.rs) and the standalone
[Hudson CLI](https://github.com/hudson-frontier/hudson-cli) cover the 14 implemented
Project API operations. Use `hudson sandbox` for terminal workflows. They use HTTPS
and have no database, supervisor, or Firecracker authority.
[Python/TypeScript clients](language-clients.md) cover the same operations with
shared conformance. SDK package publication and supported-host release gates
remain unfinished; client availability does not establish deployment readiness.

## Install and configure

Install the published Hudson CLI using its
[installation guide](https://github.com/hudson-frontier/hudson-cli#installation),
then inspect the sandbox commands:

```sh
hudson sandbox --help
```

Private-file loading supports Unix (tested on macOS and Linux); other platforms
fail closed. Use WSL on Windows for sandbox API commands. Client use does not
require a Hudsonbox source checkout, PostgreSQL, protoc, or local VM access.

An operator first provisions a project using [offline project provisioning](api-server.md#offline-project-provisioning). Retain the resulting JSON credential file unchanged. In a trusted directory owned by your user with mode `0700`, create a mode `0600` client configuration:

```json
{
  "version": 1,
  "endpoint": "https://sandbox.example.com:8443",
  "credential_file": "project.json",
  "ca_file": "ca.pem",
  "request_timeout_seconds": 30
}
```

`credential_file` and optional `ca_file` paths resolve relative to this configuration. Configuration, credential and explicit CA files must be private regular files owned by the effective user, at most 64 KiB. Symlinks, devices, FIFOs and group/other access are rejected. Protect the containing directories too; the client checks each opened file, not every parent directory. An explicit CA bundle replaces system roots. Omit `ca_file` to use normal platform trust. Hostname/certificate verification always applies.

The endpoint must be an HTTPS origin without user information, query, fragment or a non-root path. Redirects, ambient proxy variables and automatic transport retries are disabled. There is no token flag or insecure TLS option. Set a trusted `HUDSONBOX_CONFIG` path, or pass `--config /absolute/path/client.json`. Agent integrations should fix this path in a trusted wrapper outside model tool arguments.

```sh
export HUDSONBOX_CONFIG=/absolute/private/path/client.json
hudson sandbox --json list --limit 20
```

## Stable mutations and operation handles

Every create, execute, destroy, cancel and upload requires either `--key` or `--key-file`. Generate one key **before** the first attempt and retain it with the exact request. The `key` command generates a random UUIDv4, writes a private file and refuses to overwrite an existing path. It does not call the API.

```sh
hudson sandbox key --to create.key
hudson sandbox --json create --request create.json --key-file create.key
```

Example `create.json` (replace the digest with an image allowlisted by your operator):

```json
{
  "image_digest": "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
  "resources": { "vcpu": 1, "memory_mib": 512, "disk_mib": 1024 }
}
```

Admission returns the original sandbox/operation handles and current status. It does not wait for readiness. Poll that operation explicitly:

```sh
hudson sandbox --json wait op_REPLACE_WITH_RETURNED_ID --seconds 60
hudson sandbox --json get sbx_REPLACE_WITH_RETURNED_ID
```

Execute uses a saved JSON request too, so a retry preserves the absolute command deadline and argument/environment values. Create this file once; do not regenerate its deadline during a retry. The API requires a future deadline within its admission window for a new command. Exact duplicate retries follow the [admission contract](api-contract.md#retries-and-admission).

```sh
python3 - <<'PY'
import json, time
with open('execute.json', 'x') as request:
    json.dump({'argv': ['/bin/echo', 'hello'], 'cwd': '/',
               'deadline_unix_ms': int(time.time() * 1000) + 60000,
               'output_limit': 1048576}, request)
PY
hudson sandbox key --to execute.key
hudson sandbox --json execute sbx_REPLACE_WITH_RETURNED_ID \
  --request execute.json --key-file execute.key
```

A disconnect, timeout or malformed reply after a mutation can leave its outcome unknown. Retry using the same command, key and saved payload; never generate a new key as recovery. There are no automatic mutation retries. `wait` polls the same operation and never cancels it. The client constructs status routes from operation IDs and never follows a server-provided `status_url` to another origin.

| Command | Behavior |
| --- | --- |
| `list`, `operations` | One page; optional `--limit` and `--cursor`; operations also accepts `--sandbox-id` |
| `get ID`, `operation ID` | Read current sandbox or operation state |
| `wait ID --seconds N` | Bounded polling, returning terminal/unknown state or wait timeout |
| `destroy ID --key-file KEY` | Admit lifecycle cleanup; completion requires server-side evidence |
| `cancel OP_ID --key-file KEY` | Admit a distinct cancellation operation; inspect both handles |
| `upload ID PATH --from FILE --key-file KEY` | Admit bytes plus length/SHA-256; optional `--mode 0644` or `0755` |
| `download ID PATH --to FILE` | Capture once and verify before creating a private local file |
| `output OP_ID stdout --to FILE` | One binary range; supports `stderr`, `--offset` and `--limit` |
| `stream OP_ID --cursor CURSOR` | NDJSON events with base64 bytes; reconnect only the same operation |

Unknown or unsupported commands, including pause/resume, are not exposed. Shell completion and interactive workflows remain future work.

An execute request file may include `expected_generation` from an authorized `get` response. The CLI preserves that precondition in its JSON request and reports `sandbox_generation_mismatch` on stale new admission. Keep the saved key and request unchanged while recovering an admitted operation; see the [generation contract](api-contract.md#execution-generation-precondition).

## Results, bounds and exit status

Human output distinguishes admission, desired/observed state and command outcome. `--json` writes one JSON object to stdout; diagnostics are JSON on stderr. Human strings from the server are escaped to prevent terminal control sequences. Problem diagnostics include only HTTP status and known static API codes, never backend titles, unknown code text, request bodies, credentials or URLs. An authorized canonical `operation_id` from an expired/conflicting response is retained in diagnostics so lost-response recovery can identify the original work. SDK callers can inspect the bounded raw code explicitly, including additive codes.

| Exit code | Meaning |
| --- | --- |
| 0 | Requested read/admission/transfer completed, or `wait` observed success; admission is not execution success |
| 1 | Local configuration, request, file, response framing or integrity error |
| 2 | CLI argument parsing error (Clap diagnostic on stderr, including in JSON mode) |
| 3 | `wait` observed failure without a nonzero command exit |
| 4 | `wait` observed cancellation |
| 5 | `wait` observed an unknown outcome |
| 6 | Wait/stream deadline or reconnect budget exhausted; work was not cancelled |
| 7 | Transport failure; a sent mutation may have been admitted |
| 8 | HTTP/API rejection; structured diagnostics retain status and recognized problem code |
| 9 | Stream emitted an explicit gap/error |
| 10 | `wait` observed a nonzero command exit; exact exit code remains in `result.exit_code` |

`operation` returns a successful read even if the operation failed; `wait` applies the outcome exit codes. A signal or deadline failure is distinct from a numeric command exit in the returned result. Do not infer success from a transport exit code without inspecting the command used.

Connection establishment is bounded to five seconds, response inactivity to 20 seconds, and ordinary requests to the configured 1–120 seconds (default 30). Stream requests have a 100-second total deadline starting before response headers; consuming already-buffered events does not extend it. JSON request/response limits are 64 KiB/2 MiB; error bodies are capped at 64 KiB. Uploads are capped at 8 MiB. Range reads are capped at 32 KiB and validate offsets, length, EOF, size and provenance headers. Output EOF can still mean truncated output; it does not imply command success. Existing local output paths are never overwritten.

Rust integrations can call `Client::get_operation_bounded(GetOperation { operation_id }, max_response_bytes)` to lower the operation-read limit to 1 byte–2 MiB. The SDK bounds accumulated response bytes before JSON decoding, including whitespace and additive fields that typed models ignore. Oversized success bodies return `Error::Protocol`; oversized error bodies retain their HTTP status without parsed problem fields. Invalid limits fail before network IO. This method performs one read and never retries, cancels or resubmits the operation. The generated `get_operation` method retains its 2 MiB limit.

A download uses one immutable capture, bounded to 60 seconds of chunk reads, and verifies size/SHA-256 before publishing. Failure removes its temporary file; it never silently recaptures a changed file. Release is best effort with a two-second timeout; `release_confirmed: false` does not invalidate verified bytes, and the server capture still expires. Cancelling the client task can leave the bounded remote capture until expiry. Filesystem crash durability and cleanup after an uncatchable process kill are not claimed.

`stream` always writes flushed NDJSON containing `event`, `data`, and a `cursor` for output/end events. Output `data_base64` preserves arbitrary bytes; it is not printed as raw terminal content. Save the cursor only after consuming the corresponding event. Frames are capped at 64 KiB, decoded chunks at 32 KiB, and transport chunks at 256 KiB. The decoder accepts UTF-8, LF/CR/CRLF, comments, multiline data and a leading BOM; this is the API profile, not a general browser EventSource implementation. Malformed/unknown event kinds fail explicitly.

The CLI resumes after normal EOF or an interrupted body using the last fully emitted cursor, up to `--reconnects` (default 3, maximum 20) within `--seconds` (default 120, maximum one day). A connection/admission error, invalid frame, explicit gap or explicit stream error is returned directly. The library returns events/EOF/errors and lets its caller choose reconnect policy. Incomplete frames never advance a cursor. An `end` event means output capture ended; inspect the operation for process outcome. No stream reconnect submits or cancels a command.

## Rust integration and validation

Use `sandbox_client::Client::from_config`, generated `models` and `requests`, and the generated operation methods (for example `create_sandbox`, `execute_command`, `get_operation`, `stream_output`). Helpers `wait`, `upload` and `download` add the semantics above. `new_idempotency_key` generates a key but never submits a request or persists it implicitly. `Client`, requests, errors and streaming/binary wrappers redact debugging; JSON payload models remain data intended for explicit inspection. The runtime dependency graph does not include internal protocol, storage or supervisor crates.

[OpenAPI generation](openapi.md) emits both client models and request methods. Model output is identical to the API's shared wire models, preserving command defaults/order and omission/null behavior. The finite generator rejects unsupported request media, parameter locations and ambiguous success shapes. Manual code owns trust, transport bounds, waits, stream decoding and file verification.

[Transport tests](../crates/sandbox-client/tests/transport.rs) exercise real local TLS, credential file restrictions, redirects, lost replies, timeouts, binary framing, partial SSE frames, capture expiry and digest corruption. [CLI tests](../crates/sandbox-cli/tests/cli.rs) run a private test client through TLS/API/PostgreSQL across all 14 routes, verify retry identity, preserve binary bytes and distinguish command outcomes. Their guest/file/output adapters are synthetic. These tests do not validate hostile workload isolation, installer behavior or supported production hosts. The [language client guide](language-clients.md) describes the shared three-language conformance corpus and additional API integration.

### Backend development and cross-repository checks

The unpublished `sandbox-cli` package retains `sandbox-api-test-client` as a
backend test fixture. It is excluded from the native source installer and does
not provide the public `hudson` or `hudsonbox` commands. Build it only when
debugging the API or its integration tests:

```sh
cargo build --locked -p sandbox-cli --bin sandbox-api-test-client
./target/debug/sandbox-api-test-client --help
```

With the [development dependencies](local-development.md) available, run
the same API integration suite against a trusted, locally built Hudson CLI:

```sh
HUDSON_TEST_OPERATOR_CLI=/absolute/path/to/hudson \
  cargo test --locked -p sandbox-cli --test cli
```

The override invokes that executable with the `sandbox` subcommand. The default
tests use the private fixture; they download no executable. This checks the
standalone command adapter against the real TLS/API/PostgreSQL boundary without
requiring a KVM host.

## Rust client identity for recovery

`Client::identity()` returns a borrowed `ClientIdentity` with `origin: &str` and
`project_id: &str`. The origin is the validated HTTPS URL normalized by the client
(including a trailing slash and omission of the default port). The project is the
credential file's declared project; server authorization remains authoritative.
Neither field exposes a token, credential path or TLS material. The identity is
fixed when the client loads its configuration and is preserved by clones.

Harnesses own canonical profile hashing and durable binding. Compare the loaded
client identity with that binding before any recovery lookup or request. Reloading
changed origin/project configuration produces a different identity. Rotating the
token within the same origin/project does not change these two fields; this accessor
does not establish credential or CA continuity.
