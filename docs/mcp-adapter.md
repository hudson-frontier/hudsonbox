# Optional local MCP adapter

The [separate Python package](../sdk/mcp/README.md) supplies stdio tools over the existing project-authenticated HTTPS SDK. A trusted operator launches it for a local MCP host with one private SDK configuration file. Tool calls cannot select an origin, project credential, CA, object-store bucket, local destination or supervisor endpoint. Files are workspace-relative paths in the selected sandbox. There is no network MCP listener or embedded harness.

This deliberately revisits [ADR 0002](decisions/0002-no-mcp-server-initially.md) now that the HTTP API and conformance-tested clients exist, for [issue #113](https://github.com/hudson-frontier/hudsonbox/issues/113). Runtime networking, images, isolation and hardware qualification remain separate [roadmap gates](roadmap.md).

## Protocol and versioning

The maintained [official Python MCP SDK v2.3.0](https://github.com/modelcontextprotocol/python-sdk/releases/tag/v2.3.0) handles JSON-RPC, initialization, cancellation and discovery. The chosen interoperability baseline is [MCP 2025-11-25 stdio](https://modelcontextprotocol.io/specification/2025-11-25/basic/transports) and its [tools contract](https://modelcontextprotocol.io/specification/2025-11-25/server/tools), tested with the official SDK client in explicit legacy mode. The dependency also negotiates its newer supported protocol; the adapter exposes the same tools without modern-only subscriptions or server-initiated actions.

Every tool begins `hudson_v1_`; every result has `schema_version: 1`. Authoritative closed input schemas live in [schemas.py](../sdk/mcp/src/hudsonbox_mcp/schemas.py) and are returned by tools/list. Unknown fields are rejected before HTTP. Breaking argument/result changes require a new tool name/version and updated fixtures; additive changes preserve old calls. Dependencies are pinned in [mcp-requirements.txt](../scripts/mcp-requirements.txt). Package version is the repository's [product version](versioning.md); it is not published to a package index.

## Tool mapping

| Tool suffix | Existing SDK operation | Behavior |
| --- | --- | --- |
| `create` | create_sandbox | Saved key; returns sandbox and create operation IDs. |
| `sandbox_get`, `sandbox_list` | get_sandbox, list_sandboxes | Observed readiness separate from desired state; bounded pages. |
| `operation_get`, `operation_list` | get_operation, list_operations | Existing handles, state, response expiry and pagination. |
| `execute` | execute_command | Saved key, argv/payload and absolute deadline; execute handle. |
| `wait` | wait | Bounded polling of the same operation; no lifecycle side effects. |
| `cancel`, `destroy` | cancel_command, destroy_sandbox | Explicit keys; separate operations, not cleanup promises. |
| `output_read` | read_output | Base64 stdout/stderr; offset, EOF, size/seen and truncation. |
| `output_stream` | stream_output | Bounded SSE batch; validated resumable cursor and gap/error. |
| `file_upload` | upload | Entire small file, canonical base64, path/digest/mode and key. |
| `file_capture`, `file_read`, `file_release` | capture_file, read_captured_file, release_file_capture | Immutable expiring capture, ranges and explicit release; no client filesystem writes. |

## Recovery and trust boundary

Persist a logical mutation key and exact payload **before** create/execute/upload/cancel/destroy. Persist admission IDs before further work. The adapter stores no chat journal and sends each HTTP request once. Uncertain transport/protocol replies have `mutation_outcome: uncertain`; retain the original key/payload and reconcile existing handles. A supplied HTTP operation ID is preserved. Identical retry follows HTTP idempotency; changed payload is a conflict. Capture is a bounded read snapshot rather than a retained mutation; a lost handle expires server-side.

Successful tool transport is not successful execution. Returned operations may be failed, cancelled, unknown or have `response_expired: true`; preserve and classify those fields. Wait timeout and MCP cancellation affect local polling/request processing only; neither sends command cancellation or destruction. Disconnect may leave admitted work running. Destroy must be observed through its operation and verified cleanup evidence. Closing this process is not teardown.

Results carry `untrusted_data: true`. Guest bytes are base64 with provenance metadata, never prompts, resource links or automatically executed instructions. Diagnostics contain no configuration, bearer credential, raw exception, HTTP problem title or arbitrary error code. Sanitized SDK error fields preserve denial, revocation, expiry, missing output, integrity failure and uncertainty. The local MCP host must be trusted: it can request all operations allowed by the selected project credential.

The `execute` body accepts optional `expected_generation`, a positive integer at most `9007199254740991` for the adapter's safe JSON numeric profile. Save the inspected generation with the original key/payload. Stale new admission returns `sandbox_generation_mismatch`; it is never replaced by a retry against another generation. The full HTTP SDKs also support the API's signed 64-bit generation range. See the [generation contract](api-contract.md#execution-generation-precondition).

## Bounds

One tool call executes at a time; concurrent calls return `busy` without HTTP. At most eight JSON-RPC IDs may be pending; duplicates are rejected. Malformed, oversized, duplicate-key or non-UTF-8 frames close the connection without echoing input. A connection failure may leave a sent mutation admitted; retain its identity even if no error arrives.

| Boundary | Limit |
| --- | --- |
| Incoming JSON-RPC line | 128 KiB including newline, before parsing |
| Encoded tool arguments | 64 KiB |
| Structured result | 256 KiB; oversize yields result_too_large |
| Encoded outgoing JSON-RPC line | 1 MiB |
| Page | Default 20, maximum 50; next_cursor retained |
| Binary upload/read range | 32 KiB; larger uploads use ordinary SDK |
| Capture | Existing API 8 MiB, immutable digest/expiry |
| SSE batch | Default one, maximum four events and five seconds |
| Wait | Default ten, maximum sixty seconds |
| Command | Existing argv/env byte limits; output_limit at most 10 MiB |

SDK validation covers closed SSE events and byte integrity. Batch timeout retains only the last complete validated cursor. Gap/error or HTTP/transport failure returns an explicit error with validated events/cursor already received. Incomplete frames never advance cursors. No background subscriber or automatic reconnect is installed.

## Validation and limits

`make mcp-check` tests schemas, SDK mappings, request/path/byte boundaries, sanitized access/revocation/expiry errors, lost replies, timeouts/cancellation, range/truncation and SSE cursor/gap behavior. It builds and installs the optional wheel, then runs the official MCP client against a real stdio subprocess and private HTTPS fixture. Ordinary SDK conformance remains independent. Fixtures do not qualify real Firecracker cleanup or isolation; runtime smoke needs a controlled host and the local quickstart.
