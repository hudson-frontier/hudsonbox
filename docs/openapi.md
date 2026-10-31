# OpenAPI and wire models

Status: the implemented Project API has a checked [OpenAPI 3.1 document](../api/openapi.json) and [generated Rust wire models](../crates/sandbox-protocol/src/api.rs). The API uses those models for JSON requests/responses and SSE payloads. The [Rust client and project CLI](client-cli.md) now use generated client models and requests for those operations. [Python/TypeScript clients](language-clients.md) use generated models and requests for the same operations; publication remains unfinished. This is interface evidence, not sandbox isolation evidence.

The specification owns exact wire shapes for 14 operations: create, execute, destroy, cancel, sandbox/operation reads and lists, retained-output reads, SSE, binary uploads, and capture/read/release downloads. It contains no planned Admin, browser-session, pause/resume or snapshot endpoints. It is a repository artifact; the service does not expose a new discovery endpoint. Replace its reserved example HTTPS origin with your installation's trusted API URL.

[API contract](api-contract.md) owns admission, authorization, idempotency, retry, retention and streaming semantics. [The OpenAPI standard](https://spec.openapis.org/oas/v3.1.0.html) defines the document format. The contract version matches the workspace [version](versioning.md); a pre-1.0 version is not a claim that the runtime is ready for production.

## Change and check the contract

Use Python 3.11 or newer, Node 24.9.0 or newer, and the pinned Rust toolchain for all checks:

```sh
make api-setup       # isolated .venv-openapi, pinned validation dependencies
make sdk-setup       # isolated Python environment and locked Node tools
make api-generate    # after editing api/openapi.json
make api            # standards validation, generation drift, checker regressions
make check          # additionally runs Rust checks and PostgreSQL/router conformance
```

`make up` supplies PostgreSQL on port 55432. Direct `cargo test` runs the conformance test too; install both validator and SDK environments first. `HUDSON_OPENAPI_PYTHON` may point to another Python environment with the pinned [validation dependencies](../scripts/api-requirements.txt). Nothing installs into system Python. The Rust CI job performs this setup and all checks before merge.

Edit the specification, regenerate, change the handler/admission implementation as needed, and update the owning semantic document in the same PR. Generated output is committed for ordinary Cargo builds; Python is needed for regeneration and contract tests, not by the runtime. CI rejects stale output. The standalone client has no runtime dependency on internal protocol or protobuf crates.

## Generation boundaries

[The generator](../scripts/generate_api.py) supports this repository's finite model and request profile: named objects, scalar strings/booleans/integers, arrays, string-keyed maps, local references and explicit nullable unions. It is not a general OpenAPI client generator. Unsupported type composition fails. Request generation emits operation methods and argument structs from each route, parameter, body media type and success response; unsupported methods, parameter locations or media and ambiguous success shapes fail. Generated requests use a manually bounded transport. `x-rust-model` selects a named wire object; the other `x-rust-*` extensions preserve integer widths, omitted/null/default behavior, existing JSON result storage, and redacted command debugging. These extensions do not change the public JSON schema.

Generation emits wire models, not authorization or complete input validation. Existing admission code still enforces numeric and aggregate byte limits, resource compatibility, workspace path rules, deadlines, quotas and lifecycle state. For example, JSON Schema's `maxLength` counts characters; `x-max-utf8-bytes` records the additional byte limit. Operation result/error objects retain documented, extensible metadata; they are not evidence that an unresolved operation succeeded. Clients must tolerate additive response fields and status/code values.

Command argument/environment order and defaults affect durable digests. [Golden normalization tests](../crates/sandbox-protocol/tests/api_wire.rs) preserve command serialization order, sorted environment keys, defaults, create/destroy null fields, strict command/destroy/cancel inputs and redacted Debug. List `next_cursor` remains present and nullable; absent optional status fields remain omitted. No digest-version change or database migration is involved.

SSE is described as `text/event-stream` with `x-sse-events` referring to generated JSON payload schemas. This extension documents frames; it does not implement a stream decoder, reconnection or retry policy. Binary endpoints declare their content type and bounded byte-position headers. File capture tokens and output cursors are opaque positions, never credentials. Command completion, output completion and allocation release remain separate facts.

## Evidence and remaining client work

[Router conformance](../crates/sandbox-api/tests/openapi.rs) exercises every declared operation against PostgreSQL. Its storage/guest adapters supply synthetic bytes. The independent [validator](../scripts/check_api.py) checks successful request shapes, response status/media types, required headers, JSON payloads and actual SSE framing; missing successful coverage of a declared operation fails. It also validates real bad-request, unauthorized, cross-project, expired-response and invalid-range responses. The malformed UTF-8 path regression first exposed native plain-text extractor errors on five routes; those routes now return uncached `400 bad_request` problems. A source inventory guard compares literal Axum route/method registrations with the specification and rejects unrecognized registration syntax. The separate [local operator probes](api-server.md#local-operator-health-probes) are outside Project OpenAPI; their source has an exact two-route inventory, so missing or added operator routes still fail validation. This guard is specific to the current registration style; it is not a Rust parser. [Checker regression tests](../scripts/test_api_contract.py) prove schema/header drift is detected. These checks complement the existing authorization, storage and runtime suites; they do not replace them.

The validator is [openapi-spec-validator](https://github.com/python-openapi/openapi-spec-validator), pinned with its transitive Python dependencies. JSON Schema 2020-12 validates actual JSON exchanges. The repository permits local references only; validation does not fetch schema URLs supplied by a contract.

The Rust client generates its [models](../crates/sandbox-client/src/models.rs) and [request layer](../crates/sandbox-client/src/requests.rs) from this specification. `make api` checks all generated outputs. The [client guide](client-cli.md) documents transport, retry/wait/stream and digest behavior with executable TLS/CLI tests. The [language generator](../scripts/generate_clients.py) emits Python/TypeScript dataclasses/interfaces, finite decoding metadata and request methods. [Shared HTTPS cases](../api/conformance/clients.json) exercise all three clients, and [API integration](../crates/sandbox-cli/tests/sdks.rs) covers both new languages against the real API/PostgreSQL. See [language client behavior and numeric limits](language-clients.md#integer-streaming-and-file-behavior). No package or installer is published by this change.
