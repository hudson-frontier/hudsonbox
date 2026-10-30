# Owner routing review fixes: local validation

Scope: issue #193, existing PR #207. Validated source head `382cb744c4c15c989c5bc10d45fbff9f6bf81740`, fetched after the parent rebase. The evidence-only follow-up changes no executable source.

Pool admission failures skip health attempts without extending transport backoff or changing observations. A claimed command without a dispatch intent durably records owner unavailability; later preparation rejects it even if maintenance restored running state. Existing dispatch intents retain inspection/cancellation and unknown outcomes. Review of the small follow-up patch found no additional correction necessary.

## Local validation

Native macOS aarch64, pinned Rust 1.92.0, Python 3.14.6, Node 24.22.1. Read-only prerequisite diagnosis passed before setup. Installed the pinned environments with `make api-setup sdk-setup`. All Rust commands used `CARGO_BUILD_JOBS=2`, dedicated `/tmp/pr207-validation-target`, and debug information disabled. Database-backed tests used SQLx temporary databases on the existing PostgreSQL at localhost:55432; no shared schema reset or new stack was used.

Passed:

- `cargo test -p sandbox-controller --test owner_routing -- --test-threads=2`: all 8 tests, including rapid polling recovery at the same endpoint/pin, maintenance recovery before command retry with zero dispatches, and authentication loss after a lost acknowledgement with exactly one execution after reconciliation.
- `cargo test -p sandbox-controller --test client_recovery`: 1 test, SDK process restart preserving real admission and explicit cleanup.
- `cargo test -p sandbox-store --test execute`: 87 tests, including dispatch fences, retained intents, lock-wait expiry and unknown-outcome recovery.
- `cargo test -p sandbox-supervisor --lib routing::`: 2 tests passed, 4 unrelated library tests filtered out; host identity/pin validation and bounded pool invalidation.
- `cargo test -p sandbox-api --test openapi`: 1 database/router conformance test covering all 14 implemented operations.
- `cargo clippy -p sandbox-controller --lib --test owner_routing --test client_recovery -- -D warnings`.
- `cargo clippy -p sandbox-api --lib --test openapi -- -D warnings`.
- `make api`: OpenAPI validation, generated model/request drift, and 15 contract checker regressions.
- `make sdk-check`: Python lint/format, 54 Python tests, 7 TypeScript tests, and unpublished wheel/npm package installation/import/type smoke checks.
- Rust formatting, documentation checker (64 Markdown files), generated API drift and whitespace checks.

## CI diagnosis and limits

Inspected failed run 41132690583: client/contract job 124327439081 and Rust integration job 124327439161 failed at the same OpenAPI route scanner assertion. The owner client helper `.route(host)` was mistaken for an Axum registration. The validated head renames it to `owner_route`, preserving the strict checker. Current-head client/contract and format/Clippy jobs subsequently passed; hosted integration remains a separate Linux gate.

`cargo test -p sandbox-api --lib` was attempted and failed to compile on macOS with E0425 at unchanged `provision.rs:295`: `rustix::fs::mkfifoat` is unavailable. No API library unit tests ran locally; this is not a pass. Full workspace/MinIO/MCP and Linux/KVM tests were not run locally in this focused validation. No real VM, production, merge or deployment evidence is claimed.
