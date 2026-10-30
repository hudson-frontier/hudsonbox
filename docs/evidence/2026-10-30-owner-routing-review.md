# Owner routing review fixes: local validation

Scope: issue #193, existing PR #207, based on `93b16324911851e0aeb7f2ae2a40a1d9c50bffdd`.

Pool admission failures now skip health attempts without extending transport backoff or changing observations. A claimed command without a dispatch intent durably records owner unavailability; later preparation rejects it even if maintenance restored running state. Existing dispatch intents retain inspection/cancellation and unknown outcomes.

Regression coverage added (not yet executed): rapid polling after restoring the same endpoint and pinned certificate, maintenance recovery before command retry with zero executions, and authentication loss/recovery after a lost command acknowledgement with exactly one execution.

Passed locally on macOS: Rust formatting, documentation checker (63 Markdown files before this record), generated API model drift check, route registration syntax check, and whitespace check. No Rust build or database test was started: PR #206 owns the sole heavy build slot pending parent authorization. SDK/OpenAPI setup and full checks remain pending that grant. Subsequent Rust commands must use `CARGO_BUILD_JOBS=2`, an isolated target directory, and the existing PostgreSQL on port 55432 without resets or credential output.

CI run 41132690583 on the base head failed both the client/contract job 124327439081 and Rust integration job 124327439161 at the same OpenAPI route scanner assertion: the owner client helper `.route(host)` was mistaken for an Axum registration. Renaming the helper to `owner_route` fixes the genuine syntax collision without weakening the contract checker. Full OpenAPI validation and integration reruns remain pending.

The earlier macOS API library unit-test limitation remains unverified here: `rustix::fs::mkfifoat` in unchanged `provision.rs` does not compile there. This is a limitation, not a passing test. No Linux/KVM, real VM, production, merge or deployment evidence is claimed.
