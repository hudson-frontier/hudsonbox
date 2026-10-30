# Host permit registration evidence

Status: controlled fresh-host integration validation, 2026-09-22. Whole-allocation retirement, legacy migration and supported-host release certification remain incomplete. See [registration behavior](../allocation-authority.md#fresh-host-registration-and-launch).

## Controlled Linux results

The dedicated Lima `hudson-sandbox-dev` VM uses nested ARM Linux/KVM, 4 CPUs, 4 GiB RAM, Firecracker/jailer 1.17.0, guest kernel 6.1.186 and the retained development guest binary. Tests ran as root with `HUDSON_GUARDIAN_TEST_VM=1`, `CARGO_INCREMENTAL=0` and local PostgreSQL. The [source and binary manifest](host-permit-registration-source.json) contains 259 runtime-source hashes checked equal between the worktree and VM, plus host, supervisor, host-test and retained guest binary hashes. This is not a reproducible release-build attestation.

- `cargo test -p sandbox-supervisor --lib -- --include-ignored --test-threads=1`: 21 passed, zero ignored. Registration-checkpoint coverage includes discarded acknowledgements, retained fences, forgotten identities, failed staging writes, stale epochs and missing state.
- `cargo test -p sandbox-supervisor --test host -- --ignored --test-threads=1 --skip real_output_minio_archives_binary_bytes_and_reconciles_after_epoch_restart`: 22 passed in 270.96 seconds. The MinIO-specific VM test was excluded because this run had no VM MinIO fixture.
- The API lifecycle fixture now provisions a permit-enabled host. Authenticated API admission, database issuance, controller registration and Create boot a real VM; renewal and destruction complete through the same runtime.
- A direct authenticated host test rejects absent/unregistered/mismatched permits, boots exactly one allocation after registration, inspects durable progress after discarding the acknowledgement, restarts under a new epoch, rejects stale requests, denies an output-reader certificate, fails health on missing authority and refuses a configuration downgrade. Create/Stop timeouts or uncertain replies require independent exact-owner readiness/release inspection before the test proceeds.
- The test cgroup parent was empty after the host suite completed.
- `cargo clippy --workspace --all-targets -- -D warnings`: passed on Linux.

These tests do not exercise an automatic cleanup/database-completion coordinator, delete real production allocation tombstones or prove more than 1,024 allocation lifetimes. Registration progress is not release evidence. Default legacy mode remains available for existing development hosts; enabling permits requires fresh state and does not migrate those hosts.

## Local workspace validation

`make check` passed with the isolated development PostgreSQL database: formatting, strict Clippy, workspace Rust tests, OpenAPI/model checks, client conformance/package checks and documentation validation. The focused placement suite passed all 22 tests, including independent checkpoint rollback/downgrade/epoch bounds. The schema-0013 source-upgrade fixture uses legacy dispatch primitives before migration; it verifies admitted source state survives the upgrade without invoking schema-0018 controller health checks prematurely.
