# Registered host receipt evidence

Status: controlled component/admission validation, 2026-09-22. See [receipt admission](../allocation-authority.md#admission-of-host-receipts). No allocation metadata deletion or database retirement coordinator is implemented by this change.

## Source and environment

The dedicated Lima `hudson-sandbox-dev` VM uses nested ARM Linux/KVM, 4 CPUs, 4 GiB RAM, Firecracker/jailer 1.17.0, guest kernel 6.1.186 and the retained development guest binary. Root tests use `HUDSON_GUARDIAN_TEST_VM=1` and `CARGO_INCREMENTAL=0`. This is not supported x86 release certification.

All 259 runtime source files were checked byte-for-byte equal between worktree and VM. The [source delta and binary hashes](registered-receipts-source.json) replace the six changed source hashes in the [base source manifest](host-permit-registration-source.json); all other source hashes remain identical. Apply `source_sha256_overrides` by path to reconstruct the complete source snapshot. Binary hashes describe the final host, supervisor, host-test and retained guest binaries, not a reproducible-build attestation.

## Checks

- `cargo test -p sandbox-protocol`: 60 passed, zero ignored. New model coverage resolves only retained active allocations and rejects unknown, fenced, completed and forgotten identities.
- `cargo test -p sandbox-supervisor --lib -- --include-ignored --test-threads=1`: 22 passed. The new root test verifies the shared persistent gate blocks fencing until admission releases it and rejects wrong host, epoch or rollback below the independent frontier.
- Controlled host suite: 23 passed in 259.57 seconds, with `--ignored --test-threads=1 --skip real_output_minio_archives_binary_bytes_and_reconciles_after_epoch_restart`. The MinIO-specific VM test was excluded because this run had no VM MinIO fixture. The cgroup parent was empty afterward.
- Strict Linux workspace Clippy passed after removing two unnecessary test clones. The affected RPC test was rerun against that final source and passed in 3.32 seconds. Production source was unchanged by the lint cleanup.
- Formatting, documentation and diff-whitespace checks passed. The full local `make check` was not repeated for this focused change; the protocol suite and all controlled host cases cover the changed admission path. Hosted workspace checks remain separate.

The new authenticated host test sends repeated unknown-owner Inspect/Stop calls and verifies the persisted journal stays empty. A wrong project also fails without adding a receipt. It directly simulates trusted authority fence/complete/forget transitions for an unused permit, then verifies those requests cannot recreate host metadata. A different active permit can install an absence fence, and its existing receipt remains inspectable after fencing. These simulated coordinator transitions do not establish production consumer closure or authorize metadata deletion; denial is not release evidence.

## Hosted cancellation test follow-up

The first hosted Rust run at `39bbf692807ee7ac9ef3d8c3a5c64ca66d75c745` failed in the existing cancellation test while immediately claiming pending output. An idempotent cancellation retry drops a read-only SQLx transaction; its rollback can still hold the target row while the output worker uses `SKIP LOCKED`. The test now explicitly acquires that row lock, verifies the worker skips it, awaits rollback, and verifies the next claim belongs to the original command. All nine cancellation tests pass locally, with strict targeted Clippy. Runtime behavior is unchanged. The source/binary manifest above records the earlier controlled VM snapshot; this subsequent controller test edit is outside that snapshot.
