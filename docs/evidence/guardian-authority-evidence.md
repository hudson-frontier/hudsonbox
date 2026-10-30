# Guardian launch authority evidence

Status: component validation on a controlled nested ARM Linux/KVM development VM, 2026-09-22. This is not supported x86 release certification or end-to-end whole-allocation retirement evidence. See the [protocol and integration limits](../allocation-retirement.md#implemented-guardian-component).

## Environment and source

Lima `hudson-sandbox-dev`: nested VZ ARM, 4 CPUs, 4 GiB RAM, Firecracker/jailer 1.17.0, guest kernel 6.1.186 and the retained development guest binary. Tests ran as root with `HUDSON_GUARDIAN_TEST_VM=1` and `CARGO_INCREMENTAL=0`. The [source and binary SHA-256 manifest](guardian-authority-source.json) records 255 runtime source files whose bytes matched between this worktree and the VM, plus the host, supervisor, host-test and retained guest binaries. This is a source snapshot, not a reproducible release build attestation.

## Results

- `cargo test -p sandbox-supervisor --lib -- --include-ignored --test-threads=1`: 20 passed, zero ignored. Includes six root-only authority tests covering durable epochs, exact ownership, retirement/reopen, cross-process locking, lock replacement, metadata deletion, corruption, rollback checkpoints, failed writes and orphan staging.
- `cargo test -p sandbox-supervisor --test guardian --test guardian_authority -- --ignored --test-threads=1`: 11 existing guardian tests and one new authority test passed. The new test boots real VMs, denies renewal after fencing, preserves inspect/stop, verifies shutdown before deleting test-owned metadata, denies old and legacy replay, admits a newer allocation, then denies its stale epoch.
- Real-VM host suite: 21 passed, zero failures; the MinIO-specific test was excluded because this run had no VM MinIO fixture. Command: `cargo test -p sandbox-supervisor --test host -- --ignored --test-threads=1 --skip real_output_minio_archives_binary_bytes_and_reconciles_after_epoch_restart`, with the VM local PostgreSQL database. The test cgroup parent was empty after completion.
- `cargo clippy -p sandbox-supervisor --all-targets -- -D warnings`: passed on Linux. An initial non-root invocation could not open the root-owned Cargo lock; the corrected root invocation passed.

The new VM test explicitly simulates a trusted cleanup/database coordinator after verified shutdown. There is no implemented automatic deletion coordinator or durable database acknowledgement in this test. The ordinary CI run skips these privileged tests; their explicit controlled-VM execution is recorded separately here. No production image, network-isolation, supported-host or greater-than-1,024 real-allocation lifecycle gate is claimed.
