# Nested aarch64 KVM qualification run (#229)

Date: 2026-10-31. Scope: [#229](https://github.com/hudson-frontier/hudsonbox/issues/229).

Status: **PARTIAL — no real-host test passed.** The operator's Mac had to be
restarted, so the run was cut short. The host preflight, dependency
installation, source copy, pinned artifact verification and both builds are
complete. The `host_warm` suite was executed three times and every test failed
at fixture startup for environment reasons described below; no VM lifecycle
was exercised. `host.rs` was not run. Nothing here claims that a test passed.

This is a nested aarch64 run on an Apple M4 Pro under Lima/Apple
Virtualization.framework. It does **not** satisfy the supported x86_64 release
gates, real Kubernetes qualification, or the operator two-host Linux/KVM
procedure in the [fleet qualification record](2026-10-30-fleet-qualification.md#operator-two-host-linuxkvm-qualification-not-run).
Any latency from this environment is not representative of bare-metal x86_64.

## Source

- Tested source: `015c1b43ddc3fe026178a4b363dc68ff96bc9062` (origin/main),
  copied into the VM with `git archive` of that commit. No local changes.
- Rust: `rustc 1.92.0 (ded5c06cf 2025-12-08)`, `cargo 1.92.0`, installed in the
  VM with rustup (`--profile minimal --default-toolchain 1.92.0`).

## Environment

| Item | Value |
| --- | --- |
| Physical host | Apple M4 Pro, 14 cores, 48 GiB, macOS 26.3.1 |
| Lima | 2.2.0, instance `hudsonbox-qual` |
| VM config | [`dev/lima-linux.yaml`](../../dev/lima-linux.yaml) unchanged, started with `--memory=8 --disk=32` overrides (4 vCPU, 8 GiB RAM, sparse 32 GiB disk) |
| Guest OS | Ubuntu 24.04.4 LTS, kernel `6.8.0-134-generic`, aarch64 |
| KVM | `/dev/kvm`, API version 12 (nested, VZ `nestedVirtualization: true`) |
| cgroup v2 root controllers | cpu, cpuset, hugetlb, io, memory, misc, pids, rdma |
| Clock | Mac and VM UTC agreed to the second at start |
| PostgreSQL | 16.15 from Ubuntu packages, in the VM, disposable `hudson_test` role with `CREATEDB` (credentials kept in a VM-local mode-0600 file, not recorded) |

The memory and disk overrides were passed on the `limactl start` command line;
the repository's Lima configuration was not edited.

## Runtime artifacts

Prepared with `scripts/prepare_runtime_artifacts.py --architecture aarch64`.
Every digest matched [the pins](../../dev/runtime-artifacts.json):

| Artifact | SHA-256 | Reported version |
| --- | --- | --- |
| `firecracker-v1.17.0-aarch64` | `fe726e0b43c04363ac07e358be4dee982c3947c65ed3ae10c770fef5e1cd756c` | Firecracker v1.17.0 |
| `jailer-v1.17.0-aarch64` | `4d8d2dd4dfc1d47932b2bd261479181dd0c54f0a2f32714421a7e7f9f07ddec8` | Jailer v1.17.0 |
| `vmlinux-6.1.186` | `0913a93462f1762581e3b25c434212aeadc06084e97f2e9c21ec4fca3fc54b42` | upstream CI kernel 6.1.186 |

Static BusyBox (`busybox-static`) and e2fsprogs came from Ubuntu packages.

## Commands

```sh
limactl validate dev/lima-linux.yaml
limactl start --yes --name=hudsonbox-qual --memory=8 --disk=32 dev/lima-linux.yaml
limactl copy scripts/check_linux_host.py hudsonbox-qual:/tmp/
limactl shell --workdir=/tmp hudsonbox-qual sudo python3 /tmp/check_linux_host.py
# In the VM:
sudo apt-get install -y build-essential pkg-config protobuf-compiler \
  ca-certificates curl busybox-static e2fsprogs postgresql
python3 scripts/prepare_runtime_artifacts.py --architecture aarch64 \
  --destination "$HOME/runtime-artifacts"
CARGO_BUILD_JOBS=4 cargo build --locked --release -p sandbox-guest
CARGO_BUILD_JOBS=4 cargo test --locked -p sandbox-supervisor \
  --test host --test host_warm --no-run --message-format=json
# Then, per target, the prebuilt test binary as root (as CI runs root-only tests):
sudo -n env HUDSON_GUARDIAN_TEST_VM=1 \
  HUDSON_GUARDIAN_ARTIFACT_DIR="$HOME/runtime-artifacts" \
  HUDSON_GUARDIAN_GUEST_BINARY="$HOME/hudsonbox/target/release/sandbox-guest" \
  DATABASE_URL=<in-VM disposable role> \
  <target/debug/deps/host_warm-*> --ignored --nocapture --test-threads=1
```

The test binaries are run directly under `sudo` rather than `sudo cargo test`
because the pinned toolchain is installed for the unprivileged VM user. The
binary and its compiled-in paths are identical.

## Results

| Step | Result |
| --- | --- |
| Host preflight (`check_linux_host.py`) | **Passed** (exit 0, no errors): aarch64, kernel 6.8.0-134-generic, KVM API 12, cgroup v2 with cpu/memory/pids |
| Dependencies, PostgreSQL, Rust 1.92.0 | Installed |
| Pinned Firecracker/jailer/kernel | Downloaded and digest-verified |
| `sandbox-guest` release build and test-target build | Built (`target/` 3.7 GiB; VM root 7.0 GiB used) |
| `tests/host_warm.rs` (5 ignored tests) | **0 passed, 5 failed — environment**, see below |
| `tests/host.rs` (ignored real-host tests) | **NOT RUN** |
| Cold vs warm time-to-ready samples | **NOT RUN** — no timings recorded |
| Two-host supervisors in one VM | **NOT RUN** |

### `host_warm` failures (environment, not a product defect)

All five tests (`real_warm_prepare_adopt_exec_and_destroy`,
`real_warm_lost_adopt_reply_and_inspect_fence`,
`real_warm_retire_holds_capacity_until_cleanup_proof`,
`real_warm_previous_epoch_member_cleanup_after_restart`,
`real_cold_versus_warm_time_to_ready`) panicked with `host never healthy` at
`crates/sandbox-supervisor/tests/host_warm.rs:189`, about 10 seconds into
each fixture. The spawned `sandbox-host` exited at startup; its `host.stderr`
(captured from the fixture temp directory before cleanup) showed the cause:

1. First run: the dedicated cgroup parent
   `/sys/fs/cgroup/hudson-guardians-tests` did not exist. It is a documented
   prerequisite. It was created as root with `+cpu +memory +pids` in
   `cgroup.subtree_control`. The second run failed identically.
2. Second and third runs: `Error: guardian binary must be root-owned and not
   group/world writable` from
   `crates/sandbox-supervisor/src/host/executable.rs:10-13`. The fixture passes
   `CARGO_BIN_EXE_sandbox-supervisor`, which was built by the unprivileged VM
   user under `$HOME/hudsonbox/target/debug/` with Ubuntu's default umask 002
   (mode 0775). Changing that file's owner to root was not enough: the mode is
   still group-writable, and the ancestor check at `executable.rs:16-24` also
   rejects the user-owned `target/debug`, `hudsonbox` and home directories.

This is the intended host policy, not a bug. It means the real-host suites can
only run from a checkout whose entire path, including `target/`, is root-owned
and not group/world writable, for example built as root under `/opt` or
`/root`. The documented `sudo env ... cargo test` command implies that, but the
[procedure](2026-10-31-real-warm-e2e-procedure.md) does not say it. Running a
binary built by an unprivileged user from its home directory does not work.

No fixture left a cgroup child or temporary directory behind: the host
never launched a guardian. No source or test was changed.

### Expected coverage limits once resumed

- `real_output_minio_archives_binary_bytes_and_reconciles_after_epoch_restart`
  needs `HUDSON_TEST_S3_*` MinIO. The repository builds MinIO only through the
  Docker recipe in [`dev/object-storage`](../../dev/object-storage/README.md),
  and the plain-mode VM has no container tooling. Unless MinIO is built from
  the same verified sources in the VM, that test will be recorded as not run.
- `sustained_real_allocation_reuse_keeps_an_older_vm_live` defaults to 1025
  cycles. A calibration run with fewer `HUDSON_SUSTAINED_REUSE_CYCLES` is not
  full acceptance.
- `real_cold_versus_warm_time_to_ready` takes 3 samples per run. To reach at
  least 10 samples without changing the test, run it four times
  (`--exact real_cold_versus_warm_time_to_ready`) and pool the 12 samples.

## Resume steps

Fix the ownership blocker first. As root in the VM, copy the source into a
root-owned directory (for example `/opt/hudsonbox`, mode 0755, umask 022), and
build the guest binary and both test targets there as root with the same
pinned toolchain (`sudo env PATH="$HOME/.cargo/bin:$PATH" RUSTUP_HOME=...
CARGO_HOME=... cargo ...`). Point `HUDSON_GUARDIAN_GUEST_BINARY` at the
root-owned build. `/sys/fs/cgroup/hudson-guardians-tests` does not survive a
VM restart; recreate it with `+cpu +memory +pids`. Then run the suites.

The VM `hudsonbox-qual` was stopped, not deleted. Its disk keeps the source
copy, the Rust toolchain, verified artifacts, PostgreSQL role and any partial
`target/`.

The operator scripts used for this run were saved in the VM under `~/qual/`
(`build.sh`, `pg.sh`, `run-tests.sh`); they hold no credentials. `build.sh`
skips artifact preparation when `~/runtime-artifacts` exists and re-verifies its
digests. `run-tests.sh <target>` runs the prebuilt binary as root with only the
four variables above and writes a log under `~/results/`. Both scripts assume the
user-owned `$HOME/hudsonbox` tree, so adapt their paths to the root-owned
build before reuse.

```sh
limactl start --yes hudsonbox-qual
limactl shell --workdir=/tmp hudsonbox-qual bash -c 'bash "$HOME"/qual/build.sh'
limactl shell --workdir=/tmp hudsonbox-qual bash -c 'bash "$HOME"/qual/run-tests.sh host_warm'
limactl shell --workdir=/tmp hudsonbox-qual bash -c 'bash "$HOME"/qual/run-tests.sh host'
```

`$HOME` is expanded inside the VM. Afterwards confirm `/sys/fs/cgroup/hudson-guardians-tests` has
no child groups and record the per-test results and `warm_e2e` lines here.

## What this does not show

Even when completed, this nested aarch64 run is development evidence only. It
does not qualify the supported x86_64 Ubuntu 24.04 envelope, real Kubernetes
workers, hostile-workload isolation, or production latency.
