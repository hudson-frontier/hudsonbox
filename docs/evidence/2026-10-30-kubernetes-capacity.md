# Kubernetes capacity adapter portable checks

Date: 2026-10-30 (America/New_York). Tested runtime/test source commit:
`8435140495f8cfb0f89cc2aeb9bcb4e583f769ab`, stacked on #209
`f820ddf97de60b7d5543d6f88e68849f22605fbe` including #211/#212.
This evidence-only followup does not change executable source.

The [capacity contract](../kubernetes-capacity.md) owns behavior and operational
constraints. [PR #213](https://github.com/hudson-frontier/hudsonbox/pull/213), head
`659e85acd05f8ef7434df4d890d0f4c2e4ee5d82` at handoff, owns #198's common fleet
qualification. Its harness is not duplicated in this PR.

## Environment and limits

macOS development host; repository Rust 1.92.0, locked dependencies, existing
PostgreSQL at loopback port 55432. SQLx created isolated temporary databases;
no shared reset, new service stack, cluster or KVM access. Parent explicitly
released #198's sole heavy build slot and granted #197 two jobs. Checks reused
`/tmp/hudsonbox-196-target` with `CARGO_BUILD_JOBS=2`; no new target directory,
new Kubernetes client dependency or broad workspace build. Available disk was
23 GiB at start, approximately 21–22 GiB during/after focused checks. Slot released
after these checks; later work was source/evidence/PR metadata only.

## Results

All cargo commands below used `--locked`, the existing database URL privately
and the target/job settings above.

| Check | Result |
| --- | --- |
| `cargo test -p sandbox-controller --test k8s_capacity --test capacity` | 9 passed (7 adapter, 2 router/demand) |
| `cargo test -p sandbox-store --test capacity --test host_maintenance` | 12 passed (11 capacity including upgrade, 1 maintenance) |
| `cargo test -p sandbox-store --test host_registry --test host_provisioning -- --test-threads=1` | 19 passed (12 registry, 7 provisioning) |
| `cargo clippy -p sandbox-controller --lib --bin sandbox-host-manager --test k8s_capacity --test capacity -- -D warnings` | Passed |
| `cargo fmt --all -- --check` | Passed |
| `python3 scripts/check_docs.py` | Passed |
| `git diff --check` | Passed |

Total distinct focused executable tests: **40 passed**. Initial `cargo check -p
sandbox-controller --tests --locked` also passed before draft publication.

The adapter tests use a real temporary PostgreSQL database and a fake Kubernetes
API implementation. They cover lost create/delete acknowledgements, exact retry
and changed-input conflict, frozen configuration after pool changes, stale receipt
rejection, an old create held until a successor provisions and retires the worker,
retained Pod tombstone/UID protection, occupied/unknown retirement refusal,
replacement enrollment without identity reuse, explicit budgets and overflow
rejection, global node anti-affinity, pending demand bounds, failed-enqueue rollback,
admission remaining closed before a provision receipt, changed command/affinity or
missing finalizer refusal, missing-object/UID mismatch, canonical resource units
and ordinary Kubernetes metadata/default fields. Discovery reports direct per-host
endpoints and excludes terminating workers; it grants no lifecycle authority.

The following failures were investigated rather than hidden:

- The initial test compile referenced the Linux-only supervisor config type on
  macOS. The portable assertion now checks the generated JSON, with an additional
  typed supervisor-config assertion compiled on Linux (unrun locally).
- The inherited capacity upgrade assertion expected only migration 0027's
  retirement column. Migration 0028 from merged #212 also adds RPC observation
  fields. It now asserts those exact additive defaults and the empty 0029 worker
  table before comparing all original host fields. The initial draft's hosted
  integration run failed at the same assertion; the corrected local test passes.
- The parallel store run passed capacity/maintenance and 11 registry tests but
  `fleet_rpc_observation_lock_wait_is_cancelled_on_server` hit its 100 ms caller
  budget instead of the expected server-side 75 ms cancellation. The complete
  registry suite passed serially with no product-code change. This timing
  sensitivity is retained as a limitation, not reported as a clean parallel pass.

## Unrun qualification

No real Kubernetes API transport/CA/RBAC admission, scheduler/autoscaler, node
provisioning, Pod DNS, immutable Secret handling, KVM/mount/cgroup delegation,
privileged resource enforcement, multiple real guest VMs, physical termination,
eviction/partition or checkpoint recovery was tested. The static manifests were
source-reviewed, not applied or server-validated. Pod deletionTimestamp proves a
retained deletion request, not stopped processes or safe recovery. Real hardware
and cluster evidence remain required. Full portable workspace/hosted checks are
separate from these focused results. No merge, deployment, paid infrastructure,
release or monitoring was performed.
