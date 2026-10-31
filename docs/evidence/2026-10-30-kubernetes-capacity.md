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

## Startup and warm-pool safety followup

Tested executable source: `2af96b735384ea75e234147c6517dc7d1af1f173`.
This preserves #214 startup fix `bade74466f28206619af3c164cd30d6bc653a23e`,
#209 `9d4624a1bbeb3749e56b7441e31518da69537ce6`, and #215
`5e77f1224daee55fe5247a51c6fd5685521d1b4d` as ancestors. #214 is now stacked
on #215; migration 0029 remains Kubernetes-only and 0030 remains warm-pool-only.
No dependency source branch was rewritten.

Kubernetes retirement now checks the shared host report's
`unretired_warm_members` before irreversible admission closure or any Kubernetes
API effect. The new regression isolates warm reservations from customer
allocations/operations, covers an expired preparing lease without a receipt
(unknown prepare outcome), ready and retiring members without cleanup evidence,
checks that no API call or retirement marker occurs, then proves that recorded
cleanup permits the same retirement claim to proceed. Existing shared warm-pool
checks also cover stale epochs, drain and cleanup fencing.

Followup validation used Rust 1.92.0, locked dependencies, two build jobs,
`/tmp/hudsonbox-196-target`, `CARGO_PROFILE_DEV_DEBUG=0`,
`CARGO_PROFILE_TEST_DEBUG=0`, and `CARGO_INCREMENTAL=0`. A first invocation using
line-tables-only debug settings missed the cache profile and was interrupted
before test execution; the completed runs below used the settings just listed.
SQLx used fresh isolated test databases on existing PostgreSQL port 55432, without
shared reset, new secrets or a new service stack. Free disk stayed approximately
21-23 GiB during the followup.

| Check | Result |
| --- | --- |
| `cargo test --locked -p sandbox-controller --test k8s_capacity --test capacity --test warm_pool` | 12 passed (9 Kubernetes including projected-Secret/S3 reader and warm retirement, 2 capacity, 1 warm seam) |
| `cargo test --locked -p sandbox-store --test capacity --test host_maintenance --test warm_pools -- --test-threads=1` | 23 passed (11 capacity/upgrade, 1 maintenance, 11 warm pools) |

Total distinct followup tests: **35 passed**. Earlier 40-test evidence above is
historical; registry/provisioning suites were not rerun in this followup.
The artifact mount regression exercises the actual S3 private-config reader on
macOS with symlink projection and a regular inode modeling the subPath mount.
Real Kubernetes bind mounting, Linux-only typed supervisor configuration, cgroup
preparation/enforcement and real KVM were **NOT RUN**. The node preparation guide
explicitly requires delegated cgroup v2 controllers before the KVM node label.
No merge, deployment, paid infrastructure or PR watch was performed.

Strict Clippy passed on the same executable source:
`cargo clippy --locked -p sandbox-controller -p sandbox-store --lib --bin sandbox-host-manager --test k8s_capacity --test capacity --test warm_pool --test warm_pools --test host_maintenance -- -D warnings`.
`cargo fmt --all -- --check`, `python3 scripts/check_docs.py`, and
`git diff --check` passed. The final followup commit changes documentation only;
no executable source changed after the tested SHA above. The sole heavy build
slot is released, with approximately 21 GiB free disk and no build left running.
Full workspace and final-head hosted checks are separate, unclaimed results.
