# Portable fleet host-loss recovery qualification

Date: 2026-10-30. Scope: [#195](https://github.com/hudson-frontier/hudsonbox/issues/195)
under [#187](https://github.com/hudson-frontier/hudsonbox/issues/187).

## Source and dependency boundary

Qualified runtime and test source:
`c3b5c469d95e2cf0e329df09389b7848eecf50ab`, based on integrated placement
[PR #206](https://github.com/hudson-frontier/hudsonbox/pull/206) at
`468a13c933958a1ef54333c3824129ace06d0ec3`. That dependency includes the
routing work from [PR #207](https://github.com/hudson-frontier/hudsonbox/pull/207).
The #195 PR is stacked against #206, excluding its dependency diff. No schema
migration, checkpoint format, new release mechanism or infrastructure adapter is
introduced here. The later evidence-only commit does not change executable source.

## Behavior exercised

- Fleet-wide stale/null heartbeat and changed-epoch detection marks current
  sandbox observations unknown, including unaddressed hosts. Batches skip locked
  sandbox rows and retain all allocation CPU/RAM/disk reservations.
- Concurrent replicas and a restarted controller preserve allocation identities,
  generation, epochs, dispatch receipts and monotonic claim revisions. Late
  replies from an expired claim cannot complete work.
- Failed host health immediately clears placement freshness for that epoch.
  A delayed old-epoch failure preserves a newer epoch's heartbeat. Database
  writes have bounded statement timeouts; host invalidation commits before
  sandbox observation updates to avoid inverted lifecycle lock ordering.
- A returning heartbeat is insufficient to mark an allocation running. The
  original owner must supply current incarnation evidence. A lost create reply
  reconciles the original allocation, with exactly one simulated VM start.
- Dispatched commands whose owner cannot be routed report unknown without losing
  dispatch intent. A two-host mTLS regression leaves host B's command unknown
  while A completes independent work; a restarted controller inspects B's
  original operation and records its receipt without replaying the command.
- Existing prior-epoch tests require exact, fresh verified release evidence,
  retain uncertainty when a supervisor journal is unavailable, and reject stale
  claims/reporters. A database epoch change or ordinary absence cannot release
  capacity. Verified cleanup reports destruction or failed creation, not restore.

The executable checks live in [fleet recovery](../../crates/sandbox-controller/tests/fleet_recovery.rs),
[two-host routing](../../crates/sandbox-controller/tests/owner_routing.rs), and the
existing [prior-epoch recovery](../../crates/sandbox-controller/tests/recovery.rs)
and [lease maintenance](../../crates/sandbox-controller/tests/leases.rs) suites.
The owning contract is [lifecycle](../lifecycle.md#fleet-host-loss-reconciliation).

## Local configuration and commands

Native macOS, pinned Rust toolchain, `CARGO_BUILD_JOBS=2`, isolated Cargo target
`/tmp/hudsonbox-195-target`. Existing localhost:55432 PostgreSQL was used through
SQLx disposable test databases. No shared schema reset, new dependency service or
credential output was performed.

With the development database URL supplied privately in the environment:

```sh
cargo test -p sandbox-controller --test create --test destroy --test execute --test fleet_recovery --test leases --test owner_routing --test recovery --test uploads
cargo clippy -p sandbox-controller -p sandbox-store --all-targets -- -D warnings
cargo fmt --all --check
python3 scripts/check_docs.py
git diff --check
```

## Results

On the qualified source above: **111 passed, zero failed, one ignored**.
Create 19, destroy 13, execute 11, fleet recovery 7, leases 18, owner routing 10,
prior-epoch recovery 6, uploads 27 passed plus one ignored MinIO test.
Strict Clippy, formatting, documentation and whitespace checks passed.
The source/testing commits and this evidence record are distinct so the tested
source revision can be identified without claiming a future self-referential SHA.

## Limits

These checks use PostgreSQL, simulated supervisor behavior and loopback gRPC/mTLS.
No Linux/KVM, Firecracker VM, guardian watchdog termination or physical fencing
was qualified. An in-memory simulator's missing journal is explicitly uncertainty,
not a synthetic physical cleanup receipt. Published durable checkpoints remain
pending [#178](https://github.com/hudson-frontier/hudsonbox/issues/178); neither
filesystem restore nor RAM/process continuation is implemented or claimed.

One upload test requires a private versioned MinIO bucket and was skipped because
that configuration was unavailable. The complete workspace/SDK/image suite was
not run locally. Full hosted Rust CI is a separate final-head gate; a stacked PR
against a non-main dependency branch does not establish that gate. No merge,
deployment, release, paid infrastructure or PR watch was performed.
