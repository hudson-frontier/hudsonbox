# Standalone capacity adapter portable evidence

Source tested: `3d58d038973fb3db4b1fe838202c0c110975ab66` for
[#196](https://github.com/hudson-frontier/hudsonbox/issues/196), stacked on
[#208](https://github.com/hudson-frontier/hudsonbox/pull/208) dependency
`fbcec79` (including health-freshness regression `c3b5c46`). Later documentation
commits do not change that tested runtime source. No merge or deployment occurred.

## Environment and bounds

Native macOS control plane with the pinned Rust toolchain and existing local
PostgreSQL on port 55432. SQLx created isolated temporary test databases; no
shared schema was reset and no new service stack was started. Connection settings
were passed through the environment without printing credentials. Builds used
`CARGO_BUILD_JOBS=2` and the task-owned `/tmp/hudsonbox-196-target`, after the
parent granted the sole heavy build slot. No paid infrastructure was created.

## Focused results

The following checks passed:

```sh
cargo test -p sandbox-store --test capacity --test host_registry --test host_maintenance --test host_provisioning
cargo test -p sandbox-controller --lib host_discovery
cargo test -p sandbox-controller --test capacity --bin sandbox-host-manager
cargo fmt --all -- --check
python3 scripts/check_docs.py
git diff --check
```

There were 31 passing tests: eight store capacity tests, nine registry tests,
one maintenance test, seven provisioning tests, three discovery unit tests,
two controller capacity tests and one private manager configuration test.
One initial combined controller invocation applied the `host_discovery` filter
to its other targets; those filtered targets were then run without that filter
by the third command above. No filtered test is counted as passing.

The new cases exercise:

- Repeatable pre-0027 upgrade preserving the complete existing host record;
  invalid lease/receipt/warm-pool combinations rejected by database constraints.
- Pool-scoped exact/changed retries, concurrent claims, expired/replaced claims,
  explicit retry, preserved completion receipt and no duplicate enrollment.
- Concurrent enrollment bounded by `max_hosts`, with durable request input
  reloaded so mutating a caller's claim cannot redirect the target.
- Refused retirement with missing health or occupied/unknown/previous-epoch
  reservations; those allocation records remain byte-for-byte unchanged.
- Safe logical retirement retains the identity and registration while blocking
  maintenance reopening. Retirement frees an active worker slot: replacement at
  `max_hosts=1` can reuse the machine address/provider reference, both identities
  remain readable, and no third active worker fits.
- Type changes blocked by unfinished capacity intent and active workers, allowed
  after all workers retire and every capacity intent finishes.
- Concurrent pending-intent limit at 1000 and exact retries at that limit.
- Fake adapter selection by exactly `standalone`/`k8s`, absent-adapter refusal and
  expired-claim denial before the fake provider is invoked.
- Demand/inventory budgets excluding tombstones, capped additions and no implicit
  enqueue/provisioning from an advisory plan.

A separate lightweight psql check also passed the complete fresh migration
chain, pre-0027 unhealthy-host preservation and lease/receipt constraints in a
task-owned temporary database, removed after the check.

## Limits

These are portable PostgreSQL and fake provider tests. No real Linux/KVM host,
VM drain, physical host replacement/termination, checkpoint restore, isolation
qualification or performance measurement participated. The standalone provider
only enrolls existing operator-supplied hosts and commits metadata effects with
its receipt in one transaction. Tombstones do not prove the operator physically
stopped a machine. Future external providers still need provider-side idempotency,
receipts and fencing; a database lease check cannot fence an in-flight network
call. Kubernetes provisioning, warm pools and automatic paid/cloud provisioning
remain unimplemented. Full repository/hosted release validation was not run as
part of this focused build allocation.

## Resumed validation on current main

PR #206 and #208 were verified merged before rebasing the existing PR #209
onto main `d67b359a0d061c9a0b25b09f72b66bd85af2d0d9`. Their individual
commits are no longer duplicated in the branch. The diagnostics from #210,
merged into #209's branch, are preserved. The original worktree was inspected
read-only and remains unchanged. The migration chain, lockfile and toolchain
match the earlier tested source, and no Cargo/rustc process owned the prior
target when it was reused. No second target or service stack was created.

The four store targets above passed again (25 tests), as did controller capacity
and the manager binary (three tests), on the rebased source. SQLx used isolated
temporary databases on the existing port 55432 service. Free disk remained
27 GiB after the final test compilation.

Workspace strict Clippy initially found a pre-existing macOS test compilation
failure in the API credential test: rustix does not expose `mkfifoat` on this
platform. The test now uses the Unix `mkfifo` utility and retains the same FIFO
rejection assertion; no production credential behavior changes.

The resumed strict workspace check passed:

```sh
cargo clippy --workspace --all-targets -- -D warnings
```

The changed API FIFO rejection test also passed at runtime (one test).
Formatting, documentation checks and whitespace validation passed again.
Logs are retained locally as `/tmp/hudson196-resume-{store,controller,clippy,api,discovery}.log`;
they contain build/test evidence, not a hosted release qualification.

The three controller host-discovery unit tests passed again on rebased source,
bringing the resumed focused total to 32 passing tests including the API regression.
