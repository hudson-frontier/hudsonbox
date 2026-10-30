# Placement and owner-routing integration

Date: 2026-10-30. Existing [PR #206](https://github.com/hudson-frontier/hudsonbox/pull/206)
(issue #194) integrates routing [PR #207](https://github.com/hudson-frontier/hudsonbox/pull/207)
head `11c63797e6993317db1d23958a9549b1ddb9fb9d` with placement head
`71b96e59a394789a894f627fc8538c38d293ae67`. PR #207 was open, so #206 targets
`codex/193-owner-host-routing`. Both histories and their contracts are retained.

## Integrated behavior

A configured bootstrap host with registry transport selects `Store::place_create`
with `HostScope::Fleet`. NULL registry transport retains explicit configured
legacy/fake placement. Fleet placement never falls back on admission failure.
The resulting reservation's exact host and epoch select the authenticated route;
dispatch intent and response recording retain the existing ownership fences.
Bounded placement waiting, pool grants, quota accounting and uncertain capacity
remain unchanged from the placement implementation.

The owner-routing fixture now provides approved simulated capabilities and pool
grants. One controller admits and places two new sandboxes on different hosts
without pre-seeding reservations. The added create regression drains the bootstrap
host, loses the second host's create reply, then reopens the bootstrap host:
recovery inspects the reserved second host with one dispatch attempt and no
duplicate start. Existing owner-routing failure and API tenant tests use this
integrated fixture.

## Local validation

Native macOS, pinned Rust 1.92.0, `CARGO_BUILD_JOBS=2`, worktree-owned
`target-integration`. Existing localhost:55432 PostgreSQL, SQLx temporary
databases; no shared reset, new container or credential output.

```sh
cargo test -p sandbox-controller --test owner_routing --test create --test recovery --test host_maintenance -p sandbox-store --test multi_host_placement --test placement
cargo clippy -p sandbox-controller -p sandbox-store -p sandbox-supervisor -p sandbox-protocol --all-targets -- -D warnings
cargo fmt --all --check
python3 scripts/check_docs.py
git diff --check
```

Focused tests: 70 passed, zero failed or ignored. Controller: create 19,
owner routing 9, recovery 6, host maintenance 1. Store: multi-host placement 12,
placement 22, host maintenance 1 (selected by the shared test target name).
Formatting, documentation and whitespace checks passed.

## Limits

Portable loopback gRPC/mTLS and database evidence only. Full hosted CI remains
a final-head gate. No redundant full workspace suite, Linux/KVM, real Firecracker,
MinIO, deployment, merge or watch was performed. Native macOS excludes Linux host
implementation checks. Multi-host failure recovery beyond retained owner routing
remains issue #195. This integration does not authorize replacement ownership or
replay of uncertain commands.
