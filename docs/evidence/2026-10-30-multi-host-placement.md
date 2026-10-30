# Multi-host placement portable validation

Date: 2026-10-30. Scope: [issue #194](https://github.com/hudson-frontier/hudsonbox/issues/194)
and [PR #206](https://github.com/hudson-frontier/hudsonbox/pull/206), based on
`1e52140a204fd775057622f812d604e8742a4b28` plus the fleet capability-evidence
fix committed with this record. This is native macOS/PostgreSQL evidence,
not Linux/KVM qualification or a multi-host routing completion claim.

## Behavior corrected

A registered transport identity alone previously allowed an unqualified legacy
host into fleet placement via the single-host compatibility exception. Fleet
placement now requires capability evidence, then checks approved/observed
contracts at the current epoch. Configured legacy/fake placement keeps its
existing behavior. Existing allocations still return their original owner
before qualification checks, preserving reconciliation and fencing.

The regression verifies fleet denial without any allocation or permit write,
configured legacy reservation, and subsequent fleet reconciliation of that same
owner. Fleet fixtures now carry explicit simulated capability contracts.

## Commands and results

All Cargo commands used the pinned Rust 1.92.0 toolchain,
`CARGO_BUILD_JOBS=2` and this worktree's own `CARGO_TARGET_DIR=target-pr206`.
Database settings were loaded privately from the existing local configuration;
PostgreSQL was already listening at localhost port 55432. SQLx tests created
isolated databases. No shared schema reset or dependency restart was performed.

```sh
cargo test -p sandbox-store --test multi_host_placement --test placement \
  --test host_epochs --test host_capabilities --test host_identities \
  --test host_provisioning --test host_registry
cargo test -p sandbox-controller --test create --test recovery --test host_maintenance
cargo test -p sandbox-supervisor --test host
cargo clippy -p sandbox-store -p sandbox-controller -p sandbox-supervisor \
  -p sandbox-protocol --all-targets -- -D warnings
cargo fmt --all --check
python3 scripts/check_docs.py
git diff --check
```

- Store: 65 passed, zero failed or ignored: `multi_host_placement` 12,
  `placement` 22, `host_epochs` 4, `host_capabilities` 7,
  `host_identities` 4, `host_provisioning` 7, `host_registry` 9.
- Controller: 26 passed, zero failed or ignored: `create` 19,
  `recovery` 6, `host_maintenance` 1.
- Supervisor `host`: command succeeded but **zero tests executed** because the
  entire suite is gated on Linux. The modified Linux host module is also
  excluded from native macOS Clippy. No KVM was available.
- Strict focused Clippy, formatting, documentation and whitespace checks passed.
- No Rust compilation, test or Clippy failures occurred in either the initial
  focused run or the final rerun after the capability fix. The initial run had
  11 multi-host tests; the final run includes the added regression. No unrelated
  workspace builds were run.

These tests exercise concurrent reservations, project quotas, tenant-pool
boundaries, drain/placement races, host freshness, claim and epoch fencing,
uncertain capacity, bounded waiting and original-owner recovery. They do not
prove physical resource enforcement or guest isolation.

## Remaining gates at the original placement-only revision

- Required hosted `docs-check` and aggregate `rust-check` must pass on the final
  pushed head. Native focused results do not replace the full CI suite.
- The original routing integration requirement is now satisfied by the
  [integrated validation record](2026-10-30-placement-routing-integration.md).
- Execute the Linux-only supervisor host regressions on compatible Linux/KVM
  hardware, with real evidence reported separately.
- Pool grants remain an explicit upgrade/provisioning step documented in
  [API setup](../api-server.md) and [host maintenance](../host-maintenance.md#legacy-backfill-and-upgrade).
  There is no implicit tenant-pool fallback.

The heavy local Rust build slot was released after the focused commands finished.
No merge, deployment or paid infrastructure was used.
