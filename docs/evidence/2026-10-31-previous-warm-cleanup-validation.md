# Previous-epoch warm cleanup portable validation

Scope: [PR #216](https://github.com/hudson-frontier/hudsonbox/pull/216), validated
on 2026-10-31. Starting remote head:
`7f8bd8a3f07efcb5f267bec776dc8898b0745d3c`. Tested source:
`2fb49e3a280f84aa02ce639e295d47c204efd252`. The subsequent evidence commit changes
documentation only. Earlier 11 controller / 13 store results at
`c417c62e4f21b499113625668c76cf3c5c6d7c36` do not substitute for this run.

## Environment and scope

- Native macOS aarch64, pinned Rust/Cargo 1.92.0; Cargo limited to two jobs.
- Existing PostgreSQL 16.15 on loopback port 55432. A fresh disposable validation
  database supplied SQLx's temporary per-test databases. No existing database
  reset, credential change, new service stack, paid infrastructure or deployment.
- The requested `t3-cffe33ba/target` was idle, but its owning checkout lacked
  migrations 0027–0030. It was not reused. A fresh target used debug level 0 and
  disabled incremental compilation to bound disk use. Free space was about
  20 GiB before compilation and 19 GiB after the focused test runs.
- No guest, real-host or guardian implementation files changed during validation.

## Results

Commands below used the environment described above, with `DATABASE_URL` set to
the disposable local database, `CARGO_BUILD_JOBS=2`,
`CARGO_PROFILE_DEV_DEBUG=0`, `CARGO_PROFILE_TEST_DEBUG=0`, and
`CARGO_INCREMENTAL=0`.

| Command | Result |
| --- | --- |
| `cargo test -p sandbox-controller --test warm_adoption -j2 cleanup_transport -- --nocapture` | 2 passed, 11 filtered out |
| `cargo test -p sandbox-controller --test warm_adoption --test warm_pool -j2` | 13 adoption + 1 unsupported-supervisor test passed |
| `cargo test -p sandbox-store --test warm_pools -j2` | 13 passed |
| `cargo clippy -p sandbox-controller -p sandbox-store -p sandbox-fake-host --all-targets -j2 -- -D warnings` | Passed, including shared-fixture consumers |
| `cargo fmt --all --check` | Passed |
| `python3 scripts/check_docs.py` | Passed, 73 Markdown files |
| `git diff --check` and `git diff --check origin/main...HEAD` | Passed |

The complete focused suite therefore passed **27 distinct tests**, with zero
failures or ignored tests. The two new regressions passed both separately and
within the complete adoption target; they are not counted twice.

The new authenticated controller tests verify that cleanup reaches the current
epoch-2 endpoint while the epoch-1 endpoint remains listening; destruction with a
lost reply keeps the database reservation held; a restarted controller reclaims
the same cleanup operation at the next revision; and only the exact receipt
releases capacity. The old rejected create remains fenced against cold creation.
The second test advances the registry to epoch 3 while an applied epoch-2 reply is
held, then verifies a store conflict and retained cleanup intent/reservation.

Hosted CI on the starting head failed because the new restart-only fixture
fields/helper were unused in other independently compiled integration targets.
The tested source adds narrowly placed `allow(dead_code)` attributes with an
explanation on those shared fixture additions. No runtime behavior changed.
The final focused Clippy run includes the `create` and `leases` targets that
reported the original hosted failure.

## Remaining gates

This is portable PostgreSQL + loopback gRPC/mTLS evidence with simulated retained
fake state. Real-host previous-epoch cleanup remains unsupported in PR #216;
unsupported, unknown or failed cleanup keeps reservations held. Guest late
binding and guardian handoff work in
[PR #218](https://github.com/hudson-frontier/hudsonbox/pull/218) is separate and
is not qualified by these results.

Full local workspace tests, SDK/OpenAPI/image checks and Linux/KVM execution were
not run for this validation. Required hosted `docs-check` and `rust-check` must
pass on the pushed head before merge. No latency, snapshot, billing, deployment
or Linux/KVM readiness claim is made. No merge or monitoring was requested.
