# Multi-host fleet qualification

Date: 2026-10-30. Scope: [#198](https://github.com/hudson-frontier/hudsonbox/issues/198)
under [#187](https://github.com/hudson-frontier/hudsonbox/issues/187).

This record keeps three kinds of result separate:

| Kind | Status |
| --- | --- |
| Portable: fake supervisors, PostgreSQL, HTTP router, loopback gRPC/mTLS | Passed locally (below) |
| Standalone two-host Linux/KVM, operator-run | **NOT RUN.** No hardware result exists |
| Kubernetes worker adapter | **NOT RUN.** Follows [#197](https://github.com/hudson-frontier/hudsonbox/issues/197) |

Fake-supervisor results do not show VM isolation, guardian enforcement,
real network partitions, performance or supported-host compatibility.

## Source boundary

The qualification adds only the test harness
[`fleet_qualification.rs`](../../crates/sandbox-controller/tests/fleet_qualification.rs)
and documentation. It changes no runtime source, schema or protocol. It is
stacked on [#209](https://github.com/hudson-frontier/hudsonbox/pull/209), branch
`codex/196-standalone-capacity` at `f820ddf97de60b7d5543d6f88e68849f22605fbe`,
which already contains #210, #211 and #212. The harness is independent of
`owner_routing.rs`, `fleet_recovery.rs` and `common/mod.rs` and reuses only the
in-memory source backend in `common/sources.rs`.

## Portable coverage

Each test runs two simulated supervisors (A and B) with distinct per-host mTLS
identities behind the host registry, two projects, one PostgreSQL database and
the HTTP router with routed file, live-output and upload backends. Every host
fits two sandboxes.

| Test | Behavior shown |
| --- | --- |
| `two_projects_on_two_hosts_route_every_lifecycle_surface_to_its_owner` | Two allocations per host, with each project spanning both hosts. Upload, execute, file capture, live stream, cancel, lease maintenance and destroy reach each persisted owner. The other project receives 404 for captures and streams. Each host records exactly its own starts, commands and file commits. |
| `concurrent_controllers_create_and_execute_each_operation_once` | Two controllers tick concurrently. Each sandbox gets one allocation and one supervisor start, and each execute dispatches once (`attempt_count` 1). |
| `partitioned_cancel_stays_unknown_without_replay_while_other_host_progresses` | The cancel reply from B is lost, and B is then partitioned. The target stays `unknown`, and the cancel does not complete while A executes new work. After B returns, a replacement controller completes the cancel and B's command count stays 1. |
| `restarted_host_denies_old_epoch_api_reads_while_other_host_progresses` | B is drained, advanced to epoch 2 and restarted as a fresh supervisor. Capture of the epoch-1 allocation returns 404, because the file view requires the host's current epoch. Its live stream returns 410 `output_missing`. The new incarnation receives no starts or commands, A progresses, and the epoch-1 reservation is retained. |
| `lost_stop_on_partitioned_host_keeps_capacity_while_other_host_places` | The stop reply from B is lost, and B is partitioned. B's allocation stays unreleased. New placement uses A, and a further create waits because B's slot is still held. When B returns, inspection confirms the stop, frees the slot, and the waiting create is placed on B. |
| `exhausted_dedicated_pool_waits_without_shared_fallback` | The tenant's create waits when its dedicated host is full, even though the shared host is idle. Another project still places on the shared host. Freeing dedicated capacity lets the waiting create proceed there. |
| `drained_host_takes_no_new_placement_but_keeps_routed_cleanup` | A drained host takes no new placement, including after it becomes empty. Destroy on that host is still routed to it and confirmed. |

Existing suites already provide related coverage, so it is not duplicated here:

- [Owner routing](../../crates/sandbox-controller/tests/owner_routing.rs): wrong-host replies, certificate and name refusal, endpoint moves, slow-host backoff and a single lost acknowledgement.
- [Fleet recovery](../../crates/sandbox-controller/tests/fleet_recovery.rs): stale-host reconciliation and late-reply fencing.
- [Multi-host placement](../../crates/sandbox-store/tests/multi_host_placement.rs): store-level concurrency, quota, tenant-pool denial when a shared pool is full, and the drain race.
- [Standalone capacity](../../crates/sandbox-store/tests/capacity.rs): retirement of occupied hosts and intents.

### Portable retirement and replacement coverage

`retire_replace_at_pool_max_routes_new_identity_without_inheriting_history`
adds controller plus standalone-capacity integration coverage in the same
independent harness. It drives drain/retire/provision intents through the
controller capacity provider router while keeping the original lifecycle
controller across replacement. Its assertions cover:

- Retirement refusal for an occupied drained host and for a lost stop reply
  during partition, with its last reservation and dispatch history retained.
- Confirmed controller cleanup before retirement; an empty drained host still
  cannot accept new work. Replacement enrollment is refused at `max_hosts=2`
  until retirement frees the inventory slot.
- A replacement with a distinct host ID and mTLS identity starts with no
  inherited allocations or operation history. Two projects' new allocations,
  execute dispatches, uploads, file captures and live reads reach that new host.
- Late health, finish-drain, epoch advance and reprovision cannot reopen the old
  tombstone. Historical ownership/dispatch receipts stay attached to the old ID;
  active inventory excludes it, and both original supervisors' counters exclude
  the replacement work.

**Passed:** all eight qualification tests, including retirement/replacement,
and focused strict Clippy passed on tested commit
`659e85acd05f8ef7434df4d890d0f4c2e4ee5d82`. No test or runtime fix was needed.
This portable result does not complete the real-host gate for #198.

### Harness notes

Between ticks, the tests clear one operation's retry backoff and expire its
claim lease. A running controller reaches the same state when the 30-second
lease expires. A cancel completes only after the held target execution is
reclaimed and its cancel action reaches the owner, as in
[cancel](../../crates/sandbox-controller/tests/cancel.rs). A partition points
the registration at a closed loopback port. No source contract defect was
found.

### Local commands and result

Tested commit: `659e85acd05f8ef7434df4d890d0f4c2e4ee5d82`.
Validation date: 2026-10-30 (America/New_York). Native macOS aarch64, pinned
Rust 1.92.0, `CARGO_BUILD_JOBS=2`, and the idle compatible target
`/tmp/hudsonbox-196-target` from the same branch lineage. The existing
localhost:55432 PostgreSQL service was reused with a fresh temporary anchor
database, `qualification_213_659e85a`; SQLx created disposable test databases
and applied this commit's embedded `sandbox_store::MIGRATOR`, including migrations
0027 and 0028. No existing database was reset and no new service was started.
The database URL was supplied in the environment; no credentials are recorded.
The temporary anchor database was dropped after validation. Free disk remained
about 21 GiB before and after the heavy checks. No concurrent heavy Cargo job
was started by this validator; the slot was released after focused Clippy.

```sh
cargo test --locked -p sandbox-controller --test fleet_qualification
cargo clippy --locked -p sandbox-controller --test fleet_qualification -- -D warnings
cargo fmt --all -- --check
python3 scripts/check_docs.py
git diff --check
```

Result: **8 passed, 0 failed, 0 ignored** (2.66 seconds of test execution).
Focused strict Clippy, formatting, documentation checks (69 Markdown files) and
diff whitespace checks passed. The original seven tests had also passed in three
consecutive historical runs before the retirement/replacement addition; those
runs are not claimed for this eight-test head. The subsequent evidence-only
commit does not change the tested Rust sources.

Full workspace CI was not rerun locally. At validation time, hosted `docs-check`
passed on the tested head, but `rust-check` failed in the inherited
`upgrade_preserves_existing_host_state_and_is_repeatable` capacity-upgrade test
(the comparison had not excluded migration 0028 RPC fields). The parent is
handling that failure on #209; no fix is included here. Final hosted statuses
remain authoritative on the PR. Real Linux/KVM and Kubernetes results remain
**NOT RUN**.

## Operator two-host Linux/KVM qualification (NOT RUN)

This procedure is reproducible but **has not been run**. Nothing in this
repository records its result. Running it needs operator-approved x86_64 KVM
hosts that match [compatibility](../compatibility.md#host). No infrastructure
purchase, deployment or paid capacity is authorized by this document.

Record the following before starting:

- Exact commit, `rustc --version`, and the Firecracker, jailer, kernel and rootfs digests.
- Both host kernels, CPU models and KVM API versions.
- Both host IDs, their epochs, capacity documents and pool configuration.
- The controller and API configuration, with private paths named but not their contents.

Never record credentials, certificates, keys or customer data.

1. **Install.** Build both hosts from the same commit using the [source installation](../source-installation.md#build-and-install) guide. Start each supervisor with verified images by following [verified host startup](../verified-host-startup.md#start-the-existing-host) and [real supervisor configuration](../real-supervisor.md#configuration-and-operation).
2. **Enroll.** For each host, follow [initial host provisioning](../host-maintenance.md#initial-host-provisioning) and [host enrollment](../host-maintenance.md#host-enrollment-and-transport-identity). Use a distinct host ID and a per-host certificate. Create one shared pool and one dedicated pool, and grant two projects as in [pools](../host-maintenance.md#pools). `finish-drain` each host only after authenticated health arrives.
3. **Spread.** Create at least two sandboxes per project on each host. Confirm the owners with `fleet-status` ([fleet diagnostics](../host-maintenance.md#fleet-diagnostics-and-incident-runbooks)) and through the API.
4. **Lifecycle per owner.** On each host, run upload, execute, file capture, live stream, cancel and destroy. Confirm that cross-project requests are denied. Compare supervisor journals with operation receipts to show one effect per operation.
5. **Concurrent controllers.** Run two controller processes against the same database. Submit concurrent creates and executes. Confirm one allocation per sandbox and no duplicate journal entries.
6. **Partition.** Block controller-to-B traffic with a host firewall. Cancel a running command on B, and destroy another sandbox on B. Confirm both stay `unknown` or pending, that B's capacity stays reserved, and that work on A continues. Restore traffic and confirm inspection completes them without replay.
7. **Restart.** Drain B, run [advance-epoch](../host-maintenance.md#epoch-issuance-and-restart), and restart its supervisor. Confirm API file and live reads of old-epoch allocations are refused. Confirm guardian enforcement on B independently.
8. **Pools and drain.** Fill the dedicated pool and confirm there is no shared fallback. Drain A and confirm it takes no new placement while its cleanup completes.
9. **Teardown evidence.** Destroy everything, then confirm zero unreleased allocations and that no Firecracker processes, TAP devices or allocation directories remain on either host.

Save the results as a separate dated evidence file. Checkpoint-based recovery
of a lost host's sandboxes is out of scope until
[#178](https://github.com/hudson-frontier/hudsonbox/issues/178). Until then,
a lost host's allocations remain reserved and `unknown`, and restore is not
claimed.
