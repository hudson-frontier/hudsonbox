# Acknowledged history reclamation

Status: live-allocation database coordination, authenticated host/guest retirement and public capacity refunds are implemented behind the controller's `--retire-history` opt-in. Verified destruction retirement, including database coordination and refunds across host epoch changes, uses the same opt-in. The separate `--retire-allocations` worker now connects physical allocation metadata retirement and host-record forgetting. [Issue #79](https://github.com/hudson-frontier/hudsonbox/issues/79) remains open for final lifecycle/marker acceptance and current-source controlled sustained-reuse evidence. This document owns the retirement barrier and its platform prerequisites.

## Why deletion needs an admission barrier

A retained receipt prevents a delayed request from looking like a new operation. Removing the receipt without retaining a fence can execute a command or publish an upload again. An elapsed retention deadline, a successful object DELETE, or a cancelled local request does not provide that fence.

The [barrier type](../crates/sandbox-protocol/src/history.rs) closes an inclusive prefix of operation IDs for one allocation, generation, guest boot and domain (`commands` or `files`). IDs are compared as opaque ordered values. Their UUID timestamps are not an expiry clock and are not assumed to match admission order. A retained barrier rejects every later admission at or below `through`, including IDs for which no receipt remains. One monotonic value replaces the discarded per-operation admission records.

A caller must have durably retained the original outcomes and retired all consumers that still need guest bytes **before** asking the guest to remove history. The controller-only `RetireHistory` RPC asserts that these prerequisites have been durably satisfied. The host independently verifies allocation ownership, the original guest boot, and known terminal outcomes; it does not query the database or prove output/source retirement. The independent controller worker below is the automatic caller; there is no customer retirement endpoint.

## Guest implementation

[`Runner::retire_history`](../crates/sandbox-guest/src/runner.rs) and [`Transfers::retire_history`](../crates/sandbox-guest/src/files/mod.rs) follow this sequence under their existing ownership locks:

1. Validate the barrier version, domain and exact allocation/generation/boot. A healthy engine reconciles a repeated or lower request by returning its current barrier without regressing it.
2. Check the entire covered prefix. Commands must have a known terminal outcome, confirmed process-tree cleanup and no active covered command or remaining covered cgroup. Files must be committed or aborted. Active, staging, commit-intent, unknown and unconfirmed-cleanup work blocks advancement. Newer commands can remain active outside the prefix.
3. Replace and sync `context.json` with the complete versioned binding and barrier. This happens before any deletion. A separate optional watermark file would be unsafe to lose while retaining an apparently legacy context.
4. Unlink covered internal receipts, output/exit files, staged data and temporary metadata, then sync the state directory. Scanning remains bounded by the existing retained-state envelope. Basenames must be recognized internal operation names; deletion never follows symlinks or recursively deletes directories. Published upload destinations are untouched.
5. Remove the same records from in-memory accounting only after deletion/sync succeeds. The successful return acknowledges guest-local completion. `history_barrier` reports the admission floor alone and is **not** a deletion acknowledgement.

Any uncertain barrier write or deletion poisons the current engine for new work until recovery. Covered commands/uploads cannot be admitted even if some old receipt remains in memory. Reads may already hold a receipt or open file snapshot; they do not gain new mutation authority. Removing directory entries is not physical erasure of open file handles or backups. Guest filesystem quotas remain independent of declared output/upload reservations.

On reopen, load and validate the binding first, finish pruning its closed prefix, then load and charge only the remaining receipts. This handles a crash after the barrier but before deletion, a partially removed prefix, and a lost completion acknowledgement. Recovery never needs the removed receipt to decide whether a covered request may run. A malformed binding or unexpected remaining cgroup fails closed. Unknown outcomes beyond the barrier keep their normal recovery behavior and remain charged.

Existing context files load unchanged. Only an explicit retirement upgrades the binding. Older binaries reject the upgraded context shape, so downgrading cannot silently forget the barrier. Removing/replacing the binding or restoring an older backup is unsupported. Guest root can tamper with guest state; the host must retain its own independent barrier before it discards its records. A guest acknowledgement is not proof of VM destruction, host resource release, physical erasure or sandbox isolation.

## Host and authenticated transport

The controller-pinned supervisor service exposes `RetireHistory(HistoryRequest)`. Its ownership uses an independent per-domain claim revision and bounded deadline. It accepts a versioned guest barrier, never a host path or storage credential. The API/output/file-reader identities cannot call this mutation. The simulator explicitly rejects retirement because it cannot supply durable evidence.

Under the allocation gate, the host validates the exact project/sandbox/allocation/generation/host epoch and bound guest boot. Covered commands must be known terminal with confirmed cleanup, or have durable not-started fences. Covered uploads must be committed, aborted or durably not-started; `Unknown` is not sufficient. Newer work outside the prefix stays retained. A pending retirement must complete before its prefix can advance. Reusing a revision with a changed barrier or sending an older revision fails. A newer claim may reconcile an equal/lower prefix to the current barrier without regression.

The host persists the retirement intent in its existing journal before calling the guest over pinned mTLS/vsock. Both pending and completed barriers reject old command execution/inspection/cancellation, upload begin/write/inspection/commit/abort, archive setup/completion and live-output access. Missing discarded history never becomes new not-started evidence. An archive/read already in flight can fail; a late storage writer still requires the separate object-store retirement marker to fence its effects.

The guest returns a synced-deletion acknowledgement. The client validates response correlation and the **exact** approved barrier, including boot/domain/prefix. A guest cannot enlarge the prefix used for host reclamation. Timeout, lost response, malformed acknowledgement or expired claim retains the host floor and charged records. Repeating retirement reconciles guest completion; a floor-only getter is never enough.

Only after a valid acknowledgement does one durable host journal replacement remove the covered domain's records, associated command archives, and those operations' revision entries. The barrier remains. A failed journal write poisons the host until restart. Completed barriers survive journal reload; older binaries reject the added fields. The host checks journal byte headroom before installing a new barrier and rejects exhaustion before contacting the guest. This recovers operation slots and their declared budgets, but does not guarantee recovery from an already byte-full legacy journal.

New or unfinished `RetireHistory` requests require the original live guest in the current host epoch. A completed retry returns retained proof without requiring a live guest, including after same-epoch destruction. Restart advances the epoch and stops old ownership; pending retirement remains fenced and charged. The separate `RetireReleasedHistory` path below reclaims eligible history after verified destruction without requiring that guest. A direct internal RPC does not refund database reservations: the database worker must verify and persist its own exact completion. Do not delete host journals, restore older snapshots, or discard storage markers as a substitute for that protocol.

## Platform coordination

The table records the coordination contract. Live and verified-destruction paths are implemented; snapshot/restore and whole-allocation journal lifecycle remain future work:

| Layer | Required behavior before automatic retirement |
| --- | --- |
| Database preparation | Serialize preparation with admission under the same allocation lock. Reserve a monotonic per-domain barrier only when every admitted operation in its prefix has a known terminal outcome and all necessary output/source retirement has been verified. Preserve original identity, owner, key, request digest/version and outcome for the project's lifetime. Do not skip an unresolved operation merely because later operations are eligible. |
| New admission | Mint every new operation above the reserved barrier under that lock, including after clock rollback or concurrent frontend admission. Ordinary UUID generation alone is insufficient. Exact retries resolve their retained original identities before any new admission. |
| Host intent | Validate the exact approved owner, domain and prefix against retained command/file/archive evidence under the allocation gate. Durably install its own admission barrier before contacting the guest. Every delayed begin/execute/write/commit/cancel/archive path must respect it; old inspection must report retired history rather than inventing not-started evidence. |
| Guest acknowledgement | Call the authenticated guest primitive for the original bound boot. Reconcile a lost reply through the retained barrier. Validate the response scope and sufficient floor; an untrusted guest must not enlarge the prefix approved by the database/host or refund unrelated work. |
| Host completion | Discard eligible host records and release their reserved journal headroom only after the required guest acknowledgement. A separate path may use verified allocation destruction and exact ownership; a stopped flag, expiry or missing guest alone is insufficient. Keep the host barrier. |
| Database completion | Validate the retained retirement attempt and authenticated acknowledgement under the current claim, then mark only that approved prefix reclaimed. Admission accounting excludes it once, without reopening operations or freeing old idempotency keys. Cancellation, stale workers, corruption and lost acknowledgements retain enough intent to reconcile. |
| Later lifecycle | Snapshot/restore, previous-epoch recovery and whole-allocation journal retirement must preserve the admission barriers or establish a stronger independent fence. Never restore an old guest/context as authority for new work. |

Object-store markers have a different problem: an already issued conditional PUT may complete after a local timeout. Guest barriers do not fence that storage request. The [output](output-storage.md#storage-retirement) and [file-source](file-transfer.md#source-cleanup-worker) markers remain retained until a separate namespace/authority protocol proves old writers cannot recreate payloads. No marker deletion is added here.

## Retirement after verified allocation destruction

`RetireReleasedHistory` is a separate controller-only RPC for known history on a stopped allocation. Its request binds the original allocation ownership and epoch, the current reporting epoch, a domain, an inclusive operation prefix and an independent claim revision/deadline. Its acknowledgement echoes that request and returns the completed prefix, verified `Released` or `FencedAbsent` state, provenance and observation time. It does not manufacture a guest context for an allocation that never booted. The simulator refuses durable proof.

Under the existing allocation gate, the host requires retained exact ownership and a durable stopped fence, then rechecks original guardian, cgroup and filesystem cleanup, including on retries. Missing metadata, lease expiry or a stopped flag alone cannot authorize retirement. Covered commands still require known terminal outcomes and confirmed command cleanup; uploads must be committed, aborted or durably not started. Unknown commands, staging, commit intent and unknown file outcomes block the prefix even after VM destruction. Executed records must match the original manifest boot. The trusted controller must independently preserve outcomes and finish consumer retirement before calling this RPC.

One synced journal replacement removes covered records and retains a versioned per-domain completion plus the whole allocation tombstone. A save failure poisons the host; restart reloads the last durable state, and retries revalidate destruction before completion. Lower prefixes cannot regress the floor, changed requests cannot reuse a revision, and a newer reporting epoch requires a newer claim. Delayed command/file/archive/read requests remain fenced. The new journal fields make older binaries reject upgraded state. The allocation tombstone, guardian metadata, database outcomes and permanent storage markers are not deleted.

[Migration 0016](../migrations/0016_released_history.sql) adds independent `released_allocation_history` claims and retains the exact destruction request and acknowledgement. `released_at` and allocation status only schedule verification; they never authorize a refund. The coordinator preserves the original allocation epoch, uses the current host reporting epoch, reserves an eligible contiguous prefix under the allocation lock, and repeats its consumer checks before completion. Terminal guest evidence freezes an optional original context; never-started allocations need no invented boot.

The initial lower bound may use a separately verified live completion. Corrupt prior proof cannot skip old operations. Lost replies preserve the reserved prefix and use a newer claim, including after host restart. Final writes recheck the lease and reporting epoch after lock waits. Only an exact fresh `Released`/`FencedAbsent` acknowledgement completes the prefix. Unknown predecessors, unretired sources/output, partial cleanup, stale workers and mismatched ownership remain charged. No original outcome is rewritten.

The shared accounting view selects one greatest verified live/destruction prefix per allocation/domain, preventing join multiplication and duplicate refunds. Project/global upload slots recover only after verified completion; source-byte accounting still depends on its own retirement proof. Existing operation rows, retry keys, request digests and outcomes remain retained. Migration 0016 preserves existing live proofs and does not retire records by itself.

The [destruction-retirement evidence](evidence/2026-09-22-released-host-history.json) records matching source hashes, binary hashes, validation and remaining integration limits. The Linux state tests cover domain separation, prefix/revision ordering, retained tombstones, malformed completion, unknown commands and unfinished uploads. Controlled authenticated host tests cover actual VM destruction, new retirement after a host epoch change, lost replies, reader denial, mismatched/expired ownership, fenced absence without a boot, unowned filesystem state, failed persistence/restart and delayed Create rejection. These remain development-environment evidence, not supported-platform isolation certification.

## Database claims, accounting and operation ordering

[Migration 0015](../migrations/0015_history_retirement.sql) records independent command/file history claims, reserved and completed prefixes, frozen guest context, request and completion proof. A new prefix is reserved under the allocation row lock before any host request. The same lock protects command and upload ID allocation: each ID exceeds both reserved domain floors and the last admitted ID. The UUIDv7 version/variant bits survive counter carries and clock rollback. The migration backfills the last admitted ID from existing pinned operations; it neither retires history nor changes original operation rows.

The [coordinator](../crates/sandbox-store/src/history/coordinator.rs) scans up to 32 allocations per tick, rotating by per-domain scan time and skipping locked allocations. It selects a contiguous prefix of at most 33 commands or 17 uploads, stopping at the first ineligible operation. Corrupt candidates retain their data and are deferred without preventing other allocations from progressing. Retirement never takes operation/project/sandbox/host row locks after its allocation lock. Completed output-cleanup evidence is revalidated under its cleanup-row lock; source validation shares the already-owned allocation lock.

Commands require durable not-started evidence or a known terminal receipt with confirmed process cleanup. Executed commands require expired output and verified cleanup of every issued ticket, or completed payload compaction that fenced an unissued ticket. Uploads require known not-started/committed/aborted state and completed source retirement, including the frozen original plan/reference. Unknown outcomes, expiry alone and cleanup intent are insufficient. Original operation IDs, keys, digests, outcomes and database receipts remain retained.

When terminal evidence supplies a guest context, the coordinator freezes it. Otherwise controller-only `HistoryBinding(LeaseInspection)` reads the original manifest's bound boot under the host allocation gate. It never boots or rebinds a guest. The database validates the exact request echo, owner, boot, provenance and observation time before persisting a retirement request. A later response cannot replace that boot. The simulator refuses both binding and durable retirement.

Claims last at most 300 seconds; the worker uses 120 seconds and a 30-second bound per RPC. Retries retain the reserved prefix and obtain a newer independent claim revision. The prefix and its consumers are revalidated before sending a prepared request and again before completion. Final writes recheck claim expiry and current live ownership after lock waits. Only an exact authenticated acknowledgement promotes `reserved_through` to `completed_through`. Lost replies or cancelled tasks retain the floor and charges until reconciliation.

Both command and upload admission use `completed_allocation_history`, a shared view that checks the retained acknowledgement against its prefix, domain, boot and allocation ownership. Mismatched metadata remains charged or fails closed. Command accounting excludes only verified completed IDs. Upload accounting excludes those IDs from global/project/allocation operation slots and allocation declared-byte budgets; source bytes still require independent `source_retired_at` evidence. No subtractive counter is decremented, so retries cannot double-refund. Larger pending prefixes remain charged while earlier completed prefixes remain usable.

## Operator activation

Add `--retire-history` to an already configured `sandbox-controller`. The worker has its own task and cycles through live commands, live files, released commands and released files, independently of lifecycle maintenance and output archival. With `--once`, it runs all four ticks. Database, API, controller and host versions must support migration 0016 and both retirement RPCs; mixed old admission writers are unsupported because they do not obey reserved ID floors.

Configure the separate [response retention and payload compaction](operation-retention.md) and [output retirement](output-storage.md#storage-retirement) policies for commands, and [source cleanup](file-transfer.md#source-cleanup-worker) for uploads. The history worker does not shorten those policies or delete storage objects. Without consumer-retirement evidence, capacity remains reserved. Global upload accounting still scans retained metadata; no large-fleet throughput claim is made.

## Evidence

The [destruction accounting evidence](evidence/2026-09-22-released-history-accounting.json) records the final workspace checks, real API/controller destruction-and-restart case, quota regressions and remaining lifecycle limits.

The [database retirement evidence record](evidence/2026-09-22-database-history-retirement.json) includes matching source and Linux binary hashes, final validation scope and limitations.

The [host retirement evidence](evidence/2026-09-22-host-history-retirement.json) records source/binary hashes, authenticated microVM tests, failure corrections, excluded cases and the limits of these claims.

The [recorded development evidence](evidence/2026-09-22-guest-history-barriers.json) contains matching source and Linux binary hashes, test totals and limitations.

The [protocol tests](../crates/sandbox-protocol/src/history.rs) cover binding compatibility, scope/domain validation, inclusive ordering and fail-closed downgrade. The unprivileged Linux [file tests](../crates/sandbox-guest/tests/files.rs) cover full descriptor/byte reservations, repeated retirement, admission after recovery, unchanged published files, unresolved prefixes, pre-barrier write failure, partial deletion, failed unlink, malformed bindings and symlink safety.

The opt-in [guest-runner tests](../crates/sandbox-guest/tests/linux_runner.rs) run real commands in a dedicated Linux VM with cgroup v2. They cover future-deadline replay rejection after receipt/output removal, process-tree cleanup, exhausted output reservations, newer active work, unknown outcomes, wrong ownership, failed barrier persistence and partial-deletion recovery. Crash windows are represented by controlled on-disk states and failed filesystem operations; this is not a power-loss/filesystem certification test.

Run the ordinary affected tests on Linux, and run the privileged tests only inside the dedicated development VM:

```sh
cargo test -p sandbox-protocol -p sandbox-guest
sudo env HUDSON_GUEST_TEST_VM=1 cargo test -p sandbox-guest --test linux_runner -- --ignored --test-threads=1
```

The supervisor client tests reject altered acknowledgement scope/prefix and lost responses. Host state tests cover durable intent, stale claims, unknown outcomes, domain separation, retained capacity, failed journal writes and completion. Opt-in supervisor microVM tests exercise the authenticated RPC, full command/file slot recovery, delayed requests, same-epoch retries after destruction, host restart, malformed retained completion, staging refusal, failed host persistence, and an enlarged guest barrier. Direct guest calls model an acknowledgement lost before host completion; this is not packet-loss injection at every network boundary.

Hosted PR runners compile but do not execute the privileged suite. These checks establish guest and host behavior in the tested development configuration. The database and controller tests additionally cover admission ordering, exhausted slot/byte refunds, boot binding, stale claims, corrupt evidence, consumer retirement and unchanged retries. A controlled real API/controller/Firecracker case fills 32 command slots, blocks while output is unexpired, retires through the automatic worker, executes another command and confirms teardown. File database tests use explicitly simulated observations; the separate real host/guest RPC case exercises file pruning. These checks do not establish supported x86_64 release readiness or hostile-workload isolation.

## Verifying retained storage fences

Offline recovery can use `ArtifactStore::verify_reference` and
`SourceStore::verify_reference` to verify the full pinned payload, or
`verify_retirement` on either store to verify the exact retained retirement
receipt. These methods perform only bounded reads, return no payload and do not
extend customer read/write deadlines. The expected owner, plan and receipt must
come from the trusted recovery ledger. Use storage credentials restricted to
GET/HEAD for an audit.

Retirement verification requires the current marker's complete content, ETag
and version to match the saved receipt and the previously deleted version to
remain absent. Missing markers, restored payloads and identically recreated
markers with a different version fail without repair. A cleanup call is not an
audit: it may install a marker or delete an old version. Keep workers stopped
and storage quiescent throughout recovery verification. Passing an audit does
not fence old writers, authorize replay, or permit marker deletion. The
[storage regressions](../crates/sandbox-artifacts/src/retirement/tests.rs) and
[source regressions](../crates/sandbox-artifacts/src/sources/retirement/tests.rs)
cover missing/changed fences, payload corruption, ownership and capacity bounds,
plus exact marker identity and incomplete deletion against private MinIO.

## Whole-allocation lifecycle

The [whole-allocation retirement protocol](allocation-retirement.md) now has database preparation, host launch fencing, consumer closure and recoverable guardian metadata deletion components. The database retains exact metadata completion and the forgetting store/RPC handoff removes bounded host records. An opt-in [controller worker](allocation-retirement.md#automatic-controller-worker) now drives this handoff. Only the authenticated metadata-retirement RPC may remove verified guardian metadata; the subsequent forgetting RPC may remove protocol records after database acknowledgement. Manual deletion does not establish cleanup or restore admission capacity.

The [allocation forgetting handoff](allocation-retirement.md#database-and-host-forgetting-handoff) now provides store/RPC primitives for bounded host-record removal after retained metadata completion. Existing domain-history and previous-epoch callers must not reinterpret retired denial as release proof. The opt-in controller now dispatches these handoffs; the sustained real-execution reuse gate remains unfinished.

The [merged sustained-reuse PR](https://github.com/hudson-frontier/hudsonbox/pull/105) reports a 1,025-cycle controlled run at an earlier head and explicitly separates it from later lease fixes. Subsequent regressions preserve the confirmed boot deadline, reject shorter fake-host requests and compare RPC millisecond precision. Current hosted checks cover those software paths; the full sustained controlled run has not been repeated on the final source. Keep that acceptance gate pending until the controlled host is available and exact sources/artifacts and cleanup observations are recorded.

Per-operation output/source retirement retains permanent fencing markers. The allocation worker does not introduce time-based marker deletion, complete database-history garbage collection, backup/restore support or supported-host qualification. Marker lifecycle and current-source reuse/cleanup acceptance remain under #79. Dated guest/host/database evidence continues to describe its original revision and tested development environment.
