# Bounded allocation authority model

Status: implemented state model, database serial issuance and opt-in fresh-host registration with guardian enforcement. The authenticated whole-allocation forgetting handoff is implemented; opt-in controller dispatch is implemented, while legacy migration remains unfinished. [The protocol module](../crates/sandbox-protocol/src/allocation_authority.rs) and [its tests](../crates/sandbox-protocol/src/allocation_authority_tests.rs) explore the replacement authority required for whole-allocation reclamation in [issue #79](https://github.com/hudson-frontier/hudsonbox/issues/79). Host/guardian tombstones remain mandatory until the exact completion and forgetting handoff succeeds.

## Serial registration and retained owners

A trusted database issuer assigns a positive host-local serial to each immutable allocation identity: host, project, sandbox, allocation, create operation, generation and original supervisor epoch. This is an internal ordering value, not a timestamp, bearer credential or customer-controlled ID. Serial values are bounded by the signed 64-bit database range and never wrap.

The model registers contiguous batches of at most 32 permits, starting immediately after its durable high-water mark. Registration retains each owner explicitly before Create can run; registered launches may then arrive in any order. A missing batch, mixed old/new batch, duplicate owner, duplicate create operation, duplicate sandbox generation or capacity overflow fails without changing state. A wholly retained exact retry does not reactivate fenced entries.

The authority retains at most 1,024 entries, including outstanding retirement/completion records. After a completed entry is forgotten, its serial stays below the high-water mark and cannot be registered again. Older live allocations remain explicit exceptions, so they do not pin newer retired entries. No permanent record per deleted allocation is needed in this model.

The trusted issuer must never assign another serial to an already issued allocation/create identity, including after its host entry is forgotten. Host state cannot prove that global uniqueness after deleting the identity; the project-lifetime database record must enforce it. This remains an integration requirement, not behavior supplied by the model.

## Retirement stages

| State | Launch allowed by model | Required external evidence before next transition |
| --- | --- | --- |
| Active | Yes, only for the exact registered permit | Database preparation freezes owner and all consumer closure before fencing |
| Fenced | No | Persistent cross-process launch denial, verified original-owner release, completed and synced metadata deletion |
| Complete | No | Durable database acknowledgement of the exact retirement identity |
| Forgotten | No; lookup returns `Closed` | No new admission or release inference is permitted for that serial |

A retirement ID stays fixed across retries. Another ID cannot replace it, completion cannot skip fencing, and forgetting cannot skip completion. A late fence retry cannot downgrade a completed record. All three retained stages consume capacity; capacity returns only when the completed record is forgotten. There is no unfence method.

`Closed` means denied by serial ordering. It does not establish that the supplied owner ever existed, that a VM stopped, or that an object was deleted. After forgetting, even a different owner presented at the same serial receives denial; the original owner is deliberately no longer available as evidence. Callers must use their retained database results and cannot translate this response into `Released` or `FencedAbsent`.

## Transactional database issuance

[Migration 0017](../migrations/0017_allocation_serials.sql) adds a host-local counter and retained `allocation_permits` rows. [Create reservation](../crates/sandbox-store/src/placement.rs) increments the counter under its existing host row lock and inserts the exact allocation/create identity in the same transaction as capacity reservation, sandbox binding and the final operation-claim check. A failed statement, expired claim or rollback consumes neither the reservation nor the serial. Exhaustion fails placement without wrapping. An existing reservation is reconciled without assigning another serial, even after the host epoch changes.

The table retains one permit per allocation and per create operation, and unique host/serial and sandbox/generation bindings. Original permit rows outlive host metadata reclamation; there is no update or deletion worker. No PostgreSQL sequence or issuance trigger is used. Older writers that omit permits must not participate in authority activation.

The [batch reader](../crates/sandbox-store/src/allocation_permits.rs) takes a shared host lock, reads at most 32 consecutive serials and verifies each retained owner against its allocation and create operation. Missing rows, changed scope and a requested frontier above the issued counter fail. Batches include the issued frontier and an explicit `has_unissued_allocations` flag. The reader does not authenticate a host or acknowledge registration; its `after` value must ultimately be derived from verified host evidence.

Migration preserves legacy allocations unchanged and assigns them no synthetic permit. An existing legacy reservation stays unissued on retry. Before new authority activation, every such owner must undergo a separate exact-owner migration; `has_unissued_allocations` must block activation. This remains unfinished. A reader returning an empty batch does not prove that all original owners are registered.

[Database tests](../crates/sandbox-store/tests/support/allocation_serials.rs) exercise concurrent reservations, original-epoch retries, failure after issuance, counter exhaustion, malformed/gapped ownership, bounded batches, host separation and migration of an existing unissued allocation. These establish issuance behavior; they do not register permits with a real supervisor.

## Persistence boundary and integration still required

The model is deliberately in memory. Transitions must be written durably before acknowledgement or effects; mutating the value does not write a journal, hold a guardian lock or verify cleanup. Serialization validates version, host, serial bounds/order, unique retained identities, UUID variants, state shape and a 1 MiB input limit. Unknown fields and duplicate fields fail closed. A caller supplies an independently retained minimum high-water mark when loading. That check can detect a lower frontier, but cannot detect every self-consistent rollback or prove that a serialized completion is true.

Runtime adoption still requires all of the following:

- Migration of all legacy owners and orchestration of issued serials through registered/fenced authority, including cancelled creates. Issuance now rolls back with reservation, but every committed serial must still be registered or fenced before later registration can pass it.
- Ordered, authenticated registration and exact acknowledgement recovery before dispatching Create. An interrupted batch cannot silently discard an admitted operation; registration state must reconcile against original database identities.
- A durable host authority and persistent cross-process lock shared by admission, guardian prepare, launch and namespace initialization. A check followed by launch outside that serialization is unsafe.
- Versioned manifests and host journal integration, fail-closed downgrade, rollback protection, missing-authority handling and a migration that retains every legacy owner until its replacement authority is committed.
- Database consumer closure, verified physical release, recoverable owned-file deletion, exact completion acknowledgements and acknowledgement retirement. The state model accepts trusted transition calls; it does not validate external observations.
- Independent storage-writer fencing before deleting permanent object-store markers. This serial authority only concerns allocation admission and launch.

Do not use a missing authority file as permission to call `Authority::new`, delete a directory after an in-memory `fence`, or treat successful decoding as cleanup evidence. No runtime path currently does any of those things through this module.

## Validation scope

The model suite runs 4,096 newer allocation registrations/retirements while the oldest permit remains active, serializing and reloading at retirement boundaries. It also fills all 1,024 slots, confirms that fenced/completed records remain charged, frees an interior slot and admits newer work without reopening it. Other tests exercise out-of-order launches after registration, changed identities/intents, registration atomicity, serial exhaustion, malformed storage, duplicate fields and rollback below an independent frontier.

These are deterministic state-machine tests on the Mac, not 4,096 microVM lifecycles or crash/power-loss tests. No supported-host security claim follows from them. The [whole-allocation retirement protocol](allocation-retirement.md) now has separate controlled DB/RPC evidence for deletion and forgetting. Its automatic dispatch and sustained real-execution reuse gate remain incomplete.

## Fresh-host registration and launch

Set `launch_permits_required: true` in the root-owned host configuration only when provisioning a fresh host state directory. Startup initializes guardian authority and persists its checkpoint in the host journal. Existing legacy journals cannot be upgraded by toggling this setting, and an enabled journal cannot downgrade to legacy mode. Missing or corrupt activated state fails closed. Interrupted initial provisioning may require operator investigation; startup does not reinterpret partial state as a fresh host. Old binaries must be quiesced before provisioning.

`Health` reports whether permits are required and verifies the retained authority before declaring that host healthy. The authenticated controller uses `AllocationAuthority` to inspect progress with empty `permits_json`, then register at most 32 contiguous immutable permits in a bounded JSON array. The host acknowledges only after the authority and independent host-journal checkpoint are durable. A lost reply is reconciled by inspection on the next attempt. Inspection does not grant launch or prove cleanup.

[Migration 0018](../migrations/0018_allocation_registration.sql) records the required mode and acknowledged frontier in the database. The controller rejects a lower frontier, disabled mode after activation, wrong host/epoch or progress beyond issued serials. Unissued legacy allocations block registration. Progress survives controller restarts. Registration precedes Create dispatch intent; registration failure leaves that operation eligible for later admission. Create then carries the original exact-owner permit, and host admission and every guardian launch gate enforce it. Epoch advancement rejects old launches while retaining owners needed for cleanup.

The fake host advertises legacy simulated behavior and does not provide durable permit authority. Ordinary legacy hosts retain their existing allocation tombstones. This integration neither removes those tombstones nor calls completion/forget automatically. Capacity reclamation still requires the full [retirement protocol](allocation-retirement.md).

Controlled execution results and source hashes are recorded in [host permit registration evidence](evidence/host-permit-registration-evidence.md).

## Admission of host receipts

On permit-enabled hosts, creating any new allocation receipt requires a retained active permit, including when the first request is Inspect or Stop. The host compares the registered host, epoch, allocation, project, sandbox and generation, checks its independent registration frontier and holds the persistent authority lock through the journal write. The operation ID may differ from the original Create operation because Stop and inspection have their own operation identities.

Unknown, fenced, completed or forgotten owners cannot recreate a missing receipt or consume another host slot. Denial is not an absence or release observation. Existing exact-owner receipts remain available for inspection and cleanup after fencing; the original stopped-allocation proof is still required. Legacy mode keeps its existing receipt behavior. This rule closes one prerequisite for future metadata deletion and does not implement the deletion protocol itself.

Controlled results for this gate are recorded in [registered receipt evidence](evidence/registered-receipts-evidence.md).
