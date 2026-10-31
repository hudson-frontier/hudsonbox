# Warm pools

Status: **database contract, supervisor warm RPC protocol, controller create routing and bounded upkeep implemented; only the simulated fake host adopts. The real Firecracker supervisor fails closed because the guest bootstrap binds allocation identity at boot (see [blocker](#real-firecracker-blocker)).** This document owns the [#188](https://github.com/hudson-frontier/hudsonbox/issues/188) implementation of the [accepted v1 warm-pool model](data-models.md#accepted-v1-fleet-and-warm-pool-model). No readiness latency is claimed: cold/warm create-to-ready has not been measured on real Linux/KVM for this work.

Warm supervisor capacity (a ready host) is not a warm VM. A warm-pool **member** is one prepared, unused microVM reservation on one host at one supervisor epoch.

## What exists

[Migration 0030](../migrations/0030_warm_pools.sql) (0029 is reserved for the Kubernetes adapter, #197) adds:

- `sandbox_templates`: immutable versions (a trigger rejects updates and deletes) of image digest, vCPU/memory/disk, `boot_config` and `compatibility`. `publication` is `private` (owner project only) or `shared` (explicit publication); a NULL owner requires `shared` and is never an access bypass. `snapshot_id` must be NULL until durable, published, compatible snapshots exist ([#178](https://github.com/hudson-frontier/hudsonbox/issues/178)); only ordinary immutable boots are accepted.
- `warm_pool_policies`: one template version in one host pool, optional dedicated owner, `min_ready`, `max_total` (≤ 256), `max_per_host` operator budget (1–64), `idle_ttl_seconds` (60–86400), `enabled`. Template, pool and owner are fixed at creation.
- `warm_pool_members`: host, epoch, template, exact reserved size, `vm_ref`, state, lease (idle TTL), `claim_revision`, unique `claimed_allocation_id`, durable `handoff_intent`/`handoff_receipt`, `prepare_receipt` and `cleanup_evidence`. Constraints forbid a claimed member from being `ready` and require evidence for `ready`, `assigned` and `retired`.
- `capacity_operations` (#196) accepts `warm_prepare` and `warm_retire` intents with a `warm_pool_id`. Generic standalone claimers and cancellation ignore warm intents.

Store API: `sandbox_store::warm_pool`. Shared intent, request and receipt types: `sandbox_protocol::warm_pool`. Supervisor RPCs: `PrepareWarmMember`, `AdoptWarmMember`, `InspectWarmHandoff` and `RetireWarmMember` in [`proto/supervisor.proto`](../proto/supervisor.proto). Controller driver and create routing: `sandbox_controller::warm_pool`.

## Authorization

Policies are checked when written and claims are checked again in the claim transaction:

- The claiming project needs a `project_host_pool_access` grant for the policy's host pool.
- A dedicated policy is claimable only by its owner. A shared policy requires a shared template and a host pool whose tenancy is `shared`; a dedicated host pool cannot carry a shared policy.
- The template must be owned by the project or explicitly shared; the sandbox's image digest and vCPU/memory/disk must equal the template's exactly.
- The operation's persisted project credential, deadline and sandbox expiry are rechecked, and project quotas apply to a prepared VM exactly as to a cold boot.

## Capacity accounting

Placement capacity (`placement::held`) counts unreleased allocations plus `WARM_HELD`: `preparing`, `ready` and `retiring` members always hold their reservation; a `claiming` member holds it only after its claimed allocation is released. The claim transaction inserts the allocation (same host, epoch and size) and moves the member `ready → claiming` in one commit, so the reservation is counted exactly once with no unreserved gap. `assigned` transfers to allocation accounting. `max_total` counts preparing, ready, claiming and retiring members. Warm reservations consume host capacity before customer placement. Unknown or failed work keeps reservations until cleanup evidence is recorded; `retired` requires it.

Host maintenance reports `unretired_warm_members`. Static host retirement refuses while it is nonzero. Pool type changes are blocked while unretired members exist.

## Lifecycle

1. `reconcile_warm_pool(policy, budget)` (bounded by `MAX_BUDGET`, `MAX_PENDING` intents per policy) retires unusable unassigned members (TTL expired, disabled policy, drained/unhealthy/retired host, stale epoch, claiming member whose allocation was released, excess over a lowered `max_total`) and reserves new `preparing` members toward `min_ready` on fresh, compatible hosts with room, within `max_per_host` and `max_total`. Each member change and its capacity intent commit together; intents are keyed per member so passes are idempotent.
2. `claim_warm_operation` claims an intent fairly, at most `MAX_ATTEMPTS` times, rebuilding a `PrepareIntent` or retire work from durable rows. `record_warm_prepared` accepts only a `PrepareReceipt` that echoes the exact member/template/host/epoch/image/size under a live claim; a host that changed epoch or stopped admitting work turns the member `retiring` instead of `ready`. `abandon_warm_prepare` retires an exhausted preparation with its reservation held because it is unknown whether a VM booted. If the final claim itself is lost (worker crash or lease expiry on attempt `MAX_ATTEMPTS`), nothing can claim or abandon it again, so `reconcile_warm_pool` durably retires such a `preparing` member (`prepare_exhausted`, intent `exhausted_to_retirement`) with its reservation held; a live final claim is never swept.
3. `claim_warm_member(create claim, allowed images)` refuses an image outside the controller's allowlist before claiming (the cold path then reports `image_not_allowed`), then atomically claims one ready, authorized, compatible member on a fresh host at the member's epoch, issues the allocation and permit, persists the `HandoffIntent` (member, VM ref, template, allocation, project, sandbox, create operation, generation, permit serial, host, epoch, member claim revision) and parks the create in phase `warm_handoff`, which the cold dispatcher refuses. Policy rows are locked in ID order, so concurrent claims take each member once. A duplicate or restarted worker gets the same intent (`Claimed::Existing`, `warm_handoff`). No ready member returns `Claimed::None`: use ordinary placement.
4. `begin_warm_adopt(create claim, allowed images)` durably moves the create to `warm_adopt_dispatched` and sets the allocation's initial lease before the first `AdoptWarmMember`. Every later attempt (lost acknowledgement, controller restart, another worker) gets `AdoptMode::Inspect` and may only call `InspectWarmHandoff`; a create whose credential, deadline, sandbox expiry or image allowlist no longer authorizes it is also inspect-only, so it can never bind project identity. An adopted create skips the cold dispatch checks, so both warm steps enforce the allowlist.
5. `record_warm_handoff` accepts only a receipt that echoes the intent exactly, under the live create claim, while the host is still at the intent epoch (`StaleEpoch` otherwise; the claim stays uncertain). `Adopted` marks the member `assigned` and the create `create_dispatched`, so readiness is still established by inspecting the allocation; a claim is not readiness. `Rejected` retires the member and holds the create in `warm_handoff_rejected` until cleanup evidence, then returns it to `reserved` (clearing the allocation's adopt lease, since the rejection and destruction evidence prove no host holds it) for an ordinary cold boot or pre-dispatch rejection of the same allocation. Exact receipt retries are idempotent; conflicting receipts are refused.
6. `record_warm_cleanup` accepts only a `CleanupReceipt` that reports destruction of the exact member, VM ref, host and epoch. A retire intent without a VM ref (no prepare receipt was recorded, e.g. every acknowledgement was lost) asks the supervisor to destroy whatever it prepared for that exact member at that epoch; the receipt names the VM found (recorded on the member) or proves none was prepared. A named intent must be echoed exactly. Missing records or heartbeat expiry are never cleanup evidence. Customer-claimed members never return to `ready`.

## Supervisor protocol and adoption authority

All four RPCs carry bounded (16 KiB) JSON over the existing authenticated controller mTLS channel and answer a tagged `WarmReceipt`.

- **Prepare** (`PrepareIntent`): boot one unused member from the template at the intent's host and epoch. Idempotent on member ID; changed input is refused. The member holds no project identity, credential or secret, and its `PrepareReceipt` names none.
- **Adopt** (`WarmAdoptRequest`): carries the create's operation `Ownership` (claim revision and deadline), the exact `HandoffIntent`, the allocation permit and the allocation expiry. A fresh-create launch permit alone is not adoption authority: the supervisor accepts only when `HandoffIntent::admits` holds, i.e. the permit equals `HandoffIntent::permit()` (host, project, sandbox, allocation, create operation, generation, original epoch and serial) and the ownership names the same identity at the same epoch. Hosts that require launch permits register that permit through the existing allocation-authority path before adoption. Only then does the supervisor issue the per-allocation channel identity and bind the VM to the allocation; the `Adopted` receipt reports its SHA-256 (never a key) and echoes the intent digest. The adopted allocation is then inspected like a dispatched create, so readiness still comes from the ordinary `Inspect`. A member already bound to another scope is refused (`ALREADY_EXISTS`).
- **InspectWarmHandoff** never adopts. It returns the recorded receipt for the exact scope, or fences an unadopted member against that scope and returns `rejected/fenced_by_inspect`, so a delayed Adopt cannot apply afterwards. The operation claim revision is fenced as for every allocation RPC, so an Adopt from a superseded claim is refused before it reaches the member.
- **Retire** (`RetireIntent`) destroys an unassigned member at its epoch and returns the `CleanupReceipt`. It refuses an adopted member: customer-used VMs are destroyed only through their allocation.

There is no detached signature: like Create, authority is the authenticated controller identity plus the database-issued, registered permit and the exact intent digest. No signing key was introduced.

The controller (`warm_create`) tries a ready member before cold placement. No ready member, or a denial the cold path reports itself, continues the ordinary cold create unchanged. An uncertain RPC leaves the create dispatched and re-inspects; a stale epoch (host restarted) hands the allocation to ordinary owner-loss reconciliation and keeps the member's reservation held. `warm_upkeep` (opt-in `sandbox-controller --warm-pool-upkeep`) visits at most 64 policies per pass, reconciles each with a bounded budget and drives at most that many intents per policy.

The development fake host implements the RPCs in memory and marks every receipt simulated. By default it reports `launch_permits_required: false` and checks only permit/ownership/intent agreement. `FakeHost::require_launch_permits` enables an in-memory registry fed by the controller's ordinary `AllocationAuthority` registration (contiguous serials, at most 32 per batch); Create, Adopt and InspectWarmHandoff then accept only the exact registered permit, so a forged but self-consistent permit binds or fences nothing. The registry is per epoch and not durable. A restarted fake has a new epoch and refuses older-epoch requests, so it cannot supply cleanup evidence for them: those reservations stay held.

## Real Firecracker blocker

The real supervisor implements the RPCs fail-closed. Secure adoption into a running Firecracker VM is blocked by the existing guest boundary, not by this protocol:

- The guardian writes the per-allocation [bootstrap device](guest-bootstrap.md) (allocation ID, generation, channel CA and certificates, `allocation-<UUID>.sandbox.internal` server name) before launch, and the guest agent reads it once at boot. A VM booted before its allocation exists cannot receive that identity later without a guest late-binding protocol.
- The guardian's receipt, journal, lease watchdog, jail and cgroup paths are keyed by allocation ID; adopting would require re-keying a running guardian, which does not exist.

Until both exist, the real host refuses `PrepareWarmMember` (nothing boots), answers `AdoptWarmMember`/`InspectWarmHandoff` with a definitive `rejected/warm_unsupported` receipt (the create falls back to a cold boot after cleanup), and attests `RetireWarmMember` only for members that never reported a VM name. Enabling a policy on real hosts therefore produces bounded preparation retries and retirement, not warm VMs. Do not enable warm policies on real hosts.

## Remaining gates

- **Guest late binding and guardian re-key** (above). Without them, no real VM is adopted.
- **Network policy at adoption.** Network policy is bound with the allocation and is not implemented for warm members.
- **Real Linux/KVM validation.** None of this work ran on Linux/KVM; the real-host warm module compiles only on Linux and is checked there by CI. Cold vs warm create-to-ready must be measured on real KVM before any latency claim.
- **Real stale-epoch cleanup.** The controller can request verified cleanup at the host's current incarnation using the previous-epoch envelope described below. The real supervisor still rejects that envelope until its retained-state cleanup implementation exists. Unsupported, unknown and failed cleanup keep reservations held.
- **Kubernetes.** The #197/#214 Kubernetes retire path must also refuse hosts with `unretired_warm_members`.
- **Snapshots** wait for #178. No billing, pricing, plans or entitlements were added.

## Tests

`crates/sandbox-controller/tests/warm_adoption.rs` drives HTTP admission, PostgreSQL and gRPC/mTLS to the fake host: two creates adopt two prepared members with distinct identities and no cold start; a lost adopt acknowledgement is inspected after a controller restart and never replayed; an uncertain unadopted handoff is fenced, a late Adopt is refused, the member is cleaned up and the same allocation cold-boots once; adoption refuses another project's ownership, a forged permit, a rewritten intent, a stale epoch, a second scope and retirement of an adopted member; creates stay cold without ready members. The real host's fail-closed answers have a Linux-only unit test in `sandbox-supervisor`.

Review regressions in the same file: an image removed from the allowlist after preparation is denied with zero adoptions (before and after the member claim; the latter fences, cleans up and rejects the create before dispatch); prepare acknowledgements lost through exhaustion are retired and cleaned by the name the fake resolves, with the reservation held on both sides until that evidence; and on a permit-requiring fake, an unregistered self-consistent permit is denied for Adopt and Inspect while the controller-registered permit adopts once.

`crates/sandbox-store/tests/warm_pools.rs` (PostgreSQL; supervisor receipts are constructed by the test as a fake supervisor) covers immutability and snapshot refusal, bounded replenishment and capacity accounting, concurrent double claims, exactly-once capacity transfer on a full host, tenant/template/quota/pool denials, lost acknowledgement and controller restart, stale epochs, drain and blocked host retirement, failed and successful cleanup, rejected handoff with cold fallback, ordinary cold boot without members, fenced preparation exhaustion, a crash on the final preparation attempt retired by upkeep (never swept while live), and adopt-once/inspect-only dispatch including a credential revoked after the claim. `sandbox_protocol::warm_pool` unit tests cover exact receipt identity checks. None of these are Linux/KVM evidence.

## Previous-epoch unused-member cleanup

The [portable PR #216 validation record](evidence/2026-10-31-previous-warm-cleanup-validation.md)
records exact tested source, 27 focused tests, authenticated controller restart
and reporting-epoch regressions, and remaining real-host gates.

`PreviousRetireIntent` is a distinct version-1 envelope sent through `RetireWarmMember`: it carries the original `RetireIntent` unchanged plus a strictly newer `reporting_epoch`. The controller routes only to that host's authenticated current incarnation. An older supervisor rejects the envelope. A reply must be `WarmReceipt::PreviousCleanup`, echo the whole request (including operation ID and revision), and contain a destruction receipt for the original member, VM name and epoch. The store rechecks the live capacity-operation fence and locks the host to confirm the reporting epoch is still current before releasing the member reservation. This path never returns an old allocation to cold-create; allocation owner-loss reconciliation retains authority over it.

The fake's explicit `retain_warm_members_from` test helper fences the old warm handle and copies its member evidence into a newer same-host fake. Previous cleanup requires a retained record at the exact original epoch and refuses adopted members, unknown members, changed names, and wrong reporting epochs. A fresh fake with no retained records cannot attest old-member absence. This helper models retained evidence in memory; it is not durable restart or Linux/KVM evidence. Retire retries remain bounded; exhausted cleanup keeps reservations held.
