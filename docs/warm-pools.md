# Warm pools

Status: **database contract and fenced control-plane seams implemented; prepared-VM runtime not implemented.** This document owns the [#188](https://github.com/hudson-frontier/hudsonbox/issues/188) implementation of the [accepted v1 warm-pool model](data-models.md#accepted-v1-fleet-and-warm-pool-model). No readiness latency is claimed: cold/warm create-to-ready has not been measured on real Linux/KVM for this work.

Warm supervisor capacity (a ready host) is not a warm VM. A warm-pool **member** is one prepared, unused microVM reservation on one host at one supervisor epoch.

## What exists

[Migration 0030](../migrations/0030_warm_pools.sql) (0029 is reserved for the Kubernetes adapter, #197) adds:

- `sandbox_templates`: immutable versions (a trigger rejects updates and deletes) of image digest, vCPU/memory/disk, `boot_config` and `compatibility`. `publication` is `private` (owner project only) or `shared` (explicit publication); a NULL owner requires `shared` and is never an access bypass. `snapshot_id` must be NULL until durable, published, compatible snapshots exist ([#178](https://github.com/hudson-frontier/hudsonbox/issues/178)); only ordinary immutable boots are accepted.
- `warm_pool_policies`: one template version in one host pool, optional dedicated owner, `min_ready`, `max_total` (≤ 256), `max_per_host` operator budget (1–64), `idle_ttl_seconds` (60–86400), `enabled`. Template, pool and owner are fixed at creation.
- `warm_pool_members`: host, epoch, template, exact reserved size, `vm_ref`, state, lease (idle TTL), `claim_revision`, unique `claimed_allocation_id`, durable `handoff_intent`/`handoff_receipt`, `prepare_receipt` and `cleanup_evidence`. Constraints forbid a claimed member from being `ready` and require evidence for `ready`, `assigned` and `retired`.
- `capacity_operations` (#196) accepts `warm_prepare` and `warm_retire` intents with a `warm_pool_id`. Generic standalone claimers and cancellation ignore warm intents.

Store API: `sandbox_store::warm_pool`. Shared receipt types: `sandbox_protocol::warm_pool`. Driver seam: `sandbox_controller::warm_pool`.

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
2. `claim_warm_operation` claims an intent fairly, at most `MAX_ATTEMPTS` times, rebuilding a `PrepareIntent` or retire work from durable rows. `record_warm_prepared` accepts only a `PrepareReceipt` that echoes the exact member/template/host/epoch/image/size under a live claim; a host that changed epoch or stopped admitting work turns the member `retiring` instead of `ready`. `abandon_warm_prepare` retires an exhausted preparation with its reservation held because it is unknown whether a VM booted.
3. `claim_warm_member(create claim)` atomically claims one ready, authorized, compatible member on a fresh host at the member's epoch, issues the allocation and permit, persists the `HandoffIntent` (member, VM ref, allocation, project, sandbox, host, epoch, claim revision) and parks the create in phase `warm_handoff`, which the cold dispatcher refuses. Policy rows are locked in ID order, so concurrent claims take each member once. A duplicate or restarted worker gets the same intent (`Claimed::Existing`, `warm_handoff`). No ready member returns `Claimed::None`: use ordinary placement.
4. `record_warm_handoff` accepts only a receipt that echoes the intent exactly, under the live create claim, while the host is still at the intent epoch (`StaleEpoch` otherwise; the claim stays uncertain). `Adopted` marks the member `assigned` and the create `create_dispatched`, so readiness is still established by inspecting the allocation; a claim is not readiness. `Rejected` retires the member and holds the create in `warm_handoff_rejected` until cleanup evidence, then returns it to `reserved` for an ordinary cold boot of the same allocation. Exact receipt retries are idempotent; conflicting receipts are refused.
5. `record_warm_cleanup` accepts only a `CleanupReceipt` that reports destruction of the exact member, VM ref, host and epoch. Missing records or heartbeat expiry are never cleanup evidence. Customer-claimed members never return to `ready`.

## Incomplete runtime gates

These are not implemented; the database contract alone does not provide warm VMs:

- **Supervisor RPCs.** The supervisor protocol has no prepare-unused-VM, adopt-into-allocation, inspect-handoff or warm-cleanup RPC. Allocation launch permits authorize a fresh Create only; they are not adoption authority. The only production `WarmSupervisor` is `UnsupportedWarmSupervisor`, which fails closed: prepare intents retry, then are abandoned to retirement, and cleanup stays pending with reservations held. Do not enable warm policies on real hosts until these RPCs exist.
- **Identity and secret rebinding.** A prepared VM must receive project identity, credentials and network policy only at adoption, bound to the handoff intent. Not implemented.
- **Create routing.** The controller does not call `claim_warm_member`; creates use cold placement only.
- **Replenishment loop.** No daemon or operator command runs `reconcile_warm_pool` or `warm_pool::step` yet.
- **Kubernetes.** The #197 Kubernetes adapter retire path must also refuse hosts with `unretired_warm_members` once both changes land.
- **Snapshots** wait for #178. **Measurements** of cold vs warm readiness on real KVM are required before any latency claim.

## Tests

`crates/sandbox-store/tests/warm_pools.rs` (PostgreSQL; supervisor receipts are constructed by the test as a fake supervisor) covers immutability and snapshot refusal, bounded replenishment and capacity accounting, concurrent double claims, exactly-once capacity transfer on a full host, tenant/template/quota/pool denials, lost acknowledgement and controller restart, stale epochs, drain and blocked host retirement, failed and successful cleanup, rejected handoff with cold fallback, ordinary cold boot without members and fenced preparation exhaustion. `sandbox_protocol::warm_pool` unit tests cover exact receipt identity checks. None of these are Linux/KVM evidence.
