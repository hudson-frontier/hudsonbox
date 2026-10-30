# Sandbox lifecycle and recovery

Status: create, execute, cancellation, files, destroy and scoped recovery paths are implemented with controlled development evidence. Pause/resume and the complete supported-host failure matrix remain unfinished. This document owns state transitions, completion evidence, pause/resume, deadlines, cancellation, and recovery. [API contract](api-contract.md) owns client retries and HTTP behavior; [data models](data-models.md) owns persisted fields and constraints.

Implemented ownership includes [controller claim storage](../crates/sandbox-store/src/claims.rs), its [PostgreSQL concurrency/recovery tests](../crates/sandbox-store/tests/claims.rs), and [reservation and placement storage](data-models.md#implemented-reservation-and-placement). The [controller](controller.md) persists dispatch and stop intent, communicates over mTLS, and reconciles simulated and real-host observations. The [real supervisor](real-supervisor.md) links controlled microVM execution and recovery evidence; complete supported-host isolation validation remains open.

## Identity through a sandbox session

The short names in this table are explanatory aliases, not API IDs.

| Event | Sandbox | Operation | Allocation/generation | Snapshot |
| --- | --- | --- | --- | --- |
| Create | S, newly assigned | Create operation | A, generation 1 | None; boots immutable image I |
| Execute a script | S | Execute operation E | A, generation 1 | None |
| Retry the same HTTP execute request | S | The same E | The same A | None |
| Pause | S | Pause operation P | A is frozen, then released | Snapshot Q is published |
| Resume | S | Resume operation R | B, generation 2 | Restore Q |
| Continue the previously frozen script | S | The same E | B, generation 2 | Q remains immutable |
| Execute a new script | S | A new execute operation | B, generation 2 | No new snapshot automatically |
| Destroy | S, tombstoned | Destroy operation | B is released | Resume disabled; retention policy applies |

An execute operation that spans pause/resume keeps one ID and its original absolute deadline. While paused, retain nonterminal status `running` with progress phase `suspended`. Reattaching it on a new allocation is not a new execution request. Store dispatch/reconnect receipts per allocation, preserving the earlier history rather than rewriting it. If the deadline expires while paused, enforce cancellation before allowing that restored process to continue; restoring memory must not reset its timeout.

A new resume allocation receives a new ID and generation before it can run. If an allocation attempt fails, its generation is not reused. Retrying transport to the same allocation retains its ID; allocating a replacement requires the old incarnation to be stopped or fenced first.

## States and transition rules

Persist desired state, last confirmed observed state/time, and the active transition separately. An intent is not evidence that a VM started or stopped. These are proposed public lifecycle states; concrete database enums remain migration work.

| Current condition | Accepted action | Required completion evidence |
| --- | --- | --- |
| New identity | Create → `creating` → `running` | Reserved allocation and confirmed guest readiness |
| `running` | Execute; state stays `running` | Separate command receipt/result |
| `running` | Pause → `pausing` → `paused` | Published snapshot and confirmed allocation release |
| `paused` | Resume → `resuming` → `running` | New allocation and acknowledged controlled release of eligible processes |
| `running` or `paused` | Destroy → `destroying` → `destroyed` | Execution prevented, prior VM stopped/fenced, allocation released; retained-byte cleanup reported separately |
| `paused` + pause; `running` + resume; `destroyed` + destroy | Completed no-op operation | Confirm current state; no new snapshot or VM |
| Any lifecycle transition active | Reject a conflicting new transition | Return the existing authorized operation; exact retries resolve first |
| Error or unknown observation | Reconcile before admitting ordinary work | Recover last confirmed facts; do not infer safe replacement from desired state |
| `destroyed` | Inspect tombstone only; execute/resume prohibited | Identity stays permanently retired |

The [implemented destroy path](controller.md#destroy-admission-and-cleanup) can take cleanup ownership from an unknown create while preserving its uncertain history. Destruction from an error/unknown state must acquire lifecycle ownership and use the same stop/fencing procedure. It cannot free an uncertain reservation on assumption. Safety cleanup may run independently of a caller's lost connection or revoked credential.

Serialize lifecycle transitions and workspace-mutating commands initially. Reject execution during create, pause, paused, resume, destroy, and unresolved error/unknown states. An execute operation is not the lifecycle lock: a running command may be frozen by a separate pause operation.

## Operations and controller ownership

Admit a request by transactionally inserting its operation and updating the relevant desired state. The operation table is also the initial pending-work queue; an in-memory notification may accelerate discovery but cannot be the only delivery mechanism.

Controllers claim work with bounded leases, monotonically increasing claim revisions, and conditional database updates. Multiple replicas must not own the same transition concurrently. On controller restart or lease expiry, a new owner first reconciles receipts and host observations, then continues only if safe. Metadata writes and supervisor requests validate the current claim revision as well as the allocation generation, rejecting an old controller even when the VM allocation has not changed.

The implemented storage primitive claims one supported operation kind at a time using `FOR UPDATE SKIP LOCKED`. PostgreSQL time determines eligibility and expiry. Leases are bounded to 1–300 seconds; deferral delays to 1–3600 seconds. Renewal and deferral require both the current revision and an unexpired lease. An expired owner cannot renew even before replacement. Reclaim preserves the phase, receipts, deadline, and unknown outcome; claiming itself does not increment dispatch attempts. Deferral releases controller ownership only, never VM resources or reservations. These bounds govern operation storage calls. The controller also has independent [allocation maintenance claims](controller.md#allocation-maintenance); the separate [allocation guardian](allocation-guardian.md) has Linux/KVM expiry and process-death evidence, while integrated host partition and replacement fencing remain unverified.

Persist bounded attempt counts, deadlines, next retry times, and terminal errors. Distinguish queued, running, succeeded, failed, cancelled, and unknown outcomes. A lost network response does not establish failure. Requests return a durable operation handle; clients inspect status or reconnect to progress streams without keeping the original HTTP request alive.

Do not store credentials or large streams in operation records. Store artifact references, digests, bounded metadata, and redacted diagnostics. Apply access control, encryption, retention, and deletion policies to both records and artifact contents.

Operation statuses are `queued`, `running`, `succeeded`, `failed`, `cancelled`, and `unknown`. `unknown` requires recorded reconciliation before becoming confirmed. Detailed `phase` values describe intent/receipt boundaries without adding new top-level statuses. For an execute suspended inside a snapshot, preserve `running` with phase `suspended`, the same operation ID, and its original absolute deadline.

## Create and execute

1. **Client → API:** request a sandbox with an allowed image digest, resource limits, and an idempotency key. Authentication determines its project.
2. **API → PostgreSQL:** validate access and request limits, then create the sandbox and create-operation records in one transaction. Return their IDs. A retry with the same key and payload returns the same operation.
3. **Controller → PostgreSQL:** claim the operation, select an eligible host, recheck quotas/capacity, and reserve an allocation with a new sandbox generation.
4. **Controller → supervisor:** send the allocation, image digest, limits, operation identity, and ownership revisions. The supervisor rejects stale ownership, prepares networking/storage, verifies the image, and starts Firecracker through the jailer.
5. **Supervisor → controller → PostgreSQL:** confirm guest readiness. The controller records the running sandbox and successful create operation. A start request alone is not proof of readiness.
6. **Client → API:** request execution. The API creates an execute operation. The controller dispatches it to the sandbox's existing allocation; executing a command does not allocate another VM.
7. **Guest → supervisor → storage/controller:** run the command, capture bounded output, upload stored outputs, and report receipts. The controller persists result metadata and output references on the operation.
8. **Client → API:** inspect the operation and retrieve authorized output bytes by operation and output name. Large output bytes remain in object storage, not database rows.

The public API is HTTP/JSON. The controller reaches the supervisor over gRPC with mutual TLS, and the supervisor reaches the guest agent over vsock with length-prefixed protobuf. Customer code runs inside the guest, and the guest receives no PostgreSQL or object-storage credentials from this design.

A disconnected client does not cancel admitted work or extend sandbox lifetime. Do not dispatch again just because acknowledgement was lost. Record process exit code, signal, timeout, cancellation, infrastructure failure, or unknown outcome separately; a zero exit code does not establish business success.

## Pause

1. The API records a pause operation. The controller serializes the transition and prevents new command dispatch for that sandbox.
2. The controller reserves a snapshot ID, staging bytes, and an upload slot for this pause operation. The guest agent freezes all customer process groups while keeping its management process separate; the supervisor then quiesces/freezes the VM and captures matching memory, disk, and VM state. This saved process freeze is required for controlled resume.
3. The supervisor uploads the components to private object storage and returns verified object references and digests. Incomplete uploads cannot be published as resumable snapshots.
4. The controller verifies the complete manifest and transactionally publishes its snapshot metadata and the sandbox's current snapshot reference in PostgreSQL.
5. After publication, the supervisor stops the VM and reclaims its compute/network and allocation disk resources. The controller records confirmed allocation release and marks the sandbox paused. Snapshot staging cleanup is tracked separately until its files are deleted; those bytes stay reserved meanwhile.

The sandbox row and its ID remain. Its old allocation becomes released, so the host's capacity can serve another sandbox. The snapshot's bytes remain in object storage. Freezing Firecracker alone does not release compute.

Reserve staging bytes and an upload slot before freezing; while waiting, prevent new command dispatch but leave existing guest work runnable. Full snapshots are the initial format. Differential snapshots, lazy loading, and warm pools are future optimizations, but the manifest and object layout must keep them reachable from the start: see [performance](performance.md#constraints-on-designs-we-are-choosing-now) for the constraints that apply to the first implementation.

A failed upload leaves the operation incomplete and the original VM's actual phase recorded. Continue safely or explicitly roll back to running under current policy; do not delete the only viable state and report successful pause. Rollback after publication must clear any current-pause pointer that would otherwise misrepresent the now-running VM; the historical snapshot remains immutable. Do not reuse an old snapshot to claim a fresh pause succeeded.

## Resume

1. The API records a resume operation and pins the sandbox's current published pause snapshot. It does not choose an arbitrary older snapshot.
2. The controller confirms the previous VM cannot still execute, chooses a compatible host with space, and creates a new allocation ID and increasing generation.
3. The supervisor restores the disk and memory with the VM initially paused and host egress blocked. The snapshot already contains frozen customer process groups; the guest agent is outside those groups.
4. The supervisor resumes the VM so the guest agent can reconnect, while customer processes remain frozen. Authenticate the new management session and bind it to the current allocation/generation. Old connections are not reused; Firecracker resets vsock connections during snapshot/resume. See [Firecracker snapshot support](https://github.com/firecracker-microvm/firecracker/blob/main/docs/snapshotting/snapshot-support.md#vsock-device-reset).
5. Refresh management credentials, synchronize time as required, apply current project/network policy, and reconcile original command deadlines/cancellation. Arrange termination of expired or cancelled process groups before they can execute again. No customer process is released until the handshake is acknowledged; if the gate cannot be proven, stop/fence the partial restore and report failure or uncertainty.
6. With the current controller claim, supervisor epoch, and allocation generation validated, release only eligible customer process groups. Persist the release acknowledgement and record readiness and successful resume. A lost acknowledgement requires reconciliation, never another restore running alongside this one.

The guest image must separate the agent from customer processes: the agent runs in its own PID namespace and `system` cgroup, and its control socket is unreachable from the workload namespace. Reject resumable images that cannot meet this contract. A frozen whole VM cannot run the reconnect handshake; the separate guest process gate is what makes that handshake possible.

That separation is hardening, not a boundary. [Decision 0003](decisions/0003-guest-root-with-our-kernel.md) gives customers root in their own sandbox, so a hostile customer can attempt to kill or impersonate the agent. Two rules follow, and both are requirements rather than best effort. A resume whose handshake cannot be proven — no agent, no response, or a response that fails validation — stops and fences the restore and reports failure; it never releases customer processes on the assumption that silence is benign. And a sandbox whose agent disappears mid-execution is failed and reported as failed, never recorded as a sandbox that completed or that ran without consuming its deadline.

```text
Before pause: Sandbox S → Allocation A → Host 1
Paused:       Sandbox S → Snapshot Q in object storage; no live allocation
After resume: Sandbox S → Allocation B → Host 1 or another compatible host
```

An already-running script resumes under its original execute-operation ID. We do not submit it again. A host crash does not guarantee recovery of work since the last snapshot, and the service never silently restores old state that could repeat external side effects.

Moving a sandbox's memory to another host means fetching all of it before any customer process runs, and that transfer dominates resume latency. Design the restore path to read ranges from published components rather than downloading whole objects, and instrument each restore stage separately. [Performance](performance.md#the-dominant-cost-is-snapshot-bytes) owns the budgets and the reporting rules.

Publish and test a compatibility matrix covering CPU architecture/model, host and guest kernels, guest agent, and Firecracker versions. Existing network connections and credentials are not assumed valid after restore. No filesystem-only fallback can claim memory continuation. A failed partial restore must be stopped/fenced before replacement; its generation is consumed even if readiness was never reached.

## Deadlines and cancellation

Persist sandbox and command wall-clock deadlines outside snapshots. Pause does not extend them. Separate active-compute/idle limits from paused-snapshot retention. Initial defaults: an execute operation deadlines at 15 minutes unless the caller supplies one, the maximum any caller may request is 6 hours, and a sandbox with no activity for 1 hour is acted on by the idle policy below. Apply current access/network policy and terminate expired/cancelled processes before any customer execution is released on restore.

Cancellation is an idempotent operation referencing its target. A request to cancel is not confirmed cancellation. A running process must have confirmed process-tree termination; a lifecycle operation must reach a safe stop or rollback boundary. If a snapshot is already published, resolve stop/cleanup ownership before reporting cancellation. Cancellation while paused must durably record the decision for enforcement before thaw; do not claim physical process termination without evidence.

Timeout or cancellation does not roll back external side effects. Revoking a credential blocks new requests/new execution dispatch under it but does not undo already-executed work. [Auth design](auth-design.md) owns credential/session policy; service authority continues required stop and cleanup.

## Idle limits and automatic pause

This section applies once pause ships; [roadmap](roadmap.md#implementation-phases) places that in Phase 3. Paused sandboxes cost storage; running idle sandboxes cost a host's CPU, memory, and disk. Reclaiming idle compute automatically is the point of having pause at all, so the policy belongs in this contract rather than arriving later as operational improvisation. Until pause exists, an idle sandbox can only be destroyed, and that outcome is reported as destruction rather than dressed up as saved state.

Idle means: no running execute operation, no attached output stream, and no client request against the sandbox, for one hour. Those three signals are authoritative. Guest-internal activity is deliberately excluded — a sandbox busy-looping with no operation attached is idle by this definition, because the service cannot observe intent inside the guest and must not infer liveness from CPU use.

An automatic pause is an ordinary pause operation with `initiator_kind` `service`. It takes the same snapshot verification, publication, and release evidence path as a caller's pause, appears in the sandbox's operation history, and is visible to the owning project. A policy-driven transition that skipped those checks would be a second control path, which [architecture](architecture.md#purpose-and-ownership) rules out.

Automatic pause never implies automatic resume. The next authorized resume request restores the sandbox; the service does not speculatively restore state on a caller's behalf.

Separate the timers. An active-compute idle timeout decides when a running sandbox is paused. A paused-retention timeout decides when its snapshot expires and resume becomes impossible. A sandbox lifetime deadline decides when the identity is destroyed regardless of state. Each needs its own configured default and its own project-visible value, and expiry of the second must be distinguishable in the API from expiry of the third.

Until pause ships, the idle policy destroys the sandbox and reports it plainly as destruction. There is nothing to pause to, and a sandbox that was thrown away must never appear in the API as one that was saved. Where a hard limit forces termination without saving state, that outcome is reported as such. Failure to save state is never presented as a completed pause.

## Destroy and recovery

Destroy records an operation, prevents new execution/resume, and stops the VM if one exists. Only confirmed release makes its capacity available again. The sandbox becomes a permanent tombstone; snapshot/output cleanup follows retention policy and is tracked independently of compute release.

If the API or controller restarts, pending operations and reservations remain in PostgreSQL. A replacement controller acquires a new claim and reconciles supervisor receipts before continuing. Host supervisors enforce local leases and deadlines. Unknown execution outcomes are reported and investigated through reconciliation rather than blindly rerunning customer commands.

| Interrupted boundary | Persisted evidence | Recovery action |
| --- | --- | --- |
| Create reserved, readiness unknown | Operation, allocation/generation, dispatch receipt | Inspect that incarnation; adopt confirmed readiness or confirm teardown before replacing |
| Execute dispatched, result missing | Stable operation ID and dispatch/acknowledgement receipts | Query guest/supervisor state; return unknown if unprovable, never blindly execute again |
| Pause upload incomplete | Snapshot ID, upload attempt, staging reservation, guest freeze phase | Reconcile the original VM and uploads; continue safely or explicitly roll back the freeze under current policy |
| Snapshot uploaded, publication uncertain | Immutable manifest/digests and snapshot record | Verify and publish once; object presence alone is not readiness |
| Snapshot published, VM stop unconfirmed | Published snapshot and unreleased allocation | Finish stop/fencing and resource cleanup; do not start a replacement yet |
| Restore started, guest handshake incomplete | Pinned snapshot, new allocation, handshake phase | Keep customer processes frozen; reconnect or confirm teardown, never release them speculatively |
| Customer processes released, readiness reply lost | Release receipt bound to allocation/generation | Reconcile the same VM and record outcome; do not restore a second copy |
| Destroy stopped VM, cleanup incomplete | Destroy operation, stop evidence, pending cleanup references | Continue cleanup; retain outstanding disk reservations and destruction tombstone |

Persist intent before each external action and confirmed evidence afterward. Every transition validates the current claim revision and allocation generation. Failure-injection tests must interrupt every applicable boundary before that capability ships. The first runtime covers create, execute, and destroy; pause/resume must pass the additional snapshot and restore boundaries before its later delivery gate is accepted. See [roadmap](roadmap.md).

## Host leases and capacity recovery

The service needs focused loops for pending operations, host health/capacity, expired allocations, snapshot progress, and orphan cleanup. [Allocation maintenance](controller.md#allocation-maintenance) now renews confirmed running allocations and admits service-owned destruction after matching same-epoch release evidence or policy expiry; verified previous-epoch cleanup now reconciles retained ownership; snapshots and general orphan cleanup remain unfinished. It does not need a general workflow framework. PostgreSQL is their durable source of intent; the host supervisor supplies observations and enforces local deadlines.

Allocation generations and expiring leases prevent stale commands and identify current owners. A partitioned host must stop its VMs when its local lease watchdog expires. A generation change in PostgreSQL alone does not stop execution on a disconnected machine. Replacement requires confirmed termination, infrastructure fencing, or a validated lease-expiry mechanism.

Enforce CPU, RAM, disk, output, execution-time, and concurrency limits outside guest control. Keep a bounded number of pending operations per project and reject or queue capacity shortages explicitly. Expiry policy may pause or destroy a sandbox, but failure to save state must be visible; any hard-limit forced termination must be reported as such.

Automatic retries are appropriate only when receipts and operation semantics make them safe. Arbitrary commands can have external side effects, so this service does not promise exactly-once execution. A caller timeout is not cancellation, and a requested cancellation is not proof that execution stopped.

Stop admitting new allocations on draining hosts. Keep existing work reachable while safely saving/stopping it; a completed maintenance drain requires confirmed release of active allocations and outstanding snapshot work, not a service restart or a desired-state update. Snapshot staging remains accounted for until actual cleanup or host storage retirement.

## Acceptance checks

The [create controller tests](../crates/sandbox-controller/tests/create.rs) cover the implemented create subset using a fake host. The remaining contracts, including real runtime behavior, require tests for:

1. Create → execute → pause → release compute → resume → destroy without Hudson or Temporal.
2. Memory, files, process identity, and the original command deadline survive a completed pause/resume.
3. Every interrupted boundary in the recovery table reconciles without duplicate commands or VMs.
4. Concurrent lifecycle requests serialize; paused/running no-ops preserve allocations and snapshot identity.
5. Snapshot publication never precedes complete verified bytes, and pause completion never precedes release evidence.
6. Customer processes stay frozen until current policy, credentials, deadlines, and the management handshake are applied.
7. Disk/upload reservations survive partial failures; stale controller claims, supervisor epochs, and allocation generations cannot release or mutate current ownership.
8. Cancellation, expiry, credential revocation, and host loss preserve honest outcomes and continue necessary cleanup.
9. Incompatible snapshots fail explicitly; unknown external effects are never silently replayed.

## Open decisions

Supported host and guest versions are in [supported configuration](compatibility.md); deadline, idle, and output defaults are settled above. Still open: filesystem quiescing and the process-freeze implementation, lease-watchdog timing and proof, and a precise cancellation outcome for each phase. Validate those mechanisms on Linux/KVM before claiming these contracts are implemented. See [roadmap](roadmap.md) for delivery gates.
