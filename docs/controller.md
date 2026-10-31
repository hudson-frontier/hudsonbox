# Create, destroy, and allocation maintenance

Status: a create/execute/destroy controller is implemented with PostgreSQL, the authenticated HTTP router, and gRPC/mTLS. The [real Linux supervisor](real-supervisor.md) now has controlled create, readiness, renewal and destroy evidence; the development fake remains available for portable tests. Public file transfer and verified previous-epoch allocation recovery are implemented; [command cancellation](command-cancellation.md) and output streaming are implemented.

## One operation through the loop

[The controller](../crates/sandbox-controller/src/lib.rs) verifies the configured supervisor's certificate through the [shared transport](supervisor-protocol.md), then checks its reported host ID, epoch, and evidence mode. It refreshes the heartbeat of that existing database host. This cannot create a host, change its epoch or capacity, or promote a draining host to ready. Host rows, epochs, capacity, endpoints, certificates, and allowed image digests are operator-provisioned for this initial integration; authenticated registration and automatic epoch issuance remain unfinished.

Each tick maintains at most one due running allocation, then claims at most one create or destroy with a 30-second operation lease. The preferred operation kind alternates between ticks, falling back to the other kind when empty. Create reserves capacity and examines persisted dispatch history. [Dispatch storage](../crates/sandbox-store/src/dispatch.rs) locks the operation, project, sandbox, and host in the same order as reservation. It constructs ownership from persisted rows, not caller-supplied copies of project or sandbox fields.

If the operation is still provably undispatched, storage rechecks the project credential, deadlines, desired state, host freshness, and the configured image allowlist. It commits the allocation's initial boot lease (at most 60 seconds, capped by the sandbox deadline), increments the dispatch attempt count, and records a create intent before returning the request to send. This gives a cold guest time to authenticate before readiness evidence is accepted; a controller crash cannot erase the intent. Subsequent extensions use a 30-second horizon. While that horizon would preserve or shorten the confirmed deadline, maintenance only inspects the live allocation. It sends no redundant renewal and leaves the guardian's monotonic watchdog unchanged.

A previous intent or unknown status selects inspection of the same allocation. It never selects another create request. A reservation without a dispatch intent can continue under a new claim because the database proves no controller following this protocol could have sent its create yet.

The controller places new work across registered hosts and routes each reservation to its persisted owner. An owner that is not currently routable is deferred for five seconds. Work remains durable when the process exits or loses its claim.

### Placement

[Placement storage](../crates/sandbox-store/src/placement.rs) (`Store::place_create`) chooses the host and persists the allocation, permit serial and `reserved` receipt before any dispatch. A scope limits the choice. `HostScope::Fleet` considers every registered host with a transport identity (`RegisteredHost::admits_new_work`). `HostScope::Configured` considers only the single-host controller's configured host and epoch. That host may still be a legacy row without a transport identity. The controller uses fleet scope when its configured bootstrap host has a registered transport; a NULL transport explicitly retains configured legacy/fake scope. Fleet failures never fall back to configured placement. Creates dispatch only to the reservation's persisted host and epoch.

Every scope is limited to the pools the project is granted in `project_host_pool_access`; see [host registry](host-maintenance.md#host-registry-and-pools). There is no fallback. A shared pool that is full does not spill into a dedicated pool, and a dedicated pool never serves another project. A candidate must be `ready` at a positive epoch and have an authenticated observation within 30 seconds. Fleet placement requires approved and observed [capabilities](host-capabilities.md) to match at the current epoch, including architecture, image and resource limits. Only configured scope preserves the explicit legacy exception for a host with empty policy and no capability observation; registering its transport alone does not qualify it for fleet placement. Its free capacity must cover the request plus runtime overhead: 128 MiB of VMM memory and 401 MiB of staged-artifact disk per allocation. These match the [real supervisor's admission](real-supervisor.md#durable-state-and-capacity). Held capacity counts every unreleased allocation, including reserved, uncertain and expired-lease ones. Unassigned warm-pool reservations are not implemented; when they are, they must be added to the same held-capacity query under the same locks. Among fitting hosts, placement prefers the one left with the most free memory, then CPU, then disk. Ties go to the lowest host ID. Project quotas count requested resources without host overhead.

Placement locks the operation, project, sandbox and then every candidate host row in ascending host ID, then rereads freshness and authorization. All placement transactions take host locks in that order, so they cannot deadlock each other. The project lock serializes quota checks, and the host locks serialize capacity checks. A drain, epoch change or capability change that commits first is re-evaluated on the locked row and excludes the host. A drain that commits after a reservation sees and reports it. A sandbox with an unreleased allocation always returns that allocation (`Existing`) for reconciliation. It never moves to another host, even when its owner is draining or stale. Locking every candidate serializes placements that share pools. This is simple and safe for current fleet sizes. Batching or sharded selection needs its own load evidence.

### Waiting and fairness

A create that fits nowhere stays queued with phase `awaiting_placement`. Its operation `error` holds a retryable reason: `insufficient_capacity`, `project_quota_exceeded`, `no_ready_host` or `no_compatible_host`. The claim is released, and the retry delay grows with time waited, from 2 to 30 seconds (`elapsed/10`). The wait is bounded at 300 seconds after admission by the database clock. After that bound, the create fails through service-owned rejection with the same code. Nothing was reserved, so nothing is freed. A successful placement clears the reason. A project with no grant for an eligible pool fails immediately with `host_pool_access_denied`; waiting would not help. Revoked credentials and expired deadlines still fail with `authorization_revoked`.

Create claims take turns across projects. A create's turn is its position among its project's unfinished creates, so one project's backlog cannot delay another project's oldest create. Other operation kinds remain oldest first. Quotas still bound how much capacity any one project can hold.
## Per-owner host routing

[Owner routing](../crates/sandbox-controller/src/hosts.rs) replaces the single configured supervisor connection ([#193](https://github.com/hudson-frontier/hudsonbox/issues/193)). Each request resolves the persisted allocation owner (host ID and supervisor epoch) from PostgreSQL, then the owner's [registry](host-maintenance.md#host-registry-and-pools) endpoint and expected transport identity. It never selects a host from caller input and never falls back to another host, endpoint or epoch.

- **Identity on every connection.** The [channel pool](../crates/sandbox-supervisor/src/routing.rs) verifies the operator CA chain, the per-host `host-<uuid>.sandbox.internal` name and the exact registered SHA-256 of the presented server leaf during each TLS handshake. A load-balanced address, a CA-valid certificate of another host or a reissued certificate not yet registered fails the handshake. Health must then report the same host ID and epoch. A host whose registry transport is NULL is addressable only when it is the controller's configured host, through the configured endpoint, with CA and per-host name verification (legacy single-host mode).
- **Ownership per action.** Dispatch transactions still build ownership from locked rows. The controller sends lifecycle, execute/inspect/cancel, upload, lease renewal/inspection, previous-allocation reconciliation, output publication, history and allocation-retirement requests only when the persisted owner equals the host and epoch it routed to; otherwise the claim is deferred or recorded `unknown` exactly as before. A lost reply keeps the existing unknown/inspect semantics on the same owner.
- **Health per host.** Each tick refreshes every registry host with a routable transport concurrently (at most 16 at once, three seconds each). A failing host is marked runtime-unknown at its epoch and excluded from routing and maintenance; other hosts continue. Allocation maintenance, history and allocation retirement rotate across healthy hosts so one backlog cannot starve another. A command admitted while its owner was unauthenticated fails closed with `execution_target_changed` and `dispatch_intent_absent` once it can be evaluated; no late dispatch occurs.
- **Bounds.** Channels connect lazily and are cached per host (at most 1,024 hosts, idle hosts evicted first). One process holds at most eight in-flight requests per host; further requests fail fast and defer. Lifecycle RPCs use a five-second deadline and publication/history RPCs 80 seconds, with five-second connects. There are no transparent retries: after the first repeated transport or identity failure a host backs off exponentially from 250 ms to 30 s. A changed endpoint, certificate pin or epoch replaces the cached channels; requests already holding the old channel finish against the identity they were issued for.

Endpoint or certificate changes follow the registry's drain → `set-transport` → `finish-drain` procedure. Draining hosts remain routable for cleanup and inspection; they admit no new commands.

The [owner-routing tests](../crates/sandbox-controller/tests/owner_routing.rs) run two simulated supervisors with distinct mTLS identities behind the registry, PostgreSQL and the HTTP router. They cover execution and destroy on each owner from one controller, a lost acknowledgement on the second host, a replica answering as another host, certificate and name mismatches until the registration matches, an endpoint move through drain, a host that accepts TCP and never completes TLS while the other host progresses, and API file reads routed to the owner with cross-project denial. This is portable loopback evidence only; it does not exercise real Firecracker hosts, Linux/KVM, network partitions between machines or production certificate rotation.

Fleet placement and owner routing are integrated: one controller can reserve and dispatch new creates on multiple hosts.

The [fleet qualification tests](../crates/sandbox-controller/tests/fleet_qualification.rs) ([#198](https://github.com/hudson-frontier/hudsonbox/issues/198)) use an independent harness with two simulated supervisors, two projects and two allocations per host. They cover the following:

- Upload, execute, file capture, live stream, cancel, lease maintenance and destroy each reach the persisted owner, and the other project is denied.
- Two controllers ticking concurrently create one allocation and one start per sandbox, and execute each command once.
- A lost cancel reply followed by a partition leaves the target `unknown` without replay while the other host progresses. On return, a replacement controller inspects the target.
- After an operator drain and epoch advance on one host, API reads of the old allocation are refused: file capture returns 404 (the file view requires the host's current epoch), and live stream returns 410 `output_missing`. The new incarnation receives no starts or commands, and the old reservation is retained.
- A lost stop reply on a partitioned host keeps its capacity reserved while new placement uses the other host. The slot is freed only after inspection confirms the stop.
- An exhausted dedicated pool waits instead of using an idle shared host.
- A drained host takes no new placement, even when empty, while its cleanup is still routed to and confirmed by that host.

Like the owner-routing tests, this is portable fake-supervisor evidence and not Linux/KVM qualification. See the [qualification record](evidence/2026-10-30-fleet-qualification.md).

## Completion and uncertainty

A successful RPC is only a candidate observation. Storage verifies the complete host/project/sandbox/allocation/operation tuple, generation, supervisor epoch, current claim revision and deadline, original create operation ID, and a supported observed state. The observation timestamp must be within ten seconds of PostgreSQL time. Readiness must still be inside the allocation's execution lease. Final writes check the claim again after lock waits; expiry rolls back all state changes.

A matching readiness observation atomically marks the allocation and sandbox running, completes the create operation, clears lifecycle ownership, and appends the bounded receipt. A matching released observation instead fails the create and records release evidence, retires the sandbox, and frees its reservation. Deadline expiry or missing evidence alone cannot release capacity.

A lost response, absent allocation, or rejected observation records `unknown`, retains capacity and dispatch history, and schedules reconciliation after one second. Repeated inspection does not increment create attempts or append unbounded copies of the same uncertainty. `unknown` has no completion timestamp. A later matching observation can complete it without replaying create.

The crash boundary after intent but before sending remains deliberately unresolved when the supervisor reports absence. The create loop preserves that uncertainty; the destroy path below can take cleanup ownership and resolve it using stop/fencing evidence. It cannot infer that an absent in-memory record proves there was no external action in a different incarnation.

A revoked credential, disallowed image, unsupported resource size, missing pool grant or exhausted placement wait can fail an operation before dispatch. Placement and first-dispatch preparation both check the shared [guest resource envelope](compatibility.md#sandbox), including reservations retained from an earlier policy. A recorded dispatch intent still selects inspection before that policy check, preserving uncertain work. Service-owned rejection frees a reservation only when the locked records prove zero dispatch attempts, no execution lease, and an undispatched phase. It records that proof atomically with failure. Unknown or previously dispatched work cannot use this shortcut.

## Destroy admission and cleanup

`POST /v1/sandboxes/{sandbox_id}/destroy` requires project authentication, an idempotency key, and a JSON object. The only optional input is `correlation_id` (at most 200 bytes); caller-supplied lifecycle or ownership fields are rejected. Admission resolves the key and digest before lifecycle state. Identical retries return the same operation; a changed request conflicts. A new key for an already destroyed sandbox returns a completed no-op without changing its original destruction timestamp.

[Destroy storage](../crates/sandbox-store/src/destroy.rs) serializes on the active operation, project, and sandbox. A live conflicting transition returns `409` with its authorized `operation_id`; the rejected key is not consumed. An unknown create can hand cleanup ownership to destroy: its claim revision increases, its status stays unknown with phase `cleanup_owned_by_destroy`, and create claimers skip it. No in-flight create response can overwrite the new lifecycle owner.

The destroy controller persists stop intent before RPC. After a lost response it first inspects the same allocation. Confirmed release completes directly; ready or plain absent state permits another idempotent stop/fence, with intent recorded before that attempt. At most 100 stop attempts are issued for one operation; beyond that, inspection continues and unconfirmed capacity stays reserved for operator investigation. Receipt history retains the initial stop intent and final release, without appending unbounded copies of retries.

Completion requires a fresh, tuple-matching `released` or `fenced_absent` observation under the current claim, allocation generation, and supervisor epoch. Plain `absent`, an expired lease, and a stop request are insufficient. The explicit `fenced_absent` state means the supervisor has confirmed that no incarnation exists in that epoch and blocked future starts of that allocation; the fake models that fence in memory and reports simulated evidence. A restarted supervisor must have a new epoch and cannot reuse empty memory as old-epoch release proof.

Allocation release, its evidence, the sandbox tombstone, and successful destroy completion commit atomically. If destroy superseded an unknown create, that create ends as failed with `create_outcome_unknown=true` and a link to destroy; this does not invent whether it previously ran. A provably never-allocated create (generation and attempt count both zero) can be retired directly under database locks. Already admitted cleanup continues under service authority after credential revocation.

The fake's lost-stop-reply hook exercises both release of a recorded incarnation and fencing an absent one. The real supervisor must provide durable fencing and actual resource cleanup before it can return either completion state. Fake tests prove neither of those hardware guarantees.

## Allocation maintenance

[Maintenance storage](../crates/sandbox-store/src/leases.rs) uses a separate revision, claim deadline, and next-attempt time on each allocation. Completed create operations stay completed. The [migration](../migrations/0003_allocation_maintenance.sql) preserves existing execution deadlines and makes existing running allocations immediately eligible, without inventing observations or extending their leases.

Newly confirmed creates become due after ten seconds. Each controller tick claims at most one due allocation on its configured host/epoch using `FOR UPDATE SKIP LOCKED`; the running controller uses a ten-second maintenance claim. Database mutation locks follow project → sandbox → host → allocation, with final revision/deadline checks after lock waits. An expired claim cannot record a renewal or admit cleanup. No claim timeout, host timeout, or timestamp alone releases capacity.

Before renewal, storage checks the current allocation/generation/epoch, active transition, desired state, project status, and sandbox deadline. The new deadline is at most thirty seconds from database time and capped by sandbox expiry. `lease_requested_until` and `renewal_pending` commit before RPC; `lease_expires_at` advances only on matching ready evidence from the supervisor. The initial create still records its execution deadline as dispatch intent before readiness. Maintenance observations must match the complete maintenance ownership tuple, be within ten seconds of database time, and report a still-live deadline matching either the persisted request or the previously known lease. Simulation opt-in and provenance apply as in create/destroy.

A lost reply or crash with pending intent selects `InspectLease` before any new extension. If the host kept the old lease, inspection can confirm it and a subsequent claim may renew normally; if the renewal took effect, inspection confirms that deadline. Neither case replays create. A rejected or missing observation leaves the reservation counted, marks the sandbox unknown, and retries after two seconds. A failed health check also marks running observations on that host/epoch unknown without freeing capacity. Confirmed renewals schedule the next check in at most ten seconds, earlier near expiry, with a one-second minimum. Stored maintenance evidence is a bounded latest observation, not an unbounded per-heartbeat history.

A suspended/deleting project, expired sandbox deadline, or deadline shortened below an outstanding execution permit admits an ordinary `destroy` operation with `initiator_kind=service`. Credential rotation alone does not cancel already-admitted execution. A matching `released`/`fenced_absent` maintenance observation also admits service-owned destroy, preserving the observed evidence. The existing destroy path obtains its own stop/release evidence and atomically frees capacity and tombstones the sandbox; prior create outcomes remain historical facts. Plain absence never authorizes release. Destroy admission prevents later renewal commits, and the supervisor stop fence prevents a delayed renewal from reviving the incarnation.

Maintenance runs before one operation each tick, so a continuously nonempty operation queue does not skip renewal. All RPCs retain the five-second transport bound. The current loop processes one host and one maintenance item per tick: size and load-test the host against the sixty-second initial boot window and thirty-second renewal window before deployment; large-host batching and scheduling performance remain unvalidated. These are conservative development timings, not an availability guarantee. A healthy replacement epoch marks prior running observations unknown. Old-epoch allocations stay reserved until independently proven stopped/fenced; reconnecting to an empty restarted fake supplies no such proof.

[Lease integration tests](../crates/sandbox-controller/tests/leases.rs) cover competing claims, lost acknowledgements, crash-before-RPC recovery, stale observations, expiry during lock waits, deadline changes, health-check failure, service cleanup after suspension, credential rotation, and maintenance alongside operation queues. [Fake state tests](../crates/sandbox-fake-host/tests/state.rs) also verify survival beyond an initial lease after renewal, monotonic deadlines, watchdog expiry, and rejection of stale or post-stop renewal.

## Previous-epoch allocation recovery

After an operator provisions a newer supervisor epoch in both the host and database, the controller can reconcile older reservations through `ReconcilePreviousAllocation`. Health only marks old running observations unknown. It never proves cleanup or releases capacity. Registration and automatic epoch issuance remain unfinished.

The recovery request carries two distinct identities: the complete original operation/allocation ownership tuple and the current **reporting epoch**. Normal create, command, file, stop and renewal methods still reject older epochs. The [recovery RPC](supervisor-protocol.md#previous-epoch-release-evidence) requires retained matching host ownership and verifies its original cleanup; an empty restarted fake cannot answer it.

[Store preparation](../crates/sandbox-store/src/recovery.rs) runs under the existing create/destroy operation claim. It locks operation, project, sandbox and current host state, validates the original allocation/generation and the newer database host epoch, and commits a reconciliation phase before RPC. An old create reservation with no dispatch intent uses the existing rejection transaction, which independently proves no execution lease or create attempt before releasing it. Dispatched creates are never sent again.

Running allocations from an older epoch become eligible for maintenance under the currently configured epoch. Preparation admits one service-owned destroy with reason `host_epoch_changed`, retaining all capacity. Active create/destroy transitions keep their original operation handles and use the recovery path directly. No old allocation is renewed, migrated to the new epoch or silently replaced.

Completion accepts only a fresh, exact `released` or `fenced_absent` observation whose reporting epoch still matches the locked database host. The existing original epoch stays on the allocation and in the private release receipt, alongside the reporting epoch. Under a live claim, completion releases the allocation, clears the sandbox's current-allocation/transition pointers and marks it destroyed. A pending create finishes failed with its earlier outcome explicitly unknown; destroy finishes succeeded. A superseded unknown create is closed by the destroy receipt. Previously completed creates and command/file execution outcomes remain untouched: VM cleanup cannot invent an exit code or prove whether an uncertain effect ran.

[The controller recovery step](../crates/sandbox-controller/src/recovery.rs) has a ten-second bound including metadata work and RPC; lifecycle transport retains its five-second RPC bound and transactions have two-second statement limits. Missing or contradictory evidence retains the reservation and marks the operation unknown for retry. Cancellation leaves the existing claim to expire. Stale claims, changed reporting epochs and expired observations cannot commit, including after database lock waits. Repeated host cleanup observations are safe; a completed operation cannot be claimed or completed again.

[PostgreSQL tests](../crates/sandbox-controller/tests/recovery.rs) cover undispatched rejection, forged/stale observations, simulated-evidence opt-in, missing journal evidence, replacement claims, service-owned destroy, superseded creation, capacity reclamation, cancellation and lock-wait expiry. [Real-host tests](../crates/sandbox-supervisor/tests/support/previous_epoch.rs) exercise both a running VM and a create whose acknowledgement was discarded across a supervisor restart, including original ownership, cleanup, database recovery and a fresh replacement workload. An in-flight command remains unknown without another dispatch. These are nested-aarch64 development checks; missing/corrupt journals, lost hosts and supported-x86_64 acceptance still need their own recovery or release evidence.

[Recorded recovery evidence](evidence/2026-09-21-aarch64-previous-epoch-recovery.json) binds 210 matching Mac/Linux source hashes and the built artifacts to 443 local tests and 17 passing real-host regressions. The record includes the initial fixture correction and diagnosed clock-skew failure, followed by the successful full rerun. Temporary storage was removed and test cgroups were verified empty.

## Simulated observations remain visible

The controller rejects a simulated supervisor unless `--allow-simulated` is explicitly set. Use this mode only with isolated development data. A successful fake create includes `result.simulated=true` in the operation response. The sandbox response includes `observation_simulated=true`; false denotes real evidence, and omission denotes no confirmed observation source.

[Migration 0002](../migrations/0002_observation_source.sql) adds the nullable source field without assigning a source to existing rows. [The upgrade test](../crates/sandbox-store/tests/upgrade.rs) applies it over populated migration-0001 data and verifies that existing state is preserved. This field describes evidence origin; it does not make a simulated running sandbox executable.

## Running the controller

The binary connects to PostgreSQL, applies migrations, requires mTLS credentials and explicit host/image configuration, then scans for work at least every 500 ms between ticks. A background PostgreSQL listener can wake it earlier when a new create, execute, cancellation or destroy operation commits. It handles shutdown without erasing pending intent. `--once` performs one lifecycle tick plus the explicitly enabled maintenance workers for controlled diagnostics, without starting a listener.

### Work-discovery hints

The operation table remains the durable queue. New nonterminal lifecycle/command admissions attempt an empty notification on the database-local `hudsonbox_work_v1` channel **after** committing. The extra query, including pool acquisition, has a 100 ms budget. Notification failure or cancellation cannot change the committed operation or its HTTP response. Existing retries, policy rejections and immediately completed no-ops do not publish new work. File-source readiness, delayed retries, lease maintenance and cleanup continue to rely on the ordinary scans.

The listener holds one connection from the controller's configured database pool. Connection and subscription attempts have a one-second budget and retry no faster than once per second in a background task. Successful subscription, including after reconnection, triggers a scan to discover work whose notification was missed. An unavailable listener never blocks the periodic scan. Notifications are coalesced into one signal; notification-driven waits have a 50 ms minimum, limiting them to twenty scans per second without extending the original polling deadline. Dropping the worker pool stops its listener.

A hint contains no project, sandbox, operation, command or credential data. Nonempty payloads are ignored. A hint merely permits another scan: claims, authorization, host identity, leases, generations and dispatch receipts still decide which work can run. It cannot create an operation, select an allocation or replay a command. Older APIs and controllers interoperate through polling; no migration or notification delivery guarantee is required.

[Wake-up integration tests](../crates/sandbox-controller/tests/wakeup.rs) cover committed HTTP admission, duplicate and denied requests, work admitted before subscription, forged hints, disconnection/reconnection and unavailable listeners. Deterministic timer tests cover burst coalescing and polling bounds; the [store helper](../crates/sandbox-store/src/work.rs) tests bounded notification failure under pool pressure. These are control-plane checks, not measurements of VM startup performance.

```sh
cargo run -p sandbox-controller -- --help
```

Required settings are `DATABASE_URL` (or `--database-url`), `--endpoint` using HTTPS, `--host-id`, `--host-epoch`, `--ca-cert`, `--client-cert`, `--client-key`, and one or more `--image-digest` values. The configured host must already exist at that epoch, and its server certificate must match the derived host DNS identity. The fake also needs its matching epoch and controller certificate fingerprint; see [fake-host setup](supervisor-protocol.md#run-the-fake).

The image allowlist is static process configuration and checked at dispatch. Updating it currently requires restarting the controller. The API independently requires an explicit [admission allowlist](api-contract.md#implemented-image-admission). Operators must keep API, controller, and supervisor policy aligned; disagreement rejects new admission or later dispatch. Removing an image does not invalidate an existing retry handle or stop an already running allocation. Image byte verification, manifest pinning, and host compatibility checks remain necessary work.

Public state remains a timestamped last observation, not a live VM-presence guarantee. The maintenance loop renews confirmed allocations and reconciles same-epoch watchdog release. Controlled nested-aarch64 tests exercise watchdog teardown and restart fencing; supported-host failure and isolation gates remain separate.

An optional `--retire-history` task coordinates live and verified-destruction history retirement, cycling through both command/file domains for each proof path. It does not run inside the lifecycle tick and cannot refund pending or unverified prefixes. Consumer cleanup must complete first; see [activation and limits](history-reclamation.md#operator-activation). The simulator refuses durable history evidence even with `--allow-simulated`.

`--retire-allocations` requires `--retire-history` and rejects simulation mode. Its independent task discovers released allocations with registered physical permits, retires guardian metadata, records database completion and then forgets host records. Durable scan cooldowns and delivery leases bound retries; retained database proof resolves lost acknowledgements. `--once` runs four history turns followed by one allocation-retirement tick when enabled. See [automatic allocation retirement](allocation-retirement.md#automatic-controller-worker) for activation, ownership and remaining reuse gates.

## Evidence and remaining work

[Controller integration tests](../crates/sandbox-controller/tests/create.rs) exercise:

- Authenticated HTTP admission → PostgreSQL intent → real gRPC/mTLS → confirmed simulated state visible through the HTTP router.
- Multiple controllers competing for one create, with exactly one applied fake start.
- Lost acknowledgements and crash boundaries before and after intent, with no duplicate starts or premature capacity release.
- Mandatory simulation opt-in; image/credential rejection; reauthorization after reservation.
- Ownership mismatches, stale claims, expired readiness, implausible observation times, and claim expiry during lock waits.
- Confirmed release versus absent evidence and the rollback of incomplete terminal writes.

[Destroy integration tests](../crates/sandbox-controller/tests/destroy.rs) additionally cover concurrent idempotent admission, tenant isolation, live-transition conflicts, unknown-create handoff, delayed starts after fencing, lost stop/fence replies, unconfirmed release, claim expiry during completion, credential revocation after admission, and permanent tombstones.

The [standalone HTTPS API and offline project provisioning](api-server.md) are exercised through real TCP/TLS tests; the controller remains a separate process. These tests close the create control-plane loop but do not satisfy the real execution or isolation gates in [Phase 1](roadmap.md#scope-discipline-for-phase-1). Remaining work includes authenticated host registration, finite history reclamation, production images/network policy, distribution packaging, and the full supported-host/adversarial failure gates.

## Real Linux lifecycle integration

The [real supervisor](real-supervisor.md) now implements the same lifecycle RPCs with `simulated=false`. Its controlled PostgreSQL/HTTP-router test exercises authenticated admission, real VM readiness, scheduled renewal and verified destroy/release through this controller. Command execution, public file transfer and verified previous-epoch allocation recovery are integrated. See the real supervisor document for host capacity, restart and evidence limits.

## Concurrent reconciliation

The running controller starts four independent reconciliation workers by default.
Use `--workers N` to select 1 through 16; `--workers 1` restores serial
processing. `--once` still runs exactly one controller tick and the explicitly
requested publisher/retirement ticks.

Each worker performs the same cancellation, authenticated host check, allocation
maintenance, and rotating operation scan. A slow file-source read or lifecycle
RPC occupies one worker, allowing the others to maintain leases and process
independent sandboxes. Shared database/host outages or all workers waiting at
once can still delay progress. The existing
operation and allocation claims, deadlines, generations and dispatch receipts
remain authoritative; worker count never permits replaying uncertain commands.
Sibling workers serialize allocation-authority registration so concurrent
acknowledgements cannot advance beyond a worker's inspected permit batch.

Workers share one notification listener and its single pool connection. Each
subscriber coalesces notifications and retains the 500 ms polling fallback and
50 ms minimum notification wait. The twenty-scans-per-second bound applies to
each worker. The process configures at most `8 + 2 * N` database connections,
including the listener and optional background publishers. Each worker retains
at most one verified upload source, up to 8 MiB, so the pool can retain up to
`N * 8 MiB` of source bytes in addition to in-flight transport buffers. Sources
can be read again when another worker claims the next upload phase.

SIGINT and SIGTERM stop scheduling further ticks and allow current ticks to
finish within one shared ten-second drain budget. Reconciliation, optional
output publication, history retirement and allocation retirement all use the
same supervised task set. Idle workers wake immediately on shutdown. A second
stop signal or the drain deadline interrupts unfinished work and joins its
tasks before exit. The handlers are installed before workers start. `--once`
continues to run only its requested finite ticks.

Committed claims expire through the existing recovery path; interruption
neither releases reservations nor proves that a dispatched operation did not
run. Draining does not destroy sandboxes; a current tick may still renew a
lease under the existing bounds. An unexpected exit or panic in any supervised worker stops the controller process
so its service supervisor can restart it.

The [file-upload integration tests](../crates/sandbox-controller/tests/uploads.rs)
block a source read and require another sandbox's creation, maintenance and
destruction to complete before releasing it. They also cancel an in-flight read
and recover its upload under replacement workers without duplicate publication.
These checks use PostgreSQL and authenticated fake-host RPCs, not a real VM.
The [shutdown tests](../crates/sandbox-controller/tests/shutdown.rs) exercise
completion of current work, idle wake-up, a shared drain deadline, task failure
supervision, real SIGINT/SIGTERM delivery to the controller binary, and a second
signal interrupting a tick blocked on a database lock.

## Command admission and dispatch ownership

The [execution store](../crates/sandbox-store/src/execute.rs), [public execute handler](../crates/sandbox-api/src/execute.rs), controller and supervisor now connect `POST /v1/sandboxes/{id}/execute` to the guest runner. The controller rotates create, destroy and execute work while maintaining allocation leases first. The [real supervisor](real-supervisor.md#command-dispatch-and-reconciliation) owns command dispatch; the fake reports explicit simulation. [Output retrieval](output-storage.md) and [streaming](api-contract.md#implemented-output-streams) are implemented; [Command cancellation](command-cancellation.md) is implemented; file transfer remains unfinished.

[Command input](../crates/sandbox-protocol/src/command.rs) normalizes an argv array, nonsecret environment (default empty), absolute working directory (default `/`), required absolute `deadline_unix_ms`, and combined stdout/stderr `output_limit` (default 1 MiB, maximum 10 MiB). It inherits the guest runner's argument, environment and encoded-size bounds. New deadlines must be in the next six hours and cannot exceed sandbox expiry. These are the input names for the public execute route. Arguments and environment are persisted as operation payload, never copied into dispatch receipts or `Debug` output; this is not a secret transport.

Admission requires a running, leased allocation at the host's current epoch and a live project credential. It serializes on the project and sandbox, uses the project-wide idempotency constraint, and returns existing handles before applying deadline or runtime-state policy. Changed normalized input under the same key conflicts. Defaults and environment key order normalize identically. Another project's sandbox is absent; a destroyed target rejects new work. One unfinished command occupies the sandbox slot, including `unknown` outcomes. Execute does not take `active_transition_operation_id`, so destroy remains admissible while a command is running or uncertain.

[Migration 0005](../migrations/0005_execution_ownership.sql) pins `execution_allocation_id` through a same-sandbox foreign key and enforces one active pinned command per sandbox. The pin survives destroy clearing `current_allocation_id`. Existing generic execute rows keep a null pin and cannot be dispatched; the migration never guesses an allocation or changes their outcome. Legacy unfinished rows also block new admission until reconciled.

Admission also checks retained capacity under the same project/sandbox locks. All admitted commands pinned to this allocation count toward the protocol's 32-command limit; the sum of their full output limits plus the new request must fit 64 MiB. Completed, failed-before-start, expired and compacted commands stay counted until the separate acknowledged history worker completes their prefix. Payload compaction supplies the original limit through its validated command descriptor. Invalid history fails closed. Identical retries resolve before this check; rejected new requests consume no operation or key. This conservative reservation can exceed the host's actual retained bytes but never treats absence of output as proof of reclamation.

[Migration 0011](../migrations/0011_execution_capacity.sql) adds an allocation/history index without changing existing rows. The query reads at most 33 records, does not lock operation rows while holding project locks, and therefore preserves the reconciliation lock order. Compaction replaces payload and descriptor atomically, so admission sees either representation with the same budget. New admission serializes with other admissions and dispatch through project/sandbox locks. First-dispatch preparation applies the same bound to previously accepted work; dispatched/unknown commands bypass admission policy and continue inspection. This protects the supported API/controller path; independently injected host RPCs or corrupted database/host accounting still require operator reconciliation.

First-dispatch preparation locks operation → project → sandbox → host, rechecks claim ownership, authority, target identity, deadlines and runtime lease, and commits one intent with the guest command digest before returning a dispatch action. The expected host/epoch must match before that first action. Undispatched commands whose authority or target changed fail without releasing VM capacity. When owner routing is unavailable, the claimed undispatched command durably loses dispatch eligibility; subsequent maintenance or authentication recovery cannot restore it. Preparation rejects it with `execution_target_changed` and `dispatch_intent_absent`. Existing dispatch intents retain inspection/cancellation semantics and no outcome is inferred from host unavailability. Once intent exists, later claims inspect or request cancellation under the original allocation, generation, epoch and digest, even after revocation, destroy or a host restart. Missing or changed evidence fails closed. A database dispatch action is not proof that a guest received or ran the command.

[PostgreSQL execution tests](../crates/sandbox-store/tests/execute.rs) cover same-key and distinct-key concurrency, direct uniqueness and foreign-key enforcement, tenant scope, normalized retries after actual deadline expiry, invalid input, authority revocation, destroy during uncertain work, host and claim fences, lock-wait expiry, retained ownership after release, and payload redaction. [Upgrade tests](../crates/sandbox-store/tests/upgrade.rs) preserve old rows and verify that missing allocation ownership cannot dispatch. These tests seed database allocations; they make no VM execution or isolation claim.

[Capacity tests](../crates/sandbox-store/tests/support/execution_capacity.rs) cover exact slot/byte boundaries, concurrent admission at the last slot, retained reservations after compaction, malformed history, tenant scope, retry identity and migration over populated history. Legacy queued work is rejected before dispatch when full; unknown dispatched work remains inspectable even over budget. The [controller tests](../crates/sandbox-controller/tests/execute.rs) run 32 commands through HTTP and real mTLS to the fake, reject the next request without dispatch, then complete destroy. They also prove an output-budget rejection leaves its key reusable by a smaller request. These checks establish admission/reconciliation behavior, not real-VM isolation.


`ExecuteCommand` carries the pinned ownership and normalized guest command; `InspectCommand` carries the same ownership and digest. Lost RPC replies select inspection on the next claim. A host with no retained dispatch for this operation can commit a durable no-start fence and return `not_started`; a late execute then cannot start. Missing guest receipts after a host dispatch remain unknown, including when destroy has removed the guest. A replacement host epoch cannot answer old ownership and does not authorize replay.

Result recording validates the current claim, full ownership tuple, digest, observation time, guest context, deadline and output budget. A known boot identity cannot change across observations. Exit zero succeeds; nonzero exit or signal fails with `command_failed`; timeout fails with `deadline_exceeded`. Active receipts remain running and missing/unknown receipts remain unknown. [Public command cancellation](command-cancellation.md) now drives guest interruption under the target execution claim; acceptance alone never changes a target outcome. A no-start fence fails with `command_not_started`. Completion stores bounded output statistics and exit metadata with `guest_reported=true`; guest output bytes never traverse the controller and guest cleanup never releases an allocation. History retains the initial intent and latest command evidence, at most two entries. The guest is root-controlled, so command reports are workload data, not trusted isolation or external-effect evidence.

[Public execution tests](../crates/sandbox-controller/tests/execute.rs) exercise HTTP admission and polling through real mTLS to the fake, including duplicate and changed retries, lost acknowledgements, no-start fencing, nonzero results, destroy during active execution, credential revocation, forged or cross-boot evidence, and command history pressure leaving capacity for destroy. Real API/VM evidence is recorded by the [host integration tests](../crates/sandbox-supervisor/tests/host.rs).


## Independent output worker

`--archive-output` enables [final-output archival](output-storage.md#supervisor-archival-and-recovery). It runs independently of lifecycle ticks using its own publication claim and mTLS connection, so waiting on storage does not serialize renewal or destruction. The output archiver persists tickets/plans/references and never receives guest output bytes or output-storage credentials. Execution completion reports pending output first; archival can publish it later, including after destroy if the objects already exist. The API handles retained-byte retrieval independently of the controller; live SSE remains separate work.

## File upload dispatch

`--file-source-config /absolute/private/file-sources.json` opts into public file-write claims alongside create, execute and destroy. The [source configuration](api-server.md#file-source-configuration) must match the API's bucket and endpoint; source reading and supervisor mutation use separate credentials. The controller verifies and retains one bounded source buffer, then sends one chunk per tick under fresh database ownership. Source/reply loss selects reconciliation, and durable commit intent is never replayed. [Public upload orchestration](file-transfer.md#public-upload-orchestration) owns its authorization, retention and failure contract. With no source configuration, this worker does not claim file operations.
