# Local host maintenance

The offline `sandbox-host-manager` command provides initial host provisioning and draining for [#116](https://github.com/hudson-frontier/hudsonbox/issues/116), and the [multi-host registry](#host-registry-and-pools) for [#192](https://github.com/hudson-frontier/hudsonbox/issues/192). It uses trusted operator database access; no new public management endpoint or guest authority is exposed. [Capability enrollment](host-capabilities.md) adds a separate operator-approved compatibility contract. Backup/restore rehearsals and supported-host upgrade qualification remain work in that issue.

## Private configuration and commands

Create an operator-owned `0600` JSON file containing `version: 1` and `database_url`. Keep it outside customer payloads and source control. The reader rejects final symlinks, special/permissive/foreign-owned files, duplicate/unknown fields and files larger than 64 KiB. Errors use fixed diagnostics without printing database credentials or arbitrary query text. The command neither runs migrations nor resets data.

```sh
cargo run -p sandbox-controller --bin sandbox-host-manager -- \
  --database-config /private/database.json --host-id HOST_ID --host-epoch CURRENT_EPOCH status
cargo run -p sandbox-controller --bin sandbox-host-manager -- \
  --database-config /private/database.json --host-id HOST_ID --host-epoch CURRENT_EPOCH drain
cargo run -p sandbox-controller --bin sandbox-host-manager -- \
  --database-config /private/database.json --host-id HOST_ID --host-epoch CURRENT_EPOCH finish-drain
```

Use the explicitly chosen typed host ID and expected positive epoch retained in the host's private configuration. The initial provisioning command below creates its database row. The drain/status commands do not issue/replace an epoch, change capacity, approve an image or turn an unhealthy host ready. The separate `set-capabilities` action approves a contract only on an empty draining host. Database access is installation-wide operator authority, not a Project credential or implicit delegation to an agent.

## Initial host provisioning

Prepare an operator-owned `0600` JSON document with these fields:

| Field | Required value |
| --- | --- |
| `version` | `1` |
| `capacity` | Total schedulable `vcpu`, `memory_mib` and `disk_mib`, after operator headroom |
| `capabilities` | The complete approved version 1 [capability contract](host-capabilities.md#contract-and-evidence) |

All capacities must be positive, fit their database integer types and cover the
advertised per-allocation limits. The document and private database configuration
have the same ownership, regular-file, permission and 64 KiB bounds. Unknown or
duplicate fields and incompatible capability metadata fail validation.

```sh
cargo run -p sandbox-controller --bin sandbox-host-manager -- \
  --database-config /private/database.json --host-id HOST_ID --host-epoch INITIAL_EPOCH \
  provision --host-config /private/host-provision.json
```

The transaction creates a host in `draining` state with approved compatibility
metadata and no service observation. It returns the ordinary maintenance report.
The host cannot admit a new placement until the configured controller observes
matching host identity, epoch and capabilities over gRPC/mTLS, then the operator
explicitly runs `finish-drain`. Credentials, certificates, immutable runtime
artifacts and the host process are provisioned through their existing operator
procedures. This command approves metadata; it does not establish physical
capacity, execution, image integrity or isolation qualification.

Retain the ID, epoch and exact input before making the first attempt. After a
lost response, retry that same tuple. An exact retry reports current state,
including an already ready, draining or unhealthy host, without changing its
timestamps, observations, capacity, allocations or pending work. Changed
capacity, policy or epoch conflicts. Existing legacy rows cannot be implicitly
enrolled through this command; use the explicit empty-host capability procedure.
Statement execution is bounded to two seconds and lock waits to one second in
the provisioning transaction. A failed response can follow a successful commit;
retrying reconciles the existing row instead of assigning another identity.

## Drain and completion semantics

`drain` locks the host row used by placement, transitions ready to draining and returns bounded counts. Repeated drain at the same epoch is idempotent. A changed/missing epoch or unhealthy state fails. A placement serialized after drain cannot reserve that host. Work already dispatched continues through service-owned inspection/lease/cleanup; no caller mutation is replayed. New command/upload admission on that draining host follows the existing fail-closed rules.

The report contains the status/epoch, outstanding allocations, reserved CPU/RAM/disk, unfinished operations and unknown operations associated with unreleased allocations. It also reports `previous_epoch_allocations` (unreleased reservations issued under an older epoch of this host, which need [previous-epoch recovery](controller.md#previous-epoch-allocation-recovery)) and `unknown_state_allocations` (unreleased reservations whose sandbox runtime is currently unknown). Every count is scoped to the named host identity. A command or upload stays attributed to the allocation it was pinned to; unpinned lifecycle operations follow the sandbox's unreleased allocation. Counts describe database reservations; they are not independent physical cleanup receipts or proof that external effects are known. Drain preserves allocations, leases, permits, idempotency keys, histories and retirement scope. It never refunds capacity, cancels, destroys, deletes storage or promises resumable memory.

Let admitted work finish or explicitly cancel/destroy the relevant sandbox through its ordinary authenticated API. Destroy remains available after a command failure or unknown outcome. Confirm its terminal cleanup operation; reservations stay until the existing verified release path completes. Closing the manager/controller is not resource reclamation.

`finish-drain` is a separate explicit action. It requires draining status at the expected epoch, zero outstanding reservations and a fresh service observation within thirty seconds. The host row remains locked through checking/reopening, so new placement cannot race the absence check. Authenticated controller health refresh cannot promote draining automatically. Retention/retirement continue independently after a verified physical release.

## Epoch issuance and restart

The operator can advance a draining or unhealthy host to its exact successor
database epoch without changing capacity or discarding old ownership:

```sh
cargo run -p sandbox-controller --bin sandbox-host-manager -- \
  --database-config /private/database.json --host-id HOST_ID --host-epoch PREVIOUS_EPOCH \
  advance-epoch
```

Drain placement before planned maintenance and coordinate stopping the old
controller/supervisor through the installation's process manager. An unhealthy
host can enter this recovery path directly. Preserve its journals, allocation
directories, immutable images and credentials. Independent guardian deadlines
remain authoritative while the service is unavailable.

The command locks the host row, advances exactly one positive epoch and sets
the host to `draining` with no fresh service observation. It retains approved
compatibility and prior capability evidence under their original epoch. Old
allocations, claims, leases, operation outcomes, request keys and capacity
charges remain unchanged. The returned epoch is a database fence, not a physical
stop, release receipt, saved-memory proof or instruction to replay work.

Retain the returned epoch in the host and controller configurations before
starting the replacement. Matching authenticated health must arrive under that
epoch. Older allocations use the
[previous-epoch recovery path](controller.md#previous-epoch-allocation-recovery)
with their original ownership and the new reporting epoch. `finish-drain` still
requires zero outstanding reservations and fresh health; health alone never
reopens placement.

After an uncertain response, retry with the same previous epoch. If its exact
successor is already current, the command reports its current state without
resetting health or returning it to draining. Concurrent duplicate requests
advance once. A ready host cannot begin a new advancement, a newer-than-expected
epoch conflicts, and invalid/overflowing epochs are refused. Statement and lock
waits are bounded to two seconds and one second respectively. An epoch floor
does not replace a backup/restore protocol or authorize a database rollback.

## Unavailable and returning hosts

The controller's [fleet recovery loop](lifecycle.md#fleet-host-loss-reconciliation) treats missing/stale heartbeat or an epoch mismatch as runtime uncertainty. Health failures clear placement freshness for the checked epoch immediately. Neither condition proves a VM stopped; all unreleased CPU/RAM/disk reservations stay charged. Missing transport identity also cannot authorize replacement. A fresh returning host must reconcile original ownership before readiness is confirmed. An old incarnation cannot mutate a newer epoch or release its reservations.

Keep journals, launch-permit history and allocation directories through a restart. The existing [previous-epoch recovery](controller.md#previous-epoch-allocation-recovery) requires verified cleanup evidence; missing local records and a successful health response cannot substitute for it. Without a durable checkpoint, cleanup is destruction/failure, never process recovery. A provider adapter must not interpret stale inventory or a database epoch advance as an infrastructure fencing receipt.

## Upgrade and recovery

Follow the [offline upgrade and recovery procedure](operator-recovery.md) before replacing application/supervisor versions or restoring database state. It includes the compatibility matrix, private backup boundaries, quarantine requirements and an executable PostgreSQL dump/restore rehearsal. A restored epoch is not independent proof that old execution or storage writers have stopped.

## Host registry and pools

[Migration 0025](../migrations/0025_host_registry.sql) implements the accepted
[`host_pools`, `project_host_pool_access` and `hosts` extension](data-models.md#accepted-v1-fleet-and-warm-pool-model).
The [registry store](../crates/sandbox-store/src/host_registry.rs) exposes the
read model a controller resolves per host: identity, pool and type, status,
epoch, endpoint, expected transport identity, provider reference, capacity,
approved/observed capabilities and heartbeat freshness (an authenticated
observation within 30 seconds). [Placement](controller.md#placement)
([#194](https://github.com/hudson-frontier/hudsonbox/issues/194)) enforces
project pool grants over this registry. Controller and API [per-owner routing](controller.md#per-owner-host-routing) implement [#193](https://github.com/hudson-frontier/hudsonbox/issues/193).

### Pools

`put-pool` creates or updates one pool from an operator-owned `0600` JSON
document with the same file bounds as other manager inputs:

```json
{"version":1,"id":"0190f000-0000-7000-8000-00000000c0de","name":"edge","type":"standalone","min_ready_hosts":0,"max_hosts":4,"placement_policy":{"version":1,"tenancy":"shared"},"provider_config":{"version":1},"credential_secret_ref":null}
```

```sh
cargo run -p sandbox-controller --bin sandbox-host-manager -- \
  --database-config /private/database.json put-pool --pool-config /private/pool.json
cargo run -p sandbox-controller --bin sandbox-host-manager -- \
  --database-config /private/database.json grant-pool-access --pool-id POOL_UUID --project-id PROJECT_ID
cargo run -p sandbox-controller --bin sandbox-host-manager -- \
  --database-config /private/database.json registry [--pool-id POOL_UUID] \
  [--after HOST_ID] [--limit 500]
```

`registry` prints one page (1–1000 hosts, ordered by identity) of retained
history including retired tombstones, plus `next_after` for the following page.
Runtime health refresh, discovery and capacity planning enumerate only active
(non-retired) hosts, bounded at 10000; tombstones never exhaust that bound.

Names are unique DNS labels. `max_hosts` is at least `min_ready_hosts`, at most
100000, never below the enrolled host count, and bounds provisioning into the
pool. `provider_config` is decoded against `type` with unknown fields refused:
`standalone` accepts only `{"version":1}`; `k8s` accepts `version`, a namespace
label and `worker_label_selector` for the future worker adapter
([#197](https://github.com/hudson-frontier/hudsonbox/issues/197)).
`credential_secret_ref` is an opaque reference name, never a secret value.
`placement_policy.tenancy` is `shared` or `dedicated`; a dedicated pool accepts
exactly one project grant. An exact retry changes nothing.

A type change is refused while any host is enrolled in the pool. Hosts carry
every reservation and unfinished operation, are never deleted and never move
between pools, so in practice a pool's type is fixed once it holds a host.
Provisioning and pool updates lock the pool row, so a concurrent enrollment
cannot race the check. Durable capacity operations do not exist yet; their
implementation must join this check.

`grant-pool-access` records an explicit, idempotent project/pool pair. A
missing grant is denial; grants confer no host or maintenance authority.
Placement chooses only hosts in a granted pool and never falls back to another
pool, including when every granted pool is full. A create from a project with
no grant for an eligible pool fails with `host_pool_access_denied`. There is no
revoke command; removing access needs a design for already placed work.

### Host enrollment and transport identity

Add optional `host_pool_id` to the [provisioning document](#initial-host-provisioning).
Absent means the backfilled legacy pool. The pool is part of the exact-retry
tuple, and an existing host cannot be re-provisioned into a different pool.
Replacing a worker means a new host identity.

`set-transport` records how the controller reaches and authenticates one host:

```json
{"version":1,"endpoint":"https://10.0.0.5:7443","certificate_sha256":"<64 hex>","provider_ref":"rack-a-5"}
```

```sh
cargo run -p sandbox-controller --bin sandbox-host-manager -- \
  --database-config /private/database.json --host-id HOST_ID --host-epoch CURRENT_EPOCH \
  set-transport --transport-config /private/transport.json
```

The endpoint must be `https://HOST:PORT` (DNS name, IPv4 or bracketed IPv6;
no path, query or credentials) and is stored lowercased. The transport identity
has three parts. All are required:

- the existing CA-verified server certificate chain;
- `transport_server_name`: the host-ID-bound subjectAltName
  `host-<uuid>.sandbox.internal` the controller already requests. A database
  check binds it to the row's own ID;
- `transport_certificate_sha256`: SHA-256 of the exact host server leaf
  certificate DER, the same pin format as the
  [local PKI report](local-certificates.md).

Endpoints, certificate fingerprints and per-pool provider references are
unique across hosts, so two identities cannot share an address or server
certificate. `RegisteredHost::verify_transport` refuses a connection whose
epoch, endpoint or presented leaf certificate differs from the registration.
A CA-valid certificate issued to another host, or a new certificate not yet
registered, is a mismatch.

The first registration and every change require the expected epoch on a
`draining` host. A change clears `last_seen_at`, so `finish-drain` waits for
authenticated health through the new registration. An exact retry reports the
current state in any status without touching the row. A changed endpoint,
certificate or provider reference on a ready or unhealthy host, or one with a
stale epoch, is refused. Certificate rotation is therefore drain, `set-transport`
and explicit reopening. Registration survives `advance-epoch`; the new epoch
then owns further changes. Authenticated health and heartbeats only refresh
freshness and capability observation. They never change the pool, endpoint,
transport identity, provider reference, capacity or epoch, and never promote a
draining host.

### Discovery and provider routing

`sandbox_controller::host_discovery` defines the provider-neutral
`HostDiscovery` trait and the application-code `ProviderRouter` (pool type to
adapter). `StandaloneStaticDiscovery` reads operator-configured addresses. A
Kubernetes worker adapter implements the same trait for `k8s` pools. Without
one, the router refuses `k8s` discovery rather than falling back. No Kubernetes
dependency exists. Discovery is advisory:

```sh
cargo run -p sandbox-controller --bin sandbox-host-manager -- \
  --database-config /private/database.json discover --pool-id POOL_UUID \
  --discovery-config /private/standalone-hosts.json
```

The `0600` discovery file is
`{"version":1,"pools":{"POOL_UUID":[{"host_id":"hst_...","endpoint":"https://...","certificate_sha256":"...","provider_ref":"..."}]}}`.
Duplicate host IDs, endpoints, certificates or provider references are refused.
The report lists matched, mismatched, unregistered, missing and unaddressed
hosts. It never enrolls a host, issues an epoch or edits a registration; each
correction remains an explicit operator command above.

### Legacy backfill and upgrade

Migration 0025 creates the shared standalone pool `legacy-standalone`
(`00000000-0000-4000-8000-000000000001`, `min_ready_hosts` 0, `max_hosts`
1000 or the existing host count if larger). It assigns every existing host to
this pool, which is also the column default. Legacy hosts keep NULL endpoint
and transport identity, meaning the single-host controller configuration still
supplies the address. All prior constraints, epochs, capacity, health and
reservations are unchanged. No project receives pool access implicitly.
Placement requires a grant, so this upgrade has an explicit operator step.
Before starting a controller that includes multi-host placement, grant
`legacy-standalone` to every project that should keep creating sandboxes:

```sh
cargo run -p sandbox-controller --bin sandbox-host-manager -- \
  --database-config /private/database.json grant-pool-access \
  --pool-id 00000000-0000-4000-8000-000000000001 --project-id PROJECT_ID
```

Repeat for each project, and for each project provisioned later. Without a
grant, new creates fail with `host_pool_access_denied` before any reservation.
Existing allocations, running sandboxes, destroys and commands are unaffected.
The single-host controller still reaches a legacy host through its configured
endpoint, so the host needs no transport registration for this. Apply the
migration before starting a new controller; roll back by restoring a
compatible build, not by dropping the columns under live hosts.

## Validation and limits

[Store tests](../crates/sandbox-store/tests/host_maintenance.rs) exercise epoch fencing, idempotence, unhealthy refusal, stale health and explicit reopening. [Controller tests](../crates/sandbox-controller/tests/host_maintenance.rs) prove placement exclusion while an admitted command reports a nonzero exit, followed by explicit destruction and reopening. The command's unit test rejects unsafe/private configuration boundaries.

[Host identity tests](../crates/sandbox-store/tests/host_identities.rs) use two
simulated identities in one database. Draining, epoch advancement, capability
approval, health observation and runtime-unknown marking on one host leave the
other host's row, reservations, admission and report unchanged. An epoch number
valid for one host grants nothing on another: maintenance, observation,
placement, maintenance claims and previous-epoch recovery are all keyed by host
identity and epoch together. A pinned unknown command stays reported on its
original host after a fixture-only replacement owner exists elsewhere. These
tests do not implement a host registry, per-host routing, cross-host scheduling
or replacement placement.

[Registry tests](../crates/sandbox-store/tests/host_registry.rs) cover legacy
backfill over pre-0025 data, pool validation and bounds, blocked type changes,
unique project grants and dedicated pools, duplicate endpoint/certificate/provider
identities, certificate mismatch, restart with a new epoch, refused endpoint
changes outside drain, heartbeat immutability and stale/never-seen freshness.
[Discovery unit tests](../crates/sandbox-controller/src/host_discovery.rs) cover
routing by pool type, duplicate discovery identities and reconciliation reports.
These simulated identities make no gRPC connection.

[Multi-host placement tests](../crates/sandbox-store/tests/multi_host_placement.rs)
use two to five simulated hosts per test. They cover racing workers with no
overbooking or duplicate owners, contiguous permit serials, spread selection,
pinned existing allocations, capacity still held by uncertain
and previous-epoch allocations, and exclusion of stale, draining, unregistered or
incompatible hosts. They also cover drain/placement races, denial for dedicated
pools and missing grants without fallback, quota under concurrency, the bounded
wait and project fairness in claiming.
[Controller tests](../crates/sandbox-controller/tests/create.rs) check grant
denial and the bounded wait through the controller loop. No real supervisor
or VM takes part.

[Provisioning tests](../crates/sandbox-store/tests/host_provisioning.rs) cover a
new draining host, exact and competing retries, changed-input conflicts,
legacy refusal, bounds, authenticated-observation gating and retained allocation
claims on a busy draining host.

These PostgreSQL/simulated-host checks validate maintenance coordination, not a real host drain or backup/restore procedure. Supported-host qualification, independent operator acceptance and future multi-host/cloud/provider decisions remain the [roadmap](roadmap.md). Saved-memory draining requires verified full pause/resume; it is not implied by this command.

[Epoch tests](../crates/sandbox-store/tests/host_epochs.rs) cover closed-placement
requirements, duplicate requests, stale and overflowing epochs, unhealthy
recovery, fresh-observation gating, exact retries and retained old-epoch
allocation/operation records. They exercise database authority with no physical
host restart.

## Standalone durable capacity operations

The standalone adapter (#196) enrolls **existing operator-supplied** machines;
it does not purchase, boot, stop or terminate infrastructure. The application
capacity router dispatches exactly `host_pools.type` (`standalone` or `k8s`);
an unconfigured Kubernetes adapter fails closed. Sandbox operations, project
access, placement reservations and supervisor gRPC/mTLS remain unchanged.

Migration 0027 adds `capacity_operations`: pool, optional warm-pool seam (used
by `warm_prepare`/`warm_retire` intents since [migration 0030](warm-pools.md)), kind, target, idempotency key, retained request,
status, claim revision, database-clock lease, provider receipt and fixed error.
Pool-scoped exact retries preserve the ID and receipt; changed input conflicts.
Only one live claim can apply an intent. Expired/replaced claims cannot commit
host effects or receipts. Static effects and completion commit in one database
transaction; future external providers must reconcile external receipts and
fencing before replaying uncertain calls. The router rejects stale claims
before selecting the adapter. Unfinished intent blocks pool type changes;
future adapters must also freeze effect-defining configuration/credential
references and enforce provider-side idempotency or fencing at dispatch.
A lease check before a network call cannot prevent expiry during that call.
The trait alone does not supply that protocol. Pending operations prevent pool type changes even with zero hosts.

Use the existing private database configuration and `0600` input-file rules:

```sh
sandbox-host-manager --database-config /private/database.json capacity-enqueue \
  --pool-id POOL_UUID --idempotency-key UNIQUE_RETAINED_KEY \
  --request-config /private/capacity-request.json
sandbox-host-manager --database-config /private/database.json capacity-tick --pool-id POOL_UUID
sandbox-host-manager --database-config /private/database.json capacity-status --operation-id OPERATION_UUID
sandbox-host-manager --database-config /private/database.json capacity-cancel \
  --pool-id POOL_UUID --operation-id OPERATION_UUID
sandbox-host-manager --database-config /private/database.json capacity-plan \
  --pool-id POOL_UUID --waiting-sandboxes 4 --sandboxes-per-host 2 --max-additional-hosts 1
```

The request is `{"host":"hst_...","epoch":1,"action":{"kind":"status"}}`.
Kinds are `status`, `drain`, `retire`, or `provision`; provision additionally
carries `config` with the existing `HostProvision` document and an explicit
matching `host_pool_id`. Provision reuses ordinary bounded enrollment and starts
draining with no observation. Set transport identity, authenticate health and
explicitly finish drain through the existing manager commands. Drain blocks new
placement and retains running work. Status receipts describe their completion
snapshot; use registry/status again for current health.

Retire requires draining, zero unreleased allocations, zero unretired warm-pool
members (`unretired_warm_members`) and fresh authenticated
health. Occupied, unhealthy, stale and uncertain hosts fail closed. Recovery must
first complete the existing verified allocation cleanup/fencing path; a lease
expiry, provider disappearance or operator assertion never releases capacity.
Retire records a permanent tombstone while preserving the host, transport,
histories and foreign keys. It cannot reopen through finish-drain, enrollment
retry or epoch advancement. It does not authorize physical machine termination.
Retired tombstones do not count against `max_hosts`, demand inventory or pool
type-change worker counts. Active-only endpoint/certificate/provider-reference
uniqueness permits replacement enrollment while retaining historical identity.
Pool type changes still require no active workers or unfinished capacity work.

Each tick claims at most one intent, with a 30-second lease and no background
retry loop. On refusal or a lost response, inspect `capacity-status`, resolve the
host condition and rerun after the lease expires. Claims accept 1–300 seconds;
the store exposes fenced explicit retry. Claims are ordered by each intent's
`available_at` turn, which every claim moves to its lease end and every explicit
retry moves to now: a repeatedly failing older intent rejoins behind intent that
was already waiting (for example a later drain its retire depends on) instead
of starving it. Each intent is claimed at most 8 times; an exhausted intent
stays unfinished, blocking pool type changes, until cancelled.

`capacity-cancel` marks a stale intent `cancelled` only in a `standalone` pool,
only while it is unclaimed (queued or lease expired) and has no receipt. Static
effects commit atomically with their receipt, so such an intent is proven
unapplied; cancellation takes the pool lock first, serializing with apply.
Live claims, completed receipts and intents of non-static pools, whose external
effects may be uncertain, are refused and retained. Cancelled intents keep their
idempotency key (an exact retry returns the same ID) and no longer block pool
type changes. Outstanding intents are bounded to
1000 per pool; completed idempotency records are retained, never automatically
deleted. Active inventory reads retain the registry's 10000-host bound. Demand is an
explicit advisory input (at most 100000 waiting sandboxes, 1–100000 sandboxes per
host, at most 1000 additional hosts); the plan caps additions by `max_hosts`,
active inventory, minimum ready workers and the operator's request budget.
It cannot infer compatibility, actual packing or resource guarantees from a
sandbox count and makes no automatic provision or scale-down decision.

Portable PostgreSQL tests are in
[`sandbox-store/tests/capacity.rs`](../crates/sandbox-store/tests/capacity.rs)
and [`sandbox-controller/tests/capacity.rs`](../crates/sandbox-controller/tests/capacity.rs).
They cover upgrade preservation, claims/retries, occupied/uncertain retirement,
retire-then-replace at `max_hosts`, bounded intent queues, adapter routing and
advisory demand. The [portable evidence](evidence/2026-10-30-standalone-capacity.md) records
focused executable validation of the capacity adapter. A light
psql check of the full migration chain, pre-0027 host preservation and lease/
receipt constraints passed in an isolated temporary database on the existing
local PostgreSQL service. No real Linux/KVM drain, retirement, performance or
isolation evidence is claimed here.

## Fleet diagnostics and incident runbooks

The offline operator command `sandbox-host-manager --database-config <private-file> fleet-status`
reads one bounded database snapshot. `fleet-status --prometheus` prints fixed-vocabulary
Prometheus gauges for an operator-managed collector; it starts no listener. Database
access is the operator authorization boundary. Do not forward this output through a
project route. Host IDs/epochs remain operator-only; endpoints, certificate identities,
provider references, credential references, payloads and terminal output are omitted.
Project operation reads retain project authorization and fixed placement rejection codes.

Reservations include every unreleased allocation, including unknown and previous-epoch
owners, plus unassigned [warm-pool](warm-pools.md) member reservations, with the same
VMM memory and artifact disk overhead used by placement. Diagnostics reuse placement's
warm-held predicate (`placement::WARM_HELD`), so a ready, preparing or retiring member
reduces `available` by exactly what placement charges, and a claimed member is charged
once through its allocation. Per-host `warm_held` attributes the warm share (member
count and resources) in operator JSON. The
live snapshot enumerates active identities only, bounded by the active registry limit;
retired inventory history is separate and cannot exhaust this diagnostic bound. Available means accounting
headroom, not permission to place: readiness, freshness, policy and compatibility still
apply. Queue age uses database admission time. Renewal lag measures time since the last
allocation update while a renewal is pending, not supervisor watchdog lateness. Confirmed
fenced counts require retained `fenced_absent` release evidence; unavailable, stale,
draining, or a new epoch alone never establishes physical fencing.

- **Drain:** use the existing epoch-bound `drain`, then inspect active reservations,
  unknown allocations and pending cleanup. Finish existing work or reconcile explicit
  termination evidence. Never refund reservations because the queue is slow. `finish-drain`
  reopens only after the existing empty/fresh checks; it does not retire a host.
- **Host loss:** inspect freshness, epoch and previous-epoch/unknown allocations. Restore
  authenticated connectivity and reconcile the owner; retain capacity while unreachable.
  Independent fencing or verified supervisor release evidence must precede replacement.
  A diagnostic unavailable state must never be interpreted as confirmed fenced.
- **Capacity exhaustion:** compare reserved and available resources including overhead,
  active versus retired inventory, queue age and the fixed rejection counts. Check project
  quota and pool grants before adding operator-managed capacity. No fallback outside an
  allowed pool, automatic paid provisioning or forced reservation deletion is authorized.
- **Certificate rotation:** drain first; use epoch-bound `set-transport` with private
  configuration and the approved CA/server identity. Changing transport invalidates
  freshness. Confirm authenticated health at the expected epoch before reopening. Failed
  authentication means unavailable, not stopped or fenced. Keep old reservations until
  reconciliation; certificate replacement is not allocation termination evidence.

Portable SQL/fake-host tests and Linux/KVM qualification are separate evidence. These
read models establish neither physical isolation nor cross-host recovery qualification.

### Fleet telemetry interpretation

Placement queue gauges count queued **create** operations and lease-free running
creates in `awaiting_placement`, only with zero dispatch attempts, empty attempt
receipts and no allocation. Dispatched running/unknown work, commands, uploads
and cleanup operations do not represent demand for a new host. Rejection gauges
count retained failed create records for the fixed placement reason vocabulary,
including pool-access denial. They are database snapshots, not monotonic counters:
retention may reduce them. Unknown error text never becomes a metric label.

Host state gauges form an exclusive partition: ready, draining, then unavailable.
The `retired` gauge counts retained retired-host tombstones (`retired_hosts` in the
JSON snapshot); tombstones are counted, never enumerated, so history cannot exhaust
the active host bound. A stale draining host
remains in the draining bucket; inspect its JSON freshness separately. Capacity
totals exclude retired inventory tombstones.
`hudsonbox_fleet_warm_held_members` and `hudsonbox_fleet_warm_held{resource="..."}`
sum unassigned warm reservations over active hosts; they are already included in
`hudsonbox_fleet_reserved`, not additional to it.
Reserved totals retain every unreleased charge, including runtime overhead and
uncertain owners; available capacity is accounting headroom and does not promise
that a host is eligible for placement. Readiness still requires compatible image,
project pool authorization and request-specific capacity checks at placement.

Validation added for telemetry cardinality, exclusive states, tombstone capacity,
read-only two-host freshness transitions and project-read field redaction remains
pending Rust/PostgreSQL execution until the shared heavy build slot is granted.
Per-host `rpc_failures` and `rpc_last_failure_at` are operator-only observations.
Controller health, lifecycle, authority, upload, output/history/metadata retirement
calls and registry-backed API file/live reads record returned gRPC `unavailable`,
`deadline_exceeded` and `unknown` statuses. These are transport-status failures;
they do not establish physical host loss. Application rejections, pool admission
Backoff/Busy, incompatible health metadata, database failures and missing freshness
do not count. The explicit health network deadline records `deadline_exceeded`;
cancellation by an enclosing worker deadline can leave an unobserved attempt.

One fixed-size JSON record lives on each existing host row. Writes are atomic,
epoch-checked, skip retired hosts, saturate at the bigint limit and have a 100 ms
best-effort deadline. Recording failure emits a generic warning and preserves the
original RPC result. Counts survive process restart and successful health; late
old-epoch replies cannot mutate the new incarnation. They never change readiness,
leases, reservations or release evidence. No error message, endpoint, project or
request identifier is recorded. Legacy readers without a registry store cannot
persist observations.

`hudsonbox_fleet_rpc_failures{reason="..."}` sums the three fixed categories over
active hosts; no host/project label is exported. It is a snapshot gauge, not a
monotonic counter: retired identities leave the snapshot. Private operator JSON
provides per-host attribution without creating unbounded metrics series. Transport
status observations, fresh health and confirmed fencing remain separate evidence.
