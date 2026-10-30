# Data models, IDs, and storage

Status: partially implemented; the initial PostgreSQL schema and storage operations have tests. This document owns fields, relationships, identity formats, database constraints, and object-storage references. [Auth design](auth-design.md) owns credential/session enforcement; [API contract](api-contract.md) owns retry behavior; [lifecycle](lifecycle.md) owns transitions and completion evidence.

Implementation evidence: [the initial migration](../migrations/0001_initial.sql) creates projects, hosts, sandboxes, operations, and allocations. [Schema tests](../crates/sandbox-store/tests/schema.rs), [claims](../crates/sandbox-store/tests/claims.rs), and [single-host reservation tests](../crates/sandbox-store/tests/placement.rs) exercise PostgreSQL constraints and concurrency. The [source-field upgrade](../migrations/0002_observation_source.sql) and [upgrade test](../crates/sandbox-store/tests/upgrade.rs) preserve observation provenance; [create completion](controller.md) records evidence transactionally. Snapshot and UI security schemas remain planned; database reservations are not proof that a VM exists or is isolated.

[Execution ownership](controller.md#command-admission-and-dispatch-ownership) adds a nullable `operations.execution_allocation_id`, a same-sandbox allocation foreign key, and a unique active pinned command per sandbox. New store admission always pins an allocation; legacy rows retain unknown ownership and cannot dispatch.

The original target design defines six sandbox resource tables plus two supporting UI security tables, not a user/role directory. Supporting migrations and the [accepted v1 fleet and warm-pool model](#accepted-v1-fleet-and-warm-pool-model) extend that baseline. The schemas below include planned records; migrations determine the implemented subset.

## ID format and identity

**One sandbox keeps one identity through create, execute, pause, and resume.** A snapshot identifies saved state. An allocation identifies one attempt to run that sandbox as a VM. An operation identifies one requested action.

Destroy permanently retires the sandbox ID. Creating another sandbox always creates a new ID, even when its display name or image is the same. Copying saved state into a different sandbox would be a future explicit fork operation, not ordinary resume.

Hudson is an ordinary client. None of these identities depend on a Hudson run, a Temporal workflow, a Kubernetes pod, an IP address, or a guest process ID.

Use a short resource prefix followed by a canonical lowercase, hyphenated UUIDv7:

```text
sbx_01996110-7c00-7000-8000-000000000001
```

This is an illustrative valid-format ID. The prefix helps people distinguish resources in API responses and logs. Generate the UUID once in the trusted service using an established Rust UUID implementation. Store its underlying value in PostgreSQL's `uuid` type; attach or remove the fixed prefix at the API boundary. There is no separate public-to-internal ID mapping table.

UUIDv7 carries a millisecond timestamp and supports time-oriented index locality. It does not establish strict ordering across machines, prove freshness, or act as a secret. Keep explicit timestamps, revisions, and generations for those purposes. Sources: [RFC 9562, UUIDv7](https://www.rfc-editor.org/rfc/rfc9562.html#section-5.7), [PostgreSQL UUID type](https://www.postgresql.org/docs/current/datatype-uuid.html).

The parser accepts the exact resource prefix and canonical UUIDv7 format; reject wrong types, truncated IDs, malformed values, and alternate spellings. Keep database uniqueness constraints and handle the unlikely generation collision before publishing an ID. Do not hand-roll the generator, truncate random bits, or make IDs out of database row counts.

IDs are opaque identifiers, never bearer credentials. They may reveal approximate generation time. Authorization applies to every lookup regardless of whether the identifier is known, guessed, or supplied in a storage path.

The notation `<uuidv7>` below represents the full canonical UUID, not a literal API value.

| Record | API format | Lifetime and purpose |
| --- | --- | --- |
| Project | `prj_<uuidv7>` | Stable tenant boundary for ownership, quotas, and authentication |
| Sandbox | `sbx_<uuidv7>` | Stable environment identity across pause/resume; never reused after destroy |
| Operation | `op_<uuidv7>` | One create, execute, pause, resume, destroy, or other mutating request |
| Snapshot | `snp_<uuidv7>` | One immutable, completed save of memory, disk, and VM state; reserved while preparing |
| Allocation | `alc_<uuidv7>` | One reserved VM incarnation on one host; new on initial start, resume, or replacement |
| Host | `hst_<uuidv7>` | Internal registered machine identity; replacement/reprovisioning gets a new ID |

Most clients need only project, sandbox, operation, and snapshot IDs. Images are addressed by authorized immutable digests; outputs are retrieved through their producing operation. Allocation and host identities are internal/admin details. Host addresses are not exposed in ordinary sandbox responses.

Avoid extra resource types initially:

- An executed command is an operation of kind `execute`; do not add a second command/job ID for the same action.
- A controller attempt is `(operation_id, attempt_number)`, with a positive incrementing number. Its receipts live on the operation; it does not need a separate table or globally unique ID.
- A sandbox's generation is an increasing integer, not another UUID. It orders allocation replacements and rejects stale commands.
- An operation's claim revision is a separate increasing integer. It orders controller ownership, including when the allocation has not changed.
- Display names and labels are optional mutable metadata. Names may repeat and are never API lookup keys or authorization inputs.

Provision project IDs independently of any harness. An optional external reference maps a Hudson workspace or another caller's organization; project deletion revokes access and never permits ID reuse. Local setup still requires authentication, as defined in [auth design](auth-design.md).

## Relationships

```mermaid
erDiagram
    PROJECTS ||--o{ SANDBOXES : owns
    SANDBOXES ||--o{ OPERATIONS : receives
    SANDBOXES ||--o{ ALLOCATIONS : uses_over_time
    HOSTS ||--o{ ALLOCATIONS : hosts
    SANDBOXES ||--o{ SNAPSHOTS : saves
    ALLOCATIONS ||--o{ SNAPSHOTS : captured_from
    OPERATIONS ||--o| SNAPSHOTS : pause_produces
```

The diagram shows historical relationships. A sandbox can have many allocations over its lifetime, but at most one can remain unreleased. A pause operation produces at most one snapshot; a no-op pause produces none.

## The six models

IDs use PostgreSQL `uuid`, with a typed UUIDv7 prefix added in the API. All tables have `created_at` and `updated_at` timestamps. Times use `timestamptz`; counters use `bigint`; bounded structured metadata uses `jsonb`. The fields below describe the logical schema, not executable SQL.

PostgreSQL 16 is the minimum supported version. Migrations are versioned SQL applied by `sqlx migrate`, which is already in the stack; no second migration tool is introduced. Both a fresh install and a supported upgrade path are tested.

### 1. projects — ownership and limits

API ID: `prj_<uuidv7>`.

| Fields | Purpose |
| --- | --- |
| `id`, `name`, `status` | Stable owner identity and lifecycle |
| `limits` | CPU, memory, disk, snapshot storage, sandbox count, and pending-operation quotas. Initial defaults: 25 concurrent sandboxes per project, each bounded by the 4 vCPU / 8 GiB [supported ceiling](compatibility.md#sandbox) |
| `api_tokens` | Small bounded token metadata collection: key ID, SHA-256 hash, creation/expiry/revocation times; at most two active tokens for rotation |
| `external_reference` (optional) | Mapping to a caller's organization or workspace |

Project token metadata is stored on the project; the credential/session validation and rotation rules are owned by [auth design](auth-design.md#project-credentials). The Admin API/UI and local admin tooling manage project tokens; installation Admin credentials are maintained separately in controlled deployment configuration. Display names are not unique lookup keys. A project with no active tokens grants no Project access.

[Migration 0024](../migrations/0024_project_token_lookup.sql) indexes the token metadata for authentication lookup. A containment predicate selects candidate projects before expanding their token arrays, so an unknown key does not scan every project's credentials. The same query still reads current project status, token hash, expiry, and revocation metadata; the index does not cache an authorization decision. Token-index writes are applied directly rather than buffered in a pending list, favoring predictable authentication reads over batched credential updates.

### 2. sandboxes — the persistent environment

API ID: `sbx_<uuidv7>`.

| Fields | Purpose |
| --- | --- |
| `id`, `project_id`, `name`, `labels` | Identity and ownership |
| `image_digest`, `image_compatibility`, `resources` | Verified immutable starting image and requested CPU, RAM, and disk limits |
| `desired_state`, `observed_state`, `observed_at`, `observation_simulated`, `state_revision` | Intent, last confirmed state, observation freshness/source, and concurrent-update protection |
| `generation`, `current_allocation_id` (nullable) | Latest allocation generation and current compute reservation |
| `current_snapshot_id` (nullable) | Published pause snapshot used for ordinary resume |
| `active_transition_operation_id` (nullable) | Serializes create/pause/resume/destroy transitions |
| `expires_at`, `destroyed_at` (nullable) | Sandbox lifetime and permanent destruction tombstone |

The API derives its visible status from the observed state and pending transition. The sandbox keeps its ID across pause/resume. Destroy permanently prevents further execution or resume.

### 3. operations — actions, retries, and results

API ID: `op_<uuidv7>`.

| Fields | Purpose |
| --- | --- |
| `id`, `project_id`, `sandbox_id`, `kind` | One admitted create, execute, pause, resume, destroy, cancel, or file mutation |
| `initiator_kind`, `initiator_key_id`, `initiator_session_id` (nullable where inapplicable) | Server-assigned `project`, `admin`, or `service` authority and audit references; no raw credentials or credential hashes |
| `idempotency_key`, `request_digest`, `digest_version` | Deduplicate the caller's mutation and reject changed payloads under the same key |
| `payload`, `input_refs`, `target_operation_id` (nullable) | Validated request, pinned image/snapshot inputs, and cancellation target |
| `status`, `phase`, `result`, `error`, `output_refs` | Progress, bounded results, and stored-output metadata |
| `attempt_count`, `attempt_receipts`, `receipt_history_ref` (nullable) | Dispatch, acknowledgement, reconnect, and completion evidence, including allocation and claim revision |
| `claim_revision`, `lease_expires_at`, `next_retry_at`, `deadline` | Controller ownership and bounded scheduling/retries |
| `output_status`, `output_claim_revision`, `output_lease_expires_at`, `output_next_retry_at` | Independent archival progress and publisher ownership after command completion |
| `output_ticket`, `output_plan`, `output_expires_at` | Stable upload attempt, immutable pair of final stream plans, and byte-retention expiry |
| `response_expires_at`, `completed_at` (nullable) | Result retention and completion time |
| `payload_compacted_at`, `command_summary`, `payload_compaction_next_at` | Atomic body removal, private command digest/deadline/limit summary, and corruption deferral |

Enforce `UNIQUE (project_id, idempotency_key)` on this table; [API admission](api-contract.md#retries-and-admission) defines matching, conflicts, and tombstone behavior. Every client-admitted operation records its initiating project/admin credential ID and optional UI session reference. Only authenticated internal maintenance may omit the credential ID. Null fields never create service authority, and client payloads cannot assign initiator identity. Admin actions on a sandbox retain that sandbox's project ownership.

Persist phase and confirmed execution evidence without treating desired state as observed state. [Lifecycle](lifecycle.md) owns phase ordering, claim/lease behavior, and safe continuation; [auth design](auth-design.md#storage-and-audit) owns reauthorization and audit semantics.

Bound retries and receipt metadata. Preserve earlier allocation receipts when reconnecting after resume. If history exceeds the inline bound, publish an immutable history object and persist its reference before removing inline entries; do not discard unresolved execution evidence. No separate attempt table is required initially.

The shared retained-output descriptors, conditional S3 transport, supervisor archival and independently fenced publication into `operations.output_refs` are implemented in [output storage](output-storage.md). The array holds exactly the selected stdout and stderr references, while tickets and plans remain available for reconciliation and cleanup. Authenticated byte retrieval and SSE are connected; a successful upload does not change an operation's execution status or authorize reads.

Private `output_cleanup` rows are an auxiliary inventory for expired operation output, not a public artifact model. [Migration 0007](../migrations/0007_output_cleanup.sql) records a unique operation foreign key, claim revision/lease, retry time, immutable prepared manifest, earliest eligibility and creation time. Preparation freezes publication while preserving the exact ticket, plans and selected references. A manifest without references may describe orphan uploads; it is not proof that storage is empty. [Migration 0008](../migrations/0008_output_cleanup_completion.sql) adds paired `completed_at` and `receipt` fields. Completion requires a prepared manifest, positive claim revision, elapsed eligibility and cleared lease/retry fields; the store validates both retirement receipts or explicit absence of upload authority under the live claim. The [independent cleanup worker](output-storage.md#cleanup-worker) persists completion after storage verification. Completed rows are excluded from claims, and exact receipt retries reconcile lost acknowledgements. Execution state and history remain unchanged. History compaction remains unfinished; no cleanup-completed status or private receipt is exposed publicly.

Keep compact operation tombstones with identity, ownership, retry key, request digest/version, and outcome for the project's lifetime; detailed payload/output retention may be shorter. Active/unknown operations retain reconciliation evidence. See [API retention behavior](api-contract.md#errors-and-retention) for how clients observe expiry.

[Operation response retention](operation-retention.md) implements opt-in assignment of `response_expires_at` for terminal operations and expiry enforcement on reads and retries. Deadlines are measured from `completed_at`, assigned once, and never changed by later worker policy. [Migration 0009](../migrations/0009_response_retention.sql) indexes terminal rows awaiting assignment without assigning policy during upgrade. [Migration 0010](../migrations/0010_payload_compaction.sql) enables separately opted-in removal of eligible payload/result/error fields while preserving retry identity and all receipts. Execute rows retain a validated command summary for later evidence verification. Unticketed pending output can be atomically fenced and expired; already authorized output must have verified retirement completion. Live-allocation [history retirement](history-reclamation.md) is separately opt-in; whole-allocation/previous-epoch reclamation remains unfinished.

### 4. hosts — Linux compute machines

Internal ID: `hst_<uuidv7>`.

| Fields | Purpose |
| --- | --- |
| `id`, `status`, `last_seen_at` | Registered machine identity, readiness/draining, and health observation |
| `cpu_capacity`, `memory_capacity_mib`, `disk_capacity_mib`, `compatibility` | Schedulable resources after OS/cache headroom and supported VM/image configuration |
| `runtime_capabilities`, `runtime_capabilities_epoch` | Matching authenticated [capability observation](host-capabilities.md), bound to the current epoch; never operator policy or physical isolation proof |
| `max_snapshot_uploads` | Maximum concurrent snapshot uploads/staging admissions on this host |
| `supervisor_epoch` | Increasing registration epoch that rejects stale supervisor messages |

Hosts are infrastructure records shared across projects. Calculate reservations from unreleased allocations under transactional capacity checks. A heartbeat timeout makes a host uncertain; it does not prove its VMs stopped. The operator currently provisions a fresh increasing supervisor epoch after restart; automatic epoch issuance remains planned. It is separate from an OS boot ID and requires reconciliation before existing ownership is renewed. Authenticated health enrolls capability metadata only against a separately approved operator policy, and cannot create a host or change its epoch/capacity.

[Command cancellation](command-cancellation.md) uses the existing cancel kind and target foreign key. [Migration 0012](../migrations/0012_command_cancellation.sql) permits one unfinished `cancel_requested` operation per target without activating legacy generic rows. Status/list views expose `target_operation_id`; response expiry and compaction preserve it. Cancellation owns interruption requests and completion reporting, while the original execute claim owns any supervisor call.

### 5. allocations — compute reservations

Internal ID: `alc_<uuidv7>`.

| Fields | Purpose |
| --- | --- |
| `id`, `project_id`, `sandbox_id`, `host_id` | Places one sandbox incarnation on one host |
| `generation`, `supervisor_epoch` | Rejects commands from old VM incarnations or supervisors |
| `vcpu`, `memory_mib`, `disk_mib`, `status`, `lease_expires_at` | Reserved CPU, RAM, writable/restore disk and ownership lifetime |
| `release_evidence`, `released_at` (nullable) | Confirmed termination/fencing and release of the reservation |
| `maintenance_revision`, `maintenance_lease_until`, `maintenance_next_at` | Independent durable claim and scheduling for running allocation maintenance |
| `lease_requested_until`, `renewal_pending`, `lease_observation` | Renewal intent and bounded latest supervisor evidence, kept separate from the last known execution deadline |

Pause releases the allocation only after durable snapshot publication, confirmed VM shutdown, and cleanup of its reserved local resources. Snapshot staging is a separate reservation on the snapshot until its own cleanup completes. Resume creates a new allocation ID and increasing generation. A failed allocation consumes its generation; it cannot be reused.

### 6. snapshots — saved sandbox state

API ID: `snp_<uuidv7>`.

| Fields | Purpose |
| --- | --- |
| `id`, `project_id`, `sandbox_id` | Saved-state identity and ownership |
| `source_allocation_id`, `pause_operation_id` | Which VM and pause request produced the state |
| `status`, `upload_attempt_number` | Preparation, publication, and cleanup progress |
| `staging_host_id`, `staging_reserved_mib`, `staging_released_at` (nullable), `upload_slot_held`, `upload_lease_expires_at` | Temporary disk and upload-slot reservations, retained until cleanup/termination evidence |
| `manifest_key`, `manifest_version`, `manifest_digest`, `compatibility` | Verified immutable references to matching memory, disk, and VM-state objects |
| `chunk_layout` | Chunk or page size and offsets for the memory component, so a restore can fetch ranges instead of whole objects |
| `published_at`, `expires_at`, `deleted_at` (nullable) | Publication and retention lifecycle; snapshots expire 7 days after publication by default, operator-configurable |
| `encryption_key_id` | Which key the components are encrypted under, so per-project keys can replace the installation key without invalidating published snapshots |

PostgreSQL stores metadata; object storage holds the large files. One pause operation reserves one snapshot ID across retries. A published manifest is immutable, and an incomplete upload cannot become resumable state.

`chunk_layout` and `manifest_version` exist for a deferred feature on purpose. Differential snapshots and lazy loading are Phase 5 work, but a published format that only supports whole-object reads would have to break every existing snapshot to get there. Recording the layout and versioning the manifest from the first implementation costs little and keeps that path open. [Performance](performance.md#constraints-on-designs-we-are-choosing-now) owns the reasoning.

Reserve worst-case staging capacity and a host upload slot before freezing the guest. Serialize upload attempts per snapshot initially; a new attempt cannot overwrite or replace the previous reservation until its uploader is stopped and leftover bytes are accounted for. Release the upload slot after confirmed upload completion/termination; release staging bytes only after confirmed file deletion or host storage retirement. Expired leases alone release neither. No additional reservation table is required.

## What we keep inside these models

| Deferred table | Initial representation |
| --- | --- |
| `request_keys` | Retry key and digest fields on `operations` |
| `operation_attempts` | Bounded attempt receipts and history references on `operations` |
| `image_versions` | Verified image digest and compatibility pinned on the sandbox/create operation; an operator-configured image allowlist controls permitted inputs. An allowed image must carry our init and guest agent, and the guest kernel is supplied separately by the supervisor rather than being part of the image ([supported configuration](compatibility.md#how-a-sandbox-boots)) |
| `usage_records` | Derived from allocations and operations rather than sampled; revisit before any billing, per-project reporting, or hosted offering depends on it |
| `artifacts` | Output references on `operations`, containing object key/version, digest, size, retention, and cleanup state |

There are no public image or artifact IDs initially. Retrieve outputs through their authorized operation. Output names identify entries within that operation, not arbitrary bucket paths. Separate catalogs or artifact management can be introduced when needed.

Both deferrals are now deliberate rather than unexamined. The first release ships operator-allowlisted images only, with per-project images named as the next capability so the limitation is visible rather than discovered; customers install their own dependencies at runtime, which is what guest root and the egress allowlist are for. Usage is derived from allocations and operations rather than sampled, which is adequate while nothing is billed from it and must be revisited before anything is.

## Supporting UI security records

These two tables are additional to the six resource models. The authoritative session lifetimes, credential bindings, audit atomicity, and admin mutation retry rules are in [auth design](auth-design.md#storage-and-audit).

| Record | Main fields |
| --- | --- |
| `ui_sessions` | ID, unique session hash, principal kind (`project` or `admin`), credential ID/config revision, nullable project ID (required for Project), CSRF verifier, created/last-activity/absolute-expiry/revoked timestamps |
| `audit_events` | ID, time, principal kind/credential ID, session ID if applicable, action, target project/resource, request ID, safe change summary, outcome, resulting operation/reference; mutation idempotency key and request digest where applicable |

A Project session requires a project ID; an Admin session is installation-scoped. Session secrets and CSRF verifiers are security metadata, never plaintext project/admin credentials. Admin credential IDs/config revisions refer to administrator-controlled deployment configuration; project credential IDs refer to the owning project's token metadata. Validate those references at use, not only at session creation.

Use a unique session hash. Administrative admission receipts require the unique admin-credential/idempotency-key contract specified by auth design, separate from sandbox operations' project/key uniqueness. Detailed audit expiry must not remove compact deduplication receipts. Schema/index definitions remain migration work.

## Database access and migrations

**Use PostgreSQL with SQLx and explicit parameterized SQL; no ORM.** Keep database queries, row mapping, and transaction boundaries together in the proposed `sandbox-store` crate. API handlers and lifecycle controllers call focused storage functions rather than scattering SQL across services. PostgreSQL remains the source of durable metadata; SQLx is the Rust access library.

Bind customer-supplied values as query parameters instead of interpolating them into SQL strings. Any dynamic identifiers or sort expressions must come from trusted, fixed choices. Map results into Rust types and keep ownership checks, operation admission, and resource reservations within the transactions required below. SQLx does not supply tenant authorization or correct locking automatically.

Use SQLx connection pooling and explicit transactions. Its query macros can check queries against the database schema at build time; use them where practical, with a test schema or prepared offline metadata. Keep that metadata consistent with migrations in CI. Dynamic queries still need runtime validation, and compile-time checks do not replace PostgreSQL integration tests for concurrent claims, constraints, or isolation.

Maintain versioned SQL migration files in the repository. Review schema changes alongside the storage code that uses them; do not edit migrations already applied to a released installation. Validate a fresh database and upgrades from supported schema versions before release. Keep migration execution an explicit deployment step with one coordinated runner, rather than letting every API/controller replica change the schema independently. Exact migration paths, tooling commands, and upgrade/rollback procedures remain implementation work.

No SQLx dependency, migration files, or database access code exists yet. Pin the library version and select runtime/TLS features during implementation. The library's capabilities are described in the [official SQLx documentation](https://github.com/transact-rs/sqlx).

## Database rules

- Tenant-owned references carry `project_id`. Composite foreign keys must also ensure sandbox-local links point to the same sandbox: current allocation/snapshot, active transition, cancellation target, and snapshot source/pause operation.
- Enforce `UNIQUE (sandbox_id, generation)` and a partial unique constraint on `allocations(sandbox_id)` where `released_at IS NULL`.
- Enforce `UNIQUE (pause_operation_id)` on snapshots. Verify the referenced operation is a pause for the same sandbox before publication.
- Serialize lifecycle transitions with row locks/state revisions. An execute operation can stay suspended while a separate pause/resume operation runs.
- Publish the verified snapshot manifest and current snapshot pointer transactionally. Mark the sandbox paused only after allocation release is confirmed.
- A generation or expired database lease alone does not stop a VM. Require confirmed shutdown, infrastructure fencing, or the validated host lease watchdog before replacement execution.
- Perform reservation and project/host quota checks transactionally. Local disk use includes unreleased allocation disk plus unreleased snapshot staging; image cache/OS headroom is excluded from schedulable capacity. Snapshot upload slots are bounded separately. Unknown reservations stay counted until safely released, and the supervisor checks actual free disk before writes.
- Expiring outputs must not remove retry protection or unresolved receipts. Project deletion revokes access, drains resources, and completes cleanup before purging records; it never permits ID reuse.

Snapshot contents are immutable after publication; retention/deletion metadata may change. Controller receipts cannot overwrite confirmed outcomes under stale ownership. IDs and UUID timestamps never substitute for explicit revisions and transactional comparisons. All references to source allocations and producing operations must remain in the same project and sandbox.

## Implemented single-host reservation

[Reservation storage](../crates/sandbox-store/src/placement.rs) locks the current operation, project, sandbox, and selected host in that order. It counts all unreleased allocations, including uncertain allocations and allocations with expired leases. Two controllers cannot overbook a host or a project through this path. A lease expiring during lock acquisition rolls back the entire reservation.

Initial project allocation quota keys are `sandboxes`, `vcpu`, `memory_mib`, and `disk_mib`. Omitted keys default to 25, 100, 204800, and 1638400 respectively: 25 sandboxes at the current per-sandbox ceiling. Zero denies new reservations; negative or malformed values fail closed. These are allocation limits, not admission limits on queued sandboxes or pending operations; those admission checks remain to be implemented.

Fresh reservations require the persisted project credential to remain active, unexpired, and unrevoked, with an active project and unexpired operation/sandbox deadlines. The selected host must be ready, have the expected positive supervisor epoch, and have an observation within 30 seconds. The controller authenticates host health through its configured mTLS identity. Enrolled hosts additionally require matching operator/runtime capability metadata at the current epoch, an advertised image and compatible resource limits before reservation and first dispatch. Empty unenrolled policies retain the explicit legacy development path. The storage primitive does not independently authenticate a host or inspect image bytes; artifact verification remains a separate boundary.

Reservation publishes the allocation pointer, generation, and a `reserved` receipt atomically. It never reports guest readiness or increments dispatch attempts. Existing allocations return their identity for reconciliation instead of creating replacements, even when new execution is no longer authorized. A successful reservation does not authorize later dispatch without a fresh check, and no release operation is implemented here.

## Object-storage layout

Store raw snapshot components under one snapshot. Outputs, logs, and file exports are referenced by their producing operation; they have no separate public artifact ID initially. The full prefixed IDs below are represented by placeholders for readability.

```text
projects/{project_id}/sandboxes/{sandbox_id}/
  snapshots/{snapshot_id}/uploads/{upload_attempt_number}/
    memory.bin
    disk.img
    vm-state.bin
    manifest.json
  operations/{operation_id}/outputs/{output_name}/{upload_attempt_number}/content
```

Upload attempts get distinct paths. An interrupted or stale uploader must not overwrite the objects selected by a completed publication. Use immutable/conditional writes or pinned object versions, and put the exact keys, sizes, digests, source allocation/generation, format version, chunk layout, and compatibility data in the manifest. The layout entry is what lets a restore issue ranged reads against `memory.bin` instead of downloading it whole. PostgreSQL publishes one verified manifest; clients never infer readiness by listing a prefix.

Unpublished upload attempts can be garbage-collected after their leases and retention expire. Published components remain referenced until deletion policy permits removal. The controller must distinguish abandoned uploads from an in-progress publication before deleting bytes.

The prefix is organization, not security. Resolve operation output names and snapshot IDs through authorized metadata, keep buckets private, and issue only short-lived scoped transfer access where needed. Never accept caller-supplied bucket names or arbitrary object keys as trusted references.

Image digests resolve to verified, allowed immutable manifests with compatibility data pinned at create admission. No mutable tag, caller-controlled object key, or image catalog table is required initially. Snapshot publication records source allocation/generation and matching bytes; local staging reservations remain accounted for until cleanup is confirmed even if the allocation is released.

## Acceptance checks and open decisions

The initial schema, admission, claims, and reservation tests linked above cover a subset of these checks. Remaining coverage must verify typed UUIDv7 collision handling, supervisor epoch registration, replacement generations, unique pause snapshots, snapshot staging/upload accounting, and immutable verified snapshot/object references. Test that stale upload attempts cannot overwrite a publication or cause live bytes to be garbage-collected. No hardware isolation is established by storage tests.

UI session/credential constraints and audit admission must satisfy [auth acceptance](auth-design.md#acceptance-checks). Use [lifecycle](lifecycle.md) for release evidence and [API contract](api-contract.md) for retry response behavior. The session's stable-ID example lives in [lifecycle](lifecycle.md#identity-through-a-sandbox-session).

Open work: SQLx version/features and query-check setup, fresh and upgrade database tests, executable SQL types/enums, indexes/composite constraints, migration ordering, and storage cleanup tests. Development and CI run PostgreSQL 16 against MinIO for object storage. Link actual migrations and tests here once implemented.


## Collection read indexes

[Migration 0004](../migrations/0004_collection_indexes.sql) adds `(project_id, created_at DESC, id DESC)` indexes for sandboxes and operations, plus `(project_id, sandbox_id, created_at DESC, id DESC)` for filtered operation history. [Collection reads](api-contract.md#implemented-collection-reads) use strictly older key boundaries and bounded pages; they never use a cursor as authorization. Resource timestamps, state, and tombstones are unchanged by this migration.

## File upload reservations

[Migration 0013](../migrations/0013_file_uploads.sql) adds nullable `operations.file_allocation_id` and private `file_uploads` state. Composite foreign keys bind each file record to the same operation, project, sandbox and original allocation; only `file_write` operations may carry this pin. A partial unique index permits one active pinned file write per sandbox. Existing generic file rows remain unpinned and undispatched.

Each record holds the initiating token hash, normalized source plan/reference, declared size, acknowledged cursor, begin/commit/abort intent flags, inspection requirement and compact latest receipt. Bytes stay in private object storage. Constraints bound size/cursor and prevent commit without a complete admitted source. Application checks independently reconstruct ownership and request digests rather than trusting JSON fields. Global, project and allocation reservations are serialized at admission and count retained terminal/unknown rows. Completion and destruction do not reclaim those reservations. Verified source retirement can release global/project source-byte charges; retained counts and guest byte budgets require separate acknowledged [history retirement](history-reclamation.md). The [file-transfer contract](file-transfer.md#public-upload-orchestration) owns dispatch and recovery behavior.

[Migration 0014](../migrations/0014_file_source_cleanup.sql) extends each file record with independent source-cleanup state. A frozen manifest binds the original plan/reference; a cleanup revision/lease fences storage work without changing the operation claim. Paired retirement receipts/timestamps drive source-byte accounting; operation and guest histories remain intact. Fresh authorization denies source publication/selection after freezing. Original ownership is revalidated with the same identity helper used by dispatch; cleanup does not require a current allocation pointer, active token or current host epoch. [Source cleanup](file-transfer.md#source-cleanup-worker) owns eligibility, retry and completion semantics.

## History retirement state

[Migration 0015](../migrations/0015_history_retirement.sql) adds `allocation_history` keyed by allocation/domain, with independently fenced claims, reserved/completed prefixes, frozen boot context and retained request/completion proof. Allocations retain `last_admitted_operation_id` and per-domain scan timestamps. The shared `completed_allocation_history` view admits accounting refunds only when completion metadata matches the original scope and exact approved barrier. The [history contract](history-reclamation.md#database-claims-accounting-and-operation-ordering) owns locking, eligibility, retries and remaining lifecycle limitations.

[Migration 0016](../migrations/0016_released_history.sql) adds `released_allocation_history`, independent destruction claims with original scope, current reporting epoch, reserved/completed prefixes and exact request/completion evidence. The existing live proof view is retained separately; `completed_allocation_history` chooses one greatest verified prefix per allocation/domain across both proof types. A release timestamp schedules verification and cannot authorize a refund. Original operation and idempotency metadata remain retained.

[Migration 0019](../migrations/0019_allocation_retirement_preparation.sql) adds `allocation_retirements`, keyed by issued allocation permit. It retains the versioned immutable retirement intent, retry-stable retirement ID, reporting epoch, claim revision, lease and preparation timestamp. The allocation lock freezes admission and consumer scope before this row is inserted. Preparation alone is not physical cleanup; the [whole-allocation retirement contract](allocation-retirement.md#database-preparation-component) owns eligibility.

[Migration 0020](../migrations/0020_allocation_retirement_completion.sql) adds `metadata_completion`, `metadata_completed_at` and `metadata_claim_expires_at`. Completion retains the exact frozen request and physical observation, clears the active lease and preserves its original deadline for historical validation. Constraints reject partial, mismatched or simulated completion. Existing rows gain no invented proof. The [acknowledgement contract](allocation-retirement.md#database-metadata-acknowledgement-component) owns claim/epoch locking and lost-response reconciliation. The forgetting store/RPC handoff below consumes that completion; the [controller worker](allocation-retirement.md#automatic-controller-worker) dispatches the handoff.


## Allocation admission serials

[Migration 0017](../migrations/0017_allocation_serials.sql) retains immutable allocation/create permit identities and a transactional host-local serial counter. New [reservations](../crates/sandbox-store/src/placement.rs) issue the serial under the existing host lock; retries reuse their original record and rollbacks consume no serial. [Authority coordination](allocation-authority.md#transactional-database-issuance) owns bounded batch reads, legacy-owner migration limits and the still-unimplemented host registration/guardian enforcement. These rows do not establish cleanup or authorize tombstone deletion.

### Allocation forgetting

[Migration 0021](../migrations/0021_allocation_forgetting.sql) adds `forget_revision`, `forget_epoch` and `forget_lease_expires_at` to `allocation_retirements`, independently of the immutable metadata-completion claim. `forget_completion`, `forgotten_at` and `forget_claim_expires_at` retain the exact accepted forgetting response after its lease clears. Paired constraints bind it to the original metadata request and fresh forgetting claim. Application validators additionally check the historical clock window and current release/scope evidence. See the [forgetting handoff](allocation-retirement.md#database-and-host-forgetting-handoff).

[Migration 0022](../migrations/0022_allocation_retirement_scan.sql) adds `allocations.retirement_scan_at` and a partial released-allocation index for fair controller scanning. This timestamp is a retry scheduling hint, independent of immutable retirement intent, delivery claim leases and retained completion. See [automatic allocation retirement](allocation-retirement.md#automatic-controller-worker).

## Accepted v1 fleet and warm-pool model

Decision locked on 2026-10-30: the following logical models and ownership rules are the implementation baseline for multi-host and warm-pool work. **Accepted design does not mean implemented schema.** No migrations or runtime behavior are added by this decision. Changes to these boundaries need an explicit design revision; SQL types, indexes, protocol messages and migration/backfill mechanics are specified and tested in implementation PRs.

The existing `projects`, `sandboxes`, `allocations` and `operations` relationships remain. A sandbox is the stable customer identity; an allocation is one execution incarnation on a particular supervisor. Customer operations retain the existing durable operation model. Existing supporting upload, output cleanup, permit and history-retirement records remain necessary.

### Models and fields

New records use internal UUID identities and explicit creation/update timestamps. `?` denotes a nullable field. New public ID prefixes are not introduced by this decision.

| Model | Status | Accepted fields and purpose |
| --- | --- | --- |
| `host_pools` | New | `id`, `name`, `type`, `min_ready_hosts`, `max_hosts`, `placement_policy`, `provider_config`, `credential_secret_ref?`. Groups supervisor capacity under one deployment adapter. **`type` is exactly `k8s` or `standalone`.** |
| `hosts` | Extend | Existing identity, heartbeat, health, capacity, capabilities and supervisor epoch; add `host_pool_id`, `endpoint`, authenticated transport identity metadata and `provider_ref`. A host is the supervisor capacity boundary. |
| `project_host_pool_access` | New | `project_id`, `host_pool_id`, with a unique pair. Explicitly permits a project to place work in a pool; pool policy can restrict it to dedicated use. |
| `sandbox_templates` | New | `id`, `version`, `owner_project_id?`, `image_digest`, `resources`, `boot_config`, `compatibility`, `snapshot_id?`. An immutable template version describes an image plus runtime defaults, such as Linux with Node.js/npm and CPU/RAM/disk defaults. |
| `warm_pool_policies` | New | `id`, `template_id`, `host_pool_id`, `owner_project_id?`, `min_ready`, `max_total`, `idle_ttl`, `enabled`. Defines a target stock of unused VMs for one immutable template version and host pool. |
| `warm_pool_members` | New | `id`, `pool_id`, `host_id`, `supervisor_epoch`, `vm_ref`, reserved CPU/memory/disk, `state`, `lease_expires_at`, `claim_revision`, `claimed_allocation_id?`, `cleanup_evidence`. One prepared VM and its resource reservation. |
| `capacity_operations` | New | `id`, `host_pool_id`, `warm_pool_id?`, `kind`, `target_ref`, `idempotency_key`, `status`, `claim_revision`, `lease_expires_at`, `provider_receipt`, `error`. Durable provisioning, prewarming, draining and retiring work, separate from project/sandbox operations. |

`min_ready_hosts` and `max_hosts` count supervisor workers: standalone host services or Kubernetes worker Pods. They do not count physical Kubernetes nodes. Enforce nonnegative minimums, maximums at least their minimums, positive resource amounts and bounded retention/retry policy. `max_total` includes preparing, ready, claiming and retiring members whose reservations remain held; assigned members transfer to allocation accounting.

An immutable template version has its own `id`; pools pin that exact version. Changes create a new version rather than silently changing existing prepared members. Optional snapshot references must resolve to compatible, authorized, durably published snapshots under the existing snapshot contract. Ordinary boots are supported without snapshots. A template's owner must be authorized to use its image and any snapshot; public/shared templates require explicit publication policy. A null owner is not an access-control bypass.

### Provider routing

The **provider router is application code, not a table**. It resolves `hosts.host_pool_id` to `host_pools.type`:

- `k8s`: the Kubernetes adapter discovers/manages supervisor worker Pods, their resource envelopes and capacity demand. A separately configured node autoscaler may supply eligible nodes.
- `standalone`: the standalone adapter discovers/manages operator-registered Linux/KVM hosts. Automatic infrastructure provisioning requires a concrete provider implementation; it is not implied by the enum.

Both adapters preserve the same controller-to-supervisor gRPC/mTLS protocol. Existing allocations route directly to their persisted owner and epoch, never to an arbitrary worker behind a shared load balancer. Sandbox execution is independent of provider type.

Do not duplicate `type` on hosts, allocations or sandboxes. Validate `provider_config` against the selected type and store credential references, never secret values. Block pool type changes while workers, resource reservations or unfinished capacity operations exist. Provider references identify external resources; they never replace Hudsonbox ownership or authorization.

For Kubernetes, the existing host-as-machine terminology becomes host-as-supervisor-worker at this extension boundary. If multiple workers share a physical node, add `compute_nodes` and `hosts.node_id` with node identity, provider reference and physical capacity. Explicit worker budgets prevent spending the same physical capacity twice. Standalone deployments can map one worker to one machine. Replacing a worker creates a new identity; restarting the same provisioned worker requires a new epoch and ownership reconciliation. A Pod name/IP is not sufficient stable identity.

### Relationships and tenant boundaries

- A project owns many sandboxes; a sandbox has many historical allocations and at most one unreleased allocation.
- A host pool contains many hosts; each host supports many allocations and unused warm members within its capacity.
- A template version can serve multiple warm-pool policies. Each policy targets one host pool; each member belongs to that policy and one host in that same pool.
- A warm member may be claimed once and links to at most one allocation; an allocation claims at most one member. Enforce uniqueness and matching host/epoch/resources, in addition to normal sandbox/project ownership constraints.
- A dedicated warm pool belongs to one project. A shared pool has an explicit shared policy and only unused VMs without customer secrets. Claims require project-to-host-pool access and template authorization, even for shared pools.
- No placement fallback may escape a project's allowed pools. Project quota checks still apply when claiming an already prepared VM. Platform warm reservations consume host capacity before customer allocation and must have bounded operator budgets.

### Warm-member lifecycle and handoff

Normal progression is `preparing` → `ready` → `claiming` → `assigned`; unused or failed members progress to `retiring` → `retired` only after verified cleanup. Failed/unknown actions retain reservations until reconciled. An uncertain claim cannot make a VM ready for another customer. Assigned VMs are destroyed through their allocation lifecycle and **never returned to a shared warm pool**.

1. Persist the customer's sandbox and create operation under existing idempotency and quota rules.
2. Atomically claim one ready, compatible, authorized member using a claim revision and persist handoff intent tied to the target allocation identity. No ready member means bounded waiting or ordinary cold-start placement under the same capacity limits.
3. Obtain supervisor evidence for the ownership handoff, bound to member, allocation, project, host, epoch and claim identity. Persist the allocation and reservation transfer with retry-safe fencing. Database and supervisor state are not one transaction: recovery must inspect durable intent and receipts instead of assuming a timeout means nothing happened.
4. Mark the sandbox running only after verified allocation readiness. A claim or existing Firecracker process is not readiness evidence.
5. Replenish toward `min_ready` through bounded, durable capacity work; respect `max_total`, host capacity and provisioning limits.

Capacity accounting includes unreleased customer allocations **and** unassigned warm-member reservations. During handoff, count the reservation exactly once without an unreserved gap. The exact transactional transfer and supervisor protocol are implementation gates, not a claim that existing allocation launch permits already support VM adoption. Neither heartbeat expiry nor missing local records prove that a VM stopped. Safe drain, ownership fencing and cleanup evidence apply equally to warm members and assigned allocations.

### Delivery boundaries

Multi-host implementation is tracked by [#187](https://github.com/hudson-frontier/hudsonbox/issues/187) and its subissues. Host discovery/routing and atomic placement precede deployment adapters; warm pools extend the fast-startup scope in [#188](https://github.com/hudson-frontier/hudsonbox/issues/188). Checkpoints remain separately tracked in [#178](https://github.com/hudson-frontier/hudsonbox/issues/178).

Before shipping, test double claims, quota and tenant-pool denial, capacity transfer races, lost acknowledgements, worker replacement, stale epochs, controller restart, drain and failed cleanup. Fake/PostgreSQL tests and real Linux/KVM evidence must be reported separately. This baseline changes no deployment, release or infrastructure-purchase authorization.
