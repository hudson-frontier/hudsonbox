# Operation retention and payload compaction

Status: terminal response expiry, opt-in policy assignment and opt-in payload compaction are implemented. Receipt-history archival and database-coordinated reclamation remain unfinished. Internal [guest/host retirement RPCs](history-reclamation.md) are implemented but are not invoked by this worker. Response expiry changes access to the result body; compaction removes eligible request/result bodies while preserving execution outcome, retry identity, retirement evidence and resource ownership.

## Operator policy

The independent [cleanup worker](output-storage.md#cleanup-worker) accepts an optional installation-wide policy:

```sh
cargo run -p sandbox-cleanup -- \
  --output-config /absolute/path/cleanup-storage.json \
  --response-retention-seconds 604800
```

This example retains terminal responses for seven days after completion. There is no default policy: omitting the option leaves unassigned deadlines unchanged. The allowed range is one second through 365 days. Configure all workers consistently. Existing deadlines continue to apply if the option is later removed, and changing it only affects operations without a deadline.

Each tick assigns deadlines to at most 100 operations that have a confirmed `succeeded`, `failed` or `cancelled` status, a completion timestamp, and no `response_expires_at`. The deadline is `completed_at + policy`, not the time the worker discovers the row. **Enabling the option also applies to existing completed operations and can expire old responses immediately.** Queued, running and unknown work retain their unresolved evidence and are excluded from assignment.

The [store transaction](../crates/sandbox-store/src/retention.rs) uses row locks with `SKIP LOCKED` for concurrent workers. It never changes an assigned deadline, execution state, payload, receipt or retry key. Policy assignment has the worker's five-second database bound. It can progress without storage requests when no output attempt needs retirement. `--once` performs one bounded tick, including policy assignment if configured.

[Migration 0009](../migrations/0009_response_retention.sql) adds an index for pending policy assignment. Upgrading alone does not expire responses or alter existing operation records.

## Client behavior

For a terminal operation at or after its assigned deadline:

- `GET /v1/operations/{id}` returns `410 response_expired` with the original `operation_id` and no result/error body.
- An identical create, execute or destroy retry returns the same `410` and original identity. A changed request or incompatible stored digest version still returns `409`; expiry does not free the key or cause another dispatch.
- Operation lists retain identity, kind, status, phase and timestamps, add `response_expired: true`, and omit result/error bodies. Expired entries remain in pagination. The original outcome is preserved; `expired` is not a new execution status.
- Output reads and streams honor their existing response-retention bound and return `output_expired`. Retention does not extend an output ticket or grant new storage authority.

Current authentication and tenant ownership still apply. Missing and cross-project IDs return the same `404`; revoked credentials do not gain access to expired identities. All responses remain `Cache-Control: no-store`. Before expiry, existing response shapes are preserved and the optional `response_expired` field is absent.

Individual and list queries use one fresh database-time observation after lock waits for the expiry flag, result/error filtering and output-expiry metadata. Separate clock evaluations cannot label a body unexpired while filtering it as expired. The API checks expiry again before forming the body, retaining the database's expired decision even if its own clock is behind. Retry expiry is evaluated at database retry resolution, after digest comparison and before considering today's sandbox lifecycle. Active and unknown operations remain inspectable even if an out-of-band edit gave them an elapsed response deadline; ordinary policy assignment never does that.

## Payload compaction

Add `--compact-payloads` to the cleanup-worker command to enable irreversible removal of eligible request/result bodies. It is disabled by default and uses already assigned response deadlines; it can run with or without assigning a new retention policy. Enabling it can reclaim previously expired records immediately. Removing the flag stops further compaction but does not restore bodies.

Each tick attempts one [compaction transaction](../crates/sandbox-store/src/compaction.rs) within the worker's five-second database bound. It locks an expired terminal create, execute or destroy operation with `SKIP LOCKED`, excludes active transitions and unresolved lifecycle work, and validates its original request digest/version. Unsupported versions or corrupt evidence are preserved and deferred for one hour, with an operation-scoped `Deferred` result for operator investigation. Other eligible rows can proceed.

Output authority determines the next gate:

- With no authorized output (`none`), no storage operation is required.
- With a final command awaiting a ticket (`pending`), validate the execution receipts, then atomically expire publication, advance its independent revision, and clear the publication lease/retry. No ticket or object selector is invented. A stale publisher cannot mint an upload attempt after this transaction wins.
- With a previously issued ticket, wait for output retirement to complete. Revalidate its frozen manifest and both retirement receipts against the original execution, pinned allocation, plans and selected references before compacting. Grace periods, in-flight uploads, incomplete retirement and corrupt receipts leave the original body intact.

The transaction replaces `payload` with `{}`, clears `result` and `error`, and records `payload_compacted_at`. For execute operations it retains a private, versioned `command_summary` containing the command digest, deadline and output limit. This summary is generated from the validated original input; callers cannot submit it. Subsequent output/cleanup verification still binds it to the retained dispatch intent, final guest receipt and pinned allocation. In particular, an exact cleanup completion retry can still reconcile a lost acknowledgement after compaction.

[Migration 0010](../migrations/0010_payload_compaction.sql) adds the timestamp, command summary, retry time and discovery index. Constraints prevent a partial compacted row, restored request/result bodies, renewed response deadlines or reopened execution state while the compaction marker remains. Upgrade itself removes nothing. Cancellation rolls the transaction back; after a lost commit acknowledgement, the persisted marker prevents duplicate work.

## Retained evidence and remaining work

Expiry alone retains all bodies. After compaction, dispatch/completion receipts, outcome status/phase, ownership, request digest/version, idempotency keys, input references, output plans and retirement receipts remain. This implementation removes the three body fields from the current row; it does not erase PostgreSQL WAL, old MVCC versions, backups or copies in other systems.

Already-issued output plans keep their original immutable retention/grace timestamps. A newly assigned shorter response deadline stops public reads and publication but does not rewrite those plans or authorize early storage deletion. [Output retirement](output-storage.md#storage-retirement) completes independently at the original attempt's eligibility time.

Operation identity, project ownership, retry digest/version and outcome remain for the project's lifetime. Further receipt-history archival and host/guest journal reclamation require their own recovery protocol. Host and guest journals still have their documented fixed limits; this worker does not reclaim those reservations or make previously executed work runnable again. [Execute admission](controller.md#command-admission-and-dispatch-ownership) continues counting compacted commands and their original output limits toward per-allocation capacity.

## Evidence

[PostgreSQL tests](../crates/sandbox-store/tests/retention.rs) cover bounded batches, concurrent assignment and locked rows, completion-based deadlines, unchanged evidence, unresolved-work exclusions, immutable assigned deadlines, opt-in worker behavior and an upgrade over populated v8 data. [Router tests](../crates/sandbox-api/tests/retention.rs) exercise terminal reads, all three implemented retry routes, conflicts, tenant boundaries, revocation, pagination, unresolved operations and expiry during a database lock wait. The [executable test](../crates/sandbox-cleanup/tests/cli.rs) covers default-disabled behavior, invalid policies and actual assignment through `--once`. These are database/API tests with synthetic lifecycle evidence, not VM isolation tests.

[Compaction tests](../crates/sandbox-store/tests/support/compaction.rs) cover publication fencing, retained cleanup acknowledgement retries, corruption deferral, concurrent workers, cancellation/rollback, schema invariants, preserved unresolved work and populated v9 upgrades. [Cancellation tests](command-cancellation.md#validation) also preserve cancellation target identity and retry behavior through body removal. Router tests repeat create/execute/destroy retries after body removal; the executable test exercises the separate flag. An explicit PostgreSQL/MinIO test retires actual versioned output, compacts its body, verifies the old versions remain absent, and reconciles the original cleanup receipt through the compacted command summary.
