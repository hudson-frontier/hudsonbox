# Local upgrades and database recovery

This procedure covers offline operator coordination and a reproducible PostgreSQL rehearsal for [#116](https://github.com/hudson-frontier/hudsonbox/issues/116). It does not qualify a supported physical host or enable automatic recovery from an old database. [Host maintenance](host-maintenance.md), [allocation authority](allocation-authority.md) and [history reclamation](history-reclamation.md) define the ownership that must survive maintenance.

## Upgrade and rollback matrix

| Change | Permitted sequence | Rollback boundary |
| --- | --- | --- |
| API/controller/cleanup with unchanged schema and host protocol | Stop admission; finish or explicitly stop admitted work; stop workers; retain binary/configuration hashes; replace the complete compatible set; check health before reopening | Previous binaries may be reused only while every persisted schema/journal/protocol format they read is unchanged and they enforce the same admission and retirement rules |
| Database migration | Back up while the installation is quiescent; apply the release's complete embedded migration set once; repeat to check idempotence; start only matching application versions | No down migrations are supplied. An older application is not a supported way to undo a migration |
| Supervisor or guardian journal format | Drain; confirm physical release; stop the old process; retain original durable state; install matching binaries; explicitly advance the host epoch and authenticate the new observation | Never delete added fields, replace journals with old copies, reset serials, or run an old binary that cannot enforce retained fences |
| Guest/kernel/Firecracker/image policy | Qualify the exact digest/version tuple separately; approve the capability policy only on an empty draining host; obtain matching fresh health | Keep old pinned artifacts for their original allocations. A new policy does not rewrite existing allocation ownership or make snapshots compatible |
| Database/storage restore | Restore into quarantine, verify the full retained ledger and immutable objects, and reconcile independent physical ownership before any service can dispatch | A backup can predate issued epochs, permits, barriers, token revocations and external effects. Starting workers against it is unsafe without a separately verified recovery fence |

There is no released mixed-version compatibility promise. Record the source revision, migration versions/checksums, binary hashes, image/kernel/Firecracker digests, host capability policy and configuration format for both sides of any upgrade. The current schema is migration 0023; histories require migrations 0015/0016, launch registration 0017/0018, and whole-allocation retirement 0019–0022. The matching API, controller, cleanup and supervisor must all understand the active protocols. Existing migration regression tests check upgrades from retained older rows; they do not certify arbitrary historical binaries.

Before a supervisor restart, drain through the operator command, reconcile outstanding/unknown operations, confirm all required physical cleanup and stop the old supervisor. `advance-epoch` clears freshness and keeps placement closed. Configure the replacement with that issued epoch; matching health and `finish-drain` are separate steps. An epoch update alone neither stops a VM nor releases capacity. Saved execution requires the still-unimplemented full snapshot protocol.

## Capture an offline recovery set

Use an operator-owned private directory and PostgreSQL 16 client tools. Credentials belong in an owned `0600` libpq service/pass file, outside the repository and command arguments. The service must identify the intended database and approved TLS policy. Do not put credentials or dump contents into CI artifacts, issue comments or diagnostics.

1. Stop new API mutations. Drain hosts and settle or explicitly stop admitted work. Confirm physical VM release through the existing receipts; preserve every unknown outcome and outstanding reservation. Stop API/controller/archive/source/output/retirement cleanup workers so no service can mutate database or object storage during the capture. A controller exit alone does not stop its VMs.
2. Preserve the full database, including `_sqlx_migrations`, projects and revoked credential metadata, idempotency keys/digests/outcomes, allocations, permits/serials, claims, history barriers, retirement intents/completions and cleanup inventories. A subset of tables is insufficient. Record database identity and PostgreSQL/tool versions privately.
3. Capture immutable object versions and retained source/output retirement markers using the storage backend's verified backup mechanism. Record bucket/versioning configuration and pinned object versions/digests. A database dump contains references, not object bytes. Keep host/guardian state and their admission/retirement authority with the recovery set; copying a running state directory is not a consistent backup.
4. Hash the completed artifacts, sync them to the chosen durable destination, and keep the trusted manifest separately. A SHA-256 match detects changed bytes; it does not authenticate an untrusted archive or prove that a backup includes the latest issued authority. Preserve operator credentials, role/grant configuration and trust roots through their separate private recovery procedure; `--no-owner --no-privileges` does not back them up.

For a quiescent database, inside a newly created private backup directory:

```sh
umask 077
PGSERVICE=hudson-offline pg_dump --no-password --format=custom \
  --no-owner --no-privileges --file=database.dump
sha256sum database.dump > database.dump.sha256
pg_restore --list database.dump > database.contents
```

Use a new destination for each capture and fail the capture if any tool reports an error or warning requiring investigation. Retention starts only after the complete recovery set is durable and verified. Do not prune current host state or permanent object-store markers merely because a database dump succeeded. PostgreSQL documents [consistent logical dumps](https://www.postgresql.org/docs/16/app-pgdump.html) and their [scope and restore requirements](https://www.postgresql.org/docs/16/backup-dump.html).

## Restore into quarantine

Provision a new empty PostgreSQL 16 database from `template0`, with separately reviewed ownership/grants. Its network must be unreachable from live API, controller, cleanup and supervisor processes. Use a distinct libpq service entry. Restore only a trusted operator recovery set: a dump can execute SQL chosen by its source administrator.

```sh
sha256sum --check database.dump.sha256
pg_restore --no-password --single-transaction --exit-on-error \
  --no-owner --no-privileges --dbname='service=hudson-quarantine' database.dump
```

Do not use `--clean` against a running installation, suppress restore failures, or selectively discard tables to make a restore succeed. Verify table counts and canonical row hashes against the capture inventory, migration checksums, ownership references, unknown/terminal outcomes, outstanding reservations, serials, history floors and every stored completion. Verify separately that pinned object versions and retirement markers still exist with matching digests. Restore permissions deliberately; a successful SQL import does not verify application access controls. See PostgreSQL's [restore options](https://www.postgresql.org/docs/16/app-pgrestore.html).

Keep the restored copy quarantined. A backup is an earlier observation, not a monotonic authority. It may resurrect a credential revoked after capture, forget a higher host epoch or reserved retirement floor, or describe an operation whose external effects already occurred. Before reconnecting any live component, independently fence all old execution and storage writers, reconcile the greatest issued host/permit/barrier authority from retained trusted state, rotate/revoke affected credentials and determine honest outcomes for work after the recovery point. Never rerun an arbitrary command to discover its outcome. If that evidence is unavailable, stop recovery with an explicit unknown/loss report. The current toolset does not automate or certify this live recovery step.

Do not restore old object payloads over current retirement markers or replace a current host journal with an earlier copy. Local timeout, task cancellation and elapsed retention cannot prove that an issued storage write has stopped. Database claim expiry allows reconciliation; it does not prove physical release or invalidate an in-flight object-store request.

## Read-only storage evidence audit

After verifying the restored database inventory, keep services stopped and
object storage quiescent. Use the finite audit command against the quarantined
database and the corresponding retained output/source buckets:

```sh
cargo run -p sandbox-cleanup --bin sandbox-storage-audit -- \
  --database-config /private/quarantine-database.json \
  --output-config /private/output-readonly.json \
  --file-source-config /private/source-readonly.json \
  --timeout-seconds 300
```

The database configuration is an operator-owned `0600` JSON file containing
`version: 1` and `database_url`. Final symlinks, special, oversized, permissive
or foreign-owned files, unknown/duplicate fields and unsupported versions are
rejected. Storage configurations use the existing private
[S3 configuration format](output-storage.md#operator-configuration), with
GET/HEAD credentials for both current and pinned versions. An omitted bucket
configuration is accepted only if no retained evidence needs that bucket.

The command starts a repeatable-read, read-only transaction, requires its exact
complete migration ledger and checksums, scans every output
and source page with bounded queries, validates the descriptors against retained
operation/allocation ownership, and checks full pinned payload hashes or exact
retirement receipts. It rejects unresolved upload authority, corrupt metadata,
missing/changed markers and retained versions that should have been deleted.
Expired references are checked without returning bytes or extending customer
access. Simulated output is refused unless `--allow-simulated` is explicitly
selected for development fixtures. The whole audit has a one-hour maximum
deadline; timeout or any failed page produces a nonzero exit and requires a
fresh audit. No migration, cleanup, metadata repair or dispatch occurs.

The JSON report contains only verified object counts and the scope
`storage_evidence_only`. Database/storage credentials, object keys, file paths
and payloads are omitted from diagnostics. A passing audit confirms the observed
storage evidence against that database snapshot. It does not verify backup
freshness, stop an old writer, restore permissions, reconcile revoked tokens,
release compute, or authorize reopening the installation. Keep quarantine until
the independent recovery fences described above are established.

PostgreSQL tests cover paging beyond one batch, a concurrent ledger change,
compacted output, pending/unknown upload authority, source ownership and exact
completed receipts. Executable tests require no database writes and verify that
an empty/incompatible schema is never migrated. The opt-in local/CI
`output_minio_storage_recovery` test uses real versioned source payloads and
markers: storage progressing beyond the database fails; the acknowledged exact
retirement passes; an altered saved marker fails without changing the ledger.
These are recovery-verification checks, not a full object-store backup mechanism
or supported-host qualification.

## Reproducible database rehearsal

With an explicitly disposable local PostgreSQL 16 instance available through `DATABASE_URL`, install `pg_dump`, `pg_restore` and `psql` from PostgreSQL 16 and run:

```sh
cargo test -p sandbox-store --test backup_restore
```

The test uses a separate SQLx-owned database, verifies that it differs from the configured base database and restricts the endpoint to loopback. It creates synthetic known/unknown outcomes, an allocation permit, a reserved history floor and an unfinished retirement intent. A private `0700` directory holds `0600` dump/SQL files. Bounded child processes receive credentials only in their environment; failure diagnostics omit database content. No service, storage client or VM starts.

The test advances an epoch after capture, resets only its own disposable schema, and restores the earlier dump. All public table row hashes, including the migration ledger, must match the capture; rerunning migrations must preserve them. An expired claim cannot renew, its replacement retains the unknown outcome/receipts without another dispatch attempt, unacknowledged history cannot refund capacity, and the allocation remains charged. The reverted epoch explicitly demonstrates why this check cannot authorize reconnecting a backup to live hosts. CI runs this rehearsal with its private PostgreSQL service. Physical restart, object-store backup recovery, independent operator acceptance and supported-host upgrade qualification remain separate gates.
