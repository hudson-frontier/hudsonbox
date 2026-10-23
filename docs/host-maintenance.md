# Local host maintenance

The offline `sandbox-host-manager` command provides initial host provisioning and draining for [#116](https://github.com/hudson-frontier/hudsonbox/issues/116). It uses trusted operator database access; no new public management endpoint or guest authority is exposed. [Capability enrollment](host-capabilities.md) adds a separate operator-approved compatibility contract. Backup/restore rehearsals and supported-host upgrade qualification remain work in that issue.

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

The report contains the status/epoch, outstanding allocations, reserved CPU/RAM/disk, unfinished operations and unknown operations associated with unreleased allocations. Counts describe database reservations; they are not independent physical cleanup receipts or proof that external effects are known. Drain preserves allocations, leases, permits, idempotency keys, histories and retirement scope. It never refunds capacity, cancels, destroys, deletes storage or promises resumable memory.

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

## Validation and limits

[Store tests](../crates/sandbox-store/tests/host_maintenance.rs) exercise epoch fencing, idempotence, unhealthy refusal, stale health and explicit reopening. [Controller tests](../crates/sandbox-controller/tests/host_maintenance.rs) prove placement exclusion while an admitted command reports a nonzero exit, followed by explicit destruction and reopening. The command's unit test rejects unsafe/private configuration boundaries.

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
