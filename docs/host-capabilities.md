# Local host capability enrollment

The version 1 capability contract adds authenticated compatibility checks to the existing single-host controller. It is a slice of [#116](https://github.com/hudson-frontier/hudsonbox/issues/116): backup/restore rehearsal, supported-host qualification and a complete installation remain pending. It adds no provider integration, deployment, automatic epoch issuance or public registration route.

## Contract and evidence

An operator uses [offline provisioning](host-maintenance.md#initial-host-provisioning) for the initial draining host row, positive supervisor epoch, capacity and approved policy, and separately supplies the host server certificate and controller certificate pins. `hosts.compatibility` holds the approved contract. The controller obtains `HostInfo.capabilities_json` through its configured gRPC/mTLS connection, verifies host ID, epoch and simulation policy, then stores matching metadata in `hosts.runtime_capabilities`. Health cannot insert a host, approve its own policy, change capacity/epoch or promote a draining host.

[The shared contract](../crates/sandbox-protocol/src/host_capabilities.rs) rejects unknown/duplicate fields, unsupported versions, duplicate image digests and input above 64 KiB. It records:

- Schema version 1 and explicit `simulated` provenance.
- Native architecture (`x86_64` or `aarch64`), exact host kernel release, KVM API 12 and cgroup version 2.
- Supervisor/guest version `0.0.0`, Firecracker/jailer compatibility `1.17.0`, immutable Firecracker and jailer SHA-256 digests.
- Maximum per-allocation vCPU, memory MiB and writable disk MiB, bounded by the existing public runtime envelope.
- One to 64 image descriptors: rootfs and kernel `sha256:` digests, minimum disk MiB, guest version, bootstrap version 1 and guest protocol version 1.

For a real supervisor, optional `capabilities` in its existing operator JSON configuration contains this same object. Startup and health check the native architecture, `/proc/sys/kernel/osrelease`, read-only KVM API query and enabled `cpu`, `memory`, `pids` controllers at the dedicated cgroup v2 parent. Image/runtime digests must match configured artifacts; every configured image needs a descriptor, and its minimum disk must contain the rootfs bytes. Create also checks the declared resource/image bounds before staging. The guardian retains its independent artifact hashing, private writable copy and launch/cleanup fences.

Firecracker version and guest protocol compatibility are operator declarations bound to immutable artifact digests. Use the [pinned runtime preparation](linux-development.md#prepare-the-pinned-controlled-test-artifacts) and [verified image recipe](../dev/guest-image/README.md) to obtain them; this health message does not execute binaries to identify versions or inspect embedded init. Offline manifest verification remains necessary. Basic KVM/cgroup preflight and matching metadata are not proof of guest execution, filesystem integrity, egress enforcement, isolation, physical cleanup or saved memory. Simulated hosts can advertise only `simulated: true`; production controller policy still refuses simulated health.

## Enrollment and maintenance

Read the [drain procedure](host-maintenance.md). Stop new placement, finish or explicitly destroy admitted work and confirm verified release before replacing a contract. Put the complete approved JSON contract in an operator-owned `0600` regular file outside source control/customer payloads; no credentials or host paths belong in that contract.

```sh
cargo run -p sandbox-controller --bin sandbox-host-manager -- \
  --database-config /private/database.json --host-id HOST_ID --host-epoch CURRENT_EPOCH drain
cargo run -p sandbox-controller --bin sandbox-host-manager -- \
  --database-config /private/database.json --host-id HOST_ID --host-epoch CURRENT_EPOCH \
  set-capabilities --capabilities-config /private/approved-capabilities.json
```

Approval requires the expected positive epoch, draining status and zero outstanding allocations under the placement host-row lock. It preserves capacity, leases, permits, operation outcomes, retention fences and prior observations. It invalidates `last_seen_at`; `finish-drain` must wait for a matching authenticated health response. Install the same contract in the real supervisor configuration and restart only through the existing externally issued epoch/reconciliation procedure. An epoch change does not prove old allocations stopped. Use `finish-drain` explicitly once the current host is fresh and empty. Health never reopens it automatically.

When a configured, correctly authenticated host returns missing or mismatched compatibility metadata, the controller clears admission freshness, exposes `new_work_compatible() == false` and emits a fixed diagnostic. It keeps access to original ownership for inspection, maintenance and explicit destroy. A mismatch cannot strand old reservations behind the empty-host approval rule. It grants no authority to a failed identity, stale epoch or failed health transport; those failures keep the existing fail-closed behavior. No mismatch automatically cancels, destroys or replays customer work.

New placement requires policy and observed capabilities to match, an advertised image, enough disk for that image and resources within both public and host limits. The stored `runtime_capabilities_epoch` must match the current supervisor epoch; a fresh observation from an older epoch cannot admit work after restart. Dispatch rechecks these conditions under the host-row lock before committing the first launch intent. A contract update or drain cannot race that check. Existing allocations and prior dispatch intents resolve through the original ownership/inspection path before current admission rules; incompatible metadata never creates a replacement incarnation or refunds capacity.

## Upgrade matrix

| Database/host mode | Controller behavior |
| --- | --- |
| Migration 0023, empty policy `{}`, no capability observation, old host | Preserve legacy behavior; no compatibility qualification claim |
| Approved version 1 policy, old host omits capabilities | Refuse health freshness and new placement; operator must upgrade host or restore a compatible build |
| Approved version 1 policy, matching version 1 host | Refresh observation; require image/resource checks before new placement and first dispatch |
| Previously enrolled host, missing metadata or policy cleared to `{}` | Refuse downgrade; health cannot clear enrollment |
| New/unknown schema, guest protocol or unsupported artifact version | Refuse; publish a compatible contract implementation before operator approval |
| Different approved contract after a drained upgrade | Prior observation stays retained but cannot admit work; require new matching authenticated health |

Apply additive migration 0023 before starting the new controller. Legacy mode is an explicit migration boundary, not automatic supported-host admission. There is no manager command to downgrade an enrolled host to legacy. Keep the compatible controller/supervisor binaries, approved contracts, private credentials and immutable artifacts available for rollback; do not roll back a live epoch or erase retained ownership. Database/storage backup and restore procedures still need an access-controlled rehearsal before a release claim.

## Validation

[Protocol tests](../crates/sandbox-protocol/src/host_capabilities.rs) cover malformed/bounded metadata and joint image/resource limits. [Store tests](../crates/sandbox-store/tests/host_capabilities.rs) cover enrollment, stale epochs, forbidden downgrade, simulation provenance, no host self-registration, preserved allocations and dispatch rechecks. [Controller tests](../crates/sandbox-controller/tests/host_capabilities.rs) exercise matching and mismatched advertisements through actual gRPC/mTLS with two separately certified identities and PostgreSQL. These hosts are simulated; controlled Linux/KVM acceptance remains pending.
