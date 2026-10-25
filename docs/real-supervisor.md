# Real Linux lifecycle supervisor

Status: the root-operated `sandbox-host` service connects authenticated lifecycle RPCs to the real [allocation guardian](allocation-guardian.md). It supports create, inspection, stop, renewal, lease inspection and public command execution/status. Retained output, live streaming and [command cancellation](command-cancellation.md) have authenticated API routes. Controller-only file upload RPCs now retain dispatch intent and coordinate with stop; [public file routes](api-contract.md#implemented-file-uploads) use these services, with opt-in source cleanup and remaining history reclamation. This is a development component with controlled nested aarch64 evidence, not a production or x86_64 release.

[The host adapter](../crates/sandbox-supervisor/src/host/mod.rs), [durable journal](../crates/sandbox-supervisor/src/host/journal.rs), and [server entry point](../crates/sandbox-supervisor/src/bin/sandbox-host.rs) use the existing [supervisor protocol](supervisor-protocol.md). The controller needs no simulation opt-in for these observations: they carry `simulated=false`.

## Request to VM

1. The controller connects using mutual TLS. The host also checks the exact configured controller leaf fingerprint. Requests must identify this host and current epoch with canonical project, sandbox, allocation and operation IDs, positive revisions/generations, and a claim deadline at most 300 seconds away.
2. Before effects, the host persists the allocation identity, operation revision fence, immutable create request, artifact manifest and capacity reservation. Image digests select an operator-owned mapping to kernel/rootfs artifacts; startup rejects a mapping whose public image identifier differs from the configured rootfs SHA-256. RPC fields never supply host paths or executable names. The guardian independently hashes staged artifact bytes against those configured digests.
3. The guardian stages the allocation. The host then persists dispatch intent and spawns the separately installed `sandbox-supervisor` executable. Namespace setup occurs in that single-threaded executable, outside the async server. A lost RPC caller does not cancel admitted work.
4. Create or subsequent inspection authenticates the guest and durably binds its boot ID. Only a live guardian, a live allocation lease and a matching guest handshake produce `ready`. A VMM process without the guest agent cannot become ready.
5. Stop is persisted before contacting the guardian. A stopped record cannot admit a delayed create or renewal. `released` requires the guardian's completed cgroup and runtime-file cleanup; elapsed time, an absent socket or a stop acknowledgement is insufficient.

Create retains its original operation, image, resources and deadline. An exact retry observes the same allocation; changed input conflicts. An unconfirmed dispatched create is inspected or fenced, never launched again by recovery. A stop before create retains a durable `fenced_absent` tombstone only when no unowned allocation directory or cgroup contradicts absence. Inspection also retains revision fences when it finds no allocation.

Allocation locks serialize requests for that allocation. Slow staging or a guest handshake does not hold the host journal mutex throughout the operation. The server permits eight blocking workers, retaining a worker slot after client cancellation until its work ends. Maintenance rotates through allocations and never starts a VM.

## Durable state and capacity

The private root-owned state directory contains `host.lock`, an atomically replaced/fsynced `host.json`, and an `a` directory containing guardian allocations. An exclusive process lock prevents two servers owning the same journal. Any uncertain journal write disables further service operations until restart/recovery; old VM watchdogs remain independent.

The current journal retains at most 1,024 allocation/fence records and 64 operation revisions per allocation, with a 16 MiB serialized bound. It refuses excess work and does not evict deduplication evidence. Receipt archival and long-running fleet operation need further work.

Host capacity includes each unreleased allocation's requested vCPU, guest RAM plus 128 MiB of VMM allowance, and writable disk plus 401 MiB of staged-artifact allowance. This conservative artifact allowance covers the guardian's bounded Firecracker, jailer, kernel and bootstrap copies. Reservations persist during preparation, uncertain dispatch, stop and restart recovery. Only verified cleanup makes them available again. A sandbox's replacement generation must be greater than the retained generation and follow confirmed prior release or a durable absence fence.

These configured budgets do not measure all host overhead, reserve storage at the filesystem/quota level, account for the operator's source image cache, or protect unrelated processes on a shared host. Provision a dedicated development host with additional system, journal and image-cache headroom. Database scheduling capacity must be chosen consistently with these host budgets; host admission can still reject a request that the database reserved.

## Restart and epoch ownership

Every server start requires an externally issued epoch strictly greater than the retained journal epoch. Reusing the previous epoch, supplying another host ID, or starting a second server against the same state directory is rejected. A missing journal with retained state is not accepted as an empty host.

Startup durably stop-fences old records. Maintenance stops/reconciles their original guardians using the retained manifests and ownership locks, keeping their capacity reserved until cleanup. Old-epoch lifecycle and command RPCs are rejected. The output archival exception below only reconciles retained objects; the separate previous-allocation recovery method verifies original cleanup. Changing only an old request's epoch cannot reuse its allocation identity. A delayed guardian wrapper sees a stopped guardian receipt instead of launching another incarnation.

Authenticated registration and epoch issuance are still operator responsibilities. The [controller now reconciles previous-epoch reservations](controller.md#previous-epoch-allocation-recovery) using the separate `ReconcilePreviousAllocation` RPC. It validates the configured current reporting epoch and original retained allocation identity, acquires the existing allocation gate without admitting a new record, and re-verifies the original guardian cleanup or durable absence fence. Missing journal entries and inconsistent ownership fail closed. Normal stale mutations remain rejected; an epoch change alone never frees capacity. Supported-host recovery and isolation gates remain open.

## Configuration and operation

Build both binaries on Linux. Install the reviewed guardian executable as root-owned and not group/world writable; its absolute path belongs in the host configuration. `sandbox-host --help` lists the required config, CA certificate, server certificate/key and one or two controller fingerprints. The server uses the shared mTLS identity/message limits and listens on `127.0.0.1:7443` by default. No plaintext or anonymous mode exists.

The JSON configuration has these fields:

| Field | Purpose |
| --- | --- |
| `host`, `epoch` | Operator-provisioned typed host ID and fresh positive supervisor epoch |
| `state_root` | Short canonical private root-owned directory, under trusted ancestors; guardian socket paths must fit Unix limits |
| `cgroup_parent` | Dedicated empty delegated cgroup-v2 parent under `/sys/fs/cgroup` |
| `guardian_binary` | Canonical absolute path to the reviewed root-owned `sandbox-supervisor` binary, under protected root-owned ancestors |
| `firecracker`, `jailer` | Each has an absolute `path` and lowercase SHA-256 hex `sha256` |
| `images` | Map from allowlisted `sha256:` image identifiers to `kernel` and `rootfs` artifacts, each with `path`/`sha256` |
| `capabilities` | Optional [version 1 operator contract](host-capabilities.md); startup/health probe native architecture, kernel, KVM and cgroups, and bind its digests to configured artifacts |
| `jail_uid`, `jail_gid` | Unprivileged jailer IDs, each at least 65534 |
| `capacity` | `vcpu`, `memory_mib`, `disk_mib`, including the allowances above |

Guardian executable ancestors cannot be symbolic links or group/world writable.
Root-owned sticky directories are permitted, matching verified operator-path checks.

The real host accepts 1–4 vCPU, 128–8192 MiB of guest memory and 64–65536 MiB of writable disk. Admission, placement, the fake and real hosts, and guardian validation now use the same [resource envelope](../crates/sandbox-protocol/src/resources.rs). New undersized requests receive `400` before admission; existing retry handles and uncertain dispatch evidence remain available. The image must boot the [guest init and agent](guest-bootstrap.md); an arbitrary disk image without that agent cannot pass readiness. Guest userspace stays writable and workloads retain the [guest-root contract](decisions/0003-guest-root-with-our-kernel.md). The current guardian has no NIC, so this integration does not implement the network policy or claim internet access.

Start the API/controller as documented in [API server](api-server.md) and [controller](controller.md), using this host's mTLS endpoint, ID, epoch and image allowlist. Keep simulation disabled. Host registration, certificate issuance, source image preparation and fleet rollout are not automated by this command.

## Verification and limits

[Controlled host tests](../crates/sandbox-supervisor/tests/host.rs) run the actual server binary over loopback mTLS, real guardians and bootstrapped Firecracker VMs. They cover duplicate/conflicting creates, revision fences, durable stop-before-create, lease retries, a lost caller, unauthorized controller certificates, capacity reuse after cleanup, epoch advancement and old-owner cleanup, and VMM presence without guest readiness.

The API integration case uses PostgreSQL and the actual authenticated HTTP router, then the controller's real gRPC client. It checks identical admission handles, non-simulated running state, authenticated guest boot, scheduled lease renewal, destroy completion, database release evidence and removal of cgroup/runtime files. HTTP TCP/TLS itself is covered separately by the API server tests. The host tests are root-only opt-in tests in the isolated [Linux development VM](linux-development.md); ordinary CI does not run them.

The [recorded evidence](evidence/2026-09-21-aarch64-host-rpc.json) identifies source/artifact hashes and measured outcomes. It does not prove public command/file/output delivery, production Debian/kernel builds, host registration or old-epoch database recovery, adversarial isolation, network policy, snapshots, sustained fleet load, or supported x86_64 release gates.

## Command dispatch and reconciliation

The [command adapter](../crates/sandbox-supervisor/src/host/commands.rs) connects the authenticated [execute API](api-contract.md#implemented-execute-admission-and-results) to the existing bound guest client. It uses the allocation gate and controller revision fences, then persists the operation digest, allocation/boot context, deadline and output cap before sending a guest Execute. No argv or environment is retained in the host journal. Guest calls have a three-second bound within the host worker; the guest process is independently owned and can outlive both the RPC and caller.

Duplicate Execute calls inspect the same record. Changed payloads conflict. Same-epoch inspection without a dispatch record persists a `not_started` tombstone before responding, fencing delayed senders. After dispatch, missing or invalid guest evidence stays unknown. Completed receipts are retained and can be read after VM cleanup; a previously active receipt cannot be presented as a fresh running observation once the guest is unavailable. Stop still fences new work and reclaims the VM independently of command outcome.

The journal holds at most 32 command records per allocation and reserves the sum of dispatched output limits, at most 64 MiB. Finished commands do not free that retained output reservation. Capacity is checked before adding command revision fences so lifecycle operations keep room. Journal exhaustion is fail-closed. The internal [acknowledged history RPC](history-reclamation.md) can reclaim a terminal prefix after consumer retirement, retaining a durable admission fence. The opt-in controller history worker drives this RPC; whole-allocation metadata and host receipt reclamation follow the [allocation retirement worker](allocation-retirement.md#automatic-controller-worker). Restart validates retained command metadata, advances the host epoch, and stops old allocations. Old commands never restart. Unresolved command results remain unknown unless appropriate execution evidence becomes available; previous-epoch allocation release does not supply an exit code. `CancelCommand` uses the original execution identity and revision fence; it returns final receipts unchanged, forwards interruption to the bound guest, or durably fences an undispatched command against late execution. Lost cancellation replies never authorize another Execute.

[Command integration evidence](evidence/2026-09-21-aarch64-public-execute.json) covers authenticated API admission through PostgreSQL/controller/mTLS into real microVM commands: zero and nonzero exit, deadline termination, retries, and lifecycle cleanup. Direct host tests additionally check one recorded side effect across retries, durable no-start fencing, retained terminal receipts after destroy, and restart during an active command. Output bytes used to verify a test marker are read through the private guest client; this is not a public output endpoint or an isolation claim.


## Output archival

With operator `--output-config`, the [archive adapter](../crates/sandbox-supervisor/src/host/archive.rs) collects final guest output, verifies it against retained receipts and plans, and conditionally stores both streams. Its two async transfer slots are separate from lifecycle workers; allocation gates and journal locks are released during guest and object network I/O. Journals retain metadata only and enforce immutable tickets/plans with monotonic publication revisions.

A restarted host can reconcile complete objects for an older producing epoch using its retained journal, without reviving or contacting that guest. Missing objects plus missing guest history remain unavailable, including for an expected empty stream. This supervisor path adds no host spool or garbage collector; the API now exposes a separate retained-output route. See [output storage](output-storage.md) for configuration, bounds, evidence and remaining work.

[Controlled archival evidence](evidence/2026-09-21-aarch64-output-archive.json) records eight passing real-host tests, source/artifact hashes, binary stdout/stderr and empty-stream verification, one execution marker, and object reconciliation after VM destruction and host epoch advancement. This is nested aarch64 development evidence; that recording predates the public read endpoint and does not satisfy supported-release isolation gates.

[Public retrieval evidence](evidence/2026-09-21-aarch64-output-read.json) verifies authenticated final stdout/stderr reads from real microVM output, including binary and empty streams and reads after destruction/host epoch advancement. Separate HTTPS/MinIO tests cover wire transport and missing/corrupt objects. Controlled delayed-reader and database-lock tests verify post-read authorization, including revocation during final metadata lookup. [Public SSE](api-contract.md#implemented-output-streams) now adds live output and reconnects; broader release gates remain unfinished.

## Live output reads

The host now exposes the optional [read-only live-output service](supervisor-protocol.md#read-only-live-output) with separate reader certificate pins. It reads existing command metadata, contacts the bound guest and rechecks the allocation record before responding. It does not change command/lifecycle journals or acquire a mutation claim. No reader service is registered unless `--output-reader-cert-sha256` is configured; repeat that flag once for certificate rotation. Reader and controller certificates must be distinct.

The controlled test reads binary stdout and stderr while a real command runs, observes temporary EOF, disconnects and reconnects at the captured offset, then verifies final bytes and exit metadata. It compares host journal bytes before/after reads and checks one execution marker. Reads after confirmed destruction or host epoch advancement fail explicitly. [Recorded live-output evidence](evidence/2026-09-21-aarch64-live-output.json) includes the source and binary hashes and related guest/lifecycle regression results. This is nested aarch64 development evidence. That recording covers the private read RPC. [Public SSE evidence](evidence/2026-09-21-aarch64-sse.json) adds tenant authorization, real live binary delivery and reconnects through the API. [Retention cleanup](output-storage.md#cleanup-worker) is implemented as an opt-in worker; supported-release isolation gates remain unfinished.

## File upload dispatch

The [file-transfer contract](file-transfer.md#supervisor-upload-ownership-and-recovery) defines the controller-only upload RPCs, original descriptor/boot binding, single-dispatch control intents, bounded records, absence fences and stop/restart behavior. They use the same allocation gate and eight blocking workers as lifecycle requests. Host file admission serializes the global count and journal byte checks with the durable insertion, including when requests target different allocations. [Public file admission and controller orchestration](file-transfer.md#public-upload-orchestration) are implemented, with opt-in source and history retirement. The optional [supervisor download service](file-transfer.md#supervisor-download-service) now uses independently configured file-reader pins and bounded ephemeral tickets; its read operations preserve the durable journal.
