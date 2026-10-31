# Supervisor protocol and development fake

Status: the versioned gRPC transport and an in-memory fake are implemented. The [lifecycle controller](controller.md) now uses this transport. The [real Linux supervisor](real-supervisor.md) now implements these lifecycle RPCs through Firecracker guardians. Authenticated host registration and certificate provisioning remain unfinished. A fake observation never establishes that a VM exists, ran code, enforced limits, or released real resources.

## Contract and identity

[The protobuf source](../proto/supervisor.proto) generates Rust messages, a client, and a server at build time through `tonic-prost-build`. The package is `hudson.supervisor.v1`. Keep wire field numbers stable; do not reuse removed numbers. The protocol covers health, create, inspect, stop, allocation lease renewal/inspection, and command execution/inspection. Final-output archival and a separate read-only live-output service are also implemented. Authenticated upload and separate captured-download RPCs are implemented.

Every request uses gRPC over mutual TLS. [Shared transport](../crates/sandbox-supervisor/src/transport.rs) requires a trusted private CA and client certificate. The `Supervisor` server interceptor also compares the connected client's leaf-certificate SHA-256 fingerprint against one or two configured controller certificates. Another CA-valid certificate, even with the same subject name, has no authority. The second pin supports an explicitly configured rotation overlap. No raw Project or Admin token is accepted on this interface.

The client verifies the server's CA chain and the exact DNS name `host-<host UUID>.sandbox.internal`, derived from its expected host ID. Endpoint addresses are operator configuration, never customer request fields. The transport has no plaintext or certificate-verification bypass. TLS handshakes and lifecycle client requests have five-second bounds; the separate archival client uses the output-specific deadlines below; messages are limited to 64 KiB in both directions. The separate reader service carries at most 32 KiB of output per call.

Controller identity authorizes the trusted control-plane service, not arbitrary tenant requests. The controller must still check persisted project authority before dispatch. It must bind the configured endpoint and certificate identity to an authenticated registered host and current epoch; a successful TLS connection alone is not host registration.

The implementation uses [tonic's server TLS configuration](https://docs.rs/tonic/0.14.6/tonic/transport/server/struct.ServerTlsConfig.html) and [protobuf generation](https://docs.rs/tonic-prost-build/0.14.6/tonic_prost_build/). Test certificates are generated ephemerally with rcgen; no test private keys are checked in.

## Ownership and observations

Create, inspect, stop, execute-command and inspect-command carry the host, project, sandbox, allocation, operation, allocation generation, supervisor epoch, controller claim revision, and absolute claim deadline. IDs must have their canonical typed UUIDv7 form. Revisions and generations are positive. A request for the wrong host or epoch is rejected.

Claim deadlines must be in the future and at most 300 seconds away when the supervisor handles the request. Allocation lease deadlines have the same bound. Wall clocks must be synchronized; after validation, the fake converts the allocation deadline to a monotonic local timer. Controller claim expiry never extends an allocation lease.

An observation echoes the requested ownership tuple, names the original create operation when known, includes its observation time, and distinguishes `absent`, `ready`, `released`, and `fenced_absent`. The controller must verify the complete tuple and expected evidence before changing PostgreSQL state. `unspecified` is never a usable result. Every fake response sets `simulated=true`, including health. The controller requires explicit development opt-in before accepting simulated evidence and preserves that distinction in receipts and public state.

`Fenced_absent` is an explicit, epoch-bound proof that no incarnation exists and future starts of that allocation have been fenced. It can complete destroy after a committed create intent was never sent. It must not be inferred from missing records alone. The fake preserves that fence for its process lifetime; production requires durable fencing and a new epoch after restart.

`Absent` means this supervisor has no evidence for that allocation. It is not confirmation that another epoch stopped the VM, permission to free its reservation, or permission to repeat an uncertain command. Database intent, authenticated observations, and the [lifecycle reconciliation contract](lifecycle.md#destroy-and-recovery) govern the next action.

## Lease maintenance messages

`RenewLease` and `InspectLease` use `LeaseOwnership`: host, project, sandbox, allocation, generation, supervisor epoch, maintenance revision, and absolute maintenance claim deadline. This ownership is distinct from operation claims; periodic renewal must not reopen a completed create. `LeaseObservation` echoes that tuple and returns state, simulation source, observation time, and the actual execution deadline known by the supervisor.

The fake retains the greatest maintenance revision for the allocation. A lower revision is rejected; a repeated renewal at the same revision must have the same requested deadline. Renewal can only extend a ready allocation, never shorten a newer deadline, create a missing allocation, or revive release/expiry/stop-fenced state. It preserves the immutable original create request, so a later create retry cannot extend the lease. Inspecting an absent allocation returns absence, not release proof. Lease fields have the same bounded deadline and identity validation as other control requests; the independent monotonic watchdog remains authoritative locally. See [controller maintenance](controller.md#allocation-maintenance) for persistence and recovery.

## Previous-epoch release evidence

`ReconcilePreviousAllocation` is a controller-only exception for cleanup recovery, separate from normal mutation methods. `PreviousAllocationRequest` contains the complete original `Ownership` plus `reporting_epoch`. The reporting epoch must equal the serving supervisor's current epoch and be strictly greater than the original positive epoch. Canonical identities, current host identity and the bounded live claim deadline still apply.

The real supervisor requires an existing, matching, durably stopped journal record. It acquires that allocation's existing gate, rechecks ownership, and verifies the original guardian's stop/cgroup/runtime-file cleanup. A retained stopped record without a manifest can report `fenced_absent` only after verifying that no contradictory allocation directory or cgroup exists. Missing records never become evidence and are not inserted by this RPC. This path cannot create, renew, execute or revive an allocation; the ordinary methods retain their exact-epoch checks.

`PreviousAllocationObservation` echoes the entire request and contains an `Observation` for original ownership, carrying only `released` or `fenced_absent`, provenance and a fresh timestamp. The controller compares both echoed identities; PostgreSQL independently checks the current reporting epoch, original allocation/generation, operation claim and observation freshness before releasing a reservation. A cleanup effect or response with a lost acknowledgement is reconciled by repeating verification under a new claim, never by starting old work. The API-reader certificate has no authority on this method.

The in-memory fake rejects this method because it retains no evidence across restart. This preserves the rule that empty state is not proof of release. Older servers that lack the additive RPC also leave recovery unconfirmed; upgrade the supervisor before relying on [controller recovery](controller.md#previous-epoch-allocation-recovery). No wire fields were renumbered.

## What the fake exercises

[The fake library](../crates/sandbox-fake-host/src/lib.rs) runs no subprocesses. It models the following control-plane behavior:

- A repeated create for the same allocation, operation, image, resources, and original allocation deadline returns the existing result. It neither starts again nor extends the lease. Changed create input conflicts.
- A higher observed claim revision fences older requests for that operation, including when inspection found no allocation. Identity bindings and fences survive simulated release.
- A stop received before create prevents a delayed create from starting that allocation and reports `fenced_absent`; later inspections preserve that proof. Stopping a known allocation is idempotent; replaying its old create returns released state.
- A sandbox can have a new generation only after the previous recorded incarnation is released. Changing project, sandbox, or generation under an allocation ID is rejected.
- An explicit digest allowlist, per-sandbox resource bounds, and aggregate CPU/RAM/disk capacity checks control simulated admission. They do not verify image bytes or enforce hardware resources.
- The binary checks allocation leases independently every 100 ms; expired allocations become simulated released records. Calls also check expiry before serving observations. Renewal can extend a still-live allocation, but cannot restart an expired or stopped incarnation.
- Test hooks can lose create, stop, or renewal acknowledgements after applying the action. Inspection reports the same start, released incarnation, or fenced absence.

The fake bounds retained allocation/fence records to 10,000 and operation revisions to 64 per allocation. It rejects additional records rather than evicting deduplication or fencing evidence. Restart clears memory: supply a new externally assigned epoch. Do not reuse an epoch to make lost evidence appear authoritative. Production epoch issuance and durable supervisor receipts are still required.

## Run the fake

Install the pinned Rust toolchain and `protoc` (`brew install protobuf` on macOS; `sudo apt-get install protobuf-compiler` on Ubuntu). CI installs the same compiler package on its Ubuntu runner. Generated Rust files stay in Cargo's build output; the `.proto` file is their source of truth.

The binary requires operator-supplied CA/server certificate files, a server private-key file, controller certificate fingerprints, a host ID, an explicit epoch, and allowed image digests:

```sh
cargo run -p sandbox-fake-host -- --help
```

The server certificate must contain the host-specific DNS identity above and permit server authentication; controller certificates must permit client authentication. Keep private keys outside the repository. Compute each controller fingerprint from its DER certificate bytes with SHA-256; a PEM file's text hash is not the fingerprint. Certificate issuance, trusted local provisioning, and rotation tooling remain separate work.

The default listen address is `127.0.0.1:7443`; non-loopback addresses are rejected. There is no unauthenticated development mode. On restart, use a new epoch and reconcile existing reservations instead of assuming the empty fake proved release. Use an isolated development database when the controller integration is available.

## Evidence and remaining gates

[State-machine tests](../crates/sandbox-fake-host/tests/state.rs) cover concurrent duplicates, changed retries, lost acknowledgements, stale claims after absence, stop-before-create, cross-project/allocation identity, generation replacement, limits, deadlines, and monotonic lease expiry. [Loopback TLS tests](../crates/sandbox-fake-host/tests/tls.rs) exercise actual gRPC calls, configured certificate rotation, same-CA unauthorized peers, missing/untrusted client certificates, wrong host identities, plaintext rejection, and server message bounds.

These tests establish the modeled behavior and transport checks only. Further lifecycle integration, real certificate lifecycle, host watchdog fencing under partitions, Firecracker/jailer, image-byte verification, filesystem isolation, resource enforcement, and network policy require their own evidence. [Phase 1](roadmap.md#scope-discipline-for-phase-1) cannot pass on fake-host tests.

## Real VM development evidence

The [Linux development guide](linux-development.md#verified-boot-and-its-limits) records a real Firecracker/jailer boot in a nested aarch64 environment. The [real lifecycle adapter](real-supervisor.md) now uses this shared transport and public command dispatch is integrated. The separate [guest protocol](guest-protocol.md) now provides an authenticated host client and guest listener, with real command round-trip evidence; the lifecycle supervisor now requires authenticated boot binding, and the public command path uses that binding. The experiment also demonstrates why [tracking the actual Firecracker child](linux-development.md#track-the-firecracker-child-not-just-the-jailer) is necessary: the jailer parent can exit successfully while the microVM is still running.

The [allocation guardian](allocation-guardian.md) now supplies a separate root-only real VM owner with independent expiry and cleanup tests. The [real RPC adapter](real-supervisor.md) connects this owner to the controller, requiring guest boot binding for readiness and completed cleanup for release; process presence alone supplies neither.

## Command RPCs

`ExecuteCommand` reuses the bounded guest `Execute` message inside the full controller ownership tuple. Its Debug output redacts argv and environment. `InspectCommand` and `CancelCommand` identify the same operation and its SHA-256 command digest. Cancellation uses the existing execution claim and interrupts the bound guest; it grants no new execution authority. None of these RPCs carries output bytes. Responses echo ownership, digest, simulation source, observation time, and either a validated guest receipt, a durable `not_started` fence, or missing evidence (unknown).

The [real command adapter](../crates/sandbox-supervisor/src/host/commands.rs) persists metadata-only dispatch intent with the bound guest boot before sending one Execute. Retries only inspect. Inspection of an undispatched operation first persists a fence against any late Execute; mere absence is not sufficient. Guest errors after intent cannot be interpreted as proof of no execution. Each allocation retains at most 32 commands and reserves at most 64 MiB of output limits; command pressure cannot consume the remaining lifecycle revision slots. A host-to-guest call is limited to three seconds inside a bounded worker; timeout does not cancel admitted guest work. Command completion cannot authorize allocation release.

The [fake command model](../crates/sandbox-fake-host/src/commands.rs) never executes argv. It models held and completed commands, lost replies, no-start fences, expiry, retained capacity and destroy during an unresolved command. Every observation remains simulated. Restart still requires a new epoch and cannot manufacture an old command result.


## Final output archival

`PrepareOutput` and `ArchiveOutput` use an independent publication revision and deadline, plus a typed output ticket containing the original command/allocation/generation/producing epoch/boot. This revision is never an execution claim. The prepare response contains final digest/size/statistics plans; the controller persists them before the archive request. The archive response adds verified private references. Requests and responses carry bounded JSON metadata only, never guest bytes or credentials.

Both RPCs use the same mutual TLS and pinned controller identity. Responses echo the exact request, current serving host/epoch, simulation flag and observation time. The controller rejects changed ownership, stale observations, a simulation mismatch, or references outside its saved plans. The host journals ticket/revision/plan fences separately from lifecycle revisions and checks its retained terminal command receipt.

Prepare/Archive have 30/80-second client bounds on a separate connection; supervisor capture/upload have 20/75-second bounds and two dedicated admission slots. Lifecycle RPCs retain their existing deadlines. A new serving epoch may reconcile already-uploaded objects for its retained old-epoch ticket without accessing the old VM. See [output storage](output-storage.md) for configuration, transfer limits, missing-history semantics and recovery evidence.

## Read-only live output

`hudson.supervisor.v1.LiveOutput/Read` is a separate service on the same TLS listener. Both host binaries enable it only when `--output-reader-cert-sha256` supplies one or two exact reader leaf fingerprints. Reader pins must be disjoint from controller pins, including rotation certificates; overlap prevents startup. Reader identities cannot call any `Supervisor` method, and controller identities cannot call `LiveOutput`. Use the separate `connect_output_reader` client with an operator-selected host endpoint and certificate. The [API's live-output and SSE integration](api-contract.md#implemented-output-streams) uses this separate identity and derives scopes from tenant-authorized execution evidence.

[LiveOutputScope](../crates/sandbox-protocol/src/live_output.rs) binds project, sandbox, execution operation, allocation, generation, producing host/epoch, guest boot, command digest, command deadline and admitted output cap. The trusted caller must derive it from authorized execution evidence; it is not a customer credential. Scope JSON is at most 8 KiB and rejects unknown fields. Requests choose stdout or stderr, byte offset, a length of 1–32768, and a fresh deadline at most 30 seconds away. The original command deadline may have passed; output inspection cannot extend execution.

The [real reader](../crates/sandbox-supervisor/src/host/live_output.rs) requires an existing journaled command and its exact bound guest. It neither installs controller fences nor saves observations, executes commands, renews leases, archives output or performs allocation maintenance. Missing command history returns `NotFound`; a stopped/released guest returns `Unavailable`; another producing epoch or mismatched binding is rejected. These failures must never become successful empty output. Archived output remains a separate path for a destroyed or old-epoch guest.

Four concurrent reads per host are admitted without an unbounded waiting queue; additional reads receive `ResourceExhausted`. Each entire read has a four-second timeout; the client transport has a five-second bound. Guest network I/O holds neither the lifecycle gate nor journal lock. Metadata lookup uses the existing bounded host worker pool, so a blocked journal can still consume metadata workers; the independent guardian remains responsible for expiry. Disconnect/timeout drops the read and never cancels or repeats the command. Metadata workers retain their own permits until they actually finish.

Responses echo the request, serving host/epoch, observation time and simulation flag, and carry a bounded binary chunk plus a validated guest receipt. Chunk identity, offset, size and next offset must match the request and original command, and the bytes must fit the receipt's captured prefix. `at_end=true, complete=false` means only that no further captured bytes were available at that instant. A later terminal receipt is allowed after an incomplete chunk because the command may finish between the two guest calls. Final completion requires terminal cleanup evidence and matching final bounds. Consumers must revalidate correlation, freshness, ownership and public authorization before delivery. Guest data is untrusted and does not prove isolation or external effects; request/response Debug output redacts bytes and guest reasons.

The fake uses the same role separation and validation, returns explicitly simulated empty pending/final chunks, and rejects expired or missing guest state without changing it. [TLS tests](../crates/sandbox-fake-host/tests/tls.rs) verify both reader rotation pins, denial on every controller RPC, rejected ownership changes, and absence reads that do not fence later execution. [Protocol tests](../crates/sandbox-supervisor/tests/live_output.rs) exercise byte bounds, malformed scopes, completion races and redaction. Real guest evidence is recorded with the [real supervisor](real-supervisor.md#live-output-reads).

## File upload RPCs

`BeginFile`, `WriteFile`, `InspectFile`, `CommitFile` and `AbortFile` are controller-only methods on `Supervisor`. Requests bind the full allocation ownership and original guest upload descriptor. Inspection may create a durable absence fence, so it also requires mutation authority. `FileObservation` includes the owner, descriptor digest, optional guest context, guest state, absence fence and optional confirmed chunk cursor. State zero means no current guest observation; it is distinct from `not_started`. See [file upload ownership and recovery](file-transfer.md#supervisor-upload-ownership-and-recovery) for admission, limits and uncertain-outcome semantics. These additive RPCs do not expose a customer file API; older supervisors report unimplemented.

## Read-only file downloads

The optional `FileDownloads` service has `Capture`, `Read` and `Release` RPCs. Configure its independent `--file-reader-cert-sha256` allowlist explicitly; output-reader or controller credentials do not imply permission. Requests bind a versioned allocation read scope and bounded expiry. Host-generated handles bind the original guest boot and complete captured-file descriptor; responses include the exact scope, simulation provenance and observation time. The [download contract](file-transfer.md#supervisor-download-service) owns ticket lifetime, concurrency, chunk validation and post-I/O allocation checks. Handles never authorize customer access by themselves, and expired or pre-restart handles cannot reopen their prior path.

## History retirement RPC

`Supervisor.RetireHistory` is a controller-only mutation. `HistoryRequest` binds exact allocation ownership, an independent per-domain claim revision/deadline, and a guest `HistoryBarrier`. It asserts that original outcomes are durably retained and output/source consumers are retired. `HistoryObservation` echoes the request, acknowledges the completed barrier, and includes simulation provenance and observation time. The opt-in controller history worker invokes it after database preparation; no public API exposes it. The simulator returns unimplemented.

The real host persists an independent admission floor before contacting the original bound guest, retains charged records on uncertainty, and prunes records/revisions only after validating the exact guest acknowledgement. Covered delayed requests fail with `failed_precondition`, including inspection; they never manufacture a new absence fence. A completed lower-prefix retry may acknowledge the larger retained floor but never grants permission to refund unrelated database operations. The [history contract](history-reclamation.md) owns claim fencing, compatibility, resource bounds and database completion and remaining destruction coordination.

`Supervisor.HistoryBinding` is a controller-only read of the original manifest guest context. It echoes the exact `LeaseInspection`, returns the bound allocation/generation/boot, and records provenance/time without reserving a history floor. Missing, stopped, released, stale-epoch or mismatched ownership fails closed. The database uses this only when known not-started outcomes provide no guest receipt; it freezes the returned context before retirement. The simulator returns unimplemented.

`Supervisor.RetireReleasedHistory` carries original `LeaseOwnership`, current reporting epoch, domain and inclusive prefix. It requires an already stopped allocation and re-verifies destruction; its response is a destruction acknowledgement, not `HistoryObservation`. Unknown covered outcomes still block retirement. The host retains its allocation tombstone and a durable per-domain completion after pruning known records. See [destruction retirement](history-reclamation.md#retirement-after-verified-allocation-destruction) for retry, persistence and consumer prerequisites. The opt-in controller worker calls it through independent database claims; exact fresh acknowledgements are persisted before quota refunds.

## Allocation permit registration

Fresh hosts may require [durable launch permits](allocation-authority.md#fresh-host-registration-and-launch). `Health.launch_permits_required` advertises that persisted mode. `AllocationAuthority` uses the same pinned controller identity as other mutation RPCs; output/file-reader identities cannot invoke it. Requests specify the configured host and current reporting epoch. Empty `permits_json` inspects durable progress; a nonempty, bounded JSON array registers contiguous exact-owner permits. Responses contain host, epoch and registered frontier, with no release or absence claim.

The controller persists an independent database checkpoint and supplies the registered original permit in `Create.launch_permit_json`. An enabled host rejects missing, mismatched, stale, unregistered or fenced permits. Legacy mode cannot silently consume a permit-bearing request. A lost registration response is reconciled by inspection; a lost Create response still follows existing allocation inspection and cannot authorize another launch.

## Whole-allocation launch fencing

`FenceAllocation` carries a strict bounded `allocation_retirement::Request` as JSON. The mTLS controller identity must already have frozen that exact intent in the database. The host independently binds it to its registered owner and completed command/file scope, persists it, and installs durable launch denial. Its acknowledgement echoes the exact request and observation time; it has no `Released`, `FencedAbsent`, cleanup or deletion flag. Consult the [retirement contract](allocation-retirement.md#host-fencing-component) for ordering and failure semantics. The fake host returns unimplemented, and no controller worker dispatches this RPC yet.

### Host reader closure during allocation fencing

Live-output and file-download calls pin the allocation while holding the journal lock that validates admission. File worker admission uses that same lock. Whole-allocation fencing persists its intent and installs launch denial before attempting exclusive reader access; it returns unavailable while a reader or pending/unreleased capture ticket remains. An identical retry can succeed after those consumers finish or expire. A failed capture retains its ticket until monotonic expiry, because a failed response cannot establish that capture did not happen. Host reader closure does not prove guest cleanup or authorize metadata deletion.

### Recoverable allocation metadata retirement

`RetireAllocationMetadata` accepts the same retirement JSON envelope as `FenceAllocation` and returns the exact request with an observation time only after verifying physical cleanup and removing owned guardian metadata. It first reconciles launch fencing and reader closure. The host saves original cleanup evidence and a bounded file inventory before unlinking, then saves completion after directory sync. Retrying an identical scope under a renewed valid claim reconciles interrupted removal and lost replies. Legacy and fake hosts reject this RPC.

Original host records and root fencing entries remain retained. The observation cannot be substituted for the existing ordinary release response, and metadata retirement alone does not recover capacity. The [controller worker](allocation-retirement.md#automatic-controller-worker) completes the subsequent forgetting handoff. The [database acknowledgement entry point](allocation-retirement.md#database-metadata-acknowledgement-component) retains exact physical observations before a later forgetting handoff. See [whole-allocation retirement](allocation-retirement.md#host-metadata-deletion-component) for ownership, restart and filesystem requirements.

### ForgetAllocation

The authenticated controller issues a bounded `ForgetRequest` only after retaining database metadata completion. The host validates exact scope and physical metadata absence, durably completes root authority, removes its journal record and then forgets the root entry. `Forgotten` acknowledges this handoff; `Retired` is only closed-serial denial and must never be translated into `Released` or `FencedAbsent`. The database requires its independently retained metadata completion and exact fresh claim before consuming either response. See [claim ownership and crash recovery](allocation-retirement.md#database-and-host-forgetting-handoff). The [controller worker](allocation-retirement.md#automatic-controller-worker) now dispatches and reconciles these requests.

## Warm-pool RPCs

`PrepareWarmMember`, `AdoptWarmMember`, `InspectWarmHandoff` and `RetireWarmMember` carry bounded `sandbox_protocol::warm_pool` JSON. Adoption requires the exact database handoff intent together with its derived allocation permit and matching ownership; inspection never adopts and fences an unadopted member. The fake host simulates members. The real host prepares inert members and adopts them through a fenced guardian re-key and guest late binding; this is not yet validated on Linux/KVM. [Warm pools](warm-pools.md#supervisor-protocol-and-adoption-authority) owns the contract, the [real Firecracker host](warm-pools.md#real-firecracker-host) design and its [remaining gates](warm-pools.md#remaining-gates).

The real warm host accepts the distinct #216 previous-epoch cleanup envelope through `RetireWarmMember`. It requires retained original ownership, a durable monotonic cleanup-operation claim fence, the current reporting epoch and guardian-proven runtime absence. The reply echoes the exact envelope while preserving the original cleanup epoch; unknown old members and adopted members refuse. The [warm-pool contract](warm-pools.md#real-firecracker-host) owns the details and retained identity budget.
