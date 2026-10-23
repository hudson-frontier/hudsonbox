# Documentation guide

The repository is in **early implementation**. PostgreSQL-backed API routes, the HTTPS server, and the controller/fake supervisor have executable tests. The real Linux supervisor connects create, command execution, renewal and destroy to Firecracker. [Public execute](controller.md#command-admission-and-dispatch-ownership) has durable admission, retry fencing and result handling with local microVM evidence. Authenticated output reads, SSE streaming and an opt-in cleanup worker are implemented. Command cancellation is implemented. The guest file engine, authenticated transport, durable public uploads and project-authorized public downloads exist, with opt-in live/destruction history reclamation and quota recovery. Whole-allocation/storage-marker lifecycle, the UI and an installer remain unfinished. Documents distinguish implemented slices from proposed contracts. Read this index to find the authoritative home for each topic.

## Reading order and ownership

| Document | Owns | Read it to understand |
| --- | --- | --- |
| [Product goal](goal.md) | Product scope, core capabilities, and engineering priorities | What we are building and how validation supports the product |
| [Architecture](architecture.md) | Components, client interfaces, stack, topology, isolation boundaries | What runs where and how data moves |
| [Data models](data-models.md) | Fields, relationships, IDs, database constraints, storage layout | What we persist and how records connect |
| [Authentication](auth-design.md) | Project/Admin permissions, credentials, sessions, CSRF, revocation, audit semantics | Who can do what and how access is enforced |
| [Lifecycle](lifecycle.md) | State transitions, completion evidence, deadlines, cancellation, recovery | How create/pause/resume/destroy actually work |
| [Real lifecycle supervisor](real-supervisor.md) | Durable host RPCs, admission, epochs and guardian integration | How API-created allocations become real VMs |
| [Local host maintenance](host-maintenance.md) | Offline epoch-fenced drain/status/reopening and remaining reservations | How to block placement without discarding work or claiming cleanup |
| [Local host capability enrollment](host-capabilities.md) | Versioned authenticated capability policy, image/resource checks and upgrade boundaries | How to reject incompatible placement while preserving retained ownership |
| [Allocation guardian](allocation-guardian.md) | Real VM ownership, independent deadlines, staging and cleanup | How the host component fences and reclaims an allocation |
| [Guest bootstrap](guest-bootstrap.md) | Allocation credentials, guest init and durable boot binding | How one reusable image becomes an authenticated allocation |
| [Pinned local guest images](../dev/guest-image/README.md) | Native builds, source pins, manifest integrity and embedded-binary verification | How to build development workload artifacts without embedding credentials |
| [Verified host startup](verified-host-startup.md) | Full bundle verification and immutable configuration handoff | How to reject bad guest images before starting the real supervisor |
| [Guest protocol](guest-protocol.md) | Authenticated vsock framing, host client and guest listener | How commands, receipts and bounded output cross the VM boundary |
| [Guest runner](guest-runner.md) | Guest process execution, bounded output and local recovery receipts | What the command component implements and what host protections remain |
| [Linux development](linux-development.md) | Nested Linux/KVM setup and real boot evidence | How to develop the guest and real supervisor |
| [API server](api-server.md) | HTTPS transport, offline provisioning, and runnable local setup | How to start and call the implemented API |
| [Native source installation](source-installation.md) | Committed source builds, native binary profiles and private installation receipts | How to install tools without replacing an existing instance |
| [API contract](api-contract.md) | Admission, SDK/CLI behavior, request retries, response/errors, files, streaming semantics | How clients interact with the service |
| [Rust client and project CLI](client-cli.md) | Private configuration, stable keys, commands, files, streams and exit codes | How to use the implemented Project API |
| [Python and TypeScript clients](language-clients.md) | Language APIs, integer precision, streams, files and package validation | How to call the Project API from Python or Node |
| [Durable HTTP workflow](durable-workflow.md) | Caller IDs, immutable request/handle recovery, readiness, output/files and explicit cleanup | How a caller reconnects through the existing authenticated SDK |
| [Local development workload](local-workload.md) | Finite authenticated workload, private recovery state and explicit teardown | How to exercise an operator-provisioned instance and preserve qualification limits |
| [Optional local MCP adapter](mcp-adapter.md) | Versioned stdio tools, private credential boundary, bounds and recovery | How clients without a shell/custom integration call the existing SDK |
| [OpenAPI and wire models](openapi.md) | Versioned implemented routes, generated wire types and conformance | How to change and validate the client contract |
| [Output storage](output-storage.md) | Private artifact identity, archival, publication, verified reads and cleanup worker | How output is retrieved, streamed and retired with recoverable completion receipts |
| [Operation retention](operation-retention.md) | Terminal response expiry, payload compaction and stable retries | How expired bodies are removed while preserving recovery evidence |
| [History reclamation](history-reclamation.md) | Guest/host retirement, database coordination and remaining lifecycle limits | How to reclaim receipts without admitting old work again |
| [Allocation authority model](allocation-authority.md) | Bounded serial registration and retirement state model | What has been tested in memory and what runtime integration remains |
| [Command cancellation](command-cancellation.md) | Public interruption requests, ownership, outcomes and recovery | How callers interrupt commands and reconcile their results |
| [File transfer](file-transfer.md) | Guest workspace paths, staged uploads, receipts and captured downloads | How guest transfers and authenticated public file transport work |
| [UI design](ui-design.md) | Screens, navigation, user flows, loading/error states | How Project users and Admins manage the installation |
| [Supported configuration](compatibility.md) | Host and guest envelope, privilege layers, boot inputs | What a supported installation and a sandbox actually are |
| [Networking](networking.md) | Egress policy, name resolution, ingress, bandwidth | What a sandbox can reach and what can reach it |
| [Threat model](threat-model.md) | Adversaries, trust boundaries, explicit promises and non-promises | What we defend against and what we deliberately do not |
| [Performance](performance.md) | Latency/size budgets, format constraints they impose, measurement rules | Whether the design is fast enough to be usable |
| [Alternatives](alternatives.md) | Build-versus-adopt argument and revisit triggers | Why we are building this instead of using something existing |
| [Roadmap](roadmap.md) | Implementation sequence, exit gates, evidence, blocking owner decisions, deferred work | What to build next and when it is ready |
| [Decisions](decisions/README.md) | Records of significant choices and their supersession | Why a hard-to-reverse choice was made |
| [Implementation notes](implementation/README.md) | Temporary working notes, deleted as work lands | What to build next — never what is true |

Start with Product goal for scope, then Architecture for a system overview. Supported configuration and Networking define the envelope a sandbox runs in; Alternatives and Threat model explain why the system exists in this shape and what it must withstand. Backend contributors then read Data models, Lifecycle, API contract, Authentication, and Performance. UI contributors read UI design, Authentication, and API contract. Installation work starts with Roadmap; a working self-hosting guide will follow a validated installer.

For branches, commits, reviews, and local documentation checks, read [Contributing](../CONTRIBUTING.md). Security reports use the private channel in [Security](../SECURITY.md).

## What persists and what is disposable

Two kinds of document live here, and the distinction is deliberate.

**Permanent.** Everything in the table above except the last row. The contracts, the architecture, and above all the [decision records](decisions/README.md) — what we chose, what we rejected, and why. A record stays even when it is superseded; it is marked, never deleted. Someone reading this repository in three years should be able to reconstruct the reasoning without asking anyone.

**Disposable.** [Implementation notes](implementation/README.md) are checklists for building what the permanent documents already decided. Each file is deleted in the pull request that finishes its work, after anything durable — a measured number, a confirmed mechanism, a changed choice — has moved into the document that owns it. A task list outliving its task is stale by definition.

If a note and a contract disagree, the contract is right.

## Keeping the docs together

- Give each rule one authoritative home from the table. Other documents summarize and link rather than copying exact limits, phases, or permission matrices.
- Distinguish selected design, unresolved proposals, implemented behavior, and verified behavior. Do not mark a contract implemented without the code and applicable evidence.
- Each detailed contract has acceptance checks and open decisions. Add links to real test files/CI evidence as implementation lands; do not link to hypothetical test paths.
- Update the owning document with a change, then update affected links/examples and the delivery gate. Keep API examples and generated wire types consistent with [OpenAPI](../api/openapi.json).
- Record major new tradeoffs in [decisions](decisions/README.md) when they are made. Mark superseded decisions rather than maintaining two contradictory current contracts.
- Keep the root README short. Add runnable development/deployment/operations guides when the underlying commands and procedures work, not as empty placeholders.

## Where the earlier documents went

`artitecture.md`, `identity-and-resources.md`, and `implementation.md` were retired into the documents above rather than maintained in parallel. Their contents live in Architecture, Lifecycle, Data models, API contract, and Roadmap; the originals remain in Git history. The chosen stack, standalone service boundary, mandatory authentication, planned resource/security models, and pause/resume contracts remain; Product goal and Roadmap now prioritize a usable secure runtime before pause/resume and the management UI. Remove this section once the first runtime code lands and the old filenames stop appearing in open branches.

The implemented internal transport and development fake are described in [supervisor protocol](supervisor-protocol.md), with links to their state-machine and mTLS tests.

The [create controller](controller.md) documents the integrated dispatch/reconciliation path, its public evidence markers, and remaining runtime gaps.
