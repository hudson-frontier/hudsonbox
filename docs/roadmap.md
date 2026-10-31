# Roadmap and delivery gates

Status: partially implemented, with local control-plane checks and controlled nested-aarch64 runtime evidence. The authenticated API, clients, SQLx migrations, controller, real supervisor and guest execution/file paths exist. No complete supported-host release gate or reproducible production installation is established.

Implemented components and evidence are listed below and in the [documentation guide](README.md). Development evidence does not establish the selected x86_64 compatibility envelope, the complete adversarial test set, production performance, or operational readiness. Phase status describes the whole exit gate, not merely whether its code exists.

## First usable milestone

**Create → execute and transfer files → enforce isolation and limits → destroy**, on one compatible Linux/KVM host with PostgreSQL and object storage. Include failure recovery, confirmed cleanup, and a reproducible CLI installation. Run it without Hudson, Temporal, or Kubernetes. Authentication stays enabled. [Product goal](goal.md) owns the general-purpose scope and the two-sandbox acceptance scenario.

The first usable milestone requires phases 1 and 2 below, consistent with the [product scope](goal.md#scope-after-the-foundation). Pause/resume follows as a separate capability: create → run a script → pause → release resources → resume the same script → destroy. Its existing memory/disk, controlled-resume, and recovery guarantees remain required before it ships.

## Implementation phases

| Phase | Deliverable and exit gate | Status |
| --- | --- | --- |
| 0. Feasibility spikes | Throwaway code on a real Linux/KVM host answering the questions in [feasibility spikes](#feasibility-spikes-phase-0); written findings, kept or discarded design assumptions, and first [performance](performance.md) measurements | Development feasibility evidence; supported-host findings incomplete |
| 1. Foundation, execution, recovery and isolation | Two gates, built together and passed in order. **Gate 1a — it works:** the [threat model](threat-model.md) turned into enforced behavior, plus the compatibility and privilege contract; Rust protocol/API/controller/supervisor/guest, SQLx storage with versioned migrations for the records shipped operations touch, authenticated setup and project requests; one jailed VM runs commands and long-running processes, transfers files, streams output, enforces resource and network limits, and tears down. **Gate 1b — it does not lie:** failure-injection at create/execute/cancel/destroy boundaries; fenced ownership, honest unknown outcomes, retry deduplication, egress and tenant isolation, cleanup convergence, and the applicable [threat model](threat-model.md#required-validation) test set | Substantial implementation and development evidence; gates 1a/1b incomplete |
| 2. Usable distribution | Supported single-host installation, CLI, working workload example, diagnostics, monitoring, client conformance checks, and verified teardown exercised by another developer; basic backup/restore and upgrade procedures for the shipped components | CLI/SDKs and conformance implemented; installation and operator acceptance incomplete |
| 3. Pause/resume | Snapshot persistence and verified complete memory/disk publication, compute release, compatible restore, guest handshake, original deadlines, and failure-injection at every snapshot/restore boundary; the published format satisfies the [page-addressable constraints](performance.md#constraints-on-designs-we-are-choosing-now) | Planned |
| 4. Management UI and platform packaging | Session migrations, shared Project/Admin policy, UI flows and acceptance checks, and Hudson using ordinary APIs; Kubernetes packaging follows the standalone proof | Planned |
| 5. Multiple hosts and optimization | Compatible cross-host restore, placement, draining, provider autoscaling, and measured cache/snapshot optimizations including differential and lazily loaded snapshots | Planned |

The [OpenAPI contract](openapi.md), [Rust client/CLI](client-cli.md), and [Python/TypeScript clients](language-clients.md) now cover implemented Project endpoints with conformance and package smoke checks. Package publication, an installer and independent operator acceptance remain distribution work. Existing UI designs remain available for later implementation, which shares the same admission and lifecycle services instead of creating a second control path. Define generic authenticated service connectivity before exposing guest services. Exact work breakdown can be split into issues once each phase has concrete interfaces.

## Productization direction

Keep the runtime generic: callers bring arbitrary workloads within the published compatibility envelope. Product-specific templates and integrations can follow later without making them part of the isolation boundary. Borrow useful patterns from other sandbox systems incrementally; the [alternatives](alternatives.md) and [architecture](architecture.md) remain authoritative for Hudson's deployment and component choices.

| Capability | Direction and gate |
| --- | --- |
| SDK, CLI and readiness | Continue the shared API/client contract. Report VM/runtime readiness separately from any future workload or service readiness; readiness means an observed, authenticated condition, never merely a successful start request. Package publication and another-operator installation remain Phase 2 work. |
| Generic networked workloads | Define named, authenticated service endpoints only after a concrete use case. Keep ingress absent until then; any future route must be authorized per sandbox and tied to observed service health. Do not make an agent-specific guest protocol mandatory. |
| Host capacity and cleanup | Finish and measure single-host reuse and orphan reclamation before adding warm VM pools. Host capability tracking and registry-based multi-host placement exist with [portable fake-supervisor qualification](evidence/2026-10-30-fleet-qualification.md); supported two-host Linux/KVM qualification and snapshot compatibility remain open. Optimize from recorded latency, density, and cleanup data. |
| Control/data path | Preserve PostgreSQL operations, idempotency, generations, leases, and receipts as the lifecycle source of truth. Keep command output, files, and future application traffic separable from lifecycle control. Defer a general plug-in/action framework until a proven integration requires it. |

These directions let the same core serve a small team or a larger self-hosted deployment. Enterprise identity, tenant administration, policy controls, and hosted operations are separate product and operational gates; they must not be implied by the current project-token API or by passing sandbox isolation tests.

## Feasibility spikes (Phase 0)

Two assumptions still require supported-host findings. The [Linux development experiment](linux-development.md#verified-boot-and-its-limits) and component tests provide partial evidence, not completion of these questions. The first is that a jailed Firecracker VM on our supported host enforces the resource and connectivity limits the contracts assume. The second is that a guest agent can survive a snapshot outside the frozen customer process groups, reconnect afterwards, and gate the release of those processes; everything in [lifecycle](lifecycle.md#resume) depends on it.

The second group gates Phase 3, not Phase 1, but it belongs here anyway: a negative answer changes what pause/resume can promise, and it is cheaper to learn that before two phases of contracts are built on top of it.

Complete the remaining experiments on the selected supported Linux/KVM configuration and record commands, artifacts, host configuration and findings. Retain useful regression tests; do not replace missing hostile-workload or performance measurements with a successful boot.

| Question | Gates | Why it decides the design |
| --- | --- | --- |
| Do jailer, seccomp, cgroups, and host networking actually enforce the CPU, memory, disk, and egress limits we specify? | Phase 1 | The isolation promise is the product; a limit that is requested but not enforced is not a limit |
| What does create-to-readiness cost on a supported host with a warm image cache? | Phase 1 | Sets whether the [performance](performance.md#proposed-budgets) create budget is reachable |
| Does a frozen customer cgroup stay frozen across a Firecracker snapshot and restore? | Phase 3 | The entire controlled-resume contract assumes it does |
| Can the guest agent reconnect over vsock after restore, given Firecracker's vsock reset? | Phase 3 | Without a reconnect there is no handshake, and without a handshake processes cannot be gated |
| What happens to guest time, timers, and TCP connections across a long pause? | Phase 3 | Deadline enforcement and "credentials are not valid after restore" depend on the answer |
| Can expired or cancelled process groups be terminated before any thaw? | Phase 3 | [Lifecycle](lifecycle.md#deadlines-and-cancellation) requires it |
| What do a pause and a cold cross-host restore actually cost in seconds and bytes? | Phase 3 | Sets whether the [performance](performance.md) resume budgets are reachable |
| How well can the guest agent be shielded from a root customer in the same VM? | Phases 1 and 3 | [Decision 0003](decisions/0003-guest-root-with-our-kernel.md) grants root deliberately. Measure what a hostile root can actually do to the agent: kill sweeps, reaching its control socket, forging a handshake |

For each experiment record CPU, RAM, disk, kernel, Firecracker/jailer versions and exact commands. Test resource exhaustion and denied metadata/host/unapproved egress. Measure warm and cold create-to-ready over at least twenty runs (p50/p95). For snapshot qualification, use a timestamp-writing process to detect execution before explicit thaw on same-host and cross-host restore; test vsock reconnection, clocks/timers/TCP after an hour paused, and terminating expired process groups before thaw. Measure capture, upload, confirmed release, fetch, memory/disk restore and handshake separately at 1 GiB and 8 GiB. Probe hostile-root access to agent processes, sockets, cgroups, kernel modules and resume handshakes; record successful attacks as limitations or mitigations. Exit gate: a written findings document per question, with the commands run and the host configuration recorded. A negative answer is a successful spike; it redirects the design before the dependent phase rather than during it. If process-continuous resume proves unreachable on this stack, reopen [alternatives](alternatives.md#revisit-triggers).

## Scope discipline for Phase 1

Execution and recovery were previously separate phases. They are merged because the mechanics that make recovery work — allocation generations, supervisor epochs, claim revisions, receipts before and after every external action — are cheap to build into the first implementation and expensive to retrofit into a working one. Writing failure-injection tests alongside the code that must survive them is also better than bolting them on to code that was never shaped for it.

The merge does not mean one undifferentiated push. The phase has **two gates, passed in order**, so there is still a moment where the system is demonstrably working before it is asked to prove it does not lie.

**Gate 1a — it works.** One authenticated path end to end: an authorized caller creates a sandbox, runs a command and a long-running process in a real VM, transfers a file, reads output, destroys the sandbox, and the allocation's release is confirmed in the database. Resource and connectivity limits are enforced by the host, not requested politely. Record evidence for maintainer review; tagging and release require the contribution-process approval and remaining release gates.

**Gate 1b — it does not lie.** Every row of the [lifecycle recovery table](lifecycle.md#destroy-and-recovery) that applies to create, execute, cancel and destroy reconciles correctly under injected failure. Stale controller claims, stale supervisor epochs and stale allocation generations are all rejected. Uncertain outcomes are recorded as `unknown` and reconciled rather than guessed. The [threat model's adversarial set](threat-model.md#required-validation) passes for the surfaces that exist. No leaked allocations, disk, or VMs after any of it.

Scope discipline still applies to both. Migrations cover the records those operations touch and nothing further. Snapshot persistence belongs to Phase 3, sessions and browser audit surfaces to Phase 4. What 1a may not do is skip the ownership mechanics on the grounds that 1b will add them — they are in scope from the first migration, and 1b proves they work rather than introducing them.

## Required evidence by delivery gate

| Area | Required evidence | Current evidence |
| --- | --- | --- |
| Feasibility | Written [Phase 0 spike findings](#feasibility-spikes-phase-0) on a real KVM host, before the phase each question gates | [Nested aarch64 boot](linux-development.md#verified-boot-and-its-limits) and [guardian](allocation-guardian.md) component evidence; supported-host spike findings incomplete |
| Recovery under failure | Every applicable row of the [lifecycle recovery table](lifecycle.md#destroy-and-recovery) reconciled under injected failure, at gate 1b | [Controller recovery](controller.md), [host retirement tests](evidence/2026-09-22-released-host-history.json) and [destruction accounting](evidence/2026-09-22-released-history-accounting.json); complete supported-host failure matrix remains open |
| Lifecycle correctness | [Lifecycle acceptance cases](lifecycle.md#acceptance-checks) applicable to shipped operations, including controller/host failure and cleanup; snapshot/restore cases are mandatory at the pause/resume gate | [Real lifecycle adapter](real-supervisor.md), [guest execution](guest-runner.md), [files](file-transfer.md) and [cancellation](command-cancellation.md) have development evidence; full release gate and snapshot/restore remain open |
| API behavior | [Admission, retries, errors, and streaming checks](api-contract.md#acceptance-checks-and-open-decisions) for each shipped endpoint | [Implemented OpenAPI routes](openapi.md) and [API tests](../crates/sandbox-api/tests), including streams and file routes; planned Admin/snapshot surfaces remain unimplemented |
| Ownership/storage | SQLx query/schema checks, fresh and supported-upgrade migration tests, and [model constraints and ID/storage checks](data-models.md#acceptance-checks-and-open-decisions) for shipped resources; snapshot checks before pause/resume | [Versioned migrations](../migrations), [store tests](../crates/sandbox-store/tests) and [history reclamation](history-reclamation.md); operational backup/upgrade and snapshot gates remain open |
| Authentication | [Auth acceptance](auth-design.md#acceptance-checks) for shipped surfaces, mandatory locally and in deployment; browser/session checks before shipping the UI | [HTTPS and offline provisioning](api-server.md), bearer authorization and pinned internal mTLS have tests; Admin/session/UI and production credential operations remain open |
| Host isolation | The adversarial test set in [threat model](threat-model.md#required-validation): guest privilege, filesystem traversal, metadata/control-plane egress, cross-tenant access, and credential/session attacks | [Guardian controls](allocation-guardian.md) and guest/host transport tests exist; the full adversarial set is not validated. Development VMs have no NIC, which does not implement the planned network-policy engine |
| Distribution and usability | Fresh-host installation, workload/file example, diagnostics, failure recovery, and verified resource reclamation by another developer | [CLI](client-cli.md), [SDKs](language-clients.md) and runnable development setup exist; fresh supported-host installation and independent operator acceptance are not established |
| Performance | [Budget table](performance.md#proposed-budgets) measured on a supported host, with configuration recorded; resume budgets at the pause/resume gate | Supported-host latency/size budgets have no acceptance measurements; development test durations are not product benchmarks |
| Management UI | [UI acceptance](ui-design.md#acceptance-checks) before shipping the dashboard | Not implemented; remains a later phase |

Add links to actual test files, CI runs, supported-host evidence, and releases as each gate is demonstrated. A document, successful process start, or passing unit test alone does not establish snapshot or isolation correctness.

## Still blocking

Supported-host evidence and delivery infrastructure remain external prerequisites. They do not block native control-plane development or the existing nested-aarch64 tests. Implementation gaps in networking, production images, operational packaging and full lifecycle reclamation also remain; hardware alone will not complete those gates.

| Decision | What it blocks | Owner action |
| --- | --- | --- |
| Supported x86_64 KVM capacity | Supported-envelope isolation/compatibility acceptance, trustworthy performance and cross-host restore measurements | Provide a host matching [compatibility](compatibility.md#host) and record its configuration |
| Trusted privileged CI capacity | Repeatable supported-host VM, isolation and later snapshot gates | Configure a reviewed runner/workflow that never runs untrusted fork code; ordinary PR checks remain on hosted runners |

## Work breakdown

Use [release tracker #177](https://github.com/hudson-frontier/hudsonbox/issues/177)
and its subissues for implementation tasks, including [multi-host #187](https://github.com/hudson-frontier/hudsonbox/issues/187).
The [accepted fleet and warm-pool models](data-models.md#accepted-v1-fleet-and-warm-pool-model)
and [deployment architecture](arch-v1.md) define the agreed direction. Accepted design does not close release gates.

Remaining foundation acceptance covers production guest images, host-enforced networking,
authenticated host registration, certificate rotation, whole-allocation/storage-marker
reclamation, and another operator reproducing the workload on supported hardware.
Existing operator provisioning, epoch issuance, capability checks and native installation
are documented in their owning guides; qualify the complete path rather than rebuilding them.

For gate 1b inject controller loss between dispatch and confirmation, supervisor loss
mid-command, teardown interruption, dropped acknowledgements and host partitions.
Verify stale claims/epochs/generations cannot act, identical retries deduplicate,
changed requests conflict, and missing outcomes stay unknown until original evidence
resolves them. Cover guest-agent loss, combined host/process/storage failures,
concurrent hostile sandboxes, and repeated cleanup/reuse without prematurely releasing
capacity or deleting replay fences. Link evidence from the owning contract.

## Development and operational prerequisites

Begin with one Linux compute host matching [supported configuration](compatibility.md#host): x86_64, Ubuntu 24.04, KVM available. A standalone development setup needs the API/controller, PostgreSQL, object storage, and that host. It must run without the Hudson harness or a Temporal service. Standalone and Kubernetes are alternative deployment paths; Kubernetes is optional.

The implemented [offline provisioning command](api-server.md#offline-project-provisioning) requires operator database access, writes a private credential file and stores only hashed project token metadata in PostgreSQL. Project API authentication remains mandatory. Admin credentials, the management API/UI and their broader setup workflow remain planned; offline provisioning does not claim to implement them. No unprotected public bootstrap endpoint exists.

Use a remote Linux host for real VM tests from macOS. An unrestricted local process is not a substitute for the isolation boundary. Publish reproducible guest image builds with immutable digests and compatibility metadata.

Instrument operations through OpenTelemetry and expose metrics for Prometheus/Grafana. Record queue time, VM readiness, snapshot/upload duration, restore duration broken down by stage, resource usage, lease expiry, uncertain outcomes, and leaked resources. Correlate by project, sandbox, operation, attempt, host, and optional caller correlation ID. Redact secrets and keep terminal output in bounded artifacts.

Drain hosts before maintenance and prevent new placements while draining. In the initial runtime, wait for work to finish or explicitly stop it with accurate outcomes; do not imply saved memory exists. Once pause/resume ships, verify resumable snapshots before a drain that promises state preservation. Preserve explicit failure outcomes for forced termination. Database migrations, API/controller upgrades, host supervisor upgrades, and guest image changes need independent compatibility and rollback plans. Kubernetes restarts do not replace those plans.

Operational documentation should include proven setup commands, secret provisioning, backups/restores, upgrade/rollback procedures, host maintenance, and capacity recovery once those mechanisms exist. Do not publish hypothetical commands as an install guide.

## Deferred scope

Pause/resume, multiple hosts, Kubernetes packaging, enterprise identity, specialized workload experiences, a polished dashboard, and performance optimizations follow the first usable runtime. Live migration, transparent recovery of unsaved memory after host loss, one Kubernetes pod per sandbox, and a custom hypervisor are outside the initial scope. The [optional local MCP adapter](mcp-adapter.md) wraps the authenticated SDK; remote MCP remains deferred pending its own authentication/transport decision. [Decision 0002](decisions/0002-no-mcp-server-initially.md) records that revision. Agent integration also uses the API/SDK or CLI through a harness shell tool. Redis, ClickHouse, and more elaborate scheduling remain deferred. The scoped Project/Admin management UI remains part of later planned delivery; Hudson's agent/task UI remains outside this repository.

Differential snapshots, lazy loading, and warm pools are deferred to Phase 5, but the format constraints that keep them possible apply from Phase 3. [Performance](performance.md#constraints-on-designs-we-are-choosing-now) owns those constraints.

## Documentation to add when supported by implementation

- Extend [local development](local-development.md) as contributor build/run/test instructions change.
- `self-hosting.md`: actual installation, first Admin credential, TLS, storage, upgrades, and rollback.
- `operations.md`: backup recovery, monitoring, host drains, and incident procedures.

These future guides are intentionally not empty placeholders today. [CONTRIBUTING.md](../CONTRIBUTING.md) defines the PR/release process and documentation checks; [SECURITY.md](../SECURITY.md) provides the private reporting channel; [decisions](decisions/README.md) records significant choices as they are made. The repository is released under Apache-2.0. The Rust toolchain and development dependency/artifact versions are pinned or recorded, and executable tests are linked from the implemented contracts. Production guest image/release pins, frontend framework and provider integration remain open.
