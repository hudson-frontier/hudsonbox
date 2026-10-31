# Documentation

Hudsonbox has a working single-host development foundation. Multi-host placement,
Kubernetes workers, warm pools and checkpoints are accepted/planned work, not shipped
capabilities. Supported-host isolation and operational qualification remain incomplete.

## Start here

| Document | Purpose |
| --- | --- |
| [Architecture v1](arch-v1.md) | End-to-end standalone and Kubernetes deployment paths |
| [Data models](data-models.md) | Existing records and the accepted fleet/warm-pool model |
| [Warm pools](warm-pools.md) | Warm-pool records, adoption protocol and routing; real-host blocker and unfinished gates |
| [Local development](local-development.md) | Prerequisites, build, dependencies and tests |
| [API and SDKs](api-contract.md) | Customer-facing behavior and retries |
| [Roadmap](roadmap.md) | Delivery gates and current limitations |
| [Product goal](goal.md) | Scope and priorities |

Implementation tasks live in [release #177](https://github.com/hudson-frontier/hudsonbox/issues/177)
and [multi-host #187](https://github.com/hudson-frontier/hudsonbox/issues/187).

## Reference by task

- **Build an integration:** [CLI](client-cli.md), [Python/TypeScript](language-clients.md),
  [durable workflows](durable-workflow.md), [MCP](mcp-adapter.md), [OpenAPI](openapi.md),
  [files](file-transfer.md), [output](output-storage.md), [cancellation](command-cancellation.md).
- **Install and operate:** [source installation](source-installation.md),
  [API setup](api-server.md), [certificates](local-certificates.md),
  [workload example](local-workload.md), [host capabilities](host-capabilities.md),
  [draining](host-maintenance.md), [upgrade and recovery](operator-recovery.md).
- **Understand runtime internals:** [component architecture](architecture.md),
  [lifecycle](lifecycle.md), [controller](controller.md), [supervisor](real-supervisor.md),
  [gRPC protocol](supervisor-protocol.md), [guardian](allocation-guardian.md),
  [guest bootstrap](guest-bootstrap.md), [guest protocol](guest-protocol.md),
  [guest runner](guest-runner.md).
- **Change ownership or cleanup:** [allocation authority](allocation-authority.md),
  [allocation retirement](allocation-retirement.md), [history reclamation](history-reclamation.md),
  [operation retention](operation-retention.md).
- **Validate security and performance:** [supported configuration](compatibility.md),
  [authentication](auth-design.md), [networking](networking.md), [threat model](threat-model.md),
  [performance](performance.md), [Linux/KVM development](linux-development.md),
  [guest images](../dev/guest-image/README.md), [verified host startup](verified-host-startup.md).
- **Review product decisions:** [decision records](decisions/README.md),
  [alternatives](alternatives.md), [future UI](ui-design.md).

## Documentation rules

Keep each contract in one owning document; link to it instead of copying rules.
Distinguish accepted design, implemented behavior and verified behavior. Preserve
versioned test evidence in [evidence](evidence); it describes the recorded revision,
not necessarily today's implementation. Keep task checklists in GitHub issues.
Supersede decision records explicitly and remove obsolete working notes.

See [Contributing](../CONTRIBUTING.md) for checks and PRs, and
[Security](../SECURITY.md) for private reporting.
