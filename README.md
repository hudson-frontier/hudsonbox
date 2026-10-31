<h1 align="center">Hudsonbox</h1>

<p align="center">
  A Linux workload runtime with an authenticated API and an explicit lifecycle.
</p>

<p align="center">
  <a href="https://github.com/hudson-frontier/hudsonbox/actions/workflows/rust.yml"><img alt="Rust checks" src="https://github.com/hudson-frontier/hudsonbox/actions/workflows/rust.yml/badge.svg?branch=main"></a>
  <a href="https://github.com/hudson-frontier/hudsonbox/actions/workflows/docs.yml"><img alt="Documentation checks" src="https://github.com/hudson-frontier/hudsonbox/actions/workflows/docs.yml/badge.svg?branch=main"></a>
  <a href="LICENSE"><img alt="Apache-2.0 license" src="https://img.shields.io/badge/license-Apache%202.0-blue?style=flat-square"></a>
</p>

<p align="center">
  <a href="#get-started">Get started</a> ·
  <a href="#use-an-existing-installation">Client workflow</a> ·
  <a href="#developing">Development</a> ·
  <a href="#reference">Documentation</a>
</p>

---

Hudsonbox is a Hudson Frontier project building a runtime for untrusted Linux
workloads. Applications use its API to create sandboxes, execute commands,
transfer files, observe output, and destroy environments with tracked cleanup.

**Status: early implementation.** The authenticated API, clients, and control
plane have executable tests. Real VM readiness, execution, and destruction have
controlled aarch64 Linux/KVM evidence. Supported x86_64 isolation/performance
qualification and a complete operator installation remain pending.
Pause/resume and the management UI are planned. The [roadmap](docs/roadmap.md)
records the delivery gates. The current version is in [VERSION](VERSION) and
[CHANGELOG.md](CHANGELOG.md); see [versioning](docs/versioning.md) for SemVer
rules and the release flow.

## Get started

Clone the source and check local build prerequisites without starting services:

```sh
git clone https://github.com/hudson-frontier/hudsonbox.git
cd hudsonbox
python3 scripts/doctor.py --json
```

Control-plane development uses macOS or Linux, Python 3.11+, pinned Rust 1.92.0,
`protoc`, Docker with Compose v2, and Node.js 24.9.0+ with npm. Follow the
[local development guide](docs/local-development.md) for dependencies and diagnosis.
The [native source installer](docs/source-installation.md) builds committed tools
into a new private prefix; service provisioning remains a separate operator step.

Real workloads require a configured Linux/KVM host, Firecracker/jailer, and
verified guest images. Start with [Linux development](docs/linux-development.md)
and [verified host startup](docs/verified-host-startup.md). A fake supervisor
exercises the control plane without running customer code.

## Use an existing installation

The published [Hudson CLI](https://github.com/hudson-frontier/hudson-cli#installation)
provides the `hudson sandbox` commands. Obtain a private client profile and
credentials from your operator, then list the project's sandboxes:

```sh
hudson sandbox --config /private/client.json --json list --limit 20
```

Prepare `create.json` with an operator-approved image and resource limits, using
the [client guide](docs/client-cli.md). Save a mutation key before submitting it:

```sh
hudson sandbox key --to create.key
hudson sandbox --config /private/client.json --json create --request create.json --key-file create.key
hudson sandbox --config /private/client.json --json wait OPERATION_ID --seconds 60
```

Copy the returned operation ID into `wait`. Admission returns before completion.
Keep the exact request and key for retries of the same logical mutation.
Stopping observation leaves the operation running.

Hudson CLI owns the public terminal commands. Sandbox API access requires private
Unix credential files; use Linux, macOS, or WSL. This repository provides the API,
SDKs, service binaries, and a private API test client for backend development.

## Integrate with your application

| Interface | Entry point |
| --- | --- |
| HTTPS/JSON | [API contract](docs/api-contract.md) and [OpenAPI schema](api/openapi.json) |
| Rust | [Client and project CLI](docs/client-cli.md) |
| Python and TypeScript | [Language clients](docs/language-clients.md) |
| Local MCP tools | [Optional stdio adapter](sdk/mcp/README.md) |
| Resumable client workflows | [Durable HTTP workflow](docs/durable-workflow.md) |

SDK packages build from source. The MCP adapter wraps the authenticated client;
the management UI remains planned. Authentication is required even in local
development.

[Hudson](https://github.com/hudson-frontier/hudson) is an agent harness and one
possible caller. Agent workflows and orchestration stay with the application.
Hudsonbox's service uses Rust, PostgreSQL, S3-compatible storage, and
Firecracker/Linux KVM.

## Developing

After resolving the doctor report, set up the native development tools and local
dependencies:

```sh
make api-setup
make sdk-setup
make mcp-setup
make up
make check
make down
```

`make down` stops the dependency stack while keeping its data. See
[control-plane development](docs/local-development.md) for local connection
settings and the [HTTPS API guide](docs/api-server.md) for certificates,
provisioning, and service startup.

Native tests cover API and client behavior; runtime acceptance requires
controlled Linux/KVM checks. The [local workload](docs/local-workload.md)
exercises an operator-provisioned instance with recovery and explicit teardown.

| Path | Purpose |
| --- | --- |
| [crates/sandbox-api](crates/sandbox-api) | HTTPS API, authentication, and admission |
| [crates/sandbox-controller](crates/sandbox-controller) | Placement, dispatch, and reconciliation |
| [crates/sandbox-supervisor](crates/sandbox-supervisor) | Linux host service and VM ownership |
| [crates/sandbox-guest](crates/sandbox-guest) | Guest bootstrap and command execution |
| [crates/sandbox-client](crates/sandbox-client) | Rust Project API client |
| [sdk](sdk) | Python, TypeScript, and local MCP clients |

## Reference

| Guide | Covers |
| --- | --- |
| [Documentation index](docs/README.md) | Authoritative contracts and reading order |
| [Architecture](docs/architecture.md) | Components, interfaces, and isolation boundaries |
| [Supported configuration](docs/compatibility.md) | Host and guest compatibility envelope |
| [Lifecycle](docs/lifecycle.md) | Operations, completion, cancellation, and recovery |
| [Operator recovery](docs/operator-recovery.md) | Upgrade compatibility and private recovery sets |
| [Threat model](docs/threat-model.md) | Security promises, assumptions, and validation |
| [Roadmap](docs/roadmap.md) | Implemented work and remaining release evidence |
| [Versioning](docs/versioning.md) | SemVer scheme, version bumps and releases |

## Contributing

Small fixes can start with a PR; discuss substantial features or architecture
changes in an issue first. Read [CONTRIBUTING.md](CONTRIBUTING.md) for validation,
review, and squash merging. Report vulnerabilities through [SECURITY.md](SECURITY.md).

## License

Released under [Apache-2.0](LICENSE).
