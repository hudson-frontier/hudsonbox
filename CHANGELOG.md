# Changelog

All notable changes to Hudsonbox are recorded here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and versions follow
[Semantic Versioning](https://semver.org/spec/v2.0.0.html) as described in
[docs/versioning.md](docs/versioning.md). Before 1.0.0, a minor release may
contain breaking changes; each one is called out under "Breaking changes".

Release sections are generated from Conventional Commit titles by
`python3 scripts/version.py changelog X.Y.Z --write` and edited for accuracy
in the release pull request. Links inside a release section must be absolute
URLs because the section becomes the GitHub release notes.

## [0.0.1] - 2026-10-31

First versioned snapshot of the `main` branch, named "hudsonbox 0.0.1". It is a
development release for integration and evaluation, not a supported production
release. It publishes source and release notes only; no binaries, images or
packages are uploaded to any registry.

### Added

- **Sandbox lifecycle API.** Authenticated HTTPS/JSON project API with
  project-scoped bearer tokens, transactional admission and project-wide
  idempotency keys for create, execute, cancel, file transfer and destroy, plus
  operation and sandbox status/list routes and liveness/readiness probes. The
  contract is described by `api/openapi.json` and checked by
  generation and conformance tests.
- **Clients.** A Rust client and private API test CLI (`sandbox-api-test-client`,
  `--version` prints `hudsonbox 0.0.1`), Python (`hudsonbox-client`) and
  TypeScript clients generated from the same OpenAPI document, an optional
  local MCP stdio adapter (`hudsonbox-mcp`), and a durable HTTP workflow
  example with a recovery conformance corpus. Public operator commands live in
  the separate Hudson CLI (`hudson sandbox`).
- **Firecracker host runtime.** Linux/KVM supervisor service (`sandbox-host`)
  with independent per-allocation lease guardians, epoch fencing, authenticated
  host capability contracts, verified guest images built from pinned kernel and
  Debian inputs, guest bootstrap identities and an authenticated vsock guest
  agent for command execution, cancellation and file transfer.
- **Durable PostgreSQL operations.** Fenced controller claims, reconciliation
  after restarts, bounded concurrent controller workers, operation retention,
  output archiving to S3-compatible storage with expiry cleanup, allocation
  permits, retirement scopes, history reclamation and an offline storage audit
  for database recovery rehearsal.
- **Multi-host fleet.** Discovery and tracking of multiple authenticated
  supervisor hosts (#203, #204), owner routing for controller and API requests
  (#207), atomic multi-host placement with reservations (#206), host-loss
  reconciliation without premature recovery (#208), fleet diagnostics (#220)
  and qualification tests for duplicate identities and combined faults (#225).
- **Warm pools.** Warm-pool records, fenced adoption protocol and create routing
  (#215), durable allocation identities for unused warm guests (#217), real-host
  adoption through a fenced guardian handoff (#218), a guarantee that adopted
  members are never re-pooled (#224), isolated grant/revocation/quota denials
  on warm claim (#223) and a KVM-gated real supervisor warm lifecycle suite
  (#226).
- **Capacity adapters.** A standalone host capacity provider (#209) and a
  Kubernetes capacity adapter with worker node accounting and pod replacement
  guards (#221).
- **Operator tooling.** Native source installation, a read-only local doctor,
  private local certificate preparation, offline host provisioning and
  draining (`sandbox-host-manager`), and host epoch advancement.
- **Versioning.** A single root `VERSION` file, `scripts/version.py`
  consistency checks and Conventional Commit bumping, this changelog and a
  release workflow that tags `vX.Y.Z` after the release commit reaches `main`.

### Known limitations

- Not qualified on real hardware at fleet scale: multi-host, warm-pool and
  Kubernetes code is tested with fake supervisors and PostgreSQL. Real
  Linux/KVM and real Kubernetes qualification remain open in
  [#229](https://github.com/hudson-frontier/hudsonbox/issues/229).
- No checkpoint or snapshot restore: pause/resume, checkpoint-based warm
  restore and host-loss recovery wait on
  [#178](https://github.com/hudson-frontier/hudsonbox/issues/178). Host loss
  reports uncertainty instead of restoring work.
- Not deployed anywhere and not a supported production release; supported
  x86_64 isolation and performance qualification and a complete operator
  installation are pending. See the
  [roadmap](https://github.com/hudson-frontier/hudsonbox/blob/main/docs/roadmap.md).
- Pre-1.0: the HTTP API, SDKs, CLI flags, database schema and host protocols
  may change incompatibly in any 0.x release.

[0.0.1]: https://github.com/hudson-frontier/hudsonbox/releases/tag/v0.0.1
