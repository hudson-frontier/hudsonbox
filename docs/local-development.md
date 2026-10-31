# Local development without paid infrastructure

The current focus is building and running on available local hardware. PostgreSQL
and S3-compatible storage run locally; no rented host, managed service, cloud
account or deployment is required. The [implementation roadmap](https://github.com/hudson-frontier/hudsonbox/issues/117)
tracks the missing workload image, quickstart and security gates. This guide is
prerequisite diagnosis, not a complete installer or a qualified workload quickstart.

## Diagnose before starting dependencies

From the repository root, using Python 3.11 or newer:

```sh
python3 scripts/doctor.py
python3 scripts/doctor.py --json
```

The default `control-plane` profile checks native macOS/Linux build prerequisites:
Python, the repository-pinned Rust compiler and Cargo, protobuf compiler, Docker
CLI and Compose v2, Node 24.9.0+ and npm. Node/npm are needed by the shared SDK
checks in `make check`. Docker CLI presence does not mean a daemon is running or
that the local PostgreSQL/MinIO stack is healthy. Choose a local container engine
you can use without paying for infrastructure; the diagnostic does not select,
install or start one.

Version probes have a three-second timeout and use fixed commands without a shell.
Rustup automatic toolchain installation is disabled for probes; install the pinned
toolchain explicitly if it is missing. Raw subprocess output, environment values,
credential files and client configuration are not included in the report. The
doctor invokes installed tools from your trusted `PATH`; it does not sandbox those
tools. It never calls sudo, starts Docker/Lima or contacts the sandbox API.

Exit status is `0` only when required checks pass, or `1` when a required check is
missing or fails. JSON uses `schema_version: 1`, `profile`, `system`, `architecture`,
`scope`, `ready` and `checks`. Each check has `name`, `status`, `detail` and `action`;
status is `passed`, `missing`, `failed` or informational `info`. `ready` means only
that this profile's prerequisite checks passed. It does not certify a working
service, VM boot, resource enforcement, networking policy or isolation.

## Build and test the control plane

The API, controller, CLI and fake supervisor run natively on macOS or Linux.
PostgreSQL and MinIO run through Compose. The fake supervisor starts no VM or
customer process; real execution requires Linux/KVM.

Install the pinned Rust toolchain, Python 3.11+, Node 24.9.0+, Docker Compose and
`protoc` (`brew install protobuf` on macOS). Copy `.env.example` to `.env` for the
local dependency configuration, then run:

```sh
make api-setup
make sdk-setup
make mcp-setup
make up
make check
make down
```

`make down` keeps data. `make reset-db` drops the development schema; use it only
for disposable development data. The default Compose project is `hudsonbox-dev`;
set `COMPOSE_PROJECT_NAME` to reuse an existing project's volumes. Choose unused
ports rather than stopping another instance. PostgreSQL and MinIO use ports
55432 and 59000 by default. MinIO is built from [verified sources](../dev/object-storage/README.md).

Follow [authenticated API setup](api-server.md) to provision and run the service.
The [source installer](source-installation.md) builds committed native tools into
a new private prefix; installation is separate from instance setup and runtime qualification.

Multi-host behavior is exercised locally with two fake supervisors:
`cargo test -p sandbox-controller --test fleet_qualification` (with PostgreSQL
running). This portable result is not two-host Linux/KVM evidence; the
operator procedure in the [fleet qualification record](evidence/2026-10-30-fleet-qualification.md#operator-two-host-linuxkvm-qualification-not-run)
has not been run.

Use the [Linux guide](linux-development.md) for the nested-aarch64 development
host and pinned VM artifacts. The supervisor runs on Linux; the guest agent runs
inside each microVM. Build image artifacts for each supported architecture;
aarch64 development results do not qualify the supported x86_64 release envelope.

History cleanup is opt-in. Configure [history retirement](history-reclamation.md#operator-activation)
before `--retire-history`. Registered physical hosts can additionally use
`--retire-allocations` for [whole-allocation reclamation](allocation-retirement.md#automatic-controller-worker);
this requires history retirement and cannot run with simulation. For the periodic
CLI integration test, first build `cargo build -p sandbox-controller --bin sandbox-controller`
in the same Linux checkout. Use separate Cargo target directories across worktrees
to avoid testing against another checkout's embedded migration set.

## Check the actual Linux runtime host

Firecracker requires Linux/KVM. Run this profile **inside** the dedicated Linux
development VM or on an already-available compatible Linux host, from its copied
source directory:

```sh
python3 scripts/doctor.py --profile linux-runtime --json
```

The runtime profile reuses the [Linux host preflight](../scripts/check_linux_host.py),
which checks access to KVM API 12 and the root cgroup v2 CPU/memory/PID controllers.
It also requires Firecracker and jailer on `PATH` with matching reported versions.
Version equality does not verify artifact digests or qualify a production release.
Use the [recorded Linux setup](linux-development.md) for verified artifacts and the
dedicated Lima configuration. The doctor does not start or inspect Lima instances;
running `linux-runtime` on the Mac always fails, even if Lima is installed.

KVM is opened only to query its API; no VM is created. A permissions failure is a
failed prerequisite, not a request to automatically elevate privileges. Root
cgroup controller availability does not prove the supervisor's delegated parent,
actual limit enforcement or namespace/egress isolation. Those have separate
[runtime gates](roadmap.md#required-evidence-by-delivery-gate).

The existing aarch64 Lima path is local development evidence. The selected
x86_64 Ubuntu 24.04 release envelope still needs its own tests on compatible
hardware. If none is available at zero cost, leave that release gate pending and
continue native and aarch64 work. Do not purchase/rent infrastructure to satisfy
this guide, change the compatibility envelope silently or claim an emulated/fake
test proves hardware isolation. Preserve unrelated local instances and volumes.

## Check the diagnostic itself

```sh
make doctor-check
make docs
git diff --check
```

The unit tests use mocked tool/platform/KVM results. They need no daemon, network,
credentials, sudo or running VM. Passing those tests validates diagnostic behavior,
not the runtime security gates.

Optional Kubernetes capacity now has a [scoped adapter and qualification boundary](kubernetes-capacity.md).
The adapter manages worker infrastructure only; real cluster/KVM qualification remains unrun.
