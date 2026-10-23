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

After fixing prerequisites, follow the existing [native development commands](implementation/dev-env.md#stage-0--everything-except-a-real-vm-on-macos)
and [authenticated API setup](api-server.md). Starting local dependencies and
provisioning credentials are explicit operator steps, not doctor side effects.
The fake supervisor never executes customer code and is not an isolation substitute.

The [source installer](source-installation.md) builds committed native tools into
a new private prefix and records their source and binary hashes. It provides
application binaries; instance provisioning and runtime acceptance retain their
separate gates.

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
