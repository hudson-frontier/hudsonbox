# Linux runtime development

Status: a dedicated nested aarch64 Linux/KVM environment and a real Firecracker/jailer boot have been verified locally. The [guest command runner](guest-runner.md) now has separate execution evidence; the [real lifecycle supervisor](real-supervisor.md) now integrates create/execute/renew/destroy, including public command admission and results. This environment makes that work testable; it does not satisfy the supported x86_64 release gates.

## Delivered development environment and execution path

The development-environment and execution integration requested by
[#28](https://github.com/hudson-frontier/hudsonbox/issues/28) are implemented.
Its original description predates the guest runner, real supervisor and public
execute path. The following records establish those completed development
deliverables at their recorded revisions:

| Requested deliverable | Implementation and recorded evidence |
| --- | --- |
| Dedicated Linux/KVM environment without host mounts, SSH agent forwarding or automatic application forwarding | [Pinned Lima configuration](../dev/lima-linux.yaml), [startup procedure](#start-a-dedicated-development-host) and [boot record](evidence/2026-09-21-aarch64-boot.json) |
| KVM API, cgroup v2 and pinned Firecracker/jailer verified with a real guest boot | [Host preflight](../scripts/check_linux_host.py), [verified boot](#verified-boot-and-its-limits) and [artifact preparation](#prepare-the-pinned-controlled-test-artifacts) |
| Durable guest commands, bounded output, process-tree cleanup and absolute deadlines | [Guest runner](guest-runner.md) and [command execution record](evidence/2026-09-21-aarch64-commands.json) |
| Authenticated public execution and reconciliation without replaying uncertain effects | [Controller command ownership](controller.md#command-admission-and-dispatch-ownership), [public execute record](evidence/2026-09-21-aarch64-public-execute.json) and [cancellation record](evidence/2026-09-21-aarch64-cancellation.json) |

These are historical execution records, not a new run of the current source.
Closing the development-environment issue does not complete the supported-host
or hostile-workload gates. [Networking](networking.md),
[supported configuration](compatibility.md), [history reclamation](history-reclamation.md)
and [the workload quickstart](local-workload.md) retain their own unfinished
implementation and acceptance requirements. Native aarch64 image builds,
current-source sustained reuse and independent operator reproduction also remain
separate work in their respective issues.

## Start a dedicated development host

The [Lima configuration](../dev/lima-linux.yaml) requires Lima 2.2.0 or newer and an Apple Silicon Mac with nested virtualization support (M3 or later, macOS 15 or later). See [Lima's VZ documentation](https://lima-vm.io/docs/config/vmtype/vz/) and [nested virtualization configuration](https://github.com/lima-vm/lima/blob/v2.2.0/templates/default.yaml). It allocates four vCPUs, 4 GiB RAM and a sparse 24 GiB disk. The Ubuntu image is pinned to a dated URL and checksum, without an unpinned fallback.

```sh
brew install lima
limactl validate dev/lima-linux.yaml
limactl start --yes --name=hudsonbox-dev dev/lima-linux.yaml
limactl copy scripts/check_linux_host.py hudsonbox-dev:/tmp/
limactl shell --workdir=/tmp hudsonbox-dev sudo python3 /tmp/check_linux_host.py
```

Use a fresh instance name if `hudsonbox-dev` already belongs to other work. Do not overwrite an existing VM's configuration or disk to reproduce this setup. If the dated upstream image disappears, update its URL and digest in a reviewed change and validate the new image; do not remove checksum verification.

Plain mode disables host filesystem mounts, automatic application-port forwarding and container tooling. SSH agent forwarding, host SSH public-key import, X11 forwarding and proxy-environment forwarding are explicitly disabled. SSH remains available for operator access. The Linux VM still has outbound network access for development dependencies; it is not a restricted customer sandbox. Copy only the needed source files or a tracked source archive into it, without credentials or local `.env` files.

The preflight checks the [KVM API version](https://www.kernel.org/doc/html/latest/virt/kvm/api.html#kvm-get-api-version) and the root cgroup v2 CPU, memory and PID controllers. It returns nonzero on macOS, missing/inaccessible KVM, or missing controllers. A successful preflight does not create a VM or prove isolation, networking policy, kernel compatibility, namespace hardening, or workload resource enforcement.

The setup was also validated by compiling the shared protocol crate with Rust 1.92.0 and running all 29 of its library tests natively in this Linux VM. Those tests do not require PostgreSQL or prove VM isolation.

The instance can be stopped without deleting its disk:

```sh
limactl stop hudsonbox-dev
limactl start --yes hudsonbox-dev
```

## Develop without mounting the host filesystem

The guest user has passwordless sudo for development. Install build dependencies there, then install the repository's pinned Rust toolchain using the [official Rust installation process](https://www.rust-lang.org/tools/install). Do not run customer workloads directly in this outer VM or treat its development privileges as a sandbox policy.

```sh
limactl shell --workdir=/tmp hudsonbox-dev sudo apt-get update
limactl shell --workdir=/tmp hudsonbox-dev sudo apt-get install -y \
  build-essential pkg-config protobuf-compiler ca-certificates curl
```

Transfer tracked source without `.env`, Git credentials, or the Mac build directory:

```sh
git archive HEAD | limactl shell --workdir=/tmp hudsonbox-dev sh -c '
  mkdir -p "$HOME/hudsonbox"
  tar -xf - -C "$HOME/hudsonbox"
'
```

This is a copy of committed source, so uncommitted changes are not included. Repeated extraction does not delete obsolete files from an earlier copy; use a fresh guest source directory for a clean build. Changes in the guest do not write back to the Mac checkout. PostgreSQL integration tests still need their own database configuration; do not expose the Mac development database automatically.

## Check clocks before cross-host tests

Database claims, supervisor observations, TLS certificates and S3 signatures depend on synchronized wall clocks. Before connecting the Linux VM to storage running elsewhere, compare UTC time on the Mac, Linux VM and storage container. For example:

```sh
date -u
limactl shell --workdir=/tmp hudsonbox-dev date -u
```

During previous-epoch recovery validation, the development VM was about 65 minutes behind local MinIO. Health checks succeeded, but authenticated storage requests returned `RequestTimeTooSkewed`, which surfaced as an archival RPC failure. With test allocation cgroups confirmed empty, synchronizing the dedicated VM to the operator's clock restored signed storage access. Check time synchronization before changing storage credentials or recovery behavior. Correct large clock drift while test workloads are stopped, then rerun the affected checks; an expired lease or a clock correction is never allocation-release evidence.

## Verified boot and its limits

[The recorded evidence](evidence/2026-09-21-aarch64-boot.json) describes the 2026-09-21 experiment on an M4 Pro. The outer Linux kernel was `6.8.0-134-generic`; KVM returned API version 12. A matching Firecracker/jailer `1.17.0` release booted an upstream `6.1.186` aarch64 guest kernel and a disposable BusyBox ext4 rootfs. The guest ran init commands, reported its cgroup controllers and loopback-only network devices, and powered off. The actual Firecracker child exited with status 0 and its cgroup became empty; the test removed that cgroup and disposable rootfs.

The binary archive matched the checksum published with the [Firecracker release](https://github.com/firecracker-microvm/firecracker/releases/tag/v1.17.0). The guest kernel came from the upstream CI artifact source documented in [Firecracker's getting-started guide](https://github.com/firecracker-microvm/firecracker/blob/v1.17.0/docs/getting-started.md); its observed digest is recorded. Neither the kernel nor the disposable rootfs is a Hudson production image or image-attestation implementation.

The jailer was invoked with a unique ID, a private chroot base, UID/GID 65534, `--new-pid-ns`, cgroup v2, and a dedicated parent cgroup. The observed Firecracker process had `NoNewPrivs=1`, seccomp mode 2 and two PID namespace levels. Its cgroup contained `cpu.max=100000 100000`, `memory.max=268435456`, and `pids.max=64`. The microVM itself had one vCPU, 128 MiB RAM, a 64 MiB writable ext4 image and no configured network interface.

These are observed configuration and basic lifecycle facts. Hostile CPU, memory, PID, disk and egress attempts were not run, and an empty interface list is not validation of the planned network-policy engine. There is no performance claim from nested virtualization. The [Phase 1 gates](roadmap.md#scope-discipline-for-phase-1), [threat-model checks](threat-model.md#required-validation), and supported x86_64 validation remain open. The one-off boot experiment is not a shipped workload runner.

## Track the Firecracker child, not just the jailer

The first experiment incorrectly treated the jailer's successful exit as microVM completion. With `--new-pid-ns`, the jailer parent returned status 0 while the Firecracker child was still booting and its cgroup remained populated. The corrected experiment acted as a child subreaper, read the jailer-recorded child PID, waited for that actual child, and checked cgroup emptiness before cleanup.

The production supervisor must account for this behavior. A launcher acknowledgement or exit is not readiness, workload success, or release evidence. Persist ownership and launch intent, retain a reliable child identity, reconcile after restart, and prove the owned allocation has stopped before releasing capacity. Do not use an arbitrary PID file as authority to signal unrelated host processes; kernel process identity and allocation/cgroup ownership must be verified. The existing [controller evidence contract](controller.md#completion-and-uncertainty) remains authoritative.

## Prepare the pinned controlled-test artifacts

The opt-in helper downloads the native `aarch64` or `x86_64` Firecracker 1.17.0
archive and 6.1.186 development kernel using the reviewed
[runtime artifact pins](../dev/runtime-artifacts.json). The aarch64 pins preserve
the recorded boot/public-execute artifacts; the x86_64 archive matches the
[published release digest](https://github.com/firecracker-microvm/firecracker/releases/tag/v1.17.0).
The versioned upstream CI kernels are pinned by observed SHA-256; this is not
kernel source attestation or a production image build. It accepts only a new
absolute destination under an existing canonical parent, verifies bounded HTTPS
downloads before writing, verifies the selected architecture's extracted binary
hashes, selects only the two exact regular binary members
without extracting archive paths, and refuses overwrite. If publication fails, it leaves the partial destination
for operator inspection; it never deletes that destination or retries over it.
It runs without sudo
and never creates or starts a VM. Network access is explicit when invoking it.
Native `arm64`/`ARM64` host labels select `aarch64`; `amd64`/`AMD64` select
`x86_64`. The controlled VM fixture uses the same pin file and selects the native
test binary's architecture. It does not permit architecture emulation.

Run inside the development Linux host, with trusted operator-selected paths:

```sh
python3 scripts/prepare_runtime_artifacts.py \
  --architecture aarch64 --destination /absolute/new/runtime-artifacts
# On a native x86_64 Linux host, select --architecture x86_64 instead.
cargo build --locked --release -p sandbox-guest
export HUDSON_GUARDIAN_ARTIFACT_DIR=/absolute/new/runtime-artifacts
export HUDSON_GUARDIAN_GUEST_BINARY=/absolute/source/target/release/sandbox-guest
```

The root-only VM test fixture requires these absolute paths and validates pinned
Firecracker, jailer and kernel hashes. The guest binary remains a reviewed local
build; the fixture constructs a disposable BusyBox image containing that binary
and its local runtime libraries. Supply the environment explicitly through your
reviewed root test invocation; do not preserve an entire untrusted environment.
The fixture requires native Linux/KVM, cgroup v2, BusyBox, e2fsprogs and its
dedicated cgroup parent. Preparing x86_64 artifacts or compiling its fixture does
not establish that the real-VM tests or supported-host qualification passed.
Database and private object-storage configuration are independent
prerequisites for API/output/file cases. No helper supplies credentials or runs
privileged tests automatically.

```sh
python3 -m unittest discover -s scripts -p test_runtime_artifacts.py
```
