# Install native tools from source

The [source installer](../scripts/install_source.py) builds reviewed committed
Rust sources and installs native application binaries into a **new private
prefix**. It is distribution groundwork for
[#111](https://github.com/hudson-frontier/hudsonbox/issues/111). Dependency
provisioning, host/image configuration, a complete operator installation and
independent runtime acceptance remain separate steps.

## Prerequisites and profiles

Use native Linux or macOS on `x86_64` or `aarch64`/`arm64`, Python 3.11+, Git,
the repository-pinned Rust toolchain and protobuf compiler. The
[read-only doctor](local-development.md#diagnose-before-starting-dependencies)
checks build prerequisites. Cargo can use its existing dependency cache or
download locked public dependencies; toolchain auto-installation is disabled.

| Profile | Native application packages |
| --- | --- |
| `control-plane` | Authenticated API, controller/operator tools, project CLI and cleanup worker |
| `linux-runtime` | Linux supervisor, host service and guest init/runner |

Both profiles compile native binaries. The runtime profile requires Linux,
but building it does not use KVM or execute guest init. It does not build a
guest image or fetch Firecracker/jailer. Follow
[verified host startup](verified-host-startup.md) and
[Linux development](linux-development.md) for those artifacts and runtime gates.

## Build and install

Review and commit the source changes to install. Run from a checkout with a
working native Git repository. Choose a new absolute prefix under an existing
canonical directory owned by the operator with no group/other access:

```sh
python3 scripts/install_source.py \
  --profile control-plane --prefix /absolute/private/hudsonbox-tools
```

The installed binaries are in `/absolute/private/hudsonbox-tools/bin`. The JSON
report and `install-receipt.json` record the captured immutable source revision,
source archive digest, pinned toolchain, native OS/architecture and each binary's
name, size and SHA-256. The prefix is `0700`; its receipt is `0600` and its native
binaries are executable by the owner. Add its `bin` directory to the caller's
chosen shell environment when needed; the installer changes no shell profile or
system installation path.

The installer captures a Git archive from the exact recorded revision, with
Cargo/lock/toolchain inputs, Rust/protobuf source, migrations and reviewed runtime
pins. It excludes working-tree changes, `.git`, local environment/configuration,
SDK caches and unrelated repository files. The installer script's working copy
drives the build; review it as well as the captured application source. Cargo
runs trusted build tools and build scripts with the invoking user's authority;
this is a source build, not an isolated execution environment.

Compilation takes place in an invocation-owned private build directory. Native
executable headers and artifact paths are checked before exclusive publication.
An existing prefix, symlinked/public parent, redirected target or another
architecture fails without replacing an installation. Files and directories
are synced before success. A successful invocation removes only its owned
temporary build. A failure retains build output and any partially published
prefix for inspection; retry with a new prefix after addressing its cause.

The tools remain operated through the existing
[authenticated API setup](api-server.md),
[host maintenance](host-maintenance.md) and
[workload workflow](local-workload.md). Installation does not start a service,
create credentials, provision database/storage, change host networking, enable
simulation or establish hardware/isolation qualification.

## Validation

```sh
make source-install-check
python3 scripts/check_docs.py
git diff --check
```

[Boundary tests](../scripts/test_install_source.py) use tiny local Git repositories
and synthetic native headers. They cover committed-source selection, allowlist
exclusion, receipt hashes, native architecture checks, path traversal, archive
links/devices, redirected targets, concurrent prefix creation and non-overwrite.
They start no service and compile no workload. A real native build remains a
separate validation of the selected source/toolchain and resulting CLI binaries.
