# Guest warm identity late-binding validation

Date: 2026-10-31. Related: [#188](https://github.com/hudson-frontier/hudsonbox/issues/188),
[draft PR #217](https://github.com/hudson-frontier/hudsonbox/pull/217).

Tested source: `9218fb42a983193cddc3ac72b29574faa6c55f86`, based on
`a963cbff16fe723e7a023414fb2ddb7c2c0cd6e5`. Security fixes for decoded credential
identity, allocation certificate CA/time/SAN validation, bounded no-follow
retained reads and boxed Bind payload are in `31d76f0`; `9218fb4` corrects the
FIFO fixture for macOS. Later changes to this record and the owning contract are
documentation only.

## Environment and commands

Native environment: aarch64 macOS, pinned Rust 1.92.0, jobs 2. Reused the idle
existing Cargo target; no new services, stack, VM, deployment or paid resource.
Available disk after checks: 22 GiB. The build slot was explicitly granted and
released after these checks.

```sh
export CARGO_BUILD_JOBS=2
export CARGO_TARGET_DIR=/Users/safal/.t3/worktrees/hudson-sandbox/t3-cffe33ba/target
cargo test --locked -p sandbox-guest --test warm_bind
cargo clippy --locked -p sandbox-protocol -p sandbox-guest --all-targets -- -D warnings
cargo test --locked -p sandbox-protocol -p sandbox-guest
```

All passed. Focused suite: 13 tests. Full native suite: 106 tests (79 protocol
unit tests, 10 protocol wire tests, 4 guest model tests and 13 warm-binding tests).
Linux-only guest process/file/runner tests are excluded by target cfg on macOS;
zero-test binaries are not Linux evidence.

The following also passed strict Linux source checking:

```sh
CC_aarch64_unknown_linux_gnu=clang CFLAGS_aarch64_unknown_linux_gnu='--target=aarch64-unknown-linux-gnu -isystem /tmp/hb188-linux-shim' cargo clippy --locked --target aarch64-unknown-linux-gnu -p sandbox-protocol -p sandbox-guest --all-targets -- -D warnings
```

This reused an existing local shim providing C header declarations for ring's
cross compilation. It checks Rust Linux paths including boot/vsock and tests;
it does not link a runnable Linux binary and is not a substitute for native Linux
CI or runtime evidence. No Linux/KVM tests ran.

Formatting, `git diff --check` and `python3 scripts/check_docs.py` also passed.

## Tested boundary

The new suite uses real rcgen-issued TLS credentials over in-memory duplex
streams and ordinary durable filesystem state. It verifies mandatory exact peer
pinning (including an unpinned client from the same CA), ALPN separation with
otherwise identical TLS material, distinct installed allocation-channel access,
wrong SAN rejection, and rejection of PEM-reencoded member CA/cert/key material.

Binding checks cover member, slot, boot, project digest, epoch, claim revision,
permit serial, version and credential agreement; concurrent conflicting binds
install one identity. Exact retries survive agent restart with the same durable
binding. A new boot or changed bootstrap fails; missing/corrupt committed state,
interrupted pending state and concurrent directory ownership never reset to
unused. Oversized frames and unknown execution actions cannot bind. Retained
incarnation and binding reads promptly reject symlinks, FIFOs, directories,
oversize content and world-readable modes.

The FIFO fixture initially used a rustix helper unavailable on macOS and failed
to compile. It was corrected to the platform `mkfifo` command before all passing
runs above. The earlier remote Clippy large-enum failure was corrected by boxing
the Bind payload without changing its JSON representation.

## Remaining gates

- Companion real-host/guardian integration must verify registered permit issuance
  and current durable ownership before sending Bind. Guest consistency checks
  alone cannot establish host/database authority.
- Native linked Linux build and controlled AF_VSOCK/KVM execution, crash/power-loss
  fault injection, guardian restart, watchdog and cleanup verification.
- Production image packaging must retain private member state on the writable
  rootfs and never put used member state in a reusable base image. Guest root
  remains capable of compromising its own keys/state; host no-repool rules are
  authoritative.
- Host network policy at adoption and measured cold/warm readiness. No performance,
  release isolation or completed real-host warm-pool claim is made here.
