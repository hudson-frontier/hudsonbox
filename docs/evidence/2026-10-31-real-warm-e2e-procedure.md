# Real warm-pool lifecycle procedure (#188)

Status: **NOT RUN — no KVM hardware available.** This file records the
procedure only. It is not evidence that any scenario passed. Hosted CI compiles
the ignored test target on Linux; compiling proves nothing about real VM
behavior, cleanup or latency.

## What the suite covers

[`crates/sandbox-supervisor/tests/host_warm.rs`](../../crates/sandbox-supervisor/tests/host_warm.rs)
drives the real `sandbox-host` supervisor over its authenticated gRPC
interface. Each test starts a permit-enforcing host whose capacity admits one
one-vCPU VM, so a refused second reservation shows that a member still holds its
capacity. The guest image is the controlled BusyBox image from
[`tests/support/vm.rs`](../../crates/sandbox-supervisor/tests/support/vm.rs)
with the reviewed local guest binary as init. Warm members boot from that same
image; no separate template bootstrap exists.

| Test | Scenario |
| --- | --- |
| `real_warm_prepare_adopt_exec_and_destroy` | PrepareWarmMember returns a `PrepareReceipt` only after the inert member endpoint (vsock port 53, ALPN `hudson-warm/1`) authenticated the boot. Adopt with the registered permit binds the same guardian and VM. The allocation channel (port 52, ALPN `hudson-guest/1`) answers the same boot. Ordinary Inspect reports ready, then a command runs and destroy cleans up. A mismatched permit is refused first. |
| `real_warm_lost_adopt_reply_and_inspect_fence` | After a discarded Adopt reply, InspectWarmHandoff returns the persisted handoff without changing the record. On a second, unadopted member, Inspect fences the scope and never binds it. A late Adopt gets the recorded refusal, and another scope gets `AlreadyExists`. |
| `real_warm_retire_holds_capacity_until_cleanup_proof` | Retirement answers only with guardian-proven cleanup: the guardian is stopped with cleanup confirmed, and the cgroup and run directory are gone. A capacity probe is refused while the answer is pending and accepted only after the proof. A later Adopt is rejected with `member_retired`. |
| `real_warm_previous_epoch_member_cleanup_after_restart` | The host restarts at epoch 2 and refuses ordinary retirement for epoch 1. The `PreviousRetireIntent` envelope returns a verified `PreviousCleanupReceipt`. A capacity probe is never admitted while the journal still records the member as holding capacity. |
| `real_cold_versus_warm_time_to_ready` | Takes three samples each of cold create-to-ready and warm adopt-to-ready, plus prepare time, which is off the request path. It prints the measurements and makes no latency assertions. |

## Prerequisites

- The Linux/KVM host prerequisites in
  [local development](../local-development.md#check-the-actual-linux-runtime-host).
  `python3 scripts/doctor.py --profile linux-runtime --json` must pass inside the
  dedicated Linux development VM or on a compatible Linux host.
- Verified runtime artifacts and a reviewed local guest build, prepared as
  described in [Linux development](../linux-development.md):
  `HUDSON_GUARDIAN_ARTIFACT_DIR` and `HUDSON_GUARDIAN_GUEST_BINARY` must be
  canonical absolute paths.
- Root, cgroup v2 with the dedicated `/sys/fs/cgroup/hudson-guardians-tests`
  parent, static BusyBox at `/bin/busybox`, `mkfs.ext4` and `ldd`.
- No PostgreSQL or object storage is needed.

## Run command

Run from the reviewed source checkout on that host:

```sh
sudo env HUDSON_GUARDIAN_TEST_VM=1 \
  HUDSON_GUARDIAN_ARTIFACT_DIR="$HUDSON_GUARDIAN_ARTIFACT_DIR" \
  HUDSON_GUARDIAN_GUEST_BINARY="$HUDSON_GUARDIAN_GUEST_BINARY" \
  cargo test -p sandbox-supervisor --test host_warm -- --ignored --nocapture --test-threads=1
```

`--nocapture` shows the `warm_e2e ... ms` timing lines. After the run, check
that `/sys/fs/cgroup/hudson-guardians-tests` has no child groups. A fixture
whose cleanup is unconfirmed keeps its temporary directory and prints its path,
and that state must be inspected before anyone deletes it.

## Result

NOT RUN — no KVM hardware available. Record the source hashes, host details,
pass/fail counts and the printed timing lines here once the suite has run on a
qualified host.
