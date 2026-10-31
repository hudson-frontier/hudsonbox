# Real-host warm adoption integration, 2026-10-31

PR [#218](https://github.com/hudson-frontier/hudsonbox/pull/218) rebases only its two host commits onto guest [#217](https://github.com/hudson-frontier/hudsonbox/pull/217) at `04c3c1b51e86c0deea18233762d63a065c8252d1`. Guest/protocol sources are unchanged relative to that checkpoint, preserving canonical SPKI comparison, exactly one fresh allocation CA, retained-file modes and bounded reads/wire. This evidence describes source/unit behavior, not a real warm VM run.

## Contracts and changes

- Guardian issuance uses `HandoffIntent.allocation_id` and generation for allocation bootstrap and SAN. The member bootstrap and VM paths keep the independent opaque slot. A portable regression feeds actual host-issued credentials into the durable guest binder, restarts it after a lost reply, authenticates the allocation TLS channel and rejects the slot as a server name while keeping the same CA and pin.
- The host accepts the previous-retirement envelope from #216 (contract checked against `a19b11e37d6d967ffc73ddce9b498316319d28e1`) through a private wire adapter. The shared protocol crate on the #217 base lacks these types. No guest/protocol changes or controller/store/fake changes are included; replace the adapter with shared types when the stacks combine under parent coordination.
- Previous cleanup requires exact retained member, optional requested VM name resolved to the retained VM, original host/epoch and current reporting epoch. A single operation ID with nondecreasing claim revision is saved with the retirement fence before any effect. Lost responses and a subsequent host epoch retain that claim. Replies echo the exact request and original-epoch cleanup identity. Unknown old members and adopted members refuse cleanup.
- A historical stopped receipt is insufficient by itself: the guardian must yield its lifecycle lock, verify the retained receipt and confirm current cgroup/runtime/socket absence before host-local capacity is released or previous cleanup is acknowledged. Partial/missing metadata refuses proof.
- An unnamed retire that races ahead of prepare saves a bounded refusal tombstone before returning absence. A delayed prepare cannot launch that identity, including after restart. Named unknown retire remains refused.
- The 256-identity limit remains an explicit operator lifetime budget, shared by member records and refusal tombstones. No retired/adopted authority is deleted. The owning [warm-pool document](../warm-pools.md#remaining-gates) describes exhaustion and replacement-host procedure. Automatic pruning is unsafe without a proven fence against delayed callbacks and identity reuse.
- Preparation/adoption preserve the existing no-NIC isolated baseline. No allocation egress integration is implemented or claimed.

## Validation

The initial work stayed source/light only while #216 owned the build slot. After an explicit slot grant, focused validation uses the idle existing `t3-cffe33ba/target`, jobs=2, with disk monitored (about 19 GiB free at start). No new stack or infrastructure was started.

Final focused results are recorded here after completion. Native host unit tests, native strict Clippy and Linux all-target compilation are in progress. Linux compilation reuses the existing clang/header shim and is type-check evidence only; it does not link or run Linux code.

New regressions cover portable cleanup identity/claim/reporting-epoch fences, reclaim after crash/lost reply, adopted-member refusal, wire bounds, host-issued opaque-slot TLS identity; Linux host journal write poisoning, missing guardian evidence, retained identity budget and unknown-retire tombstones; and a dedicated-root Linux guardian absence/lock/path test that needs no KVM but remains opt-in.

## Runtime and review gates

No Linux/KVM environment is available. No actual prepare/adopt/re-key, vsock transport, guardian process crash/restart, workload isolation or cold-versus-warm latency measurement was run. Native linked Linux tests (including host journal regressions) and the dedicated-root guardian absence test still need execution. Full hosted required checks and independent test/security review must apply to the final head. PR #218 remains draft. No merge, deployment, watch, paid infrastructure, snapshots or billing claim.
