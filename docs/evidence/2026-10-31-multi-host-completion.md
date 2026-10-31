# Multi-host completion evidence, 2026-10-31

This record separates five independent facts for each multi-host/warm-pool item tracked under [#187](https://github.com/hudson-frontier/hudsonbox/issues/187) and [#188](https://github.com/hudson-frontier/hudsonbox/issues/188): implemented, hosted CI passed (exact PR head and workflow run), merged (merge commit), real Linux/KVM verified and deployed. SHAs and run IDs were read with `gh pr view N --json headRefOid,mergeCommit,statusCheckRollup` on 2026-10-31. A passing hosted CI run is source/portable test evidence only; it is not real-KVM or deployment evidence.

Hosted CI for merged items means `docs-check` and the runtime workflow (`Determine runtime scope`, `Client, contract and image checks`, `Rust format and Clippy`, `Rust and integration tests`, `rust-check`) concluded SUCCESS at the listed head. Run links are under `https://github.com/hudson-frontier/hudsonbox/actions/runs/<id>`.

## Merged items

| PR | Scope | Implemented | Hosted CI passed (head, runs) | Merged | Real KVM verified | Deployed |
| --- | --- | --- | --- | --- | --- | --- |
| [#218](https://github.com/hudson-frontier/hudsonbox/pull/218) | Real supervisor warm prepare/adopt/inspect/retire through fenced guardian handoff | Yes | Yes: `d7212a431c541472ae31f72a574b6e41b4b10c4f`; runs 41218959354 (runtime), 41218959230 (docs) | `8bd9f47eafef31ec70e9a2532f7a5a0a8a93e543` | NO | NO |
| [#219](https://github.com/hudson-frontier/hudsonbox/pull/219) | Docs status | Yes (docs only) | Yes: `b56c613a2b07ba51a91646df37530d4759d04ad8`; runs 41218604234 (runtime; Rust/client jobs SKIPPED as docs-only scope), 41218604195 (docs) | `f494ac7585849f6fe40a81645a9129135e9fcaa9` | NO | NO |
| [#222](https://github.com/hudson-frontier/hudsonbox/pull/222) | Supervisor uses shared previous-cleanup protocol types; root-only guardian absence test runs in CI | Yes | Yes: `53eabe984dbf50b8e662ac62f364311517ea26a1`; runs 41219727648 (runtime), 41219727687 (docs) | `99388156cb0c5d5f3fe319cd02753bc60cf0fc53` | NO | NO |
| [#221](https://github.com/hudson-frontier/hudsonbox/pull/221) | Kubernetes same-node worker refusal and pod recreate identity | Yes | Yes: `0a63ead24d74aee38932ba658b9104ea548a64f8`; runs 41220673724 (runtime), 41220673782 (docs) | `1fe3999b1ed7bace65cf7cf9f1633f2b375049e2` | NO | NO |
| [#220](https://github.com/hudson-frontier/hudsonbox/pull/220) | Fleet diagnostics warm reservation accounting and retired gauge | Yes | Yes: `33be17e185b02f82dab0fdce873c2233f38834eb`; runs 41221557956 (runtime), 41221558020 (docs) | `cf2c8f87ebfea76bcec9e254c874ccf85274b606` | NO | NO |
| [#224](https://github.com/hudson-frontier/hudsonbox/pull/224) | Never re-pool adopted warm members: trigger migration 0031 and test | Yes | Yes: `f97c88028ee3f725cc04f65b6717f5455a0606a3`; runs 41222254395 (runtime), 41222254388 (docs) | `78ddb60723857a61d00a6989f272f30504e0a267` | NO | NO |
| [#223](https://github.com/hudson-frontier/hudsonbox/pull/223) | Warm-claim grant, revocation and quota denial tests | Yes | Yes: `8a32471a9e17509e3744683593721c48e3641e7e`; runs 41224785673, 41224785700 | `dcc776f9fe32fa87d107030498815a39100a37fd` | NO | NO |
| [#225](https://github.com/hudson-frontier/hudsonbox/pull/225) | Duplicate supervisor and combined-fault qualification (one same-epoch clone gap ignored, #227) | Yes | Yes: `18dc831d776c3c8da8369ca5a621732dbc8f812c`; runs 41223157210, 41223157253 | `727bebca34c1c214a72fc77ce5a27653b2a5d16a` | NO | NO |
| [#226](https://github.com/hudson-frontier/hudsonbox/pull/226) | KVM-gated real warm lifecycle suite (compiled in CI; all tests ignored there) | Yes | Yes: `fc934dac38780d7b5f432a13724d41c79c2c6a3b`; runs 41223968046, 41223968048 | `f7159853a0d4b579bfe119226fe243d5dbeeb776` | NO (suite NOT RUN) | NO |

Kubernetes evidence for #221 (and the existing [Kubernetes capacity evidence](2026-10-30-kubernetes-capacity.md)) uses a fake Kubernetes API only.

## Remaining limits

Every unrun hardware gate is tracked in [#229](https://github.com/hudson-frontier/hudsonbox/issues/229).

- No real Linux/KVM two-host qualification has been run; see the [operator procedure](2026-10-30-fleet-qualification.md#operator-two-host-linuxkvm-qualification-not-run).
- No real Kubernetes cluster has been used; Kubernetes evidence is fake-API only.
- Cold versus warm readiness has not been measured. No performance claims are made.
- Checkpoint/snapshot restore is blocked on [#178](https://github.com/hudson-frontier/hudsonbox/issues/178).
- Locality-aware placement remains open in [#189](https://github.com/hudson-frontier/hudsonbox/issues/189).
- Network policy at adoption is not implemented; warm members keep the no-NIC isolated baseline.
- The retained warm identity budget is 256 per host with no compaction (see [warm pools](../warm-pools.md#remaining-gates)).
- [#227](https://github.com/hudson-frontier/hudsonbox/issues/227): a same-epoch supervisor clone holding a copied key can release capacity via `FencedAbsent`. Needs a design decision.
- [#195](https://github.com/hudson-frontier/hudsonbox/issues/195): no operator-recorded fencing exists for a host that never returns. Needs a design decision.
- Local PostgreSQL tests were not run during this round; database test evidence is hosted CI only.
