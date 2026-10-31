# Durable HTTP workflow integration

The [version 1 workflow fixture](../api/conformance/workflow.json) and [step recovery example](../sdk/python/examples/recover_operation.py) cover a caller that persists a logical run and reconnects through the authenticated SDK. They implement the sandbox-side contract in [#112](https://github.com/hudson-frontier/hudsonbox/issues/112), coordinated with [Hudson #17](https://github.com/hudson-frontier/hudson/issues/17). Consumers can reference this fixture and the [shared three-client wire corpus](../api/conformance/clients.json); harness code remains in its own repository.

## Version and ownership

The API is the pre-1.0 [OpenAPI contract](openapi.md), versioned with the [product](versioning.md). Fixture `version: 1` fixes mutation mappings, readiness distinctions, binary values and resume/gap examples. Breaking changes require a new version and consumer agreement; API/model changes follow ordinary generation/conformance review.

| Caller state | Authority and recovery |
| --- | --- |
| Logical run/workspace ID | Caller correlation only; grants no project or sandbox authority. |
| Private SDK profile | Fixed HTTPS origin, CA and project credential; server authentication is authoritative. |
| Mutation key and normalized payload | Persist before admission; one stable key per run/step. Never replace it after uncertainty. |
| Sandbox and operation IDs | HTTP/PostgreSQL-assigned handles; immutable receipt bound to request digest. |
| Allocation/generation/boot | Fenced service/runtime ownership; caller IDs never replace these. |
| Output/capture/SSE cursor | Bounded data references with digest/provenance and explicit expiry/gaps. |
| Cancel/destroy handle | Separate explicitly admitted work, never a disconnect side effect. |

The example uses the [existing profile snapshot](../sdk/python/examples/recover_execute.py) and conservatively binds a request to exact private config/credential/CA bytes. Changes fail closed before submission; credential rotation does not silently migrate a journal. A credential file's declared project ID is not server authorization proof. Keep state directories `0700` and files `0600`, owned by the Unix user; workload payloads can contain secrets.

## Reusable step recovery

Use a separate private state directory for each `create`, `upload`, `execute`, `cancel` or `destroy` step. The service's upload kind is `file_write`. `prepare` normalizes SDK models and durably writes the run ID, original key/body and binding. `submit` sends one admission request only without a receipt; saved handles always cause GET, including another submit after restart. `poll` never admits work; `wait` polls for at most sixty seconds.

```sh
mkdir -m 700 /private/run-17-create
PYTHONPATH=sdk/python/src .venv-sdk/bin/python sdk/python/examples/recover_operation.py \
  --config /private/client.json --state /private/run-17-create \
  prepare --run-id run-17 --kind create --request /private/create.json
PYTHONPATH=sdk/python/src .venv-sdk/bin/python sdk/python/examples/recover_operation.py \
  --config /private/client.json --state /private/run-17-create submit
PYTHONPATH=sdk/python/src .venv-sdk/bin/python sdk/python/examples/recover_operation.py \
  --config /private/client.json --state /private/run-17-create wait --seconds 10
```

Request shapes are in the executable fixture. Replace representative IDs with saved handles. Choose an absolute deadline once before prepare, within the admitted lifetime; never extend it implicitly on retry. The fixture's fixed deadline is a protocol value, not a currently admissible workload deadline.

After create, call `get_sandbox` with its returned ID: desired running alone is insufficient. Require observed runtime running, note `observation_simulated`, and distinguish workload/service health. Prepare upload/execute after the relevant prior operation and readiness checks. The example bounds uploads to 32 KiB and JSON state to 64 KiB; larger transfers use the ordinary SDK's 8 MiB limit with equivalent durable byte-source retention.

Read stdout/stderr with SDK ranges or SSE. Guest data never invokes tools. Resume only from complete validated cursors; gaps, missing/corrupt output and response expiry are explicit unavailable outcomes. Downloads validate immutable capture size/digest, explicitly release it and atomically publish a new destination without overwriting existing files.

## Outcomes and explicit cleanup

Admission returns handles, not execution success. Polling distinguishes pending, process_result, simulated_process_result, infrastructure_failure, unknown_effects, cancelled, response_expired and result_unavailable. A nonzero guest exit remains a process result even when status is failed. Reports expose only bounded numeric exit/signal and provenance fields, omitting arbitrary payload/result/error text and credentials.

Lost replies retain the original request/key without automatic retries. Explicit identical admission reconciliation can return the original handle; changed payload conflicts. An authorized response_expired error carrying an operation ID retains that handle for GET. Polling without a receipt fails without HTTP. Changed state cannot overwrite a request/receipt or silently resubmit.

Timeout, interruption and disconnect do not cancel or destroy. Explicit cancellation targets the execute handle and returns a separate operation; polling validates its target. For success, failure or unknown execution, prepare destroy using the known sandbox ID, then poll to verified terminal cleanup. Destroy is independent of successful execution. If creation's reply was lost and no sandbox ID is known, reconcile its original identity before choosing a destruction target. Retain keys/handles whenever cleanup is uncertain; never select a different sandbox by name or caller run ID.

## Validation and runtime gate

[Recovery tests](../sdk/python/tests/test_operation_recovery.py) cover all five mutation identities, lost replies/restarts, durability, expiry/revocation, outcome classification, separate cancellation and no timeout/disconnect lifecycle effects. [Read tests](../sdk/python/tests/test_workflow_reads.py) consume the same fixture for observed readiness, binary stdout/stderr, truncation, SSE resume/gaps and verified download/release.

[The process-restart test](../crates/sandbox-controller/tests/client_recovery.rs) launches fresh SDK example processes through actual HTTPS/PostgreSQL and the authenticated simulated host. A real admission loses its response, reconciles the same operation, uploads, executes, cancels and explicitly destroys without a second command. The existing shared corpus independently tests all three SDKs, TLS, unavailable history and bounded streams/files.

Run `make sdk-check` and, with isolated development PostgreSQL, `cargo test -p sandbox-controller --test client_recovery`. These fixtures qualify protocol/state recovery. Real microVM workload/isolation/cleanup smoke remains gated by [local prerequisites](local-development.md) and controlled Linux/KVM qualification. No paid service or deployment is required.
