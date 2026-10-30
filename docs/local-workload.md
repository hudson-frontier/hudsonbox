# Authenticated local development workload

This development example implements the finite client flow in [#111](https://github.com/hudson-frontier/hudsonbox/issues/111): create, observed readiness, binary upload, guest execution, bounded output/SSE, verified download and explicit destruction. It uses the authenticated Python SDK and durable recovery steps. An operator installer, independent operator reproduction and real microVM teardown/isolation acceptance remain pending. Running the example does not qualify a host or promise saved memory.

## Isolate the local instance

Use already available hardware. Control plane/clients run on macOS or Linux, including WSL for development. Real workloads require the dedicated Linux/KVM runtime described in [Linux development](linux-development.md) and [supported configuration](compatibility.md). Existing WSL may build artifacts without satisfying the controlled-host cgroup/kernel envelope. Free local software options include PostgreSQL, the source-built MinIO fixture, native Docker on Linux, and Lima with a compatible Linux/KVM guest on a Mac. No rented host, paid builder, provider DNS, registry publication or deployment is required.

Run the [read-only doctor](local-development.md). Use a fresh private operator directory, explicitly choose a Compose project name and unused loopback ports, and preserve existing volumes/home mounts. A port conflict means choose another port, not stop another instance.

Choose a new directory you own; keep this shell variable for the commands below. Creation fails if the directory already exists:

```sh
hudson_local_dir="$HOME/hudson-local-alice"
umask 077
mkdir -m 700 "$hudson_local_dir"
```

Create an operator-owned `0600` env file outside source control with the settings below. Generate separate passwords with `python3 -c 'import secrets; print(secrets.token_urlsafe(32))'` and substitute the generated values. Repository fixture credentials are not deployable settings.

```dotenv
HUDSON_POSTGRES_PORT=55433
HUDSON_POSTGRES_USER=hudson
HUDSON_POSTGRES_DB=hudson
HUDSON_POSTGRES_PASSWORD=GENERATED_POSTGRES_PASSWORD
HUDSON_MINIO_PORT=59010
HUDSON_MINIO_CONSOLE_PORT=59011
HUDSON_MINIO_ROOT_USER=hudson-local
HUDSON_MINIO_ROOT_PASSWORD=SEPARATELY_GENERATED_STORAGE_PASSWORD
```

Before the first startup, check that the project name has neither containers nor retained volumes. If either command prints a name, choose a different project name and ports:

```sh
docker ps --all --filter label=com.docker.compose.project=hudson-local-alice --format '{{.Names}}'
docker volume ls --filter label=com.docker.compose.project=hudson-local-alice --format '{{.Name}}'
```

From the source checkout, render configuration privately and start only this project:

```sh
umask 077
docker compose --project-name hudson-local-alice --env-file "$hudson_local_dir/dependencies.env" \
  config > "$hudson_local_dir/rendered-compose.yaml"
docker compose --project-name hudson-local-alice --env-file "$hudson_local_dir/dependencies.env" \
  up -d --wait
cargo build --locked --workspace
python3 -m venv "$hudson_local_dir/python"
"$hudson_local_dir/python/bin/python" -m pip install -r scripts/sdk-requirements.txt
"$hudson_local_dir/python/bin/python" -m pip install --no-build-isolation --no-deps -e ./sdk/python
```

The rendered configuration contains service passwords. Keep it private. Set the operator database URI to this project's explicit port/database/user; never fall back to native PostgreSQL on `5432`. Follow [private storage setup](../dev/object-storage/README.md) for versioned output/source buckets and separately scoped service credentials. Public/anonymous storage and credentials inside the guest image are excluded.

Follow [API server setup](api-server.md) for source-built HTTPS, offline Project provisioning, the Project's host-pool grant and private credential/profile files. Choose a separate CA/certificates and explicit API/supervisor addresses for this instance. Configure the [controller](controller.md) and [real supervisor](real-supervisor.md) with the provisioned host identity/epoch, mTLS pins, image allowlists and service readers. Build and offline-verify the [pinned guest image](../dev/guest-image/README.md). This example requests 1 vCPU, 512 MiB RAM and 1024 MiB writable disk. Start those operator services using their documented commands before the client flow; this example does not silently install or reset them.

## Run the workload

The [example](../sdk/python/examples/local_workload.py) requires a private owned `0700` state directory on a durable local filesystem. One directory represents one logical run. Its Project profile is the SDK's private JSON file with credential/CA references; the profile, credential JSON and referenced CA PEM must be owned by the invoking user and mode `0600`. No token or insecure-auth flag is accepted on the command line.

```sh
umask 077
mkdir -m 700 "$hudson_local_dir/workload-state"
"$hudson_local_dir/python/bin/python" sdk/python/examples/local_workload.py \
  --config "$hudson_local_dir/client.json" --state "$hudson_local_dir/workload-state" \
  --image-digest sha256:REPLACE_WITH_VERIFIED_ROOTFS_DIGEST --wait-seconds 30 run
```

The sample uploads a fixed binary fixture containing NUL/non-UTF-8 bytes, runs an explicit guest `/bin/sh` command to copy it, verifies bounded stdout/stderr and at most four SSE frames within five seconds, captures/downloads `result.bin`, checks its exact bytes/SHA-256 and releases the capture. It then explicitly submits destroy and waits for that operation to succeed. Guest bytes remain in private files; the report contains IDs, counts, digests and provenance, never guest text as instructions or opaque capture credentials.

Create acknowledgement is admission only. Upload/execute require a successful create operation plus authenticated observed running state, generation and simulation metadata. The observed generation is pinned across restarts; replacement refuses continuation. Each operation wait is at most sixty seconds and the invocation has a finite timeout. Waiting or closing the client never cancels work.

Simulated readiness is refused by default before upload/execute. For protocol-only development with an authenticated fake, `--allow-simulated` must be chosen from the start with a new state directory. It neither executes code in the fake nor enables anonymous access. The full byte-for-byte workload still needs a guest; the fake does not run `/bin/cp` or supply its outputs unless a test deliberately models those responses. Reports preserve simulation, and teardown without an observation uses `simulated: null`. Hardware qualification always remains pending.

## Recover or request teardown

Retain the entire private directory. Create/upload/execute/destroy have immutable fsynced requests, stable mutation keys and saved operation handles. Restart with the same profile/image/options: saved handles use GET; an admission with a lost response gets at most one explicit retry with the unchanged key/body. Execute deadlines are chosen once. Credential/origin/CA changes fail closed; coordinate identity recovery instead of editing a saved request.

Unknown/pending/failed operations, expired create/upload/execute results, output gaps, readiness loss and integrity failures stop the example. They never rotate keys or automatically cancel/destroy. A successful destroy can finish cleanup from its retained terminal status even after its result body expires; recovery never repeats upload, execute or capture. Capture has no mutation key: its attempt is persisted before POST and its opaque handle privately after the response. A lost capture response refuses automatic recapture; explicitly destroy the known sandbox:

```sh
"$hudson_local_dir/python/bin/python" sdk/python/examples/local_workload.py \
  --config "$hudson_local_dir/client.json" --state "$hudson_local_dir/workload-state" teardown
```

Teardown never submits create to discover unknown admission. If no create handle arrived, explicitly reconcile that saved step first. This may admit the original create if the first request never reached the API:

```sh
"$hudson_local_dir/python/bin/python" sdk/python/examples/recover_operation.py \
  --config "$hudson_local_dir/client.json" --state "$hudson_local_dir/workload-state/create" submit
```

Then request teardown. This keeps the same create key/request. If submission returns `410 response_expired` with an operation ID, run the same recovery command with `poll` instead of `submit`. Its authenticated GET saves the sandbox identity in a separate immutable receipt, allowing teardown even after the original create response expires. If no operation handle is available or its GET is denied/unavailable, retain the state for operator inspection. Once destroy is prepared, restart completes cleanup without repeating upload, execute or capture. Finished directories return a validated local report marked `saved_report: true`, not new server authorization or current health evidence.

## Verify release; retain local data

The report means authenticated destroy completed through the implemented controller/guardian release contract. Independent controlled-host acceptance still requires actual child exit, empty owned cgroup, removed allocation disk/bootstrap/network resources and zero outstanding reservations. See [guardian cleanup](allocation-guardian.md) and [host maintenance](host-maintenance.md). A CLI exit, database count or simulated result alone is not physical evidence. Record exact source/artifact/host versions when qualifying a run.

After checking release, stop only this dependency project when desired:

```sh
docker compose --project-name hudson-local-alice --env-file "$hudson_local_dir/dependencies.env" down
```

This retains its PostgreSQL/storage volumes. Credentials, certificates, private journals/downloads, immutable artifacts/build caches and separately created development VMs also remain. Avoid volume removal, `make reset-db`, `make clean`, global Docker pruning or deletion of another Lima instance as workload teardown. Storage/history retirement follows separate authenticated fences.

[SDK tests](../sdk/python/tests/test_local_workload.py) cover binary integrity, observed readiness, simulation refusal, generation changes, identical execute retry/deadline, directory-sync failure, lost capture acknowledgement, unknown create admission and cleanup after a lost destroy reply. They use explicitly simulated HTTP responses through the actual SDK. Real hardware flow, a fresh operator installer, network/isolation enforcement and an independent operator result remain open in #111 and its dependencies.
