# Local development certificates

The `sandbox-local-pki` operator command prepares a private development CA,
loopback API server certificate, host server certificate and three distinct
service client identities. It supplies certificate files and public fingerprints
for the existing [API](api-server.md) and [internal transport](supervisor-protocol.md).
It is a local setup component for [#111](https://github.com/hudson-frontier/hudsonbox/issues/111)
and [#116](https://github.com/hudson-frontier/hudsonbox/issues/116); installation,
rotation/revocation operations and runtime qualification remain separate work.

## Prepare a new bundle

Build from the pinned workspace source, then choose a **new absolute directory**
under an existing canonical, operator-owned directory with no group/other access.
On Unix, the command creates the bundle directory as `0700` and every file as
`0600`, syncs files and directories, and refuses an existing destination or
symlinked parent. Partial output is retained on failure for operator inspection.

```sh
cargo build --locked --release -p sandbox-controller --bin sandbox-local-pki
target/release/sandbox-local-pki --directory /private/hudsonbox/local-pki
```

The command generates a new typed host ID, recorded in `manifest.json`. To
prepare certificates for an already retained operator host identity, add
`--host-id HOST_ID`. Retain that identity when preparing its host configuration
and initial database provisioning. Preparing a different identity does not
replace an existing host or reconcile its allocations.

The JSON report contains the host ID, expiry, file paths and service leaf
fingerprints. It contains no private key bytes. Filesystem permissions protect
the private CA and service keys; keep this directory outside source control and
customer payloads. Nothing is added to a system trust store. The command starts
no service and contacts no database, object store, VM or external endpoint.

## Bundle and roles

| Report entry | Identity and use |
| --- | --- |
| `ca` | Development trust root and private signing key; distribute only its public certificate to clients/services |
| `api_server` | Server authentication for `localhost`, `127.0.0.1` and `::1` |
| `host_server` | Server authentication for the existing host-ID-specific internal DNS identity |
| `controller` | Separate client-authentication key and leaf pin for lifecycle/control RPCs |
| `output_reader` | Separate client-authentication key and leaf pin for read-only output service |
| `file_reader` | Separate client-authentication key and leaf pin for read-only file service |

All certificates expire seven days after preparation. The CA can sign leaf
certificates, with no subordinate-CA path. Every service has a different private
key and certificate. The `sha256_der` values hash the exact service leaf DER,
matching the existing supervisor's pin format. A CA-valid reader certificate
still cannot call the controller's RPC service.

Pass the API entry's certificate/key paths to `sandbox-api serve` and trust the
CA certificate explicitly in the SDK. Pass the host entry to the host's existing
`--server-cert`/`--server-key` arguments, its public CA certificate to `--ca-cert`,
and the controller entry's fingerprint to `--controller-cert-sha256`. Configure
the optional read services with their distinct reader fingerprints and keys;
the [supervisor transport guide](supervisor-protocol.md) owns their permissions.
Project bearer credentials remain required by the public API.

Use operator-managed certificate issuance and rotation for production. This
bundle does not verify a workload image, attest host capabilities, authorize a
new epoch, rotate an existing trust root or establish a supported installation.

## Validation

[Local PKI tests](../crates/sandbox-controller/tests/local_pki.rs) run the actual
binary in private temporary directories. They verify file permissions,
non-overwrite/symlink boundaries, distinct service keys, public-only reports and
an actual loopback gRPC/mTLS handshake. The generated controller can call the
authenticated fake host, reader identities cannot call its lifecycle service,
and a different host identity fails certificate verification. These are native
software tests; they run no microVM or customer workload.
