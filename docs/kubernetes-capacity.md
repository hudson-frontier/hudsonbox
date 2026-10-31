# Optional Kubernetes worker capacity

The #197 adapter supplies worker Pods only. PostgreSQL, controller operations,
allocation reservations and supervisor/guardian fences remain authoritative for
sandbox lifecycle. No sandbox CRD, per-sandbox Pod, automatic checkpoint/replay,
cloud provisioning or installation is introduced. A supervisor Pod runs multiple
Firecracker microVMs; customer code runs inside their guest kernels.

This is an operator integration, not a qualified cluster installer. The reviewed
[capacity policy skeleton](../deploy/kubernetes/capacity-policy.yaml),
[static worker security/mount example](../deploy/kubernetes/worker-envelope.example.yaml) and generated
[Pod envelope](../crates/sandbox-store/src/k8s_capacity.rs) require a separately
built digest-pinned Linux worker image and compatible, dedicated KVM nodes. No
cluster or KVM was available for this change. Do not treat portable tests as an
installation or isolation certificate.

## Durable effects and identity

Use a pool with `type: k8s`, a namespace, worker selector
`hudsonbox.io/worker=true`, explicit access grants and bounded `max_hosts`.
`credential_secret_ref` identifies an immutable, versioned cluster endpoint/CA/
credential configuration; changing its meaning in place is forbidden. Rotate by
using a new reference for new workers. Each worker freezes namespace, credential
reference, complete Pod body, image digest, host ID, epoch, admission budget,
runtime artifact paths/digests, controller certificate pin and per-worker immutable
TLS Secret reference in PostgreSQL before any API effect. Tokens and private keys
are never stored in that intent.

The operator-only `capacity-enqueue` request accepts `action.kind: k8s_provision`,
`action.config` (the existing `HostProvision`) and `action.worker` (the typed
[WorkerConfig](../crates/sandbox-store/src/k8s_capacity.rs)). It enrolls a fresh
UUIDv7 host at epoch 1, initially draining, and reserves an inventory slot even
before the Pod exists. Ordinary standalone `provision` is refused for k8s pools.
Identical idempotency retries return the original intent; changed requests conflict.
A host identity is never reused. Pool, inventory and pending-operation locks bound
concurrent enqueue; up to 1,000 retained workers per pool and eight claim attempts
are permitted. Exhausted or uncertain intents remain retained for operator action.

`capacity-tick --pool-id POOL --k8s-config PRIVATE_FILE` claims at most one intent.
The private JSON file contains version 1, `server` (explicit HTTPS API origin),
`credential_secret_ref`, `ca_file` and `token_file`. These files use the existing
owned, regular, 0600/private-file checks. HTTPS uses the explicit CA, with no
ambient kubeconfig, environment proxies or redirects. Each API request has a
10-second deadline and a 4 MiB response bound; one tick is a bounded sequence,
not a controller-wide polling loop. No new client dependency/version is added.

Provision is create-only: deterministic `hb-<host UUID>` name, no apply/update,
restart policy Never. After timeout or a lost acknowledgement, re-claim and GET
the same object, verify immutable identity/envelope and persist the original UID.
Receipt writes require the current live claim; stale workers cannot commit.
A stale network create is constrained by the same frozen name/body even after
lease loss. Provider retry identity is the unique host/provision intent, not a
claim revision or a mutable Pod IP.

Every Pod has `hudsonbox.io/capacity-tombstone`. Retirement uses UID-conditional
DELETE and retains the finalizer permanently. A late create then conflicts with
the retained object instead of resurrecting a retired identity. The adapter has
no finalizer update permission. Missing or UID-mismatched objects fail closed;
never remove tombstones or reuse their names while any old dispatcher/request
could exist. Retention is deliberately bounded and conservative: exhausted object
quota needs an operator archival/migration plan after independently fencing all
old dispatchers, not automatic garbage collection. This strategy follows
[Kubernetes finalizer semantics](https://kubernetes.io/docs/concepts/overview/working-with-objects/finalizers/)
and [UID delete preconditions](https://kubernetes.io/docs/reference/kubernetes-api/definitions/delete-options-v1-meta/).

## Resource envelope and runtime preparation

Pod requests equal limits for CPU, memory and ephemeral storage. They equal the
supervisor admission capacity plus explicit positive worker overhead (at least
one vCPU, 256 MiB memory and 512 MiB disk). Supervisor memory/disk admission already
includes per-VM allowances (128 MiB VMM, 401 MiB artifacts); leave room for these
within the budget rather than spending the same overhead twice. The generated
host.json pins the enrolled budget, approved capabilities, host/epoch, launch
permits and private state paths; it cannot silently use an operator Secret's
larger admission capacity. The container writes it privately from a frozen JSON
environment value before exec. The immutable Secret holds only `ca.pem`, `tls.crt`
and `tls.key`, mounted root-only and read-only. Never share it across workers.

The worker image must contain `/bin/sh`, `/usr/local/bin/sandbox-host`, guardian,
Firecracker/jailer and verified guest artifacts at the configured paths. The
runtime files describe those image-local paths/digests and nonroot jail UID/GID.
The supervisor remains root for jailer, namespaces, mounts and cgroup setup;
`privileged: true`, writable host cgroup-v2 mount and `/dev/kvm` CharDevice are
explicit review boundaries. The root filesystem is read-only. Allocation state
and temporary files have separate bounded emptyDir volumes. No host state or
allocation directory is inherited on replacement. There is no host PID/network
namespace or host-network guest NIC in this skeleton. Optional distinct API output/file reader certificate pins enable the existing
read-only gRPC services. `artifacts_secret` optionally mounts a private
`config.json` through a single-file `subPath` mount and passes the existing `--output-config` flag for S3 artifacts. Mounting the whole Secret directory is unsupported: projected Secret entries are symlinks and the private credential reader rejects symlinks. The Secret must remain immutable for the worker lifetime; subPath mounts do not refresh rotated contents. Replace the worker with a fresh identity to use a new Secret version;
review matching network destinations and Secret lifetime before use. These are
versioned Secret references, never credential bytes in durable intent.
Network-enabled guests require separate reviewed integration.

Before admitting a node to this pool, its administrator must create and delegate
`/sys/fs/cgroup/hudsonbox` using the node's cgroup manager. The parent must expose
cgroup v2 `cpu`, `memory`, and `pids` controller files, and enable all three in its
`cgroup.subtree_control` so allocation children can enforce their limits. Root
controller availability alone is insufficient. Verify this inside the worker's
cgroup namespace and writable mount before starting `sandbox-host`; its preflight
rejects a missing or undelegated parent. The Pod intentionally does not alter the
node's cgroup hierarchy or systemd delegation. A fresh node without this setup is
not ready for the `hudsonbox.io/kvm` label. Preparation and enforcement on a real
node remain unqualified gates, not results of the fake API tests.

There is no `compute_nodes` accounting model yet. Required Pod anti-affinity on
`kubernetes.io/hostname`, with an all-namespace selector for the common worker
label, permits **one Hudsonbox worker per node across all pools/namespaces**.
Nodes must have the reviewed KVM label and matching architecture, a dedicated
NoSchedule taint and enough allocatable resources including kube/system overhead.
Do not colocate standalone supervisors or manually bypass the common label,
scheduler or affinity. Privileged VM cgroups may live outside the container's
cgroup; dedicated node capacity plus supervisor admission is essential, and
actual cgroup delegation, CPU/memory/storage enforcement and emptyDir accounting
remain real Linux/KVM qualification gates. Requests/limits alone are not evidence
that a privileged nested runtime is constrained. Kubernetes does not provision
usable KVM: a separately configured external autoscaler must supply eligible nodes.

## Registration, drain and failure

`discover --pool-id POOL --k8s-config PRIVATE_FILE` reads a bounded Pod list,
selects the exact pool and reports observed identities through the existing
provider router. Discovery never opens admission. A headless Service and each
Pod's hostname/subdomain give a direct per-worker endpoint; issue its certificate
for the existing registry SAN `host-UUID.sandbox.internal`, with the frozen
certificate pin. The endpoint hostname and authenticated server name are distinct.
Set the host transport using that endpoint, pin and `k8s/NAMESPACE/NAME/UID`
provider reference; approve only after authenticated supervisor health matches
the enrolled epoch/capabilities. This uses the same gRPC/mTLS paths as standalone,
never a shared load balancer. Worker replacement requires a new host ID, Secret,
certificate, endpoint and Pod; old allocations remain with their old owner/epoch.

Advisory `capacity-plan` bounds unmet sandbox demand in workers using
`sandboxes_per_host`, `max_additional_hosts` and pool limits. The operator enqueues
that bounded number of explicit worker intents; repeated suggestions do not
create Pods automatically. Pending workers reserve slots. Only pending Pod demand
is visible to the separately configured node autoscaler. This change does not
implement warm pools or a continuous automatic demand loop.

Drain persists placement closure without stopping or refunding any allocation.
Cleanup continues through the allocation's original owner. Retirement requires
completed provisioning, a draining worker, zero unreleased allocations/unfinished
operations, zero unretired warm members (including preparing, ready, claiming
and retiring members with uncertain cleanup), the expected epoch and fresh
authenticated health. Occupied, unknown,
stale or missing owners refuse voluntary deletion. Before DELETE, retirement
irreversibly closes admission in PostgreSQL. A lost reply leaves an unfinished
intent and occupied inventory slot; retry observes the same UID/deletionTimestamp.
Cancellation of all k8s intents is forbidden because network effects may be
uncertain. A receipt records deletion requested, **not physical termination or
checkpoint durability**. The finalizer-backed terminating Pod remains visible
and may conservatively block node reuse through anti-affinity.

The PDB denies ordinary voluntary eviction until explicit adapter drain/retire;
it cannot prevent force deletion, node failure, pressure eviction or a hostile
administrator. External autoscaler configuration must honor this boundary and
must not remove nodes with occupied/unknown workers. Forced failure uses the
existing host-loss/guardian protocol: retain reservations, fence old ownership,
report unknown outcomes, never infer safe replay/recovery from Pod absence.

## Qualification

[Common portable fleet qualification PR #213](https://github.com/hudson-frontier/hudsonbox/pull/213)
is the #198 source for shared routing/isolation/recovery tests (head
`659e85acd05f8ef7434df4d890d0f4c2e4ee5d82` at handoff). This adapter is stacked on
#209 `f820ddf`, which includes #211/#212, without duplicating #213's harness.
The focused fake Kubernetes integration tests exercise durable effects with real
PostgreSQL and simulated API responses. They do not test scheduling, RBAC
admission, DNS, KVM access, cgroup enforcement, eviction, physical termination or
multiple real guests. Those require a controlled cluster with two compatible
Linux/KVM nodes, independent control-plane persistence, and explicit evidence of
no duplicate live owner before this path can be called qualified.

[Portable check evidence](evidence/2026-10-30-kubernetes-capacity.md) records the
tested source, commands, failures and real-cluster limitations.
