//! Portable multi-host qualification (#198): one PostgreSQL database, the HTTP
//! router, and two simulated supervisors with distinct mTLS identities behind
//! the host registry. Two projects hold several allocations on each host.
//! These are fake-supervisor results, never Linux/KVM or Kubernetes evidence.
//!
//! The harness is independent of `owner_routing.rs` and `fleet_recovery.rs`;
//! it reuses only the in-memory source backend from `common/sources.rs`.
#![allow(clippy::unwrap_used, clippy::expect_used)]
#[path = "common/sources.rs"]
mod sources;

use axum::{Router, body::Body};
use http::{Request, StatusCode};
use rcgen::{
    BasicConstraints, CertificateParams, CertifiedIssuer, ExtendedKeyUsagePurpose, IsCa, KeyPair,
    KeyUsagePurpose,
};
use sandbox_api::{
    AppState, files::client::FileClient, router_with_uploads, streams::live::LiveClient,
};
use sandbox_controller::{
    Controller, ControllerConfig, Tick,
    capacity::{ProviderRouter, StandaloneStaticProvider},
};
use sandbox_fake_host::{FakeConfig, FakeHost};
use sandbox_protocol::{
    HostId, Id, OperationId, ProjectId, ProjectToken, SandboxId,
    host_capabilities::{Capabilities, Image, Limits},
    supervisor::{
        file_downloads_server::FileDownloadsServer, live_output_server::LiveOutputServer,
        supervisor_server::SupervisorServer,
    },
};
use sandbox_store::{
    Store,
    capacity::{Action, Claim, Error as CapacityError, Request as CapacityRequest},
    dispatch::DispatchError,
    host_maintenance::{HostCapacity, HostProvision},
    host_registry::{HostPoolSpec, PlacementPolicy, PoolType, Tenancy},
};
use sandbox_supervisor::transport::{self, ControllerIdentity, MAX_MESSAGE_BYTES};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use sources::Sources;
use sqlx::PgPool;
use std::{collections::BTreeSet, sync::Arc, time::Duration};
use tokio::{net::TcpListener, task::JoinHandle};
use tokio_stream::{StreamExt, wrappers::TcpListenerStream};
use tonic::{service::interceptor::InterceptedService, transport::Server};
use tower::ServiceExt;
use uuid::Uuid;

fn digest() -> String {
    format!("sha256:{}", "a".repeat(64))
}

fn capabilities() -> Capabilities {
    Capabilities {
        version: 1,
        simulated: true,
        architecture: "x86_64".into(),
        host_kernel_release: "6.8.0-fixture".into(),
        kvm_api_version: 12,
        cgroup_version: 2,
        supervisor_version: env!("CARGO_PKG_VERSION").into(),
        firecracker_version: "1.17.0".into(),
        firecracker_digest: format!("sha256:{}", "c".repeat(64)),
        jailer_digest: format!("sha256:{}", "d".repeat(64)),
        limits: Limits {
            vcpu: 4,
            memory_mib: 8192,
            disk_mib: 65536,
        },
        images: vec![Image {
            rootfs_digest: digest(),
            kernel_digest: format!("sha256:{}", "b".repeat(64)),
            minimum_disk_mib: 256,
            guest_version: env!("CARGO_PKG_VERSION").into(),
            bootstrap_version: 1,
            guest_protocol_version: 1,
        }],
    }
}

/// Each sandbox takes half of a host's vCPUs: exactly two allocations fit per host.
fn sandbox_request() -> Value {
    json!({"image_digest":digest(),"resources":{"vcpu":2,"memory_mib":2048,"disk_mib":8192}})
}

struct Pki {
    ca: CertifiedIssuer<'static, KeyPair>,
    controller: (String, String, [u8; 32]),
    reader: (String, String, [u8; 32]),
}

impl Pki {
    fn new() -> Self {
        let mut params = CertificateParams::new(Vec::new()).unwrap();
        params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        params.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
        let ca = CertifiedIssuer::self_signed(params, KeyPair::generate().unwrap()).unwrap();
        let client = |name: &str| {
            let key = KeyPair::generate().unwrap();
            let mut params = CertificateParams::new(vec![name.into()]).unwrap();
            params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ClientAuth];
            let cert = params.signed_by(&key, &ca).unwrap();
            (
                cert.pem(),
                key.serialize_pem(),
                Sha256::digest(cert.der()).into(),
            )
        };
        let controller = client("controller.sandbox.internal");
        let reader = client("file-reader.sandbox.internal");
        Self {
            ca,
            controller,
            reader,
        }
    }

    fn server(&self, host: HostId) -> (String, String, String) {
        let key = KeyPair::generate().unwrap();
        let mut params = CertificateParams::new(vec![transport::host_server_name(host)]).unwrap();
        params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
        let cert = params.signed_by(&key, &self.ca).unwrap();
        (
            cert.pem(),
            key.serialize_pem(),
            hex::encode(Sha256::digest(cert.der())),
        )
    }
}

/// One listening supervisor: control, file download and live output services.
struct Supervisor {
    endpoint: String,
    pin: String,
    /// Server certificate and key PEM, so a test can model copied key material.
    identity: (String, String),
    task: JoinHandle<()>,
}

impl Drop for Supervisor {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn serve(pki: &Pki, fake: &FakeHost, identity: HostId) -> Supervisor {
    serve_with(pki, fake, pki.server(identity)).await
}

/// Serve `fake` with the given server certificate, key and leaf pin.
async fn serve_with(
    pki: &Pki,
    fake: &FakeHost,
    (cert, key, pin): (String, String, String),
) -> Supervisor {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("https://{}", listener.local_addr().unwrap());
    let control = InterceptedService::new(
        SupervisorServer::new(fake.clone())
            .max_decoding_message_size(MAX_MESSAGE_BYTES)
            .max_encoding_message_size(MAX_MESSAGE_BYTES),
        ControllerIdentity::new(vec![pki.controller.2]).unwrap(),
    );
    let files = InterceptedService::new(
        FileDownloadsServer::new(fake.clone())
            .max_decoding_message_size(MAX_MESSAGE_BYTES)
            .max_encoding_message_size(MAX_MESSAGE_BYTES),
        ControllerIdentity::new(vec![pki.reader.2]).unwrap(),
    );
    let live = InterceptedService::new(
        LiveOutputServer::new(fake.clone())
            .max_decoding_message_size(MAX_MESSAGE_BYTES)
            .max_encoding_message_size(MAX_MESSAGE_BYTES),
        ControllerIdentity::new(vec![pki.reader.2]).unwrap(),
    );
    let tls = transport::server_tls(pki.ca.pem().as_bytes(), cert.as_bytes(), key.as_bytes());
    let task = tokio::spawn(async move {
        let _ = Server::builder()
            .tls_config(tls)
            .unwrap()
            .add_service(control)
            .add_service(files)
            .add_service(live)
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await;
    });
    Supervisor {
        endpoint,
        pin,
        identity: (cert, key),
        task,
    }
}

async fn fake(host: HostId, epoch: i64) -> FakeHost {
    let fake = FakeHost::new(FakeConfig {
        host,
        epoch,
        images: BTreeSet::from([digest()]),
        capacity: sandbox_protocol::supervisor::Resources {
            vcpu: 4,
            memory_mib: 8192,
            disk_mib: 65536,
        },
    })
    .unwrap();
    fake.set_capabilities(capabilities()).await.unwrap();
    fake
}

async fn register(pool: &PgPool, host: HostId, endpoint: &str, pin: &str) {
    sqlx::query(
        "UPDATE hosts SET endpoint=$2,transport_server_name=$3,transport_certificate_sha256=$4 WHERE id=$1",
    )
    .bind(host.uuid())
    .bind(endpoint)
    .bind(transport::host_server_name(host))
    .bind(pin)
    .execute(pool)
    .await
    .unwrap();
}

async fn project(pool: &PgPool) -> (ProjectId, String) {
    let project = ProjectId::generate();
    let token = ProjectToken::generate().unwrap();
    sqlx::query(
        "INSERT INTO projects(id,name,status,limits,api_tokens) VALUES($1,$2,'active','{}',$3)",
    )
    .bind(project.uuid())
    .bind(format!("p-{}", project.uuid().simple()))
    .bind(json!([{"key_id":token.key_id().as_str(),"hash":hex::encode(token.hash().as_bytes())}]))
    .execute(pool)
    .await
    .unwrap();
    (project, token.render_once())
}

/// Two hosts (A sorts first and wins placement ties) and two projects.
struct Fleet {
    pool: PgPool,
    store: Store,
    pki: Pki,
    app: Router,
    sources: Arc<Sources>,
    projects: [(ProjectId, String); 2],
    hosts: [HostId; 2],
    fakes: Vec<FakeHost>,
    supervisors: Vec<Supervisor>,
}

impl Fleet {
    /// `shared` grants both projects the hosts' default shared pool.
    async fn new(pool: &PgPool, shared: bool) -> Self {
        let projects = [project(pool).await, project(pool).await];
        let pki = Pki::new();
        let mut hosts = [HostId::generate(), HostId::generate()];
        hosts.sort();
        let mut fakes = Vec::new();
        let mut supervisors = Vec::new();
        for host in hosts {
            sqlx::query("INSERT INTO hosts(id,status,cpu_capacity,memory_capacity_mib,disk_capacity_mib,supervisor_epoch,compatibility) VALUES($1,'ready',4,8192,65536,1,$2)")
                .bind(host.uuid())
                .bind(serde_json::to_value(capabilities()).unwrap())
                .execute(pool)
                .await
                .unwrap();
            let fake = fake(host, 1).await;
            let supervisor = serve(&pki, &fake, host).await;
            register(pool, host, &supervisor.endpoint, &supervisor.pin).await;
            fakes.push(fake);
            supervisors.push(supervisor);
        }
        if shared {
            sqlx::query("INSERT INTO project_host_pool_access(id,project_id,host_pool_id) SELECT gen_random_uuid(),p.id,h.host_pool_id FROM projects p CROSS JOIN hosts h GROUP BY p.id,h.host_pool_id")
                .execute(pool).await.unwrap();
        }
        let store = Store::from_pool(pool.clone());
        let files = FileClient::routed(
            Some(store.clone()),
            None,
            pki.ca.pem().as_bytes(),
            pki.reader.0.as_bytes(),
            pki.reader.1.as_bytes(),
            true,
        )
        .unwrap();
        let live = LiveClient::routed(
            Some(store.clone()),
            None,
            pki.ca.pem().as_bytes(),
            pki.reader.0.as_bytes(),
            pki.reader.1.as_bytes(),
            true,
        )
        .unwrap();
        let sources = Arc::new(Sources::default());
        let app = router_with_uploads(
            AppState {
                images: sandbox_protocol::images::ImageAllowlist::new([digest()]).unwrap(),
                store: store.clone(),
            },
            None,
            Some(Arc::new(live)),
            Some(Arc::new(files)),
            Some(sources.clone()),
        );
        Self {
            pool: pool.clone(),
            store,
            pki,
            app,
            sources,
            projects,
            hosts,
            fakes,
            supervisors,
        }
    }

    /// A controller whose configured host is only a legacy default: every
    /// registered host is reached through the registry.
    async fn controller(&self, index: usize) -> Controller {
        Controller::connect(
            self.store.clone(),
            ControllerConfig {
                endpoint: "https://127.0.0.1:9".into(),
                host: self.hosts[index],
                epoch: 1,
                allowed_images: BTreeSet::from([digest()]),
                allow_simulated: true,
            },
            self.pki.ca.pem().as_bytes(),
            self.pki.controller.0.as_bytes(),
            self.pki.controller.1.as_bytes(),
        )
        .await
        .unwrap()
        .with_file_sources(self.sources.clone())
    }

    async fn request(&self, project: usize, request: Request<Body>) -> (StatusCode, Value) {
        let mut request = request;
        request.headers_mut().insert(
            "authorization",
            format!("Bearer {}", self.projects[project].1)
                .parse()
                .unwrap(),
        );
        let response = self.app.clone().oneshot(request).await.unwrap();
        let status = response.status();
        let bytes = axum::body::to_bytes(response.into_body(), 64 * 1024)
            .await
            .unwrap();
        (
            status,
            serde_json::from_slice(&bytes).unwrap_or(Value::Null),
        )
    }

    async fn send(
        &self,
        project: usize,
        method: &str,
        path: &str,
        body: Value,
    ) -> (StatusCode, Value) {
        let request = Request::builder()
            .method(method)
            .uri(path)
            .header("content-type", "application/json")
            .header("idempotency-key", Uuid::now_v7().to_string())
            .body(Body::from(body.to_string()))
            .unwrap();
        self.request(project, request).await
    }

    async fn accepted(&self, project: usize, path: &str, body: Value) -> OperationId {
        let (status, body) = self.send(project, "POST", path, body).await;
        assert_eq!(status, StatusCode::ACCEPTED, "{body}");
        body["operation_id"].as_str().unwrap().parse().unwrap()
    }

    async fn create(&self, project: usize) -> (OperationId, SandboxId) {
        let (status, body) = self
            .send(project, "POST", "/v1/sandboxes", sandbox_request())
            .await;
        assert_eq!(status, StatusCode::ACCEPTED, "{body}");
        (
            body["operation_id"].as_str().unwrap().parse().unwrap(),
            body["sandbox_id"].as_str().unwrap().parse().unwrap(),
        )
    }

    async fn execute(&self, project: usize, sandbox: SandboxId) -> OperationId {
        self.accepted(
            project,
            &format!("/v1/sandboxes/{sandbox}/execute"),
            json!({"argv":["/bin/true"],"deadline_unix_ms":sandbox_fake_host::unix_ms().unwrap()+60_000}),
        )
        .await
    }

    async fn cancel(&self, project: usize, target: OperationId) -> OperationId {
        self.accepted(
            project,
            &format!("/v1/operations/{target}/cancel"),
            json!({}),
        )
        .await
    }

    async fn destroy(&self, project: usize, sandbox: SandboxId) -> OperationId {
        self.accepted(
            project,
            &format!("/v1/sandboxes/{sandbox}/destroy"),
            json!({}),
        )
        .await
    }

    async fn upload(&self, project: usize, sandbox: SandboxId, bytes: &[u8]) -> OperationId {
        let request = Request::builder()
            .method("PUT")
            .uri(format!("/v1/sandboxes/{sandbox}/files?path=uploaded.bin"))
            .header("content-type", "application/octet-stream")
            .header("idempotency-key", Uuid::now_v7().to_string())
            .header("x-file-size", bytes.len())
            .header("x-file-sha256", hex::encode(Sha256::digest(bytes)))
            .body(Body::from(bytes.to_vec()))
            .unwrap();
        let (status, body) = self.request(project, request).await;
        assert_eq!(status, StatusCode::ACCEPTED, "{body}");
        body["operation_id"].as_str().unwrap().parse().unwrap()
    }

    async fn capture(&self, project: usize, sandbox: SandboxId) -> (StatusCode, Value) {
        self.send(
            project,
            "POST",
            &format!("/v1/sandboxes/{sandbox}/files/captures"),
            json!({"path":"uploaded.bin"}),
        )
        .await
    }

    /// Text received from the operation's live stream before the first
    /// output, gap or error event, or before `wait` elapses.
    async fn stream(&self, project: usize, operation: OperationId) -> (StatusCode, String) {
        let request = Request::builder()
            .method("GET")
            .uri(format!("/v1/operations/{operation}/stream"))
            .body(Body::empty())
            .unwrap();
        let mut request = request;
        request.headers_mut().insert(
            "authorization",
            format!("Bearer {}", self.projects[project].1)
                .parse()
                .unwrap(),
        );
        let response = self.app.clone().oneshot(request).await.unwrap();
        let status = response.status();
        let mut body = response.into_body().into_data_stream();
        let mut text = String::new();
        let _ = tokio::time::timeout(Duration::from_secs(5), async {
            while let Some(Ok(bytes)) = body.next().await {
                text.push_str(&String::from_utf8_lossy(&bytes));
                if text.contains("data:") {
                    return;
                }
            }
        })
        .await;
        (status, text)
    }

    async fn status(&self, operation: OperationId) -> String {
        sqlx::query_scalar("SELECT status FROM operations WHERE id=$1")
            .bind(operation.uuid())
            .fetch_one(&self.pool)
            .await
            .unwrap()
    }

    /// Make one operation claimable now; other deferred work keeps its backoff.
    async fn retry_now(&self, operation: OperationId) {
        sqlx::query("UPDATE operations SET next_retry_at=NULL,lease_expires_at=LEAST(lease_expires_at,clock_timestamp()-interval '1 second') WHERE id=$1 AND completed_at IS NULL")
            .bind(operation.uuid())
            .execute(&self.pool)
            .await
            .unwrap();
    }

    async fn drive(&self, controller: &mut Controller, operation: OperationId) -> String {
        for _ in 0..24 {
            let _ = controller.tick().await;
            let status = self.status(operation).await;
            if matches!(status.as_str(), "succeeded" | "failed" | "cancelled") {
                return status;
            }
            self.retry_now(operation).await;
        }
        self.status(operation).await
    }

    /// Drive a cancellation. The held target's claim lease is expired between
    /// ticks, as it would lapse in a running controller, so the target is
    /// reclaimed and its cancel action reaches the owner.
    async fn drive_cancel(
        &self,
        controller: &mut Controller,
        cancel: OperationId,
        target: OperationId,
    ) -> String {
        for _ in 0..24 {
            let _ = controller.tick().await;
            let status = self.status(cancel).await;
            if matches!(status.as_str(), "succeeded" | "failed") {
                return status;
            }
            self.retry_now(cancel).await;
            self.retry_now(target).await;
        }
        self.status(cancel).await
    }

    async fn owner(&self, sandbox: SandboxId) -> Option<HostId> {
        let owner: Option<Uuid> = sqlx::query_scalar(
            "SELECT a.host_id FROM sandboxes s JOIN allocations a ON a.id=s.current_allocation_id WHERE s.id=$1",
        )
        .bind(sandbox.uuid())
        .fetch_optional(&self.pool)
        .await
        .unwrap();
        owner.map(HostId::from_uuid)
    }

    async fn last_seen(&self, host: HostId) -> bool {
        sqlx::query_scalar("SELECT last_seen_at IS NOT NULL FROM hosts WHERE id=$1")
            .bind(host.uuid())
            .fetch_one(&self.pool)
            .await
            .unwrap()
    }

    /// Ownership invariants that must hold at every step of any fault mix:
    /// no sandbox has two unreleased allocations, and every released
    /// allocation carries supervisor release evidence for exactly that
    /// allocation, host and epoch. Capacity is derived from unreleased
    /// allocations, so this is also "no refund without fencing evidence".
    async fn assert_single_live_owner(&self) {
        let duplicated: Vec<(Uuid, i64)> = sqlx::query_as(
            "SELECT sandbox_id,count(*) FROM allocations WHERE released_at IS NULL
             GROUP BY sandbox_id HAVING count(*)>1",
        )
        .fetch_all(&self.pool)
        .await
        .unwrap();
        assert!(duplicated.is_empty(), "{duplicated:?}");
        let unfenced: Vec<(Uuid, Option<Value>)> = sqlx::query_as(
            "SELECT id,release_evidence FROM allocations WHERE released_at IS NOT NULL AND NOT (
                status='released'
                AND release_evidence->>'phase'='allocation_released'
                AND right(release_evidence->>'allocation_id',36)=id::text
                AND right(release_evidence->>'host_id',36)=host_id::text
                AND (release_evidence->>'supervisor_epoch')::bigint=supervisor_epoch
                AND (release_evidence->>'simulated')::boolean)",
        )
        .fetch_all(&self.pool)
        .await
        .unwrap();
        assert!(unfenced.is_empty(), "{unfenced:?}");
    }

    async fn unreleased(&self, sandbox: SandboxId) -> i64 {
        sqlx::query_scalar(
            "SELECT count(*) FROM allocations WHERE sandbox_id=$1 AND released_at IS NULL",
        )
        .bind(sandbox.uuid())
        .fetch_one(&self.pool)
        .await
        .unwrap()
    }

    /// Durable operator intent, dispatched through the controller's capacity router.
    async fn capacity_claim(&self, pool: Uuid, host: HostId, action: Action) -> Claim {
        let id = self
            .store
            .enqueue_capacity(
                pool,
                &Uuid::now_v7().to_string(),
                &CapacityRequest {
                    host,
                    epoch: 1,
                    action,
                },
            )
            .await
            .unwrap();
        let claim = self.store.claim_capacity(pool, 300).await.unwrap().unwrap();
        assert_eq!(claim.id, id);
        claim
    }

    /// Ownership and dispatch history, excluding maintenance scheduling metadata.
    async fn host_history(&self, host: HostId) -> Value {
        sqlx::query_scalar(
            "SELECT jsonb_build_object(
                'allocations', (SELECT jsonb_agg(jsonb_build_object(
                    'id',id,'sandbox',sandbox_id,'host',host_id,'epoch',supervisor_epoch,
                    'generation',generation,'released_at',released_at) ORDER BY id)
                    FROM allocations WHERE host_id=$1),
                'operations', (SELECT jsonb_agg(jsonb_build_object(
                    'id',o.id,'status',o.status,'attempt_count',o.attempt_count,
                    'execution_allocation',o.execution_allocation_id,
                    'receipts',o.attempt_receipts) ORDER BY o.id)
                    FROM operations o WHERE o.sandbox_id IN
                        (SELECT sandbox_id FROM allocations WHERE host_id=$1)))",
        )
        .bind(host.uuid())
        .fetch_one(&self.pool)
        .await
        .unwrap()
    }

    /// Point a host's registration at a port nobody listens on.
    async fn partition(&self, index: usize) {
        let hole = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("https://{}", hole.local_addr().unwrap());
        drop(hole);
        register(
            &self.pool,
            self.hosts[index],
            &endpoint,
            &self.supervisors[index].pin,
        )
        .await;
    }

    async fn heal(&self, index: usize) {
        register(
            &self.pool,
            self.hosts[index],
            &self.supervisors[index].endpoint,
            &self.supervisors[index].pin,
        )
        .await;
    }

    /// Create two sandboxes per project in order p0, p0, p1, p1. Spread
    /// placement alternates A, B, A, B, so each project spans both hosts.
    async fn fill(&self, controller: &mut Controller) -> Vec<(usize, SandboxId)> {
        let mut placed = Vec::new();
        for project in [0, 0, 1, 1] {
            let (create, sandbox) = self.create(project).await;
            assert_eq!(self.drive(controller, create).await, "succeeded");
            placed.push((project, sandbox));
        }
        let mut owners = Vec::new();
        for (project, sandbox) in &placed {
            owners.push((*project, self.owner(*sandbox).await.unwrap()));
        }
        assert_eq!(
            owners,
            vec![
                (0, self.hosts[0]),
                (0, self.hosts[1]),
                (1, self.hosts[0]),
                (1, self.hosts[1]),
            ]
        );
        placed
    }
}

fn denied(status: StatusCode, text: &str) -> bool {
    status != StatusCode::OK || text.contains("event: error") || text.contains("event: gap")
}

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn two_projects_on_two_hosts_route_every_lifecycle_surface_to_its_owner(pool: PgPool) {
    let fleet = Fleet::new(&pool, true).await;
    let mut controller = fleet.controller(0).await;
    let sandboxes = fleet.fill(&mut controller).await;
    for (index, (project, sandbox)) in sandboxes.iter().enumerate() {
        let bytes = format!("fleet-file-{index}").into_bytes();
        let upload = fleet.upload(*project, *sandbox, &bytes).await;
        assert_eq!(fleet.drive(&mut controller, upload).await, "succeeded");
        let command = fleet.execute(*project, *sandbox).await;
        assert_eq!(fleet.drive(&mut controller, command).await, "succeeded");
        // File and live-output reads route to the owner that holds the file.
        let (status, body) = fleet.capture(*project, *sandbox).await;
        assert_eq!(status, StatusCode::CREATED, "{body}");
        assert_eq!(body["size"], bytes.len(), "{body}");
        let (status, text) = fleet.stream(*project, command).await;
        assert_eq!(status, StatusCode::OK, "{text}");
        assert!(text.contains("\"complete\":true"), "{text}");
        assert!(!denied(status, &text), "{text}");
        // The other project learns nothing about the sandbox or its output.
        let (status, _) = fleet.capture(1 - *project, *sandbox).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        let (status, _) = fleet.stream(1 - *project, command).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
    }
    for fake in &fleet.fakes {
        assert_eq!(fake.total_starts().await, 2);
        assert_eq!(fake.total_commands().await, 2);
        assert_eq!(fake.total_file_commits().await, 2);
    }
    // Cancel a running command on each host through the same controller.
    for (host, &(project, sandbox)) in sandboxes.iter().take(2).enumerate() {
        fleet.fakes[host].hold_next_command().await;
        let target = fleet.execute(project, sandbox).await;
        for _ in 0..8 {
            let _ = controller.tick().await;
            if fleet.fakes[host].total_commands().await == 3 {
                break;
            }
        }
        assert_eq!(fleet.fakes[host].total_commands().await, 3);
        let cancel = fleet.cancel(project, target).await;
        assert_eq!(
            fleet.drive_cancel(&mut controller, cancel, target).await,
            "succeeded"
        );
        assert_eq!(fleet.status(target).await, "cancelled");
        assert_eq!(
            fleet.fakes[1 - host].total_commands().await,
            2 + host as u64
        );
    }
    // Lease maintenance reaches every allocation on both owners.
    sqlx::query("UPDATE allocations SET maintenance_next_at=NULL,maintenance_lease_until=NULL WHERE released_at IS NULL")
        .execute(&pool)
        .await
        .unwrap();
    for _ in 0..12 {
        let _ = controller.tick().await;
    }
    let maintained: Vec<(Uuid, i64)> = sqlx::query_as(
        "SELECT host_id,count(*) FROM allocations WHERE released_at IS NULL AND maintenance_next_at IS NOT NULL AND lease_expires_at>clock_timestamp() GROUP BY host_id ORDER BY host_id",
    )
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(
        maintained,
        vec![(fleet.hosts[0].uuid(), 2), (fleet.hosts[1].uuid(), 2)]
    );
    for (project, sandbox) in &sandboxes {
        let destroy = fleet.destroy(*project, *sandbox).await;
        assert_eq!(fleet.drive(&mut controller, destroy).await, "succeeded");
        assert_eq!(fleet.unreleased(*sandbox).await, 0);
    }
    for fake in &fleet.fakes {
        assert_eq!(fake.total_starts().await, 2);
        assert_eq!(fake.total_commands().await, 3);
    }
}

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn concurrent_controllers_create_and_execute_each_operation_once(pool: PgPool) {
    let fleet = Fleet::new(&pool, true).await;
    let mut first = fleet.controller(0).await;
    let mut second = fleet.controller(1).await;
    let mut creates = Vec::new();
    for project in [0, 1, 0, 1] {
        creates.push(fleet.create(project).await);
    }
    for _ in 0..24 {
        let _ = tokio::join!(first.tick(), second.tick());
        let mut pending = false;
        for (operation, _) in &creates {
            if fleet.status(*operation).await != "succeeded" {
                pending = true;
                fleet.retry_now(*operation).await;
            }
        }
        if !pending {
            break;
        }
    }
    for (operation, _) in &creates {
        assert_eq!(fleet.status(*operation).await, "succeeded");
    }
    // One live owner per sandbox and one supervisor start per create.
    let owners: Vec<(Uuid, i64, i64)> = sqlx::query_as(
        "SELECT sandbox_id,count(*),count(*) FILTER (WHERE released_at IS NULL) FROM allocations GROUP BY sandbox_id",
    )
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(owners.len(), 4);
    assert!(owners.iter().all(|(_, all, live)| (*all, *live) == (1, 1)));
    assert_eq!(
        fleet.fakes[0].total_starts().await + fleet.fakes[1].total_starts().await,
        4
    );
    let mut commands = Vec::new();
    for (index, (_, sandbox)) in creates.iter().enumerate() {
        commands.push(fleet.execute(index % 2, *sandbox).await);
    }
    for _ in 0..24 {
        let _ = tokio::join!(first.tick(), second.tick());
        let mut pending = false;
        for operation in &commands {
            if fleet.status(*operation).await != "succeeded" {
                pending = true;
                fleet.retry_now(*operation).await;
            }
        }
        if !pending {
            break;
        }
    }
    for operation in &commands {
        assert_eq!(fleet.status(*operation).await, "succeeded");
    }
    assert_eq!(
        fleet.fakes[0].total_commands().await + fleet.fakes[1].total_commands().await,
        4
    );
    let attempts: Vec<(i32,)> =
        sqlx::query_as("SELECT attempt_count FROM operations WHERE kind='execute'")
            .fetch_all(&pool)
            .await
            .unwrap();
    assert_eq!(attempts, vec![(1,); 4]);
}

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn partitioned_cancel_stays_unknown_without_replay_while_other_host_progresses(pool: PgPool) {
    let fleet = Fleet::new(&pool, true).await;
    let mut controller = fleet.controller(0).await;
    let sandboxes = fleet.fill(&mut controller).await;
    let (project, on_b) = sandboxes[1];
    fleet.fakes[1].hold_next_command().await;
    let target = fleet.execute(project, on_b).await;
    for _ in 0..8 {
        let _ = controller.tick().await;
        if fleet.fakes[1].total_commands().await == 1 {
            break;
        }
    }
    assert_eq!(fleet.fakes[1].total_commands().await, 1);
    let cancel = fleet.cancel(project, target).await;
    // The cancel reaches B but its reply is lost, then B is partitioned.
    fleet.fakes[1].lose_next_cancel_reply().await;
    let mut unknown = false;
    for _ in 0..8 {
        if controller.tick().await.ok() == Some(Tick::Unknown) {
            unknown = true;
            break;
        }
        fleet.retry_now(cancel).await;
        fleet.retry_now(target).await;
    }
    assert!(unknown);
    assert_eq!(fleet.status(target).await, "unknown");
    fleet.partition(1).await;
    for _ in 0..3 {
        fleet.retry_now(cancel).await;
        fleet.retry_now(target).await;
        let _ = controller.tick().await;
    }
    assert!(!matches!(
        fleet.status(cancel).await.as_str(),
        "succeeded" | "failed"
    ));
    assert_eq!(fleet.status(target).await, "unknown");
    // Host A keeps executing under the same controller.
    let (project_a, on_a) = sandboxes[0];
    let healthy = fleet.execute(project_a, on_a).await;
    assert_eq!(fleet.drive(&mut controller, healthy).await, "succeeded");
    // A replacement controller inspects B on return; nothing is replayed.
    fleet.heal(1).await;
    tokio::time::sleep(Duration::from_millis(600)).await;
    let mut replacement = fleet.controller(0).await;
    assert_eq!(
        fleet.drive_cancel(&mut replacement, cancel, target).await,
        "succeeded"
    );
    let result: Value = sqlx::query_scalar("SELECT result FROM operations WHERE id=$1")
        .bind(cancel.uuid())
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(result["cancelled"], true, "{result}");
    assert_eq!(fleet.fakes[1].total_commands().await, 1);
    assert_eq!(fleet.fakes[0].total_commands().await, 1);
    assert_eq!(fleet.fakes[0].total_starts().await, 2);
    assert_eq!(fleet.fakes[1].total_starts().await, 2);
}

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn restarted_host_denies_old_epoch_api_reads_while_other_host_progresses(pool: PgPool) {
    let mut fleet = Fleet::new(&pool, true).await;
    let mut controller = fleet.controller(0).await;
    let sandboxes = fleet.fill(&mut controller).await;
    let (project, on_b) = sandboxes[1];
    let upload = fleet.upload(project, on_b, b"old-epoch").await;
    assert_eq!(fleet.drive(&mut controller, upload).await, "succeeded");
    let command = fleet.execute(project, on_b).await;
    assert_eq!(fleet.drive(&mut controller, command).await, "succeeded");
    let (status, body) = fleet.capture(project, on_b).await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    let (status, text) = fleet.stream(project, command).await;
    assert!(!denied(status, &text), "{text}");
    // Operator restart of B: drain, issue epoch 2, start a fresh supervisor
    // process that has none of epoch 1's evidence.
    let host = fleet.hosts[1];
    fleet.store.drain_host(host, 1).await.unwrap();
    fleet.store.advance_host_epoch(host, 1).await.unwrap();
    let restarted = fake(host, 2).await;
    let supervisor = serve(&fleet.pki, &restarted, host).await;
    register(&pool, host, &supervisor.endpoint, &supervisor.pin).await;
    fleet.supervisors[1] = supervisor;
    fleet.fakes[1] = restarted;
    // Reads scoped to the epoch-1 allocation are never served by epoch 2.
    let (status, body) = fleet.capture(project, on_b).await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{body}");
    assert!(body.get("capture").is_none(), "{body}");
    let (status, text) = fleet.stream(project, command).await;
    assert_eq!(status, StatusCode::GONE, "{text}");
    assert!(text.contains("output_missing"), "{text}");
    // A progresses; B's new incarnation starts and runs nothing it never received.
    let (project_a, on_a) = sandboxes[0];
    let healthy = fleet.execute(project_a, on_a).await;
    assert_eq!(fleet.drive(&mut controller, healthy).await, "succeeded");
    assert_eq!(fleet.fakes[1].total_starts().await, 0);
    assert_eq!(fleet.fakes[1].total_commands().await, 0);
    // Without #178 checkpoint recovery the epoch-1 allocation keeps its
    // reservation: no release is inferred from the new incarnation.
    assert_eq!(fleet.unreleased(on_b).await, 1);
}

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn lost_stop_on_partitioned_host_keeps_capacity_while_other_host_places(pool: PgPool) {
    let fleet = Fleet::new(&pool, true).await;
    let mut controller = fleet.controller(0).await;
    let sandboxes = fleet.fill(&mut controller).await;
    // Destroy on B applies but its reply is lost; then B is partitioned.
    let (project_b, on_b) = sandboxes[1];
    fleet.fakes[1].lose_next_stop_reply().await;
    let destroy_b = fleet.destroy(project_b, on_b).await;
    let mut unknown = false;
    for _ in 0..8 {
        if controller.tick().await.ok() == Some(Tick::Unknown) {
            unknown = true;
            break;
        }
        fleet.retry_now(destroy_b).await;
    }
    assert!(unknown);
    fleet.partition(1).await;
    assert_eq!(fleet.unreleased(on_b).await, 1);
    // A frees a slot through confirmed cleanup; new work must use A only.
    let (project_a, on_a) = sandboxes[0];
    let destroy_a = fleet.destroy(project_a, on_a).await;
    assert_eq!(fleet.drive(&mut controller, destroy_a).await, "succeeded");
    let (create, placed) = fleet.create(1).await;
    assert_eq!(fleet.drive(&mut controller, create).await, "succeeded");
    assert_eq!(fleet.owner(placed).await, Some(fleet.hosts[0]));
    // Both hosts are now full: B's uncertain stop still holds its slot.
    let (waiting, queued) = fleet.create(1).await;
    for _ in 0..4 {
        fleet.retry_now(waiting).await;
        fleet.retry_now(destroy_b).await;
        let _ = controller.tick().await;
    }
    assert_ne!(fleet.status(waiting).await, "succeeded");
    assert_eq!(fleet.owner(queued).await, None);
    assert_eq!(fleet.unreleased(on_b).await, 1);
    let healthy = fleet.execute(1, placed).await;
    assert_eq!(fleet.drive(&mut controller, healthy).await, "succeeded");
    // B returns; inspection confirms the stop and only then frees the slot.
    fleet.heal(1).await;
    tokio::time::sleep(Duration::from_millis(600)).await;
    let mut replacement = fleet.controller(0).await;
    assert_eq!(fleet.drive(&mut replacement, destroy_b).await, "succeeded");
    assert_eq!(fleet.unreleased(on_b).await, 0);
    assert_eq!(fleet.drive(&mut replacement, waiting).await, "succeeded");
    assert_eq!(fleet.owner(queued).await, Some(fleet.hosts[1]));
    assert_eq!(fleet.fakes[1].total_starts().await, 3);
}

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn exhausted_dedicated_pool_waits_without_shared_fallback(pool: PgPool) {
    let fleet = Fleet::new(&pool, false).await;
    let dedicated = Uuid::now_v7();
    fleet
        .store
        .put_host_pool(&HostPoolSpec {
            version: 1,
            id: dedicated,
            name: "tenant-dedicated".into(),
            pool_type: PoolType::Standalone,
            min_ready_hosts: 0,
            max_hosts: 4,
            placement_policy: PlacementPolicy {
                version: 1,
                tenancy: Tenancy::Dedicated,
            },
            provider_config: json!({"version":1}),
            credential_secret_ref: None,
        })
        .await
        .unwrap();
    sqlx::query("UPDATE hosts SET host_pool_id=$2 WHERE id=$1")
        .bind(fleet.hosts[1].uuid())
        .bind(dedicated)
        .execute(&pool)
        .await
        .unwrap();
    let shared: Uuid = sqlx::query_scalar("SELECT host_pool_id FROM hosts WHERE id=$1")
        .bind(fleet.hosts[0].uuid())
        .fetch_one(&pool)
        .await
        .unwrap();
    fleet
        .store
        .grant_host_pool_access(fleet.projects[0].0, dedicated)
        .await
        .unwrap();
    fleet
        .store
        .grant_host_pool_access(fleet.projects[1].0, shared)
        .await
        .unwrap();
    let mut controller = fleet.controller(0).await;
    let mut tenant = Vec::new();
    for _ in 0..2 {
        let (create, sandbox) = fleet.create(0).await;
        assert_eq!(fleet.drive(&mut controller, create).await, "succeeded");
        assert_eq!(fleet.owner(sandbox).await, Some(fleet.hosts[1]));
        tenant.push(sandbox);
    }
    // The dedicated host is full and the shared host is idle: no fallback.
    let (waiting, queued) = fleet.create(0).await;
    for _ in 0..4 {
        fleet.retry_now(waiting).await;
        let _ = controller.tick().await;
    }
    assert!(!matches!(
        fleet.status(waiting).await.as_str(),
        "succeeded" | "failed"
    ));
    assert_eq!(fleet.owner(queued).await, None);
    // The shared-pool project still places on A meanwhile.
    let (create, shared_sandbox) = fleet.create(1).await;
    assert_eq!(fleet.drive(&mut controller, create).await, "succeeded");
    assert_eq!(fleet.owner(shared_sandbox).await, Some(fleet.hosts[0]));
    // Freeing dedicated capacity is what lets the tenant's create proceed.
    let destroy = fleet.destroy(0, tenant[0]).await;
    assert_eq!(fleet.drive(&mut controller, destroy).await, "succeeded");
    assert_eq!(fleet.drive(&mut controller, waiting).await, "succeeded");
    assert_eq!(fleet.owner(queued).await, Some(fleet.hosts[1]));
    let on_shared: i64 =
        sqlx::query_scalar("SELECT count(*) FROM allocations WHERE project_id=$1 AND host_id=$2")
            .bind(fleet.projects[0].0.uuid())
            .bind(fleet.hosts[0].uuid())
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(on_shared, 0);
}

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn drained_host_takes_no_new_placement_but_keeps_routed_cleanup(pool: PgPool) {
    let fleet = Fleet::new(&pool, true).await;
    let mut controller = fleet.controller(0).await;
    let (create, on_a) = fleet.create(0).await;
    assert_eq!(fleet.drive(&mut controller, create).await, "succeeded");
    assert_eq!(fleet.owner(on_a).await, Some(fleet.hosts[0]));
    fleet.store.drain_host(fleet.hosts[0], 1).await.unwrap();
    // A has room for one more sandbox, yet both new creates target B only.
    let mut on_b = Vec::new();
    for project in [0, 1] {
        let (create, sandbox) = fleet.create(project).await;
        assert_eq!(fleet.drive(&mut controller, create).await, "succeeded");
        assert_eq!(fleet.owner(sandbox).await, Some(fleet.hosts[1]));
        on_b.push(sandbox);
    }
    let (waiting, queued) = fleet.create(1).await;
    for _ in 0..4 {
        fleet.retry_now(waiting).await;
        let _ = controller.tick().await;
    }
    assert!(!matches!(
        fleet.status(waiting).await.as_str(),
        "succeeded" | "failed"
    ));
    assert_eq!(fleet.owner(queued).await, None);
    // Cleanup on the draining host is still routed to and confirmed by A.
    let destroy = fleet.destroy(0, on_a).await;
    assert_eq!(fleet.drive(&mut controller, destroy).await, "succeeded");
    assert_eq!(fleet.unreleased(on_a).await, 0);
    // An empty draining host still accepts no placement.
    for _ in 0..3 {
        fleet.retry_now(waiting).await;
        let _ = controller.tick().await;
    }
    assert_eq!(fleet.owner(queued).await, None);
    assert_eq!(fleet.fakes[0].total_starts().await, 1);
    assert_eq!(fleet.fakes[1].total_starts().await, 2);
    let destroy = fleet.destroy(1, on_b[1]).await;
    assert_eq!(fleet.drive(&mut controller, destroy).await, "succeeded");
    assert_eq!(fleet.drive(&mut controller, waiting).await, "succeeded");
    assert_eq!(fleet.owner(queued).await, Some(fleet.hosts[1]));
}

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn retire_replace_at_pool_max_routes_new_identity_without_inheriting_history(pool: PgPool) {
    let fleet = Fleet::new(&pool, true).await;
    let old = fleet.hosts[0];
    let pool_id: Uuid = sqlx::query_scalar("SELECT host_pool_id FROM hosts WHERE id=$1")
        .bind(old.uuid())
        .fetch_one(&pool)
        .await
        .unwrap();
    let spec = HostPoolSpec {
        version: 1,
        id: pool_id,
        name: "qualification-replacement".into(),
        pool_type: PoolType::Standalone,
        min_ready_hosts: 0,
        max_hosts: 2,
        placement_policy: PlacementPolicy {
            version: 1,
            tenancy: Tenancy::Shared,
        },
        provider_config: json!({"version": 1}),
        credential_secret_ref: None,
    };
    fleet.store.put_host_pool(&spec).await.unwrap();
    let router = ProviderRouter::default().with(Arc::new(StandaloneStaticProvider));
    // Keep this controller across replacement, including its connection to A.
    let mut controller = fleet.controller(0).await;
    let sandboxes = fleet.fill(&mut controller).await;
    for &(project, sandbox) in [&sandboxes[0], &sandboxes[2]] {
        let command = fleet.execute(project, sandbox).await;
        assert_eq!(fleet.drive(&mut controller, command).await, "succeeded");
    }
    let drain = fleet.capacity_claim(pool_id, old, Action::Drain).await;
    router.apply(&fleet.store, &drain).await.unwrap();
    let retire = fleet.capacity_claim(pool_id, old, Action::Retire).await;
    let occupied_history = fleet.host_history(old).await;
    assert!(matches!(
        router.apply(&fleet.store, &retire).await,
        Err(CapacityError::Conflict)
    ));
    assert_eq!(fleet.host_history(old).await, occupied_history);

    // The pool is at its worker limit, independently of free sandbox slots.
    let new = HostId::generate();
    let provision = fleet
        .capacity_claim(
            pool_id,
            new,
            Action::Provision {
                config: Box::new(HostProvision {
                    version: 1,
                    capacity: HostCapacity {
                        vcpu: 4,
                        memory_mib: 8192,
                        disk_mib: 65536,
                    },
                    capabilities: capabilities(),
                    host_pool_id: Some(pool_id),
                }),
            },
        )
        .await;
    assert!(router.apply(&fleet.store, &provision).await.is_err());
    assert!(fleet.store.registered_host(new).await.unwrap().is_none());

    // One A allocation cleans up normally. The other's stop applies, but the
    // lost reply and partition cannot authorize release or retirement.
    let destroy = fleet.destroy(sandboxes[2].0, sandboxes[2].1).await;
    assert_eq!(fleet.drive(&mut controller, destroy).await, "succeeded");
    fleet.fakes[0].lose_next_stop_reply().await;
    let destroy = fleet.destroy(sandboxes[0].0, sandboxes[0].1).await;
    let mut unknown = false;
    for _ in 0..8 {
        if controller.tick().await.ok() == Some(Tick::Unknown) {
            unknown = true;
            break;
        }
        fleet.retry_now(destroy).await;
    }
    assert!(unknown);
    fleet.partition(0).await;
    let (waiting, queued) = fleet.create(0).await;
    for _ in 0..4 {
        fleet.retry_now(destroy).await;
        fleet.retry_now(waiting).await;
        let _ = controller.tick().await;
    }
    assert_eq!(fleet.status(destroy).await, "unknown");
    assert_eq!(fleet.unreleased(sandboxes[0].1).await, 1);
    assert_eq!(fleet.owner(queued).await, None);
    let uncertain_history = fleet.host_history(old).await;
    assert!(matches!(
        router.apply(&fleet.store, &retire).await,
        Err(CapacityError::Conflict)
    ));
    assert_eq!(fleet.host_history(old).await, uncertain_history);
    assert!(
        fleet
            .store
            .capacity_operation(retire.id)
            .await
            .unwrap()
            .unwrap()["provider_receipt"]
            .is_null()
    );
    assert!(router.apply(&fleet.store, &provision).await.is_err());

    fleet.heal(0).await;
    assert_eq!(fleet.drive(&mut controller, destroy).await, "succeeded");
    for &(_, sandbox) in [&sandboxes[0], &sandboxes[2]] {
        assert_eq!(fleet.unreleased(sandbox).await, 0);
    }
    // Empty A is still drained; B is full. Cleanup alone cannot place work.
    for _ in 0..3 {
        fleet.retry_now(waiting).await;
        let _ = controller.tick().await;
    }
    assert_eq!(fleet.owner(queued).await, None);
    let old_history = fleet.host_history(old).await;
    let receipt = router.apply(&fleet.store, &retire).await.unwrap();
    assert_eq!(receipt["retired"], true);
    assert_eq!(receipt["report"]["outstanding_allocations"], 0);
    assert_eq!(
        fleet
            .store
            .host_pool(pool_id)
            .await
            .unwrap()
            .unwrap()
            .enrolled_hosts,
        1
    );
    router.apply(&fleet.store, &provision).await.unwrap();
    assert_eq!(
        fleet
            .store
            .host_pool(pool_id)
            .await
            .unwrap()
            .unwrap()
            .enrolled_hosts,
        2
    );
    // A new host starts with no allocations, execution pins or dispatch receipts.
    let empty_history = fleet.host_history(new).await;
    assert!(empty_history["allocations"].is_null());
    assert!(empty_history["operations"].is_null());
    let replacement = fake(new, 1).await;
    let supervisor = serve(&fleet.pki, &replacement, new).await;
    register(&pool, new, &supervisor.endpoint, &supervisor.pin).await;
    fleet
        .store
        .observe_configured_host_with_capabilities(new, 1, &capabilities().to_json().unwrap(), true)
        .await
        .unwrap();
    fleet.store.finish_host_drain(new, 1).await.unwrap();

    // Even a late authenticated observation cannot reopen the old tombstone.
    fleet
        .store
        .observe_configured_host_with_capabilities(old, 1, &capabilities().to_json().unwrap(), true)
        .await
        .unwrap();
    assert!(fleet.store.finish_host_drain(old, 1).await.is_err());
    assert!(fleet.store.advance_host_epoch(old, 1).await.is_err());
    let reopen = fleet
        .capacity_claim(pool_id, old, provision.request.action.clone())
        .await;
    assert!(matches!(
        router.apply(&fleet.store, &reopen).await,
        Err(CapacityError::Conflict)
    ));
    let history = fleet
        .store
        .registered_host_history(Some(pool_id), None, 10)
        .await
        .unwrap();
    assert_eq!(history.len(), 3);
    let tombstone = history.iter().find(|h| h.host_id == old).unwrap();
    assert!(tombstone.capacity_retired);
    assert!(!tombstone.admits_new_work());
    assert_eq!(
        tombstone.transport.as_ref().unwrap().endpoint,
        fleet.supervisors[0].endpoint
    );
    let active = fleet.store.registered_hosts(Some(pool_id)).await.unwrap();
    assert_eq!(active.len(), 2);
    assert!(active.iter().all(|h| h.host_id != old));
    assert!(active.iter().any(|h| h.host_id == new));

    assert_eq!(fleet.drive(&mut controller, waiting).await, "succeeded");
    assert_eq!(fleet.owner(queued).await, Some(new));
    let (create, second) = fleet.create(1).await;
    assert_eq!(fleet.drive(&mut controller, create).await, "succeeded");
    assert_eq!(fleet.owner(second).await, Some(new));
    for (project, sandbox) in [(0, queued), (1, second)] {
        let command = fleet.execute(project, sandbox).await;
        assert_eq!(fleet.drive(&mut controller, command).await, "succeeded");
        let dispatch: (Uuid, i64, i32) = sqlx::query_as(
            "SELECT a.host_id,a.supervisor_epoch,o.attempt_count FROM operations o
             JOIN allocations a ON a.id=o.execution_allocation_id WHERE o.id=$1",
        )
        .bind(command.uuid())
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(dispatch, (new.uuid(), 1, 1));
        let upload = fleet.upload(project, sandbox, b"replacement-only").await;
        assert_eq!(fleet.drive(&mut controller, upload).await, "succeeded");
        let (status, body) = fleet.capture(project, sandbox).await;
        assert_eq!(status, StatusCode::CREATED, "{body}");
        assert_eq!(body["size"], 16);
        let (status, text) = fleet.stream(project, command).await;
        assert_eq!(status, StatusCode::OK, "{text}");
        assert!(!denied(status, &text), "{text}");
    }
    assert_eq!(replacement.total_starts().await, 2);
    assert_eq!(replacement.total_commands().await, 2);
    assert_eq!(replacement.total_file_commits().await, 2);
    assert_eq!(fleet.fakes[0].total_starts().await, 2);
    assert_eq!(fleet.fakes[0].total_commands().await, 2);
    assert_eq!(fleet.fakes[0].total_file_commits().await, 0);
    assert_eq!(fleet.fakes[1].total_starts().await, 2);
    assert_eq!(fleet.fakes[1].total_commands().await, 0);
    assert_eq!(fleet.host_history(old).await, old_history);
}

/// Tick until `operation` is durably unknown (its reply was lost).
async fn until_unknown(fleet: &Fleet, controller: &mut Controller, operation: OperationId) {
    for _ in 0..8 {
        let _ = controller.tick().await;
        if fleet.status(operation).await == "unknown" {
            return;
        }
        fleet.retry_now(operation).await;
    }
    panic!("operation {operation} never became unknown");
}

/// Supervisors never push heartbeats: the controller dials the registered
/// endpoint, verifies the pinned leaf certificate, and only then records an
/// observation at the host ID and epoch the supervisor reports. A "heartbeat"
/// from a duplicate process is therefore either a health reply to the
/// controller or a direct store observation; both are exercised here.
#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn duplicate_supervisor_same_host_id_is_fenced(pool: PgPool) {
    let fleet = Fleet::new(&pool, true).await;
    let host = fleet.hosts[1];
    // (a) Operator restart of B issues epoch 2, but the epoch-1 process keeps
    // running and answering at the registered endpoint with its own key.
    fleet.store.drain_host(host, 1).await.unwrap();
    fleet.store.advance_host_epoch(host, 1).await.unwrap();
    let old = &fleet.fakes[1];
    let mut controller = fleet.controller(0).await;
    // Only A admits work: fill it so later placement can only target B.
    for project in [0, 1] {
        let (create, sandbox) = fleet.create(project).await;
        assert_eq!(fleet.drive(&mut controller, create).await, "succeeded");
        assert_eq!(fleet.owner(sandbox).await, Some(fleet.hosts[0]));
    }
    // The controller's checks reached the old process, which reports epoch 1:
    // identity is refused, so B has no fresh observation at epoch 2.
    assert!(!fleet.last_seen(host).await);
    // Old-epoch heartbeats cannot refresh the host, with or without capabilities.
    assert!(matches!(
        fleet.store.observe_configured_host(host, 1).await,
        Err(DispatchError::HostUnavailable)
    ));
    assert!(matches!(
        fleet
            .store
            .observe_configured_host_with_capabilities(
                host,
                1,
                &capabilities().to_json().unwrap(),
                true
            )
            .await,
        Err(DispatchError::HostUnavailable)
    ));
    assert!(!fleet.last_seen(host).await);
    assert!(fleet.store.finish_host_drain(host, 2).await.is_err());

    // The fresh epoch-2 process is registered, observed and reopened.
    let fresh = fake(host, 2).await;
    let current = serve(&fleet.pki, &fresh, host).await;
    register(&pool, host, &current.endpoint, &current.pin).await;
    let mut controller = fleet.controller(0).await;
    for _ in 0..3 {
        let _ = controller.tick().await;
    }
    assert!(fleet.last_seen(host).await);
    fleet.store.finish_host_drain(host, 2).await.unwrap();
    // The old process keeps heartbeating; it is still refused.
    assert!(matches!(
        fleet.store.observe_configured_host(host, 1).await,
        Err(DispatchError::HostUnavailable)
    ));
    let epoch: i64 = sqlx::query_scalar("SELECT supervisor_epoch FROM hosts WHERE id=$1")
        .bind(host.uuid())
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(epoch, 2);

    // The old process regains the address (stale registration with its own
    // pin). Health reports epoch 1 against registered epoch 2 and is refused.
    register(
        &pool,
        host,
        &fleet.supervisors[1].endpoint,
        &fleet.supervisors[1].pin,
    )
    .await;
    tokio::time::sleep(Duration::from_millis(600)).await;
    let mut controller = fleet.controller(0).await;
    let (waiting, queued) = fleet.create(0).await;
    for _ in 0..4 {
        fleet.retry_now(waiting).await;
        let _ = controller.tick().await;
    }
    assert_ne!(fleet.status(waiting).await, "succeeded");
    assert!(!fleet.last_seen(host).await);

    // (b) A second process at the same host ID and the current epoch, with a
    // CA-valid certificate for the same server name but its own key, takes
    // over the address. The registered leaf pin refuses it before any RPC.
    let twin = fake(host, 2).await;
    let twin_supervisor = serve(&fleet.pki, &twin, host).await;
    assert_ne!(twin_supervisor.pin, current.pin);
    register(&pool, host, &twin_supervisor.endpoint, &current.pin).await;
    tokio::time::sleep(Duration::from_millis(600)).await;
    let mut controller = fleet.controller(0).await;
    for _ in 0..4 {
        fleet.retry_now(waiting).await;
        let _ = controller.tick().await;
    }
    assert_ne!(fleet.status(waiting).await, "succeeded");
    assert!(!fleet.last_seen(host).await);

    // Neither duplicate received any work; nothing was placed at epoch 1.
    for duplicate in [old, &twin] {
        assert_eq!(duplicate.total_starts().await, 0);
        assert_eq!(duplicate.total_commands().await, 0);
    }
    assert_eq!(fresh.total_starts().await, 0);
    let old_epoch: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM allocations WHERE host_id=$1 AND supervisor_epoch<>2",
    )
    .bind(host.uuid())
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(old_epoch, 0);
    fleet.assert_single_live_owner().await;

    // Restoring the genuine registration routes the queued create to the
    // epoch-2 process only.
    register(&pool, host, &current.endpoint, &current.pin).await;
    tokio::time::sleep(Duration::from_millis(600)).await;
    let mut controller = fleet.controller(0).await;
    assert_eq!(fleet.drive(&mut controller, waiting).await, "succeeded");
    assert_eq!(fleet.owner(queued).await, Some(host));
    let owner_epoch: i64 = sqlx::query_scalar(
        "SELECT a.supervisor_epoch FROM sandboxes s JOIN allocations a ON a.id=s.current_allocation_id WHERE s.id=$1",
    )
    .bind(queued.uuid())
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(owner_epoch, 2);
    let command = fleet.execute(0, queued).await;
    assert_eq!(fleet.drive(&mut controller, command).await, "succeeded");
    assert_eq!(fresh.total_starts().await, 1);
    assert_eq!(fresh.total_commands().await, 1);
    for duplicate in [old, &twin] {
        assert_eq!(duplicate.total_starts().await, 0);
        assert_eq!(duplicate.total_commands().await, 0);
    }
    fleet.assert_single_live_owner().await;
}

/// A process at the same host ID and epoch holding a copy of the registered
/// certificate and key is indistinguishable at the transport and health
/// layers. The safe behaviour is that it cannot supply release evidence for
/// an allocation it never ran while the genuine process still holds it.
///
/// Observed (CI run 41219994173): the clone answers the stop with
/// `FencedAbsent` from empty memory and the allocation is released. Same-epoch
/// uniqueness of a supervisor process is an unenforced deployment assumption.
#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
#[ignore = "gap: a same-host, same-epoch clone holding the registered key can release capacity via FencedAbsent"]
async fn same_epoch_clone_with_copied_key_cannot_release_capacity(pool: PgPool) {
    let fleet = Fleet::new(&pool, true).await;
    let mut controller = fleet.controller(0).await;
    let sandboxes = fleet.fill(&mut controller).await;
    let (project, on_b) = sandboxes[1];
    let host = fleet.hosts[1];
    let genuine = &fleet.supervisors[1];
    let clone = fake(host, 1).await;
    let (cert, key) = genuine.identity.clone();
    let cloned = serve_with(&fleet.pki, &clone, (cert, key, genuine.pin.clone())).await;
    assert_eq!(cloned.pin, genuine.pin);
    // The clone takes over B's address; the registration is otherwise unchanged.
    register(&pool, host, &cloned.endpoint, &genuine.pin).await;
    let destroy = fleet.destroy(project, on_b).await;
    tokio::time::sleep(Duration::from_millis(600)).await;
    let mut controller = fleet.controller(0).await;
    for _ in 0..6 {
        fleet.retry_now(destroy).await;
        let _ = controller.tick().await;
    }
    // The genuine B still runs the sandbox; its slot must stay reserved.
    assert_eq!(fleet.unreleased(on_b).await, 1);
    assert_ne!(fleet.status(destroy).await, "succeeded");
    assert_eq!(clone.total_starts().await, 0);
}

/// Partition, stale heartbeat, controller restart and lost replies across
/// both hosts at once. Ownership is checked after every fault.
#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn combined_faults_preserve_single_live_owner(pool: PgPool) {
    let fleet = Fleet::new(&pool, true).await;
    let mut first = fleet.controller(0).await;
    // A owns sandboxes 0 and 2; B owns 1 and 3.
    let sandboxes = fleet.fill(&mut first).await;
    fleet.assert_single_live_owner().await;
    let (a, b) = (fleet.hosts[0], fleet.hosts[1]);

    // Lost reply on A: the stop applies but the controller never hears it.
    fleet.fakes[0].lose_next_stop_reply().await;
    let lost_stop = fleet.destroy(sandboxes[0].0, sandboxes[0].1).await;
    until_unknown(&fleet, &mut first, lost_stop).await;
    assert_eq!(fleet.unreleased(sandboxes[0].1).await, 1);
    fleet.assert_single_live_owner().await;

    // Partition B, request a destroy there, then let B's heartbeat go stale.
    fleet.partition(1).await;
    let stranded = fleet.destroy(sandboxes[1].0, sandboxes[1].1).await;
    sqlx::query(
        "UPDATE hosts SET last_seen_at=clock_timestamp()-interval '31 seconds' WHERE id=$1",
    )
    .bind(b.uuid())
    .execute(&pool)
    .await
    .unwrap();
    fleet.store.reconcile_unavailable_hosts().await.unwrap();
    // A heartbeat at any epoch other than B's current one cannot refresh it.
    assert!(fleet.store.observe_configured_host(b, 2).await.is_err());
    for _ in 0..3 {
        fleet.retry_now(stranded).await;
        let _ = first.tick().await;
    }
    assert_ne!(fleet.status(stranded).await, "succeeded");
    for &(_, sandbox) in [&sandboxes[1], &sandboxes[3]] {
        assert_eq!(fleet.unreleased(sandbox).await, 1);
    }
    fleet.assert_single_live_owner().await;

    // A frees one slot through confirmed cleanup; a create there loses its reply.
    let confirmed = fleet.destroy(sandboxes[2].0, sandboxes[2].1).await;
    assert_eq!(fleet.drive(&mut first, confirmed).await, "succeeded");
    assert_eq!(fleet.unreleased(sandboxes[2].1).await, 0);
    fleet.fakes[0].lose_next_create_reply().await;
    let (create, placed) = fleet.create(1).await;
    until_unknown(&fleet, &mut first, create).await;
    assert_eq!(fleet.owner(placed).await, Some(a));
    assert_eq!(fleet.unreleased(placed).await, 1);
    fleet.assert_single_live_owner().await;

    // Controller restart: the replacement has no in-memory state and resolves
    // both uncertain A operations by inspection, without replaying a start.
    drop(first);
    let mut second = fleet.controller(0).await;
    assert_eq!(fleet.drive(&mut second, create).await, "succeeded");
    assert_eq!(fleet.owner(placed).await, Some(a));
    assert_eq!(fleet.drive(&mut second, lost_stop).await, "succeeded");
    assert_eq!(fleet.unreleased(sandboxes[0].1).await, 0);
    assert_eq!(fleet.fakes[0].total_starts().await, 3);
    fleet.assert_single_live_owner().await;

    // B is still partitioned: the restarted controller cannot finish its
    // destroy, and no capacity there is refunded from absence of contact.
    for _ in 0..3 {
        fleet.retry_now(stranded).await;
        let _ = second.tick().await;
    }
    assert_ne!(fleet.status(stranded).await, "succeeded");
    for &(_, sandbox) in [&sandboxes[1], &sandboxes[3]] {
        assert_eq!(fleet.unreleased(sandbox).await, 1);
    }
    fleet.assert_single_live_owner().await;

    // B heals; another restarted controller confirms the stop and only then
    // releases. Nothing was ever started twice on either host.
    fleet.heal(1).await;
    tokio::time::sleep(Duration::from_millis(600)).await;
    drop(second);
    let mut third = fleet.controller(0).await;
    assert_eq!(fleet.drive(&mut third, stranded).await, "succeeded");
    assert_eq!(fleet.unreleased(sandboxes[1].1).await, 0);
    assert_eq!(fleet.unreleased(sandboxes[3].1).await, 1);
    assert_eq!(fleet.owner(sandboxes[3].1).await, Some(b));
    assert_eq!(fleet.fakes[0].total_starts().await, 3);
    assert_eq!(fleet.fakes[1].total_starts().await, 2);
    let live: Vec<(Uuid, i64)> = sqlx::query_as(
        "SELECT host_id,count(*) FROM allocations WHERE released_at IS NULL GROUP BY host_id ORDER BY host_id",
    )
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(live, vec![(a.uuid(), 1), (b.uuid(), 1)]);
    fleet.assert_single_live_owner().await;
}
