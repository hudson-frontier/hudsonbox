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
use sandbox_controller::{Controller, ControllerConfig, Tick};
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
    task: JoinHandle<()>,
}

impl Drop for Supervisor {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn serve(pki: &Pki, fake: &FakeHost, identity: HostId) -> Supervisor {
    let (cert, key, pin) = pki.server(identity);
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

    async fn unreleased(&self, sandbox: SandboxId) -> i64 {
        sqlx::query_scalar(
            "SELECT count(*) FROM allocations WHERE sandbox_id=$1 AND released_at IS NULL",
        )
        .bind(sandbox.uuid())
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
