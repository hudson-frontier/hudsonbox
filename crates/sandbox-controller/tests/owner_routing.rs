//! Two simulated supervisors with distinct mTLS identities behind the host
//! registry: PostgreSQL + the HTTP router + loopback gRPC/mTLS. No VM is used.
#![allow(clippy::unwrap_used, clippy::expect_used)]
use axum::{Router, body::Body};
use http::{Request, StatusCode};
use rcgen::{
    BasicConstraints, CertificateParams, CertifiedIssuer, ExtendedKeyUsagePurpose, IsCa, KeyPair,
    KeyUsagePurpose,
};
use sandbox_api::{AppState, files::client::FileClient, router_with_files};
use sandbox_controller::{Controller, ControllerConfig, HEALTH_DEADLINE, Tick};
use sandbox_fake_host::{FakeConfig, FakeHost};
use sandbox_protocol::{
    HostId, Id, OperationId, ProjectId, ProjectToken, SandboxId,
    supervisor::{file_downloads_server::FileDownloadsServer, supervisor_server::SupervisorServer},
};
use sandbox_store::{Store, host_registry::HostTransport};
use sandbox_supervisor::transport::{self, ControllerIdentity, MAX_MESSAGE_BYTES};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use sqlx::PgPool;
use std::{
    collections::BTreeSet,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::{net::TcpListener, task::JoinHandle};
use tokio_stream::wrappers::TcpListenerStream;
use tonic::{service::interceptor::InterceptedService, transport::Server};
use tower::ServiceExt;

fn digest() -> String {
    format!("sha256:{}", "a".repeat(64))
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

    /// A CA-valid server certificate for `host`'s per-host DNS name.
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

/// One listening supervisor process: a fake host's state behind one certificate.
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
    let tls = transport::server_tls(pki.ca.pem().as_bytes(), cert.as_bytes(), key.as_bytes());
    let task = tokio::spawn(async move {
        let _ = Server::builder()
            .tls_config(tls)
            .unwrap()
            .add_service(control)
            .add_service(files)
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await;
    });
    Supervisor {
        endpoint,
        pin,
        task,
    }
}

fn fake(host: HostId) -> FakeHost {
    FakeHost::new(FakeConfig {
        host,
        epoch: 1,
        images: BTreeSet::from([digest()]),
        capacity: sandbox_protocol::supervisor::Resources {
            vcpu: 4,
            memory_mib: 8192,
            disk_mib: 65536,
        },
    })
    .unwrap()
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

struct Fleet {
    pool: PgPool,
    store: Store,
    pki: Pki,
    app: Router,
    token: String,
    hosts: [HostId; 2],
    fakes: [FakeHost; 2],
    supervisors: Vec<Supervisor>,
    /// Sandboxes placed on host A and host B respectively.
    sandboxes: [SandboxId; 2],
}

async fn project(pool: &PgPool) -> String {
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
    token.render_once()
}

impl Fleet {
    async fn new(pool: &PgPool) -> Self {
        let token = project(pool).await;
        let pki = Pki::new();
        let hosts = [HostId::generate(), HostId::generate()];
        let fakes = [fake(hosts[0]), fake(hosts[1])];
        let mut supervisors = Vec::new();
        for (host, fake) in hosts.iter().zip(&fakes) {
            sqlx::query("INSERT INTO hosts(id,status,cpu_capacity,memory_capacity_mib,disk_capacity_mib,supervisor_epoch) VALUES($1,'ready',4,8192,65536,1)")
                .bind(host.uuid()).execute(pool).await.unwrap();
            let supervisor = serve(&pki, fake, *host).await;
            register(pool, *host, &supervisor.endpoint, &supervisor.pin).await;
            supervisors.push(supervisor);
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
        let app = router_with_files(
            AppState {
                images: sandbox_protocol::images::ImageAllowlist::new([digest()]).unwrap(),
                store: store.clone(),
            },
            None,
            None,
            Some(Arc::new(files)),
        );
        let mut fleet = Self {
            pool: pool.clone(),
            store,
            pki,
            app,
            token,
            hosts,
            fakes,
            supervisors,
            sandboxes: [SandboxId::generate(), SandboxId::generate()],
        };
        // Placement still targets each controller's configured host (#194 owns
        // multi-host placement); one sandbox is placed on each host.
        for index in 0..2 {
            let (_, body) = fleet
                .send(
                    "POST",
                    "/v1/sandboxes",
                    json!({"image_digest":digest(),"resources":{"vcpu":2,"memory_mib":2048,"disk_mib":8192}}),
                )
                .await;
            fleet.sandboxes[index] = body["sandbox_id"].as_str().unwrap().parse().unwrap();
            let mut placing = fleet.controller(index).await;
            assert_eq!(placing.tick().await.unwrap(), Tick::Confirmed);
        }
        let owners: Vec<(uuid::Uuid,)> = sqlx::query_as(
            "SELECT a.host_id FROM sandboxes s JOIN allocations a ON a.id=s.current_allocation_id ORDER BY s.created_at",
        )
        .fetch_all(pool)
        .await
        .unwrap();
        assert_eq!(
            owners,
            vec![(fleet.hosts[0].uuid(),), (fleet.hosts[1].uuid(),)]
        );
        fleet
    }

    fn config(&self, index: usize) -> ControllerConfig {
        ControllerConfig {
            // Ignored: a registered transport always wins over configuration.
            endpoint: "https://127.0.0.1:9".into(),
            host: self.hosts[index],
            epoch: 1,
            allowed_images: BTreeSet::from([digest()]),
            allow_simulated: true,
        }
    }

    async fn controller(&self, index: usize) -> Controller {
        Controller::connect(
            self.store.clone(),
            self.config(index),
            self.pki.ca.pem().as_bytes(),
            self.pki.controller.0.as_bytes(),
            self.pki.controller.1.as_bytes(),
        )
        .await
        .unwrap()
    }

    async fn send_as(
        &self,
        token: &str,
        method: &str,
        path: &str,
        body: Value,
    ) -> (StatusCode, Value) {
        let request = Request::builder()
            .method(method)
            .uri(path)
            .header("authorization", format!("Bearer {token}"))
            .header("content-type", "application/json")
            .header("idempotency-key", uuid::Uuid::now_v7().to_string())
            .body(Body::from(body.to_string()))
            .unwrap();
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

    async fn send(&self, method: &str, path: &str, body: Value) -> (StatusCode, Value) {
        self.send_as(&self.token, method, path, body).await
    }

    async fn execute(&self, index: usize) -> OperationId {
        let (status, body) = self
            .send(
                "POST",
                &format!("/v1/sandboxes/{}/execute", self.sandboxes[index]),
                json!({"argv":["/bin/true"],"deadline_unix_ms":sandbox_fake_host::unix_ms().unwrap()+60_000}),
            )
            .await;
        assert_eq!(status, StatusCode::ACCEPTED, "{body}");
        body["operation_id"].as_str().unwrap().parse().unwrap()
    }

    async fn status(&self, operation: OperationId) -> String {
        sqlx::query_scalar("SELECT status FROM operations WHERE id=$1")
            .bind(operation.uuid())
            .fetch_one(&self.pool)
            .await
            .unwrap()
    }

    /// Make one deferred operation immediately claimable again. Other deferred
    /// work keeps its backoff, as it would in a running controller.
    async fn retry_now(&self, operation: OperationId) {
        sqlx::query("UPDATE operations SET next_retry_at=NULL WHERE id=$1")
            .bind(operation.uuid())
            .execute(&self.pool)
            .await
            .unwrap();
    }

    async fn drive(&self, controller: &mut Controller, operation: OperationId) -> String {
        for _ in 0..12 {
            let _ = controller.tick().await;
            let status = self.status(operation).await;
            if matches!(status.as_str(), "succeeded" | "failed" | "cancelled") {
                return status;
            }
            self.retry_now(operation).await;
        }
        self.status(operation).await
    }

    async fn observed(&self, index: usize) -> String {
        sqlx::query_scalar("SELECT observed_state FROM sandboxes WHERE id=$1")
            .bind(self.sandboxes[index].uuid())
            .fetch_one(&self.pool)
            .await
            .unwrap()
    }
}

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn one_controller_routes_lifecycle_and_execution_to_each_persisted_owner(pool: PgPool) {
    let fleet = Fleet::new(&pool).await;
    // Configured for host A only; host B is reached solely through its registration.
    let mut controller = fleet.controller(0).await;
    let on_a = fleet.execute(0).await;
    let on_b = fleet.execute(1).await;
    assert_eq!(fleet.drive(&mut controller, on_a).await, "succeeded");
    assert_eq!(fleet.drive(&mut controller, on_b).await, "succeeded");
    assert_eq!(fleet.fakes[0].total_commands().await, 1);
    assert_eq!(fleet.fakes[1].total_commands().await, 1);
    // Health authenticated both hosts and refreshed their own observations.
    let fresh: Vec<(bool,)> =
        sqlx::query_as("SELECT last_seen_at IS NOT NULL FROM hosts ORDER BY id")
            .fetch_all(&pool)
            .await
            .unwrap();
    assert_eq!(fresh, vec![(true,), (true,)]);
    for index in [1, 0] {
        let (status, body) = fleet
            .send(
                "POST",
                &format!("/v1/sandboxes/{}/destroy", fleet.sandboxes[index]),
                json!({}),
            )
            .await;
        assert_eq!(status, StatusCode::ACCEPTED, "{body}");
        let destroy = body["operation_id"].as_str().unwrap().parse().unwrap();
        assert_eq!(fleet.drive(&mut controller, destroy).await, "succeeded");
    }
    let held: i64 =
        sqlx::query_scalar("SELECT count(*) FROM allocations WHERE released_at IS NULL")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(held, 0);
}

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn lost_acknowledgement_on_a_routed_host_keeps_unknown_then_inspects(pool: PgPool) {
    let fleet = Fleet::new(&pool).await;
    let mut controller = fleet.controller(0).await;
    fleet.fakes[1].lose_next_command_reply().await;
    let on_b = fleet.execute(1).await;
    assert_eq!(controller.tick().await.unwrap(), Tick::Unknown);
    assert_eq!(fleet.status(on_b).await, "unknown");
    assert_eq!(fleet.drive(&mut controller, on_b).await, "succeeded");
    // The retry inspected the same owner; nothing ran twice or on host A.
    assert_eq!(fleet.fakes[1].total_commands().await, 1);
    assert_eq!(fleet.fakes[0].total_commands().await, 0);
}

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn wrong_host_reply_is_rejected_and_other_hosts_continue(pool: PgPool) {
    let fleet = Fleet::new(&pool).await;
    let mut controller = fleet.controller(0).await;
    // A replica presenting host B's own certificate but answering as another host.
    let imposter = fake(HostId::generate());
    let replica = serve(&fleet.pki, &imposter, fleet.hosts[1]).await;
    register(&pool, fleet.hosts[1], &replica.endpoint, &replica.pin).await;
    let on_b = fleet.execute(1).await;
    let on_a = fleet.execute(0).await;
    assert_eq!(fleet.drive(&mut controller, on_a).await, "succeeded");
    for _ in 0..3 {
        let _ = controller.tick().await;
        fleet.retry_now(on_b).await;
    }
    assert_eq!(
        fleet.status(on_b).await,
        "running",
        "claimed and deferred, never dispatched"
    );
    assert_eq!(imposter.total_commands().await, 0);
    assert_eq!(fleet.fakes[1].total_commands().await, 0);
    assert_eq!(fleet.observed(1).await, "unknown");
    assert_eq!(fleet.observed(0).await, "running");
}

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn certificate_mismatch_is_refused_until_the_registration_matches(pool: PgPool) {
    let mut fleet = Fleet::new(&pool).await;
    let mut controller = fleet.controller(0).await;
    // A server with host A's identity, same CA, at host B's registration:
    // wrong name and pin. (Registered endpoints are unique, so it listens anew.)
    let a = serve(&fleet.pki, &fleet.fakes[0], fleet.hosts[0]).await;
    let b_pin = fleet.supervisors[1].pin.clone();
    register(&pool, fleet.hosts[1], &a.endpoint, &b_pin).await;
    let on_b = fleet.execute(1).await;
    for _ in 0..2 {
        let _ = controller.tick().await;
        fleet.retry_now(on_b).await;
    }
    assert_eq!(
        fleet.status(on_b).await,
        "running",
        "claimed and deferred, never dispatched"
    );
    // A reissued CA-valid certificate with host B's own name, but not the pinned leaf.
    let reissued = serve(&fleet.pki, &fleet.fakes[1], fleet.hosts[1]).await;
    register(&pool, fleet.hosts[1], &reissued.endpoint, &b_pin).await;
    for _ in 0..2 {
        let _ = controller.tick().await;
        fleet.retry_now(on_b).await;
    }
    assert_eq!(
        fleet.status(on_b).await,
        "running",
        "claimed and deferred, never dispatched"
    );
    assert_eq!(fleet.fakes[1].total_commands().await, 0);
    assert_eq!(fleet.fakes[0].total_commands().await, 0);
    // Pinning the presented leaf makes the same server acceptable.
    register(&pool, fleet.hosts[1], &reissued.endpoint, &reissued.pin).await;
    fleet.supervisors.push(reissued);
    // Two failures put host B in backoff; wait it out rather than skip it.
    tokio::time::sleep(Duration::from_millis(600)).await;
    // The command was admitted while its owner was unauthenticated, so it
    // fails closed with no dispatch intent rather than running late.
    assert_eq!(fleet.drive(&mut controller, on_b).await, "failed");
    let error: Value = sqlx::query_scalar("SELECT error FROM operations WHERE id=$1")
        .bind(on_b.uuid())
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(
        error,
        json!({"code":"execution_target_changed","dispatch_intent_absent":true})
    );
    assert_eq!(fleet.fakes[1].total_commands().await, 0);
    // The re-pinned owner is now reachable for cleanup.
    let (status, body) = fleet
        .send(
            "POST",
            &format!("/v1/sandboxes/{}/destroy", fleet.sandboxes[1]),
            json!({}),
        )
        .await;
    assert_eq!(status, StatusCode::ACCEPTED, "{body}");
    let destroy = body["operation_id"].as_str().unwrap().parse().unwrap();
    assert_eq!(fleet.drive(&mut controller, destroy).await, "succeeded");
    assert_eq!(fleet.observed(1).await, "destroyed");
}

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn endpoint_change_through_drain_invalidates_cached_connections(pool: PgPool) {
    let mut fleet = Fleet::new(&pool).await;
    let mut controller = fleet.controller(0).await;
    let first = fleet.execute(1).await;
    assert_eq!(fleet.drive(&mut controller, first).await, "succeeded");
    // Move host B: the same supervisor state behind a new address and certificate.
    let moved = serve(&fleet.pki, &fleet.fakes[1], fleet.hosts[1]).await;
    fleet.supervisors.remove(1);
    let host = fleet.hosts[1];
    fleet.store.drain_host(host, 1).await.unwrap();
    fleet
        .store
        .set_host_transport(
            host,
            1,
            &HostTransport {
                version: 1,
                endpoint: moved.endpoint.clone(),
                certificate_sha256: moved.pin.clone(),
                provider_ref: None,
            },
        )
        .await
        .unwrap();
    // A draining host admits no new commands; cleanup must reach the new address.
    let (status, body) = fleet
        .send(
            "POST",
            &format!("/v1/sandboxes/{}/destroy", fleet.sandboxes[1]),
            json!({}),
        )
        .await;
    assert_eq!(status, StatusCode::ACCEPTED, "{body}");
    let destroy = body["operation_id"].as_str().unwrap().parse().unwrap();
    assert_eq!(fleet.drive(&mut controller, destroy).await, "succeeded");
    assert_eq!(fleet.observed(1).await, "destroyed");
    // Authenticated health through the new registration restores freshness.
    let fresh: bool = sqlx::query_scalar("SELECT last_seen_at IS NOT NULL FROM hosts WHERE id=$1")
        .bind(host.uuid())
        .fetch_one(&pool)
        .await
        .unwrap();
    assert!(fresh);
    fleet.supervisors.push(moved);
}

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn slow_host_is_isolated_and_backs_off(pool: PgPool) {
    let fleet = Fleet::new(&pool).await;
    let mut controller = fleet.controller(0).await;
    // Host B now accepts TCP connections and never answers the handshake.
    let hole = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("https://{}", hole.local_addr().unwrap());
    let held = tokio::spawn(async move {
        let mut open = Vec::new();
        while let Ok((stream, _)) = hole.accept().await {
            open.push(stream);
        }
    });
    let pin = fleet.supervisors[1].pin.clone();
    register(&pool, fleet.hosts[1], &endpoint, &pin).await;
    let on_b = fleet.execute(1).await;
    let on_a = fleet.execute(0).await;
    let bound = HEALTH_DEADLINE + Duration::from_secs(2);
    let mut slow = 0;
    for _ in 0..4 {
        let started = Instant::now();
        let _ = controller.tick().await;
        let elapsed = started.elapsed();
        assert!(elapsed < bound, "tick took {elapsed:?}");
        if elapsed >= HEALTH_DEADLINE {
            slow += 1;
        }
        fleet.retry_now(on_a).await;
    }
    // Host A progressed; host B cost at most two deadlines, then backed off.
    assert_eq!(fleet.status(on_a).await, "succeeded");
    assert!(slow <= 2, "{slow} slow ticks");
    assert_eq!(
        fleet.status(on_b).await,
        "running",
        "claimed and deferred, never dispatched"
    );
    assert_eq!(fleet.fakes[1].total_commands().await, 0);
    held.abort();
}

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn api_file_reads_route_to_the_owner_and_deny_other_projects(pool: PgPool) {
    let fleet = Fleet::new(&pool).await;
    let path = format!("/v1/sandboxes/{}/files/captures", fleet.sandboxes[1]);
    let capture = json!({"path":"absent.txt"});
    // Host B itself answered: its fake has no such published file.
    let (status, body) = fleet.send("POST", &path, capture.clone()).await;
    assert_eq!(status, StatusCode::GONE, "{body}");
    assert_eq!(body["code"], "file_capture_missing", "{body}");
    // Another project cannot learn about or reach the owner host.
    let other = project(&pool).await;
    let (status, _) = fleet.send_as(&other, "POST", &path, capture.clone()).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let (status, _) = fleet
        .send_as(
            &other,
            "GET",
            &format!("/v1/sandboxes/{}", fleet.sandboxes[1]),
            Value::Null,
        )
        .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    // A registration pointing at another host's server is refused, never served.
    let a = serve(&fleet.pki, &fleet.fakes[0], fleet.hosts[0]).await;
    register(
        &pool,
        fleet.hosts[1],
        &a.endpoint,
        &fleet.supervisors[1].pin,
    )
    .await;
    let (status, _) = fleet.send("POST", &path, capture).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
}
