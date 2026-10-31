//! PostgreSQL + the actual HTTP router + loopback gRPC/mTLS. No VM is used.
#![allow(clippy::unwrap_used, clippy::expect_used)]
use axum::{
    Router,
    body::{Body, to_bytes},
};
use http::{Request, StatusCode};
use rcgen::{
    BasicConstraints, CertificateParams, CertifiedIssuer, ExtendedKeyUsagePurpose, IsCa, KeyPair,
    KeyUsagePurpose,
};
use sandbox_api::{AppState, router};
use sandbox_controller::{Controller, ControllerConfig};
use sandbox_fake_host::{FakeConfig, FakeHost};
use sandbox_protocol::{
    HostId, Id, OperationId, ProjectId, ProjectToken, SandboxId,
    supervisor::{CreateRequest, supervisor_server::SupervisorServer},
};
use sandbox_store::{
    Store,
    claims::{Claim, OperationKind},
    dispatch::CreateAction,
};
use sandbox_supervisor::transport::{self, ControllerIdentity, MAX_MESSAGE_BYTES};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use sqlx::PgPool;
use std::collections::BTreeSet;
use tokio::{net::TcpListener, task::JoinHandle};
use tokio_stream::wrappers::TcpListenerStream;
use tonic::{service::interceptor::InterceptedService, transport::Server};
use tower::ServiceExt;

pub(super) struct Fixture {
    pub(super) store: Store,
    pub(super) app: Router,
    pub(super) token: String,
    pub(super) config: ControllerConfig,
    pub(super) ca: String,
    pub(super) cert: String,
    pub(super) key: String,
    pub(super) fake: FakeHost,
    server_cert: String,
    server_key: String,
    controller_pin: [u8; 32],
    server_pin: String,
    task: JoinHandle<()>,
}
impl Drop for Fixture {
    fn drop(&mut self) {
        self.task.abort();
    }
}
impl Fixture {
    pub(super) async fn new(pool: &PgPool) -> Self {
        Self::with_artifacts(pool, None).await
    }
    pub(super) async fn with_artifacts(
        pool: &PgPool,
        artifacts: Option<sandbox_artifacts::ArtifactStore>,
    ) -> Self {
        let project = ProjectId::generate();
        let token = ProjectToken::generate().unwrap();
        sqlx::query("INSERT INTO projects(id,name,status,limits,api_tokens) VALUES($1,'controller-test','active','{}',$2)")
            .bind(project.uuid()).bind(json!([{"key_id":token.key_id().as_str(),"hash":hex::encode(token.hash().as_bytes())}])).execute(pool).await.unwrap();
        let host = HostId::generate();
        sqlx::query("INSERT INTO hosts(id,status,cpu_capacity,memory_capacity_mib,disk_capacity_mib,supervisor_epoch) VALUES($1,'ready',4,8192,65536,1)")
            .bind(host.uuid()).execute(pool).await.unwrap();
        // Placement requires an explicit grant for the configured host's legacy
        // pool. Upgrade fixtures can start before migration 0025 adds the table.
        if sqlx::query_scalar::<_, bool>(
            "SELECT to_regclass('project_host_pool_access') IS NOT NULL",
        )
        .fetch_one(pool)
        .await
        .unwrap()
        {
            sqlx::query("INSERT INTO project_host_pool_access(id,project_id,host_pool_id) SELECT gen_random_uuid(),$1,host_pool_id FROM hosts WHERE id=$2")
                .bind(project.uuid()).bind(host.uuid()).execute(pool).await.unwrap();
        }
        let digest = format!("sha256:{}", "a".repeat(64));
        let mut ca_params = CertificateParams::new(Vec::new()).unwrap();
        ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        ca_params.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
        let ca = CertifiedIssuer::self_signed(ca_params, KeyPair::generate().unwrap()).unwrap();
        let controller_key = KeyPair::generate().unwrap();
        let mut controller_params =
            CertificateParams::new(vec!["controller.sandbox.internal".into()]).unwrap();
        controller_params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ClientAuth];
        let controller_cert = controller_params.signed_by(&controller_key, &ca).unwrap();
        let server_key = KeyPair::generate().unwrap();
        let mut server_params =
            CertificateParams::new(vec![transport::host_server_name(host)]).unwrap();
        server_params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
        let server_cert = server_params.signed_by(&server_key, &ca).unwrap();
        let fake = FakeHost::new(FakeConfig {
            host,
            epoch: 1,
            images: BTreeSet::from([digest.clone()]),
            capacity: sandbox_protocol::supervisor::Resources {
                vcpu: 4,
                memory_mib: 8192,
                disk_mib: 65536,
            },
        })
        .unwrap();
        let fake = match artifacts {
            Some(store) => fake.with_artifacts(store),
            None => fake,
        };
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("https://{}", listener.local_addr().unwrap());
        let service = SupervisorServer::new(fake.clone())
            .max_decoding_message_size(MAX_MESSAGE_BYTES)
            .max_encoding_message_size(MAX_MESSAGE_BYTES);
        let service = InterceptedService::new(
            service,
            ControllerIdentity::new(vec![Sha256::digest(controller_cert.der()).into()]).unwrap(),
        );
        let tls = transport::server_tls(
            ca.pem().as_bytes(),
            server_cert.pem().as_bytes(),
            server_key.serialize_pem().as_bytes(),
        );
        let task = tokio::spawn(async move {
            Server::builder()
                .tls_config(tls)
                .unwrap()
                .add_service(service)
                .serve_with_incoming(TcpListenerStream::new(listener))
                .await
                .unwrap();
        });
        let store = Store::from_pool(pool.clone());
        Self {
            app: router(AppState {
                images: sandbox_protocol::images::ImageAllowlist::new([digest.clone()]).unwrap(),
                store: store.clone(),
            }),
            store,
            token: token.render_once(),
            config: ControllerConfig {
                endpoint,
                host,
                epoch: 1,
                allowed_images: BTreeSet::from([digest]),
                allow_simulated: true,
            },
            ca: ca.pem(),
            cert: controller_cert.pem(),
            key: controller_key.serialize_pem(),
            fake,
            server_cert: server_cert.pem(),
            server_key: server_key.serialize_pem(),
            controller_pin: Sha256::digest(controller_cert.der()).into(),
            server_pin: hex::encode(Sha256::digest(server_cert.der())),
            task,
        }
    }
    /// A second incarnation using the same test trust roots and host identity.
    pub(super) async fn serve_supervisor(
        &self,
        supervisor: impl sandbox_protocol::supervisor::supervisor_server::Supervisor,
    ) -> (String, String, JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("https://{}", listener.local_addr().unwrap());
        let service = InterceptedService::new(
            SupervisorServer::new(supervisor)
                .max_decoding_message_size(MAX_MESSAGE_BYTES)
                .max_encoding_message_size(MAX_MESSAGE_BYTES),
            ControllerIdentity::new(vec![self.controller_pin]).unwrap(),
        );
        let tls = transport::server_tls(
            self.ca.as_bytes(),
            self.server_cert.as_bytes(),
            self.server_key.as_bytes(),
        );
        let task = tokio::spawn(async move {
            Server::builder()
                .tls_config(tls)
                .unwrap()
                .add_service(service)
                .serve_with_incoming(TcpListenerStream::new(listener))
                .await
                .unwrap();
        });
        (endpoint, self.server_pin.clone(), task)
    }
    pub(super) async fn controller(&self) -> Controller {
        Controller::connect(
            self.store.clone(),
            self.config.clone(),
            self.ca.as_bytes(),
            self.cert.as_bytes(),
            self.key.as_bytes(),
        )
        .await
        .unwrap()
    }
    pub(super) async fn send(&self, method: &str, path: &str, body: Value) -> (StatusCode, Value) {
        let request = Request::builder()
            .method(method)
            .uri(path)
            .header("authorization", format!("Bearer {}", self.token))
            .header("content-type", "application/json")
            .header("idempotency-key", uuid::Uuid::now_v7().to_string())
            .body(Body::from(body.to_string()))
            .unwrap();
        let response = self.app.clone().oneshot(request).await.unwrap();
        let status = response.status();
        let body = to_bytes(response.into_body(), 64 * 1024).await.unwrap();
        (status, serde_json::from_slice(&body).unwrap())
    }
    pub(super) async fn admit(&self) -> (OperationId, SandboxId) {
        let (status, body) = self
            .send(
                "POST",
                "/v1/sandboxes",
                json!({"image_digest":format!("sha256:{}","a".repeat(64)),
            "resources":{"vcpu":2,"memory_mib":2048,"disk_mib":8192}}),
            )
            .await;
        assert_eq!(status, StatusCode::ACCEPTED, "{body}");
        (
            body["operation_id"].as_str().unwrap().parse().unwrap(),
            body["sandbox_id"].as_str().unwrap().parse().unwrap(),
        )
    }
    pub(super) async fn reclaim_now(&self) {
        sqlx::query("UPDATE operations SET lease_expires_at=clock_timestamp()-interval '1 second',next_retry_at=NULL WHERE completed_at IS NULL")
            .execute(self.store.pool()).await.unwrap();
    }
    pub(super) async fn prepared(&self) -> (Claim, CreateRequest) {
        self.store
            .observe_configured_host(self.config.host, 1)
            .await
            .unwrap();
        let claim = self
            .store
            .claim_next(OperationKind::Create, 30)
            .await
            .unwrap()
            .unwrap();
        self.store
            .reserve_create(&claim, self.config.host, 1)
            .await
            .unwrap();
        let CreateAction::Start(request) = self
            .store
            .prepare_create_dispatch(&claim, &self.config.allowed_images)
            .await
            .unwrap()
        else {
            panic!("fresh start")
        };
        (claim, request)
    }
}
