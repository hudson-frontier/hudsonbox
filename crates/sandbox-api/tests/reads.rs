//! Status routes, through the real router.
//!
//! The important assertions here are the negative ones: another project's
//! resources must be indistinguishable from resources that do not exist.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use axum::Router;
use axum::body::Body;
use http::{Request, StatusCode, header};
use sandbox_api::{AppState, router};
use sandbox_protocol::{Id, ProjectId, ProjectToken};
use sandbox_store::Store;
use serde_json::{Value, json};
use tower::ServiceExt as _;

struct Tenant {
    project_id: ProjectId,
    token: String,
}

struct Fixture {
    app: Router,
    store: Store,
    tenants: Vec<Tenant>,
}

async fn fixture(count: usize) -> Option<Fixture> {
    let Ok(url) = std::env::var("DATABASE_URL") else {
        eprintln!("skipping: DATABASE_URL is unset — run `make up` and export it");
        return None;
    };
    let store = Store::connect(&url, 5).await.expect("connect");
    store.migrate().await.expect("migrate");

    let mut tenants = Vec::new();
    for _ in 0..count {
        let token = ProjectToken::generate().expect("generate");
        let project_id = ProjectId::generate();
        sqlx::query(
            "INSERT INTO projects (id, name, status, limits, api_tokens)
             VALUES ($1, 'reads-test', 'active', '{}', $2)",
        )
        .bind(project_id.uuid())
        .bind(json!([{
            "key_id": token.key_id().as_str(),
            "hash": hex::encode(token.hash().as_bytes()),
            "expires_at": null,
            "revoked_at": null,
        }]))
        .execute(store.pool())
        .await
        .expect("insert project");

        tenants.push(Tenant {
            project_id,
            token: token.render_once(),
        });
    }

    Some(Fixture {
        app: router(AppState {
            images: sandbox_protocol::images::ImageAllowlist::new([format!(
                "sha256:{}",
                "a".repeat(64)
            )])
            .unwrap(),
            store: store.clone(),
        }),
        store,
        tenants,
    })
}

impl Fixture {
    async fn send(&self, request: Request<Body>) -> (StatusCode, Value) {
        let response = self.app.clone().oneshot(request).await.expect("response");
        let status = response.status();
        let bytes = axum::body::to_bytes(response.into_body(), 64 * 1024)
            .await
            .expect("read body");
        (
            status,
            serde_json::from_slice(&bytes).unwrap_or(Value::Null),
        )
    }

    async fn get(&self, tenant: usize, path: &str) -> (StatusCode, Value) {
        let request = Request::builder()
            .method("GET")
            .uri(path)
            .header(
                header::AUTHORIZATION,
                format!("Bearer {}", self.tenants[tenant].token),
            )
            .body(Body::empty())
            .expect("build request");
        self.send(request).await
    }

    /// Create a sandbox as `tenant`, returning its ids.
    async fn create(&self, tenant: usize, key: &str) -> (String, String) {
        let request = Request::builder()
            .method("POST")
            .uri("/v1/sandboxes")
            .header(header::CONTENT_TYPE, "application/json")
            .header("idempotency-key", key)
            .header(
                header::AUTHORIZATION,
                format!("Bearer {}", self.tenants[tenant].token),
            )
            .body(Body::from(
                json!({
                    "image_digest": format!("sha256:{}", "a".repeat(64)),
                    "name": "readable",
                    "resources": {"vcpu": 2, "memory_mib": 2048, "disk_mib": 8192},
                })
                .to_string(),
            ))
            .expect("build request");

        let (status, body) = self.send(request).await;
        assert_eq!(status, StatusCode::ACCEPTED, "{body}");
        (
            body["sandbox_id"].as_str().expect("sandbox_id").to_owned(),
            body["operation_id"]
                .as_str()
                .expect("operation_id")
                .to_owned(),
        )
    }

    async fn cleanup(&self) {
        for tenant in &self.tenants {
            for statement in [
                "UPDATE sandboxes SET active_transition_operation_id = NULL WHERE project_id = $1",
                "DELETE FROM operations WHERE project_id = $1",
                "DELETE FROM sandboxes WHERE project_id = $1",
                "DELETE FROM projects WHERE id = $1",
            ] {
                sqlx::query(statement)
                    .bind(tenant.project_id.uuid())
                    .execute(self.store.pool())
                    .await
                    .expect("cleanup");
            }
        }
    }
}

#[tokio::test]
async fn the_status_url_from_create_resolves() {
    let Some(f) = fixture(1).await else { return };
    let (sandbox_id, operation_id) = f.create(0, "status-url-resolves-1").await;

    let (status, body) = f.get(0, &format!("/v1/operations/{operation_id}")).await;

    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["operation_id"], operation_id);
    assert_eq!(body["sandbox_id"], sandbox_id);
    assert_eq!(body["kind"], "create");
    assert_eq!(body["status"], "queued");
    assert!(
        body.get("completed_at").is_none(),
        "a queued operation must not claim a completion time"
    );

    f.cleanup().await;
}

#[tokio::test]
async fn a_sandbox_separates_intent_from_observation() {
    let Some(f) = fixture(1).await else { return };
    let (sandbox_id, operation_id) = f.create(0, "intent-vs-observed-1").await;

    let (status, body) = f.get(0, &format!("/v1/sandboxes/{sandbox_id}")).await;

    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["desired_state"], "running");
    assert_eq!(
        body["observed_state"], "creating",
        "the observed state must not be reported as the desired one"
    );
    assert!(
        body.get("observed_at").is_none(),
        "nothing has been confirmed yet, so there is no observation time"
    );
    assert_eq!(body["active_operation_id"], operation_id);
    assert_eq!(body["generation"], 0);

    f.cleanup().await;
}

#[tokio::test]
async fn another_projects_operation_is_not_found() {
    let Some(f) = fixture(2).await else { return };
    let (_, operation_id) = f.create(0, "cross-tenant-op-read-1").await;

    let (status, body) = f.get(1, &format!("/v1/operations/{operation_id}")).await;

    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body["code"], "not_found");
    assert_eq!(
        body["title"], "No such resource",
        "the response must not reveal that the operation exists elsewhere"
    );

    f.cleanup().await;
}

#[tokio::test]
async fn another_projects_sandbox_is_not_found() {
    let Some(f) = fixture(2).await else { return };
    let (sandbox_id, _) = f.create(0, "cross-tenant-sbx-read-1").await;

    let (status, body) = f.get(1, &format!("/v1/sandboxes/{sandbox_id}")).await;

    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body["title"], "No such resource");

    f.cleanup().await;
}

#[tokio::test]
async fn an_unused_id_is_the_same_not_found() {
    let Some(f) = fixture(2).await else { return };
    let (sandbox_id, _) = f.create(0, "identical-404-shape-1").await;

    let (mine_elsewhere, theirs) = (
        f.get(1, &format!("/v1/sandboxes/{sandbox_id}")).await,
        f.get(1, "/v1/sandboxes/sbx_01996110-7c00-7000-8000-00000000dead")
            .await,
    );

    assert_eq!(
        mine_elsewhere, theirs,
        "an existing resource in another project answered differently from one that does not exist"
    );

    f.cleanup().await;
}

#[tokio::test]
async fn a_malformed_id_is_a_bad_request() {
    let Some(f) = fixture(1).await else { return };

    for path in [
        "/v1/operations/not-an-id",
        "/v1/operations/sbx_01996110-7c00-7000-8000-000000000001",
        "/v1/sandboxes/op_01996110-7c00-7000-8000-000000000001",
        "/v1/sandboxes/01996110-7c00-7000-8000-000000000001",
    ] {
        let (status, _) = f.get(0, path).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "for {path}");
    }

    f.cleanup().await;
}

#[tokio::test]
async fn reads_require_a_credential() {
    let Some(f) = fixture(1).await else { return };
    let (sandbox_id, _) = f.create(0, "reads-need-auth-key1").await;

    let request = Request::builder()
        .method("GET")
        .uri(format!("/v1/sandboxes/{sandbox_id}"))
        .body(Body::empty())
        .expect("build request");
    let (status, _) = f.send(request).await;

    assert_eq!(status, StatusCode::UNAUTHORIZED);

    f.cleanup().await;
}

#[tokio::test]
async fn project_reads_do_not_expose_operator_fleet_fields() {
    let Some(f) = fixture(1).await else { return };
    let (sandbox_id, operation_id) = f.create(0, "fleet-redaction-read").await;
    for path in [
        format!("/v1/sandboxes/{sandbox_id}"),
        format!("/v1/operations/{operation_id}"),
    ] {
        let (status, body) = f.get(0, &path).await;
        assert_eq!(status, StatusCode::OK);
        fn check(value: &Value) {
            match value {
                Value::Object(fields) => {
                    for (key, value) in fields {
                        assert!(
                            ![
                                "host_id",
                                "host_pool_id",
                                "supervisor_epoch",
                                "endpoint",
                                "transport_server_name",
                                "transport_certificate_sha256",
                                "provider_ref",
                                "credential_secret_ref"
                            ]
                            .contains(&key.as_str()),
                            "private field {key}"
                        );
                        check(value);
                    }
                }
                Value::Array(values) => values.iter().for_each(check),
                _ => {}
            }
        }
        check(&body);
    }
    f.cleanup().await;
}
