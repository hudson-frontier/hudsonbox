//! End-to-end tests for `POST /v1/sandboxes`, through the real router.
//!
//! These drive the assembled axum service rather than calling handlers
//! directly, so the extractors — authentication and the idempotency header —
//! are part of what is tested. A route that works only when called from a test
//! helper is not a route that works.
//!
//! Set `DATABASE_URL` to run them (`make up` provides one).

#![allow(clippy::expect_used, clippy::unwrap_used)]

use axum::Router;
use axum::body::Body;
use http::{Request, StatusCode, header};
use sandbox_api::{AppState, router};
use sandbox_protocol::{Id, ProjectId, ProjectToken};
use sandbox_store::Store;
use serde_json::{Value, json};
use tower::ServiceExt as _;

struct Fixture {
    app: Router,
    store: Store,
    project_id: ProjectId,
    token: String,
}

async fn fixture(status: &str) -> Option<Fixture> {
    let Ok(url) = std::env::var("DATABASE_URL") else {
        eprintln!("skipping: DATABASE_URL is unset — run `make up` and export it");
        return None;
    };
    let store = Store::connect(&url, 5).await.expect("connect");
    store.migrate().await.expect("migrate");

    let token = ProjectToken::generate().expect("generate");
    let project_id = ProjectId::generate();

    sqlx::query(
        "INSERT INTO projects (id, name, status, limits, api_tokens)
         VALUES ($1, 'api-test', $2, '{}', $3)",
    )
    .bind(project_id.uuid())
    .bind(status)
    .bind(json!([{
        "key_id": token.key_id().as_str(),
        "hash": hex::encode(token.hash().as_bytes()),
        "expires_at": null,
        "revoked_at": null,
    }]))
    .execute(store.pool())
    .await
    .expect("insert project");

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
        project_id,
        token: token.render_once(),
    })
}

impl Fixture {
    fn request(&self, key: Option<&str>, auth: Option<&str>, body: Value) -> Request<Body> {
        let mut builder = Request::builder()
            .method("POST")
            .uri("/v1/sandboxes")
            .header(header::CONTENT_TYPE, "application/json");

        if let Some(key) = key {
            builder = builder.header("idempotency-key", key);
        }
        if let Some(auth) = auth {
            builder = builder.header(header::AUTHORIZATION, format!("Bearer {auth}"));
        }

        builder
            .body(Body::from(body.to_string()))
            .expect("build request")
    }

    fn valid_body(&self) -> Value {
        json!({
            "image_digest": format!("sha256:{}", "a".repeat(64)),
            "name": "example",
            "resources": {"vcpu": 2, "memory_mib": 2048, "disk_mib": 8192},
        })
    }

    async fn send(&self, request: Request<Body>) -> (StatusCode, Value, http::HeaderMap) {
        let response = self.app.clone().oneshot(request).await.expect("response");
        let status = response.status();
        let headers = response.headers().clone();
        let bytes = axum::body::to_bytes(response.into_body(), 64 * 1024)
            .await
            .expect("read body");
        let value = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
        (status, value, headers)
    }

    async fn cleanup(&self) {
        for statement in [
            "UPDATE sandboxes SET active_transition_operation_id = NULL WHERE project_id = $1",
            "DELETE FROM operations WHERE project_id = $1",
            "DELETE FROM sandboxes WHERE project_id = $1",
            "DELETE FROM projects WHERE id = $1",
        ] {
            sqlx::query(statement)
                .bind(self.project_id.uuid())
                .execute(self.store.pool())
                .await
                .expect("cleanup");
        }
    }
}

#[tokio::test]
async fn admits_a_valid_request() {
    let Some(f) = fixture("active").await else {
        return;
    };

    let (status, body, headers) = f
        .send(f.request(Some("valid-request-key-01"), Some(&f.token), f.valid_body()))
        .await;

    assert_eq!(status, StatusCode::ACCEPTED, "{body}");
    assert_eq!(headers[header::CACHE_CONTROL], "no-store");

    let sandbox_id = body["sandbox_id"].as_str().expect("sandbox_id");
    let operation_id = body["operation_id"].as_str().expect("operation_id");
    assert!(sandbox_id.starts_with("sbx_"), "{sandbox_id}");
    assert!(operation_id.starts_with("op_"), "{operation_id}");
    assert_eq!(body["status"], "queued");
    assert_eq!(body["status_url"], format!("/v1/operations/{operation_id}"));

    f.cleanup().await;
}

#[tokio::test]
async fn bearer_scheme_is_case_insensitive_while_credentials_remain_exact() {
    let Some(f) = fixture("active").await else {
        return;
    };
    for (index, scheme) in ["Bearer", "bearer", "BEARER", "bEaReR"]
        .into_iter()
        .enumerate()
    {
        let key = format!("bearer-casing-request-{index}");
        let mut request = f.request(Some(&key), Some(&f.token), f.valid_body());
        request.headers_mut().insert(
            header::AUTHORIZATION,
            format!("{scheme} {}", f.token).parse().unwrap(),
        );
        let (status, body, _) = f.send(request).await;
        assert_eq!(status, StatusCode::ACCEPTED, "{scheme}: {body}");
    }
    let wrong = ProjectToken::generate().unwrap().render_once();
    for value in [
        format!("bearer {wrong}"),
        format!("Basic {}", f.token),
        format!("BearerX {}", f.token),
    ] {
        let mut request = f.request(Some("invalid-auth-request-01"), None, f.valid_body());
        request
            .headers_mut()
            .insert(header::AUTHORIZATION, value.parse().unwrap());
        let (status, _, _) = f.send(request).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
    }
    f.cleanup().await;
}

#[tokio::test]
async fn a_retry_returns_the_same_handles() {
    let Some(f) = fixture("active").await else {
        return;
    };
    let key = "retry-same-handles-01";

    let (_, first, _) = f
        .send(f.request(Some(key), Some(&f.token), f.valid_body()))
        .await;
    let (status, second, _) = f
        .send(f.request(Some(key), Some(&f.token), f.valid_body()))
        .await;

    assert_eq!(status, StatusCode::ACCEPTED);
    assert_eq!(first["sandbox_id"], second["sandbox_id"]);
    assert_eq!(first["operation_id"], second["operation_id"]);

    f.cleanup().await;
}

#[tokio::test]
async fn a_changed_payload_under_the_same_key_is_a_conflict() {
    let Some(f) = fixture("active").await else {
        return;
    };
    let key = "conflicting-payload-1";

    f.send(f.request(Some(key), Some(&f.token), f.valid_body()))
        .await;

    let mut changed = f.valid_body();
    changed["resources"]["vcpu"] = json!(4);
    let (status, body, _) = f.send(f.request(Some(key), Some(&f.token), changed)).await;

    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(body["code"], "conflict");

    f.cleanup().await;
}

#[tokio::test]
async fn rejects_a_request_with_no_credential() {
    let Some(f) = fixture("active").await else {
        return;
    };

    let (status, body, _) = f
        .send(f.request(Some("no-credential-key-01"), None, f.valid_body()))
        .await;

    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert_eq!(body["code"], "unauthenticated");
    assert_eq!(
        body["title"], "Authentication is required",
        "the response must not say why the credential failed"
    );

    f.cleanup().await;
}

#[tokio::test]
async fn rejects_an_unknown_credential() {
    let Some(f) = fixture("active").await else {
        return;
    };
    let other = ProjectToken::generate().expect("generate").render_once();

    let (status, body, _) = f
        .send(f.request(Some("unknown-credential-1"), Some(&other), f.valid_body()))
        .await;

    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert_eq!(
        body["title"], "Authentication is required",
        "an unknown key must be indistinguishable from a bad secret"
    );

    f.cleanup().await;
}

#[tokio::test]
async fn rejects_a_suspended_projects_credential() {
    let Some(f) = fixture("suspended").await else {
        return;
    };

    let (status, _, _) = f
        .send(f.request(Some("suspended-project-01"), Some(&f.token), f.valid_body()))
        .await;

    assert_eq!(status, StatusCode::UNAUTHORIZED);

    f.cleanup().await;
}

#[tokio::test]
async fn requires_an_idempotency_key() {
    let Some(f) = fixture("active").await else {
        return;
    };

    let (status, body, _) = f
        .send(f.request(None, Some(&f.token), f.valid_body()))
        .await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["code"], "bad_request");

    f.cleanup().await;
}

#[tokio::test]
async fn rejects_a_malformed_idempotency_key() {
    let Some(f) = fixture("active").await else {
        return;
    };

    let (status, _, _) = f
        .send(f.request(Some("short"), Some(&f.token), f.valid_body()))
        .await;

    assert_eq!(status, StatusCode::BAD_REQUEST);

    f.cleanup().await;
}

#[tokio::test]
async fn rejects_a_sandbox_beyond_the_envelope() {
    let Some(f) = fixture("active").await else {
        return;
    };

    let mut body = f.valid_body();
    body["resources"]["memory_mib"] = json!(65_536);
    let (status, body, _) = f
        .send(f.request(Some("too-large-sandbox-01"), Some(&f.token), body))
        .await;

    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");

    f.cleanup().await;
}

#[tokio::test]
async fn errors_are_problem_json() {
    let Some(f) = fixture("active").await else {
        return;
    };

    let response = f
        .app
        .clone()
        .oneshot(f.request(None, Some(&f.token), f.valid_body()))
        .await
        .expect("response");

    assert_eq!(
        response.headers()[header::CONTENT_TYPE],
        "application/problem+json"
    );
    assert_eq!(response.headers()[header::CACHE_CONTROL], "no-store");

    f.cleanup().await;
}

fn configured_router(store: &Store, digit: char) -> Router {
    router(AppState {
        store: store.clone(),
        images: sandbox_protocol::images::ImageAllowlist::new([format!(
            "sha256:{}",
            digit.to_string().repeat(64)
        )])
        .unwrap(),
    })
}

#[tokio::test]
async fn unapproved_image_has_no_side_effects_and_does_not_consume_key() {
    let Some(f) = fixture("active").await else {
        return;
    };
    let key = "denied-image-retry-key";
    let mut denied = f.valid_body();
    denied["image_digest"] = json!(format!("sha256:{}", "b".repeat(64)));
    let (status, body, headers) = f.send(f.request(Some(key), Some(&f.token), denied)).await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert_eq!(body["code"], "image_not_allowed");
    assert_eq!(headers[header::CONTENT_TYPE], "application/problem+json");
    assert_eq!(headers[header::CACHE_CONTROL], "no-store");
    for table in ["sandboxes", "operations", "allocations"] {
        let (count,): (i64,) =
            sqlx::query_as(&format!("SELECT count(*) FROM {table} WHERE project_id=$1"))
                .bind(f.project_id.uuid())
                .fetch_one(f.store.pool())
                .await
                .unwrap();
        assert_eq!(count, 0, "denial inserted {table}");
    }
    let (status, _, _) = f
        .send(f.request(Some(key), Some(&f.token), f.valid_body()))
        .await;
    assert_eq!(status, StatusCode::ACCEPTED);
    f.cleanup().await;
}

#[tokio::test]
async fn policy_removal_preserves_retries_and_conflicts_but_rejects_new_work() {
    let Some(mut f) = fixture("active").await else {
        return;
    };
    let key = "policy-removal-retry-01";
    let (status, original, _) = f
        .send(f.request(Some(key), Some(&f.token), f.valid_body()))
        .await;
    assert_eq!(status, StatusCode::ACCEPTED);
    f.app = configured_router(&f.store, 'b');
    let (status, retry, _) = f
        .send(f.request(Some(key), Some(&f.token), f.valid_body()))
        .await;
    assert_eq!(status, StatusCode::ACCEPTED);
    assert_eq!(retry, original);
    let mut changed = f.valid_body();
    changed["name"] = json!("changed after revocation");
    let (status, body, _) = f.send(f.request(Some(key), Some(&f.token), changed)).await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(body["code"], "conflict");
    let (status, body, _) = f
        .send(f.request(Some("new-after-removal-01"), Some(&f.token), f.valid_body()))
        .await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert_eq!(body["code"], "image_not_allowed");
    let (count,): (i64,) = sqlx::query_as("SELECT count(*) FROM operations WHERE project_id=$1")
        .bind(f.project_id.uuid())
        .fetch_one(f.store.pool())
        .await
        .unwrap();
    assert_eq!(count, 1);
    f.cleanup().await;
}

#[tokio::test]
async fn denial_still_requires_auth_and_canonical_digest() {
    let Some(f) = fixture("active").await else {
        return;
    };
    let mut body = f.valid_body();
    body["image_digest"] = json!(format!("sha256:{}", "b".repeat(64)));
    let (status, body, _) = f
        .send(f.request(Some("unauthorized-image-01"), None, body))
        .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert_eq!(body["code"], "unauthenticated");
    let mut body = f.valid_body();
    body["image_digest"] = json!(format!("sha256:{}", "A".repeat(64)));
    let (status, body, _) = f
        .send(f.request(Some("noncanonical-image-1"), Some(&f.token), body))
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["code"], "bad_request");
    f.cleanup().await;
}

#[tokio::test]
async fn concurrent_approved_creates_share_one_handle_and_denials_write_nothing() {
    let Some(f) = fixture("active").await else {
        return;
    };
    let mut tasks = tokio::task::JoinSet::new();
    for i in 0..16 {
        let denied = i % 2 == 0;
        let mut body = f.valid_body();
        if denied {
            body["image_digest"] = json!(format!("sha256:{}", "b".repeat(64)));
        }
        let key = if denied {
            "concurrent-denied-image"
        } else {
            "concurrent-allowed-img"
        };
        let request = f.request(Some(key), Some(&f.token), body);
        let app = f.app.clone();
        tasks.spawn(async move {
            let response = app.oneshot(request).await.unwrap();
            let status = response.status();
            let bytes = axum::body::to_bytes(response.into_body(), 64 * 1024)
                .await
                .unwrap();
            let body: Value = serde_json::from_slice(&bytes).unwrap();
            (denied, status, body)
        });
    }
    let mut handle = None;
    while let Some(result) = tasks.join_next().await {
        let (denied, status, body) = result.unwrap();
        if denied {
            assert_eq!(status, StatusCode::FORBIDDEN, "{body}");
        } else {
            assert_eq!(status, StatusCode::ACCEPTED, "{body}");
            if let Some(ref previous) = handle {
                assert_eq!(previous, &body["operation_id"]);
            }
            handle = Some(body["operation_id"].clone());
        }
    }
    for table in ["sandboxes", "operations"] {
        let (count,): (i64,) =
            sqlx::query_as(&format!("SELECT count(*) FROM {table} WHERE project_id=$1"))
                .bind(f.project_id.uuid())
                .fetch_one(f.store.pool())
                .await
                .unwrap();
        assert_eq!(count, 1);
    }
    f.cleanup().await;
}

#[tokio::test]
async fn another_projects_key_cannot_bypass_image_policy() {
    let Some(a) = fixture("active").await else {
        return;
    };
    let Some(mut b) = fixture("active").await else {
        return;
    };
    let key = "shared-key-image-policy";
    let (status, _, _) = a
        .send(a.request(Some(key), Some(&a.token), a.valid_body()))
        .await;
    assert_eq!(status, StatusCode::ACCEPTED);
    b.app = configured_router(&b.store, 'b');
    let (status, body, _) = b
        .send(b.request(Some(key), Some(&b.token), b.valid_body()))
        .await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert_eq!(body["code"], "image_not_allowed");
    a.cleanup().await;
    b.cleanup().await;
}

#[tokio::test]
async fn host_resource_minimums_are_checked_before_admission() {
    let Some(f) = fixture("active").await else {
        return;
    };
    let key = "minimum-resources-key-01";
    for (memory, disk) in [(127, 64), (128, 63)] {
        let mut body = f.valid_body();
        body["resources"] = json!({"vcpu":1,"memory_mib":memory,"disk_mib":disk});
        let (status, _, _) = f.send(f.request(Some(key), Some(&f.token), body)).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
    }
    let (operations,sandboxes):(i64,i64)=sqlx::query_as("SELECT (SELECT count(*) FROM operations WHERE project_id=$1),(SELECT count(*) FROM sandboxes WHERE project_id=$1)")
        .bind(f.project_id.uuid()).fetch_one(f.store.pool()).await.unwrap();
    assert_eq!((operations, sandboxes), (0, 0));
    let mut body = f.valid_body();
    body["resources"] = json!({"vcpu":1,"memory_mib":128,"disk_mib":64});
    let (status, _, _) = f.send(f.request(Some(key), Some(&f.token), body)).await;
    assert_eq!(
        status,
        StatusCode::ACCEPTED,
        "a rejected request must not consume the key"
    );
    f.cleanup().await;
}

#[tokio::test]
async fn tighter_resource_minimum_preserves_legacy_retry_handles() {
    let Some(f) = fixture("active").await else {
        return;
    };
    let key = "legacy-resource-retry-01";
    let body = f.valid_body();
    let (_, admitted, _) = f
        .send(f.request(Some(key), Some(&f.token), body.clone()))
        .await;
    // Model an operation admitted under the previous positive-only size policy.
    let mut legacy = body.clone();
    legacy["resources"] = json!({"vcpu":1,"memory_mib":127,"disk_mib":64});
    let typed: sandbox_api::sandboxes::CreateRequest =
        serde_json::from_value(legacy.clone()).unwrap();
    let digest = sandbox_protocol::RequestDigest::compute("POST", "/v1/sandboxes", &typed).unwrap();
    sqlx::query("UPDATE operations SET request_digest=$2,payload=$3 WHERE project_id=$1")
        .bind(f.project_id.uuid())
        .bind(digest.as_bytes().as_slice())
        .bind(serde_json::to_value(&typed).unwrap())
        .execute(f.store.pool())
        .await
        .unwrap();
    sqlx::query("UPDATE sandboxes SET resources=$2 WHERE project_id=$1")
        .bind(f.project_id.uuid())
        .bind(&legacy["resources"])
        .execute(f.store.pool())
        .await
        .unwrap();
    let (status, retry, _) = f
        .send(f.request(Some(key), Some(&f.token), legacy.clone()))
        .await;
    assert_eq!(status, StatusCode::ACCEPTED);
    assert_eq!(retry, admitted);
    let (status, _, _) = f.send(f.request(Some(key), Some(&f.token), body)).await;
    assert_eq!(status, StatusCode::CONFLICT);
    let (status, _, _) = f
        .send(f.request(Some("legacy-size-new-key-01"), Some(&f.token), legacy))
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    f.cleanup().await;
}
