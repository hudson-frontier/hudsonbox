//! Invalid optional metadata must fail before admission, preserving retry keys.
#![allow(clippy::unwrap_used, clippy::expect_used)]
use axum::{
    Router,
    body::{Body, to_bytes},
};
use http::{Request, StatusCode};
use sandbox_api::{AppState, router};
use sandbox_protocol::{Id, ProjectId, ProjectToken, SandboxId};
use sandbox_store::Store;
use serde_json::{Value, json};
use sqlx::PgPool;
use tower::ServiceExt;

async fn fixture(pool: &PgPool) -> (Router, ProjectId, String) {
    let project = ProjectId::generate();
    let token = ProjectToken::generate().unwrap();
    sqlx::query("INSERT INTO projects(id,name,status,limits,api_tokens) VALUES($1,'metadata','active','{}',$2)")
        .bind(project.uuid()).bind(json!([{"key_id":token.key_id().as_str(),"hash":hex::encode(token.hash().as_bytes())}]))
        .execute(pool).await.unwrap();
    let app = router(AppState {
        store: Store::from_pool(pool.clone()),
        images: sandbox_protocol::images::ImageAllowlist::new([format!(
            "sha256:{}",
            "a".repeat(64)
        )])
        .unwrap(),
    });
    (app, project, token.render_once())
}
async fn send(
    app: &Router,
    token: &str,
    route: &str,
    key: &str,
    body: Value,
) -> (StatusCode, Value) {
    let request = Request::builder()
        .method("POST")
        .uri(route)
        .header("authorization", format!("Bearer {token}"))
        .header("content-type", "application/json")
        .header("idempotency-key", key)
        .body(Body::from(body.to_string()))
        .unwrap();
    let response = app.clone().oneshot(request).await.unwrap();
    let status = response.status();
    let bytes = to_bytes(response.into_body(), 65536).await.unwrap();
    (status, serde_json::from_slice(&bytes).unwrap())
}

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn create_rejects_nul_metadata_without_consuming_the_key(pool: PgPool) {
    let (app, project, token) = fixture(&pool).await;
    for (index, field) in ["name", "correlation_id"].into_iter().enumerate() {
        let key = format!("nul-metadata-{field}-request");
        let mut body = json!({"image_digest":format!("sha256:{}", "a".repeat(64)),
            "resources":{"vcpu":1,"memory_mib":128,"disk_mib":64}});
        body[field] = json!("before\0after");
        let bad = send(&app, &token, "/v1/sandboxes", &key, body.clone()).await;
        assert_eq!(bad.0, StatusCode::BAD_REQUEST, "{}", bad.1);
        assert_eq!(bad.1["code"], "bad_request");
        let count: i64 = sqlx::query_scalar("SELECT count(*) FROM operations WHERE project_id=$1")
            .bind(project.uuid())
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(count, index as i64);
        body[field] = json!("corrected\tmetadata\n");
        let accepted = send(&app, &token, "/v1/sandboxes", &key, body.clone()).await;
        assert_eq!(accepted.0, StatusCode::ACCEPTED, "{}", accepted.1);
        let retry = send(&app, &token, "/v1/sandboxes", &key, body).await;
        assert_eq!(retry.0, StatusCode::ACCEPTED);
        assert_eq!(retry.1["operation_id"], accepted.1["operation_id"]);
    }
}

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn destroy_rejects_nul_correlation_without_consuming_the_key(pool: PgPool) {
    let (app, project, token) = fixture(&pool).await;
    let sandbox = SandboxId::generate();
    sqlx::query("INSERT INTO sandboxes(id,project_id,image_digest,resources,desired_state,observed_state,destroyed_at) VALUES($1,$2,$3,'{}','destroyed','destroyed',clock_timestamp())")
        .bind(sandbox.uuid()).bind(project.uuid()).bind(format!("sha256:{}", "a".repeat(64))).execute(&pool).await.unwrap();
    let route = format!("/v1/sandboxes/{sandbox}/destroy");
    let key = "nul-destroy-correlation-request";
    let bad = send(
        &app,
        &token,
        &route,
        key,
        json!({"correlation_id":"before\0after"}),
    )
    .await;
    assert_eq!(bad.0, StatusCode::BAD_REQUEST, "{}", bad.1);
    assert_eq!(bad.1["code"], "bad_request");
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM operations WHERE project_id=$1")
            .bind(project.uuid())
            .fetch_one(&pool)
            .await
            .unwrap(),
        0
    );
    let body = json!({"correlation_id":"corrected\tmetadata\n"});
    let accepted = send(&app, &token, &route, key, body.clone()).await;
    assert_eq!(accepted.0, StatusCode::ACCEPTED, "{}", accepted.1);
    assert_eq!(accepted.1["status"], "succeeded");
    let retry = send(&app, &token, &route, key, body).await;
    assert_eq!(retry.0, StatusCode::ACCEPTED);
    assert_eq!(retry.1["operation_id"], accepted.1["operation_id"]);
}
