//! Expired response behavior through real authentication, admission and lists.
//! Database lifecycle fixtures are synthetic; no VM or storage effects implied.
#![allow(clippy::unwrap_used, clippy::expect_used)]
#[path = "support/output.rs"]
mod fixture;
use axum::{
    Router,
    body::{Body, to_bytes},
};
use fixture::Fixture;
use http::{Request, StatusCode};
use sandbox_protocol::{Id, OperationId};
use sandbox_store::retention::ResponseRetention;
use serde_json::{Value, json};
use sqlx::PgPool;
use tower::ServiceExt;

async fn send(
    app: &Router,
    token: &str,
    method: &str,
    path: &str,
    key: &str,
    body: Value,
) -> (StatusCode, Value) {
    let request = Request::builder()
        .method(method)
        .uri(path)
        .header("authorization", format!("Bearer {token}"))
        .header("content-type", "application/json")
        .header("idempotency-key", key)
        .body(if method == "GET" {
            Body::empty()
        } else {
            Body::from(body.to_string())
        })
        .unwrap();
    let response = app.clone().oneshot(request).await.unwrap();
    assert_eq!(response.headers()["cache-control"], "no-store");
    let status = response.status();
    let bytes = to_bytes(response.into_body(), 128 * 1024).await.unwrap();
    (status, serde_json::from_slice(&bytes).unwrap())
}
async fn get(f: &Fixture, path: &str) -> (StatusCode, Value) {
    send(
        &f.app(None),
        &f.token,
        "GET",
        path,
        "unused-read-key-123",
        Value::Null,
    )
    .await
}
async fn expire(pool: &PgPool, id: OperationId) {
    sqlx::query("UPDATE operations SET response_expires_at=clock_timestamp()-interval '1 second' WHERE id=$1")
        .bind(id.uuid()).execute(pool).await.unwrap();
}
fn expired(response: (StatusCode, Value), id: &str) {
    assert_eq!(response.0, StatusCode::GONE, "{}", response.1);
    assert_eq!(response.1["code"], "response_expired");
    assert_eq!(response.1["operation_id"], id);
    assert!(response.1.get("result").is_none() && response.1.get("error").is_none());
}

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn terminal_response_expiry_covers_reads_lists_and_execute_retries(pool: PgPool) {
    let f = Fixture::new(&pool).await;
    f.finish().await;
    let before = f.snapshot().await;
    let id = f.operation.to_string();
    let path = format!("/v1/operations/{id}");
    assert_eq!(get(&f, &path).await.0, StatusCode::OK);
    expire(&pool, f.operation).await;
    expired(get(&f, &path).await, &id);
    let list = get(&f, "/v1/operations").await;
    assert_eq!(list.0, StatusCode::OK);
    assert_eq!(list.1["items"][0]["operation_id"], id);
    assert_eq!(list.1["items"][0]["status"], "succeeded");
    assert_eq!(list.1["items"][0]["response_expired"], true);
    assert!(list.1["items"][0].get("result").is_none());
    assert!(list.1["items"][0].get("error").is_none());
    let (key, mut body): (String, Value) =
        sqlx::query_as("SELECT idempotency_key,payload FROM operations WHERE id=$1")
            .bind(f.operation.uuid())
            .fetch_one(&pool)
            .await
            .unwrap();
    let execute = format!("/v1/sandboxes/{}/execute", f.sandbox);
    expired(
        send(&f.app(None), &f.token, "POST", &execute, &key, body.clone()).await,
        &id,
    );
    body["argv"] = json!(["changed"]);
    assert_eq!(
        send(&f.app(None), &f.token, "POST", &execute, &key, body)
            .await
            .0,
        StatusCode::CONFLICT
    );
    assert_eq!(
        get(&f, &format!("{path}/outputs/stdout")).await.1["code"],
        "output_expired"
    );
    assert_eq!(f.snapshot().await, before);
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM operations WHERE project_id=$1")
        .bind(f.project.uuid())
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(count, 1);
    let other = Fixture::new(&pool).await;
    let foreign = get(&other, &path).await;
    let missing = get(
        &other,
        &format!("/v1/operations/{}", OperationId::generate()),
    )
    .await;
    assert_eq!(foreign, missing);
    assert_eq!(foreign.0, StatusCode::NOT_FOUND);
    sqlx::query("UPDATE projects SET api_tokens='[]' WHERE id=$1")
        .bind(f.project.uuid())
        .execute(&pool)
        .await
        .unwrap();
    let revoked = get(&f, &path).await;
    assert_eq!(revoked.0, StatusCode::UNAUTHORIZED);
    assert!(revoked.1.get("operation_id").is_none());
}

fn create_body() -> Value {
    json!({"image_digest":format!("sha256:{}","a".repeat(64)),"resources":{"vcpu":1,"memory_mib":128,"disk_mib":64}})
}
#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn create_and_destroy_retries_keep_identity_after_policy_expiry(pool: PgPool) {
    let f = Fixture::new(&pool).await;
    let app = f.app(None);
    let key = "retention-create-key";
    let body = create_body();
    let admitted = send(&app, &f.token, "POST", "/v1/sandboxes", key, body.clone()).await;
    assert_eq!(admitted.0, StatusCode::ACCEPTED);
    let id: OperationId = admitted.1["operation_id"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap();
    let sandbox = admitted.1["sandbox_id"].as_str().unwrap();
    // Synthetic final create rejection; no allocation was ever reserved.
    sqlx::query("UPDATE operations SET status='failed',completed_at=clock_timestamp()-interval '2 seconds',error='{}' WHERE id=$1")
        .bind(id.uuid()).execute(&pool).await.unwrap();
    sqlx::query("UPDATE sandboxes SET active_transition_operation_id=NULL,desired_state='destroyed',observed_state='destroyed',destroyed_at=clock_timestamp() WHERE id=(SELECT sandbox_id FROM operations WHERE id=$1)")
        .bind(id.uuid()).execute(&pool).await.unwrap();
    assert_eq!(
        f.store
            .assign_response_retention(ResponseRetention::new(1).unwrap())
            .await
            .unwrap(),
        1
    );
    expired(
        send(&app, &f.token, "POST", "/v1/sandboxes", key, body.clone()).await,
        &id.to_string(),
    );
    let mut changed = body.clone();
    changed["name"] = json!("changed");
    assert_eq!(
        send(&app, &f.token, "POST", "/v1/sandboxes", key, changed)
            .await
            .0,
        StatusCode::CONFLICT
    );
    sqlx::query("UPDATE operations SET digest_version=digest_version+1 WHERE id=$1")
        .bind(id.uuid())
        .execute(&pool)
        .await
        .unwrap();
    assert_eq!(
        send(&app, &f.token, "POST", "/v1/sandboxes", key, body)
            .await
            .0,
        StatusCode::CONFLICT
    );
    let path = format!("/v1/sandboxes/{sandbox}/destroy");
    let key = "retention-destroy-key";
    let destroyed = send(&app, &f.token, "POST", &path, key, json!({})).await;
    assert_eq!(destroyed.0, StatusCode::ACCEPTED);
    assert_eq!(destroyed.1["status"], "succeeded");
    let destroy_id: OperationId = destroyed.1["operation_id"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap();
    expire(&pool, destroy_id).await;
    expired(
        send(&app, &f.token, "POST", &path, key, json!({})).await,
        &destroy_id.to_string(),
    );
    assert_eq!(
        send(
            &app,
            &f.token,
            "POST",
            &path,
            key,
            json!({"correlation_id":"changed"})
        )
        .await
        .0,
        StatusCode::CONFLICT
    );
    let fresh = send(
        &app,
        &f.token,
        "POST",
        &path,
        "retention-new-destroy-key",
        json!({}),
    )
    .await;
    assert_eq!(fresh.0, StatusCode::ACCEPTED);
    assert_ne!(fresh.1["operation_id"], destroy_id.to_string());
}

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn unresolved_work_remains_inspectable_and_retries_never_restart_it(pool: PgPool) {
    let f = Fixture::new(&pool).await;
    let (key, body): (String, Value) =
        sqlx::query_as("SELECT idempotency_key,payload FROM operations WHERE id=$1")
            .bind(f.operation.uuid())
            .fetch_one(&pool)
            .await
            .unwrap();
    expire(&pool, f.operation).await;
    for status in ["running", "unknown", "queued"] {
        sqlx::query("UPDATE operations SET status=$2 WHERE id=$1")
            .bind(f.operation.uuid())
            .bind(status)
            .execute(&pool)
            .await
            .unwrap();
        let before = f.snapshot().await;
        let read = get(&f, &format!("/v1/operations/{}", f.operation)).await;
        assert_eq!(read.0, StatusCode::OK);
        assert_eq!(read.1["status"], status);
        assert!(read.1.get("response_expired").is_none());
        let retry = send(
            &f.app(None),
            &f.token,
            "POST",
            &format!("/v1/sandboxes/{}/execute", f.sandbox),
            &key,
            body.clone(),
        )
        .await;
        assert_eq!(retry.0, StatusCode::ACCEPTED);
        assert_eq!(retry.1["operation_id"], f.operation.to_string());
        assert_eq!(f.snapshot().await, before);
    }
}

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn expired_entries_remain_in_paginated_history_without_result_bytes(pool: PgPool) {
    let f = Fixture::new(&pool).await;
    f.finish().await;
    sqlx::query("UPDATE operations SET result=$2,error=$3 WHERE id=$1")
        .bind(f.operation.uuid())
        .bind(json!({"private_result":true}))
        .bind(json!({"private_error":true}))
        .execute(&pool)
        .await
        .unwrap();
    expire(&pool, f.operation).await;
    let admitted = send(
        &f.app(None),
        &f.token,
        "POST",
        "/v1/sandboxes",
        "retention-list-create",
        create_body(),
    )
    .await;
    assert_eq!(admitted.0, StatusCode::ACCEPTED);
    let page = get(&f, "/v1/operations?limit=1").await.1;
    assert_eq!(page["items"][0]["operation_id"], admitted.1["operation_id"]);
    let page = get(
        &f,
        &format!(
            "/v1/operations?limit=1&cursor={}",
            page["next_cursor"].as_str().unwrap()
        ),
    )
    .await
    .1;
    assert_eq!(page["items"][0]["operation_id"], f.operation.to_string());
    assert_eq!(page["items"][0]["response_expired"], true);
    assert!(page["next_cursor"].is_null());
    assert!(!page.to_string().contains("private_result"));
    assert!(!page.to_string().contains("private_error"));
}

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn response_expiry_is_rechecked_after_a_database_lock_wait(pool: PgPool) {
    use std::time::Duration;
    let f = Fixture::new(&pool).await;
    f.finish().await;
    let expires:time::OffsetDateTime=sqlx::query_scalar("UPDATE operations SET response_expires_at=clock_timestamp()+interval '500 milliseconds' WHERE id=$1 RETURNING response_expires_at")
        .bind(f.operation.uuid()).fetch_one(&pool).await.unwrap();
    let mut lock = pool.begin().await.unwrap();
    sqlx::query("LOCK TABLE operations IN ACCESS EXCLUSIVE MODE")
        .execute(&mut *lock)
        .await
        .unwrap();
    let app = f.app(None);
    let token = f.token.clone();
    let path = format!("/v1/operations/{}", f.operation);
    let task = tokio::spawn(async move {
        send(
            &app,
            &token,
            "GET",
            &path,
            "unused-read-key-123",
            Value::Null,
        )
        .await
    });
    tokio::time::timeout(Duration::from_secs(5),async {
        loop {
            // Match the unique read prefix: pg_stat_activity can truncate long SQL.
            let blocked:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock' AND query LIKE 'WITH response_clock AS MATERIALIZED%SELECT id,sandbox_id,%')")
                .fetch_one(&pool).await.unwrap();
            if blocked { break; }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        loop {
            let expired:bool=sqlx::query_scalar("SELECT clock_timestamp()>=$1").bind(expires).fetch_one(&pool).await.unwrap();
            if expired { break; }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }).await.unwrap();
    lock.commit().await.unwrap();
    expired(
        tokio::time::timeout(Duration::from_secs(5), task)
            .await
            .unwrap()
            .unwrap(),
        &f.operation.to_string(),
    );
}

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn compaction_keeps_all_three_mutation_retry_contracts(pool: PgPool) {
    use sandbox_store::compaction::Compaction;
    let f = Fixture::new(&pool).await;
    f.finish().await;
    let (key, command): (String, Value) =
        sqlx::query_as("SELECT idempotency_key,payload FROM operations WHERE id=$1")
            .bind(f.operation.uuid())
            .fetch_one(&pool)
            .await
            .unwrap();
    expire(&pool, f.operation).await;
    assert_eq!(
        f.store.compact_expired_response().await.unwrap(),
        Compaction::Completed(f.operation)
    );
    let path = format!("/v1/sandboxes/{}/execute", f.sandbox);
    expired(
        send(&f.app(None), &f.token, "POST", &path, &key, command.clone()).await,
        &f.operation.to_string(),
    );
    let mut changed = command;
    changed["argv"] = json!(["different"]);
    assert_eq!(
        send(&f.app(None), &f.token, "POST", &path, &key, changed)
            .await
            .0,
        StatusCode::CONFLICT
    );
    expired(
        get(&f, &format!("/v1/operations/{}", f.operation)).await,
        &f.operation.to_string(),
    );
    assert_eq!(
        get(
            &f,
            &format!("/v1/operations/{}/outputs/stdout", f.operation)
        )
        .await
        .1["code"],
        "output_expired"
    );
    assert_eq!(
        get(&f, &format!("/v1/operations/{}/stream", f.operation))
            .await
            .1["code"],
        "output_expired"
    );
    let other = Fixture::new(&pool).await;
    assert_eq!(
        get(&other, &format!("/v1/operations/{}", f.operation))
            .await
            .0,
        StatusCode::NOT_FOUND
    );

    let app = f.app(None);
    let key = "compacted-create-key";
    let body = create_body();
    let created = send(&app, &f.token, "POST", "/v1/sandboxes", key, body.clone()).await;
    assert_eq!(created.0, StatusCode::ACCEPTED);
    let create: OperationId = created.1["operation_id"].as_str().unwrap().parse().unwrap();
    sqlx::query("UPDATE operations SET status='failed',completed_at=clock_timestamp(),error='{}' WHERE id=$1")
        .bind(create.uuid()).execute(&pool).await.unwrap();
    sqlx::query("UPDATE sandboxes SET active_transition_operation_id=NULL,desired_state='destroyed',observed_state='destroyed',destroyed_at=clock_timestamp() WHERE id=(SELECT sandbox_id FROM operations WHERE id=$1)")
        .bind(create.uuid()).execute(&pool).await.unwrap();
    expire(&pool, create).await;
    assert_eq!(
        f.store.compact_expired_response().await.unwrap(),
        Compaction::Completed(create)
    );
    expired(
        send(&app, &f.token, "POST", "/v1/sandboxes", key, body).await,
        &create.to_string(),
    );
    let path = format!(
        "/v1/sandboxes/{}/destroy",
        created.1["sandbox_id"].as_str().unwrap()
    );
    let key = "compacted-destroy-key";
    let destroyed = send(&app, &f.token, "POST", &path, key, json!({})).await;
    assert_eq!(destroyed.0, StatusCode::ACCEPTED);
    let destroy: OperationId = destroyed.1["operation_id"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap();
    expire(&pool, destroy).await;
    assert_eq!(
        f.store.compact_expired_response().await.unwrap(),
        Compaction::Completed(destroy)
    );
    expired(
        send(&app, &f.token, "POST", &path, key, json!({})).await,
        &destroy.to_string(),
    );
    let cleared:i64=sqlx::query_scalar("SELECT count(*) FROM operations WHERE project_id=$1 AND payload_compacted_at IS NOT NULL AND payload='{}' AND result IS NULL AND error IS NULL")
        .bind(f.project.uuid()).fetch_one(&pool).await.unwrap();
    assert_eq!(cleared, 3);
}
