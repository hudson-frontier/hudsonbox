//! Public file admission, PostgreSQL claims and real mTLS to the simulated supervisor.
#![allow(clippy::unwrap_used, clippy::expect_used)]
#[allow(dead_code)]
mod common;
#[path = "common/sources.rs"]
mod sources;
use axum::{
    Router,
    body::{Body, to_bytes},
};
use common::Fixture;
use http::{Request, StatusCode};
use sandbox_controller::Controller;
use sandbox_protocol::{Id, OperationId, SandboxId};
use sandbox_store::claims::OperationKind;
use sandbox_store::uploads::UploadAction;
use serde_json::Value;
use sha2::{Digest, Sha256};
use sources::Sources;
use sqlx::PgPool;
use std::{
    sync::{Arc, atomic::Ordering},
    time::Duration,
};
use tower::ServiceExt;
static TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());
async fn setup(pool: &PgPool) -> (Fixture, Arc<Sources>, Controller, SandboxId) {
    let mut f = Fixture::new(pool).await;
    let sources = Arc::new(Sources::default());
    f.app = sandbox_api::router_with_uploads(
        sandbox_api::AppState {
            store: f.store.clone(),
            images: sandbox_protocol::images::ImageAllowlist::new(f.config.allowed_images.clone())
                .unwrap(),
        },
        None,
        None,
        None,
        Some(sources.clone()),
    );
    let (_, sandbox) = f.admit().await;
    let mut c = f.controller().await.with_file_sources(sources.clone());
    c.tick().await.unwrap();
    (f, sources, c, sandbox)
}
async fn put(
    app: Router,
    token: String,
    s: SandboxId,
    key: String,
    bytes: Vec<u8>,
) -> (StatusCode, Value) {
    let request = Request::builder()
        .method("PUT")
        .uri(format!("/v1/sandboxes/{s}/files?path=uploaded.bin"))
        .header("authorization", format!("Bearer {token}"))
        .header("content-type", "application/octet-stream")
        .header("idempotency-key", key)
        .header("x-file-size", bytes.len())
        .header("x-file-sha256", hex::encode(Sha256::digest(&bytes)))
        .body(Body::from(bytes))
        .unwrap();
    let r = app.oneshot(request).await.unwrap();
    let status = r.status();
    assert_eq!(r.headers()["cache-control"], "no-store");
    (
        status,
        serde_json::from_slice(&to_bytes(r.into_body(), 65536).await.unwrap()).unwrap(),
    )
}
async fn admit(f: &Fixture, s: SandboxId, bytes: Vec<u8>) -> (String, Value) {
    let key = OperationId::generate().to_string();
    let (status, body) = put(f.app.clone(), f.token.clone(), s, key.clone(), bytes).await;
    assert_eq!(status, StatusCode::ACCEPTED, "{body}");
    (key, body)
}
async fn tick(f: &Fixture, c: &mut Controller) {
    sqlx::query("UPDATE operations SET next_retry_at=NULL WHERE completed_at IS NULL")
        .execute(f.store.pool())
        .await
        .unwrap();
    c.tick().await.unwrap();
}
async fn state(f: &Fixture, id: &str) -> Value {
    f.send("GET", &format!("/v1/operations/{id}"), Value::Null)
        .await
        .1
}
async fn settle(f: &Fixture, c: &mut Controller, id: &str) -> Value {
    for _ in 0..300 {
        tick(f, c).await;
        let r = state(f, id).await;
        if matches!(r["status"].as_str(), Some("succeeded" | "failed")) {
            return r;
        }
    }
    panic!("upload did not settle: {}", state(f, id).await)
}
#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn binary_empty_and_maximum_files_preserve_one_operation_and_one_commit(pool: PgPool) {
    let _guard = TEST_LOCK.lock().await;
    let (f, sources, mut c, s) = setup(&pool).await;
    for bytes in [
        vec![],
        (0..65539).map(|n| (n % 251) as u8).collect(),
        vec![0xfe; 8 * 1024 * 1024],
    ] {
        let before = f.fake.total_file_commits().await;
        let reads = sources.reads.load(Ordering::SeqCst);
        let (key, a) = admit(&f, s, bytes.clone()).await;
        let id = a["operation_id"].as_str().unwrap();
        assert_eq!(
            put(
                f.app.clone(),
                f.token.clone(),
                s,
                key.clone(),
                bytes.clone()
            )
            .await
            .1["operation_id"],
            id
        );
        let result = settle(&f, &mut c, id).await;
        assert_eq!(result["status"], "succeeded", "{result}");
        assert_eq!(result["result"]["guest_reported"], true);
        assert_eq!(result["result"]["simulated"], true);
        assert_eq!(
            result["result"]["sha256"],
            hex::encode(Sha256::digest(&bytes))
        );
        assert_eq!(f.fake.total_file_commits().await, before + 1);
        assert_eq!(
            sources.reads.load(Ordering::SeqCst) - reads,
            usize::from(!bytes.is_empty())
        );
        assert_eq!(
            put(f.app.clone(), f.token.clone(), s, key.clone(), bytes)
                .await
                .1["operation_id"],
            id
        );
        assert_eq!(
            put(f.app.clone(), f.token.clone(), s, key, b"changed".to_vec())
                .await
                .0,
            StatusCode::CONFLICT
        );
    }
}
#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn concurrent_retries_and_lost_source_begin_chunk_commit_replies_reconcile(pool: PgPool) {
    let _guard = TEST_LOCK.lock().await;
    let (f, sources, mut c, s) = setup(&pool).await;
    sources.lose_upload.store(true, Ordering::SeqCst);
    let key = OperationId::generate().to_string();
    let mut jobs = Vec::new();
    for _ in 0..4 {
        jobs.push(tokio::spawn(put(
            f.app.clone(),
            f.token.clone(),
            s,
            key.clone(),
            vec![4; 65539],
        )));
    }
    let mut id = String::new();
    for job in jobs {
        let (status, a) = job.await.unwrap();
        assert_eq!(status, StatusCode::ACCEPTED, "{a}");
        if id.is_empty() {
            id = a["operation_id"].as_str().unwrap().into()
        } else {
            assert_eq!(a["operation_id"], id)
        }
    }
    let mut dropped = std::collections::HashSet::new();
    for _ in 0..30 {
        let row: (bool, bool, i64, bool, bool) = sqlx::query_as(
            "SELECT begin_requested,commit_requested,written,needs_inspect,source_ref IS NOT NULL FROM file_uploads WHERE operation_id=$1"
        ).bind(id.parse::<OperationId>().unwrap().uuid()).fetch_one(&pool).await.unwrap();
        let phase = if !row.0 && row.4 {
            "begin"
        } else if row.0 && !row.3 && row.2 < 65539 {
            "write"
        } else if row.0 && !row.3 && row.2 == 65539 && !row.1 {
            "commit"
        } else {
            "inspect"
        };
        if phase != "inspect" && dropped.insert(phase) {
            f.fake.lose_next_file_reply().await;
        }
        tick(&f, &mut c).await;
        if state(&f, &id).await["status"] == "succeeded" {
            break;
        }
    }
    assert_eq!(dropped.len(), 3);
    assert_eq!(state(&f, &id).await["status"], "succeeded");
    assert_eq!(f.fake.total_file_commits().await, 1);
    let mut replacement = f.controller().await.with_file_sources(sources);
    tick(&f, &mut replacement).await;
    assert_eq!(f.fake.total_file_commits().await, 1);
    let n: i64 = sqlx::query_scalar("SELECT count(*) FROM file_uploads")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(n, 1);
}
#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn lost_commit_request_is_not_republished_and_can_abort_after_deadline(pool: PgPool) {
    let _guard = TEST_LOCK.lock().await;
    let (f, sources, mut c, s) = setup(&pool).await;
    let (_, a) = admit(&f, s, b"data".to_vec()).await;
    let id = a["operation_id"].as_str().unwrap();
    loop {
        tick(&f, &mut c).await;
        let written: i64 = sqlx::query_scalar("SELECT written FROM file_uploads")
            .fetch_one(&pool)
            .await
            .unwrap();
        if written == 4 {
            break;
        }
    }
    sqlx::query("UPDATE operations SET next_retry_at=NULL WHERE kind='file_write'")
        .execute(&pool)
        .await
        .unwrap();
    let claim = f
        .store
        .claim_next(OperationKind::FileWrite, 30)
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        f.store
            .prepare_upload(&claim, f.config.host, 1)
            .await
            .unwrap(),
        UploadAction::Commit(_)
    ));
    // Durable commit intent exists, but the RPC is lost before reaching the host.
    f.reclaim_now().await;
    let mut replacement = f.controller().await.with_file_sources(sources);
    for _ in 0..4 {
        tick(&f, &mut replacement).await;
    }
    assert_eq!(f.fake.total_file_commits().await, 0);
    assert_ne!(state(&f, id).await["status"], "succeeded");
    sqlx::query("UPDATE operations SET deadline=clock_timestamp()-interval '1 second' WHERE kind='file_write'").execute(&pool).await.unwrap();
    let result = settle(&f, &mut replacement, id).await;
    assert_eq!(result["phase"], "file_aborted");
    assert_eq!(f.fake.total_file_commits().await, 0);
}
#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn revocation_during_source_read_prevents_chunks_and_triggers_abort(pool: PgPool) {
    let _guard = TEST_LOCK.lock().await;
    let (f, sources, mut c, s) = setup(&pool).await;
    let (_, a) = admit(&f, s, b"private".to_vec()).await;
    let id = a["operation_id"].as_str().unwrap().to_owned();
    tick(&f, &mut c).await;
    tick(&f, &mut c).await;
    sources.pause_read.store(true, Ordering::SeqCst);
    sqlx::query("UPDATE operations SET next_retry_at=NULL WHERE kind='file_write'")
        .execute(&pool)
        .await
        .unwrap();
    let task = tokio::spawn(async move {
        c.tick().await.unwrap();
        c
    });
    tokio::time::timeout(Duration::from_secs(3), sources.started.notified())
        .await
        .unwrap();
    sqlx::query("UPDATE projects SET status='suspended'")
        .execute(&pool)
        .await
        .unwrap();
    sources.resume.add_permits(1);
    let mut c = task.await.unwrap();
    tick(&f, &mut c).await;
    let row: (String, i64) = sqlx::query_as(
        "SELECT o.status,f.written FROM operations o JOIN file_uploads f ON f.operation_id=o.id",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(row, ("failed".into(), 0));
    assert_eq!(f.fake.total_file_commits().await, 0);
    assert_eq!(state(&f, &id).await["code"], "unauthenticated");
}
#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn suspended_during_ingestion_keeps_private_source_but_never_dispatches(pool: PgPool) {
    let _guard = TEST_LOCK.lock().await;
    let (f, sources, mut c, s) = setup(&pool).await;
    sources.pause_upload.store(true, Ordering::SeqCst);
    let task = tokio::spawn(put(
        f.app.clone(),
        f.token.clone(),
        s,
        OperationId::generate().to_string(),
        b"data".to_vec(),
    ));
    tokio::time::timeout(Duration::from_secs(3), sources.started.notified())
        .await
        .unwrap();
    sqlx::query("UPDATE projects SET status='suspended'")
        .execute(&pool)
        .await
        .unwrap();
    sources.resume.add_permits(1);
    assert_eq!(task.await.unwrap().0, StatusCode::UNAUTHORIZED);
    tick(&f, &mut c).await;
    let begun: bool = sqlx::query_scalar("SELECT begin_requested FROM file_uploads")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert!(!begun);
    assert_eq!(f.fake.total_file_commits().await, 0);
}

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn malformed_metadata_and_cross_project_requests_create_no_source(pool: PgPool) {
    let _guard = TEST_LOCK.lock().await;
    let (f, sources, _c, s) = setup(&pool).await;
    for (suffix, header, value, body, expected) in [
        (
            "path=../escape",
            "x-file-size",
            "4",
            b"data".to_vec(),
            StatusCode::BAD_REQUEST,
        ),
        (
            "path=x&extra=y",
            "x-file-size",
            "4",
            b"data".to_vec(),
            StatusCode::BAD_REQUEST,
        ),
        (
            "path=x&path=y",
            "x-file-size",
            "4",
            b"data".to_vec(),
            StatusCode::BAD_REQUEST,
        ),
        (
            "path=x",
            "x-file-size",
            "3",
            b"data".to_vec(),
            StatusCode::BAD_REQUEST,
        ),
        (
            "path=x",
            "x-file-size",
            "8388609",
            vec![],
            StatusCode::PAYLOAD_TOO_LARGE,
        ),
        (
            "path=x",
            "x-file-sha256",
            "bad",
            b"data".to_vec(),
            StatusCode::BAD_REQUEST,
        ),
        (
            "path=x",
            "x-file-mode",
            "0777",
            b"data".to_vec(),
            StatusCode::BAD_REQUEST,
        ),
        (
            "path=x",
            "content-encoding",
            "gzip",
            b"data".to_vec(),
            StatusCode::BAD_REQUEST,
        ),
        (
            "path=x",
            "range",
            "bytes=0-1",
            b"data".to_vec(),
            StatusCode::BAD_REQUEST,
        ),
        (
            "path=x",
            "content-type",
            "application/json",
            b"data".to_vec(),
            StatusCode::BAD_REQUEST,
        ),
        (
            "path=x",
            "x-file-size",
            "4",
            b"nope".to_vec(),
            StatusCode::BAD_REQUEST,
        ),
        (
            "path=x",
            "x-file-size",
            "4",
            vec![0; 8388609],
            StatusCode::PAYLOAD_TOO_LARGE,
        ),
    ] {
        let mut req = Request::builder()
            .method("PUT")
            .uri(format!("/v1/sandboxes/{s}/files?{suffix}"))
            .header("authorization", format!("Bearer {}", f.token))
            .header("idempotency-key", OperationId::generate().to_string())
            .header("content-type", "application/octet-stream")
            .header("x-file-size", "4")
            .header("x-file-sha256", hex::encode(Sha256::digest(b"data")))
            .body(Body::from(body))
            .unwrap();
        req.headers_mut().insert(
            http::HeaderName::from_bytes(header.as_bytes()).unwrap(),
            value.parse().unwrap(),
        );
        assert_eq!(
            f.app.clone().oneshot(req).await.unwrap().status(),
            expected,
            "{suffix} {header}"
        );
    }
    let other = sandbox_protocol::ProjectToken::generate().unwrap();
    sqlx::query("INSERT INTO projects(id,name,status,limits,api_tokens) VALUES($1,'other','active','{}',$2)")
        .bind(uuid::Uuid::now_v7()).bind(serde_json::json!([{"key_id":other.key_id().as_str(),"hash":hex::encode(other.hash().as_bytes())}])).execute(&pool).await.unwrap();
    assert_eq!(
        put(
            f.app.clone(),
            other.render_once(),
            s,
            OperationId::generate().to_string(),
            b"data".to_vec()
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM file_uploads")
            .fetch_one(&pool)
            .await
            .unwrap(),
        0
    );
    assert!(sources.data.lock().await.is_empty());
}

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn retained_capacity_counts_empty_files_and_retries_survive_destruction(pool: PgPool) {
    let _guard = TEST_LOCK.lock().await;
    let (f, _sources, mut c, s) = setup(&pool).await;
    let mut original = None;
    for _ in 0..16 {
        let (key, a) = admit(&f, s, vec![]).await;
        assert_eq!(
            settle(&f, &mut c, a["operation_id"].as_str().unwrap()).await["status"],
            "succeeded"
        );
        original.get_or_insert((key, a));
    }
    assert_eq!(
        put(
            f.app.clone(),
            f.token.clone(),
            s,
            OperationId::generate().to_string(),
            vec![]
        )
        .await
        .0,
        StatusCode::CONFLICT
    );
    let (key, a) = original.unwrap();
    let (status, destroy) = f
        .send(
            "POST",
            &format!("/v1/sandboxes/{s}/destroy"),
            serde_json::json!({}),
        )
        .await;
    assert_eq!(status, StatusCode::ACCEPTED);
    assert_eq!(
        settle(&f, &mut c, destroy["operation_id"].as_str().unwrap()).await["status"],
        "succeeded"
    );
    assert_eq!(
        put(f.app.clone(), f.token.clone(), s, key, vec![]).await.1["operation_id"],
        a["operation_id"]
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM file_uploads")
            .fetch_one(&pool)
            .await
            .unwrap(),
        16
    );
}

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn missing_source_expires_without_a_guest_attempt(pool: PgPool) {
    let _guard = TEST_LOCK.lock().await;
    let (f, sources, mut c, s) = setup(&pool).await;
    sources.fail_upload.store(true, Ordering::SeqCst);
    let (_, a) = admit(&f, s, b"data".to_vec()).await;
    let id = a["operation_id"].as_str().unwrap();
    tick(&f, &mut c).await;
    assert_eq!(state(&f, id).await["phase"], "awaiting_source");
    sqlx::query("UPDATE operations SET deadline=clock_timestamp()-interval '1 second' WHERE kind='file_write'").execute(&pool).await.unwrap();
    assert_eq!(settle(&f, &mut c, id).await["phase"], "file_not_started");
    assert_eq!(
        sqlx::query_scalar::<_, i32>(
            "SELECT attempt_count FROM operations WHERE kind='file_write'"
        )
        .fetch_one(&pool)
        .await
        .unwrap(),
        0
    );
    assert_eq!(f.fake.total_file_commits().await, 0);
}

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn original_token_hash_claim_and_source_identity_remain_authoritative(pool: PgPool) {
    let _guard = TEST_LOCK.lock().await;
    let (f, _sources, mut c, s) = setup(&pool).await;
    let (_, a) = admit(&f, s, b"data".to_vec()).await;
    tick(&f, &mut c).await; // retain source, no Begin yet
    f.reclaim_now().await;
    let stale = f
        .store
        .claim_next(OperationKind::FileWrite, 30)
        .await
        .unwrap()
        .unwrap();
    f.reclaim_now().await;
    let live = f
        .store
        .claim_next(OperationKind::FileWrite, 30)
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        f.store.prepare_upload(&stale, f.config.host, 1).await,
        Err(sandbox_store::dispatch::DispatchError::LostClaim)
    ));
    sqlx::query("UPDATE operations SET payload=jsonb_set(payload,'{path}','\"different\"') WHERE kind='file_write'").execute(&pool).await.unwrap();
    assert!(matches!(
        f.store.prepare_upload(&live, f.config.host, 1).await,
        Err(sandbox_store::dispatch::DispatchError::InvalidData)
    ));
    sqlx::query("UPDATE operations SET payload=jsonb_set(payload,'{path}','\"uploaded.bin\"') WHERE kind='file_write'").execute(&pool).await.unwrap();
    let reference: Value = sqlx::query_scalar("SELECT source_ref FROM file_uploads")
        .fetch_one(&pool)
        .await
        .unwrap();
    sqlx::query("UPDATE file_uploads SET source_ref=jsonb_set(source_ref,'{plan,upload,path}','\"different\"')").execute(&pool).await.unwrap();
    assert!(matches!(
        f.store.prepare_upload(&live, f.config.host, 1).await,
        Err(sandbox_store::dispatch::DispatchError::InvalidData)
    ));
    sqlx::query("UPDATE file_uploads SET source_ref=$1")
        .bind(reference)
        .execute(&pool)
        .await
        .unwrap();
    // Replacing a token hash under the same key id must not revive old authority.
    sqlx::query(
        "UPDATE projects SET api_tokens=jsonb_set(api_tokens,'{0,hash}',to_jsonb(repeat('f',64)))",
    )
    .execute(&pool)
    .await
    .unwrap();
    assert!(matches!(
        f.store
            .prepare_upload(&live, f.config.host, 1)
            .await
            .unwrap(),
        UploadAction::Rejected
    ));
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT status FROM operations WHERE id=$1")
            .bind(
                a["operation_id"]
                    .as_str()
                    .unwrap()
                    .parse::<OperationId>()
                    .unwrap()
                    .uuid()
            )
            .fetch_one(&pool)
            .await
            .unwrap(),
        "failed"
    );
    assert_eq!(f.fake.total_file_commits().await, 0);
}

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn https_binary_upload_exceeds_json_limit_but_remains_bounded(pool: PgPool) {
    let _guard = TEST_LOCK.lock().await;
    let (f, sources, mut c, s) = setup(&pool).await;
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    let acceptor = sandbox_api::server::tls_acceptor(
        cert.cert.pem().as_bytes(),
        cert.signing_key.serialize_pem().as_bytes(),
    )
    .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!(
        "https://localhost:{}/v1/sandboxes/{s}/files?path=https.bin",
        listener.local_addr().unwrap().port()
    );
    let client = reqwest::Client::builder()
        .no_proxy()
        .https_only(true)
        .add_root_certificate(reqwest::Certificate::from_pem(cert.cert.pem().as_bytes()).unwrap())
        .timeout(Duration::from_secs(20))
        .build()
        .unwrap();
    let (stop, signal) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(sandbox_api::server::serve(
        listener,
        acceptor,
        f.app.clone(),
        sandbox_api::server::ServerLimits::default(),
        async {
            let _ = signal.await;
        },
    ));
    let bytes = vec![0xff; 65539];
    let res = client
        .put(&url)
        .bearer_auth(&f.token)
        .header("idempotency-key", OperationId::generate().to_string())
        .header("content-type", "application/octet-stream")
        .header("x-file-size", bytes.len())
        .header("x-file-sha256", hex::encode(Sha256::digest(&bytes)))
        .body(bytes)
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::ACCEPTED);
    let a: Value = serde_json::from_slice(&res.bytes().await.unwrap()).unwrap();
    assert_eq!(
        settle(&f, &mut c, a["operation_id"].as_str().unwrap()).await["status"],
        "succeeded"
    );
    let res = client
        .put(&url)
        .bearer_auth(&f.token)
        .header("idempotency-key", OperationId::generate().to_string())
        .header("content-type", "application/octet-stream")
        .header("x-file-size", 8388608)
        .header("x-file-sha256", hex::encode(Sha256::digest([])))
        .body(vec![0; 8388609])
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::PAYLOAD_TOO_LARGE);
    assert_eq!(sources.data.lock().await.len(), 1);
    stop.send(()).unwrap();
    server.await.unwrap().unwrap();
}

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn unsolicited_stale_or_foreign_file_observations_cannot_complete_an_upload(pool: PgPool) {
    let _guard = TEST_LOCK.lock().await;
    let (f, _sources, mut c, s) = setup(&pool).await;
    admit(&f, s, b"data".to_vec()).await;
    tick(&f, &mut c).await;
    tick(&f, &mut c).await; // Source then Begin
    f.reclaim_now().await;
    let claim = f
        .store
        .claim_next(OperationKind::FileWrite, 30)
        .await
        .unwrap()
        .unwrap();
    let UploadAction::Write { request, .. } = f
        .store
        .prepare_upload(&claim, f.config.host, 1)
        .await
        .unwrap()
    else {
        panic!("expected write")
    };
    let record: Value = sqlx::query_scalar("SELECT record FROM file_uploads")
        .fetch_one(&pool)
        .await
        .unwrap();
    let record: sandbox_protocol::supervisor_files::FileRecord =
        serde_json::from_value(record).unwrap();
    let o = record.observation(
        request.ownership.unwrap(),
        true,
        (time::OffsetDateTime::now_utc().unix_timestamp_nanos() / 1_000_000) as i64,
        None,
    );
    for mut bad in [
        o.clone(),
        o.clone(),
        o.clone(),
        o.clone(),
        o.clone(),
        o.clone(),
    ]
    .into_iter()
    .enumerate()
    {
        match bad.0 {
            0 => bad.1.state = 3, // no commit intent
            1 => bad.1.ownership.as_mut().unwrap().claim_revision += 1,
            2 => bad.1.context.as_mut().unwrap().boot_id = "other-boot".into(),
            3 => bad.1.upload_digest[0] ^= 1,
            4 => bad.1.observed_unix_ms -= 20000,
            _ => bad.1.stored = Some(4), // an inspection has no write acknowledgement
        }
        assert!(matches!(
            f.store.observe_upload(&claim, &bad.1, None, true).await,
            Err(sandbox_store::dispatch::DispatchError::BadEvidence)
        ));
    }
    assert!(matches!(
        f.store.observe_upload(&claim, &o, None, false).await,
        Err(sandbox_store::dispatch::DispatchError::SimulationDenied)
    ));
    f.store
        .observe_upload(&claim, &o, None, true)
        .await
        .unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT status FROM operations WHERE kind='file_write'")
            .fetch_one(&pool)
            .await
            .unwrap(),
        "running"
    );
}

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn body_deadline_and_four_ingestions_bound_api_memory(pool: PgPool) {
    let _guard = TEST_LOCK.lock().await;
    let (f, sources, _c, s) = setup(&pool).await;
    let req = Request::builder()
        .method("PUT")
        .uri(format!("/v1/sandboxes/{s}/files?path=x"))
        .header("authorization", format!("Bearer {}", f.token))
        .header("content-type", "application/octet-stream")
        .header("idempotency-key", OperationId::generate().to_string())
        .header("x-file-size", 4)
        .header("x-file-sha256", hex::encode(Sha256::digest(b"data")))
        .body(Body::from_stream(tokio_stream::pending::<
            Result<axum::body::Bytes, std::io::Error>,
        >()))
        .unwrap();
    let started = std::time::Instant::now();
    let response = tokio::time::timeout(Duration::from_secs(15), f.app.clone().oneshot(req))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert!(started.elapsed() >= Duration::from_secs(9));
    assert!(sources.data.lock().await.is_empty());
    sources.pause_upload.store(true, Ordering::SeqCst);
    let key = OperationId::generate().to_string();
    let mut tasks = Vec::new();
    for _ in 0..4 {
        tasks.push(tokio::spawn(put(
            f.app.clone(),
            f.token.clone(),
            s,
            key.clone(),
            b"data".to_vec(),
        )));
        tokio::time::timeout(Duration::from_secs(3), sources.started.notified())
            .await
            .unwrap();
    }
    assert_eq!(
        put(
            f.app.clone(),
            f.token.clone(),
            s,
            key.clone(),
            b"data".to_vec()
        )
        .await
        .0,
        StatusCode::SERVICE_UNAVAILABLE
    );
    sources.resume.add_permits(4);
    for task in tasks {
        assert_eq!(task.await.unwrap().0, StatusCode::ACCEPTED);
    }
    assert_eq!(sources.data.lock().await.len(), 1);
}

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn expiry_during_host_lock_wait_rolls_back_begin_intent(pool: PgPool) {
    let _guard = TEST_LOCK.lock().await;
    let (f, _sources, mut c, s) = setup(&pool).await;
    admit(&f, s, b"data".to_vec()).await;
    tick(&f, &mut c).await;
    f.reclaim_now().await;
    let claim = f
        .store
        .claim_next(OperationKind::FileWrite, 30)
        .await
        .unwrap()
        .unwrap();
    let mut locked = pool.begin().await.unwrap();
    sqlx::query("SELECT id FROM hosts FOR UPDATE")
        .execute(&mut *locked)
        .await
        .unwrap();
    sqlx::query("UPDATE projects SET api_tokens=jsonb_set(api_tokens,'{0,expires_at}',to_jsonb(clock_timestamp()+interval '300 milliseconds'))").execute(&pool).await.unwrap();
    let store = f.store.clone();
    let host = f.config.host;
    let job = tokio::spawn(async move { store.prepare_upload(&claim, host, 1).await });
    tokio::time::sleep(Duration::from_millis(500)).await;
    locked.commit().await.unwrap();
    assert!(matches!(
        job.await.unwrap().unwrap(),
        UploadAction::Rejected
    ));
    assert!(
        !sqlx::query_scalar::<_, bool>("SELECT begin_requested FROM file_uploads")
            .fetch_one(&pool)
            .await
            .unwrap()
    );
    assert_eq!(f.fake.total_file_commits().await, 0);
}

#[path = "common/source_cleanup.rs"]
mod cleanup;

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn worker_pool_maintains_and_destroys_other_sandboxes_during_a_blocked_source_read(
    pool: PgPool,
) {
    let _guard = TEST_LOCK.lock().await;
    let (f, sources, mut c, s) = setup(&pool).await;
    let (_, upload) = admit(&f, s, b"blocked data".to_vec()).await;
    let upload_id = upload["operation_id"].as_str().unwrap();
    tick(&f, &mut c).await; // Source
    tick(&f, &mut c).await; // Begin
    sources.pause_read.store(true, Ordering::SeqCst);
    let mut workers = sandbox_controller::workers::WorkerPool::start(&c, 2).unwrap();
    tokio::time::timeout(Duration::from_secs(3), sources.started.notified())
        .await
        .unwrap();

    // The read remains blocked for the whole assertion window. Another worker
    // must renew/inspect the first sandbox and create then destroy the second.
    sqlx::query("UPDATE allocations SET maintenance_next_at=NULL,maintenance_lease_until=NULL")
        .execute(&pool)
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        let (created, second) = f.admit().await;
        wait_for_success(&f, &created.to_string()).await;
        let (status, destroy) = f
            .send(
                "POST",
                &format!("/v1/sandboxes/{second}/destroy"),
                serde_json::json!({}),
            )
            .await;
        assert_eq!(status, StatusCode::ACCEPTED, "{destroy}");
        wait_for_success(&f, destroy["operation_id"].as_str().unwrap()).await;
    })
    .await
    .unwrap();
    assert_ne!(state(&f, upload_id).await["status"], "succeeded");
    assert_eq!(f.fake.total_file_commits().await, 0);
    assert_eq!(f.fake.total_starts().await, 2);
    assert!(
        sqlx::query_scalar::<_, bool>(
            "SELECT lease_observation IS NOT NULL FROM allocations WHERE released_at IS NULL"
        )
        .fetch_one(&pool)
        .await
        .unwrap()
    );

    sources.pause_read.store(false, Ordering::SeqCst);
    sources.resume.add_permits(1);
    tokio::time::timeout(Duration::from_secs(5), wait_for_success(&f, upload_id))
        .await
        .unwrap();
    assert_eq!(f.fake.total_file_commits().await, 1);
    workers.shutdown().await;
}

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn worker_pool_drain_deadline_cancels_blocked_reads_and_recovers_without_republishing(
    pool: PgPool,
) {
    let _guard = TEST_LOCK.lock().await;
    let (f, sources, mut c, s) = setup(&pool).await;
    assert!(matches!(
        sandbox_controller::workers::WorkerPool::start(&c, 0),
        Err(sandbox_controller::ControllerError::InvalidWorkerCount)
    ));
    assert!(matches!(
        sandbox_controller::workers::WorkerPool::start(&c, 17),
        Err(sandbox_controller::ControllerError::InvalidWorkerCount)
    ));
    let (_, upload) = admit(&f, s, b"recover data".to_vec()).await;
    let upload_id = upload["operation_id"].as_str().unwrap();
    tick(&f, &mut c).await;
    tick(&f, &mut c).await;
    sources.pause_read.store(true, Ordering::SeqCst);
    let mut workers = sandbox_controller::workers::WorkerPool::start(&c, 1).unwrap();
    tokio::time::timeout(Duration::from_secs(3), sources.started.notified())
        .await
        .unwrap();
    assert!(
        !tokio::time::timeout(
            Duration::from_secs(2),
            workers.drain(Duration::from_millis(50)),
        )
        .await
        .unwrap()
        .unwrap()
    );
    drop(workers);
    assert_eq!(f.fake.total_file_commits().await, 0);
    assert_eq!(sources.reads.load(Ordering::SeqCst), 1);
    assert_ne!(state(&f, upload_id).await["status"], "succeeded");

    // Only normal claim recovery permits the replacement pool to continue.
    sources.pause_read.store(false, Ordering::SeqCst);
    f.reclaim_now().await;
    let mut replacement = sandbox_controller::workers::WorkerPool::start(&c, 2).unwrap();
    tokio::time::timeout(Duration::from_secs(5), wait_for_success(&f, upload_id))
        .await
        .unwrap();
    replacement.shutdown().await;
    assert_eq!(f.fake.total_file_commits().await, 1);
    assert_eq!(f.fake.total_starts().await, 1);
}

async fn wait_for_success(f: &Fixture, id: &str) {
    loop {
        let result = state(f, id).await;
        assert_ne!(result["status"], "failed", "{result}");
        if result["status"] == "succeeded" {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}
