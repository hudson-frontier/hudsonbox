//! Real CLI -> TLS -> API -> PostgreSQL. Guest/output/file adapters are synthetic.
#![allow(clippy::unwrap_used, clippy::expect_used)]
#[path = "../../sandbox-api/tests/support/contract_backends.rs"]
mod backends;
#[path = "../../sandbox-api/tests/support/output.rs"]
mod fixture;
#[path = "../../sandbox-client/tests/support/server.rs"]
mod server;
use fixture::Fixture;
use sandbox_protocol::Id;
use serde_json::{Value, json};
use server::{Server, private};
use sqlx::PgPool;
use std::{path::Path, sync::Arc, time::Duration};
fn path(path: &Path) -> &str {
    path.to_str().unwrap()
}
fn command() -> tokio::process::Command {
    if let Some(binary) = std::env::var_os("HUDSON_TEST_OPERATOR_CLI") {
        let mut command = tokio::process::Command::new(binary);
        command.arg("sandbox");
        command
    } else {
        tokio::process::Command::new(env!("CARGO_BIN_EXE_sandbox-api-test-client"))
    }
}
async fn cli(s: &Server, args: &[&str], expected: i32) -> Vec<u8> {
    let output = tokio::time::timeout(
        Duration::from_secs(5),
        command()
            .args(["--config", path(&s.config), "--json"])
            .args(args)
            .output(),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(
        output.status.code(),
        Some(expected),
        "args={args:?} stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(!String::from_utf8_lossy(&output.stderr).contains("test-private-credential"));
    if expected == 0 {
        assert!(
            output.stderr.is_empty(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    output.stdout
}
async fn value(s: &Server, args: &[&str]) -> Value {
    serde_json::from_slice(&cli(s, args, 0).await).unwrap()
}
fn credential(s: &Server, f: &Fixture) {
    private(&s.directory.path().join("credential.json"),&serde_json::to_vec(&json!({"version":1,"project_id":f.project.to_string(),"name":"fixture","token":f.token,"created_at":1,"expires_at":4102444800_i64})).unwrap());
}
async fn canonical(pool: &PgPool, f: &Fixture, digest: &str) {
    sqlx::query(
        "UPDATE sandboxes SET image_digest=$1,resources=$2,observation_simulated=true WHERE id=$3",
    )
    .bind(digest)
    .bind(json!({"vcpu":1,"memory_mib":128,"disk_mib":64}))
    .bind(f.sandbox.uuid())
    .execute(pool)
    .await
    .unwrap();
}
#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn cli_forwards_generation_preconditions_and_keeps_guarded_retry_identity(pool: PgPool) {
    let f = Fixture::new(&pool).await;
    f.finish().await;
    let server = Server::start(f.app(None)).await;
    credential(&server, &f);
    let sandbox = f.sandbox.to_string();
    let request = server.directory.path().join("guarded-execute.json");
    let now = (time::OffsetDateTime::now_utc().unix_timestamp_nanos() / 1_000_000) as i64;
    let mut body = json!({"argv":["true"],"deadline_unix_ms":now+60000,"expected_generation":2});
    private(&request, &serde_json::to_vec(&body).unwrap());
    cli(
        &server,
        &[
            "execute",
            &sandbox,
            "--request",
            path(&request),
            "--key",
            "cli-stale-generation-01",
        ],
        8,
    )
    .await;
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM operations WHERE project_id=$1")
        .bind(f.project.uuid())
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(count, 1);
    body["expected_generation"] = json!(1);
    private(&request, &serde_json::to_vec(&body).unwrap());
    let args = [
        "execute",
        &sandbox,
        "--request",
        path(&request),
        "--key",
        "cli-matching-generation-01",
    ];
    let accepted = value(&server, &args).await;
    assert_eq!(accepted, value(&server, &args).await);
    let payload: Value =
        sqlx::query_scalar("SELECT payload FROM operations WHERE idempotency_key=$1")
            .bind("cli-matching-generation-01")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(payload["expected_generation"], 1);
}

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn cli_all_routes_preserve_keys_handles_cursors_and_binary_files(pool: PgPool) {
    let f = Fixture::new(&pool).await;
    f.publish().await;
    let digest = format!("sha256:{}", "a".repeat(64));
    canonical(&pool, &f, &digest).await;
    let backend = Arc::new(backends::Backend);
    let app = sandbox_api::router_with_uploads(
        sandbox_api::AppState {
            store: f.store.clone(),
            images: sandbox_protocol::images::ImageAllowlist::new([digest.clone()]).unwrap(),
        },
        Some(backend.clone()),
        None,
        Some(backend.clone()),
        Some(backend),
    );
    let s = Server::start(app).await;
    credential(&s, &f);
    let sandbox = f.sandbox.to_string();
    let op = f.operation.to_string();
    assert_eq!(
        value(&s, &["get", &sandbox]).await["observed_state"],
        "running"
    );
    assert_eq!(
        value(&s, &["list", "--limit", "1"]).await["items"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        value(&s, &["operations", "--sandbox-id", &sandbox]).await["items"][0]["operation_id"],
        op
    );
    assert_eq!(value(&s, &["operation", &op]).await["status"], "succeeded");
    assert_eq!(
        value(&s, &["wait", &op, "--seconds", "1"]).await["status"],
        "succeeded"
    );
    let key = s.directory.path().join("mutation.key");
    value(&s, &["key", "--to", path(&key)]).await;
    let saved = std::fs::read(&key).unwrap();
    cli(&s, &["key", "--to", path(&key)], 1).await;
    assert_eq!(saved, std::fs::read(&key).unwrap());
    let cancel = value(&s, &["cancel", &op, "--key-file", path(&key)]).await;
    assert_ne!(cancel["operation_id"], op);
    let cancel_retry = value(&s, &["cancel", &op, "--key-file", path(&key)]).await;
    assert_eq!(cancel["operation_id"], cancel_retry["operation_id"]);
    let output = s.directory.path().join("stdout.bin");
    let meta = value(&s, &["output", &op, "stdout", "--to", path(&output)]).await;
    assert_eq!(std::fs::read(&output).unwrap(), b"a\0b\xff");
    assert_eq!(meta["truncated"], true);
    let events = cli(&s, &["stream", &op, "--seconds", "2"], 0).await;
    let events: Vec<Value> = std::str::from_utf8(&events)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert_eq!(events.last().unwrap()["event"], "end");
    let cursor = events[0]["cursor"].as_str().unwrap();
    let resumed = cli(
        &s,
        &["stream", &op, "--cursor", cursor, "--seconds", "2"],
        0,
    )
    .await;
    assert!(String::from_utf8_lossy(&resumed).contains("\"event\":\"end\""));
    let download = s.directory.path().join("download.bin");
    let verified = value(
        &s,
        &["download", &sandbox, "hello.bin", "--to", path(&download)],
    )
    .await;
    assert_eq!(std::fs::read(&download).unwrap(), b"hello\0\xff");
    assert_eq!(verified["release_confirmed"], true);
    let uploaded = value(
        &s,
        &[
            "upload",
            &sandbox,
            "hello.bin",
            "--from",
            path(&download),
            "--key",
            "cli-upload-persistent-1",
        ],
    )
    .await;
    let retry = value(
        &s,
        &[
            "upload",
            &sandbox,
            "hello.bin",
            "--from",
            path(&download),
            "--key",
            "cli-upload-persistent-1",
        ],
    )
    .await;
    assert_eq!(uploaded["operation_id"], retry["operation_id"]);
    assert_eq!(
        value(
            &s,
            &["operation", uploaded["operation_id"].as_str().unwrap()]
        )
        .await["kind"],
        "file_write"
    );
    // Separate running sandbox/project avoids bypassing the unresolved upload gate.
    let g = Fixture::new(&pool).await;
    g.finish().await;
    canonical(&pool, &g, &digest).await;
    credential(&s, &g);
    let sandbox = g.sandbox.to_string();
    let request = s.directory.path().join("execute.json");
    private(&request,&serde_json::to_vec(&json!({"argv":["/bin/true"],"deadline_unix_ms":(time::OffsetDateTime::now_utc().unix_timestamp_nanos()/1_000_000) as i64+60000})).unwrap());
    let first = value(
        &s,
        &[
            "execute",
            &sandbox,
            "--request",
            path(&request),
            "--key",
            "cli-execute-persistent",
        ],
    )
    .await;
    let retry = value(
        &s,
        &[
            "execute",
            &sandbox,
            "--request",
            path(&request),
            "--key",
            "cli-execute-persistent",
        ],
    )
    .await;
    assert_eq!(first["operation_id"], retry["operation_id"]);
    cli(
        &s,
        &[
            "wait",
            first["operation_id"].as_str().unwrap(),
            "--seconds",
            "1",
        ],
        6,
    )
    .await;
    assert_eq!(
        value(&s, &["operation", first["operation_id"].as_str().unwrap()]).await["status"],
        "queued"
    );
    value(
        &s,
        &["destroy", &sandbox, "--key", "cli-destroy-persistent"],
    )
    .await;
    let create = s.directory.path().join("create.json");
    private(
        &create,
        &serde_json::to_vec(
            &json!({"image_digest":digest,"resources":{"vcpu":1,"memory_mib":128,"disk_mib":64}}),
        )
        .unwrap(),
    );
    let first = value(
        &s,
        &[
            "create",
            "--request",
            path(&create),
            "--key",
            "cli-create-persistent",
        ],
    )
    .await;
    let retry = value(
        &s,
        &[
            "create",
            "--request",
            path(&create),
            "--key",
            "cli-create-persistent",
        ],
    )
    .await;
    assert_eq!(first, retry);
    let mut changed: Value = serde_json::from_slice(&std::fs::read(&create).unwrap()).unwrap();
    changed["name"] = "changed".into();
    private(&create, &serde_json::to_vec(&changed).unwrap());
    cli(
        &s,
        &[
            "create",
            "--request",
            path(&create),
            "--key",
            "cli-create-persistent",
        ],
        8,
    )
    .await;
    cli(&s, &["get", &f.sandbox.to_string()], 8).await; // Foreign project remains inaccessible.
}
#[tokio::test]
async fn wait_exit_codes_distinguish_terminal_outcomes_and_keep_json_results() {
    for (status, exit, expected) in [
        ("succeeded", Some(0), 0),
        ("failed", Some(23), 10),
        ("failed", None, 3),
        ("cancelled", None, 4),
        ("unknown", None, 5),
    ] {
        let s = Server::start(axum::Router::new().route("/v1/operations/op_fixture",axum::routing::get(move || async move {
            ([("cache-control","no-store")],axum::Json(json!({"operation_id":"op_fixture","sandbox_id":"sbx_fixture","kind":"execute","status":status,"result":{"exit_code":exit},"created_at":"2026-09-21T00:00:00Z"})))
        }))).await;
        let bytes = cli(&s, &["wait", "op_fixture"], expected).await;
        let data: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(data["status"], status);
        assert_eq!(data["result"]["exit_code"], json!(exit));
    }
}
#[tokio::test]
async fn cli_reconnects_same_stream_only_after_a_complete_flushed_event() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    let attempts = Arc::new(AtomicUsize::new(0));
    let count = attempts.clone();
    let s = Server::start(axum::Router::new().route("/v1/operations/op_fixture/stream",axum::routing::get(move |axum::extract::Query(q): axum::extract::Query<std::collections::HashMap<String,String>>| {
        let count = count.clone(); async move {
            let body = if count.fetch_add(1,Ordering::SeqCst) == 0 {
                assert!(q.is_empty());
                format!("event: output\nid: after-complete\ndata: {}\n\nevent: output\nid: partial\ndata:",json!({"stream":"stdout","offset":0,"next_offset":1,"data_base64":"YQ==","at_end":true,"complete":true,"seen":1,"stored":1,"truncated":false,"simulated":true,"guest_reported":true}))
            } else {
                assert_eq!(q["cursor"],"after-complete");
                "event: gap\ndata: {\"code\":\"output_expired\"}\n\n".into()
            };
            ([("cache-control","no-store"),("content-type","text/event-stream")],body)
        }
    }))).await;
    let bytes = cli(&s, &["stream", "op_fixture", "--seconds", "3"], 9).await;
    assert_eq!(attempts.load(Ordering::SeqCst), 2);
    let lines: Vec<Value> = std::str::from_utf8(&bytes)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert_eq!(lines.len(), 2);
    assert_eq!(lines[0]["cursor"], "after-complete");
    assert_eq!(lines[1]["event"], "gap");
}

#[tokio::test]
async fn expired_retry_reports_original_operation_without_leaking_backend_text() {
    const ID: &str = "op_019a9fad-3000-7000-8000-000000000001";
    let s=Server::start(axum::Router::new().route("/v1/sandboxes",axum::routing::post(|| async {
        (axum::http::StatusCode::GONE,axum::Json(json!({"title":"private-backend-text","status":410,"code":"response_expired","operation_id":ID})))
    }))).await;
    let request = s.directory.path().join("create.json");
    private(&request,&serde_json::to_vec(&json!({"image_digest":"sha256:fixture","resources":{"vcpu":1,"memory_mib":128,"disk_mib":64}})).unwrap());
    let result = command()
        .args([
            "--config",
            path(&s.config),
            "--json",
            "create",
            "--request",
            path(&request),
            "--key",
            "original-create-key",
        ])
        .output()
        .await
        .unwrap();
    assert_eq!(result.status.code(), Some(8));
    assert!(result.stdout.is_empty());
    assert!(!String::from_utf8_lossy(&result.stderr).contains("private-backend-text"));
    let problem: Value = serde_json::from_slice(&result.stderr).unwrap();
    assert_eq!(problem["operation_id"], ID);
    assert_eq!(problem["code"], "response_expired");
}
