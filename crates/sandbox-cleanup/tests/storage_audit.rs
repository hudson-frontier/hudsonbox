//! Real executable and PostgreSQL, no VM or storage mutation authority.
#![cfg(unix)]
#![allow(clippy::unwrap_used, clippy::expect_used)]
use sandbox_protocol::Id;
use sqlx::PgPool;
use std::{os::unix::fs::PermissionsExt, path::Path, time::Duration};

fn private_file(path: &Path, bytes: &[u8]) {
    std::fs::write(path, bytes).unwrap();
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600)).unwrap();
}
fn database_url(pool: &PgPool) -> String {
    let mut url = url::Url::parse(&std::env::var("DATABASE_URL").unwrap()).unwrap();
    url.set_path(pool.connect_options().get_database().unwrap());
    url.into()
}
async fn command(database: &Path, storage: &Path) -> std::process::Output {
    tokio::time::timeout(
        Duration::from_secs(10),
        tokio::process::Command::new(env!("CARGO_BIN_EXE_sandbox-storage-audit"))
            .arg("--database-config")
            .arg(database)
            .arg("--output-config")
            .arg(storage)
            .args(["--timeout-seconds", "5"])
            .kill_on_drop(true)
            .output(),
    )
    .await
    .unwrap()
    .unwrap()
}
fn storage(path: &Path) {
    private_file(path, br#"{"endpoint":"https://objects.example.test","region":"us-east-1","bucket":"audit-test","access_key":"audit-private-access","secret_key":"audit-private-secret"}"#);
}

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn audit_cli_runs_against_readonly_quarantine_without_contacting_empty_storage(pool: PgPool) {
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("database.json");
    let output = directory.path().join("output.json");
    let url = database_url(&pool);
    private_file(
        &database,
        &serde_json::to_vec(&serde_json::json!({"version":1,"database_url":url})).unwrap(),
    );
    storage(&output);
    // The new process must never need a write, migration or row-locking SELECT.
    let name = pool.connect_options().get_database().unwrap().to_owned();
    assert!(name.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_'));
    sqlx::query(&format!(
        "ALTER DATABASE \"{name}\" SET default_transaction_read_only=on"
    ))
    .execute(&pool)
    .await
    .unwrap();
    let result = command(&database, &output).await;
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    let report: serde_json::Value = serde_json::from_slice(&result.stdout).unwrap();
    assert_eq!(report["scope"], "storage_evidence_only");
    for count in report["verified"].as_object().unwrap().values() {
        assert_eq!(count, 0);
    }
    for secret in [&url, "audit-private-access", "audit-private-secret"] {
        assert!(!String::from_utf8_lossy(&result.stdout).contains(secret));
        assert!(!String::from_utf8_lossy(&result.stderr).contains(secret));
    }
    let migrations: i64 = sqlx::query_scalar("SELECT count(*) FROM _sqlx_migrations")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(migrations as usize, sandbox_store::MIGRATOR.iter().count());
    for bad in [
        b"{\"version\":1,\"database_url\":\"PRIVATE_DATABASE_SECRET\",\"version\":1}".as_slice(),
        b"{\"version\":2,\"database_url\":\"PRIVATE_DATABASE_SECRET\"}",
    ] {
        private_file(&database, bad);
        let result = command(&database, &output).await;
        assert!(!result.status.success());
        assert!(!String::from_utf8_lossy(&result.stderr).contains("PRIVATE_DATABASE_SECRET"));
    }
    private_file(
        &database,
        &serde_json::to_vec(&serde_json::json!({"version":1,"database_url":url})).unwrap(),
    );
    std::fs::set_permissions(&database, std::fs::Permissions::from_mode(0o644)).unwrap();
    assert!(!command(&database, &output).await.status.success());
    let link = directory.path().join("linked-database.json");
    std::os::unix::fs::symlink(&database, &link).unwrap();
    assert!(!command(&link, &output).await.status.success());
}

#[sqlx::test]
async fn audit_cli_never_migrates_an_empty_or_incompatible_database(pool: PgPool) {
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("database.json");
    let output = directory.path().join("output.json");
    private_file(
        &database,
        &serde_json::to_vec(&serde_json::json!({"version":1,"database_url":database_url(&pool)}))
            .unwrap(),
    );
    storage(&output);
    let before: i64 =
        sqlx::query_scalar("SELECT count(*) FROM pg_tables WHERE schemaname='public'")
            .fetch_one(&pool)
            .await
            .unwrap();
    let result = command(&database, &output).await;
    assert!(!result.status.success());
    assert!(
        String::from_utf8_lossy(&result.stderr).contains("storage audit database query failed")
    );
    let after: i64 = sqlx::query_scalar("SELECT count(*) FROM pg_tables WHERE schemaname='public'")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(before, after);
    assert_eq!(after, 0);
}

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
#[ignore = "requires private versioned MinIO bucket"]
async fn output_minio_storage_recovery_verifies_payload_then_exact_retirement_from_the_ledger(
    pool: PgPool,
) {
    use sandbox_artifacts::S3Config;
    use sandbox_cleanup::audit;
    use sandbox_protocol::{
        AllocationId, HostId, IdempotencyKey, OperationId, ProjectId, ProjectToken, SandboxId,
    };
    use sandbox_store::{
        Store,
        uploads::{UploadAdmission, UploadCommand, UploadInput},
    };
    use sha2::{Digest, Sha256};
    let config = || S3Config {
        endpoint: std::env::var("HUDSON_TEST_S3_ENDPOINT").unwrap(),
        region: "us-east-1".into(),
        bucket: std::env::var("HUDSON_TEST_S3_VERSIONED_BUCKET").unwrap(),
        access_key: std::env::var("HUDSON_TEST_S3_ACCESS_KEY").unwrap(),
        secret_key: std::env::var("HUDSON_TEST_S3_SECRET_KEY").unwrap(),
        session_token: None,
        allow_loopback_http: true,
    };
    let project = ProjectId::generate();
    let sandbox = SandboxId::generate();
    let allocation = AllocationId::generate();
    let host = HostId::generate();
    let token = ProjectToken::generate().unwrap();
    // Synthetic admission state; no host or guest is claimed to have executed.
    sqlx::query("INSERT INTO projects(id,name,status,limits,api_tokens) VALUES($1,'storage-audit','active','{}',$2)")
        .bind(project.uuid()).bind(serde_json::json!([{"key_id":token.key_id().as_str(),"hash":token.hash().as_bytes().iter().map(|b|format!("{b:02x}")).collect::<String>()}])).execute(&pool).await.unwrap();
    sqlx::query("INSERT INTO hosts(id,status,cpu_capacity,memory_capacity_mib,disk_capacity_mib,supervisor_epoch,last_seen_at) VALUES($1,'ready',4,8192,65536,1,clock_timestamp())")
        .bind(host.uuid()).execute(&pool).await.unwrap();
    sqlx::query("INSERT INTO sandboxes(id,project_id,image_digest,resources,desired_state,observed_state,generation) VALUES($1,$2,'sha256:fixture','{}','running','running',1)")
        .bind(sandbox.uuid()).bind(project.uuid()).execute(&pool).await.unwrap();
    sqlx::query("INSERT INTO allocations(id,project_id,sandbox_id,host_id,generation,supervisor_epoch,vcpu,memory_mib,disk_mib,status,lease_expires_at) VALUES($1,$2,$3,$4,1,1,1,128,64,'running',clock_timestamp()+interval '5 minutes')")
        .bind(allocation.uuid()).bind(project.uuid()).bind(sandbox.uuid()).bind(host.uuid()).execute(&pool).await.unwrap();
    sqlx::query("UPDATE sandboxes SET current_allocation_id=$2 WHERE id=$1")
        .bind(sandbox.uuid())
        .bind(allocation.uuid())
        .execute(&pool)
        .await
        .unwrap();
    let store = Store::from_pool(pool.clone());
    let bytes = b"private-recovery-source";
    let request = UploadCommand {
        project_id: project,
        sandbox_id: sandbox,
        key_id: token.key_id().clone(),
        key: IdempotencyKey::parse(&OperationId::generate().to_string()).unwrap(),
        input: UploadInput {
            path: "restored.bin".into(),
            size: bytes.len() as u64,
            sha256: Sha256::digest(bytes).into(),
            mode: 0o644,
        },
    };
    let UploadAdmission::Accepted { mut plan, .. } =
        store.admit_upload(&request, &token.hash()).await.unwrap()
    else {
        panic!("admission")
    };
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    // Seed the complete retained policy in the past instead of waiting for
    // retention. The object is uploaded and hashed against that exact plan.
    plan.created_unix_ms = now - 6000;
    plan.write_expires_unix_ms = now - 5000;
    plan.expires_unix_ms = now - 4000;
    plan.delete_after_unix_ms = now - 3000;
    let sources = config().build_sources().unwrap();
    let reference = sources
        .upload(&plan, &plan.owner, plan.created_unix_ms, bytes)
        .await
        .unwrap();
    sqlx::query("UPDATE file_uploads SET plan=$2,source_ref=$3 WHERE operation_id=$1")
        .bind(plan.owner.operation_id.uuid())
        .bind(serde_json::json!(plan))
        .bind(serde_json::json!(reference))
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query(
        "UPDATE operations SET deadline=clock_timestamp()-interval '6 minutes' WHERE id=$1",
    )
    .bind(plan.owner.operation_id.uuid())
    .execute(&pool)
    .await
    .unwrap();
    let report = audit::verify(&store, None, Some(&sources), false)
        .await
        .unwrap();
    assert_eq!(report.sources_verified, 1);
    assert!(matches!(
        audit::verify(&store, None, None, false).await,
        Err(audit::Error::MissingSourceConfig)
    ));
    let claim = store.claim_file_source_cleanup(30).await.unwrap().unwrap();
    let work = store.prepare_file_source_cleanup(&claim).await.unwrap();
    let receipt = config()
        .build_source_retirer()
        .unwrap()
        .retire(&plan, &plan.owner, Some(&reference), work.now_unix_ms)
        .await
        .unwrap();
    // Storage progressed beyond the captured database: audit cannot invent a
    // cleanup acknowledgement or silently replace the missing payload.
    assert!(matches!(
        audit::verify(&store, None, Some(&sources), false).await,
        Err(audit::Error::Storage)
    ));
    store
        .complete_file_source_cleanup(&claim, &receipt)
        .await
        .unwrap();
    let ledger = || {
        sqlx::query_scalar::<_, serde_json::Value>("SELECT to_jsonb(f) FROM file_uploads f")
            .fetch_one(&pool)
    };
    let before = ledger().await.unwrap();
    for _ in 0..2 {
        let report = audit::verify(&store, None, Some(&sources), false)
            .await
            .unwrap();
        assert_eq!(report.sources_verified, 0);
        assert_eq!(report.source_markers_verified, 1);
    }
    assert_eq!(ledger().await.unwrap(), before);
    sqlx::query("UPDATE file_uploads SET source_retirement=jsonb_set(source_retirement,'{marker_etag}','\"wrong-marker\"')")
        .execute(&pool).await.unwrap();
    let corrupt = ledger().await.unwrap();
    assert!(matches!(
        audit::verify(&store, None, Some(&sources), false).await,
        Err(audit::Error::Storage)
    ));
    assert_eq!(ledger().await.unwrap(), corrupt);
}
