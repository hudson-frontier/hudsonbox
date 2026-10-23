//! Real dump/restore of a disposable SQLx database. No service or VM is started.
#![cfg(unix)]
#![allow(clippy::unwrap_used, clippy::expect_used)]
use sandbox_protocol::{HostId, Id, OperationId};
use sandbox_store::{MIGRATOR, Store, claims::OperationKind};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use sqlx::{ConnectOptions, PgPool};
use std::{
    collections::BTreeMap,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    time::Duration,
};
use tokio::process::Command;

async fn inventory(pool: &PgPool) -> BTreeMap<String, [u8; 32]> {
    let tables: Vec<String> = sqlx::query_scalar(
        "SELECT tablename::text FROM pg_tables WHERE schemaname='public' ORDER BY tablename",
    )
    .fetch_all(pool)
    .await
    .unwrap();
    let mut result = BTreeMap::new();
    for table in tables {
        // Names come only from the fixture's catalog; never accept SQL input.
        assert!(
            table
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
        );
        let rows: Value = sqlx::query_scalar(&format!(
            "SELECT COALESCE(jsonb_agg(to_jsonb(t) ORDER BY to_jsonb(t)::text),'[]') FROM public.\"{table}\" t"
        )).fetch_one(pool).await.unwrap();
        result.insert(
            table,
            Sha256::digest(serde_json::to_vec(&rows).unwrap()).into(),
        );
    }
    result
}

async fn tool(program: &str, args: &[&str], database: &sqlx::postgres::PgConnectOptions) {
    let label = program;
    let url = database.to_url_lossy();
    let password = percent_encoding::percent_decode_str(url.password().unwrap_or(""))
        .decode_utf8()
        .unwrap();
    let pinned = Path::new("/usr/lib/postgresql/16/bin").join(program);
    let program = if pinned.is_file() {
        pinned
    } else {
        PathBuf::from(program)
    };
    let version = tokio::time::timeout(
        Duration::from_secs(5),
        Command::new(&program)
            .arg("--version")
            .kill_on_drop(true)
            .output(),
    )
    .await
    .unwrap()
    .expect("PostgreSQL 16 client tool is missing");
    assert!(
        version.status.success()
            && String::from_utf8_lossy(&version.stdout).contains("(PostgreSQL) 16."),
        "PostgreSQL 16 client tools are required"
    );
    let mut command = Command::new(program);
    // Credentials stay in the child environment, never arguments or diagnostics.
    // Inherited libpq settings cannot redirect this fixture to another endpoint.
    for (key, _) in std::env::vars_os().filter(|(key, _)| key.to_string_lossy().starts_with("PG")) {
        command.env_remove(key);
    }
    let output = tokio::time::timeout(
        Duration::from_secs(30),
        command
            .args(args)
            .env("PGDATABASE", database.get_database().unwrap())
            .env("PGHOST", database.get_host())
            .env("PGPORT", database.get_port().to_string())
            .env("PGUSER", database.get_username())
            .env("PGPASSWORD", password.as_ref())
            .env("PGSSLMODE", "disable")
            .env("PGCONNECT_TIMEOUT", "5")
            .env(
                "PGOPTIONS",
                "-c statement_timeout=10000 -c lock_timeout=5000",
            )
            .kill_on_drop(true)
            .output(),
    )
    .await
    .expect("database tool exceeded rehearsal deadline")
    .expect("install PostgreSQL 16 client tools for the recovery rehearsal");
    assert!(
        output.status.success(),
        "{label} recovery tool failed (diagnostics withheld)"
    );
}

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn private_dump_restores_outcomes_claims_permits_and_retirement_floors(pool: PgPool) {
    let options = pool.connect_options();
    let name: String = sqlx::query_scalar("SELECT current_database()")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(Some(name.as_str()), options.get_database());
    let base: sqlx::postgres::PgConnectOptions =
        std::env::var("DATABASE_URL").unwrap().parse().unwrap();
    assert_ne!(base.get_database(), Some(name.as_str()));
    assert!(matches!(
        options.get_host(),
        "127.0.0.1" | "::1" | "localhost"
    ));
    assert!(options.get_socket().is_none());
    let project = uuid::Uuid::now_v7();
    let sandbox = uuid::Uuid::now_v7();
    let allocation = uuid::Uuid::now_v7();
    let host = HostId::generate();
    let create = uuid::Uuid::now_v7();
    let unknown = OperationId::generate();
    let floor = OperationId::generate();
    sqlx::query(
        "INSERT INTO projects(id,name,status,limits) VALUES($1,'backup-rehearsal','active','{}')",
    )
    .bind(project)
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query("INSERT INTO hosts(id,status,cpu_capacity,memory_capacity_mib,disk_capacity_mib,supervisor_epoch,last_allocation_serial,registered_allocation_serial,launch_authority_required) VALUES($1,'draining',4,8192,65536,7,11,11,true)")
        .bind(host.uuid()).execute(&pool).await.unwrap();
    sqlx::query("INSERT INTO sandboxes(id,project_id,image_digest,resources,desired_state,observed_state,generation) VALUES($1,$2,'sha256:rehearsal','{}','running','unknown',3)")
        .bind(sandbox).bind(project).execute(&pool).await.unwrap();
    sqlx::query("INSERT INTO allocations(id,project_id,sandbox_id,host_id,generation,supervisor_epoch,vcpu,memory_mib,disk_mib,status,lease_expires_at,last_admitted_operation_id) VALUES($1,$2,$3,$4,3,7,1,128,256,'running',clock_timestamp()+interval '1 minute',$5)")
        .bind(allocation).bind(project).bind(sandbox).bind(host.uuid()).bind(unknown.uuid()).execute(&pool).await.unwrap();
    for (id, kind, status) in [
        (create, "create", "succeeded"),
        (unknown.uuid(), "execute", "unknown"),
        (uuid::Uuid::now_v7(), "execute", "cancelled"),
    ] {
        sqlx::query("INSERT INTO operations(id,project_id,sandbox_id,kind,initiator_kind,idempotency_key,request_digest,digest_version,payload,status,completed_at,claim_revision,attempt_count,attempt_receipts,execution_allocation_id) VALUES($1,$2,$3,$4,'service',$5,$6,1,'{}',$7,CASE WHEN $7 IN ('succeeded','cancelled') THEN clock_timestamp() END,3,1,$8,CASE WHEN $4='execute' THEN $9 END)")
            .bind(id).bind(project).bind(sandbox).bind(kind).bind(id.to_string()).bind(vec![19_u8;32]).bind(status)
            .bind(json!([{"synthetic_rehearsal_evidence":true,"outcome":status}])).bind(allocation).execute(&pool).await.unwrap();
    }
    sqlx::query("INSERT INTO allocation_permits(allocation_id,host_id,serial,project_id,sandbox_id,create_operation_id,generation,original_epoch) VALUES($1,$2,11,$3,$4,$5,3,7)")
        .bind(allocation).bind(host.uuid()).bind(project).bind(sandbox).bind(create).execute(&pool).await.unwrap();
    // Reserved intent has no completion: restore must not refund its budget.
    sqlx::query("INSERT INTO allocation_history(allocation_id,domain,reserved_through,claim_revision) VALUES($1,'commands',$2,4)")
        .bind(allocation).bind(floor.uuid()).execute(&pool).await.unwrap();
    let retirement = OperationId::generate();
    sqlx::query("INSERT INTO allocation_retirements(allocation_id,retirement_id,intent,claim_revision,reporting_epoch,lease_expires_at) VALUES($1,$2,$3,6,7,clock_timestamp()-interval '1 second')")
        .bind(allocation).bind(retirement.uuid())
        .bind(json!({"retirement":retirement.to_string(),"permit":{"allocation":format!("alc_{allocation}")},"synthetic_rehearsal_evidence":true}))
        .execute(&pool).await.unwrap();
    let store = Store::from_pool(pool.clone());
    let stale = store
        .claim_next(OperationKind::Execute, 30)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stale.operation_id, unknown);
    sqlx::query(
        "UPDATE operations SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE id=$1",
    )
    .bind(unknown.uuid())
    .execute(&pool)
    .await
    .unwrap();
    let before = inventory(&pool).await;
    assert!(before.contains_key("_sqlx_migrations"));
    assert!(before.contains_key("allocation_retirements"));
    let directory = tempfile::tempdir().unwrap();
    std::fs::set_permissions(directory.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    let archive = directory.path().join("database.dump");
    let sql = directory.path().join("restore.sql");
    for path in [&archive, &sql] {
        use std::os::unix::fs::OpenOptionsExt;
        std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(path)
            .unwrap();
    }
    tool(
        "pg_dump",
        &[
            "--no-password",
            "--format=custom",
            "--no-owner",
            "--no-privileges",
            "--file",
            archive.to_str().unwrap(),
        ],
        options.as_ref(),
    )
    .await;
    assert_private(&archive);
    let digest = Sha256::digest(std::fs::read(&archive).unwrap());
    tool(
        "pg_restore",
        &[
            "--no-owner",
            "--no-privileges",
            "--file",
            sql.to_str().unwrap(),
            archive.to_str().unwrap(),
        ],
        options.as_ref(),
    )
    .await;
    assert_private(&sql);
    // A valid backup still predates later epochs: it cannot rejoin live hosts.
    store.advance_host_epoch(host, 7).await.unwrap();
    assert_ne!(inventory(&pool).await, before);
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT current_database()")
            .fetch_one(&pool)
            .await
            .unwrap(),
        name
    );
    // Reset only this macro-owned disposable database, never DATABASE_URL's DB.
    sqlx::raw_sql("DROP SCHEMA public CASCADE; CREATE SCHEMA public;")
        .execute(&pool)
        .await
        .unwrap();
    tool(
        "psql",
        &[
            "--no-password",
            "--no-psqlrc",
            "--quiet",
            "--set=ON_ERROR_STOP=1",
            "--single-transaction",
            "--file",
            sql.to_str().unwrap(),
        ],
        options.as_ref(),
    )
    .await;
    assert_eq!(Sha256::digest(std::fs::read(&archive).unwrap()), digest);
    assert_eq!(inventory(&pool).await, before);
    MIGRATOR.run(&pool).await.unwrap();
    assert_eq!(
        inventory(&pool).await,
        before,
        "migration retry changed restored evidence"
    );
    assert!(store.renew_claim(&stale, 30).await.is_err());
    let replacement = store
        .claim_next(OperationKind::Execute, 30)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(replacement.operation_id, unknown);
    assert!(replacement.revision > stale.revision);
    assert_eq!(replacement.previous_status, "unknown");
    assert_eq!(replacement.receipts, stale.receipts);
    assert_eq!(
        sqlx::query_scalar::<_, i32>("SELECT attempt_count FROM operations WHERE id=$1")
            .bind(unknown.uuid())
            .fetch_one(&pool)
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM completed_allocation_history")
            .fetch_one(&pool)
            .await
            .unwrap(),
        0
    );
    let status = store.host_maintenance(host, 7).await.unwrap();
    assert_eq!(
        (
            status.status.as_str(),
            status.outstanding_allocations,
            status.reserved_vcpu
        ),
        ("draining", 1, 1)
    );
}

fn assert_private(path: &Path) {
    assert_eq!(
        std::fs::metadata(path).unwrap().permissions().mode() & 0o777,
        0o600
    );
}
