//! Expiry metadata and retained bodies must describe the same database-time read.
#![allow(clippy::unwrap_used, clippy::expect_used)]
use sandbox_protocol::{Id, OperationId, ProjectId, SandboxId};
use sandbox_store::{Store, lists::PageLimit, reads::OperationView};
use serde_json::json;
use sqlx::{PgPool, postgres::PgPoolOptions};

async fn fixture(pool: &PgPool, controlled_clock: bool) -> (Store, ProjectId, OperationId) {
    let project = ProjectId::generate();
    let sandbox = SandboxId::generate();
    let operation = OperationId::generate();
    sqlx::query(
        "INSERT INTO projects(id,name,status,limits) VALUES($1,'read-clock','active','{}')",
    )
    .bind(project.uuid())
    .execute(pool)
    .await
    .unwrap();
    sqlx::query("INSERT INTO sandboxes(id,project_id,image_digest,resources,desired_state,observed_state) VALUES($1,$2,'sha256:fixture','{}','destroyed','destroyed')")
        .bind(sandbox.uuid()).bind(project.uuid()).execute(pool).await.unwrap();
    sqlx::query("INSERT INTO operations(id,project_id,sandbox_id,kind,initiator_kind,idempotency_key,request_digest,digest_version,payload,status,completed_at,response_expires_at,result,error) VALUES($1,$2,$3,'execute','service',$4,$5,1,'{}','failed',pg_catalog.clock_timestamp(),pg_catalog.clock_timestamp()+interval '1 hour',$6,$7)")
        .bind(operation.uuid()).bind(project.uuid()).bind(sandbox.uuid()).bind(operation.to_string()).bind(vec![7_u8;32])
        .bind(json!({"exit_code":1})).bind(json!({"code":"fixture_failure"})).execute(pool).await.unwrap();
    // The macro owns this entire isolated database. Only the dedicated reader
    // below uses this shadow clock; no shared cluster function is replaced.
    if controlled_clock {
        sqlx::raw_sql(
            r#"
        CREATE SEQUENCE expiry_read_clock;
        CREATE FUNCTION public.clock_timestamp() RETURNS timestamptz
        LANGUAGE plpgsql VOLATILE AS $$
        DECLARE expiry timestamptz;
        BEGIN
          SELECT response_expires_at INTO expiry FROM operations;
          IF nextval('expiry_read_clock')=1 THEN
            RETURN expiry-interval '1 millisecond';
          END IF;
          RETURN expiry+interval '1 millisecond';
        END $$;
    "#,
        )
        .execute(pool)
        .await
        .unwrap();
    }
    let reader = PgPoolOptions::new()
        .max_connections(1)
        .after_connect(|connection, _| {
            Box::pin(async move {
                sqlx::query("SET search_path=public,pg_catalog")
                    .execute(connection)
                    .await?;
                Ok(())
            })
        })
        .connect_with(pool.connect_options().as_ref().clone())
        .await
        .unwrap();
    (Store::from_pool(reader), project, operation)
}

fn coherent(view: &OperationView) {
    if view.response_expired() {
        assert!(view.result.is_none() && view.error.is_none());
    } else {
        assert_eq!(
            view.result,
            Some(json!({"exit_code":1})),
            "unexpired read lost its result"
        );
        assert_eq!(
            view.error,
            Some(json!({"code":"fixture_failure"})),
            "unexpired read lost its error"
        );
    }
}

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn operation_expiry_and_payload_remain_coherent_when_the_clock_crosses_the_deadline(
    pool: PgPool,
) {
    let (store, project, operation) = fixture(&pool, true).await;
    let view = store
        .operation_for_project(project, operation)
        .await
        .unwrap()
        .unwrap();
    coherent(&view);
    let expired = store
        .operation_for_project(project, operation)
        .await
        .unwrap()
        .unwrap();
    assert!(expired.response_expired());
    coherent(&expired);
    store.pool().close().await;
}

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn collection_expiry_and_payload_remain_coherent_when_the_clock_crosses_the_deadline(
    pool: PgPool,
) {
    let (store, project, operation) = fixture(&pool, true).await;
    let page = store
        .list_operations(project, None, None, PageLimit::new(1).unwrap())
        .await
        .unwrap();
    assert_eq!(page.items.len(), 1);
    assert_eq!(page.items[0].id, operation);
    coherent(&page.items[0]);
    let expired = store
        .list_operations(project, None, None, PageLimit::new(1).unwrap())
        .await
        .unwrap();
    assert_eq!(expired.items.len(), 1);
    assert!(expired.items[0].response_expired());
    coherent(&expired.items[0]);
    store.pool().close().await;
}

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn cached_operation_query_samples_database_time_after_relation_lock_wait(pool: PgPool) {
    use std::time::Duration;
    let (store, project, operation) = fixture(&pool, false).await;
    let warm = store
        .operation_for_project(project, operation)
        .await
        .unwrap()
        .unwrap();
    assert!(!warm.response_expired);
    let expiry: time::OffsetDateTime = sqlx::query_scalar("UPDATE operations SET response_expires_at=pg_catalog.clock_timestamp()+interval '500 milliseconds' WHERE id=$1 RETURNING response_expires_at")
        .bind(operation.uuid()).fetch_one(&pool).await.unwrap();
    let mut lock = pool.begin().await.unwrap();
    sqlx::query("LOCK TABLE operations IN ACCESS EXCLUSIVE MODE")
        .execute(&mut *lock)
        .await
        .unwrap();
    let reader = store.clone();
    let task = tokio::spawn(async move { reader.operation_for_project(project, operation).await });
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let blocked: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock' AND query LIKE 'WITH response_clock AS MATERIALIZED%SELECT id,sandbox_id,%')")
                .fetch_one(&pool).await.unwrap();
            if blocked { break; }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        loop {
            let expired: bool = sqlx::query_scalar("SELECT pg_catalog.clock_timestamp()>=$1")
                .bind(expiry).fetch_one(&pool).await.unwrap();
            if expired { break; }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }).await.unwrap();
    lock.commit().await.unwrap();
    let view = tokio::time::timeout(Duration::from_secs(5), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(
        view.response_expired,
        "database decision must be fresh without the API fallback"
    );
    assert!(view.result.is_none() && view.error.is_none());
    store.pool().close().await;
}
