//! Expiry metadata and retained bodies must describe the same database-time read.
#![allow(clippy::unwrap_used, clippy::expect_used)]
use sandbox_protocol::{Id, OperationId, ProjectId, SandboxId};
use sandbox_store::{Store, lists::PageLimit, reads::OperationView};
use serde_json::json;
use sqlx::{PgPool, postgres::PgPoolOptions};

async fn fixture(pool: &PgPool) -> (Store, ProjectId, OperationId) {
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
    let (store, project, operation) = fixture(&pool).await;
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
    let (store, project, operation) = fixture(&pool).await;
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
