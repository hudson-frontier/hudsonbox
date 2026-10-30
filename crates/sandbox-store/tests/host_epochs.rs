//! Operator epoch floors and exact retries; no physical host or VM is started.
#![allow(clippy::unwrap_used, clippy::expect_used)]
use sandbox_protocol::{HostId, Id, IdempotencyKey, ProjectId, ProjectToken, RequestDigest};
use sandbox_store::{
    Store,
    admission::{CreateSandbox, Resources},
    claims::OperationKind,
    host_maintenance::Error,
};
use serde_json::{Value, json};
use sqlx::PgPool;

async fn host(pool: &PgPool, status: &str) -> HostId {
    let id = HostId::generate();
    sqlx::query("INSERT INTO hosts(id,status,cpu_capacity,memory_capacity_mib,disk_capacity_mib,supervisor_epoch,last_seen_at) VALUES($1,$2,4,8192,65536,1,clock_timestamp())")
        .bind(id.uuid()).bind(status).execute(pool).await.unwrap();
    id
}
async fn snapshot(pool: &PgPool, id: HostId) -> Value {
    sqlx::query_scalar("SELECT to_jsonb(hosts) FROM hosts WHERE id=$1")
        .bind(id.uuid())
        .fetch_one(pool)
        .await
        .unwrap()
}

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn advance_requires_closed_placement_and_fresh_health_before_explicit_reopening(
    pool: PgPool,
) {
    let store = Store::from_pool(pool.clone());
    let id = host(&pool, "ready").await;
    let before = snapshot(&pool, id).await;
    assert!(matches!(
        store.advance_host_epoch(id, 1).await,
        Err(Error::Conflict)
    ));
    assert_eq!(snapshot(&pool, id).await, before);
    store.drain_host(id, 1).await.unwrap();
    let advanced = store.advance_host_epoch(id, 1).await.unwrap();
    assert_eq!(
        (advanced.supervisor_epoch, advanced.status.as_str()),
        (2, "draining")
    );
    assert!(snapshot(&pool, id).await["last_seen_at"].is_null());
    assert!(matches!(
        store.finish_host_drain(id, 2).await,
        Err(Error::Outstanding)
    ));
    assert!(store.observe_configured_host(id, 1).await.is_err());
    store.observe_configured_host(id, 2).await.unwrap();
    assert_eq!(
        store.host_maintenance(id, 2).await.unwrap().status,
        "draining"
    );
    store.finish_host_drain(id, 2).await.unwrap();
    let ready = snapshot(&pool, id).await;
    assert_eq!(
        store.advance_host_epoch(id, 1).await.unwrap().status,
        "ready"
    );
    assert_eq!(
        snapshot(&pool, id).await,
        ready,
        "an exact retry must not mutate the successor"
    );
}

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn duplicate_requests_advance_once_and_older_requests_cannot_change_newer_epochs(
    pool: PgPool,
) {
    let store = Store::from_pool(pool.clone());
    let id = host(&pool, "draining").await;
    let (a, b) = tokio::join!(
        store.advance_host_epoch(id, 1),
        store.advance_host_epoch(id, 1)
    );
    assert_eq!(a.unwrap(), b.unwrap());
    assert_eq!(
        store
            .host_maintenance(id, 2)
            .await
            .unwrap()
            .supervisor_epoch,
        2
    );
    store.advance_host_epoch(id, 2).await.unwrap();
    let before = snapshot(&pool, id).await;
    assert!(matches!(
        store.advance_host_epoch(id, 1).await,
        Err(Error::Conflict)
    ));
    assert_eq!(snapshot(&pool, id).await, before);
    for epoch in [0, -1, i64::MAX] {
        assert!(matches!(
            store.advance_host_epoch(id, epoch).await,
            Err(Error::Conflict)
        ));
    }
    assert!(matches!(
        store.advance_host_epoch(HostId::generate(), 1).await,
        Err(Error::Conflict)
    ));
}

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn unhealthy_recovery_preserves_opaque_policy_and_prior_observation(pool: PgPool) {
    let store = Store::from_pool(pool.clone());
    let id = host(&pool, "unhealthy").await;
    let policy = json!({"retained":"operator-policy"});
    let observed = json!({"retained":"old-epoch-observation"});
    sqlx::query("UPDATE hosts SET compatibility=$2,runtime_capabilities=$3,runtime_capabilities_epoch=1 WHERE id=$1")
        .bind(id.uuid()).bind(&policy).bind(&observed).execute(&pool).await.unwrap();
    let result = store.advance_host_epoch(id, 1).await.unwrap();
    assert_eq!(
        (result.supervisor_epoch, result.status.as_str()),
        (2, "draining")
    );
    let after = snapshot(&pool, id).await;
    assert_eq!(after["compatibility"], policy);
    assert_eq!(after["runtime_capabilities"], observed);
    assert_eq!(after["runtime_capabilities_epoch"], 1);
    assert!(after["last_seen_at"].is_null());
    assert!(matches!(
        store.finish_host_drain(id, 2).await,
        Err(Error::Outstanding)
    ));
}

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn advancing_a_busy_host_preserves_old_ownership_claims_and_all_capacity_charges(
    pool: PgPool,
) {
    let store = Store::from_pool(pool.clone());
    let id = host(&pool, "ready").await;
    let project = ProjectId::generate();
    let token = ProjectToken::generate().unwrap();
    sqlx::query("INSERT INTO projects(id,name,status,limits,api_tokens) VALUES($1,'epoch-test','active','{}',$2)")
        .bind(project.uuid()).bind(json!([{"key_id":token.key_id().as_str(),"hash":hex::encode(token.hash().as_bytes())}]))
        .execute(&pool).await.unwrap();
    // Placement requires an explicit pool grant; these fixtures use only shared pools.
    sqlx::query("INSERT INTO project_host_pool_access(id,project_id,host_pool_id) SELECT gen_random_uuid(),$1,id FROM host_pools")
        .bind(project.uuid()).execute(&pool).await.unwrap();
    let image = format!("sha256:{}", "a".repeat(64));
    let payload =
        json!({"image_digest":image,"resources":{"vcpu":1,"memory_mib":128,"disk_mib":256}});
    store
        .admit_create_sandbox(
            &CreateSandbox {
                project_id: project,
                key_id: token.key_id().clone(),
                idempotency_key: IdempotencyKey::parse("retained-old-epoch").unwrap(),
                request_digest: RequestDigest::compute("POST", "/v1/sandboxes", &payload).unwrap(),
                image_digest: image.clone(),
                name: None,
                resources: Resources {
                    vcpu: 1,
                    memory_mib: 128,
                    disk_mib: 256,
                },
                payload,
            },
            &sandbox_protocol::images::ImageAllowlist::new([image]).unwrap(),
        )
        .await
        .unwrap();
    let claim = store
        .claim_next(OperationKind::Create, 30)
        .await
        .unwrap()
        .unwrap();
    store.reserve_create(&claim, id, 1).await.unwrap();
    store.drain_host(id, 1).await.unwrap();
    let allocation: Value = sqlx::query_scalar("SELECT to_jsonb(allocations) FROM allocations")
        .fetch_one(&pool)
        .await
        .unwrap();
    let operation: Value = sqlx::query_scalar("SELECT to_jsonb(operations) FROM operations")
        .fetch_one(&pool)
        .await
        .unwrap();
    let after = store.advance_host_epoch(id, 1).await.unwrap();
    assert_eq!(
        (
            after.supervisor_epoch,
            after.status.as_str(),
            after.outstanding_allocations,
            after.reserved_vcpu,
            after.reserved_memory_mib,
            after.reserved_disk_mib
        ),
        (2, "draining", 1, 1, 128, 256)
    );
    assert_eq!(
        sqlx::query_scalar::<_, Value>("SELECT to_jsonb(allocations) FROM allocations")
            .fetch_one(&pool)
            .await
            .unwrap(),
        allocation
    );
    assert_eq!(
        sqlx::query_scalar::<_, Value>("SELECT to_jsonb(operations) FROM operations")
            .fetch_one(&pool)
            .await
            .unwrap(),
        operation
    );
    assert!(matches!(
        store.finish_host_drain(id, 2).await,
        Err(Error::Outstanding)
    ));
}
