//! Placement races and authorization against isolated PostgreSQL databases.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use sandbox_protocol::{HostId, Id, IdempotencyKey, ProjectId, ProjectToken, RequestDigest};
use sandbox_store::{
    Store,
    admission::{CreateSandbox, Resources},
    claims::{Claim, OperationKind},
    placement::{PlacementError, Reservation},
};
use serde_json::json;
use sqlx::PgPool;

async fn project(pool: &PgPool) -> (ProjectId, ProjectToken) {
    let id = ProjectId::generate();
    let token = ProjectToken::generate().unwrap();
    sqlx::query("INSERT INTO projects(id,name,status,limits,api_tokens) VALUES($1,'placement','active','{}',$2)")
        .bind(id.uuid()).bind(json!([{"key_id":token.key_id().as_str(),"hash":hex::encode(token.hash().as_bytes())}]))
        .execute(pool).await.unwrap();
    // Placement requires an explicit pool grant; fixture hosts use the legacy
    // pool. Upgrade fixtures run before migration 0025 creates the table.
    let registry: bool =
        sqlx::query_scalar("SELECT to_regclass('project_host_pool_access') IS NOT NULL")
            .fetch_one(pool)
            .await
            .unwrap();
    if registry {
        sqlx::query(
            "INSERT INTO project_host_pool_access(id,project_id,host_pool_id) VALUES($1,$2,$3)",
        )
        .bind(uuid::Uuid::now_v7())
        .bind(id.uuid())
        .bind(sandbox_store::host_registry::LEGACY_POOL_ID)
        .execute(pool)
        .await
        .unwrap();
    }
    (id, token)
}

async fn host(pool: &PgPool) -> HostId {
    let id = HostId::generate();
    sqlx::query("INSERT INTO hosts(id,status,cpu_capacity,memory_capacity_mib,disk_capacity_mib,supervisor_epoch,last_seen_at) VALUES($1,'ready',2,2176,8593,1,clock_timestamp())")
        .bind(id.uuid()).execute(pool).await.unwrap();
    id
}

async fn claim(store: &Store, project: ProjectId, token: &ProjectToken, seconds: u32) -> Claim {
    let payload = json!({"image_digest":format!("sha256:{}","a".repeat(64))});
    store
        .admit_create_sandbox(
            &CreateSandbox {
                project_id: project,
                key_id: token.key_id().clone(),
                idempotency_key: IdempotencyKey::parse(&uuid::Uuid::now_v7().to_string()).unwrap(),
                request_digest: RequestDigest::compute("POST", "/v1/sandboxes", &payload).unwrap(),
                image_digest: format!("sha256:{}", "a".repeat(64)),
                name: None,
                resources: Resources {
                    vcpu: 2,
                    memory_mib: 2048,
                    disk_mib: 8192,
                },
                payload,
            },
            &sandbox_protocol::images::ImageAllowlist::new([format!("sha256:{}", "a".repeat(64))])
                .unwrap(),
        )
        .await
        .unwrap();
    store
        .claim_next(OperationKind::Create, seconds)
        .await
        .unwrap()
        .unwrap()
}

async fn fixture(pool: &PgPool) -> (Store, Claim, HostId) {
    let store = Store::from_pool(pool.clone());
    let (id, token) = project(pool).await;
    let claim = claim(&store, id, &token, 30).await;
    let host = host(pool).await;
    (store, claim, host)
}

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn reservation_is_atomic_and_not_readiness(pool: PgPool) {
    let (store, claim, host) = fixture(&pool).await;
    let Reservation::Reserved(allocation) = store.reserve_create(&claim, host, 1).await.unwrap()
    else {
        panic!("new")
    };
    let (current,generation,state,observed):(Option<uuid::Uuid>,i64,String,Option<time::OffsetDateTime>)=
        sqlx::query_as("SELECT current_allocation_id,generation,observed_state,observed_at FROM sandboxes WHERE id=$1")
        .bind(claim.sandbox_id.uuid()).fetch_one(&pool).await.unwrap();
    assert_eq!(current, Some(allocation.id.uuid()));
    assert_eq!(generation, 1);
    assert_eq!(state, "creating");
    assert!(observed.is_none());
    let (phase, attempts, receipts): (String, i32, serde_json::Value) =
        sqlx::query_as("SELECT phase,attempt_count,attempt_receipts FROM operations WHERE id=$1")
            .bind(claim.operation_id.uuid())
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(phase, "reserved");
    assert_eq!(attempts, 0);
    assert_eq!(receipts[0]["allocation_id"], allocation.id.to_string());
    assert_eq!(
        store.reserve_create(&claim, host, 1).await.unwrap(),
        Reservation::Existing(allocation)
    );
    let (count,): (i64,) = sqlx::query_as("SELECT count(*) FROM allocations")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(count, 1);
}

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn concurrent_projects_cannot_overbook_host(pool: PgPool) {
    let host = host(&pool).await;
    let store = Store::from_pool(pool.clone());
    let mut claims = Vec::new();
    for _ in 0..8 {
        let (id, token) = project(&pool).await;
        claims.push(claim(&store, id, &token, 30).await);
    }
    let mut tasks = tokio::task::JoinSet::new();
    for claim in claims {
        let store = store.clone();
        tasks.spawn(async move { store.reserve_create(&claim, host, 1).await });
    }
    let mut admitted = 0;
    let mut full = 0;
    while let Some(result) = tasks.join_next().await {
        match result.unwrap() {
            Ok(Reservation::Reserved(_)) => admitted += 1,
            Err(PlacementError::Capacity) => full += 1,
            other => panic!("{other:?}"),
        }
    }
    assert_eq!((admitted, full), (1, 7));
}

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn each_host_resource_is_checked_before_any_write(pool: PgPool) {
    let (store, claim, host) = fixture(&pool).await;
    // Capacity must cover the request plus 128 MiB VMM and 401 MiB artifact overhead.
    for (cpu, memory, disk) in [(1, 2176_i64, 8593_i64), (2, 2175, 8593), (2, 2176, 8592)] {
        sqlx::query("UPDATE hosts SET cpu_capacity=$1,memory_capacity_mib=$2,disk_capacity_mib=$3 WHERE id=$4")
            .bind(cpu).bind(memory).bind(disk).bind(host.uuid()).execute(&pool).await.unwrap();
        assert!(matches!(
            store.reserve_create(&claim, host, 1).await,
            Err(PlacementError::Capacity)
        ));
    }
    let (count,): (i64,) = sqlx::query_as("SELECT count(*) FROM allocations")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(count, 0);
    let (generation,): (i64,) = sqlx::query_as("SELECT generation FROM sandboxes WHERE id=$1")
        .bind(claim.sandbox_id.uuid())
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(generation, 0);
}

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn project_quota_serializes_across_different_hosts(pool: PgPool) {
    let store = Store::from_pool(pool.clone());
    let (id, token) = project(&pool).await;
    sqlx::query("UPDATE projects SET limits='{\"sandboxes\":1}' WHERE id=$1")
        .bind(id.uuid())
        .execute(&pool)
        .await
        .unwrap();
    let a = claim(&store, id, &token, 30).await;
    let b = claim(&store, id, &token, 30).await;
    let ha = host(&pool).await;
    let hb = host(&pool).await;
    let (a, b) = tokio::join!(
        store.reserve_create(&a, ha, 1),
        store.reserve_create(&b, hb, 1)
    );
    assert!(matches!(
        (&a, &b),
        (Ok(Reservation::Reserved(_)), Err(PlacementError::Quota))
            | (Err(PlacementError::Quota), Ok(Reservation::Reserved(_)))
    ));
}

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn authorization_is_rechecked_after_admission(pool: PgPool) {
    let (store, claim, host) = fixture(&pool).await;
    sqlx::query("UPDATE projects SET status='suspended'")
        .execute(&pool)
        .await
        .unwrap();
    assert!(matches!(
        store.reserve_create(&claim, host, 1).await,
        Err(PlacementError::Unauthorized)
    ));
    sqlx::query("UPDATE projects SET status='active',api_tokens='[]'")
        .execute(&pool)
        .await
        .unwrap();
    assert!(matches!(
        store.reserve_create(&claim, host, 1).await,
        Err(PlacementError::Unauthorized)
    ));
    let (count,): (i64,) = sqlx::query_as("SELECT count(*) FROM allocations")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(count, 0);
}

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn existing_uncertain_allocation_keeps_capacity_and_identity(pool: PgPool) {
    let (store, first, host) = fixture(&pool).await;
    let Reservation::Reserved(allocation) = store.reserve_create(&first, host, 1).await.unwrap()
    else {
        panic!("new")
    };
    sqlx::query("UPDATE operations SET status='unknown',lease_expires_at=clock_timestamp()-interval '1 second'").execute(&pool).await.unwrap();
    sqlx::query("UPDATE allocations SET lease_expires_at=clock_timestamp()-interval '1 hour'")
        .execute(&pool)
        .await
        .unwrap();
    let current = store
        .claim_next(OperationKind::Create, 30)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        store.reserve_create(&current, host, 1).await.unwrap(),
        Reservation::Existing(allocation)
    );
    assert!(matches!(
        store.reserve_create(&first, host, 1).await,
        Err(PlacementError::LostClaim)
    ));
    let (id, token) = project(&pool).await;
    let other = claim(&store, id, &token, 30).await;
    assert!(matches!(
        store.reserve_create(&other, host, 1).await,
        Err(PlacementError::Capacity)
    ));
}

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn draining_stale_and_wrong_epoch_hosts_are_ineligible(pool: PgPool) {
    let (store, claim, host) = fixture(&pool).await;
    assert!(matches!(
        store.reserve_create(&claim, host, 2).await,
        Err(PlacementError::HostUnavailable)
    ));
    sqlx::query("UPDATE hosts SET status='draining'")
        .execute(&pool)
        .await
        .unwrap();
    assert!(matches!(
        store.reserve_create(&claim, host, 1).await,
        Err(PlacementError::HostUnavailable)
    ));
    sqlx::query(
        "UPDATE hosts SET status='ready',last_seen_at=clock_timestamp()-interval '1 minute'",
    )
    .execute(&pool)
    .await
    .unwrap();
    assert!(matches!(
        store.reserve_create(&claim, host, 1).await,
        Err(PlacementError::HostUnavailable)
    ));
}

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn lease_expiring_while_waiting_rolls_back_reservation(pool: PgPool) {
    let store = Store::from_pool(pool.clone());
    let (id, token) = project(&pool).await;
    let claim = claim(&store, id, &token, 1).await;
    let host = host(&pool).await;
    let mut tx = pool.begin().await.unwrap();
    sqlx::query("SELECT id FROM hosts WHERE id=$1 FOR UPDATE")
        .bind(host.uuid())
        .execute(&mut *tx)
        .await
        .unwrap();
    let task = tokio::spawn(async move { store.reserve_create(&claim, host, 1).await });
    tokio::time::sleep(std::time::Duration::from_millis(1200)).await;
    tx.commit().await.unwrap();
    assert!(matches!(
        task.await.unwrap(),
        Err(PlacementError::LostClaim)
    ));
    let (count,): (i64,) = sqlx::query_as("SELECT count(*) FROM allocations")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(count, 0);
}

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn unknown_without_allocation_is_not_a_fresh_start(pool: PgPool) {
    let (store, claim, host) = fixture(&pool).await;
    sqlx::query("UPDATE operations SET status='unknown'")
        .execute(&pool)
        .await
        .unwrap();
    assert!(matches!(
        store.reserve_create(&claim, host, 1).await,
        Err(PlacementError::Reconcile)
    ));
}

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn project_resource_quotas_and_invalid_metadata_fail_closed(pool: PgPool) {
    let (store, claim, host) = fixture(&pool).await;
    for limits in [
        json!({"vcpu":1}),
        json!({"memory_mib":2047}),
        json!({"disk_mib":8191}),
        json!({"sandboxes":0}),
    ] {
        sqlx::query("UPDATE projects SET limits=$1")
            .bind(limits)
            .execute(&pool)
            .await
            .unwrap();
        assert!(matches!(
            store.reserve_create(&claim, host, 1).await,
            Err(PlacementError::Quota)
        ));
    }
    for limits in [json!({"vcpu":"unlimited"}), json!({"memory_mib":-1})] {
        sqlx::query("UPDATE projects SET limits=$1")
            .bind(limits)
            .execute(&pool)
            .await
            .unwrap();
        assert!(matches!(
            store.reserve_create(&claim, host, 1).await,
            Err(PlacementError::InvalidResources)
        ));
    }
    sqlx::query("UPDATE projects SET limits='{}'")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("UPDATE sandboxes SET resources='{\"vcpu\":2}'")
        .execute(&pool)
        .await
        .unwrap();
    assert!(matches!(
        store.reserve_create(&claim, host, 1).await,
        Err(PlacementError::InvalidResources)
    ));
}

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn expired_credentials_and_deadlines_cannot_reserve(pool: PgPool) {
    let (store, claim, host) = fixture(&pool).await;
    let original: (serde_json::Value,) = sqlx::query_as("SELECT api_tokens FROM projects")
        .fetch_one(&pool)
        .await
        .unwrap();
    for field in ["expires_at", "revoked_at"] {
        let mut tokens = original.0.clone();
        tokens[0][field] = json!("2000-01-01T00:00:00Z");
        sqlx::query("UPDATE projects SET api_tokens=$1")
            .bind(tokens)
            .execute(&pool)
            .await
            .unwrap();
        assert!(matches!(
            store.reserve_create(&claim, host, 1).await,
            Err(PlacementError::Unauthorized)
        ));
    }
    sqlx::query("UPDATE projects SET api_tokens=$1")
        .bind(original.0)
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("UPDATE operations SET deadline=clock_timestamp()-interval '1 second'")
        .execute(&pool)
        .await
        .unwrap();
    assert!(matches!(
        store.reserve_create(&claim, host, 1).await,
        Err(PlacementError::Unauthorized)
    ));
    sqlx::query("UPDATE operations SET deadline=NULL")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("UPDATE sandboxes SET expires_at=clock_timestamp()-interval '1 second'")
        .execute(&pool)
        .await
        .unwrap();
    assert!(matches!(
        store.reserve_create(&claim, host, 1).await,
        Err(PlacementError::Unauthorized)
    ));
}

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn deadline_expiring_during_host_wait_denies_reservation(pool: PgPool) {
    let (store, claim, host) = fixture(&pool).await;
    sqlx::query("UPDATE operations SET deadline=clock_timestamp()+interval '1 second'")
        .execute(&pool)
        .await
        .unwrap();
    let mut tx = pool.begin().await.unwrap();
    sqlx::query("SELECT id FROM hosts WHERE id=$1 FOR UPDATE")
        .bind(host.uuid())
        .execute(&mut *tx)
        .await
        .unwrap();
    let task = tokio::spawn(async move { store.reserve_create(&claim, host, 1).await });
    tokio::time::sleep(std::time::Duration::from_millis(1200)).await;
    tx.commit().await.unwrap();
    assert!(matches!(
        task.await.unwrap(),
        Err(PlacementError::Unauthorized)
    ));
    let (count,): (i64,) = sqlx::query_as("SELECT count(*) FROM allocations")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(count, 0);
}

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn host_observation_expiring_during_lock_wait_is_rejected(pool: PgPool) {
    let (store, claim, host) = fixture(&pool).await;
    sqlx::query("UPDATE hosts SET last_seen_at=clock_timestamp()-interval '29 seconds'")
        .execute(&pool)
        .await
        .unwrap();
    let mut tx = pool.begin().await.unwrap();
    sqlx::query("SELECT id FROM hosts WHERE id=$1 FOR UPDATE")
        .bind(host.uuid())
        .execute(&mut *tx)
        .await
        .unwrap();
    let task = tokio::spawn(async move { store.reserve_create(&claim, host, 1).await });
    tokio::time::sleep(std::time::Duration::from_millis(1200)).await;
    tx.commit().await.unwrap();
    assert!(matches!(
        task.await.unwrap(),
        Err(PlacementError::HostUnavailable)
    ));
    let (count,): (i64,) = sqlx::query_as("SELECT count(*) FROM allocations")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(count, 0);
}

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn legacy_undersized_sandboxes_do_not_reserve_capacity(pool: PgPool) {
    let (store, claim, host) = fixture(&pool).await;
    for resources in [
        json!({"vcpu":1,"memory_mib":127,"disk_mib":64}),
        json!({"vcpu":1,"memory_mib":128,"disk_mib":63}),
    ] {
        sqlx::query("UPDATE sandboxes SET resources=$2 WHERE id=$1")
            .bind(claim.sandbox_id.uuid())
            .bind(resources)
            .execute(&pool)
            .await
            .unwrap();
        assert!(matches!(
            store.reserve_create(&claim, host, 1).await,
            Err(PlacementError::InvalidResources)
        ));
    }
    let (count,): (i64,) = sqlx::query_as("SELECT count(*) FROM allocations")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(count, 0);
    let (generation,): (i64,) = sqlx::query_as("SELECT generation FROM sandboxes WHERE id=$1")
        .bind(claim.sandbox_id.uuid())
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(generation, 0);
    sqlx::query("UPDATE sandboxes SET resources=$2 WHERE id=$1")
        .bind(claim.sandbox_id.uuid())
        .bind(json!({"vcpu":1,"memory_mib":128,"disk_mib":64}))
        .execute(&pool)
        .await
        .unwrap();
    assert!(matches!(
        store.reserve_create(&claim, host, 1).await,
        Ok(Reservation::Reserved(_))
    ));
}

#[path = "support/allocation_serials.rs"]
mod allocation_serials;
