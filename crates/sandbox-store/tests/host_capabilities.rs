//! Enrollment, downgrade and placement fencing against real isolated PostgreSQL.
#![allow(clippy::unwrap_used, clippy::expect_used)]
use sandbox_protocol::{
    HostId, Id, IdempotencyKey, ProjectId, ProjectToken, RequestDigest,
    host_capabilities::{Capabilities, Image, Limits},
};
use sandbox_store::{
    Store,
    admission::{CreateSandbox, Resources},
    claims::OperationKind,
    dispatch::DispatchError,
    host_maintenance,
    placement::{PlacementError, Reservation},
};
use serde_json::{Value, json};
use sqlx::PgPool;
use std::collections::BTreeSet;

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn a_new_epoch_needs_its_own_capability_observation_even_when_prior_health_is_fresh(
    pool: PgPool,
) {
    let (store, id) = host(&pool).await;
    let value = capabilities();
    enroll(&store, id, &value).await;
    let claim = claim(&store, 256).await;
    // An operator has issued a new epoch; old health is still within 30 seconds.
    sqlx::query("UPDATE hosts SET supervisor_epoch=2")
        .execute(&pool)
        .await
        .unwrap();
    assert!(matches!(
        store.reserve_create(&claim, id, 2).await,
        Err(PlacementError::HostCompatibility)
    ));
    assert!(matches!(
        store
            .observe_configured_host_with_capabilities(id, 1, &value.to_json().unwrap(), true)
            .await,
        Err(DispatchError::HostUnavailable)
    ));
    store
        .observe_configured_host_with_capabilities(id, 2, &value.to_json().unwrap(), true)
        .await
        .unwrap();
    assert!(matches!(
        store.reserve_create(&claim, id, 2).await.unwrap(),
        Reservation::Reserved(_)
    ));
}

#[sqlx::test(migrations = false)]
async fn additive_upgrade_preserves_legacy_host_capacity_epoch_and_policy(pool: PgPool) {
    let old = sqlx::migrate::Migrator {
        migrations: std::borrow::Cow::Owned(
            sandbox_store::MIGRATOR
                .iter()
                .filter(|m| m.version < 23)
                .cloned()
                .collect(),
        ),
        ..sqlx::migrate::Migrator::DEFAULT
    };
    old.run(&pool).await.unwrap();
    let (store, id) = host(&pool).await;
    sandbox_store::MIGRATOR.run(&pool).await.unwrap();
    sandbox_store::MIGRATOR.run(&pool).await.unwrap();
    let (epoch, cpu, policy, caps): (i64,i32,Value,Option<Value>) = sqlx::query_as("SELECT supervisor_epoch,cpu_capacity,compatibility,runtime_capabilities FROM hosts WHERE id=$1")
        .bind(id.uuid()).fetch_one(&pool).await.unwrap();
    assert_eq!((epoch, cpu, policy, caps), (1, 4, json!({}), None));
    store.observe_configured_host(id, 1).await.unwrap();
    let request = claim(&store, 64).await;
    assert!(matches!(
        store.reserve_create(&request, id, 1).await.unwrap(),
        Reservation::Reserved(_)
    ));
}

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn health_releases_host_lock_before_reconciling_prior_epoch_sandbox(pool: PgPool) {
    let (store, id) = host(&pool).await;
    let caps = capabilities();
    enroll(&store, id, &caps).await;
    let request = claim(&store, 256).await;
    store.reserve_create(&request, id, 1).await.unwrap();
    sqlx::query("UPDATE sandboxes SET observed_state='running',active_transition_operation_id=NULL,observed_at=clock_timestamp(),observation_simulated=true").execute(&pool).await.unwrap();
    sqlx::query("UPDATE hosts SET supervisor_epoch=2,last_seen_at=NULL")
        .execute(&pool)
        .await
        .unwrap();
    let mut placement = pool.begin().await.unwrap();
    sqlx::query("SELECT id FROM sandboxes WHERE id=$1 FOR UPDATE")
        .bind(request.sandbox_id.uuid())
        .fetch_one(&mut *placement)
        .await
        .unwrap();
    let observer = store.clone();
    let bytes = caps.to_json().unwrap();
    let health = tokio::spawn(async move {
        observer
            .observe_configured_host_with_capabilities(id, 2, &bytes, true)
            .await
    });
    // Health may wait for our sandbox, but its host lock must already be released.
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            let seen: bool =
                sqlx::query_scalar("SELECT last_seen_at IS NOT NULL FROM hosts WHERE id=$1")
                    .bind(id.uuid())
                    .fetch_one(&pool)
                    .await
                    .unwrap();
            if seen {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        sqlx::query("SELECT id FROM hosts WHERE id=$1 FOR UPDATE NOWAIT")
            .bind(id.uuid())
            .fetch_one(&mut *placement)
            .await
            .unwrap();
    })
    .await
    .unwrap();
    placement.commit().await.unwrap();
    health.await.unwrap().unwrap();
    let state: String = sqlx::query_scalar("SELECT observed_state FROM sandboxes")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(state, "unknown");
}

fn capabilities() -> Capabilities {
    Capabilities {
        version: 1,
        simulated: true,
        architecture: "x86_64".into(),
        host_kernel_release: "6.8.0-fixture".into(),
        kvm_api_version: 12,
        cgroup_version: 2,
        supervisor_version: "0.0.0".into(),
        firecracker_version: "1.17.0".into(),
        firecracker_digest: format!("sha256:{}", "f".repeat(64)),
        jailer_digest: format!("sha256:{}", "e".repeat(64)),
        limits: Limits {
            vcpu: 2,
            memory_mib: 2048,
            disk_mib: 8192,
        },
        images: vec![Image {
            rootfs_digest: format!("sha256:{}", "a".repeat(64)),
            kernel_digest: format!("sha256:{}", "b".repeat(64)),
            minimum_disk_mib: 256,
            guest_version: "0.0.0".into(),
            bootstrap_version: 1,
            guest_protocol_version: 1,
        }],
    }
}

async fn host(pool: &PgPool) -> (Store, HostId) {
    let id = HostId::generate();
    sqlx::query("INSERT INTO hosts(id,status,cpu_capacity,memory_capacity_mib,disk_capacity_mib,supervisor_epoch,last_seen_at)
        VALUES($1,'ready',4,8192,65536,1,clock_timestamp())")
        .bind(id.uuid()).execute(pool).await.unwrap();
    (Store::from_pool(pool.clone()), id)
}

async fn enroll(store: &Store, host: HostId, value: &Capabilities) {
    store.drain_host(host, 1).await.unwrap();
    store
        .set_host_capability_policy(host, 1, value)
        .await
        .unwrap();
    store
        .observe_configured_host_with_capabilities(host, 1, &value.to_json().unwrap(), true)
        .await
        .unwrap();
    store.finish_host_drain(host, 1).await.unwrap();
}

async fn claim(store: &Store, disk_mib: i64) -> sandbox_store::claims::Claim {
    let project = ProjectId::generate();
    let token = ProjectToken::generate().unwrap();
    sqlx::query(
        "INSERT INTO projects(id,name,status,limits,api_tokens) VALUES($1,'capability-test','active','{}',$2)",
    )
    .bind(project.uuid())
    .bind(json!([{"key_id":token.key_id().as_str(),"hash":hex::encode(token.hash().as_bytes())}]))
    .execute(store.pool())
    .await
    .unwrap();
    // Placement requires an explicit grant for the fixture host's legacy pool.
    store
        .grant_host_pool_access(project, sandbox_store::host_registry::LEGACY_POOL_ID)
        .await
        .unwrap();
    let digest = capabilities().images[0].rootfs_digest.clone();
    let payload =
        json!({"image_digest":digest,"resources":{"vcpu":1,"memory_mib":128,"disk_mib":disk_mib}});
    store
        .admit_create_sandbox(
            &CreateSandbox {
                project_id: project,
                key_id: token.key_id().clone(),
                idempotency_key: IdempotencyKey::parse(&uuid::Uuid::now_v7().to_string()).unwrap(),
                request_digest: RequestDigest::compute("POST", "/v1/sandboxes", &payload).unwrap(),
                image_digest: digest.clone(),
                name: None,
                resources: Resources {
                    vcpu: 1,
                    memory_mib: 128,
                    disk_mib,
                },
                payload,
            },
            &sandbox_protocol::images::ImageAllowlist::new([digest]).unwrap(),
        )
        .await
        .unwrap();
    store
        .claim_next(OperationKind::Create, 30)
        .await
        .unwrap()
        .unwrap()
}

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn enrollment_requires_operator_policy_and_never_registers_an_unknown_host(pool: PgPool) {
    let (store, host) = host(&pool).await;
    let value = capabilities();
    assert!(matches!(
        store
            .observe_configured_host_with_capabilities(host, 1, &value.to_json().unwrap(), true)
            .await,
        Err(DispatchError::HostCompatibility)
    ));
    assert!(matches!(
        store.set_host_capability_policy(host, 1, &value).await,
        Err(host_maintenance::Error::Conflict)
    ));
    enroll(&store, host, &value).await;
    assert!(matches!(
        store
            .observe_configured_host_with_capabilities(
                HostId::generate(),
                1,
                &value.to_json().unwrap(),
                true
            )
            .await,
        Err(DispatchError::HostUnavailable)
    ));
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM hosts")
            .fetch_one(&pool)
            .await
            .unwrap(),
        1
    );
    let (status, epoch, cpu): (String, i64, i32) =
        sqlx::query_as("SELECT status,supervisor_epoch,cpu_capacity FROM hosts")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!((status.as_str(), epoch, cpu), ("ready", 1, 4));
}

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn health_cannot_downgrade_spoof_simulation_or_overwrite_policy_at_stale_epoch(pool: PgPool) {
    let (store, host) = host(&pool).await;
    let value = capabilities();
    enroll(&store, host, &value).await;
    let before: (Value, Option<Value>, Option<time::OffsetDateTime>) =
        sqlx::query_as("SELECT compatibility,runtime_capabilities,last_seen_at FROM hosts")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert!(matches!(
        store.observe_configured_host(host, 1).await,
        Err(DispatchError::HostCompatibility)
    ));
    assert!(matches!(
        store
            .observe_configured_host_with_capabilities(host, 1, &value.to_json().unwrap(), false)
            .await,
        Err(DispatchError::HostCompatibility)
    ));
    assert!(matches!(
        store
            .observe_configured_host_with_capabilities(host, 2, &value.to_json().unwrap(), true)
            .await,
        Err(DispatchError::HostUnavailable)
    ));
    let mut wrong = value.clone();
    wrong.architecture = "aarch64".into();
    assert!(matches!(
        store
            .observe_configured_host_with_capabilities(host, 1, &wrong.to_json().unwrap(), true)
            .await,
        Err(DispatchError::HostCompatibility)
    ));
    let after = sqlx::query_as("SELECT compatibility,runtime_capabilities,last_seen_at FROM hosts")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(before, after);
    store.drain_host(host, 1).await.unwrap();
    store
        .set_host_capability_policy(host, 1, &wrong)
        .await
        .unwrap();
    assert!(matches!(
        store.finish_host_drain(host, 1).await,
        Err(host_maintenance::Error::Outstanding)
    ));
    assert!(
        store
            .observe_configured_host_with_capabilities(host, 1, &value.to_json().unwrap(), true)
            .await
            .is_err()
    );
    store
        .observe_configured_host_with_capabilities(host, 1, &wrong.to_json().unwrap(), true)
        .await
        .unwrap();
    assert_eq!(
        store.host_maintenance(host, 1).await.unwrap().status,
        "draining"
    );
    store.finish_host_drain(host, 1).await.unwrap();
}

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn image_minimum_and_limits_are_checked_before_reserving_and_existing_work_is_preserved(
    pool: PgPool,
) {
    let (store, host) = host(&pool).await;
    let value = capabilities();
    enroll(&store, host, &value).await;
    let claim = claim(&store, 64).await;
    assert!(matches!(
        store.reserve_create(&claim, host, 1).await,
        Err(PlacementError::HostCompatibility)
    ));
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM allocations")
            .fetch_one(&pool)
            .await
            .unwrap(),
        0
    );
    sqlx::query("UPDATE sandboxes SET resources=jsonb_set(resources,'{disk_mib}','256')")
        .execute(&pool)
        .await
        .unwrap();
    let Reservation::Reserved(allocation) = store.reserve_create(&claim, host, 1).await.unwrap()
    else {
        panic!("new");
    };
    assert!(matches!(
        store.set_host_capability_policy(host, 1, &value).await,
        Err(host_maintenance::Error::Conflict)
    ));
    store.drain_host(host, 1).await.unwrap();
    assert!(matches!(
        store.set_host_capability_policy(host, 1, &value).await,
        Err(host_maintenance::Error::Outstanding)
    ));
    // Corrupt current policy cannot erase an already retained incarnation.
    sqlx::query("UPDATE hosts SET compatibility='{}'")
        .execute(&pool)
        .await
        .unwrap();
    assert_eq!(
        store.reserve_create(&claim, host, 1).await.unwrap(),
        Reservation::Existing(allocation)
    );
    assert!(store.observe_configured_host(host, 1).await.is_err());
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM allocation_permits")
            .fetch_one(&pool)
            .await
            .unwrap(),
        1
    );
}

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn dispatch_rechecks_policy_without_recording_an_intent_and_inspects_old_intents(
    pool: PgPool,
) {
    let (store, host) = host(&pool).await;
    let value = capabilities();
    enroll(&store, host, &value).await;
    let claim = claim(&store, 256).await;
    store.reserve_create(&claim, host, 1).await.unwrap();
    let images = BTreeSet::from([value.images[0].rootfs_digest.clone()]);
    sqlx::query("UPDATE hosts SET runtime_capabilities=NULL")
        .execute(&pool)
        .await
        .unwrap();
    assert!(matches!(
        store.prepare_create_dispatch(&claim, &images).await,
        Err(DispatchError::HostCompatibility)
    ));
    assert_eq!(
        sqlx::query_scalar::<_, i32>("SELECT attempt_count FROM operations")
            .fetch_one(&pool)
            .await
            .unwrap(),
        0
    );
    store
        .observe_configured_host_with_capabilities(host, 1, &value.to_json().unwrap(), true)
        .await
        .unwrap();
    assert!(matches!(
        store
            .prepare_create_dispatch(&claim, &images)
            .await
            .unwrap(),
        sandbox_store::dispatch::CreateAction::Start(_)
    ));
    sqlx::query("UPDATE hosts SET compatibility='{}'")
        .execute(&pool)
        .await
        .unwrap();
    assert!(matches!(
        store
            .prepare_create_dispatch(&claim, &images)
            .await
            .unwrap(),
        sandbox_store::dispatch::CreateAction::Inspect(_)
    ));
    assert_eq!(
        sqlx::query_scalar::<_, i32>("SELECT attempt_count FROM operations")
            .fetch_one(&pool)
            .await
            .unwrap(),
        1
    );
}
