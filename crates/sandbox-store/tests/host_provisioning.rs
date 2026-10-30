//! Offline operator provisioning against isolated PostgreSQL; no runtime host is started.
#![allow(clippy::unwrap_used, clippy::expect_used)]
use sandbox_protocol::{
    HostId, Id, IdempotencyKey, ProjectId, ProjectToken, RequestDigest,
    host_capabilities::{Capabilities, Image, Limits},
};
use sandbox_store::{
    Store,
    admission::{CreateSandbox, Resources},
    claims::OperationKind,
    host_maintenance::{Error, HostCapacity, HostProvision},
};
use serde_json::{Value, json};
use sqlx::PgPool;

fn request() -> HostProvision {
    HostProvision {
        version: 1,
        capacity: HostCapacity {
            vcpu: 4,
            memory_mib: 8192,
            disk_mib: 65536,
        },
        capabilities: Capabilities {
            version: 1,
            simulated: true,
            architecture: "x86_64".into(),
            host_kernel_release: "6.8.0-fixture".into(),
            kvm_api_version: 12,
            cgroup_version: 2,
            supervisor_version: env!("CARGO_PKG_VERSION").into(),
            firecracker_version: "1.17.0".into(),
            firecracker_digest: format!("sha256:{}", "c".repeat(64)),
            jailer_digest: format!("sha256:{}", "d".repeat(64)),
            limits: Limits {
                vcpu: 4,
                memory_mib: 8192,
                disk_mib: 65536,
            },
            images: vec![Image {
                rootfs_digest: format!("sha256:{}", "a".repeat(64)),
                kernel_digest: format!("sha256:{}", "b".repeat(64)),
                minimum_disk_mib: 256,
                guest_version: env!("CARGO_PKG_VERSION").into(),
                bootstrap_version: 1,
                guest_protocol_version: 1,
            }],
        },
        host_pool_id: None,
    }
}
async fn row(pool: &PgPool, id: HostId) -> Value {
    sqlx::query_scalar("SELECT to_jsonb(hosts) FROM hosts WHERE id=$1")
        .bind(id.uuid())
        .fetch_one(pool)
        .await
        .unwrap()
}

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn new_host_stays_draining_until_authenticated_health_and_explicit_reopening(pool: PgPool) {
    let store = Store::from_pool(pool.clone());
    let id = HostId::generate();
    let input = request();
    let first = store.provision_host(id, 1, &input).await.unwrap();
    assert_eq!(first.status, "draining");
    assert_eq!(first.outstanding_allocations, 0);
    let saved = row(&pool, id).await;
    assert!(saved["last_seen_at"].is_null());
    assert!(saved["runtime_capabilities"].is_null());
    assert_eq!(
        saved["compatibility"],
        serde_json::to_value(&input.capabilities).unwrap()
    );
    assert_eq!(store.provision_host(id, 1, &input).await.unwrap(), first);
    assert_eq!(row(&pool, id).await, saved);
    assert!(matches!(
        store.finish_host_drain(id, 1).await,
        Err(Error::Outstanding)
    ));
    store
        .observe_configured_host_with_capabilities(
            id,
            1,
            &input.capabilities.to_json().unwrap(),
            true,
        )
        .await
        .unwrap();
    assert_eq!(
        store.host_maintenance(id, 1).await.unwrap().status,
        "draining"
    );
    store.finish_host_drain(id, 1).await.unwrap();
    let ready = row(&pool, id).await;
    assert_eq!(
        store.provision_host(id, 1, &input).await.unwrap().status,
        "ready"
    );
    assert_eq!(
        row(&pool, id).await,
        ready,
        "retry must preserve observation and timestamps"
    );
    sqlx::query("UPDATE hosts SET status='unhealthy' WHERE id=$1")
        .bind(id.uuid())
        .execute(&pool)
        .await
        .unwrap();
    let unhealthy = row(&pool, id).await;
    assert_eq!(
        store.provision_host(id, 1, &input).await.unwrap().status,
        "unhealthy"
    );
    assert_eq!(row(&pool, id).await, unhealthy);
}

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn competing_exact_retries_create_one_host(pool: PgPool) {
    let store = Store::from_pool(pool.clone());
    let id = HostId::generate();
    let input = request();
    let (first, second) = tokio::join!(
        store.provision_host(id, 1, &input),
        store.provision_host(id, 1, &input)
    );
    assert_eq!(first.unwrap(), second.unwrap());
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM hosts")
            .fetch_one(&pool)
            .await
            .unwrap(),
        1
    );
}

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn changed_inputs_or_epoch_never_overwrite_existing_host(pool: PgPool) {
    let store = Store::from_pool(pool.clone());
    let id = HostId::generate();
    let input = request();
    store.provision_host(id, 1, &input).await.unwrap();
    let original = row(&pool, id).await;
    let mut capacity = input.clone();
    capacity.capacity.vcpu = 8;
    let mut compatibility = input.clone();
    compatibility.capabilities.host_kernel_release = "6.8.0-other".into();
    for changed in [capacity, compatibility] {
        assert!(matches!(
            store.provision_host(id, 1, &changed).await,
            Err(Error::Conflict)
        ));
        assert_eq!(row(&pool, id).await, original);
    }
    assert!(matches!(
        store.provision_host(id, 2, &input).await,
        Err(Error::Conflict)
    ));
    assert_eq!(row(&pool, id).await, original);
}

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn competing_different_configuration_has_one_winner(pool: PgPool) {
    let store = Store::from_pool(pool.clone());
    let id = HostId::generate();
    let first = request();
    let mut second = first.clone();
    second.capacity.vcpu = 8;
    let (a, b) = tokio::join!(
        store.provision_host(id, 1, &first),
        store.provision_host(id, 1, &second)
    );
    assert_eq!(usize::from(a.is_ok()) + usize::from(b.is_ok()), 1);
    assert!(matches!(
        (a, b),
        (Ok(_), Err(Error::Conflict)) | (Err(Error::Conflict), Ok(_))
    ));
}

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn invalid_or_legacy_configuration_is_never_implicitly_enrolled(pool: PgPool) {
    let store = Store::from_pool(pool.clone());
    let id = HostId::generate();
    let input = request();
    for epoch in [0, -1] {
        assert!(matches!(
            store.provision_host(id, epoch, &input).await,
            Err(Error::Conflict)
        ));
    }
    let mut invalid = input.clone();
    invalid.capacity.memory_mib = u64::MAX;
    assert!(matches!(
        store.provision_host(id, 1, &invalid).await,
        Err(Error::Conflict)
    ));
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM hosts")
            .fetch_one(&pool)
            .await
            .unwrap(),
        0
    );
    sqlx::query("INSERT INTO hosts(id,status,cpu_capacity,memory_capacity_mib,disk_capacity_mib,supervisor_epoch) VALUES($1,'ready',4,8192,65536,1)")
        .bind(id.uuid()).execute(&pool).await.unwrap();
    let original = row(&pool, id).await;
    assert!(matches!(
        store.provision_host(id, 1, &input).await,
        Err(Error::Conflict)
    ));
    assert_eq!(row(&pool, id).await, original);
}

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn an_exact_retry_on_a_busy_draining_host_preserves_reservations_and_claims(pool: PgPool) {
    let store = Store::from_pool(pool.clone());
    let id = HostId::generate();
    let input = request();
    store.provision_host(id, 1, &input).await.unwrap();
    store
        .observe_configured_host_with_capabilities(
            id,
            1,
            &input.capabilities.to_json().unwrap(),
            true,
        )
        .await
        .unwrap();
    store.finish_host_drain(id, 1).await.unwrap();
    let project = ProjectId::generate();
    let token = ProjectToken::generate().unwrap();
    sqlx::query("INSERT INTO projects(id,name,status,limits,api_tokens) VALUES($1,'provisioning-test','active','{}',$2)")
        .bind(project.uuid()).bind(json!([{"key_id":token.key_id().as_str(),"hash":hex::encode(token.hash().as_bytes())}]))
        .execute(&pool).await.unwrap();
    let digest = input.capabilities.images[0].rootfs_digest.clone();
    let payload =
        json!({"image_digest":digest,"resources":{"vcpu":1,"memory_mib":128,"disk_mib":256}});
    store
        .admit_create_sandbox(
            &CreateSandbox {
                project_id: project,
                key_id: token.key_id().clone(),
                idempotency_key: IdempotencyKey::parse("retain-host-allocation").unwrap(),
                request_digest: RequestDigest::compute("POST", "/v1/sandboxes", &payload).unwrap(),
                image_digest: digest.clone(),
                name: None,
                resources: Resources {
                    vcpu: 1,
                    memory_mib: 128,
                    disk_mib: 256,
                },
                payload,
            },
            &sandbox_protocol::images::ImageAllowlist::new([digest]).unwrap(),
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
    let host_before = row(&pool, id).await;
    let allocation: Value = sqlx::query_scalar("SELECT to_jsonb(allocations) FROM allocations")
        .fetch_one(&pool)
        .await
        .unwrap();
    let operation: Value = sqlx::query_scalar("SELECT to_jsonb(operations) FROM operations")
        .fetch_one(&pool)
        .await
        .unwrap();
    let report = store.provision_host(id, 1, &input).await.unwrap();
    assert_eq!(
        (
            report.status.as_str(),
            report.outstanding_allocations,
            report.reserved_vcpu,
            report.reserved_memory_mib,
            report.reserved_disk_mib
        ),
        ("draining", 1, 1, 128, 256)
    );
    assert_eq!(row(&pool, id).await, host_before);
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
}

#[test]
fn operator_document_rejects_unknown_duplicates_and_invalid_bounds() {
    let valid = serde_json::to_value(request()).unwrap();
    assert!(HostProvision::from_json(valid.to_string().as_bytes()).is_ok());
    for (field, bad) in [
        ("version", json!(2)),
        ("capacity", json!({})),
        ("capabilities", json!({})),
        ("unknown", json!(true)),
    ] {
        let mut changed = valid.clone();
        changed[field] = bad;
        assert!(HostProvision::from_json(changed.to_string().as_bytes()).is_err());
    }
    let mut changed = valid.clone();
    changed["capacity"]["vcpu"] = json!(1);
    assert!(HostProvision::from_json(changed.to_string().as_bytes()).is_err());
    assert!(HostProvision::from_json(&vec![b' '; 65537]).is_err());
    let duplicate = valid
        .to_string()
        .replacen("\"version\":1", "\"version\":1,\"version\":1", 1);
    assert!(HostProvision::from_json(duplicate.as_bytes()).is_err());
}
