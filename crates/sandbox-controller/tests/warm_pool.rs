//! Warm-pool driver against the current (unsupported) supervisor protocol: it
//! must fail closed, never mark a member ready and never release a reservation.
#![allow(clippy::unwrap_used, clippy::expect_used)]
use sandbox_controller::warm_pool::{Step, UnsupportedWarmSupervisor, step};
use sandbox_protocol::{
    HostId, Id,
    host_capabilities::{Capabilities, Image, Limits},
};
use sandbox_store::{
    Store,
    host_registry::{HostPoolSpec, PlacementPolicy, PoolType, Tenancy},
    warm_pool::{MAX_ATTEMPTS, PolicySpec, Publication, TemplateSpec},
};
use serde_json::json;
use sha2::Digest;
use sqlx::PgPool;
use uuid::Uuid;

fn image() -> String {
    format!("sha256:{}", "a".repeat(64))
}

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn unsupported_supervisor_abandons_preparation_and_keeps_cleanup_pending(pg: PgPool) {
    let store = Store::from_pool(pg.clone());
    let pool = Uuid::now_v7();
    store
        .put_host_pool(&HostPoolSpec {
            version: 1,
            id: pool,
            name: "warm".into(),
            pool_type: PoolType::Standalone,
            min_ready_hosts: 0,
            max_hosts: 4,
            placement_policy: PlacementPolicy {
                version: 1,
                tenancy: Tenancy::Shared,
            },
            provider_config: json!({"version":1}),
            credential_secret_ref: None,
        })
        .await
        .unwrap();
    let caps = serde_json::to_value(Capabilities {
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
            rootfs_digest: image(),
            kernel_digest: format!("sha256:{}", "b".repeat(64)),
            minimum_disk_mib: 256,
            guest_version: env!("CARGO_PKG_VERSION").into(),
            bootstrap_version: 1,
            guest_protocol_version: 1,
        }],
    })
    .unwrap();
    let host = HostId::generate();
    sqlx::query("INSERT INTO hosts(id,status,cpu_capacity,memory_capacity_mib,disk_capacity_mib,supervisor_epoch,
        last_seen_at,host_pool_id,endpoint,transport_server_name,transport_certificate_sha256,
        compatibility,runtime_capabilities,runtime_capabilities_epoch)
        VALUES($1,'ready',4,4096,8192,1,clock_timestamp(),$2,'https://10.2.0.1:7443',$3,$4,$5,$5,1)")
        .bind(host.uuid()).bind(pool).bind(format!("host-{}.sandbox.internal", host.uuid()))
        .bind(hex::encode(sha2::Sha256::digest(host.uuid().as_bytes()))).bind(caps)
        .execute(&pg).await.unwrap();
    let template = TemplateSpec {
        id: Uuid::now_v7(),
        name: "node".into(),
        version: 1,
        owner_project_id: None,
        publication: Publication::Shared,
        image_digest: image(),
        vcpu: 1,
        memory_mib: 512,
        disk_mib: 1024,
        boot_config: json!({"version":1}),
        compatibility: json!({"version":1}),
        snapshot_id: None,
    };
    store.put_sandbox_template(&template).await.unwrap();
    let policy = PolicySpec {
        id: Uuid::now_v7(),
        template_id: template.id,
        host_pool_id: pool,
        owner_project_id: None,
        min_ready: 1,
        max_total: 1,
        max_per_host: 1,
        idle_ttl_seconds: 600,
        enabled: true,
    };
    store.put_warm_pool_policy(&policy).await.unwrap();
    store.reconcile_warm_pool(policy.id, 4).await.unwrap();
    let supervisor = UnsupportedWarmSupervisor;
    for _ in 1..MAX_ATTEMPTS {
        assert_eq!(
            step(&store, &supervisor, policy.id, 30).await.unwrap(),
            Step::Retried
        );
    }
    assert_eq!(
        step(&store, &supervisor, policy.id, 30).await.unwrap(),
        Step::Abandoned
    );
    let state = |pg: PgPool| async move {
        sqlx::query_scalar::<_, String>("SELECT state FROM warm_pool_members")
            .fetch_one(&pg)
            .await
            .unwrap()
    };
    assert_eq!(state(pg.clone()).await, "retiring");
    // Cleanup without a receipt never completes; the reservation stays held.
    for _ in 1..MAX_ATTEMPTS {
        assert_eq!(
            step(&store, &supervisor, policy.id, 30).await.unwrap(),
            Step::Retried
        );
    }
    assert_eq!(
        step(&store, &supervisor, policy.id, 30).await.unwrap(),
        Step::CleanupPending
    );
    assert_eq!(state(pg.clone()).await, "retiring");
    // Max_total includes the retiring member: no replacement is reserved.
    assert!(
        store
            .reconcile_warm_pool(policy.id, 4)
            .await
            .unwrap()
            .prepare
            .is_empty()
    );
    let report = store.host_maintenance(host, 1).await.unwrap();
    assert_eq!(report.unretired_warm_members, 1);
}
