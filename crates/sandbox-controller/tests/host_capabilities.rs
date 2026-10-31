//! Capability advertisements traverse configured mTLS; all runtime evidence is simulated.
#![allow(clippy::unwrap_used, clippy::expect_used)]
#[allow(dead_code)]
mod common;
use common::Fixture;
use sandbox_controller::{Controller, ControllerError, Tick};
use sandbox_protocol::{
    Id,
    host_capabilities::{Capabilities, Image, Limits},
};
use sqlx::PgPool;

fn capabilities() -> Capabilities {
    Capabilities {
        version: 1,
        simulated: true,
        architecture: "x86_64".into(),
        host_kernel_release: "6.8.0-fixture".into(),
        kvm_api_version: 12,
        cgroup_version: 2,
        supervisor_version: env!("CARGO_PKG_VERSION").into(),
        firecracker_version: "1.17.0".into(),
        firecracker_digest: format!("sha256:{}", "f".repeat(64)),
        jailer_digest: format!("sha256:{}", "e".repeat(64)),
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
    }
}

async fn connect(f: &Fixture) -> Result<Controller, ControllerError> {
    Controller::connect(
        f.store.clone(),
        f.config.clone(),
        f.ca.as_bytes(),
        f.cert.as_bytes(),
        f.key.as_bytes(),
    )
    .await
}

async fn enroll(f: &Fixture, value: &Capabilities) {
    f.store.drain_host(f.config.host, 1).await.unwrap();
    f.store
        .set_host_capability_policy(f.config.host, 1, value)
        .await
        .unwrap();
    f.fake.set_capabilities(value.clone()).await.unwrap();
    connect(f).await.unwrap();
    f.store.finish_host_drain(f.config.host, 1).await.unwrap();
}

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn missing_or_changed_advertisement_never_dispatches_and_does_not_release_old_work(
    pool: PgPool,
) {
    let f = Fixture::new(&pool).await;
    let value = capabilities();
    f.store.drain_host(f.config.host, 1).await.unwrap();
    f.store
        .set_host_capability_policy(f.config.host, 1, &value)
        .await
        .unwrap();
    assert!(!connect(&f).await.unwrap().new_work_compatible());
    assert_eq!(f.fake.total_starts().await, 0);
    f.fake.set_capabilities(value.clone()).await.unwrap();
    let mut controller = connect(&f).await.unwrap();
    f.store.finish_host_drain(f.config.host, 1).await.unwrap();
    let (_, sandbox) = f.admit().await;
    assert_eq!(controller.tick().await.unwrap(), Tick::Confirmed);
    let before: (uuid::Uuid, i64, i64, Option<time::OffsetDateTime>) =
        sqlx::query_as("SELECT id,generation,supervisor_epoch,released_at FROM allocations")
            .fetch_one(&pool)
            .await
            .unwrap();
    let mut changed = value;
    changed.architecture = "aarch64".into();
    f.fake.set_capabilities(changed).await.unwrap();
    f.admit().await;
    assert_eq!(controller.tick().await.unwrap(), Tick::Deferred);
    assert!(!controller.new_work_compatible());
    let after =
        sqlx::query_as("SELECT id,generation,supervisor_epoch,released_at FROM allocations")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(before, after);
    assert_eq!(f.fake.total_starts().await, 1);
    // Declared compatibility failure must not strand the original allocation:
    // explicit, authenticated destroy still uses its retained ownership.
    let (_, destroyed) = f
        .send(
            "POST",
            &format!("/v1/sandboxes/{sandbox}/destroy"),
            serde_json::json!({}),
        )
        .await;
    let operation = destroyed["operation_id"].as_str().unwrap();
    for _ in 0..12 {
        controller.tick().await.unwrap();
        let (_, body) = f
            .send(
                "GET",
                &format!("/v1/operations/{operation}"),
                serde_json::Value::Null,
            )
            .await;
        if body["status"] == "succeeded" {
            break;
        }
    }
    let (_, body) = f
        .send(
            "GET",
            &format!("/v1/operations/{operation}"),
            serde_json::Value::Null,
        )
        .await;
    assert_eq!(body["status"], "succeeded");
    assert_eq!(
        f.store
            .host_maintenance(f.config.host, 1)
            .await
            .unwrap()
            .outstanding_allocations,
        0
    );
    assert_eq!(f.fake.total_starts().await, 1);
}

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn two_identities_keep_distinct_policies_and_simulation_cannot_be_hidden(pool: PgPool) {
    let a = Fixture::new(&pool).await;
    let b = Fixture::new(&pool).await;
    let first = capabilities();
    let mut second = first.clone();
    second.architecture = "aarch64".into();
    enroll(&a, &first).await;
    enroll(&b, &second).await;
    let policies: Vec<serde_json::Value> =
        sqlx::query_scalar("SELECT runtime_capabilities FROM hosts")
            .fetch_all(&pool)
            .await
            .unwrap();
    assert!(policies.contains(&serde_json::to_value(first.clone()).unwrap()));
    assert!(policies.contains(&serde_json::to_value(second).unwrap()));
    let mut real = first;
    real.simulated = false;
    assert!(a.fake.set_capabilities(real).await.is_err());
    let mut cfg = a.config.clone();
    cfg.allow_simulated = false;
    assert!(matches!(
        Controller::connect(
            a.store.clone(),
            cfg,
            a.ca.as_bytes(),
            a.cert.as_bytes(),
            a.key.as_bytes()
        )
        .await,
        Err(ControllerError::HostIdentity)
    ));
    // B's valid advertisement cannot refresh A's pinned policy even with the
    // shared DB: each controller verifies its own server certificate and ID.
    let seen: Option<serde_json::Value> =
        sqlx::query_scalar("SELECT runtime_capabilities FROM hosts WHERE id=$1")
            .bind(a.config.host.uuid())
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(seen.unwrap()["architecture"], "x86_64");
}
