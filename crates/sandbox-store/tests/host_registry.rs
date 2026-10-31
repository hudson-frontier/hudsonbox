//! Host registry, pools and project pool access against isolated PostgreSQL.
//! Hosts are simulated database identities; no supervisor or VM is started.
#![allow(clippy::unwrap_used, clippy::expect_used)]
use sandbox_protocol::{
    HostId, Id, ProjectId,
    host_capabilities::{Capabilities, Image, Limits},
};
use sandbox_store::{
    MIGRATOR, Store,
    host_maintenance::{self, HostCapacity, HostProvision},
    host_registry::{
        HostPoolSpec, HostTransport, LEGACY_POOL_ID, PlacementPolicy, PoolType, RegistryError,
        Tenancy,
    },
};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use sqlx::{PgPool, migrate::Migrator};
use std::borrow::Cow;
use uuid::Uuid;

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
    }
}

fn provision(pool: Option<Uuid>) -> HostProvision {
    HostProvision {
        version: 1,
        capacity: HostCapacity {
            vcpu: 4,
            memory_mib: 8192,
            disk_mib: 65536,
        },
        capabilities: capabilities(),
        host_pool_id: pool,
    }
}

fn spec(id: Uuid, name: &str) -> HostPoolSpec {
    HostPoolSpec {
        version: 1,
        id,
        name: name.into(),
        pool_type: PoolType::Standalone,
        min_ready_hosts: 0,
        max_hosts: 2,
        placement_policy: PlacementPolicy {
            version: 1,
            tenancy: Tenancy::Shared,
        },
        provider_config: json!({"version": 1}),
        credential_secret_ref: None,
    }
}

fn k8s_config() -> Value {
    json!({"version": 1, "namespace": "hudsonbox-workers", "worker_label_selector": "app=supervisor"})
}

fn leaf(n: u8) -> Vec<u8> {
    vec![n; 32]
}

fn transport(n: u8) -> HostTransport {
    HostTransport {
        version: 1,
        endpoint: format!("https://10.0.0.{n}:7443"),
        certificate_sha256: hex::encode(Sha256::digest(leaf(n))),
        provider_ref: Some(format!("rack-a-{n}")),
    }
}

async fn row(pool: &PgPool, id: HostId) -> Value {
    sqlx::query_scalar("SELECT to_jsonb(hosts) FROM hosts WHERE id=$1")
        .bind(id.uuid())
        .fetch_one(pool)
        .await
        .unwrap()
}

async fn project(pool: &PgPool) -> ProjectId {
    let id = ProjectId::generate();
    sqlx::query("INSERT INTO projects(id,name,status,limits) VALUES($1,'registry','active','{}')")
        .bind(id.uuid())
        .execute(pool)
        .await
        .unwrap();
    id
}

/// Provision, observe and register `n` in `pool` at epoch 1 (left draining).
async fn registered(store: &Store, pool: Option<Uuid>, n: u8) -> HostId {
    let id = HostId::generate();
    store.provision_host(id, 1, &provision(pool)).await.unwrap();
    store
        .set_host_transport(id, 1, &transport(n))
        .await
        .unwrap();
    id
}

async fn observe(store: &Store, id: HostId, epoch: i64) {
    store
        .observe_configured_host_with_capabilities(
            id,
            epoch,
            &capabilities().to_json().unwrap(),
            true,
        )
        .await
        .unwrap();
}

#[sqlx::test(migrations = false)]
async fn legacy_hosts_backfill_into_shared_standalone_pool_without_transport(pool: PgPool) {
    let previous = Migrator {
        migrations: Cow::Owned(MIGRATOR.iter().take(24).cloned().collect()),
        ..Migrator::DEFAULT
    };
    previous.run(&pool).await.unwrap();
    let host = Uuid::now_v7();
    sqlx::query("INSERT INTO hosts(id,status,cpu_capacity,memory_capacity_mib,disk_capacity_mib,supervisor_epoch,last_seen_at) VALUES($1,'ready',4,8192,65536,3,clock_timestamp())")
        .bind(host).execute(&pool).await.unwrap();
    let before = row(&pool, HostId::from_uuid(host)).await;
    MIGRATOR.run(&pool).await.unwrap();
    MIGRATOR.run(&pool).await.unwrap();
    let after = row(&pool, HostId::from_uuid(host)).await;
    for column in ["status", "supervisor_epoch", "last_seen_at", "cpu_capacity"] {
        assert_eq!(before[column], after[column], "{column} changed");
    }
    assert_eq!(after["host_pool_id"], json!(LEGACY_POOL_ID));
    assert!(after["endpoint"].is_null() && after["transport_certificate_sha256"].is_null());
    let store = Store::from_pool(pool.clone());
    let legacy = store.host_pool(LEGACY_POOL_ID).await.unwrap().unwrap();
    assert_eq!(legacy.pool_type, PoolType::Standalone);
    assert_eq!(legacy.placement_policy.tenancy, Tenancy::Shared);
    assert_eq!(legacy.enrolled_hosts, 1);
    // Backfill grants no project access; legacy hosts stay readable, unaddressed.
    let access: i64 = sqlx::query_scalar("SELECT count(*) FROM project_host_pool_access")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(access, 0);
    let hosts = store.registered_hosts(None).await.unwrap();
    assert_eq!(hosts.len(), 1);
    assert!(hosts[0].transport.is_none() && !hosts[0].admits_new_work());
    assert_eq!(hosts[0].supervisor_epoch, 3);
}

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn pool_validation_bounds_and_exact_retries(pool: PgPool) {
    let store = Store::from_pool(pool.clone());
    let id = Uuid::now_v7();
    let created = store.put_host_pool(&spec(id, "edge")).await.unwrap();
    let stored: Value =
        sqlx::query_scalar("SELECT to_jsonb(host_pools) FROM host_pools WHERE id=$1")
            .bind(id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(
        store.put_host_pool(&spec(id, "edge")).await.unwrap(),
        created
    );
    let retried: Value =
        sqlx::query_scalar("SELECT to_jsonb(host_pools) FROM host_pools WHERE id=$1")
            .bind(id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(stored, retried, "exact retry must not touch the row");

    let mut invalid = Vec::new();
    let mut s = spec(Uuid::now_v7(), "bad");
    s.min_ready_hosts = 3;
    invalid.push(s);
    let mut s = spec(Uuid::now_v7(), "bad");
    s.provider_config = k8s_config();
    invalid.push(s);
    let mut s = spec(Uuid::now_v7(), "bad");
    s.provider_config = json!({"version": 1, "api_token": "secret-value"});
    invalid.push(s);
    let mut s = spec(Uuid::now_v7(), "bad");
    s.pool_type = PoolType::K8s;
    invalid.push(s);
    invalid.push(spec(Uuid::now_v7(), "Not_A_Label"));
    let mut s = spec(Uuid::now_v7(), "bad");
    s.credential_secret_ref = Some("has spaces".into());
    invalid.push(s);
    for s in invalid {
        assert!(matches!(
            store.put_host_pool(&s).await,
            Err(RegistryError::Invalid)
        ));
    }
    assert!(
        HostPoolSpec::from_json(br#"{"version":1,"version":1}"#).is_err(),
        "duplicate fields refused"
    );
    // Database constraints back the code checks.
    for sql in [
        "INSERT INTO host_pools(id,name,type,min_ready_hosts,max_hosts,placement_policy,provider_config) VALUES(gen_random_uuid(),'x','standalone',3,2,'{}','{}')",
        "INSERT INTO host_pools(id,name,type,min_ready_hosts,max_hosts,placement_policy,provider_config) VALUES(gen_random_uuid(),'y','standalone',-1,2,'{}','{}')",
        "INSERT INTO host_pools(id,name,type,min_ready_hosts,max_hosts,placement_policy,provider_config) VALUES(gen_random_uuid(),'z','vm',0,2,'{}','{}')",
    ] {
        assert!(sqlx::query(sql).execute(&pool).await.is_err(), "{sql}");
    }
    // Names are unique.
    assert!(matches!(
        store.put_host_pool(&spec(Uuid::now_v7(), "edge")).await,
        Err(RegistryError::Duplicate)
    ));

    // max_hosts bounds enrollment; it cannot shrink below enrolled hosts.
    store
        .provision_host(HostId::generate(), 1, &provision(Some(id)))
        .await
        .unwrap();
    store
        .provision_host(HostId::generate(), 1, &provision(Some(id)))
        .await
        .unwrap();
    assert!(matches!(
        store
            .provision_host(HostId::generate(), 1, &provision(Some(id)))
            .await,
        Err(host_maintenance::Error::Conflict)
    ));
    let mut shrink = spec(id, "edge");
    shrink.max_hosts = 1;
    assert!(matches!(
        store.put_host_pool(&shrink).await,
        Err(RegistryError::Conflict)
    ));
    // Unknown pool refused; legacy default remains available.
    assert!(matches!(
        store
            .provision_host(HostId::generate(), 1, &provision(Some(Uuid::now_v7())))
            .await,
        Err(host_maintenance::Error::Conflict)
    ));
    let legacy = HostId::generate();
    store
        .provision_host(legacy, 1, &provision(None))
        .await
        .unwrap();
    assert_eq!(
        row(&pool, legacy).await["host_pool_id"],
        json!(LEGACY_POOL_ID)
    );
    // A host never moves pools, even on an otherwise exact retry.
    assert!(matches!(
        store.provision_host(legacy, 1, &provision(Some(id))).await,
        Err(host_maintenance::Error::Conflict)
    ));
}

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn pool_type_change_is_blocked_while_hosts_are_enrolled(pool: PgPool) {
    let store = Store::from_pool(pool.clone());
    let id = Uuid::now_v7();
    store.put_host_pool(&spec(id, "flex")).await.unwrap();
    // Empty pool: type change permitted with a matching provider config.
    let mut k8s = spec(id, "flex");
    k8s.pool_type = PoolType::K8s;
    k8s.provider_config = k8s_config();
    k8s.credential_secret_ref = Some("kubernetes/hudsonbox-workers".into());
    let changed = store.put_host_pool(&k8s).await.unwrap();
    assert_eq!(changed.pool_type, PoolType::K8s);
    store.put_host_pool(&spec(id, "flex")).await.unwrap();

    let host = HostId::generate();
    store
        .provision_host(host, 1, &provision(Some(id)))
        .await
        .unwrap();
    let before: Value =
        sqlx::query_scalar("SELECT to_jsonb(host_pools) FROM host_pools WHERE id=$1")
            .bind(id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert!(matches!(
        store.put_host_pool(&k8s).await,
        Err(RegistryError::InUse)
    ));
    let after: Value =
        sqlx::query_scalar("SELECT to_jsonb(host_pools) FROM host_pools WHERE id=$1")
            .bind(id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(before, after);
    // Same-type bounds/policy updates remain possible.
    let mut grown = spec(id, "flex");
    grown.max_hosts = 8;
    assert_eq!(store.put_host_pool(&grown).await.unwrap().max_hosts, 8);
    // A draining host with no reservations still blocks: hosts are never removed.
    assert_eq!(
        store
            .host_maintenance(host, 1)
            .await
            .unwrap()
            .outstanding_allocations,
        0
    );
    assert!(matches!(
        store.put_host_pool(&k8s).await,
        Err(RegistryError::InUse)
    ));
    // Pools with enrolled hosts cannot be deleted from under them.
    assert!(
        sqlx::query("DELETE FROM host_pools WHERE id=$1")
            .bind(id)
            .execute(&pool)
            .await
            .is_err()
    );
}

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn project_pool_access_is_a_unique_pair_and_dedicated_pools_take_one_project(pool: PgPool) {
    let store = Store::from_pool(pool.clone());
    let (shared, dedicated) = (Uuid::now_v7(), Uuid::now_v7());
    store.put_host_pool(&spec(shared, "shared")).await.unwrap();
    let mut d = spec(dedicated, "dedicated");
    d.placement_policy.tenancy = Tenancy::Dedicated;
    store.put_host_pool(&d).await.unwrap();
    let (a, b) = (project(&pool).await, project(&pool).await);

    let first = store.grant_host_pool_access(a, shared).await.unwrap();
    let (x, y) = tokio::join!(
        store.grant_host_pool_access(a, shared),
        store.grant_host_pool_access(a, shared)
    );
    assert_eq!((x.unwrap(), y.unwrap()), (first, first));
    store.grant_host_pool_access(b, shared).await.unwrap();
    let raw = sqlx::query(
        "INSERT INTO project_host_pool_access(id,project_id,host_pool_id) VALUES($1,$2,$3)",
    )
    .bind(Uuid::now_v7())
    .bind(a.uuid())
    .bind(shared)
    .execute(&pool)
    .await
    .unwrap_err();
    assert_eq!(
        raw.as_database_error().unwrap().code().as_deref(),
        Some("23505")
    );

    store.grant_host_pool_access(a, dedicated).await.unwrap();
    assert!(matches!(
        store.grant_host_pool_access(b, dedicated).await,
        Err(RegistryError::Conflict)
    ));
    // Switching a shared pool with two grants to dedicated is refused.
    let mut to_dedicated = spec(shared, "shared");
    to_dedicated.placement_policy.tenancy = Tenancy::Dedicated;
    assert!(matches!(
        store.put_host_pool(&to_dedicated).await,
        Err(RegistryError::Conflict)
    ));
    assert!(matches!(
        store
            .grant_host_pool_access(ProjectId::generate(), shared)
            .await,
        Err(RegistryError::Conflict)
    ));
    assert!(matches!(
        store.grant_host_pool_access(a, Uuid::now_v7()).await,
        Err(RegistryError::Conflict)
    ));
    let mut expected = vec![shared, dedicated];
    expected.sort();
    assert_eq!(store.project_host_pools(a).await.unwrap(), expected);
    assert_eq!(store.project_host_pools(b).await.unwrap(), vec![shared]);
}

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn duplicate_endpoint_certificate_or_provider_identity_is_rejected(pool: PgPool) {
    let store = Store::from_pool(pool.clone());
    let first = registered(&store, None, 1).await;
    let second = HostId::generate();
    store
        .provision_host(second, 1, &provision(None))
        .await
        .unwrap();
    let before = row(&pool, second).await;

    let mut same_endpoint = transport(2);
    same_endpoint.endpoint = "HTTPS://10.0.0.1:7443".into();
    let mut same_certificate = transport(2);
    same_certificate.certificate_sha256 = transport(1).certificate_sha256.to_uppercase();
    let mut same_provider = transport(2);
    same_provider.provider_ref = transport(1).provider_ref;
    for duplicate in [same_endpoint, same_certificate, same_provider] {
        assert!(matches!(
            store.set_host_transport(second, 1, &duplicate).await,
            Err(RegistryError::Duplicate)
        ));
        assert_eq!(row(&pool, second).await, before);
    }
    // Raw SQL cannot bind another host's DNS identity either.
    let forged = sqlx::query(
        "UPDATE hosts SET endpoint='https://10.0.0.9:7443',transport_server_name=$2,
        transport_certificate_sha256=repeat('e',64) WHERE id=$1",
    )
    .bind(second.uuid())
    .bind(format!("host-{}.sandbox.internal", first.uuid()))
    .execute(&pool)
    .await;
    assert!(forged.is_err());
    for invalid in [
        HostTransport {
            endpoint: "http://10.0.0.2:7443".into(),
            ..transport(2)
        },
        HostTransport {
            endpoint: "https://10.0.0.2".into(),
            ..transport(2)
        },
        HostTransport {
            endpoint: "https://user@10.0.0.2:7443".into(),
            ..transport(2)
        },
        HostTransport {
            endpoint: "https://10.0.0.2:7443/path".into(),
            ..transport(2)
        },
        HostTransport {
            certificate_sha256: "zz".into(),
            ..transport(2)
        },
    ] {
        assert!(matches!(
            store.set_host_transport(second, 1, &invalid).await,
            Err(RegistryError::Invalid)
        ));
    }
    store
        .set_host_transport(second, 1, &transport(2))
        .await
        .unwrap();
    let hosts = store.registered_hosts(Some(LEGACY_POOL_ID)).await.unwrap();
    assert_eq!(hosts.len(), 2);
    assert_ne!(hosts[0].transport, hosts[1].transport);
}

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn endpoint_and_identity_changes_require_drain_and_expected_epoch(pool: PgPool) {
    let store = Store::from_pool(pool.clone());
    let id = registered(&store, None, 1).await;
    observe(&store, id, 1).await;
    store.finish_host_drain(id, 1).await.unwrap();
    let ready = row(&pool, id).await;

    // Exact retry on a ready host reports state without touching the row.
    assert_eq!(
        store
            .set_host_transport(id, 1, &transport(1))
            .await
            .unwrap()
            .status,
        "ready"
    );
    assert_eq!(row(&pool, id).await, ready);
    let mut moved = transport(1);
    moved.endpoint = "https://10.0.0.77:7443".into();
    let mut rotated = transport(1);
    rotated.certificate_sha256 = hex::encode(Sha256::digest(leaf(77)));
    for change in [&moved, &rotated] {
        assert!(matches!(
            store.set_host_transport(id, 1, change).await,
            Err(RegistryError::Conflict)
        ));
        for epoch in [0, 2] {
            assert!(matches!(
                store.set_host_transport(id, epoch, change).await,
                Err(RegistryError::Conflict)
            ));
        }
        assert_eq!(row(&pool, id).await, ready);
    }
    assert!(matches!(
        store
            .set_host_transport(HostId::generate(), 1, &moved)
            .await,
        Err(RegistryError::Conflict)
    ));

    store.drain_host(id, 1).await.unwrap();
    store.set_host_transport(id, 1, &moved).await.unwrap();
    let host = store.registered_host(id).await.unwrap().unwrap();
    assert_eq!(host.transport.unwrap().endpoint, "https://10.0.0.77:7443");
    assert!(!host.heartbeat_fresh, "identity change clears freshness");
    assert!(matches!(
        store.finish_host_drain(id, 1).await,
        Err(host_maintenance::Error::Outstanding)
    ));
    observe(&store, id, 1).await;
    store.finish_host_drain(id, 1).await.unwrap();
}

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn heartbeat_cannot_change_registration_or_promote_draining_host(pool: PgPool) {
    let store = Store::from_pool(pool.clone());
    let pool_id = Uuid::now_v7();
    store.put_host_pool(&spec(pool_id, "beats")).await.unwrap();
    let id = registered(&store, Some(pool_id), 3).await;
    let before = row(&pool, id).await;
    for _ in 0..2 {
        observe(&store, id, 1).await;
    }
    let after = row(&pool, id).await;
    for column in [
        "status",
        "supervisor_epoch",
        "host_pool_id",
        "endpoint",
        "transport_server_name",
        "transport_certificate_sha256",
        "provider_ref",
        "cpu_capacity",
        "memory_capacity_mib",
        "disk_capacity_mib",
        "compatibility",
    ] {
        assert_eq!(before[column], after[column], "health changed {column}");
    }
    assert_eq!(after["status"], "draining");
    let host = store.registered_host(id).await.unwrap().unwrap();
    assert!(host.heartbeat_fresh && !host.admits_new_work());
    // Health at a wrong epoch is refused and changes nothing.
    assert!(
        store
            .observe_configured_host_with_capabilities(
                id,
                2,
                &capabilities().to_json().unwrap(),
                true
            )
            .await
            .is_err()
    );
    assert_eq!(row(&pool, id).await["supervisor_epoch"], 1);
}

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn certificate_mismatch_and_restart_with_new_epoch(pool: PgPool) {
    let store = Store::from_pool(pool.clone());
    let id = registered(&store, None, 4).await;
    let other = registered(&store, None, 5).await;
    let host = store.registered_host(id).await.unwrap().unwrap();
    let t = host.transport.clone().unwrap();
    assert_eq!(
        t.server_name,
        format!("host-{}.sandbox.internal", id.uuid())
    );
    host.verify_transport(1, "https://10.0.0.4:7443", &leaf(4))
        .unwrap();
    host.verify_transport(1, "HTTPS://10.0.0.4:7443", &leaf(4))
        .unwrap();
    for (epoch, endpoint, presented) in [
        (1, "https://10.0.0.4:7443", leaf(5)), // another host's certificate
        (1, "https://10.0.0.4:7443", leaf(9)), // unknown certificate
        (1, "https://10.0.0.5:7443", leaf(4)), // another host's address
        (2, "https://10.0.0.4:7443", leaf(4)), // future epoch
        (0, "https://10.0.0.4:7443", leaf(4)),
    ] {
        assert!(matches!(
            host.verify_transport(epoch, endpoint, &presented),
            Err(RegistryError::TransportMismatch)
        ));
    }

    // Restart: the operator issues a new epoch; registration survives it, the
    // old epoch loses authority over transport and verification.
    store.advance_host_epoch(id, 1).await.unwrap();
    assert!(matches!(
        store.set_host_transport(id, 1, &transport(4)).await,
        Err(RegistryError::Conflict)
    ));
    let before = row(&pool, id).await;
    store
        .set_host_transport(id, 2, &transport(4))
        .await
        .unwrap();
    assert_eq!(row(&pool, id).await, before, "exact retry at new epoch");
    let restarted = store.registered_host(id).await.unwrap().unwrap();
    assert_eq!(restarted.supervisor_epoch, 2);
    assert_eq!(restarted.transport, host.transport);
    assert!(!restarted.heartbeat_fresh);
    assert!(matches!(
        restarted.verify_transport(1, "https://10.0.0.4:7443", &leaf(4)),
        Err(RegistryError::TransportMismatch)
    ));
    restarted
        .verify_transport(2, "https://10.0.0.4:7443", &leaf(4))
        .unwrap();
    // Re-registration on the new incarnation may move it while drained.
    let mut moved = transport(4);
    moved.endpoint = "https://supervisor-4.example.internal:7443".into();
    store.set_host_transport(id, 2, &moved).await.unwrap();
    // The other identity is untouched throughout.
    let untouched = store.registered_host(other).await.unwrap().unwrap();
    assert_eq!(untouched.supervisor_epoch, 1);
    untouched
        .verify_transport(1, "https://10.0.0.5:7443", &leaf(5))
        .unwrap();
}

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn stale_and_never_seen_hosts_are_not_fresh(pool: PgPool) {
    let store = Store::from_pool(pool.clone());
    let pool_id = Uuid::now_v7();
    store.put_host_pool(&spec(pool_id, "fresh")).await.unwrap();
    let (live, stale) = (
        registered(&store, Some(pool_id), 6).await,
        registered(&store, Some(pool_id), 7).await,
    );
    let never = registered(&store, None, 8).await;
    for id in [live, stale] {
        observe(&store, id, 1).await;
        store.finish_host_drain(id, 1).await.unwrap();
    }
    sqlx::query(
        "UPDATE hosts SET last_seen_at=clock_timestamp()-interval '31 seconds' WHERE id=$1",
    )
    .bind(stale.uuid())
    .execute(&pool)
    .await
    .unwrap();
    let hosts = store.registered_hosts(Some(pool_id)).await.unwrap();
    assert_eq!(hosts.len(), 2, "pool filter excludes other pools");
    let by_id = |id| hosts.iter().find(|h| h.host_id == id).unwrap();
    assert!(by_id(live).heartbeat_fresh && by_id(live).admits_new_work());
    assert_eq!(by_id(live).pool_type, PoolType::Standalone);
    assert_eq!(by_id(live).capacity.vcpu, 4);
    assert_eq!(by_id(live).observed_capabilities_epoch, Some(1));
    let s = by_id(stale);
    assert!(!s.heartbeat_fresh && !s.admits_new_work() && s.heartbeat_age_ms.unwrap() >= 31_000);
    assert_eq!(
        s.status, "ready",
        "staleness is reported, not inferred as a status change"
    );
    let n = store.registered_host(never).await.unwrap().unwrap();
    assert!(!n.heartbeat_fresh && n.heartbeat_age_ms.is_none() && n.last_seen_at.is_none());
    assert!(
        store
            .registered_host(HostId::generate())
            .await
            .unwrap()
            .is_none()
    );
}

/// Diagnostics use one read-only snapshot and omit transport/provider identities.
#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn fleet_snapshot_tracks_freshness_without_leaking_registration(pool: PgPool) {
    let store = Store::from_pool(pool.clone());
    let first = registered(&store, None, 61).await;
    let second = registered(&store, None, 62).await;
    observe(&store, first, 1).await;
    store.finish_host_drain(first, 1).await.unwrap();
    let before: Vec<Value> = sqlx::query_scalar("SELECT to_jsonb(hosts) FROM hosts ORDER BY id")
        .fetch_all(&pool)
        .await
        .unwrap();
    let snapshot = store.fleet_status().await.unwrap();
    let hosts = snapshot["hosts"].as_array().unwrap();
    let a = hosts
        .iter()
        .find(|h| h["host_id"] == first.uuid().to_string())
        .unwrap();
    let b = hosts
        .iter()
        .find(|h| h["host_id"] == second.uuid().to_string())
        .unwrap();
    assert_eq!(a["ready"], true);
    assert_eq!(b["ready"], false);
    assert_eq!(b["status"], "draining");
    assert_eq!(a["available"]["vcpu"], 4);
    for h in hosts {
        for field in [
            "endpoint",
            "transport_server_name",
            "transport_certificate_sha256",
            "provider_ref",
            "capabilities",
        ] {
            assert!(h.get(field).is_none(), "unexpected private field {field}");
        }
    }
    let after: Vec<Value> = sqlx::query_scalar("SELECT to_jsonb(hosts) FROM hosts ORDER BY id")
        .fetch_all(&pool)
        .await
        .unwrap();
    assert_eq!(
        before, after,
        "diagnostics must not refresh or mutate hosts"
    );
    sqlx::query(
        "UPDATE hosts SET last_seen_at=clock_timestamp()-interval '31 seconds' WHERE id=$1",
    )
    .bind(first.uuid())
    .execute(&pool)
    .await
    .unwrap();
    let stale = store.fleet_status().await.unwrap();
    let a = stale["hosts"]
        .as_array()
        .unwrap()
        .iter()
        .find(|h| h["host_id"] == first.uuid().to_string())
        .unwrap();
    assert_eq!(a["fresh"], false);
    assert_eq!(a["ready"], false);
}

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn fleet_rpc_counts_are_epoch_fenced_and_history_does_not_exhaust_active_bound(pool: PgPool) {
    use sandbox_protocol::rpc_telemetry::RpcFailure;
    let store = Store::from_pool(pool.clone());
    let a = registered(&store, None, 71).await;
    let b = registered(&store, None, 72).await;
    observe(&store, a, 1).await;
    let before = row(&pool, a).await;
    store
        .record_host_rpc_failure(a, 1, RpcFailure::Unavailable)
        .await
        .unwrap();
    store
        .record_host_rpc_failure(a, 1, RpcFailure::DeadlineExceeded)
        .await
        .unwrap();
    store
        .record_host_rpc_failure(a, 2, RpcFailure::Unknown)
        .await
        .unwrap();
    let after = row(&pool, a).await;
    assert_eq!(after["last_seen_at"], before["last_seen_at"]);
    assert_eq!(after["status"], before["status"]);
    assert_eq!(
        after["rpc_failures"],
        json!({"unavailable":1,"deadline_exceeded":1,"unknown":0})
    );
    assert_eq!(row(&pool, b).await["rpc_failures"]["unavailable"], 0);
    store.advance_host_epoch(a, 1).await.unwrap();
    store
        .record_host_rpc_failure(a, 1, RpcFailure::Unknown)
        .await
        .unwrap();
    assert_eq!(row(&pool, a).await["rpc_failures"]["unknown"], 0);
    sqlx::query("UPDATE hosts SET capacity_retired_at=clock_timestamp() WHERE id=$1")
        .bind(a.uuid())
        .execute(&pool)
        .await
        .unwrap();
    store
        .record_host_rpc_failure(a, 2, RpcFailure::Unknown)
        .await
        .unwrap();
    assert_eq!(row(&pool, a).await["rpc_failures"]["unknown"], 0);
    sqlx::query("INSERT INTO hosts(id,status,cpu_capacity,memory_capacity_mib,disk_capacity_mib,supervisor_epoch,capacity_retired_at)
        SELECT gen_random_uuid(),'draining',4,8192,65536,1,clock_timestamp() FROM generate_series(1,10001)")
        .execute(&pool).await.unwrap();
    let snapshot = store.fleet_status().await.unwrap();
    assert_eq!(snapshot["hosts"].as_array().unwrap().len(), 1);
    assert_eq!(snapshot["hosts"][0]["host_id"], b.uuid().to_string());
    // Tombstones are counted (`a` plus the bulk history), never enumerated.
    assert_eq!(snapshot["retired_hosts"], 10002);
    let metrics = sandbox_store::fleet::prometheus(&snapshot);
    assert!(metrics.contains("hudsonbox_fleet_hosts{state=\"retired\"} 10002\n"));
    assert!(!metrics.contains(&a.uuid().to_string()));
}

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn fleet_rpc_observation_lock_wait_is_cancelled_on_server(pool: PgPool) {
    use sandbox_protocol::rpc_telemetry::RpcFailure;
    let store = Store::from_pool(pool.clone());
    let a = registered(&store, None, 81).await;
    let b = registered(&store, None, 82).await;
    let mut lock = pool.begin().await.unwrap();
    sqlx::query("SELECT id FROM hosts WHERE id=$1 FOR UPDATE")
        .bind(a.uuid())
        .execute(&mut *lock)
        .await
        .unwrap();
    let failure = store
        .record_host_rpc_failure(a, 1, RpcFailure::Unavailable)
        .await
        .unwrap_err();
    match failure {
        sqlx::Error::Database(database) => {
            assert_eq!(database.code().as_deref(), Some("57014"));
        }
        // Pool acquisition, scheduling and the transaction setup share the
        // caller's 100ms bound. Under load that bound may win the race.
        sqlx::Error::PoolTimedOut => {}
        other => panic!("expected a bounded telemetry timeout, got {other}"),
    }
    // Keep A locked while proving that cancellation left no blocked telemetry
    // statement on the server, regardless of which deadline the caller saw.
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        loop {
            let blocked: i64 = sqlx::query_scalar(
                "SELECT count(*) FROM pg_stat_activity WHERE datname=current_database()
                 AND wait_event_type='Lock' AND query LIKE 'UPDATE hosts SET rpc_failures=%'",
            )
            .fetch_one(&pool)
            .await
            .unwrap();
            if blocked == 0 {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("telemetry update remained blocked after its server deadline");
    store
        .record_host_rpc_failure(b, 1, RpcFailure::Unavailable)
        .await
        .unwrap();
    lock.rollback().await.unwrap();
    assert_eq!(row(&pool, a).await["rpc_failures"]["unavailable"], 0);
    assert_eq!(row(&pool, b).await["rpc_failures"]["unavailable"], 1);
    store
        .record_host_rpc_failure(a, 1, RpcFailure::Unavailable)
        .await
        .unwrap();
    assert_eq!(row(&pool, a).await["rpc_failures"]["unavailable"], 1);
}
