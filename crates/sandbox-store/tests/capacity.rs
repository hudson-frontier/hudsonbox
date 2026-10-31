//! Portable PostgreSQL identities; no Linux/KVM or paid provider effects.
#![allow(clippy::unwrap_used, clippy::expect_used)]
use sandbox_protocol::{
    HostId, Id,
    host_capabilities::{Capabilities, Image, Limits},
};
use sandbox_store::{
    Store,
    capacity::{Action, Error, Request},
    host_maintenance::{HostCapacity, HostProvision},
    host_registry::{HostPoolSpec, PlacementPolicy, PoolType, Tenancy},
};
use serde_json::json;
use sqlx::PgPool;
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

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn duplicate_intents_claim_races_and_stale_retry(pool: PgPool) {
    let store = Store::from_pool(pool.clone());
    let p = Uuid::now_v7();
    store.put_host_pool(&spec(p, "capacity")).await.unwrap();
    let request = Request {
        host: HostId::generate(),
        epoch: 1,
        action: Action::Provision {
            config: Box::new(provision(Some(p))),
        },
    };
    let (a, b) = tokio::join!(
        store.enqueue_capacity(p, "same", &request),
        store.enqueue_capacity(p, "same", &request)
    );
    let id = a.unwrap();
    assert_eq!(id, b.unwrap());
    let mut changed = request.clone();
    changed.epoch = 2;
    assert!(matches!(
        store.enqueue_capacity(p, "same", &changed).await,
        Err(Error::Conflict)
    ));
    let (a, b) = tokio::join!(store.claim_capacity(p, 30), store.claim_capacity(p, 30));
    let (a, b) = (a.unwrap(), b.unwrap());
    assert_ne!(a.is_some(), b.is_some());
    let old = a.or(b).unwrap();
    sqlx::query("UPDATE capacity_operations SET lease_expires_at=clock_timestamp()-interval '1 second',available_at=clock_timestamp()-interval '1 second' WHERE id=$1").bind(id).execute(&pool).await.unwrap();
    let new = store.claim_capacity(p, 30).await.unwrap().unwrap();
    assert!(new.revision > old.revision);
    assert!(matches!(
        store.apply_static_capacity(&old).await,
        Err(Error::LostClaim)
    ));
    assert!(matches!(
        store.retry_capacity(&old).await,
        Err(Error::LostClaim)
    ));
    store.retry_capacity(&new).await.unwrap();
    let latest = store.claim_capacity(p, 30).await.unwrap().unwrap();
    store.apply_static_capacity(&latest).await.unwrap();
    assert_eq!(
        store.enqueue_capacity(p, "same", &request).await.unwrap(),
        id
    );
    assert!(store.claim_capacity(p, 30).await.unwrap().is_none());
    assert_eq!(
        store.capacity_operation(id).await.unwrap().unwrap()["status"],
        "succeeded"
    );
    assert_eq!(store.registered_hosts(Some(p)).await.unwrap().len(), 1);
    assert!(matches!(
        store.apply_static_capacity(&latest).await,
        Err(Error::LostClaim)
    ));
}

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn retire_requires_fresh_empty_draining_and_retains_tombstone(pool: PgPool) {
    let store = Store::from_pool(pool.clone());
    let p = Uuid::now_v7();
    store.put_host_pool(&spec(p, "retire")).await.unwrap();
    let host = HostId::generate();
    store
        .provision_host(host, 1, &provision(Some(p)))
        .await
        .unwrap();
    let request = Request {
        host,
        epoch: 1,
        action: Action::Retire,
    };
    let id = store.enqueue_capacity(p, "retire", &request).await.unwrap();
    let claim = store.claim_capacity(p, 30).await.unwrap().unwrap();
    assert!(matches!(
        store.apply_static_capacity(&claim).await,
        Err(Error::Conflict)
    ));
    // Fixture-only authenticated observation; no supervisor is contacted.
    store
        .observe_configured_host_with_capabilities(
            host,
            1,
            &capabilities().to_json().unwrap(),
            true,
        )
        .await
        .unwrap();
    store.apply_static_capacity(&claim).await.unwrap();
    assert!(store.registered_host(host).await.unwrap().is_some());
    assert!(store.finish_host_drain(host, 1).await.is_err());
    assert!(
        store
            .provision_host(host, 1, &provision(Some(p)))
            .await
            .is_err()
    );
    assert!(store.advance_host_epoch(host, 1).await.is_err());
    assert!(store.capacity_operation(id).await.unwrap().unwrap()["provider_receipt"].is_object());
    let again = store
        .enqueue_capacity(p, "retire-again", &request)
        .await
        .unwrap();
    let claim = store.claim_capacity(p, 30).await.unwrap().unwrap();
    assert_eq!(claim.id, again);
    assert_eq!(
        store.apply_static_capacity(&claim).await.unwrap()["retired"],
        true
    );
}

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn pending_operations_block_type_change_and_pool_mismatch(pool: PgPool) {
    let store = Store::from_pool(pool);
    let p = Uuid::now_v7();
    let other = Uuid::now_v7();
    store.put_host_pool(&spec(p, "pending")).await.unwrap();
    store.put_host_pool(&spec(other, "other")).await.unwrap();
    let request = Request {
        host: HostId::generate(),
        epoch: 1,
        action: Action::Status,
    };
    store.enqueue_capacity(p, "status", &request).await.unwrap();
    let mut k8s = spec(p, "pending");
    k8s.pool_type = PoolType::K8s;
    k8s.provider_config =
        json!({"version":1,"namespace":"workers","worker_label_selector":"app=supervisor"});
    assert!(store.put_host_pool(&k8s).await.is_err());
    let claim = store.claim_capacity(p, 30).await.unwrap().unwrap();
    assert!(matches!(
        store.apply_static_capacity(&claim).await,
        Err(Error::Conflict)
    ));
    assert!(store.claim_capacity(p, 0).await.is_err());
    let bad = Request {
        action: Action::Provision {
            config: Box::new(provision(Some(other))),
        },
        ..request
    };
    assert!(matches!(
        store.enqueue_capacity(p, "bad", &bad).await,
        Err(Error::Invalid)
    ));
}

#[sqlx::test(migrations = false)]
async fn upgrade_preserves_existing_host_state_and_is_repeatable(pool: PgPool) {
    use sandbox_store::MIGRATOR;
    use sqlx::migrate::Migrator;
    use std::borrow::Cow;
    Migrator {
        migrations: Cow::Owned(
            MIGRATOR
                .iter()
                .filter(|m| m.version < 27)
                .cloned()
                .collect(),
        ),
        ..Migrator::DEFAULT
    }
    .run(&pool)
    .await
    .unwrap();
    let host = Uuid::now_v7();
    sqlx::query("INSERT INTO hosts(id,status,cpu_capacity,memory_capacity_mib,disk_capacity_mib,supervisor_epoch,last_seen_at) VALUES($1,'unhealthy',4,8192,65536,7,clock_timestamp())")
        .bind(host).execute(&pool).await.unwrap();
    let before: serde_json::Value =
        sqlx::query_scalar("SELECT to_jsonb(h) FROM hosts h WHERE id=$1")
            .bind(host)
            .fetch_one(&pool)
            .await
            .unwrap();
    MIGRATOR.run(&pool).await.unwrap();
    MIGRATOR.run(&pool).await.unwrap();
    let mut after: serde_json::Value =
        sqlx::query_scalar("SELECT to_jsonb(h) FROM hosts h WHERE id=$1")
            .bind(host)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert!(
        after
            .as_object_mut()
            .unwrap()
            .remove("capacity_retired_at")
            .unwrap()
            .is_null()
    );
    assert_eq!(before, after);
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM capacity_operations")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(count, 0);
    for sql in [
        "INSERT INTO capacity_operations(id,host_pool_id,kind,target_ref,idempotency_key,request,warm_pool_id) SELECT gen_random_uuid(),id,'status','host','x','{}',gen_random_uuid() FROM host_pools",
        "INSERT INTO capacity_operations(id,host_pool_id,kind,target_ref,idempotency_key,request,status) SELECT gen_random_uuid(),id,'retire','host','x','{}','succeeded' FROM host_pools",
        "INSERT INTO capacity_operations(id,host_pool_id,kind,target_ref,idempotency_key,request,status) SELECT gen_random_uuid(),id,'status','host','x','{}','running' FROM host_pools",
    ] {
        assert!(sqlx::query(sql).execute(&pool).await.is_err());
    }
}

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn occupied_and_unknown_reservations_never_retire_or_disappear(pool: PgPool) {
    let store = Store::from_pool(pool.clone());
    let p = Uuid::now_v7();
    store.put_host_pool(&spec(p, "occupied")).await.unwrap();
    let host = HostId::generate();
    store
        .provision_host(host, 2, &provision(Some(p)))
        .await
        .unwrap();
    store
        .observe_configured_host_with_capabilities(
            host,
            2,
            &capabilities().to_json().unwrap(),
            true,
        )
        .await
        .unwrap();
    let project = Uuid::now_v7();
    let sandbox = Uuid::now_v7();
    let allocation = Uuid::now_v7();
    sqlx::query("INSERT INTO projects(id,name,status,limits,api_tokens) VALUES($1,'capacity','active','{}','[]')").bind(project).execute(&pool).await.unwrap();
    sqlx::query("INSERT INTO sandboxes(id,project_id,image_digest,resources,desired_state,observed_state) VALUES($1,$2,'fixture','{}','running','unknown')")
        .bind(sandbox).bind(project).execute(&pool).await.unwrap();
    sqlx::query("INSERT INTO allocations(id,project_id,sandbox_id,host_id,generation,supervisor_epoch,vcpu,memory_mib,disk_mib,status) VALUES($1,$2,$3,$4,1,1,1,512,1024,'running')")
        .bind(allocation).bind(project).bind(sandbox).bind(host.uuid()).execute(&pool).await.unwrap();
    let request = Request {
        host,
        epoch: 2,
        action: Action::Retire,
    };
    store.enqueue_capacity(p, "busy", &request).await.unwrap();
    let claim = store.claim_capacity(p, 30).await.unwrap().unwrap();
    let before: serde_json::Value =
        sqlx::query_scalar("SELECT to_jsonb(a) FROM allocations a WHERE id=$1")
            .bind(allocation)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert!(matches!(
        store.apply_static_capacity(&claim).await,
        Err(Error::Conflict)
    ));
    let report = store.host_maintenance(host, 2).await.unwrap();
    assert_eq!(
        (
            report.outstanding_allocations,
            report.previous_epoch_allocations,
            report.unknown_state_allocations
        ),
        (1, 1, 1)
    );
    let after: serde_json::Value =
        sqlx::query_scalar("SELECT to_jsonb(a) FROM allocations a WHERE id=$1")
            .bind(allocation)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(before, after);
    assert!(
        !store
            .registered_host(host)
            .await
            .unwrap()
            .unwrap()
            .capacity_retired
    );
    // A late fresh health reply is still not allocation cleanup/fencing evidence.
    store
        .observe_configured_host_with_capabilities(
            host,
            2,
            &capabilities().to_json().unwrap(),
            true,
        )
        .await
        .unwrap();
    assert!(matches!(
        store.apply_static_capacity(&claim).await,
        Err(Error::Conflict)
    ));
}

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn concurrent_provision_respects_inventory_bound_and_durable_target(pool: PgPool) {
    let store = Store::from_pool(pool);
    let p = Uuid::now_v7();
    let mut spec = spec(p, "bounded");
    spec.max_hosts = 1;
    store.put_host_pool(&spec).await.unwrap();
    let first = Request {
        host: HostId::generate(),
        epoch: 1,
        action: Action::Provision {
            config: Box::new(provision(Some(p))),
        },
    };
    let second = Request {
        host: HostId::generate(),
        epoch: 1,
        action: Action::Provision {
            config: Box::new(provision(Some(p))),
        },
    };
    store.enqueue_capacity(p, "a", &first).await.unwrap();
    store.enqueue_capacity(p, "b", &second).await.unwrap();
    let mut a = store.claim_capacity(p, 30).await.unwrap().unwrap();
    let b = store.claim_capacity(p, 30).await.unwrap().unwrap();
    // Even a caller-mutated Claim cannot redirect the retained request.
    let impostor = HostId::generate();
    a.request.host = impostor;
    let (x, y) = tokio::join!(
        store.apply_static_capacity(&a),
        store.apply_static_capacity(&b)
    );
    assert_ne!(x.is_ok(), y.is_ok());
    assert_eq!(store.registered_hosts(Some(p)).await.unwrap().len(), 1);
    assert!(store.registered_host(impostor).await.unwrap().is_none());
}

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn retire_then_replace_at_max_preserves_history_without_spending_slot(pool: PgPool) {
    use sandbox_store::host_registry::HostTransport;
    let store = Store::from_pool(pool.clone());
    let p = Uuid::now_v7();
    let mut one = spec(p, "replace");
    one.max_hosts = 1;
    store.put_host_pool(&one).await.unwrap();
    let old = HostId::generate();
    store
        .provision_host(old, 1, &provision(Some(p)))
        .await
        .unwrap();
    let transport = HostTransport {
        version: 1,
        endpoint: "https://machine.internal:7443".into(),
        certificate_sha256: "a".repeat(64),
        provider_ref: Some("static/machine".into()),
    };
    store.set_host_transport(old, 1, &transport).await.unwrap();
    store
        .observe_configured_host_with_capabilities(old, 1, &capabilities().to_json().unwrap(), true)
        .await
        .unwrap();
    let replacement = HostId::generate();
    assert!(
        store
            .provision_host(replacement, 1, &provision(Some(p)))
            .await
            .is_err()
    );
    store
        .enqueue_capacity(
            p,
            "retire",
            &Request {
                host: old,
                epoch: 1,
                action: Action::Retire,
            },
        )
        .await
        .unwrap();
    let claim = store.claim_capacity(p, 30).await.unwrap().unwrap();
    store.apply_static_capacity(&claim).await.unwrap();
    assert_eq!(store.host_pool(p).await.unwrap().unwrap().enrolled_hosts, 0);
    store
        .provision_host(replacement, 1, &provision(Some(p)))
        .await
        .unwrap();
    store
        .set_host_transport(replacement, 1, &transport)
        .await
        .unwrap();
    assert_eq!(store.registered_hosts(Some(p)).await.unwrap().len(), 1);
    let hosts = store
        .registered_host_history(Some(p), None, 10)
        .await
        .unwrap();
    assert_eq!(hosts.len(), 2);
    assert!(
        hosts
            .iter()
            .find(|h| h.host_id == old)
            .unwrap()
            .capacity_retired
    );
    assert_eq!(store.host_pool(p).await.unwrap().unwrap().enrolled_hosts, 1);
    assert!(
        store
            .provision_host(HostId::generate(), 1, &provision(Some(p)))
            .await
            .is_err()
    );
    // Tombstone retains its registration, but no maintenance retry may reopen.
    assert_eq!(
        store
            .registered_host(old)
            .await
            .unwrap()
            .unwrap()
            .transport
            .unwrap()
            .endpoint,
        transport.endpoint
    );
    assert!(store.finish_host_drain(old, 1).await.is_err());
    store
        .observe_configured_host_with_capabilities(
            replacement,
            1,
            &capabilities().to_json().unwrap(),
            true,
        )
        .await
        .unwrap();
    store
        .enqueue_capacity(
            p,
            "retire-replacement",
            &Request {
                host: replacement,
                epoch: 1,
                action: Action::Retire,
            },
        )
        .await
        .unwrap();
    let claim = store.claim_capacity(p, 30).await.unwrap().unwrap();
    store.apply_static_capacity(&claim).await.unwrap();
    // All workers are retired and no intent remains unfinished: reconfigure
    // adapter type, preserving both historical host identities and receipts.
    let mut k8s = one;
    k8s.pool_type = PoolType::K8s;
    k8s.provider_config =
        json!({"version":1,"namespace":"workers","worker_label_selector":"app=supervisor"});
    assert_eq!(
        store.put_host_pool(&k8s).await.unwrap().pool_type,
        PoolType::K8s
    );
    assert!(store.registered_hosts(Some(p)).await.unwrap().is_empty());
    assert_eq!(
        store
            .registered_host_history(Some(p), None, 10)
            .await
            .unwrap()
            .len(),
        2
    );
}

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn pending_intent_bound_is_serialized_and_exact_retry_survives_bound(pool: PgPool) {
    let store = Store::from_pool(pool.clone());
    let p = Uuid::now_v7();
    store.put_host_pool(&spec(p, "queue-bound")).await.unwrap();
    let request = Request {
        host: HostId::generate(),
        epoch: 1,
        action: Action::Status,
    };
    let retained = store
        .enqueue_capacity(p, "retained", &request)
        .await
        .unwrap();
    sqlx::query("INSERT INTO capacity_operations(id,host_pool_id,kind,target_ref,idempotency_key,request) SELECT gen_random_uuid(),$1,'status','fixture',n::text,$2 FROM generate_series(1,998) n")
        .bind(p).bind(serde_json::to_value(&request).unwrap()).execute(&pool).await.unwrap();
    let (a, b) = tokio::join!(
        store.enqueue_capacity(p, "last-a", &request),
        store.enqueue_capacity(p, "last-b", &request)
    );
    assert_ne!(a.is_ok(), b.is_ok());
    assert_eq!(
        store
            .enqueue_capacity(p, "retained", &request)
            .await
            .unwrap(),
        retained
    );
    assert!(matches!(
        store.enqueue_capacity(p, "overflow", &request).await,
        Err(Error::Invalid)
    ));
}

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn failing_oldest_intent_rejoins_queue_and_attempts_are_bounded(pool: PgPool) {
    let store = Store::from_pool(pool.clone());
    let p = Uuid::now_v7();
    store.put_host_pool(&spec(p, "fair")).await.unwrap();
    let host = HostId::generate();
    store
        .provision_host(host, 1, &provision(Some(p)))
        .await
        .unwrap();
    store
        .observe_configured_host_with_capabilities(
            host,
            1,
            &capabilities().to_json().unwrap(),
            true,
        )
        .await
        .unwrap();
    store.finish_host_drain(host, 1).await.unwrap();
    let request = |action| Request {
        host,
        epoch: 1,
        action,
    };
    // Retire is enqueued first but can only succeed after the later drain.
    let retire = store
        .enqueue_capacity(p, "retire", &request(Action::Retire))
        .await
        .unwrap();
    let drain = store
        .enqueue_capacity(p, "drain", &request(Action::Drain))
        .await
        .unwrap();
    let claim = store.claim_capacity(p, 30).await.unwrap().unwrap();
    assert_eq!(claim.id, retire);
    assert!(matches!(
        store.apply_static_capacity(&claim).await,
        Err(Error::Conflict)
    ));
    store.retry_capacity(&claim).await.unwrap();
    let claim = store.claim_capacity(p, 30).await.unwrap().unwrap();
    assert_eq!(claim.id, drain);
    // Abandoned (lease-expired) work also rejoins behind already waiting work.
    sqlx::query("UPDATE capacity_operations SET lease_expires_at=clock_timestamp(),available_at=clock_timestamp() WHERE id=$1")
        .bind(drain).execute(&pool).await.unwrap();
    let claim = store.claim_capacity(p, 30).await.unwrap().unwrap();
    assert_eq!(claim.id, retire);
    store.retry_capacity(&claim).await.unwrap();
    let claim = store.claim_capacity(p, 30).await.unwrap().unwrap();
    assert_eq!(claim.id, drain);
    store.apply_static_capacity(&claim).await.unwrap();
    let claim = store.claim_capacity(p, 30).await.unwrap().unwrap();
    assert_eq!(claim.id, retire);
    assert_eq!(
        store.apply_static_capacity(&claim).await.unwrap()["retired"],
        true
    );

    // A permanently failing intent is claimed at most MAX_ATTEMPTS times.
    let stuck = store
        .enqueue_capacity(
            p,
            "stuck",
            &Request {
                host: HostId::generate(),
                epoch: 1,
                action: Action::Status,
            },
        )
        .await
        .unwrap();
    for _ in 0..sandbox_store::capacity::MAX_ATTEMPTS {
        let claim = store.claim_capacity(p, 30).await.unwrap().unwrap();
        assert_eq!(claim.id, stuck);
        assert!(store.apply_static_capacity(&claim).await.is_err());
        store.retry_capacity(&claim).await.unwrap();
    }
    assert!(store.claim_capacity(p, 30).await.unwrap().is_none());
    assert_eq!(
        store.capacity_operation(stuck).await.unwrap().unwrap()["status"],
        "queued"
    );
}

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn stale_unapplied_static_intent_cancels_and_unblocks_type_change(pool: PgPool) {
    let store = Store::from_pool(pool.clone());
    let p = Uuid::now_v7();
    store.put_host_pool(&spec(p, "cancel")).await.unwrap();
    let request = Request {
        host: HostId::generate(),
        epoch: 1,
        action: Action::Status,
    };
    let id = store.enqueue_capacity(p, "stale", &request).await.unwrap();
    let mut k8s = spec(p, "cancel");
    k8s.pool_type = PoolType::K8s;
    k8s.provider_config =
        json!({"version":1,"namespace":"workers","worker_label_selector":"app=supervisor"});
    assert!(store.put_host_pool(&k8s).await.is_err());
    // A live claim may be applying: retained.
    let claim = store.claim_capacity(p, 30).await.unwrap().unwrap();
    assert!(matches!(
        store.cancel_capacity(p, id).await,
        Err(Error::Conflict)
    ));
    assert!(matches!(
        store.cancel_capacity(Uuid::now_v7(), id).await,
        Err(Error::Conflict)
    ));
    sqlx::query("UPDATE capacity_operations SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE id=$1")
        .bind(id).execute(&pool).await.unwrap();
    store.cancel_capacity(p, id).await.unwrap();
    store.cancel_capacity(p, id).await.unwrap();
    assert!(matches!(
        store.apply_static_capacity(&claim).await,
        Err(Error::LostClaim)
    ));
    let row = store.capacity_operation(id).await.unwrap().unwrap();
    assert_eq!(row["status"], "cancelled");
    assert!(row["provider_receipt"].is_null());
    assert_eq!(
        store.enqueue_capacity(p, "stale", &request).await.unwrap(),
        id
    );
    assert!(store.claim_capacity(p, 30).await.unwrap().is_none());

    // Completed receipts are retained, never cancelled.
    let host = HostId::generate();
    store
        .provision_host(host, 1, &provision(Some(p)))
        .await
        .unwrap();
    let done = store
        .enqueue_capacity(
            p,
            "done",
            &Request {
                host,
                epoch: 1,
                action: Action::Status,
            },
        )
        .await
        .unwrap();
    let claim = store.claim_capacity(p, 30).await.unwrap().unwrap();
    store.apply_static_capacity(&claim).await.unwrap();
    assert!(matches!(
        store.cancel_capacity(p, done).await,
        Err(Error::Conflict)
    ));
    assert_eq!(
        store.capacity_operation(done).await.unwrap().unwrap()["status"],
        "succeeded"
    );

    // With no active worker or unfinished intent, the type change proceeds.
    let empty = Uuid::now_v7();
    store.put_host_pool(&spec(empty, "empty")).await.unwrap();
    let stale = store
        .enqueue_capacity(empty, "stale", &request)
        .await
        .unwrap();
    store.cancel_capacity(empty, stale).await.unwrap();
    let mut k8s = spec(empty, "empty");
    k8s.pool_type = PoolType::K8s;
    k8s.provider_config =
        json!({"version":1,"namespace":"workers","worker_label_selector":"app=supervisor"});
    store.put_host_pool(&k8s).await.unwrap();
    // Unfinished intent of a non-static pool may have uncertain effects: retained.
    let uncertain = store
        .enqueue_capacity(empty, "uncertain", &request)
        .await
        .unwrap();
    assert!(matches!(
        store.cancel_capacity(empty, uncertain).await,
        Err(Error::Conflict)
    ));
}

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn retired_history_beyond_registry_bound_pages_and_keeps_active_read(pool: PgPool) {
    use sandbox_store::host_registry::{MAX_HISTORY_PAGE, MAX_REGISTRY_HOSTS};
    let store = Store::from_pool(pool.clone());
    let p = Uuid::now_v7();
    store.put_host_pool(&spec(p, "history")).await.unwrap();
    let active = HostId::generate();
    store
        .provision_host(active, 1, &provision(Some(p)))
        .await
        .unwrap();
    sqlx::query("INSERT INTO hosts(id,status,cpu_capacity,memory_capacity_mib,disk_capacity_mib,supervisor_epoch,host_pool_id,capacity_retired_at) SELECT gen_random_uuid(),'draining',4,8192,65536,1,$1,clock_timestamp() FROM generate_series(1,$2)")
        .bind(p).bind(MAX_REGISTRY_HOSTS + 5).execute(&pool).await.unwrap();
    let hosts = store.registered_hosts(None).await.unwrap();
    assert_eq!(hosts.len(), 1);
    assert_eq!(hosts[0].host_id, active);
    assert_eq!(store.registered_hosts(Some(p)).await.unwrap().len(), 1);
    assert!(
        store
            .registered_host_history(None, None, MAX_HISTORY_PAGE + 1)
            .await
            .is_err()
    );
    assert!(store.registered_host_history(None, None, 0).await.is_err());
    let (mut after, mut seen, mut retired) = (None, 0_i64, 0_i64);
    loop {
        let page = store
            .registered_host_history(Some(p), after, MAX_HISTORY_PAGE)
            .await
            .unwrap();
        seen += page.len() as i64;
        retired += page.iter().filter(|h| h.capacity_retired).count() as i64;
        if (page.len() as i64) < MAX_HISTORY_PAGE {
            break;
        }
        after = page.last().map(|h| h.host_id);
    }
    assert_eq!(seen, MAX_REGISTRY_HOSTS + 6);
    assert_eq!(retired, MAX_REGISTRY_HOSTS + 5);
}
