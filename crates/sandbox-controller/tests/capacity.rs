//! Fake adapter routing and advisory demand; no physical provider or KVM.
#![allow(clippy::unwrap_used, clippy::expect_used)]
use sandbox_controller::capacity::{self, CapacityFuture, CapacityProvider, ProviderRouter};
use sandbox_protocol::{HostId, Id};
use sandbox_store::{
    Store,
    capacity::{Action, Claim, Error, Request},
    host_registry::{HostPoolSpec, PlacementPolicy, PoolType, Tenancy},
};
use serde_json::json;
use sqlx::PgPool;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use uuid::Uuid;

#[derive(Debug)]
struct Fake {
    kind: PoolType,
    calls: Arc<AtomicUsize>,
}
impl CapacityProvider for Fake {
    fn pool_type(&self) -> PoolType {
        self.kind
    }
    fn apply<'a>(&'a self, _store: &'a Store, _claim: &'a Claim) -> CapacityFuture<'a> {
        Box::pin(async move {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(json!({"fake":true}))
        })
    }
}
async fn pool(store: &Store, kind: PoolType, name: &str) -> Uuid {
    let id = Uuid::now_v7();
    store.put_host_pool(&HostPoolSpec {version:1,id,name:name.into(),pool_type:kind,min_ready_hosts:1,max_hosts:2,
        placement_policy:PlacementPolicy{version:1,tenancy:Tenancy::Shared},
        provider_config:match kind {PoolType::Standalone=>json!({"version":1}),PoolType::K8s=>json!({"version":1,"namespace":"workers","worker_label_selector":"app=supervisor"})},credential_secret_ref:None}).await.unwrap();
    id
}

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn router_selects_exact_type_and_never_calls_adapter_with_expired_claim(pg: PgPool) {
    let store = Store::from_pool(pg.clone());
    let calls = [Arc::new(AtomicUsize::new(0)), Arc::new(AtomicUsize::new(0))];
    let standalone = pool(&store, PoolType::Standalone, "static").await;
    let k8s = pool(&store, PoolType::K8s, "k8s").await;
    let router = ProviderRouter::default().with(Arc::new(Fake {
        kind: PoolType::Standalone,
        calls: calls[0].clone(),
    }));
    let request = Request {
        host: HostId::generate(),
        epoch: 1,
        action: Action::Status,
    };
    store
        .enqueue_capacity(standalone, "static", &request)
        .await
        .unwrap();
    store
        .enqueue_capacity(k8s, "worker", &request)
        .await
        .unwrap();
    let a = store.claim_capacity(standalone, 30).await.unwrap().unwrap();
    let b = store.claim_capacity(k8s, 30).await.unwrap().unwrap();
    router.apply(&store, &a).await.unwrap();
    assert!(matches!(
        router.apply(&store, &b).await,
        Err(Error::Conflict)
    ));
    assert_eq!(calls[0].load(Ordering::SeqCst), 1);
    let router = router.with(Arc::new(Fake {
        kind: PoolType::K8s,
        calls: calls[1].clone(),
    }));
    router.apply(&store, &b).await.unwrap();
    assert_eq!(calls[1].load(Ordering::SeqCst), 1);
    sqlx::query("UPDATE capacity_operations SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE id=$1").bind(b.id).execute(&pg).await.unwrap();
    assert!(matches!(
        router.apply(&store, &b).await,
        Err(Error::LostClaim)
    ));
    assert_eq!(calls[1].load(Ordering::SeqCst), 1);
}

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn demand_bounds_exclude_retired_inventory_and_never_enqueue(pg: PgPool) {
    let store = Store::from_pool(pg.clone());
    let id = pool(&store, PoolType::Standalone, "demand").await;
    let host = HostId::generate();
    // Fixture-only tombstone, equivalent to a completed safe retirement.
    sqlx::query("INSERT INTO hosts(id,status,cpu_capacity,memory_capacity_mib,disk_capacity_mib,supervisor_epoch,host_pool_id,capacity_retired_at) VALUES($1,'draining',4,8192,65536,1,$2,clock_timestamp())")
        .bind(host.uuid()).bind(id).execute(&pg).await.unwrap();
    let pool = store.host_pool(id).await.unwrap().unwrap();
    let demand = capacity::Demand {
        waiting_sandboxes: 100,
        sandboxes_per_host: 2,
        max_additional_hosts: 1,
    };
    let plan = capacity::plan(&store, &pool, demand).await.unwrap();
    assert_eq!(plan.enrolled_hosts, 0);
    assert_eq!(plan.suggested_additional_hosts, 1);
    assert_eq!(
        capacity::plan(
            &store,
            &pool,
            capacity::Demand {
                max_additional_hosts: 1000,
                ..demand
            }
        )
        .await
        .unwrap()
        .suggested_additional_hosts,
        2
    );
    assert!(
        capacity::plan(
            &store,
            &pool,
            capacity::Demand {
                sandboxes_per_host: 0,
                ..demand
            }
        )
        .await
        .is_err()
    );
    assert!(
        capacity::plan(
            &store,
            &pool,
            capacity::Demand {
                waiting_sandboxes: 100001,
                ..demand
            }
        )
        .await
        .is_err()
    );
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM capacity_operations")
        .fetch_one(&pg)
        .await
        .unwrap();
    assert_eq!(count, 0);
}
