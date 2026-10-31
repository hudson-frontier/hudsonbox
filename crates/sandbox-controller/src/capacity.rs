//! Capacity providers supply infrastructure only. PostgreSQL/controller remain
//! the authority for sandbox admission, placement, cleanup and lifecycle.
use sandbox_store::{
    Store,
    capacity::{Claim, Error},
    host_registry::{HostPool, PoolType},
};
use serde::Serialize;
use serde_json::Value;
use std::{collections::HashMap, future::Future, pin::Pin, sync::Arc};

pub type CapacityFuture<'a> = Pin<Box<dyn Future<Output = Result<Value, Error>> + Send + 'a>>;
/// Future adapters must persist/reconcile external provider receipts before
/// retrying uncertain effects. Use the durable operation ID as the provider
/// idempotency identity; revalidate the claim/type at the dispatch boundary and
/// retain unfinished intent until external effects are reconciled. Pool type
/// changes are blocked for unfinished intent, but config may be updated: freeze
/// any effect-defining config/credential reference in provider intent/receipts.
/// A database lease alone cannot fence a provider. This trait does not claim
/// to implement external provider fencing; standalone effects are transactional.
pub trait CapacityProvider: Send + Sync + std::fmt::Debug {
    fn pool_type(&self) -> PoolType;
    fn apply<'a>(&'a self, store: &'a Store, claim: &'a Claim) -> CapacityFuture<'a>;
}
#[derive(Debug, Default)]
pub struct StandaloneStaticProvider;
impl CapacityProvider for StandaloneStaticProvider {
    fn pool_type(&self) -> PoolType {
        PoolType::Standalone
    }
    fn apply<'a>(&'a self, store: &'a Store, claim: &'a Claim) -> CapacityFuture<'a> {
        Box::pin(store.apply_static_capacity(claim))
    }
}
/// Application code dispatches exactly the registry's standalone/k8s enum.
/// An absent k8s implementation fails closed; it never runs standalone effects.
#[derive(Debug, Default)]
pub struct ProviderRouter {
    adapters: HashMap<PoolType, Arc<dyn CapacityProvider>>,
}
impl ProviderRouter {
    pub fn with(mut self, adapter: Arc<dyn CapacityProvider>) -> Self {
        self.adapters.insert(adapter.pool_type(), adapter);
        self
    }
    pub async fn apply(&self, store: &Store, claim: &Claim) -> Result<Value, Error> {
        store.validate_capacity_claim(claim).await?;
        let pool = store
            .host_pool(claim.pool)
            .await
            .map_err(|_| Error::Conflict)?
            .ok_or(Error::Conflict)?;
        self.adapters
            .get(&pool.pool_type)
            .ok_or(Error::Conflict)?
            .apply(store, claim)
            .await
    }
}

/// Bounded advisory demand, expressed in supervisor workers, never machines or
/// sandbox lifecycle actions. No automatic provisioning/retirement follows it.
#[derive(Debug, Clone, Copy, Serialize)]
pub struct Demand {
    pub waiting_sandboxes: u32,
    pub sandboxes_per_host: u32,
    pub max_additional_hosts: u32,
}
#[derive(Debug, PartialEq, Eq, Serialize)]
pub struct CapacityPlan {
    pub enrolled_hosts: u32,
    pub ready_hosts: u32,
    pub suggested_additional_hosts: u32,
    pub operator_action_required: bool,
}
pub async fn plan(store: &Store, pool: &HostPool, demand: Demand) -> Result<CapacityPlan, Error> {
    if demand.waiting_sandboxes > 100_000
        || demand.sandboxes_per_host == 0
        || demand.sandboxes_per_host > 100_000
        || demand.max_additional_hosts > 1000
    {
        return Err(Error::Invalid);
    }
    let hosts = store
        .registered_hosts(Some(pool.id))
        .await
        .map_err(|_| Error::Invalid)?;
    let enrolled = if pool.pool_type == PoolType::K8s {
        store.k8s_capacity_inventory(pool.id).await?
    } else {
        u32::try_from(hosts.iter().filter(|h| !h.capacity_retired).count())
            .map_err(|_| Error::Invalid)?
    };
    let ready = u32::try_from(hosts.iter().filter(|h| h.admits_new_work()).count())
        .map_err(|_| Error::Invalid)?;
    let max = u32::try_from(pool.max_hosts).map_err(|_| Error::Invalid)?;
    let min = u32::try_from(pool.min_ready_hosts).map_err(|_| Error::Invalid)?;
    let additional = demand
        .waiting_sandboxes
        .div_ceil(demand.sandboxes_per_host)
        .max(min.saturating_sub(ready))
        .min(max.saturating_sub(enrolled))
        .min(demand.max_additional_hosts);
    Ok(CapacityPlan {
        enrolled_hosts: enrolled,
        ready_hosts: ready,
        suggested_additional_hosts: additional,
        operator_action_required: additional > 0,
    })
}
