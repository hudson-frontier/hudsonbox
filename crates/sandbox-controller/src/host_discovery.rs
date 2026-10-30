//! Provider-neutral supervisor discovery and the pool-type provider router.
//!
//! Discovery is advisory: it reports which supervisor workers a deployment
//! adapter can see. It never enrolls hosts, issues epochs, changes transport
//! identity or reopens placement; those remain operator registry actions.
//! PostgreSQL stays the sole lifecycle authority for both pool types.

use std::{
    collections::{BTreeMap, BTreeSet},
    future::Future,
    pin::Pin,
    sync::Arc,
};

use sandbox_protocol::HostId;
use sandbox_store::host_registry::{
    HostPool, HostTransport, PoolType, ProviderConfig, RegisteredHost,
};
use serde::{Deserialize, Serialize};

/// Bound on one static configuration or discovery result.
pub const MAX_DISCOVERED_WORKERS: usize = 1000;

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum DiscoveryError {
    #[error("discovery configuration is invalid")]
    InvalidConfig,
    #[error("no discovery adapter is configured for pool type {0}")]
    Unsupported(&'static str),
    #[error("discovery adapter does not serve this pool type")]
    WrongPoolType,
    #[error("discovery result exceeds bounds or repeats an identity")]
    InvalidResult,
    #[error("discovery provider is unavailable")]
    Unavailable,
}

/// A supervisor worker as seen by a deployment adapter. Not authority by itself.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DiscoveredWorker {
    pub host_id: HostId,
    pub endpoint: String,
    pub certificate_sha256: String,
    #[serde(default)]
    pub provider_ref: Option<String>,
}

impl DiscoveredWorker {
    fn normalized(&self) -> Result<Self, DiscoveryError> {
        let transport = HostTransport {
            version: 1,
            endpoint: self.endpoint.clone(),
            certificate_sha256: self.certificate_sha256.clone(),
            provider_ref: self.provider_ref.clone(),
        }
        .normalized()
        .map_err(|_| DiscoveryError::InvalidResult)?;
        Ok(Self {
            host_id: self.host_id,
            endpoint: transport.endpoint,
            certificate_sha256: transport.certificate_sha256,
            provider_ref: transport.provider_ref,
        })
    }
}

pub type DiscoveryFuture<'a> =
    Pin<Box<dyn Future<Output = Result<Vec<DiscoveredWorker>, DiscoveryError>> + Send + 'a>>;

/// One deployment adapter's discovery. Implementations: [`StandaloneStaticDiscovery`]
/// now; a Kubernetes worker adapter (#197) implements this same trait for `k8s`
/// pools, translating worker Pods into [`DiscoveredWorker`] values.
pub trait HostDiscovery: Send + Sync + std::fmt::Debug {
    fn pool_type(&self) -> PoolType;
    fn discover<'a>(&'a self, pool: &'a HostPool) -> DiscoveryFuture<'a>;
}

/// Validate bounds, canonical form and identity uniqueness of a discovery result.
pub fn validate_workers(
    workers: &[DiscoveredWorker],
) -> Result<Vec<DiscoveredWorker>, DiscoveryError> {
    if workers.len() > MAX_DISCOVERED_WORKERS {
        return Err(DiscoveryError::InvalidResult);
    }
    let mut hosts = BTreeSet::new();
    let mut endpoints = BTreeSet::new();
    let mut certificates = BTreeSet::new();
    let mut providers = BTreeSet::new();
    let mut out = Vec::with_capacity(workers.len());
    for worker in workers {
        let worker = worker.normalized()?;
        if !hosts.insert(worker.host_id)
            || !endpoints.insert(worker.endpoint.clone())
            || !certificates.insert(worker.certificate_sha256.clone())
            || worker
                .provider_ref
                .as_ref()
                .is_some_and(|r| !providers.insert(r.clone()))
        {
            return Err(DiscoveryError::InvalidResult);
        }
        out.push(worker);
    }
    Ok(out)
}

/// Operator-configured standalone addresses, per pool.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StaticDiscoveryConfig {
    pub version: u32,
    pub pools: BTreeMap<uuid::Uuid, Vec<DiscoveredWorker>>,
}

#[derive(Debug, Clone)]
pub struct StandaloneStaticDiscovery {
    pools: BTreeMap<uuid::Uuid, Vec<DiscoveredWorker>>,
}

impl StandaloneStaticDiscovery {
    pub fn new(config: StaticDiscoveryConfig) -> Result<Self, DiscoveryError> {
        if config.version != 1 || config.pools.len() > MAX_DISCOVERED_WORKERS {
            return Err(DiscoveryError::InvalidConfig);
        }
        let mut pools = BTreeMap::new();
        let all: Vec<_> = config.pools.values().flatten().cloned().collect();
        // A host identity, address or certificate appears in at most one pool.
        validate_workers(&all).map_err(|_| DiscoveryError::InvalidConfig)?;
        for (pool, workers) in config.pools {
            pools.insert(
                pool,
                validate_workers(&workers).map_err(|_| DiscoveryError::InvalidConfig)?,
            );
        }
        Ok(Self { pools })
    }

    pub fn from_json(bytes: &[u8]) -> Result<Self, DiscoveryError> {
        if bytes.is_empty() || bytes.len() > 1 << 20 {
            return Err(DiscoveryError::InvalidConfig);
        }
        Self::new(serde_json::from_slice(bytes).map_err(|_| DiscoveryError::InvalidConfig)?)
    }
}

impl HostDiscovery for StandaloneStaticDiscovery {
    fn pool_type(&self) -> PoolType {
        PoolType::Standalone
    }
    fn discover<'a>(&'a self, pool: &'a HostPool) -> DiscoveryFuture<'a> {
        Box::pin(async move {
            if !matches!(pool.provider_config, ProviderConfig::Standalone(_)) {
                return Err(DiscoveryError::WrongPoolType);
            }
            Ok(self.pools.get(&pool.id).cloned().unwrap_or_default())
        })
    }
}

/// Application-code provider router: pool type → discovery adapter. No table.
#[derive(Debug, Default, Clone)]
pub struct ProviderRouter {
    adapters: BTreeMap<&'static str, Arc<dyn HostDiscovery>>,
}

impl ProviderRouter {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn with(mut self, adapter: Arc<dyn HostDiscovery>) -> Self {
        self.adapters.insert(adapter.pool_type().as_str(), adapter);
        self
    }

    pub fn adapter(&self, pool_type: PoolType) -> Result<&dyn HostDiscovery, DiscoveryError> {
        self.adapters
            .get(pool_type.as_str())
            .map(AsRef::as_ref)
            .ok_or(DiscoveryError::Unsupported(pool_type.as_str()))
    }

    /// Discover through the adapter for the pool's own type, then validate.
    pub async fn discover(&self, pool: &HostPool) -> Result<Vec<DiscoveredWorker>, DiscoveryError> {
        let adapter = self.adapter(pool.pool_type)?;
        if adapter.pool_type() != pool.provider_config.pool_type() {
            return Err(DiscoveryError::WrongPoolType);
        }
        validate_workers(&adapter.discover(pool).await?)
    }
}

/// Difference between discovery and the registry. Report only: every
/// corrective action is an explicit operator registry update.
#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize)]
pub struct DiscoveryReport {
    /// Registered with matching endpoint, certificate and provider reference.
    pub matched: Vec<HostId>,
    /// Seen by discovery but not enrolled in this pool.
    pub unregistered: Vec<DiscoveredWorker>,
    /// Enrolled in this pool but not discovered (possibly stopped or unreachable).
    pub missing: Vec<HostId>,
    /// Discovered with a different endpoint, certificate or provider reference.
    pub mismatched: Vec<HostId>,
    /// Registered hosts whose transport is still legacy controller configuration.
    pub unaddressed: Vec<HostId>,
}

#[must_use]
pub fn reconcile(
    discovered: &[DiscoveredWorker],
    registered: &[RegisteredHost],
) -> DiscoveryReport {
    let mut report = DiscoveryReport::default();
    let by_id: BTreeMap<HostId, &RegisteredHost> =
        registered.iter().map(|h| (h.host_id, h)).collect();
    let seen: BTreeSet<HostId> = discovered.iter().map(|w| w.host_id).collect();
    for worker in discovered {
        let worker = match worker.normalized() {
            Ok(worker) => worker,
            Err(_) => {
                report.mismatched.push(worker.host_id);
                continue;
            }
        };
        match by_id.get(&worker.host_id) {
            None => report.unregistered.push(worker),
            Some(host) => match &host.transport {
                None => report.unaddressed.push(host.host_id),
                Some(t)
                    if t.endpoint == worker.endpoint
                        && t.certificate_sha256 == worker.certificate_sha256
                        && host.provider_ref == worker.provider_ref =>
                {
                    report.matched.push(host.host_id);
                }
                Some(_) => report.mismatched.push(host.host_id),
            },
        }
    }
    report.missing = registered
        .iter()
        .filter(|h| !seen.contains(&h.host_id))
        .map(|h| h.host_id)
        .collect();
    report
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use sandbox_protocol::Id;
    use sandbox_store::host_registry::{
        K8sProviderConfig, PlacementPolicy, StandaloneProviderConfig, Tenancy,
    };

    fn pool(id: uuid::Uuid, config: ProviderConfig) -> HostPool {
        HostPool {
            id,
            name: "fixture".into(),
            pool_type: config.pool_type(),
            min_ready_hosts: 0,
            max_hosts: 4,
            placement_policy: PlacementPolicy {
                version: 1,
                tenancy: Tenancy::Shared,
            },
            provider_config: config,
            credential_secret_ref: None,
            enrolled_hosts: 0,
        }
    }
    fn worker(n: u8) -> DiscoveredWorker {
        DiscoveredWorker {
            host_id: HostId::generate(),
            endpoint: format!("https://10.0.0.{n}:7443"),
            certificate_sha256: format!("{n:02x}").repeat(32),
            provider_ref: None,
        }
    }

    #[test]
    fn server_name_matches_supervisor_transport() {
        let host = HostId::generate();
        assert_eq!(
            sandbox_store::host_registry::host_server_name(host),
            sandbox_supervisor::transport::host_server_name(host)
        );
    }

    #[tokio::test]
    async fn static_discovery_routes_by_pool_type_and_rejects_duplicates() {
        let id = uuid::Uuid::now_v7();
        let (a, b) = (worker(1), worker(2));
        let config = StaticDiscoveryConfig {
            version: 1,
            pools: BTreeMap::from([(id, vec![a.clone(), b.clone()])]),
        };
        let router = ProviderRouter::new().with(Arc::new(
            StandaloneStaticDiscovery::new(config.clone()).unwrap(),
        ));
        let standalone = pool(
            id,
            ProviderConfig::Standalone(StandaloneProviderConfig { version: 1 }),
        );
        assert_eq!(
            router.discover(&standalone).await.unwrap(),
            vec![a.clone(), b.clone()]
        );
        // Kubernetes pools have no adapter without #197; no fallback to standalone.
        let k8s = pool(
            id,
            ProviderConfig::K8s(K8sProviderConfig {
                version: 1,
                namespace: "workers".into(),
                worker_label_selector: "app=supervisor".into(),
            }),
        );
        assert_eq!(
            router.discover(&k8s).await,
            Err(DiscoveryError::Unsupported("k8s"))
        );
        for duplicate in [
            DiscoveredWorker {
                host_id: a.host_id,
                ..worker(3)
            },
            DiscoveredWorker {
                endpoint: "HTTPS://10.0.0.1:7443".into(),
                ..worker(4)
            },
            DiscoveredWorker {
                certificate_sha256: a.certificate_sha256.to_uppercase(),
                ..worker(5)
            },
        ] {
            let mut config = config.clone();
            config.pools.get_mut(&id).unwrap().push(duplicate);
            assert_eq!(
                StandaloneStaticDiscovery::new(config).unwrap_err(),
                DiscoveryError::InvalidConfig
            );
        }
        let mut insecure = config;
        insecure.pools.get_mut(&id).unwrap()[0].endpoint = "http://10.0.0.1:7443".into();
        assert!(StandaloneStaticDiscovery::new(insecure).is_err());
    }

    fn registered(worker: &DiscoveredWorker) -> RegisteredHost {
        RegisteredHost {
            host_id: worker.host_id,
            host_pool_id: uuid::Uuid::now_v7(),
            pool_type: PoolType::Standalone,
            status: "ready".into(),
            supervisor_epoch: 1,
            transport: Some(sandbox_store::host_registry::TransportIdentity {
                endpoint: worker.endpoint.clone(),
                server_name: sandbox_store::host_registry::host_server_name(worker.host_id),
                certificate_sha256: worker.certificate_sha256.clone(),
            }),
            provider_ref: worker.provider_ref.clone(),
            capacity: sandbox_store::host_maintenance::HostCapacity {
                vcpu: 4,
                memory_mib: 8192,
                disk_mib: 65536,
            },
            capability_policy: serde_json::json!({}),
            observed_capabilities: None,
            observed_capabilities_epoch: None,
            last_seen_at: None,
            heartbeat_age_ms: None,
            heartbeat_fresh: false,
        }
    }

    #[test]
    fn reconcile_reports_without_enrolling_or_trusting_discovery() {
        let (same, moved, fresh, gone, legacy) =
            (worker(1), worker(2), worker(3), worker(4), worker(5));
        let mut moved_registration = registered(&moved);
        moved_registration.transport.as_mut().unwrap().endpoint = "https://10.0.0.99:7443".into();
        let mut legacy_registration = registered(&legacy);
        legacy_registration.transport = None;
        let report = reconcile(
            &[same.clone(), moved.clone(), fresh.clone(), legacy.clone()],
            &[
                registered(&same),
                moved_registration,
                registered(&gone),
                legacy_registration,
            ],
        );
        assert_eq!(report.matched, vec![same.host_id]);
        assert_eq!(report.mismatched, vec![moved.host_id]);
        assert_eq!(report.unregistered, vec![fresh]);
        assert_eq!(report.missing, vec![gone.host_id]);
        assert_eq!(report.unaddressed, vec![legacy.host_id]);
    }
}
