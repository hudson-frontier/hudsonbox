//! Per-host owner routing over bounded, identity-pinned supervisor channels.
//!
//! Registered hosts (`hosts.endpoint` set) are addressed only at their
//! registered endpoint and accepted only with the registered server leaf
//! certificate and per-host subjectAltName. The single host configured on the
//! command line remains addressable through its configured endpoint while its
//! registry transport is NULL (legacy single-host mode). Every request uses the
//! persisted allocation owner and the epoch this process last authenticated.
use super::*;
use futures_util::{StreamExt, stream};
use sandbox_store::host_registry::RegisteredHost;
use sandbox_supervisor::routing::{ChannelPool, ClientTls, HostAddress, Lease, PoolLimits};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Duration,
};

/// Health, identity and allocation-authority refresh for one host.
pub const HEALTH_DEADLINE: Duration = Duration::from_secs(3);
/// Hosts refreshed concurrently by one tick.
pub const HEALTH_CONCURRENCY: usize = 16;

#[derive(Debug)]
pub(crate) struct Hosts {
    pool: ChannelPool,
    state: Mutex<HashMap<HostId, HostState>>,
}

#[derive(Debug)]
struct HostState {
    address: HostAddress,
    healthy: bool,
    checking: bool,
    launch_permits_required: bool,
    new_work_compatible: bool,
    /// Serializes allocation-authority registration for this host only.
    authority: Arc<tokio::sync::Mutex<()>>,
}

impl HostState {
    fn new(address: HostAddress) -> Self {
        Self {
            address,
            healthy: false,
            checking: false,
            launch_permits_required: false,
            new_work_compatible: true,
            authority: Arc::new(tokio::sync::Mutex::new(())),
        }
    }
}

/// One request slot on an authenticated, currently healthy owner incarnation.
#[derive(Debug)]
pub(crate) struct Route {
    pub(crate) host: HostId,
    pub(crate) epoch: i64,
    pub(crate) client: SupervisorClient<Channel>,
    authority: Arc<tokio::sync::Mutex<()>>,
    launch_permits_required: bool,
    _lease: Lease,
}

impl Route {
    /// True only when `owner` names exactly this host incarnation.
    pub(crate) fn owns(&self, owner: &sandbox_protocol::supervisor::Ownership) -> bool {
        owner.host_id == self.host.to_string() && owner.supervisor_epoch == self.epoch
    }
}

/// Persisted owner of a claimed operation, resolved before any intent commits.
#[derive(Debug)]
pub(crate) enum Owner {
    Ready(Box<Route>),
    /// The owner host is unregistered, unaddressed, unhealthy or saturated.
    Unavailable,
    /// The sandbox has no current allocation and the configured host is
    /// unavailable; only database-side outcomes can apply.
    None,
}

fn client(lease: &Lease) -> SupervisorClient<Channel> {
    SupervisorClient::new(lease.channel.clone())
        .max_decoding_message_size(transport::MAX_MESSAGE_BYTES)
        .max_encoding_message_size(transport::MAX_MESSAGE_BYTES)
}

impl Hosts {
    pub(crate) fn new(ca: &[u8], cert: &[u8], key: &[u8]) -> Result<Self, ControllerError> {
        let tls = ClientTls::from_pem(ca, cert, key)?;
        Ok(Self {
            pool: ChannelPool::new(tls, PoolLimits::default())?,
            state: Mutex::new(HashMap::new()),
        })
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<HostId, HostState>> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Hosts whose latest check in this process authenticated the current incarnation.
    pub(crate) fn healthy(&self) -> Vec<(HostId, i64)> {
        let mut hosts: Vec<_> = self
            .lock()
            .values()
            .filter(|s| s.healthy)
            .map(|s| (s.address.host, s.address.epoch))
            .collect();
        hosts.sort();
        hosts
    }

    pub(crate) fn new_work_compatible(&self, host: HostId) -> bool {
        self.lock()
            .get(&host)
            .is_none_or(|state| state.new_work_compatible)
    }
}

/// Clears the in-progress mark even when a tick is cancelled mid-check.
struct Checking<'a> {
    hosts: &'a Hosts,
    address: HostAddress,
}

impl Drop for Checking<'_> {
    fn drop(&mut self) {
        if let Some(state) = self.hosts.lock().get_mut(&self.address.host)
            && state.address == self.address
        {
            state.checking = false;
        }
    }
}

impl Controller {
    fn address_of(&self, registered: &RegisteredHost) -> Option<HostAddress> {
        match &registered.transport {
            Some(t) => HostAddress::registered(
                registered.host_id,
                registered.supervisor_epoch,
                &t.endpoint,
                &t.server_name,
                &t.certificate_sha256,
            )
            .ok(),
            None if registered.host_id == self.config.host => {
                HostAddress::legacy(self.config.host, self.config.epoch, &self.config.endpoint).ok()
            }
            None => None,
        }
    }

    /// Current registered address. Unregistered legacy configuration stays addressable
    /// so startup reports the same storage error it always did.
    pub(crate) async fn resolve(
        &self,
        host: HostId,
    ) -> Result<Option<HostAddress>, ControllerError> {
        match self.store.registered_host(host).await? {
            Some(registered) => Ok(self.address_of(&registered)),
            None if host == self.config.host => {
                Ok(
                    HostAddress::legacy(self.config.host, self.config.epoch, &self.config.endpoint)
                        .ok(),
                )
            }
            None => Ok(None),
        }
    }

    /// Reserve one request slot on `host` at exactly `epoch`. Never falls back
    /// to another host, endpoint or incarnation.
    pub(crate) async fn route(
        &self,
        host: HostId,
        epoch: i64,
        long: bool,
    ) -> Result<Route, ControllerError> {
        let address = self
            .resolve(host)
            .await?
            .ok_or(ControllerError::Unroutable)?;
        if address.epoch != epoch {
            return Err(ControllerError::Unroutable);
        }
        let (authority, launch_permits_required) = {
            let state = self.hosts.lock();
            let state = state
                .get(&host)
                .filter(|s| s.healthy && s.address == address)
                .ok_or(ControllerError::Unroutable)?;
            (state.authority.clone(), state.launch_permits_required)
        };
        let lease = self.hosts.pool.acquire(&address, long)?;
        Ok(Route {
            host,
            epoch,
            client: client(&lease),
            authority,
            launch_permits_required,
            _lease: lease,
        })
    }

    /// Route to the current incarnation of the claimed operation's owner host.
    /// Without a current allocation the configured host is used, as in
    /// single-host mode; dispatch transactions still verify ownership.
    pub(crate) async fn owner(&self, claim: &Claim) -> Result<Owner, ControllerError> {
        let Some(host) = self.store.operation_owner_host(claim.operation_id).await? else {
            return Ok(
                match self.route(self.config.host, self.config.epoch, false).await {
                    Ok(route) => Owner::Ready(Box::new(route)),
                    Err(_) => Owner::None,
                },
            );
        };
        let Some(address) = self.resolve(host).await? else {
            return Ok(Owner::Unavailable);
        };
        Ok(match self.route(host, address.epoch, false).await {
            Ok(route) => Owner::Ready(Box::new(route)),
            Err(_) => Owner::Unavailable,
        })
    }

    /// Refresh every routable host concurrently, each bounded by
    /// [`HEALTH_DEADLINE`]. Returns the first failure for reporting; one slow or
    /// failing host never prevents others from being checked or used.
    pub(crate) async fn check_hosts(&self) -> Result<Option<ControllerError>, ControllerError> {
        let registered = self.store.registered_hosts(None).await?;
        let addresses: Vec<HostAddress> = registered
            .iter()
            .filter_map(|r| self.address_of(r))
            .collect();
        {
            let mut state = self.hosts.lock();
            state.retain(|host, _| addresses.iter().any(|a| a.host == *host));
        }
        for host in registered.iter().map(|r| r.host_id) {
            if !addresses.iter().any(|a| a.host == host) {
                self.hosts.pool.invalidate(host);
            }
        }
        let results: Vec<_> = stream::iter(addresses)
            .map(|address| self.check_host(address))
            .buffer_unordered(HEALTH_CONCURRENCY)
            .collect()
            .await;
        Ok(results.into_iter().find_map(Result::err))
    }

    /// Authenticate one host incarnation and refresh its observation.
    pub(crate) async fn check_host(&self, address: HostAddress) -> Result<(), ControllerError> {
        let authority = {
            let mut state = self.hosts.lock();
            let entry = state
                .entry(address.host)
                .or_insert_with(|| HostState::new(address.clone()));
            if entry.address != address {
                *entry = HostState::new(address.clone());
            }
            if entry.checking {
                // A sibling worker is checking; its result governs routing.
                return Ok(());
            }
            entry.checking = true;
            entry.authority.clone()
        };
        let checking = Checking {
            hosts: &self.hosts,
            address: address.clone(),
        };
        // Pool admission is not a network attempt. In particular, polling
        // during backoff must not extend the retry deadline or erase health
        // merely because all request slots are occupied.
        let lease = match self.hosts.pool.acquire(&address, false) {
            Ok(lease) => lease,
            Err(error) => return Err(error.into()),
        };
        let result = tokio::time::timeout(HEALTH_DEADLINE, self.health(&address, authority, lease))
            .await
            .unwrap_or(Err(ControllerError::Health));
        {
            let mut state = self.hosts.lock();
            if let Some(entry) = state.get_mut(&address.host)
                && entry.address == address
            {
                match &result {
                    Ok((permits, compatible)) => {
                        if entry.new_work_compatible && !compatible {
                            eprintln!(
                                "Host {} compatibility changed: new work denied; retained work still requires reconciliation and explicit cleanup.",
                                address.host
                            );
                        }
                        entry.healthy = true;
                        entry.launch_permits_required = *permits;
                        entry.new_work_compatible = *compatible;
                    }
                    Err(_) => entry.healthy = false,
                }
            }
        }
        drop(checking);
        match result {
            Ok(_) => {
                self.hosts.pool.report_success(&address);
                Ok(())
            }
            Err(error) => {
                self.hosts.pool.report_failure(&address);
                self.store
                    .mark_host_runtime_unknown(address.host, address.epoch)
                    .await?;
                Err(error)
            }
        }
    }

    async fn health(
        &self,
        address: &HostAddress,
        authority: Arc<tokio::sync::Mutex<()>>,
        lease: Lease,
    ) -> Result<(bool, bool), ControllerError> {
        let mut client = client(&lease);
        let health = client
            .health(HealthRequest {})
            .await
            .map_err(|_| ControllerError::Health)?
            .into_inner();
        if health.host_id != address.host.to_string()
            || health.supervisor_epoch != address.epoch
            || (health.simulated && !self.config.allow_simulated)
        {
            return Err(ControllerError::HostIdentity);
        }
        self.sync_allocation_authority(
            address.host,
            address.epoch,
            health.launch_permits_required,
            &mut client,
            &authority,
        )
        .await?;
        // This only touches the existing operator-provisioned epoch. Registration
        // and epoch issuance remain separate; the RPC cannot insert arbitrary hosts.
        let observed = self
            .store
            .observe_configured_host_with_capabilities(
                address.host,
                address.epoch,
                &health.capabilities_json,
                health.simulated,
            )
            .await;
        let compatible = match observed {
            Ok(()) => true,
            Err(DispatchError::HostCompatibility) => {
                self.store
                    .invalidate_host_capability_observation(address.host, address.epoch)
                    .await?;
                false
            }
            Err(error) => return Err(error.into()),
        };
        Ok((health.launch_permits_required, compatible))
    }

    pub(crate) async fn route_permit(
        &self,
        route: &mut Route,
        claim: &Claim,
        allocation: &sandbox_store::placement::Allocation,
    ) -> Result<Vec<u8>, ControllerError> {
        if !route.launch_permits_required {
            return Ok(Vec::new());
        }
        let authority = route.authority.clone();
        self.prepare_launch_permit(claim, allocation, &mut route.client, &authority)
            .await
    }
}
