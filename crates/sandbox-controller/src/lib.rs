//! Durable create/destroy dispatch and reconciliation over per-host mTLS.
//! Each request goes to its allocation's persisted owner host and epoch; a
//! supervisor is trusted only after its certificate, host ID, and epoch match.
mod allocation_authority;
pub mod archive;
pub mod history;
pub mod host_discovery;
mod hosts;
mod recovery;
pub mod retirement;
mod uploads;
pub mod wakeup;
pub mod workers;

use sandbox_protocol::{
    HostId,
    supervisor::{
        AllocationState, HealthRequest, InspectRequest, LeaseInspection, StopRequest,
        supervisor_client::SupervisorClient,
    },
};
use sandbox_store::{
    Store,
    claims::{Claim, ClaimError, OperationKind},
    destroy::DestroyAction,
    dispatch::{CreateAction, CreateRejection, DispatchError},
    leases::{AllocationClaim, LeaseAction, LeaseResult},
    placement::{HostScope, PlacementError, PlacementWait, Reservation, WaitOutcome},
};
use sandbox_supervisor::{
    routing::RouteError,
    transport::{self, TransportError},
};
use std::{collections::BTreeSet, sync::Arc};
use tonic::transport::Channel;

pub use hosts::{HEALTH_CONCURRENCY, HEALTH_DEADLINE};

/// `endpoint`, `host` and `epoch` name the configured host: the placement target
/// for legacy creates and the legacy address of that host while its registry
/// transport is NULL. Registered hosts are addressed only by their registration.
#[derive(Debug, Clone)]
pub struct ControllerConfig {
    pub endpoint: String,
    pub host: HostId,
    pub epoch: i64,
    pub allowed_images: BTreeSet<String>,
    pub allow_simulated: bool,
}

#[derive(Debug, thiserror::Error)]
pub enum ControllerError {
    #[error(transparent)]
    AllocationAuthority(#[from] sandbox_store::allocation_permits::Error),
    #[error("configure a positive host epoch and nonempty immutable image allowlist")]
    InvalidConfig,
    #[error("configure between 1 and 16 controller workers")]
    InvalidWorkerCount,
    #[error("supervisor health response does not match the configured host or evidence policy")]
    HostIdentity,
    #[error("supervisor health request failed")]
    Health,
    #[error(transparent)]
    Transport(#[from] TransportError),
    #[error(transparent)]
    Route(#[from] RouteError),
    #[error("owner host is unregistered, unaddressed, unhealthy or at another epoch")]
    Unroutable,
    #[error(transparent)]
    Registry(#[from] sandbox_store::host_registry::RegistryError),
    #[error(transparent)]
    Store(#[from] sandbox_store::StoreError),
    #[error(transparent)]
    Claim(#[from] ClaimError),
    #[error(transparent)]
    Placement(#[from] PlacementError),
    #[error(transparent)]
    Dispatch(#[from] DispatchError),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Tick {
    Idle,
    Deferred,
    Rejected,
    Confirmed,
    Unknown,
    LostOwnership,
    Maintained,
}

#[derive(Debug)]
pub struct Controller {
    store: Store,
    config: ControllerConfig,
    hosts: Arc<hosts::Hosts>,
    operation_cursor: usize,
    host_cursor: usize,
    file_sources: Option<std::sync::Arc<dyn sandbox_artifacts::sources::SourceBackend>>,
    file_cache: uploads::FileCache,
}

impl Controller {
    pub async fn connect(
        store: Store,
        config: ControllerConfig,
        ca: &[u8],
        cert: &[u8],
        key: &[u8],
    ) -> Result<Self, ControllerError> {
        if config.epoch <= 0
            || config.allowed_images.is_empty()
            || config
                .allowed_images
                .iter()
                .any(|digest| !sandbox_protocol::images::valid_image_digest(digest))
        {
            return Err(ControllerError::InvalidConfig);
        }
        let hosts = Arc::new(hosts::Hosts::new(ca, cert, key)?);
        let controller = Self {
            store,
            config,
            hosts,
            operation_cursor: 0,
            host_cursor: 0,
            file_sources: None,
            file_cache: None,
        };
        // The configured host must authenticate at startup, as in single-host mode.
        let address = controller
            .resolve(controller.config.host)
            .await?
            .ok_or(ControllerError::InvalidConfig)?;
        controller.check_host(address).await?;
        Ok(controller)
    }

    /// A worker sharing this controller's connections and host observations.
    pub(crate) fn worker(&self, operation_cursor: usize) -> Self {
        Self {
            store: self.store.clone(),
            config: self.config.clone(),
            hosts: self.hosts.clone(),
            operation_cursor,
            host_cursor: operation_cursor,
            file_sources: self.file_sources.clone(),
            file_cache: None,
        }
    }

    /// Admission status only; true also includes unqualified legacy mode.
    /// Incompatible but authenticated hosts remain available for retained cleanup.
    /// Reports the configured host.
    pub fn new_work_compatible(&self) -> bool {
        self.hosts.new_work_compatible(self.config.host)
    }

    async fn defer(&self, claim: &Claim) -> Result<Tick, ControllerError> {
        match self.store.defer_claim(claim, 5).await {
            Ok(()) => Ok(Tick::Deferred),
            Err(ClaimError::LostClaim) => Ok(Tick::LostOwnership),
            Err(error) => Err(error.into()),
        }
    }

    /// Bounded wait for placement; fails the create once the bound passes.
    async fn await_placement(
        &self,
        claim: &Claim,
        reason: PlacementWait,
    ) -> Result<Tick, ControllerError> {
        match self.store.await_placement(claim, reason).await {
            Ok(WaitOutcome::Waiting) => Ok(Tick::Deferred),
            Ok(WaitOutcome::Expired) => self.reject(claim, reason.rejection()).await,
            Err(PlacementError::LostClaim) => Ok(Tick::LostOwnership),
            Err(error) => Err(error.into()),
        }
    }

    async fn reject(
        &self,
        claim: &Claim,
        reason: CreateRejection,
    ) -> Result<Tick, ControllerError> {
        match self.store.reject_undispatched_create(claim, reason).await {
            Ok(()) => Ok(Tick::Rejected),
            Err(DispatchError::Conflict) => self.defer(claim).await,
            Err(DispatchError::LostClaim) => Ok(Tick::LostOwnership),
            Err(error) => Err(error.into()),
        }
    }

    async fn unknown(&self, claim: &Claim) -> Result<Tick, ControllerError> {
        match self.store.record_create_unknown(claim).await {
            Ok(()) => Ok(Tick::Unknown),
            Err(DispatchError::LostClaim) => Ok(Tick::LostOwnership),
            Err(DispatchError::Conflict) => self.defer(claim).await,
            Err(error) => Err(error.into()),
        }
    }

    async fn destroy_unknown(&self, claim: &Claim) -> Result<Tick, ControllerError> {
        match self.store.record_destroy_unknown(claim).await {
            Ok(()) => Ok(Tick::Unknown),
            Err(DispatchError::LostClaim) => Ok(Tick::LostOwnership),
            Err(DispatchError::Conflict) => self.defer(claim).await,
            Err(error) => Err(error.into()),
        }
    }

    async fn destroy_tick(&mut self, claim: &Claim) -> Result<Tick, ControllerError> {
        if let Some(tick) = self.previous_tick(claim).await? {
            return Ok(tick);
        }
        // Resolve the owner before committing another stop attempt.
        let mut route = match self.owner(claim).await? {
            hosts::Owner::Ready(route) => route,
            hosts::Owner::Unavailable | hosts::Owner::None => {
                return self.owner_unavailable(claim).await;
            }
        };
        let action = match self.store.prepare_destroy(claim, false).await {
            Ok(action) => action,
            Err(DispatchError::LostClaim) => return Ok(Tick::LostOwnership),
            Err(DispatchError::Conflict) => return self.defer(claim).await,
            Err(error) => return Err(error.into()),
        };
        let owner = match &action {
            DestroyAction::Stop(owner) | DestroyAction::Inspect(owner) => owner,
        };
        if !route.owns(owner) {
            return self.defer(claim).await;
        }
        let response = match action {
            DestroyAction::Stop(owner) => {
                route
                    .client
                    .stop(StopRequest {
                        ownership: Some(owner),
                    })
                    .await
            }
            DestroyAction::Inspect(owner) => {
                match route
                    .client
                    .inspect(InspectRequest {
                        ownership: Some(owner.clone()),
                    })
                    .await
                {
                    Ok(response)
                        if response.get_ref().ownership.as_ref() == Some(&owner)
                            && matches!(
                                AllocationState::try_from(response.get_ref().state),
                                Ok(AllocationState::Ready | AllocationState::Absent)
                            ) =>
                    {
                        // Stop is an idempotent fence for this exact incarnation,
                        // but each transport attempt still commits intent first.
                        let next = match self.store.prepare_destroy(claim, true).await {
                            Ok(next) => next,
                            Err(DispatchError::LostClaim) => return Ok(Tick::LostOwnership),
                            Err(DispatchError::Conflict) => return self.defer(claim).await,
                            Err(error) => return Err(error.into()),
                        };
                        let DestroyAction::Stop(owner) = next else {
                            return self.destroy_unknown(claim).await;
                        };
                        if !route.owns(&owner) {
                            return self.destroy_unknown(claim).await;
                        }
                        route
                            .client
                            .stop(StopRequest {
                                ownership: Some(owner),
                            })
                            .await
                    }
                    other => other,
                }
            }
        };
        let observation = match response {
            Ok(response) => response.into_inner(),
            Err(_) => return self.destroy_unknown(claim).await,
        };
        match self
            .store
            .record_destroy_observation(claim, &observation, self.config.allow_simulated)
            .await
        {
            Ok(()) => Ok(Tick::Confirmed),
            Err(DispatchError::LostClaim) => Ok(Tick::LostOwnership),
            Err(DispatchError::BadEvidence | DispatchError::SimulationDenied) => {
                self.destroy_unknown(claim).await
            }
            Err(DispatchError::Conflict) => self.defer(claim).await,
            Err(error) => Err(error.into()),
        }
    }

    async fn lease_error(
        &self,
        claim: &AllocationClaim,
        error: DispatchError,
    ) -> Result<Tick, ControllerError> {
        match error {
            DispatchError::LostClaim => Ok(Tick::LostOwnership),
            DispatchError::Conflict => match self.store.defer_allocation(claim).await {
                Ok(()) => Ok(Tick::Deferred),
                Err(DispatchError::LostClaim) => Ok(Tick::LostOwnership),
                Err(error) => Err(error.into()),
            },
            other => Err(other.into()),
        }
    }

    async fn lease_unknown(&self, claim: &AllocationClaim) -> Result<Tick, ControllerError> {
        match self.store.record_lease_unknown(claim).await {
            Ok(()) => Ok(Tick::Unknown),
            Err(error) => self.lease_error(claim, error).await,
        }
    }

    /// One due allocation per tick, independently of operation queue pressure.
    /// Healthy hosts take turns so one host's backlog cannot starve another's.
    async fn maintenance_tick(&mut self) -> Result<Tick, ControllerError> {
        let hosts = self.hosts.healthy();
        let first = self.host_cursor;
        self.host_cursor = self.host_cursor.wrapping_add(1);
        for offset in 0..hosts.len() {
            let (host, epoch) = hosts[(first + offset) % hosts.len()];
            if let Some(claim) = self.store.claim_allocation(host, epoch, 10).await? {
                return self.maintain(claim, host, epoch).await;
            }
        }
        Ok(Tick::Idle)
    }

    async fn maintain(
        &mut self,
        claim: AllocationClaim,
        host: HostId,
        epoch: i64,
    ) -> Result<Tick, ControllerError> {
        let mut route = match self.route(host, epoch, false).await {
            Ok(route) => route,
            Err(_) => return self.lease_error(&claim, DispatchError::Conflict).await,
        };
        let action = match self.store.prepare_lease(&claim).await {
            Ok(action) => action,
            Err(error) => return self.lease_error(&claim, error).await,
        };
        let response = match action {
            LeaseAction::Cleanup(_) => return Ok(Tick::Maintained),
            LeaseAction::Renew(request) => route.client.renew_lease(request).await,
            LeaseAction::Inspect(owner) => {
                route
                    .client
                    .inspect_lease(LeaseInspection {
                        ownership: Some(owner),
                    })
                    .await
            }
        };
        let observation = match response {
            Ok(response) => response.into_inner(),
            Err(_) => return self.lease_unknown(&claim).await,
        };
        match self
            .store
            .record_lease_observation(&claim, &observation, self.config.allow_simulated)
            .await
        {
            Ok(LeaseResult::Renewed | LeaseResult::Cleanup(_)) => Ok(Tick::Maintained),
            Err(DispatchError::BadEvidence | DispatchError::SimulationDenied) => {
                self.lease_unknown(&claim).await
            }
            Err(error) => self.lease_error(&claim, error).await,
        }
    }

    /// Reconcile one cancellation with a two-second database bound, then
    /// maintain at most one due allocation and process at most one operation.
    /// Rotating preference prevents create/destroy/execute queues starving each other.
    pub async fn tick(&mut self) -> Result<Tick, ControllerError> {
        self.store.reconcile_unavailable_hosts().await?;
        let cancellation = match tokio::time::timeout(std::time::Duration::from_secs(2), async {
            let Some(claim) = self.store.claim_next(OperationKind::Cancel, 30).await? else {
                return Ok(None);
            };
            match self.store.reconcile_cancel(&claim).await {
                Ok(sandbox_store::cancel::CancelProgress::Completed) => Ok(Some(Tick::Confirmed)),
                Ok(sandbox_store::cancel::CancelProgress::Pending) => Ok(Some(Tick::Deferred)),
                Err(DispatchError::LostClaim) => Ok(Some(Tick::LostOwnership)),
                Err(error) => Err(ControllerError::from(error)),
            }
        })
        .await
        {
            Ok(result) => result?,
            // Cancelling the future rolls back any uncommitted target changes.
            // A previously committed claim expires normally; lifecycle continues.
            Err(_) => Some(Tick::Deferred),
        };
        // Failed hosts are marked unknown and excluded; the rest still progress.
        let unhealthy = self.check_hosts().await?;
        let maintenance = self.maintenance_tick().await?;
        let kinds = [
            OperationKind::Destroy,
            OperationKind::Create,
            OperationKind::Execute,
            OperationKind::FileWrite,
        ];
        let first = self.operation_cursor;
        self.operation_cursor = (first + 1) % kinds.len();
        for offset in 0..kinds.len() {
            let kind = kinds[(first + offset) % kinds.len()];
            if matches!(kind, OperationKind::FileWrite) && self.file_sources.is_none() {
                continue;
            }
            if let Some(claim) = self.store.claim_next(kind, 30).await? {
                return match kind {
                    OperationKind::Destroy => self.destroy_tick(&claim).await,
                    OperationKind::Execute => self.execute_tick(&claim).await,
                    OperationKind::FileWrite => self.upload_tick(&claim).await,
                    _ => self.create_tick(&claim).await,
                };
            }
        }
        match (cancellation, maintenance, unhealthy) {
            (Some(tick), _, _) => Ok(tick),
            // Report a host failure only when no other work progressed.
            (None, Tick::Idle, Some(error)) => Err(error),
            (None, tick, _) => Ok(tick),
        }
    }

    async fn create_tick(&mut self, claim: &Claim) -> Result<Tick, ControllerError> {
        if let Some(tick) = self.previous_tick(claim).await? {
            return Ok(tick);
        }
        // A registered bootstrap host opts into fleet placement. NULL transport
        // keeps the explicit configured legacy/fake scope; never fall back from
        // an unavailable fleet to that host.
        let scope = match self.store.registered_host(self.config.host).await? {
            Some(host) if host.transport.is_some() => HostScope::Fleet,
            _ => HostScope::Configured {
                host: self.config.host,
                epoch: self.config.epoch,
            },
        };
        let reservation = match self.store.place_create(claim, scope).await {
            Ok(value) => value,
            Err(PlacementError::Unauthorized) => {
                return self.reject(claim, CreateRejection::Unauthorized).await;
            }
            Err(PlacementError::InvalidResources) => {
                return self.reject(claim, CreateRejection::InvalidResources).await;
            }
            Err(PlacementError::NoPoolAccess) => {
                return self
                    .reject(claim, CreateRejection::HostPoolAccessDenied)
                    .await;
            }
            Err(PlacementError::Reconcile) => return self.defer(claim).await,
            Err(PlacementError::LostClaim) => return Ok(Tick::LostOwnership),
            Err(error) => match error.wait_reason() {
                Some(reason) => return self.await_placement(claim, reason).await,
                None => return Err(error.into()),
            },
        };
        let allocation = match reservation {
            Reservation::Reserved(a) | Reservation::Existing(a) => a,
        };
        // Route to the reservation's persisted owner, including a retried
        // reservation made earlier on another host, before committing intent.
        let mut route = match self
            .route(allocation.host_id, allocation.supervisor_epoch, false)
            .await
        {
            Ok(route) => route,
            Err(_) => return self.owner_unavailable(claim).await,
        };
        let launch_permit = match self.route_permit(&mut route, claim, &allocation).await {
            Ok(permit) => permit,
            Err(_) => return self.defer(claim).await,
        };
        let action = match self
            .store
            .prepare_create_dispatch(claim, &self.config.allowed_images)
            .await
        {
            Ok(action) => action,
            Err(DispatchError::Unauthorized) => {
                return self.reject(claim, CreateRejection::Unauthorized).await;
            }
            Err(DispatchError::ImageDenied) => {
                return self.reject(claim, CreateRejection::ImageDenied).await;
            }
            Err(DispatchError::InvalidResources) => {
                return self.reject(claim, CreateRejection::InvalidResources).await;
            }
            Err(
                DispatchError::HostUnavailable
                | DispatchError::HostCompatibility
                | DispatchError::Conflict,
            ) => {
                return self.defer(claim).await;
            }
            Err(DispatchError::LostClaim) => return Ok(Tick::LostOwnership),
            Err(error) => return Err(error.into()),
        };
        let response = match action {
            CreateAction::Start(mut request) => {
                if !request.ownership.as_ref().is_some_and(|o| route.owns(o)) {
                    return self.unknown(claim).await;
                }
                request.launch_permit_json = launch_permit;
                route.client.create(request).await
            }
            CreateAction::Inspect(owner) => {
                if !route.owns(&owner) {
                    return self.unknown(claim).await;
                }
                route
                    .client
                    .inspect(InspectRequest {
                        ownership: Some(owner),
                    })
                    .await
            }
        };
        let observation = match response {
            Ok(response) => response.into_inner(),
            Err(_) => return self.unknown(claim).await,
        };
        match self
            .store
            .record_create_observation(claim, &observation, self.config.allow_simulated)
            .await
        {
            Ok(()) => Ok(Tick::Confirmed),
            Err(DispatchError::BadEvidence | DispatchError::SimulationDenied) => {
                self.unknown(claim).await
            }
            Err(DispatchError::LostClaim) => Ok(Tick::LostOwnership),
            Err(DispatchError::Conflict) => self.defer(claim).await,
            Err(error) => Err(error.into()),
        }
    }
}

impl Controller {
    async fn execute_unknown(&self, claim: &Claim) -> Result<Tick, ControllerError> {
        match self.store.record_execute_unknown(claim).await {
            Ok(()) => Ok(Tick::Unknown),
            Err(DispatchError::LostClaim) => Ok(Tick::LostOwnership),
            Err(DispatchError::Conflict) => self.defer(claim).await,
            Err(e) => Err(e.into()),
        }
    }
    async fn execute_tick(&mut self, claim: &Claim) -> Result<Tick, ControllerError> {
        use sandbox_protocol::supervisor::{CommandInspection, CommandRequest};
        use sandbox_store::execute::ExecuteAction;
        // An owned sandbox is routed before any dispatch intent commits. Without
        // an owner, only database-side outcomes (rejection, cancellation) apply.
        let mut route = match self.owner(claim).await? {
            hosts::Owner::Ready(route) => Some(route),
            hosts::Owner::Unavailable => match self.store.fence_unavailable_execute(claim).await {
                Ok(()) => return self.owner_unavailable(claim).await,
                Err(DispatchError::LostClaim) => return Ok(Tick::LostOwnership),
                Err(error) => return Err(error.into()),
            },
            hosts::Owner::None => None,
        };
        let (host, epoch) = route
            .as_ref()
            .map_or((self.config.host, self.config.epoch), |r| (r.host, r.epoch));
        let action = match self.store.prepare_execute(claim, host, epoch).await {
            Ok(a) => a,
            Err(DispatchError::LostClaim) => return Ok(Tick::LostOwnership),
            Err(DispatchError::Conflict | DispatchError::HostUnavailable) => {
                return self.defer(claim).await;
            }
            Err(e) => return Err(e.into()),
        };
        let owner = match &action {
            ExecuteAction::Rejected => return Ok(Tick::Rejected),
            ExecuteAction::Dispatch { owner, .. }
            | ExecuteAction::Inspect { owner, .. }
            | ExecuteAction::Cancel { owner, .. } => owner,
        };
        let Some(route) = route.as_mut().filter(|r| r.owns(owner)) else {
            return self.execute_unknown(claim).await;
        };
        let response = match action {
            ExecuteAction::Dispatch { owner, command } => {
                route
                    .client
                    .execute_command(CommandRequest {
                        ownership: Some(owner),
                        command: Some((&command).into()),
                    })
                    .await
            }
            ExecuteAction::Inspect { owner, digest } => {
                route
                    .client
                    .inspect_command(CommandInspection {
                        ownership: Some(owner),
                        command_digest: digest.to_vec(),
                    })
                    .await
            }
            ExecuteAction::Cancel { owner, digest } => {
                route
                    .client
                    .cancel_command(CommandInspection {
                        ownership: Some(owner),
                        command_digest: digest.to_vec(),
                    })
                    .await
            }
            ExecuteAction::Rejected => return Ok(Tick::Rejected),
        };
        let observation = match response {
            Ok(r) => r.into_inner(),
            Err(_) => return self.execute_unknown(claim).await,
        };
        match self
            .store
            .record_execute_observation(claim, &observation, self.config.allow_simulated)
            .await
        {
            Ok(()) => Ok(Tick::Confirmed),
            Err(DispatchError::LostClaim) => Ok(Tick::LostOwnership),
            Err(DispatchError::BadEvidence | DispatchError::SimulationDenied) => {
                self.execute_unknown(claim).await
            }
            Err(e) => Err(e.into()),
        }
    }
}
