//! One configured host, durable create/destroy dispatch, and reconciliation over mTLS.
//! The supervisor is trusted only after its certificate, host ID, and epoch match.
mod allocation_authority;
pub mod archive;
pub mod history;
pub mod host_discovery;
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
    placement::{PlacementError, Reservation},
};
use sandbox_supervisor::transport::{self, TransportError};
use std::collections::BTreeSet;
use tonic::transport::Channel;

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
    new_work_compatible: bool,
    launch_permits_required: bool,
    store: Store,
    config: ControllerConfig,
    client: SupervisorClient<Channel>,
    archive_client: SupervisorClient<Channel>,
    operation_cursor: usize,
    authority_sync: std::sync::Arc<tokio::sync::Mutex<()>>,
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
        let client = transport::connect(&config.endpoint, config.host, ca, cert, key).await?;
        let archive_client =
            transport::connect_archiver(&config.endpoint, config.host, ca, cert, key).await?;
        let mut controller = Self {
            new_work_compatible: true,
            launch_permits_required: false,
            store,
            config,
            client,
            archive_client,
            operation_cursor: 0,
            authority_sync: std::sync::Arc::new(tokio::sync::Mutex::new(())),
            file_sources: None,
            file_cache: None,
        };
        controller.check_host().await?;
        Ok(controller)
    }

    async fn check_host(&mut self) -> Result<(), ControllerError> {
        let health = self
            .client
            .health(HealthRequest {})
            .await
            .map_err(|_| ControllerError::Health)?
            .into_inner();
        if health.host_id != self.config.host.to_string()
            || health.supervisor_epoch != self.config.epoch
            || (health.simulated && !self.config.allow_simulated)
        {
            return Err(ControllerError::HostIdentity);
        }
        self.launch_permits_required = health.launch_permits_required;
        self.sync_allocation_authority().await?;
        // This only touches the existing operator-provisioned epoch. Registration
        // and epoch issuance remain separate; the RPC cannot insert arbitrary hosts.
        let observed = self
            .store
            .observe_configured_host_with_capabilities(
                self.config.host,
                self.config.epoch,
                &health.capabilities_json,
                health.simulated,
            )
            .await;
        match observed {
            Ok(()) => self.new_work_compatible = true,
            Err(DispatchError::HostCompatibility) => {
                self.store
                    .invalidate_host_capability_observation(self.config.host, self.config.epoch)
                    .await?;
                if self.new_work_compatible {
                    eprintln!(
                        "Host compatibility changed: new work denied; retained work still requires reconciliation and explicit cleanup."
                    );
                }
                self.new_work_compatible = false;
            }
            Err(error) => return Err(error.into()),
        }
        Ok(())
    }

    /// Admission status only; true also includes unqualified legacy mode.
    /// Incompatible but authenticated hosts remain available for retained cleanup.
    pub fn new_work_compatible(&self) -> bool {
        self.new_work_compatible
    }

    async fn defer(&self, claim: &Claim) -> Result<Tick, ControllerError> {
        match self.store.defer_claim(claim, 5).await {
            Ok(()) => Ok(Tick::Deferred),
            Err(ClaimError::LostClaim) => Ok(Tick::LostOwnership),
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
        let action = match self.store.prepare_destroy(claim, false).await {
            Ok(action) => action,
            Err(DispatchError::LostClaim) => return Ok(Tick::LostOwnership),
            Err(DispatchError::Conflict) => return self.defer(claim).await,
            Err(error) => return Err(error.into()),
        };
        let owner = match &action {
            DestroyAction::Stop(owner) | DestroyAction::Inspect(owner) => owner,
        };
        if owner.host_id != self.config.host.to_string()
            || owner.supervisor_epoch != self.config.epoch
        {
            return self.defer(claim).await;
        }
        let response = match action {
            DestroyAction::Stop(owner) => {
                self.client
                    .stop(StopRequest {
                        ownership: Some(owner),
                    })
                    .await
            }
            DestroyAction::Inspect(owner) => {
                match self
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
                        self.client
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
    async fn maintenance_tick(&mut self) -> Result<Tick, ControllerError> {
        let Some(claim) = self
            .store
            .claim_allocation(self.config.host, self.config.epoch, 10)
            .await?
        else {
            return Ok(Tick::Idle);
        };
        let action = match self.store.prepare_lease(&claim).await {
            Ok(action) => action,
            Err(error) => return self.lease_error(&claim, error).await,
        };
        let response = match action {
            LeaseAction::Cleanup(_) => return Ok(Tick::Maintained),
            LeaseAction::Renew(request) => self.client.renew_lease(request).await,
            LeaseAction::Inspect(owner) => {
                self.client
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
        if let Err(error) = self.check_host().await {
            self.store
                .mark_host_runtime_unknown(self.config.host, self.config.epoch)
                .await?;
            return cancellation.map(Ok).unwrap_or(Err(error));
        }
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
        Ok(cancellation.unwrap_or(maintenance))
    }

    async fn create_tick(&mut self, claim: &Claim) -> Result<Tick, ControllerError> {
        if let Some(tick) = self.previous_tick(claim).await? {
            return Ok(tick);
        }
        let reservation = match self
            .store
            .reserve_create(claim, self.config.host, self.config.epoch)
            .await
        {
            Ok(value) => value,
            Err(PlacementError::Unauthorized) => {
                return self.reject(claim, CreateRejection::Unauthorized).await;
            }
            Err(PlacementError::InvalidResources) => {
                return self.reject(claim, CreateRejection::InvalidResources).await;
            }
            Err(
                PlacementError::Capacity
                | PlacementError::Quota
                | PlacementError::HostUnavailable
                | PlacementError::HostCompatibility
                | PlacementError::Reconcile,
            ) => return self.defer(claim).await,
            Err(PlacementError::LostClaim) => return Ok(Tick::LostOwnership),
            Err(error) => return Err(error.into()),
        };
        let allocation = match reservation {
            Reservation::Reserved(a) | Reservation::Existing(a) => a,
        };
        if allocation.host_id != self.config.host
            || allocation.supervisor_epoch != self.config.epoch
        {
            return self.defer(claim).await;
        }
        let launch_permit = if self.launch_permits_required {
            match self.prepare_launch_permit(claim, &allocation).await {
                Ok(permit) => permit,
                Err(_) => return self.defer(claim).await,
            }
        } else {
            Vec::new()
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
                request.launch_permit_json = launch_permit;
                self.client.create(request).await
            }
            CreateAction::Inspect(owner) => {
                self.client
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
        let action = match self
            .store
            .prepare_execute(claim, self.config.host, self.config.epoch)
            .await
        {
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
        if owner.host_id != self.config.host.to_string()
            || owner.supervisor_epoch != self.config.epoch
        {
            return self.execute_unknown(claim).await;
        }
        let response = match action {
            ExecuteAction::Dispatch { owner, command } => {
                self.client
                    .execute_command(CommandRequest {
                        ownership: Some(owner),
                        command: Some((&command).into()),
                    })
                    .await
            }
            ExecuteAction::Inspect { owner, digest } => {
                self.client
                    .inspect_command(CommandInspection {
                        ownership: Some(owner),
                        command_digest: digest.to_vec(),
                    })
                    .await
            }
            ExecuteAction::Cancel { owner, digest } => {
                self.client
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
