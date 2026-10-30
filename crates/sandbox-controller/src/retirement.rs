//! Physical allocation retirement, independent of operation dispatch. Database
//! preparation and retained completion, never a missing host record, authorize
//! each stage of the handoff.
use crate::{Controller, ControllerConfig};
use sandbox_protocol::{
    HostId,
    supervisor::{AllocationForgetRequest, AllocationMetadataRequest},
};
use sandbox_store::{Store, allocation_retirement::Candidate};
use std::time::Duration;

#[derive(Debug)]
pub struct Retirer {
    store: Store,
    config: ControllerConfig,
    hosts: Controller,
    host_cursor: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetirementTick {
    Idle,
    Deferred,
    Completed,
}

#[derive(Debug, thiserror::Error)]
pub enum RetirementError {
    #[error("allocation retirement storage failed: {0}")]
    Store(#[from] sandbox_store::allocation_retirement::Error),
    #[error("allocation retirement RPC failed; retained claim requires reconciliation: {0}")]
    Rpc(#[source] tonic::Status),
    #[error("allocation retirement request encoding failed")]
    Encoding,
    #[error("allocation owner host is not currently routable")]
    Unroutable,
}

impl Controller {
    pub fn allocation_retirer(&self) -> Retirer {
        Retirer {
            store: self.store.clone(),
            config: self.config.clone(),
            hosts: self.worker(0),
            host_cursor: 0,
        }
    }
}

impl Retirer {
    pub async fn tick(&mut self) -> Result<RetirementTick, RetirementError> {
        // Fake/legacy hosts provide no physical forgetting authority.
        if self.config.allow_simulated {
            return Ok(RetirementTick::Idle);
        }
        let mut healthy = self.hosts.hosts.healthy();
        if !healthy.is_empty() {
            let first = self.host_cursor % healthy.len();
            healthy.rotate_left(first);
        }
        self.host_cursor = self.host_cursor.wrapping_add(1);
        for (host, epoch) in healthy {
            if let Some(candidate) = self.store.next_allocation_retirement(host, epoch).await? {
                return self.retire(host, epoch, candidate).await;
            }
        }
        Ok(RetirementTick::Idle)
    }

    async fn retire(
        &mut self,
        host: HostId,
        epoch: i64,
        candidate: Candidate,
    ) -> Result<RetirementTick, RetirementError> {
        match self.process(host, epoch, candidate).await {
            Err(RetirementError::Store(
                sandbox_store::allocation_retirement::Error::Ineligible,
            )) => Ok(RetirementTick::Deferred),
            result => result,
        }
    }

    async fn process(
        &mut self,
        host: HostId,
        epoch: i64,
        candidate: Candidate,
    ) -> Result<RetirementTick, RetirementError> {
        let mut route = self
            .hosts
            .route(host, epoch, true)
            .await
            .map_err(|_| RetirementError::Unroutable)?;
        let mut metadata = None;
        if let Some(intent) = &candidate.intent {
            if self
                .store
                .allocation_forgetting_completion(intent)
                .await?
                .is_some()
            {
                return Ok(RetirementTick::Completed);
            }
            metadata = self.store.allocation_retirement_completion(intent).await?;
        }
        let metadata = if let Some(metadata) = metadata {
            metadata
        } else {
            let Some(request) = self
                .store
                .prepare_allocation_retirement(candidate.allocation, host, epoch, 120, false)
                .await?
            else {
                return Ok(RetirementTick::Deferred);
            };
            let observed = tokio::time::timeout(
                Duration::from_secs(30),
                route
                    .client
                    .retire_allocation_metadata(AllocationMetadataRequest {
                        request_json: request.encode().map_err(|_| RetirementError::Encoding)?,
                    }),
            )
            .await
            .map_err(|_| {
                RetirementError::Rpc(tonic::Status::deadline_exceeded(
                    "allocation metadata retirement timed out",
                ))
            })?
            .map_err(RetirementError::Rpc)?
            .into_inner();
            match self
                .store
                .complete_allocation_retirement(&request, &observed)
                .await
            {
                Ok(completion) => completion,
                Err(error) => {
                    // An uncertain database acknowledgement must be resolved by
                    // retained proof before another host request is considered.
                    match self
                        .store
                        .allocation_retirement_completion(&request.intent)
                        .await?
                    {
                        Some(completion) => completion,
                        None => return Err(error.into()),
                    }
                }
            }
        };
        if self
            .store
            .allocation_forgetting_completion(&metadata.request.intent)
            .await?
            .is_some()
        {
            return Ok(RetirementTick::Completed);
        }
        let Some(request) = self
            .store
            .prepare_allocation_forgetting(candidate.allocation, host, epoch, 120)
            .await?
        else {
            return Ok(RetirementTick::Deferred);
        };
        let observed = tokio::time::timeout(
            Duration::from_secs(30),
            route.client.forget_allocation(AllocationForgetRequest {
                request_json: request.encode().map_err(|_| RetirementError::Encoding)?,
            }),
        )
        .await
        .map_err(|_| {
            RetirementError::Rpc(tonic::Status::deadline_exceeded(
                "allocation forgetting timed out",
            ))
        })?
        .map_err(RetirementError::Rpc)?
        .into_inner();
        if let Err(error) = self
            .store
            .complete_allocation_forgetting(&request, &observed)
            .await
            && self
                .store
                .allocation_forgetting_completion(&request.claim.intent)
                .await?
                .is_none()
        {
            return Err(error.into());
        }
        Ok(RetirementTick::Completed)
    }
}
