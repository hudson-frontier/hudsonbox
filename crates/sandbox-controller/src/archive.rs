//! Independent output worker. No output bytes, object credentials, or execution
//! dispatch calls belong here. Run separately from lifecycle maintenance.
use crate::Controller;
use sandbox_protocol::{
    HostId,
    output::{OutputPlans, OutputRefs},
    supervisor::{OutputObservation, OutputRequest},
};
use sandbox_store::{
    Store,
    output::{OutputClaim, OutputError},
};
use sandbox_supervisor::archive::{MAX_METADATA, encode};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[derive(Debug)]
pub struct Archiver {
    store: Store,
    config: crate::ControllerConfig,
    hosts: Controller,
    retention: u32,
    grace: u32,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArchiveTick {
    Idle,
    Published,
    Expired,
}
#[derive(Debug, thiserror::Error)]
pub enum ArchiveError {
    #[error("output publication storage failed")]
    Store(#[from] OutputError),
    #[error("output RPC failed; retry the persisted publication plan")]
    Rpc,
    #[error("output supervisor evidence does not match publication ownership")]
    Evidence,
}
fn now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|d| i64::try_from(d.as_millis()).ok())
        .unwrap_or(i64::MAX)
}
impl Controller {
    pub fn output_archiver(&self, retention: u32, grace: u32) -> Result<Archiver, ArchiveError> {
        if !(1..=30 * 24 * 3600).contains(&retention) || grace > 7 * 24 * 3600 {
            return Err(OutputError::InvalidPolicy.into());
        }
        Ok(Archiver {
            store: self.store.clone(),
            config: self.config.clone(),
            hosts: self.worker(0),
            retention,
            grace,
        })
    }
}
impl Archiver {
    pub async fn tick(&mut self) -> Result<ArchiveTick, ArchiveError> {
        let Some(claim) = self.store.claim_output(120).await? else {
            return Ok(ArchiveTick::Idle);
        };
        let result = self.process(&claim).await;
        if result.is_err() {
            let _ = self.store.defer_output(&claim, 5).await;
        }
        result
    }
    fn validate(
        &self,
        (host, epoch): (HostId, i64),
        request: &OutputRequest,
        response: &OutputObservation,
    ) -> Result<OutputPlans, ArchiveError> {
        if response.request.as_ref() != Some(request)
            || response.host_id != host.to_string()
            || response.supervisor_epoch != epoch
            || (response.simulated && !self.config.allow_simulated)
            || now().abs_diff(response.observed_unix_ms) > 10_000
            || response.plans_json.len() > MAX_METADATA
            || response.references_json.len() > MAX_METADATA
        {
            return Err(ArchiveError::Evidence);
        }
        serde_json::from_slice(&response.plans_json).map_err(|_| ArchiveError::Evidence)
    }
    async fn process(&mut self, claim: &OutputClaim) -> Result<ArchiveTick, ArchiveError> {
        let mut work = match self
            .store
            .prepare_output(
                claim,
                self.retention,
                self.grace,
                self.config.allow_simulated,
            )
            .await
        {
            Ok(w) => w,
            Err(OutputError::Expired) => return Ok(ArchiveTick::Expired),
            Err(e) => return Err(e.into()),
        };
        // Only the ticket's exact owner incarnation can publish its output.
        let owner = (work.ticket.owner.host_id, work.ticket.owner.host_epoch);
        let mut route = self
            .hosts
            .route(owner.0, owner.1, true)
            .await
            .map_err(|_| ArchiveError::Rpc)?;
        let mut request = OutputRequest {
            ticket_json: encode(&work.ticket).map_err(|_| ArchiveError::Evidence)?,
            publication_revision: claim.revision,
            claim_expires_unix_ms: i64::try_from(
                claim.lease_expires_at.unix_timestamp_nanos() / 1_000_000,
            )
            .map_err(|_| ArchiveError::Evidence)?,
            plans_json: vec![],
        };
        if work.plans.is_none() {
            let response = tokio::time::timeout(
                Duration::from_secs(30),
                self.hosts.rpc(
                    owner.0,
                    owner.1,
                    route.client.prepare_output(request.clone()),
                ),
            )
            .await
            .map_err(|_| ArchiveError::Rpc)?
            .map_err(|_| ArchiveError::Rpc)?
            .into_inner();
            let plans = self.validate(owner, &request, &response)?;
            if !response.references_json.is_empty() || response.simulated != work.simulated {
                return Err(ArchiveError::Evidence);
            }
            self.store
                .save_output_plans(claim, &plans, self.config.allow_simulated)
                .await?;
            work.plans = Some(plans);
        }
        let plans = work.plans.ok_or(ArchiveError::Evidence)?;
        request.plans_json = encode(&plans).map_err(|_| ArchiveError::Evidence)?;
        let response = tokio::time::timeout(
            Duration::from_secs(80),
            self.hosts.rpc(
                owner.0,
                owner.1,
                route.client.archive_output(request.clone()),
            ),
        )
        .await
        .map_err(|_| ArchiveError::Rpc)?
        .map_err(|_| ArchiveError::Rpc)?
        .into_inner();
        let observed = self.validate(owner, &request, &response)?;
        if observed != plans || response.simulated != work.simulated {
            return Err(ArchiveError::Evidence);
        }
        let refs: OutputRefs = serde_json::from_slice(&response.references_json)
            .map_err(|_| ArchiveError::Evidence)?;
        self.store
            .publish_output(claim, &refs, self.config.allow_simulated)
            .await?;
        Ok(ArchiveTick::Published)
    }
}
