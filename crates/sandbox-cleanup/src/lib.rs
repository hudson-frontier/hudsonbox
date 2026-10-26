//! Independent storage cleanup. No supervisor/guest credentials, command
//! dispatch, or VM lifecycle calls belong in this worker.
pub mod audit;
pub mod sources;
use sandbox_artifacts::{ArtifactRetirer, Error as ArtifactError};
use sandbox_protocol::{
    OperationId,
    output::{OutputOwner, OutputPlan, OutputRef, OutputRetirement},
};
use sandbox_store::{
    Store,
    compaction::Compaction,
    output::OutputError,
    output_cleanup::{CleanupClaim, CleanupCompletion, CleanupPreparation},
    retention::ResponseRetention,
};
use std::{
    fmt,
    future::Future,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

const CLAIM_SECONDS: u32 = 120;
const QUERY_TIMEOUT: Duration = Duration::from_secs(5);
const PROCESS_TIMEOUT: Duration = Duration::from_secs(90);

/// Trusted service adapter, never customer-provided evidence. Implementations
/// must verify retirement of the exact plan before returning a receipt.
pub trait Retirement: Send + Sync {
    fn retire(
        &self,
        plan: &OutputPlan,
        owner: &OutputOwner,
        selected: Option<&OutputRef>,
        now: i64,
    ) -> impl Future<Output = Result<OutputRetirement, ArtifactError>> + Send;
}
impl Retirement for ArtifactRetirer {
    async fn retire(
        &self,
        plan: &OutputPlan,
        owner: &OutputOwner,
        selected: Option<&OutputRef>,
        now: i64,
    ) -> Result<OutputRetirement, ArtifactError> {
        ArtifactRetirer::retire(self, plan, owner, selected, now).await
    }
}

pub struct Cleaner<R = ArtifactRetirer> {
    store: Store,
    retirer: R,
    allow_simulated: bool,
    response_retention: Option<ResponseRetention>,
    compact_payloads: bool,
}
impl<R> fmt::Debug for Cleaner<R> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Cleaner")
            .field("allow_simulated", &self.allow_simulated)
            .field("response_retention", &self.response_retention)
            .field("compact_payloads", &self.compact_payloads)
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CleanupTick {
    Idle,
    RetentionAssigned(u64),
    PayloadCompaction(Compaction),
    Waiting(OperationId),
    Completed(OperationId),
}
#[derive(Debug, thiserror::Error)]
pub enum CleanupError {
    #[error("cleanup database operation failed: {0}")]
    Store(#[from] OutputError),
    #[error("response retention database operation failed")]
    Retention,
    #[error("cleanup storage retirement failed: {0}")]
    Storage(#[from] ArtifactError),
    #[error("cleanup deadline exceeded; outcome may be uncertain")]
    Timeout,
    #[error("cleanup service clock is invalid")]
    Clock,
    #[error("cleanup attempt for {operation_id} failed: {source}")]
    Attempt {
        operation_id: OperationId,
        source: Box<CleanupError>,
    },
}
fn now() -> Result<i64, CleanupError> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|d| i64::try_from(d.as_millis()).ok())
        .ok_or(CleanupError::Clock)
}
async fn query<T>(future: impl Future<Output = Result<T, OutputError>>) -> Result<T, CleanupError> {
    tokio::time::timeout(QUERY_TIMEOUT, future)
        .await
        .map_err(|_| CleanupError::Timeout)?
        .map_err(CleanupError::Store)
}

impl<R: Retirement> Cleaner<R> {
    pub fn new(store: Store, retirer: R, allow_simulated: bool) -> Self {
        Self {
            store,
            retirer,
            allow_simulated,
            response_retention: None,
            compact_payloads: false,
        }
    }

    /// Explicit operator policy. Previously assigned deadlines are immutable
    /// through this interface; active and unknown operations are left alone.
    pub fn with_response_retention(mut self, policy: ResponseRetention) -> Self {
        self.response_retention = Some(policy);
        self
    }

    /// Explicitly enable irreversible removal of eligible expired bodies.
    pub fn with_payload_compaction(mut self) -> Self {
        self.compact_payloads = true;
        self
    }

    /// Discover at most 100 candidates and process one, keeping storage I/O
    /// independent of lifecycle dispatch. Multiple processes share DB claims.
    pub async fn tick(&self) -> Result<CleanupTick, CleanupError> {
        let assigned = if let Some(policy) = self.response_retention {
            tokio::time::timeout(QUERY_TIMEOUT, self.store.assign_response_retention(policy))
                .await
                .map_err(|_| CleanupError::Timeout)?
                .map_err(|_| CleanupError::Retention)?
        } else {
            0
        };
        let compacted = if self.compact_payloads {
            query(self.store.compact_expired_response()).await?
        } else {
            Compaction::Idle
        };
        if matches!(compacted, Compaction::Deferred(_)) {
            return Ok(CleanupTick::PayloadCompaction(compacted));
        }
        query(self.store.enqueue_expired_output(100)).await?;
        let Some(claim) = query(self.store.claim_output_cleanup(CLAIM_SECONDS)).await? else {
            return Ok(if compacted != Compaction::Idle {
                CleanupTick::PayloadCompaction(compacted)
            } else if assigned == 0 {
                CleanupTick::Idle
            } else {
                CleanupTick::RetentionAssigned(assigned)
            });
        };
        let result = tokio::time::timeout(PROCESS_TIMEOUT, self.process(&claim))
            .await
            .unwrap_or(Err(CleanupError::Timeout));
        if result.is_err() {
            // A failed completion commit may already have succeeded. Deferral
            // cannot reopen completed work; a lost claim is left to its owner.
            let shift = claim.revision.saturating_sub(1).clamp(0, 10) as u32;
            let seconds = (5u32 * (1 << shift)).min(3600);
            let _ = query(self.store.defer_output_cleanup(&claim, seconds)).await;
        }
        result.map_err(|source| CleanupError::Attempt {
            operation_id: claim.operation_id,
            source: Box::new(source),
        })
    }

    async fn process(&self, claim: &CleanupClaim) -> Result<CleanupTick, CleanupError> {
        let work = match query(self.store.prepare_output_cleanup(claim)).await? {
            CleanupPreparation::Waiting { .. } => {
                return Ok(CleanupTick::Waiting(claim.operation_id));
            }
            CleanupPreparation::Ready { manifest } => manifest,
        };
        if work.simulated && !self.allow_simulated {
            return Err(OutputError::SimulationDenied.into());
        }
        let receipt = if let Some(plans) = &work.plans {
            let stdout = self
                .retirer
                .retire(
                    &plans.stdout,
                    &work.ticket.owner,
                    work.references.as_ref().map(|r| &r.stdout),
                    now()?,
                )
                .await?;
            let stderr = self
                .retirer
                .retire(
                    &plans.stderr,
                    &work.ticket.owner,
                    work.references.as_ref().map(|r| &r.stderr),
                    now()?,
                )
                .await?;
            CleanupCompletion::Retired {
                stdout: Box::new(stdout),
                stderr: Box::new(stderr),
            }
        } else {
            CleanupCompletion::NoUploadsAuthorized
        };
        query(
            self.store
                .complete_output_cleanup(claim, &receipt, self.allow_simulated),
        )
        .await?;
        Ok(CleanupTick::Completed(claim.operation_id))
    }
}
