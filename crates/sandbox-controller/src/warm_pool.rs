//! Warm-pool driver seam (#188). Durable intent, fencing and receipts live in
//! `sandbox_store::warm_pool`; this module only moves one claimed intent
//! through a [`WarmSupervisor`].
//!
//! The deployed supervisor has no prepare, adopt or warm-cleanup RPC, and an
//! allocation launch permit does not authorize adopting an existing VM. The
//! only production implementation is therefore [`UnsupportedWarmSupervisor`],
//! which fails closed: preparation intents retry and are then abandoned to
//! retirement, whose cleanup stays pending (reservation held) until a real
//! supervisor can prove destruction. Create routing does not call
//! `claim_warm_member`, so ordinary cold boots are unchanged.
use sandbox_protocol::warm_pool::{
    CleanupReceipt, HandoffIntent, HandoffReceipt, PrepareIntent, PrepareReceipt,
};
use sandbox_store::{
    Store,
    warm_pool::{Error, MAX_ATTEMPTS, WarmClaim, WarmWork},
};
use std::{future::Future, pin::Pin};

/// Supervisor RPC outcome. `Unknown` means the effect may have happened.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum WarmRpcError {
    #[error("supervisor does not implement warm-pool RPCs")]
    Unsupported,
    #[error("supervisor outcome unknown")]
    Unknown,
    #[error("supervisor proved the request was not applied")]
    NotApplied,
}

pub type WarmFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T, WarmRpcError>> + Send + 'a>>;

/// Implementations must be idempotent on the intent's member/allocation
/// identity, reject a stale supervisor epoch, and answer an inspection of a
/// previously applied intent with the same receipt (lost acknowledgements).
pub trait WarmSupervisor: Send + Sync + std::fmt::Debug {
    fn prepare<'a>(&'a self, intent: &'a PrepareIntent) -> WarmFuture<'a, PrepareReceipt>;
    fn handoff<'a>(&'a self, intent: &'a HandoffIntent) -> WarmFuture<'a, HandoffReceipt>;
    fn cleanup<'a>(&'a self, work: &'a WarmWork) -> WarmFuture<'a, CleanupReceipt>;
}

/// The current supervisor protocol: no warm-pool support.
#[derive(Debug, Default)]
pub struct UnsupportedWarmSupervisor;

impl WarmSupervisor for UnsupportedWarmSupervisor {
    fn prepare<'a>(&'a self, _: &'a PrepareIntent) -> WarmFuture<'a, PrepareReceipt> {
        Box::pin(async { Err(WarmRpcError::Unsupported) })
    }
    fn handoff<'a>(&'a self, _: &'a HandoffIntent) -> WarmFuture<'a, HandoffReceipt> {
        Box::pin(async { Err(WarmRpcError::Unsupported) })
    }
    fn cleanup<'a>(&'a self, _: &'a WarmWork) -> WarmFuture<'a, CleanupReceipt> {
        Box::pin(async { Err(WarmRpcError::Unsupported) })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Step {
    Idle,
    Prepared,
    Cleaned,
    /// Requeued behind waiting work; intent and receipt identity preserved.
    Retried,
    /// Preparation exhausted; the member was retired with its reservation held.
    Abandoned,
    /// Cleanup exhausted without evidence; the intent and reservation remain.
    CleanupPending,
}

/// Claim and drive at most one warm intent of a policy. Never loops.
pub async fn step(
    store: &Store,
    supervisor: &dyn WarmSupervisor,
    policy: uuid::Uuid,
    lease_seconds: u32,
) -> Result<Step, Error> {
    let Some(claim) = store.claim_warm_operation(policy, lease_seconds).await? else {
        return Ok(Step::Idle);
    };
    drive(store, supervisor, &claim).await
}

async fn drive(
    store: &Store,
    supervisor: &dyn WarmSupervisor,
    claim: &WarmClaim,
) -> Result<Step, Error> {
    let last = claim.revision >= MAX_ATTEMPTS;
    match &claim.work {
        WarmWork::Prepare(intent) => match supervisor.prepare(intent).await {
            Ok(receipt) => {
                store.record_warm_prepared(claim, &receipt).await?;
                Ok(Step::Prepared)
            }
            Err(_) if last => {
                store.abandon_warm_prepare(claim).await?;
                Ok(Step::Abandoned)
            }
            Err(_) => {
                store.retry_warm_operation(claim).await?;
                Ok(Step::Retried)
            }
        },
        work @ WarmWork::Retire { .. } => match supervisor.cleanup(work).await {
            Ok(receipt) => {
                store.record_warm_cleanup(claim, &receipt).await?;
                Ok(Step::Cleaned)
            }
            // Absence of a receipt is never cleanup evidence: keep the intent.
            Err(_) if last => Ok(Step::CleanupPending),
            Err(_) => {
                store.retry_warm_operation(claim).await?;
                Ok(Step::Retried)
            }
        },
    }
}
