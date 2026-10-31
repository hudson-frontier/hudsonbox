//! Warm-pool driver (#188). Durable intent, fencing and receipts live in
//! `sandbox_store::warm_pool`; this module moves claimed intents through the
//! supervisor's warm RPCs and routes creates to ready members.
//!
//! - Upkeep ([`Controller::warm_upkeep`]) reconciles each policy and drives a
//!   bounded number of prepare/retire intents through [`RpcWarmSupervisor`].
//! - Create ([`Controller::warm_create`]) claims a ready member atomically,
//!   marks the adopt dispatched durably, and sends AdoptWarmMember once. Any
//!   uncertain outcome is reconciled with InspectWarmHandoff, which never
//!   adopts and fences an unadopted member; Adopt is never replayed.
//!
//! The real Firecracker supervisor answers every warm RPC fail-closed (its
//! guest bootstrap binds allocation identity at boot), so on real hosts
//! preparation is refused and creates stay cold. Only the simulated fake host
//! adopts. [`UnsupportedWarmSupervisor`] remains for hosts without the RPCs.
use crate::{Controller, ControllerError, Tick};
use sandbox_protocol::{
    supervisor::{Ownership, WarmAdoptRequest, WarmPrepareRequest, WarmRetireRequest},
    warm_pool::{
        self, CleanupReceipt, HandoffOutcome, PrepareIntent, PrepareReceipt, RetireIntent, VERSION,
        WarmReceipt,
    },
};
use sandbox_store::{
    Store,
    claims::Claim,
    warm_pool::{AdoptMode, Claimed, Error, MAX_ATTEMPTS, MAX_BUDGET, WarmClaim, WarmWork},
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

/// Upkeep RPCs. Implementations must be idempotent on member identity and
/// reject a stale supervisor epoch. Handoff is driven by the create path.
pub trait WarmSupervisor: Send + Sync + std::fmt::Debug {
    fn prepare<'a>(&'a self, intent: &'a PrepareIntent) -> WarmFuture<'a, PrepareReceipt>;
    fn cleanup<'a>(&'a self, intent: &'a RetireIntent) -> WarmFuture<'a, CleanupReceipt>;
}

/// A host without warm RPCs: always fails closed.
#[derive(Debug, Default)]
pub struct UnsupportedWarmSupervisor;

impl WarmSupervisor for UnsupportedWarmSupervisor {
    fn prepare<'a>(&'a self, _: &'a PrepareIntent) -> WarmFuture<'a, PrepareReceipt> {
        Box::pin(async { Err(WarmRpcError::Unsupported) })
    }
    fn cleanup<'a>(&'a self, _: &'a RetireIntent) -> WarmFuture<'a, CleanupReceipt> {
        Box::pin(async { Err(WarmRpcError::Unsupported) })
    }
}

/// Warm upkeep RPCs over the controller's authenticated per-host routes. Each
/// call goes only to the intent's host at the intent's epoch.
#[derive(Debug)]
pub struct RpcWarmSupervisor<'a>(pub &'a Controller);

fn decode(bytes: &[u8]) -> Result<WarmReceipt, WarmRpcError> {
    warm_pool::decode(bytes).map_err(|_| WarmRpcError::Unknown)
}

impl WarmSupervisor for RpcWarmSupervisor<'_> {
    fn prepare<'a>(&'a self, intent: &'a PrepareIntent) -> WarmFuture<'a, PrepareReceipt> {
        Box::pin(async move {
            let mut route = self
                .0
                .route(intent.host_id, intent.supervisor_epoch, false)
                .await
                .map_err(|_| WarmRpcError::NotApplied)?;
            let request = WarmPrepareRequest {
                intent_json: warm_pool::encode(intent).map_err(|_| WarmRpcError::NotApplied)?,
            };
            let reply = self
                .0
                .rpc(
                    route.host,
                    route.epoch,
                    route.client.prepare_warm_member(request),
                )
                .await
                .map_err(|_| WarmRpcError::Unknown)?
                .into_inner();
            match decode(&reply.receipt_json)? {
                WarmReceipt::Prepared { receipt } => Ok(receipt),
                _ => Err(WarmRpcError::Unknown),
            }
        })
    }

    fn cleanup<'a>(&'a self, intent: &'a RetireIntent) -> WarmFuture<'a, CleanupReceipt> {
        Box::pin(async move {
            let mut route = self
                .0
                .route(intent.host_id, intent.supervisor_epoch, false)
                .await
                .map_err(|_| WarmRpcError::NotApplied)?;
            let request = WarmRetireRequest {
                intent_json: warm_pool::encode(intent).map_err(|_| WarmRpcError::NotApplied)?,
            };
            let reply = self
                .0
                .rpc(
                    route.host,
                    route.epoch,
                    route.client.retire_warm_member(request),
                )
                .await
                .map_err(|_| WarmRpcError::Unknown)?
                .into_inner();
            match decode(&reply.receipt_json)? {
                WarmReceipt::Cleanup { receipt } => Ok(receipt),
                _ => Err(WarmRpcError::Unknown),
            }
        })
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
        WarmWork::Retire {
            member_id,
            vm_ref,
            host_id,
            supervisor_epoch,
        } => {
            let intent = RetireIntent {
                version: VERSION,
                member_id: *member_id,
                vm_ref: vm_ref.clone(),
                host_id: *host_id,
                supervisor_epoch: *supervisor_epoch,
                operation_id: claim.id,
                operation_revision: claim.revision,
            };
            match supervisor.cleanup(&intent).await {
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
            }
        }
    }
}

/// Bounded upkeep result.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Upkeep {
    pub policies: usize,
    pub reserved: usize,
    pub retiring: usize,
    pub steps: Vec<Step>,
}

/// Policies visited per upkeep pass.
pub const UPKEEP_POLICIES: u32 = 64;

impl Controller {
    /// A worker sharing this controller's host connections, for upkeep.
    pub fn warm_upkeep_worker(&self) -> Self {
        self.worker(0)
    }

    /// One bounded upkeep pass: reconcile each policy (at most `budget` member
    /// changes) and drive at most `budget` intents per policy. Never loops.
    pub async fn warm_upkeep(&self, budget: u32) -> Result<Upkeep, Error> {
        if budget == 0 || budget > MAX_BUDGET {
            return Err(Error::Invalid);
        }
        let supervisor = RpcWarmSupervisor(self);
        let mut out = Upkeep::default();
        for policy in self.store.warm_pool_policy_ids(UPKEEP_POLICIES).await? {
            out.policies += 1;
            let reconciled = self.store.reconcile_warm_pool(policy, budget).await?;
            out.reserved += reconciled.prepare.len();
            out.retiring += reconciled.retire.len();
            for _ in 0..budget {
                match step(&self.store, &supervisor, policy, 30).await? {
                    Step::Idle => break,
                    s => out.steps.push(s),
                }
            }
        }
        Ok(out)
    }

    /// Route a create through a ready warm member, if any. `None` continues the
    /// ordinary cold path (no member, a denial the cold path reports itself, or
    /// an adopted member whose allocation is now inspected like a dispatched
    /// create).
    pub(crate) async fn warm_create(
        &mut self,
        claim: &Claim,
    ) -> Result<Option<Tick>, ControllerError> {
        match claim.phase.as_deref() {
            None | Some("queued" | "awaiting_placement") => {
                match self.store.claim_warm_member(claim).await {
                    Ok(Claimed::Claimed(_) | Claimed::Existing(_)) => {}
                    Ok(Claimed::None) => return Ok(None),
                    Err(Error::LostClaim) => return Ok(Some(Tick::LostOwnership)),
                    // The cold path rechecks and reports these itself.
                    Err(
                        Error::Unauthorized
                        | Error::Quota
                        | Error::NoPoolAccess
                        | Error::Reconcile
                        | Error::Invalid,
                    ) => return Ok(None),
                    Err(error) => return Err(error.into()),
                }
            }
            Some("warm_handoff" | "warm_adopt_dispatched") => {}
            // Waiting for cleanup evidence before the cold boot.
            Some("warm_handoff_rejected") => return self.defer(claim).await.map(Some),
            _ => return Ok(None),
        }
        self.warm_handoff(claim).await
    }

    async fn warm_handoff(&mut self, claim: &Claim) -> Result<Option<Tick>, ControllerError> {
        let dispatch = match self.store.begin_warm_adopt(claim).await {
            Ok(d) => d,
            Err(Error::LostClaim) => return Ok(Some(Tick::LostOwnership)),
            Err(Error::Reconcile | Error::Conflict) => return self.defer(claim).await.map(Some),
            Err(error) => return Err(error.into()),
        };
        let intent = &dispatch.handoff.intent;
        let mut route = match self
            .route(intent.host_id, intent.supervisor_epoch, false)
            .await
        {
            Ok(route) => route,
            Err(_) => {
                // A host at another epoch can never answer for this member: hand
                // the allocation to ordinary owner-loss reconciliation. The
                // member stays claimed and its reservation held.
                let moved = self
                    .store
                    .registered_host(intent.host_id)
                    .await?
                    .is_some_and(|h| h.supervisor_epoch != intent.supervisor_epoch);
                return if moved {
                    self.unknown(claim).await.map(Some)
                } else {
                    self.defer(claim).await.map(Some)
                };
            }
        };
        // Registers the exact allocation permit on hosts that require it; its
        // bytes must be the permit derived from the intent.
        let registered = match self
            .route_permit(&mut route, claim, &dispatch.handoff.allocation)
            .await
        {
            Ok(permit) => permit,
            Err(_) => return self.defer(claim).await.map(Some),
        };
        let permit = intent.permit();
        let launch_permit_json = if registered.is_empty() {
            warm_pool::encode(&permit).map_err(|_| ControllerError::InvalidConfig)?
        } else {
            match warm_pool::decode::<sandbox_protocol::allocation_authority::Permit>(&registered) {
                Ok(p) if p == permit => registered,
                _ => return self.unknown(claim).await.map(Some),
            }
        };
        let request = WarmAdoptRequest {
            ownership: Some(Ownership {
                host_id: intent.host_id.to_string(),
                project_id: intent.project_id.to_string(),
                sandbox_id: intent.sandbox_id.to_string(),
                allocation_id: intent.allocation_id.to_string(),
                operation_id: claim.operation_id.to_string(),
                generation: intent.generation,
                supervisor_epoch: intent.supervisor_epoch,
                claim_revision: claim.revision,
                claim_expires_unix_ms: i64::try_from(
                    claim.lease_expires_at.unix_timestamp_nanos() / 1_000_000,
                )
                .map_err(|_| ControllerError::InvalidConfig)?,
            }),
            intent_json: warm_pool::encode(intent).map_err(|_| ControllerError::InvalidConfig)?,
            launch_permit_json,
            allocation_expires_unix_ms: dispatch.allocation_expires_unix_ms,
        };
        let reply = match dispatch.mode {
            AdoptMode::Adopt => {
                self.rpc(
                    route.host,
                    route.epoch,
                    route.client.adopt_warm_member(request),
                )
                .await
            }
            AdoptMode::Inspect => {
                self.rpc(
                    route.host,
                    route.epoch,
                    route.client.inspect_warm_handoff(request),
                )
                .await
            }
        };
        // Uncertain: the phase stays dispatched, so the next attempt inspects.
        let receipt = match reply
            .ok()
            .and_then(|r| warm_pool::decode::<WarmReceipt>(&r.into_inner().receipt_json).ok())
        {
            Some(WarmReceipt::Handoff { receipt }) => receipt,
            _ => return self.defer(claim).await.map(Some),
        };
        match self.store.record_warm_handoff(claim, &receipt).await {
            // The create is now dispatched: the cold path inspects the adopted
            // allocation for readiness in this same tick.
            Ok(()) if matches!(receipt.outcome, HandoffOutcome::Adopted { .. }) => Ok(None),
            Ok(()) => self.defer(claim).await.map(Some),
            Err(Error::LostClaim) => Ok(Some(Tick::LostOwnership)),
            Err(Error::StaleEpoch) => self.unknown(claim).await.map(Some),
            // Mismatched or conflicting evidence: retain everything, re-inspect.
            Err(Error::Receipt(_) | Error::Conflict) => self.defer(claim).await.map(Some),
            Err(error) => Err(error.into()),
        }
    }
}
