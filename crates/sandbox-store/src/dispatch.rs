//! Create dispatch and completion. Intent is committed before a request escapes.
//! A lost response preserves the allocation and becomes unknown, never a retry.

use crate::{Store, claims::Claim};
use sandbox_protocol::{
    AllocationId, HostId, Id, OperationId, ProjectId, SandboxId,
    supervisor::{AllocationState, CreateRequest, Observation, Ownership, Resources},
};
use serde_json::{Value, json};
use sqlx::{PgConnection, Row, postgres::PgRow};
use std::collections::BTreeSet;
use time::OffsetDateTime;

#[derive(Debug)]
pub enum CreateAction {
    Start(CreateRequest),
    Inspect(Ownership),
}

#[derive(Debug, thiserror::Error)]
pub enum DispatchError {
    #[error("allocation resources are outside the supported envelope")]
    InvalidResources,
    #[error("create claim expired or changed")]
    LostClaim,
    #[error("create ownership or allocation requires reconciliation")]
    Conflict,
    #[error("project authority or deadline no longer permits dispatch")]
    Unauthorized,
    #[error("image is not in the controller allowlist")]
    ImageDenied,
    #[error("host capability policy does not match authenticated runtime or image resources")]
    HostCompatibility,
    #[error("configured host is absent, stale, or at another epoch")]
    HostUnavailable,
    #[error("supervisor observation does not match current ownership")]
    BadEvidence,
    #[error("simulated evidence requires explicit development opt-in")]
    SimulationDenied,
    #[error("persisted create data is invalid")]
    InvalidData,
    #[error("dispatch storage failed: {0}")]
    Query(#[from] sqlx::Error),
}

#[derive(Debug)]
pub(crate) struct Context {
    pub(crate) op: PgRow,
    pub(crate) sandbox: PgRow,
    pub(crate) allocation: PgRow,
    pub(crate) owner: Ownership,
}

fn millis(stamp: OffsetDateTime) -> Result<i64, DispatchError> {
    i64::try_from(stamp.unix_timestamp_nanos() / 1_000_000).map_err(|_| DispatchError::InvalidData)
}

pub(crate) async fn context(
    db: &mut PgConnection,
    claim: &Claim,
    kind: &str,
) -> Result<Context, DispatchError> {
    context_at_epoch(db, claim, kind, None).await
}

pub(crate) async fn context_at_epoch(
    db: &mut PgConnection,
    claim: &Claim,
    kind: &str,
    reporting_epoch: Option<i64>,
) -> Result<Context, DispatchError> {
    sqlx::query("SET LOCAL statement_timeout='2s'")
        .execute(&mut *db)
        .await?;
    let op = sqlx::query(
        "SELECT * FROM operations WHERE id=$1 AND claim_revision=$2
        AND lease_expires_at > clock_timestamp() AND status IN ('running','unknown') FOR UPDATE",
    )
    .bind(claim.operation_id.uuid())
    .bind(claim.revision)
    .fetch_optional(&mut *db)
    .await?
    .ok_or(DispatchError::LostClaim)?;
    let project: uuid::Uuid = op.try_get("project_id")?;
    let sandbox_id: uuid::Uuid = op.try_get("sandbox_id")?;
    if kind == "destroy" {
        let payload: Value = op.try_get("payload")?;
        if let Some(previous) = payload
            .get("supersedes_operation_id")
            .and_then(Value::as_str)
        {
            let previous: uuid::Uuid = previous.parse().map_err(|_| DispatchError::InvalidData)?;
            sqlx::query("SELECT id FROM operations WHERE id=$1 AND project_id=$2 AND sandbox_id=$3 AND kind='create' AND phase='cleanup_owned_by_destroy' FOR UPDATE")
                .bind(previous).bind(project).bind(sandbox_id).fetch_optional(&mut *db).await?.ok_or(DispatchError::Conflict)?;
        }
    }
    sqlx::query("SELECT id FROM projects WHERE id=$1 FOR UPDATE")
        .bind(project)
        .fetch_one(&mut *db)
        .await?;
    let sandbox = sqlx::query("SELECT * FROM sandboxes WHERE id=$1 FOR UPDATE")
        .bind(sandbox_id)
        .fetch_one(&mut *db)
        .await?;
    if op.try_get::<String, _>("kind")? != kind
        || sandbox.try_get::<Option<uuid::Uuid>, _>("active_transition_operation_id")?
            != Some(claim.operation_id.uuid())
    {
        return Err(DispatchError::Conflict);
    }
    let allocation_id: uuid::Uuid = sandbox
        .try_get::<Option<uuid::Uuid>, _>("current_allocation_id")?
        .ok_or(DispatchError::Conflict)?;
    let allocation = sqlx::query("SELECT * FROM allocations WHERE id=$1 AND released_at IS NULL")
        .bind(allocation_id)
        .fetch_optional(&mut *db)
        .await?
        .ok_or(DispatchError::Conflict)?;
    let host: uuid::Uuid = allocation.try_get("host_id")?;
    let host_row = sqlx::query("SELECT supervisor_epoch FROM hosts WHERE id=$1 FOR UPDATE")
        .bind(host)
        .fetch_one(&mut *db)
        .await?;
    let generation: i64 = allocation.try_get("generation")?;
    let epoch: i64 = allocation.try_get("supervisor_epoch")?;
    let current_epoch: i64 = host_row.try_get("supervisor_epoch")?;
    if sandbox.try_get::<i64, _>("generation")? != generation
        || reporting_epoch.map_or(current_epoch != epoch, |e| current_epoch != e || e <= epoch)
    {
        return Err(DispatchError::Conflict);
    }
    let owner = Ownership {
        host_id: HostId::from_uuid(host).to_string(),
        project_id: ProjectId::from_uuid(project).to_string(),
        sandbox_id: SandboxId::from_uuid(sandbox_id).to_string(),
        allocation_id: AllocationId::from_uuid(allocation_id).to_string(),
        operation_id: OperationId::from_uuid(op.try_get("id")?).to_string(),
        generation,
        supervisor_epoch: epoch,
        claim_revision: claim.revision,
        claim_expires_unix_ms: millis(op.try_get("lease_expires_at")?)?,
    };
    Ok(Context {
        op,
        sandbox,
        allocation,
        owner,
    })
}

/// Final fence after any lock wait. All state changes remain in the same transaction.
pub(crate) async fn fence(db: &mut PgConnection, claim: &Claim) -> Result<(), DispatchError> {
    let valid: (bool,) = sqlx::query_as(
        "SELECT claim_revision=$2 AND lease_expires_at > clock_timestamp()
        AND status IN ('running','unknown') FROM operations WHERE id=$1",
    )
    .bind(claim.operation_id.uuid())
    .bind(claim.revision)
    .fetch_one(db)
    .await?;
    if !valid.0 {
        return Err(DispatchError::LostClaim);
    }
    Ok(())
}

pub(crate) fn evidence(owner: &Ownership, simulated: Option<bool>, phase: &str) -> Value {
    json!({"phase":phase,"host_id":owner.host_id,"project_id":owner.project_id,"sandbox_id":owner.sandbox_id,
        "operation_id":owner.operation_id,"allocation_id":owner.allocation_id,"generation":owner.generation,
        "supervisor_epoch":owner.supervisor_epoch,"claim_revision":owner.claim_revision,"simulated":simulated})
}

impl Store {
    /// Refresh only an operator-provisioned host at its existing epoch. This cannot
    /// register a new host, change capacity/epoch, or turn a draining host ready.
    pub async fn observe_configured_host(
        &self,
        host: HostId,
        epoch: i64,
    ) -> Result<(), DispatchError> {
        self.observe_configured_host_with_capabilities(host, epoch, &[], false)
            .await
    }

    pub async fn prepare_create_dispatch(
        &self,
        claim: &Claim,
        images: &BTreeSet<String>,
    ) -> Result<CreateAction, DispatchError> {
        let mut tx = self.pool().begin().await?;
        let ctx = context(&mut tx, claim, "create").await?;
        // A prior durable intent is uncertainty even if its RPC was never sent.
        if ctx.op.try_get::<i32, _>("attempt_count")? > 0
            || ctx.op.try_get::<String, _>("status")? == "unknown"
        {
            sqlx::query("UPDATE operations SET phase='reconciling',updated_at=clock_timestamp() WHERE id=$1")
                .bind(claim.operation_id.uuid()).execute(&mut *tx).await?;
            fence(&mut tx, claim).await?;
            tx.commit().await?;
            return Ok(CreateAction::Inspect(ctx.owner));
        }
        if ctx.op.try_get::<Option<String>, _>("phase")?.as_deref() != Some("reserved")
            || ctx.allocation.try_get::<String, _>("status")? != "reserved"
            || ctx.sandbox.try_get::<String, _>("desired_state")? != "running"
        {
            return Err(DispatchError::Conflict);
        }
        let authorized: (bool,) = sqlx::query_as("SELECT p.status='active' AND o.initiator_kind='project'
            AND (o.deadline IS NULL OR o.deadline>clock_timestamp())
            AND (s.expires_at IS NULL OR s.expires_at>clock_timestamp())
            AND EXISTS(SELECT 1 FROM jsonb_array_elements(p.api_tokens) t WHERE t->>'key_id'=o.initiator_key_id
                AND (t->>'revoked_at' IS NULL OR (t->>'revoked_at')::timestamptz>clock_timestamp())
                AND (t->>'expires_at' IS NULL OR (t->>'expires_at')::timestamptz>clock_timestamp()))
            FROM operations o JOIN projects p ON p.id=o.project_id JOIN sandboxes s ON s.id=o.sandbox_id WHERE o.id=$1")
            .bind(claim.operation_id.uuid()).fetch_one(&mut *tx).await?;
        if !authorized.0 {
            return Err(DispatchError::Unauthorized);
        }
        let digest: String = ctx.sandbox.try_get("image_digest")?;
        if !images.contains(&digest) {
            return Err(DispatchError::ImageDenied);
        }
        let host: uuid::Uuid = ctx.allocation.try_get("host_id")?;
        let host_id = host;
        let host = sqlx::query("SELECT * FROM hosts WHERE id=$1 FOR UPDATE")
            .bind(host_id)
            .fetch_one(&mut *tx)
            .await?;
        // Evaluate time after the lock; health can expire while waiting for it.
        let fresh: bool = sqlx::query_scalar(
            "SELECT status='ready' AND supervisor_epoch=$2
            AND COALESCE(last_seen_at>clock_timestamp()-interval '30 seconds',false)
            FROM hosts WHERE id=$1",
        )
        .bind(host_id)
        .bind(ctx.owner.supervisor_epoch)
        .fetch_one(&mut *tx)
        .await?;
        if !fresh {
            return Err(DispatchError::HostUnavailable);
        }
        let resources = Resources {
            vcpu: u32::try_from(ctx.allocation.try_get::<i32, _>("vcpu")?)
                .map_err(|_| DispatchError::InvalidData)?,
            memory_mib: u64::try_from(ctx.allocation.try_get::<i64, _>("memory_mib")?)
                .map_err(|_| DispatchError::InvalidData)?,
            disk_mib: u64::try_from(ctx.allocation.try_get::<i64, _>("disk_mib")?)
                .map_err(|_| DispatchError::InvalidData)?,
        };
        if !sandbox_protocol::resources::supported(
            resources.vcpu.into(),
            resources.memory_mib.into(),
            resources.disk_mib.into(),
        ) {
            return Err(DispatchError::InvalidResources);
        }
        crate::host_capabilities::compatible(
            &host,
            &digest,
            u64::from(resources.vcpu),
            resources.memory_mib,
            resources.disk_mib,
        )?;
        let allocation_id: uuid::Uuid = ctx.allocation.try_get("id")?;
        let lease: (OffsetDateTime,) = sqlx::query_as(
            "UPDATE allocations SET lease_expires_at=least(clock_timestamp()+interval '60 seconds',
            (SELECT expires_at FROM sandboxes WHERE id=allocations.sandbox_id)),
            updated_at=clock_timestamp() WHERE id=$1 RETURNING lease_expires_at",
        )
        .bind(allocation_id)
        .fetch_one(&mut *tx)
        .await?;
        sqlx::query("UPDATE operations SET phase='create_dispatched',attempt_count=attempt_count+1,
            attempt_receipts=attempt_receipts||jsonb_build_array($2::jsonb),updated_at=clock_timestamp() WHERE id=$1")
            .bind(claim.operation_id.uuid()).bind(evidence(&ctx.owner,None,"create_dispatch_intent")).execute(&mut *tx).await?;
        fence(&mut tx, claim).await?;
        tx.commit().await?;
        Ok(CreateAction::Start(CreateRequest {
            launch_permit_json: Vec::new(),
            ownership: Some(ctx.owner),
            image_digest: digest,
            resources: Some(resources),
            allocation_expires_unix_ms: millis(lease.0)?,
        }))
    }

    /// Mark uncertainty without discarding its allocation, generation, or dispatch
    /// receipt. Retry scheduling is bounded; repeated polls do not grow receipts.
    pub async fn record_create_unknown(&self, claim: &Claim) -> Result<(), DispatchError> {
        let mut tx = self.pool().begin().await?;
        let ctx = context(&mut tx, claim, "create").await?;
        sqlx::query("UPDATE sandboxes SET observed_state='unknown',state_revision=state_revision+1,updated_at=clock_timestamp() WHERE id=$1")
            .bind(ctx.sandbox.try_get::<uuid::Uuid,_>("id")?).execute(&mut *tx).await?;
        fence(&mut tx, claim).await?;
        let changed = sqlx::query("UPDATE operations SET status='unknown',phase='reconciliation_required',lease_expires_at=NULL,
            next_retry_at=clock_timestamp()+interval '1 second',error=$2,updated_at=clock_timestamp()
            WHERE id=$1 AND claim_revision=$3 AND lease_expires_at>clock_timestamp()")
            .bind(claim.operation_id.uuid()).bind(json!({"code":"outcome_unknown","message":"Awaiting matching supervisor evidence; create will not be replayed."}))
            .bind(claim.revision).execute(&mut *tx).await?.rows_affected();
        if changed != 1 {
            return Err(DispatchError::LostClaim);
        }
        tx.commit().await?;
        Ok(())
    }

    /// Only authenticated, tuple-matching observations may reach this boundary.
    /// The caller supplies development opt-in; the source is persisted and public.
    pub async fn record_create_observation(
        &self,
        claim: &Claim,
        observation: &Observation,
        allow_simulated: bool,
    ) -> Result<(), DispatchError> {
        if observation.simulated && !allow_simulated {
            return Err(DispatchError::SimulationDenied);
        }
        let mut tx = self.pool().begin().await?;
        let ctx = context(&mut tx, claim, "create").await?;
        if observation.ownership.as_ref() != Some(&ctx.owner)
            || observation.create_operation_id != ctx.owner.operation_id
            || observation.observed_unix_ms <= 0
            || ctx.op.try_get::<i32, _>("attempt_count")? != 1
        {
            return Err(DispatchError::BadEvidence);
        }
        let fresh: (bool,) = sqlx::query_as(
            "SELECT abs(extract(epoch FROM clock_timestamp())*1000-$1::bigint)<=10000",
        )
        .bind(observation.observed_unix_ms)
        .fetch_one(&mut *tx)
        .await?;
        if !fresh.0 {
            return Err(DispatchError::BadEvidence);
        }
        let state =
            AllocationState::try_from(observation.state).map_err(|_| DispatchError::BadEvidence)?;
        if !matches!(state, AllocationState::Ready | AllocationState::Released) {
            return Err(DispatchError::BadEvidence);
        }
        // A readiness response must still be inside the execution lease when it
        // is committed. Expiry never becomes release evidence on its own.
        if state == AllocationState::Ready {
            let valid: (bool,) = sqlx::query_as("SELECT COALESCE(lease_expires_at>clock_timestamp(),false) FROM allocations WHERE id=$1")
                .bind(ctx.allocation.try_get::<uuid::Uuid,_>("id")?).fetch_one(&mut *tx).await?;
            if !valid.0 {
                return Err(DispatchError::BadEvidence);
            }
        }
        let mut receipt = evidence(
            &ctx.owner,
            Some(observation.simulated),
            if state == AllocationState::Ready {
                "guest_ready"
            } else {
                "allocation_released"
            },
        );
        receipt["observed_unix_ms"] = json!(observation.observed_unix_ms);
        receipt["start_count"] = json!(observation.start_count);
        let sandbox_id: uuid::Uuid = ctx.sandbox.try_get("id")?;
        let allocation_id: uuid::Uuid = ctx.allocation.try_get("id")?;
        if state == AllocationState::Ready {
            sqlx::query(
                "UPDATE allocations SET status='running',maintenance_next_at=clock_timestamp()+interval '10 seconds',updated_at=clock_timestamp() WHERE id=$1",
            )
            .bind(allocation_id)
            .execute(&mut *tx)
            .await?;
            sqlx::query("UPDATE sandboxes SET observed_state='running',observed_at=clock_timestamp(),observation_simulated=$2,
                active_transition_operation_id=NULL,state_revision=state_revision+1,updated_at=clock_timestamp() WHERE id=$1")
                .bind(sandbox_id).bind(observation.simulated).execute(&mut *tx).await?;
        } else {
            sqlx::query("UPDATE allocations SET status='released',released_at=clock_timestamp(),release_evidence=$2,updated_at=clock_timestamp() WHERE id=$1")
                .bind(allocation_id).bind(&receipt).execute(&mut *tx).await?;
            sqlx::query("UPDATE sandboxes SET desired_state='destroyed',observed_state='destroyed',observed_at=clock_timestamp(),
                observation_simulated=$2,destroyed_at=clock_timestamp(),current_allocation_id=NULL,active_transition_operation_id=NULL,
                state_revision=state_revision+1,updated_at=clock_timestamp() WHERE id=$1")
                .bind(sandbox_id).bind(observation.simulated).execute(&mut *tx).await?;
        }
        fence(&mut tx, claim).await?;
        let ready = state == AllocationState::Ready;
        let changed = sqlx::query("UPDATE operations SET status=$2,phase=$3,result=$4,error=$5,completed_at=clock_timestamp(),
            lease_expires_at=NULL,next_retry_at=NULL,attempt_receipts=attempt_receipts||jsonb_build_array($6::jsonb),updated_at=clock_timestamp()
            WHERE id=$1 AND claim_revision=$7 AND lease_expires_at>clock_timestamp()
            AND (NOT $8::boolean OR EXISTS(SELECT 1 FROM allocations WHERE id=$9 AND lease_expires_at>clock_timestamp()))
            AND abs(extract(epoch FROM clock_timestamp())*1000-$10::bigint)<=10000")
            .bind(claim.operation_id.uuid()).bind(if ready {"succeeded"} else {"failed"})
            .bind(if ready {"ready"} else {"released_before_completion"})
            .bind(if ready {Some(json!({"sandbox_id":ctx.owner.sandbox_id,"simulated":observation.simulated}))} else {None})
            .bind(if ready {None} else {Some(json!({"code":"allocation_released","simulated":observation.simulated}))})
            .bind(receipt).bind(claim.revision).bind(ready).bind(allocation_id).bind(observation.observed_unix_ms)
            .execute(&mut *tx).await?.rows_affected();
        if changed != 1 {
            fence(&mut tx, claim).await?;
            return Err(DispatchError::BadEvidence);
        }
        tx.commit().await?;
        Ok(())
    }
}

#[derive(Debug, Clone, Copy)]
pub enum CreateRejection {
    HostEpochChanged,
    Unauthorized,
    ImageDenied,
    InvalidResources,
    /// The project has no grant for any eligible host pool. Not retried.
    HostPoolAccessDenied,
    /// Placement waits below expired without a fit.
    InsufficientCapacity,
    ProjectQuota,
    NoReadyHost,
    NoCompatibleHost,
}
impl CreateRejection {
    fn code(self) -> &'static str {
        match self {
            Self::HostEpochChanged => "host_epoch_changed",
            Self::Unauthorized => "authorization_revoked",
            Self::ImageDenied => "image_not_allowed",
            Self::InvalidResources => "invalid_resources",
            Self::HostPoolAccessDenied => "host_pool_access_denied",
            Self::InsufficientCapacity => "insufficient_capacity",
            Self::ProjectQuota => "project_quota_exceeded",
            Self::NoReadyHost => "no_ready_host",
            Self::NoCompatibleHost => "no_compatible_host",
        }
    }
}

impl Store {
    /// Service-owned cleanup is safe only when durable evidence proves that no
    /// create RPC could have been sent. Unknown or dispatched work is excluded.
    pub async fn reject_undispatched_create(
        &self,
        claim: &Claim,
        reason: CreateRejection,
    ) -> Result<(), DispatchError> {
        let mut tx = self.pool().begin().await?;
        let op = sqlx::query("SELECT * FROM operations WHERE id=$1 AND claim_revision=$2 AND lease_expires_at>clock_timestamp()
            AND status='running' AND kind='create' AND attempt_count=0 FOR UPDATE")
            .bind(claim.operation_id.uuid()).bind(claim.revision).fetch_optional(&mut *tx).await?.ok_or(DispatchError::Conflict)?;
        if !matches!(
            op.try_get::<Option<String>, _>("phase")?.as_deref(),
            None | Some("queued" | "awaiting_placement" | "reserved")
        ) {
            return Err(DispatchError::Conflict);
        }
        sqlx::query("SELECT id FROM projects WHERE id=$1 FOR UPDATE")
            .bind(op.try_get::<uuid::Uuid, _>("project_id")?)
            .fetch_one(&mut *tx)
            .await?;
        let sandbox_id: uuid::Uuid = op.try_get("sandbox_id")?;
        let sandbox = sqlx::query("SELECT * FROM sandboxes WHERE id=$1 FOR UPDATE")
            .bind(sandbox_id)
            .fetch_one(&mut *tx)
            .await?;
        if sandbox.try_get::<Option<uuid::Uuid>, _>("active_transition_operation_id")?
            != Some(claim.operation_id.uuid())
        {
            return Err(DispatchError::Conflict);
        }
        let receipt = json!({"phase":"rejected_before_dispatch","operation_id":claim.operation_id.to_string(),
            "claim_revision":claim.revision,"dispatch_intent_absent":true,"code":reason.code()});
        if let Some(allocation_id) =
            sandbox.try_get::<Option<uuid::Uuid>, _>("current_allocation_id")?
        {
            let allocation = sqlx::query("SELECT * FROM allocations WHERE id=$1 AND status='reserved' AND released_at IS NULL AND lease_expires_at IS NULL")
                .bind(allocation_id).fetch_optional(&mut *tx).await?.ok_or(DispatchError::Conflict)?;
            sqlx::query("SELECT id FROM hosts WHERE id=$1 FOR UPDATE")
                .bind(allocation.try_get::<uuid::Uuid, _>("host_id")?)
                .fetch_one(&mut *tx)
                .await?;
            sqlx::query("UPDATE allocations SET status='released',released_at=clock_timestamp(),release_evidence=$2,updated_at=clock_timestamp() WHERE id=$1")
                .bind(allocation_id).bind(&receipt).execute(&mut *tx).await?;
        }
        sqlx::query("UPDATE sandboxes SET desired_state='destroyed',observed_state='destroyed',observed_at=clock_timestamp(),
            destroyed_at=clock_timestamp(),current_allocation_id=NULL,active_transition_operation_id=NULL,state_revision=state_revision+1,
            updated_at=clock_timestamp() WHERE id=$1").bind(sandbox_id).execute(&mut *tx).await?;
        fence(&mut tx, claim).await?;
        let changed = sqlx::query("UPDATE operations SET status='failed',phase='rejected_before_dispatch',error=$2,completed_at=clock_timestamp(),
            lease_expires_at=NULL,next_retry_at=NULL,attempt_receipts=attempt_receipts||jsonb_build_array($3::jsonb),updated_at=clock_timestamp()
            WHERE id=$1 AND claim_revision=$4 AND lease_expires_at>clock_timestamp()")
            .bind(claim.operation_id.uuid()).bind(json!({"code":reason.code()})).bind(receipt).bind(claim.revision)
            .execute(&mut *tx).await?.rows_affected();
        if changed != 1 {
            return Err(DispatchError::LostClaim);
        }
        tx.commit().await?;
        Ok(())
    }
}
