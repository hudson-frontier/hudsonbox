//! Reconcile prior-epoch allocation release without granting old mutation authority.
use crate::{
    Store,
    claims::Claim,
    dispatch::{self, DispatchError},
};
use sandbox_protocol::{
    HostId, Id,
    supervisor::{AllocationState, PreviousAllocationObservation, PreviousAllocationRequest},
};
use serde_json::{Value, json};
use sqlx::Row;

#[derive(Debug)]
pub enum PreviousAction {
    RejectUndispatched,
    Reconcile(PreviousAllocationRequest),
}

impl Store {
    /// Reconcile stale inventory across every registered host, including hosts
    /// without a routable endpoint. Freshness is placement evidence, never proof
    /// of termination. Keep allocation/operation authority and all reservations.
    /// A bounded SKIP LOCKED batch lets replicas share work without waiting on
    /// lifecycle transactions; later ticks revisit skipped sandboxes.
    pub async fn reconcile_unavailable_hosts(&self) -> Result<u64, DispatchError> {
        let mut tx = self.pool().begin().await?;
        sqlx::query("SET LOCAL statement_timeout='2s'")
            .execute(&mut *tx)
            .await?;
        let changed = sqlx::query(
            "WITH candidates AS (
                SELECT s.id FROM sandboxes s
                JOIN allocations a ON a.id=s.current_allocation_id
                JOIN hosts h ON h.id=a.host_id
                WHERE a.released_at IS NULL AND s.observed_state<>'unknown'
                AND (h.last_seen_at IS NULL
                    OR h.last_seen_at<=clock_timestamp()-interval '30 seconds'
                    OR h.supervisor_epoch<>a.supervisor_epoch)
                ORDER BY s.id FOR UPDATE OF s SKIP LOCKED LIMIT 128)
            UPDATE sandboxes s SET observed_state='unknown',
                state_revision=state_revision+1,updated_at=clock_timestamp()
            FROM candidates c WHERE s.id=c.id",
        )
        .execute(&mut *tx)
        .await?
        .rows_affected();
        tx.commit().await?;
        Ok(changed)
    }

    /// Called under a live operation claim when its persisted owner cannot be
    /// reached. Preserve phase and receipts so recovery inspects the original
    /// intent. Never classify an undispatched request as an uncertain command.
    pub async fn record_owner_unavailable(&self, claim: &Claim) -> Result<(), DispatchError> {
        let mut tx = self.pool().begin().await?;
        sqlx::query("SET LOCAL statement_timeout='2s'")
            .execute(&mut *tx)
            .await?;
        let row = sqlx::query(
            "SELECT attempt_count FROM operations WHERE id=$1 AND claim_revision=$2
            AND lease_expires_at>clock_timestamp() AND status IN ('running','unknown')
            FOR UPDATE",
        )
        .bind(claim.operation_id.uuid())
        .bind(claim.revision)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or(DispatchError::LostClaim)?;
        if row.try_get::<i32, _>("attempt_count")? > 0 {
            sqlx::query(
                "UPDATE operations SET status='unknown',
                error=coalesce(error,'{\"code\":\"owner_unavailable\"}'::jsonb),
                updated_at=clock_timestamp() WHERE id=$1",
            )
            .bind(claim.operation_id.uuid())
            .execute(&mut *tx)
            .await?;
        }
        dispatch::fence(&mut tx, claim).await?;
        tx.commit().await?;
        Ok(())
    }

    pub async fn prepare_previous_allocation(
        &self,
        claim: &Claim,
        host: HostId,
        reporting_epoch: i64,
    ) -> Result<Option<PreviousAction>, DispatchError> {
        if reporting_epoch <= 0 {
            return Err(DispatchError::InvalidData);
        }
        let mut tx = self.pool().begin().await?;
        sqlx::query("SET LOCAL statement_timeout='2s'")
            .execute(&mut *tx)
            .await?;
        let candidate = sqlx::query(
            "SELECT o.kind FROM operations o JOIN sandboxes s ON s.id=o.sandbox_id
            JOIN allocations a ON a.id=s.current_allocation_id WHERE o.id=$1 AND a.host_id=$2
            AND a.supervisor_epoch<$3 AND a.released_at IS NULL",
        )
        .bind(claim.operation_id.uuid())
        .bind(host.uuid())
        .bind(reporting_epoch)
        .fetch_optional(&mut *tx)
        .await?;
        let Some(candidate) = candidate else {
            return Ok(None);
        };
        let kind: String = candidate.try_get("kind")?;
        if !matches!(kind.as_str(), "create" | "destroy") {
            return Err(DispatchError::Conflict);
        }
        let ctx = dispatch::context_at_epoch(&mut tx, claim, &kind, Some(reporting_epoch)).await?;
        if ctx.owner.host_id != host.to_string() {
            return Err(DispatchError::Conflict);
        }
        let attempts: i32 = ctx.op.try_get("attempt_count")?;
        if kind == "create" && attempts == 0 {
            // The normal rejection transaction must still independently prove
            // no intent, no execution lease, and an unreleased reservation.
            dispatch::fence(&mut tx, claim).await?;
            tx.commit().await?;
            return Ok(Some(PreviousAction::RejectUndispatched));
        }
        if (kind == "create" && attempts != 1)
            || (kind == "destroy"
                && ctx.sandbox.try_get::<String, _>("desired_state")? != "destroyed")
        {
            return Err(DispatchError::Conflict);
        }
        sqlx::query("UPDATE operations SET phase='reconciling_previous_epoch',updated_at=clock_timestamp() WHERE id=$1")
            .bind(claim.operation_id.uuid()).execute(&mut *tx).await?;
        dispatch::fence(&mut tx, claim).await?;
        tx.commit().await?;
        Ok(Some(PreviousAction::Reconcile(PreviousAllocationRequest {
            ownership: Some(ctx.owner),
            reporting_epoch,
        })))
    }

    pub async fn record_previous_unknown(
        &self,
        claim: &Claim,
        request: &PreviousAllocationRequest,
    ) -> Result<(), DispatchError> {
        let mut tx = self.pool().begin().await?;
        sqlx::query("SET LOCAL statement_timeout='2s'")
            .execute(&mut *tx)
            .await?;
        let kind: String = sqlx::query_scalar("SELECT kind FROM operations WHERE id=$1")
            .bind(claim.operation_id.uuid())
            .fetch_one(&mut *tx)
            .await?;
        if !matches!(kind.as_str(), "create" | "destroy") {
            return Err(DispatchError::Conflict);
        }
        let ctx = dispatch::context_at_epoch(&mut tx, claim, &kind, Some(request.reporting_epoch))
            .await?;
        if request.ownership.as_ref() != Some(&ctx.owner)
            || ctx.op.try_get::<String, _>("phase")? != "reconciling_previous_epoch"
        {
            return Err(DispatchError::BadEvidence);
        }
        sqlx::query("UPDATE sandboxes SET observed_state='unknown',state_revision=state_revision+1,updated_at=clock_timestamp() WHERE id=$1")
            .bind(ctx.sandbox.try_get::<uuid::Uuid,_>("id")?).execute(&mut *tx).await?;
        let n = sqlx::query("UPDATE operations SET status='unknown',phase='previous_release_unconfirmed',
            error=$3,lease_expires_at=NULL,next_retry_at=clock_timestamp()+interval '2 seconds',updated_at=clock_timestamp()
            WHERE id=$1 AND claim_revision=$2 AND lease_expires_at>clock_timestamp()")
            .bind(claim.operation_id.uuid()).bind(claim.revision)
            .bind(json!({"code":"previous_release_unconfirmed"})).execute(&mut *tx).await?.rows_affected();
        if n != 1 {
            return Err(DispatchError::LostClaim);
        }
        tx.commit().await?;
        Ok(())
    }

    pub async fn record_previous_release(
        &self,
        claim: &Claim,
        observation: &PreviousAllocationObservation,
        allow_simulated: bool,
    ) -> Result<(), DispatchError> {
        let request = observation
            .request
            .as_ref()
            .ok_or(DispatchError::BadEvidence)?;
        let release = observation
            .release
            .as_ref()
            .ok_or(DispatchError::BadEvidence)?;
        if release.simulated && !allow_simulated {
            return Err(DispatchError::SimulationDenied);
        }
        let mut tx = self.pool().begin().await?;
        sqlx::query("SET LOCAL statement_timeout='2s'")
            .execute(&mut *tx)
            .await?;
        let kind: String = sqlx::query_scalar("SELECT kind FROM operations WHERE id=$1")
            .bind(claim.operation_id.uuid())
            .fetch_one(&mut *tx)
            .await?;
        if !matches!(kind.as_str(), "create" | "destroy") {
            return Err(DispatchError::Conflict);
        }
        let ctx = dispatch::context_at_epoch(&mut tx, claim, &kind, Some(request.reporting_epoch))
            .await?;
        let state =
            AllocationState::try_from(release.state).map_err(|_| DispatchError::BadEvidence)?;
        if ctx.op.try_get::<String, _>("phase")? != "reconciling_previous_epoch"
            || request.ownership.as_ref() != Some(&ctx.owner)
            || release.ownership.as_ref() != Some(&ctx.owner)
            || release.observed_unix_ms <= 0
            || !matches!(
                state,
                AllocationState::Released | AllocationState::FencedAbsent
            )
            || (kind == "create"
                && (ctx.op.try_get::<i32, _>("attempt_count")? != 1
                    || (state == AllocationState::Released
                        && release.create_operation_id != ctx.owner.operation_id)
                    || (state == AllocationState::FencedAbsent
                        && (!release.create_operation_id.is_empty() || release.start_count != 0))))
            || (kind == "destroy"
                && ctx.sandbox.try_get::<String, _>("desired_state")? != "destroyed")
        {
            return Err(DispatchError::BadEvidence);
        }
        let mut receipt =
            dispatch::evidence(&ctx.owner, Some(release.simulated), "allocation_released");
        receipt["reporting_epoch"] = json!(request.reporting_epoch);
        receipt["previous_epoch_recovery"] = json!(true);
        receipt["fenced_absent"] = json!(state == AllocationState::FencedAbsent);
        receipt["observed_unix_ms"] = json!(release.observed_unix_ms);
        sqlx::query("UPDATE allocations SET status='released',released_at=clock_timestamp(),release_evidence=$2,
            maintenance_lease_until=NULL,updated_at=clock_timestamp() WHERE id=$1")
            .bind(ctx.allocation.try_get::<uuid::Uuid,_>("id")?).bind(&receipt).execute(&mut *tx).await?;
        sqlx::query("UPDATE sandboxes SET observed_state='destroyed',desired_state='destroyed',destroyed_at=clock_timestamp(),
            observed_at=clock_timestamp(),observation_simulated=$2,current_allocation_id=NULL,active_transition_operation_id=NULL,
            state_revision=state_revision+1,updated_at=clock_timestamp() WHERE id=$1")
            .bind(ctx.sandbox.try_get::<uuid::Uuid,_>("id")?).bind(release.simulated).execute(&mut *tx).await?;
        let payload: Value = ctx.op.try_get("payload")?;
        if kind == "destroy"
            && let Some(previous) = payload
                .get("supersedes_operation_id")
                .and_then(Value::as_str)
        {
            let previous: uuid::Uuid = previous.parse().map_err(|_| DispatchError::InvalidData)?;
            sqlx::query("UPDATE operations SET status='failed',phase='destroyed_during_create',completed_at=clock_timestamp(),
                error=$2,attempt_receipts=attempt_receipts||jsonb_build_array($3::jsonb),updated_at=clock_timestamp() WHERE id=$1")
                .bind(previous).bind(json!({"code":"destroyed_during_create","create_outcome_unknown":true,"destroy_operation_id":claim.operation_id.to_string()}))
                .bind(&receipt).execute(&mut *tx).await?;
        }
        let destroy = kind == "destroy";
        let n = sqlx::query("UPDATE operations SET status=$3,phase=$4,completed_at=clock_timestamp(),result=$5,error=$6,
            lease_expires_at=NULL,next_retry_at=NULL,attempt_receipts=attempt_receipts||jsonb_build_array($7::jsonb),updated_at=clock_timestamp()
            WHERE id=$1 AND claim_revision=$2 AND lease_expires_at>clock_timestamp()
            AND abs(extract(epoch FROM clock_timestamp())*1000-$8::bigint)<=10000")
            .bind(claim.operation_id.uuid()).bind(claim.revision).bind(if destroy {"succeeded"} else {"failed"})
            .bind(if destroy {"destroyed"} else {"released_before_completion"})
            .bind(if destroy {Some(json!({"sandbox_id":ctx.owner.sandbox_id,"simulated":release.simulated}))} else {None})
            .bind(if destroy {None} else {Some(json!({"code":"allocation_released_after_restart","create_outcome_unknown":true,"simulated":release.simulated}))})
            .bind(receipt).bind(release.observed_unix_ms).execute(&mut *tx).await?.rows_affected();
        if n != 1 {
            dispatch::fence(&mut tx, claim).await?;
            return Err(DispatchError::BadEvidence);
        }
        tx.commit().await?;
        Ok(())
    }
}
