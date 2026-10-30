//! Fenced allocation maintenance, independent of completed customer operations.
use crate::{Store, dispatch::DispatchError};
use sandbox_protocol::{
    AllocationId, HostId, Id, OperationId, ProjectId, RequestDigest, SandboxId,
    supervisor::{AllocationState, LeaseObservation, LeaseOwnership, LeaseRequest},
};
use serde_json::{Value, json};
use sqlx::{PgConnection, Row, postgres::PgRow};
use time::OffsetDateTime;

#[derive(Debug, Clone)]
pub struct AllocationClaim {
    pub allocation_id: AllocationId,
    pub revision: i64,
}
#[derive(Debug)]
pub enum LeaseAction {
    Renew(LeaseRequest),
    Inspect(LeaseOwnership),
    Cleanup(OperationId),
}
#[derive(Debug, PartialEq, Eq)]
pub enum LeaseResult {
    Renewed,
    Cleanup(OperationId),
}
struct Context {
    allocation: PgRow,
    sandbox: PgRow,
    project: PgRow,
    owner: LeaseOwnership,
    current_epoch: i64,
}
fn millis(stamp: OffsetDateTime) -> Result<i64, DispatchError> {
    i64::try_from(stamp.unix_timestamp_nanos() / 1_000_000).map_err(|_| DispatchError::InvalidData)
}
// PostgreSQL timestamps have finer precision than the supervisor RPC. A
// sub-millisecond increase must still select inspection when both deadlines
// encode to the same Unix millisecond.
fn renewal_deadline(
    confirmed: Option<OffsetDateTime>,
    candidate: OffsetDateTime,
) -> Result<Option<i64>, DispatchError> {
    let prior = confirmed.map(millis).transpose()?;
    let next = millis(candidate)?;
    Ok(prior.filter(|prior| next > *prior).map(|_| next))
}
async fn context(db: &mut PgConnection, claim: &AllocationClaim) -> Result<Context, DispatchError> {
    sqlx::query("SET LOCAL statement_timeout='2s'")
        .execute(&mut *db)
        .await?;
    // Discover identities without holding an allocation lock. The mutating
    // paths all serialize project -> sandbox -> host -> allocation, so this
    // cannot invert destroy's lock order.
    let ids = sqlx::query("SELECT project_id,sandbox_id,host_id FROM allocations WHERE id=$1")
        .bind(claim.allocation_id.uuid())
        .fetch_optional(&mut *db)
        .await?
        .ok_or(DispatchError::Conflict)?;
    let project_id: uuid::Uuid = ids.try_get("project_id")?;
    let sandbox_id: uuid::Uuid = ids.try_get("sandbox_id")?;
    let host_id: uuid::Uuid = ids.try_get("host_id")?;
    let project = sqlx::query("SELECT * FROM projects WHERE id=$1 FOR UPDATE")
        .bind(project_id)
        .fetch_one(&mut *db)
        .await?;
    let sandbox = sqlx::query("SELECT * FROM sandboxes WHERE id=$1 FOR UPDATE")
        .bind(sandbox_id)
        .fetch_one(&mut *db)
        .await?;
    let host = sqlx::query("SELECT supervisor_epoch FROM hosts WHERE id=$1 FOR UPDATE")
        .bind(host_id)
        .fetch_one(&mut *db)
        .await?;
    let allocation=sqlx::query("SELECT * FROM allocations WHERE id=$1 AND maintenance_revision=$2
        AND maintenance_lease_until>clock_timestamp() AND status='running' AND released_at IS NULL FOR UPDATE")
        .bind(claim.allocation_id.uuid()).bind(claim.revision).fetch_optional(&mut *db).await?.ok_or(DispatchError::LostClaim)?;
    let epoch: i64 = allocation.try_get("supervisor_epoch")?;
    let generation: i64 = allocation.try_get("generation")?;
    let current_epoch: i64 = host.try_get("supervisor_epoch")?;
    if current_epoch < epoch
        || sandbox.try_get::<i64, _>("generation")? != generation
        || sandbox.try_get::<Option<uuid::Uuid>, _>("current_allocation_id")?
            != Some(claim.allocation_id.uuid())
        || sandbox
            .try_get::<Option<uuid::Uuid>, _>("active_transition_operation_id")?
            .is_some()
        || sandbox.try_get::<String, _>("desired_state")? != "running"
    {
        return Err(DispatchError::Conflict);
    }
    let owner = LeaseOwnership {
        host_id: HostId::from_uuid(host_id).to_string(),
        project_id: ProjectId::from_uuid(project_id).to_string(),
        sandbox_id: SandboxId::from_uuid(sandbox_id).to_string(),
        allocation_id: claim.allocation_id.to_string(),
        generation,
        supervisor_epoch: epoch,
        revision: claim.revision,
        claim_expires_unix_ms: millis(allocation.try_get("maintenance_lease_until")?)?,
    };
    Ok(Context {
        allocation,
        sandbox,
        project,
        owner,
        current_epoch,
    })
}
async fn fence(db: &mut PgConnection, claim: &AllocationClaim) -> Result<(), DispatchError> {
    let (valid,): (bool,) = sqlx::query_as(
        "SELECT maintenance_revision=$2 AND maintenance_lease_until>clock_timestamp()
        AND status='running' AND released_at IS NULL FROM allocations WHERE id=$1",
    )
    .bind(claim.allocation_id.uuid())
    .bind(claim.revision)
    .fetch_one(db)
    .await?;
    if !valid {
        return Err(DispatchError::LostClaim);
    }
    Ok(())
}
async fn cleanup(
    db: &mut PgConnection,
    ctx: &Context,
    reason: &str,
) -> Result<OperationId, DispatchError> {
    let operation = OperationId::generate();
    let payload = json!({"reason":reason,"allocation_id":ctx.owner.allocation_id});
    let digest = RequestDigest::compute("SERVICE", "/allocation-cleanup", &payload)
        .map_err(|_| DispatchError::InvalidData)?;
    sqlx::query("INSERT INTO operations(id,project_id,sandbox_id,kind,initiator_kind,idempotency_key,request_digest,digest_version,payload,status,phase)
        VALUES($1,$2,$3,'destroy','service',$4,$5,$6,$7,'queued','admitted')")
        .bind(operation.uuid()).bind(ctx.project.try_get::<uuid::Uuid,_>("id")?).bind(ctx.sandbox.try_get::<uuid::Uuid,_>("id")?)
        .bind(format!("service-destroy-{operation}")).bind(digest.as_bytes().as_slice()).bind(sandbox_protocol::DIGEST_VERSION)
        .bind(payload).execute(&mut *db).await?;
    sqlx::query("UPDATE sandboxes SET desired_state='destroyed',observed_state='destroying',active_transition_operation_id=$2,
        state_revision=state_revision+1,updated_at=clock_timestamp() WHERE id=$1")
        .bind(ctx.sandbox.try_get::<uuid::Uuid,_>("id")?).bind(operation.uuid()).execute(db).await?;
    Ok(operation)
}
fn receipt(observation: &LeaseObservation) -> Value {
    let owner = observation.ownership.as_ref();
    json!({"allocation_id":owner.map(|o|&o.allocation_id),"host_id":owner.map(|o|&o.host_id),
        "supervisor_epoch":owner.map(|o|o.supervisor_epoch),"generation":owner.map(|o|o.generation),
        "revision":owner.map(|o|o.revision),"state":observation.state,"simulated":observation.simulated,
        "lease_expires_unix_ms":observation.allocation_expires_unix_ms,"observed_unix_ms":observation.observed_unix_ms})
}
impl Store {
    pub async fn claim_allocation(
        &self,
        host: HostId,
        epoch: i64,
        seconds: u32,
    ) -> Result<Option<AllocationClaim>, DispatchError> {
        if !(1..=300).contains(&seconds) {
            return Err(DispatchError::InvalidData);
        }
        let row=sqlx::query("WITH candidate AS (
            SELECT a.id FROM allocations a JOIN sandboxes s ON s.current_allocation_id=a.id JOIN hosts h ON h.id=a.host_id
            WHERE a.host_id=$1 AND h.supervisor_epoch=$2 AND a.supervisor_epoch<=$2 AND a.status='running' AND a.released_at IS NULL
                AND s.desired_state='running' AND s.active_transition_operation_id IS NULL
                AND (a.maintenance_lease_until IS NULL OR a.maintenance_lease_until<=clock_timestamp())
                AND (a.maintenance_next_at IS NULL OR a.maintenance_next_at<=clock_timestamp())
            ORDER BY a.maintenance_next_at NULLS FIRST,a.lease_expires_at,a.id FOR UPDATE OF a SKIP LOCKED LIMIT 1)
            UPDATE allocations a SET maintenance_revision=a.maintenance_revision+1,
                maintenance_lease_until=clock_timestamp()+make_interval(secs=>$3),updated_at=clock_timestamp()
            FROM candidate c WHERE a.id=c.id RETURNING a.id,a.maintenance_revision")
            .bind(host.uuid()).bind(epoch).bind(seconds as i32).fetch_optional(self.pool()).await?;
        row.map(|r| {
            Ok(AllocationClaim {
                allocation_id: AllocationId::from_uuid(r.try_get("id")?),
                revision: r.try_get("maintenance_revision")?,
            })
        })
        .transpose()
    }
    pub async fn prepare_lease(
        &self,
        claim: &AllocationClaim,
    ) -> Result<LeaseAction, DispatchError> {
        let mut tx = self.pool().begin().await?;
        let ctx = context(&mut tx, claim).await?;
        let (now,): (OffsetDateTime,) = sqlx::query_as("SELECT clock_timestamp()")
            .fetch_one(&mut *tx)
            .await?;
        let expiry: Option<OffsetDateTime> = ctx.sandbox.try_get("expires_at")?;
        let confirmed: Option<OffsetDateTime> = ctx.allocation.try_get("lease_expires_at")?;
        let requested: Option<OffsetDateTime> = ctx.allocation.try_get("lease_requested_until")?;
        // A shortened sandbox deadline cannot be implemented by extending a
        // lease. Stop through the ordinary service-owned lifecycle instead.
        if ctx.current_epoch > ctx.owner.supervisor_epoch
            || ctx.project.try_get::<String, _>("status")? != "active"
            || expiry.is_some_and(|e| {
                e <= now || confirmed.is_some_and(|v| v > e) || requested.is_some_and(|v| v > e)
            })
        {
            let reason = if ctx.current_epoch > ctx.owner.supervisor_epoch {
                "host_epoch_changed"
            } else {
                "execution_policy_revoked"
            };
            let operation = cleanup(&mut tx, &ctx, reason).await?;
            fence(&mut tx, claim).await?;
            let changed=sqlx::query("UPDATE allocations SET maintenance_lease_until=NULL,updated_at=clock_timestamp() WHERE id=$1 AND maintenance_revision=$2 AND maintenance_lease_until>clock_timestamp()")
                .bind(claim.allocation_id.uuid()).bind(claim.revision).execute(&mut *tx).await?.rows_affected();
            if changed != 1 {
                return Err(DispatchError::LostClaim);
            }
            tx.commit().await?;
            return Ok(LeaseAction::Cleanup(operation));
        }
        if ctx.allocation.try_get::<bool, _>("renewal_pending")?
            || confirmed.is_none_or(|v| v <= now)
        {
            fence(&mut tx, claim).await?;
            tx.commit().await?;
            return Ok(LeaseAction::Inspect(ctx.owner));
        }
        let until = expiry.map_or(now + time::Duration::seconds(30), |e| {
            e.min(now + time::Duration::seconds(30))
        });
        // Early boot maintenance and a fixed sandbox expiry need no extension.
        // Re-converting an unchanged Unix deadline with fresh wall/boot samples
        // can shorten the monotonic deadline by a millisecond and make the real
        // guardian reject a healthy allocation. Inspect the existing deadline
        // instead, without persisting a renewal intent or touching the watchdog.
        let Some(until_ms) = renewal_deadline(confirmed, until)? else {
            fence(&mut tx, claim).await?;
            tx.commit().await?;
            return Ok(LeaseAction::Inspect(ctx.owner));
        };
        sqlx::query("UPDATE allocations SET renewal_pending=true,lease_requested_until=$2,updated_at=clock_timestamp() WHERE id=$1")
            .bind(claim.allocation_id.uuid()).bind(until).execute(&mut *tx).await?;
        fence(&mut tx, claim).await?;
        tx.commit().await?;
        Ok(LeaseAction::Renew(LeaseRequest {
            ownership: Some(ctx.owner),
            allocation_expires_unix_ms: until_ms,
        }))
    }
    pub async fn defer_allocation(&self, claim: &AllocationClaim) -> Result<(), DispatchError> {
        let changed=sqlx::query("UPDATE allocations SET maintenance_lease_until=NULL,maintenance_next_at=clock_timestamp()+interval '2 seconds'
            WHERE id=$1 AND maintenance_revision=$2 AND maintenance_lease_until>clock_timestamp()")
            .bind(claim.allocation_id.uuid()).bind(claim.revision).execute(self.pool()).await?.rows_affected();
        if changed != 1 {
            return Err(DispatchError::LostClaim);
        }
        Ok(())
    }
    pub async fn record_lease_unknown(&self, claim: &AllocationClaim) -> Result<(), DispatchError> {
        let mut tx = self.pool().begin().await?;
        let ctx = context(&mut tx, claim).await?;
        sqlx::query("UPDATE sandboxes SET observed_state='unknown',state_revision=state_revision+1,updated_at=clock_timestamp() WHERE id=$1")
            .bind(ctx.sandbox.try_get::<uuid::Uuid,_>("id")?).execute(&mut *tx).await?;
        fence(&mut tx, claim).await?;
        let changed = sqlx::query(
            "UPDATE allocations SET renewal_pending=true,maintenance_lease_until=NULL,
            maintenance_next_at=clock_timestamp()+interval '2 seconds',updated_at=clock_timestamp()
            WHERE id=$1 AND maintenance_revision=$2 AND maintenance_lease_until>clock_timestamp()",
        )
        .bind(claim.allocation_id.uuid())
        .bind(claim.revision)
        .execute(&mut *tx)
        .await?
        .rows_affected();
        if changed != 1 {
            return Err(DispatchError::LostClaim);
        }
        tx.commit().await?;
        Ok(())
    }
    pub async fn record_lease_observation(
        &self,
        claim: &AllocationClaim,
        observation: &LeaseObservation,
        allow_simulated: bool,
    ) -> Result<LeaseResult, DispatchError> {
        if observation.simulated && !allow_simulated {
            return Err(DispatchError::SimulationDenied);
        }
        let mut tx = self.pool().begin().await?;
        let ctx = context(&mut tx, claim).await?;
        if ctx.current_epoch != ctx.owner.supervisor_epoch {
            return Err(DispatchError::Conflict);
        }
        if observation.ownership.as_ref() != Some(&ctx.owner) {
            return Err(DispatchError::BadEvidence);
        }
        let (now,): (OffsetDateTime,) = sqlx::query_as("SELECT clock_timestamp()")
            .fetch_one(&mut *tx)
            .await?;
        let state =
            AllocationState::try_from(observation.state).map_err(|_| DispatchError::BadEvidence)?;
        let result = match state {
            AllocationState::Released | AllocationState::FencedAbsent => {
                LeaseResult::Cleanup(cleanup(&mut tx, &ctx, "allocation_release_observed").await?)
            }
            AllocationState::Ready => {
                let observed = observation.allocation_expires_unix_ms;
                let requested = ctx
                    .allocation
                    .try_get::<Option<OffsetDateTime>, _>("lease_requested_until")?
                    .map(millis)
                    .transpose()?;
                let confirmed = ctx
                    .allocation
                    .try_get::<Option<OffsetDateTime>, _>("lease_expires_at")?
                    .map(millis)
                    .transpose()?;
                if Some(observed) != requested && Some(observed) != confirmed
                    || confirmed.is_some_and(|previous| observed < previous)
                    || observed <= millis(now)?
                {
                    return Err(DispatchError::BadEvidence);
                }
                let until =
                    OffsetDateTime::from_unix_timestamp_nanos(i128::from(observed) * 1_000_000)
                        .map_err(|_| DispatchError::BadEvidence)?;
                if ctx.project.try_get::<String, _>("status")? != "active"
                    || ctx
                        .sandbox
                        .try_get::<Option<OffsetDateTime>, _>("expires_at")?
                        .is_some_and(|e| e < until)
                {
                    LeaseResult::Cleanup(cleanup(&mut tx, &ctx, "execution_policy_revoked").await?)
                } else {
                    sqlx::query("UPDATE allocations SET lease_expires_at=$2,renewal_pending=false,
                        maintenance_next_at=greatest(clock_timestamp()+interval '1 second',least(clock_timestamp()+interval '10 seconds',$2-interval '10 seconds')) WHERE id=$1")
                        .bind(claim.allocation_id.uuid()).bind(until).execute(&mut *tx).await?;
                    sqlx::query("UPDATE sandboxes SET observed_state='running',observed_at=clock_timestamp(),observation_simulated=$2,
                        state_revision=state_revision+1,updated_at=clock_timestamp() WHERE id=$1")
                        .bind(ctx.sandbox.try_get::<uuid::Uuid,_>("id")?).bind(observation.simulated).execute(&mut *tx).await?;
                    LeaseResult::Renewed
                }
            }
            _ => return Err(DispatchError::BadEvidence),
        };
        // Final freshness and ownership check after every lock and write. An
        // invalid response rolls back cleanup admission as well as observations.
        let changed=sqlx::query("UPDATE allocations SET lease_observation=$3,maintenance_lease_until=NULL,updated_at=clock_timestamp()
            WHERE id=$1 AND maintenance_revision=$2 AND maintenance_lease_until>clock_timestamp()
            AND abs(extract(epoch FROM clock_timestamp())*1000-$4::bigint)<=10000
            AND ($5::boolean OR $6::bigint>extract(epoch FROM clock_timestamp())*1000)")
            .bind(claim.allocation_id.uuid()).bind(claim.revision).bind(receipt(observation)).bind(observation.observed_unix_ms)
            .bind(matches!(result,LeaseResult::Cleanup(_))).bind(observation.allocation_expires_unix_ms).execute(&mut *tx).await?.rows_affected();
        if changed != 1 {
            fence(&mut tx, claim).await?;
            return Err(DispatchError::BadEvidence);
        }
        tx.commit().await?;
        Ok(result)
    }
    /// A failed authenticated health check closes placement freshness, then
    /// reconciles a bounded batch of unknown runtime observations. Neither write
    /// frees reservations or changes operation/allocation ownership.
    pub async fn mark_host_runtime_unknown(
        &self,
        host: HostId,
        epoch: i64,
    ) -> Result<(), DispatchError> {
        // Commit before touching sandboxes: lifecycle writers lock sandbox ->
        // host, so holding host here would invert them. A concurrent epoch
        // advance is preserved; failure supplies no stop proof.
        let mut tx = self.pool().begin().await?;
        sqlx::query("SET LOCAL statement_timeout='2s'")
            .execute(&mut *tx)
            .await?;
        sqlx::query("SET LOCAL lock_timeout='1s'")
            .execute(&mut *tx)
            .await?;
        sqlx::query(
            "UPDATE hosts SET last_seen_at=NULL,updated_at=clock_timestamp()
            WHERE id=$1 AND supervisor_epoch=$2",
        )
        .bind(host.uuid())
        .bind(epoch)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        self.reconcile_unavailable_hosts().await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;

    #[test]
    fn submillisecond_deadline_changes_do_not_send_an_unchanged_rpc() {
        let prior = OffsetDateTime::UNIX_EPOCH
            + time::Duration::milliseconds(1000)
            + time::Duration::microseconds(100);
        let candidate = prior + time::Duration::microseconds(500);
        assert!(candidate > prior);
        assert_eq!(millis(prior).unwrap(), millis(candidate).unwrap());
        assert_eq!(renewal_deadline(Some(prior), candidate).unwrap(), None);
        assert_eq!(renewal_deadline(Some(prior), prior).unwrap(), None);
        assert_eq!(
            renewal_deadline(Some(prior), prior - time::Duration::milliseconds(1)).unwrap(),
            None
        );
        assert_eq!(renewal_deadline(None, candidate).unwrap(), None);
        assert_eq!(
            renewal_deadline(Some(prior), prior + time::Duration::microseconds(900)).unwrap(),
            Some(1001)
        );
    }
}
