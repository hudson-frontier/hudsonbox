//! Durable command admission and dispatch intent, independent of lifecycle ownership.
//! This module does not execute commands; only its Dispatch action permits a first RPC.
use crate::{
    Store,
    claims::Claim,
    dispatch::{self, DispatchError},
};
use sandbox_protocol::{
    AllocationId, HostId, Id, IdempotencyKey, OperationId, ProjectId, RequestDigest, SandboxId,
    TokenKeyId,
    command::{CommandInput, MAX_COMMANDS, MAX_DURATION_MS},
    guest_model::{Execute, MAX_RESERVED_OUTPUT},
    idempotency::DIGEST_VERSION,
    supervisor::Ownership,
};
use serde_json::json;
use sqlx::{PgConnection, Row};
use time::OffsetDateTime;

#[derive(Debug, Clone)]
pub struct ExecuteCommand {
    pub project_id: ProjectId,
    pub sandbox_id: SandboxId,
    pub key_id: TokenKeyId,
    pub idempotency_key: IdempotencyKey,
    pub command: CommandInput,
}
#[derive(Debug, PartialEq, Eq)]
pub enum ExecuteAdmission {
    ResponseExpired(OperationId),
    Accepted {
        operation_id: OperationId,
        status: String,
    },
    NotFound,
    Gone,
    Unauthorized,
    DigestConflict,
    InvalidCommand,
    InvalidDeadline,
    NotRunning,
    GenerationMismatch,
    CapacityExhausted,
    Busy(OperationId),
}
#[derive(Debug)]
pub enum ExecuteAction {
    Dispatch {
        owner: Ownership,
        command: Execute,
    },
    Inspect {
        owner: Ownership,
        digest: [u8; 32],
    },
    Cancel {
        owner: Ownership,
        digest: [u8; 32],
    },
    /// Proven never dispatched. No VM reservation or lifecycle state changes.
    Rejected,
}

async fn existing(
    db: &mut PgConnection,
    r: &ExecuteCommand,
    digest: &RequestDigest,
) -> Result<Option<ExecuteAdmission>, sqlx::Error> {
    let row = sqlx::query("SELECT id,request_digest,status,digest_version,
        COALESCE(status IN ('succeeded','failed','cancelled') AND response_expires_at<=clock_timestamp(),false) AS response_expired
        FROM operations WHERE project_id=$1 AND idempotency_key=$2")
        .bind(r.project_id.uuid()).bind(r.idempotency_key.as_str()).fetch_optional(db).await?;
    row.map(|row| -> Result<ExecuteAdmission, sqlx::Error> {
        let old: Vec<u8> = row.try_get("request_digest")?;
        let id = OperationId::from_uuid(row.try_get("id")?);
        Ok(
            if old.as_slice() != digest.as_bytes().as_slice()
                || row.try_get::<i32, _>("digest_version")? != DIGEST_VERSION
            {
                ExecuteAdmission::DigestConflict
            } else if row.try_get::<bool, _>("response_expired")? {
                ExecuteAdmission::ResponseExpired(id)
            } else {
                ExecuteAdmission::Accepted {
                    operation_id: id,
                    status: row.try_get("status")?,
                }
            },
        )
    })
    .transpose()
}

impl Store {
    pub async fn admit_execute(
        &self,
        r: &ExecuteCommand,
    ) -> Result<ExecuteAdmission, DispatchError> {
        if r.command.validate().is_err() {
            return Ok(ExecuteAdmission::InvalidCommand);
        }
        let digest = RequestDigest::compute(
            "POST",
            &format!("/v1/sandboxes/{}/execute", r.sandbox_id),
            &r.command,
        )
        .map_err(|_| DispatchError::InvalidData)?;
        match self.try_admit_execute(r, &digest).await {
            Err(DispatchError::Query(error))
                if error
                    .as_database_error()
                    .and_then(sqlx::error::DatabaseError::code)
                    .is_some_and(|code| code == "23505") =>
            {
                let mut db = self.pool().acquire().await?;
                existing(&mut db, r, &digest)
                    .await?
                    .ok_or(DispatchError::Conflict)
            }
            other => other,
        }
    }

    async fn try_admit_execute(
        &self,
        r: &ExecuteCommand,
        digest: &RequestDigest,
    ) -> Result<ExecuteAdmission, DispatchError> {
        let mut tx = self.pool().begin().await?;
        // Admission and credential changes serialize on the project. Do not lock
        // an existing command here: controllers lock operation before project.
        let authorized: Option<(bool,)> = sqlx::query_as(
            "SELECT status='active' AND EXISTS(
            SELECT 1 FROM jsonb_array_elements(api_tokens) t WHERE t->>'key_id'=$2
            AND (t->>'revoked_at' IS NULL OR (t->>'revoked_at')::timestamptz>clock_timestamp())
            AND (t->>'expires_at' IS NULL OR (t->>'expires_at')::timestamptz>clock_timestamp()))
            FROM projects WHERE id=$1 FOR UPDATE",
        )
        .bind(r.project_id.uuid())
        .bind(r.key_id.as_str())
        .fetch_optional(&mut *tx)
        .await?;
        if authorized != Some((true,)) {
            return Ok(ExecuteAdmission::Unauthorized);
        }
        if let Some(result) = existing(&mut tx, r, digest).await? {
            return Ok(result);
        }
        let sandbox =
            sqlx::query("SELECT * FROM sandboxes WHERE id=$1 AND project_id=$2 FOR UPDATE")
                .bind(r.sandbox_id.uuid())
                .bind(r.project_id.uuid())
                .fetch_optional(&mut *tx)
                .await?;
        let Some(sandbox) = sandbox else {
            return Ok(ExecuteAdmission::NotFound);
        };
        if sandbox.try_get::<String, _>("desired_state")? == "destroyed" {
            return Ok(ExecuteAdmission::Gone);
        }
        let generation: i64 = sandbox.try_get("generation")?;
        if r.command
            .expected_generation
            .is_some_and(|expected| expected != generation)
        {
            return Ok(ExecuteAdmission::GenerationMismatch);
        }
        if let Some(id) =
            sandbox.try_get::<Option<uuid::Uuid>, _>("active_transition_operation_id")?
        {
            return Ok(ExecuteAdmission::Busy(OperationId::from_uuid(id)));
        }
        if sandbox.try_get::<String, _>("observed_state")? != "running" {
            return Ok(ExecuteAdmission::NotRunning);
        }
        let Some(allocation) = sandbox.try_get::<Option<uuid::Uuid>, _>("current_allocation_id")?
        else {
            return Ok(ExecuteAdmission::NotRunning);
        };
        let active: Option<(uuid::Uuid,)> = sqlx::query_as(
            "SELECT id FROM operations WHERE sandbox_id=$1
            AND kind='execute' AND status IN ('queued','running','unknown')",
        )
        .bind(r.sandbox_id.uuid())
        .fetch_optional(&mut *tx)
        .await?;
        if let Some((id,)) = active {
            return Ok(ExecuteAdmission::Busy(OperationId::from_uuid(id)));
        }
        let now: (OffsetDateTime,) = sqlx::query_as("SELECT clock_timestamp()")
            .fetch_one(&mut *tx)
            .await?;
        let now_ms = now.0.unix_timestamp_nanos() / 1_000_000;
        let remaining = i128::from(r.command.deadline_unix_ms) - now_ms;
        if !(1..=i128::from(MAX_DURATION_MS)).contains(&remaining)
            || sandbox
                .try_get::<Option<OffsetDateTime>, _>("expires_at")?
                .is_some_and(|t| t <= now.0)
        {
            return Ok(ExecuteAdmission::InvalidDeadline);
        }
        let deadline = OffsetDateTime::from_unix_timestamp_nanos(
            i128::from(r.command.deadline_unix_ms) * 1_000_000,
        )
        .map_err(|_| DispatchError::InvalidData)?;
        if sandbox
            .try_get::<Option<OffsetDateTime>, _>("expires_at")?
            .is_some_and(|t| deadline > t)
        {
            return Ok(ExecuteAdmission::InvalidDeadline);
        }
        // Lock the host after the sandbox, as placement/maintenance do. The
        // sandbox lock protects its allocation from concurrent lifecycle changes.
        let ready: Option<(bool,)> = sqlx::query_as("SELECT a.status='running' AND a.released_at IS NULL
            AND COALESCE(a.lease_expires_at>clock_timestamp(),false)
            AND a.generation=$3 AND a.supervisor_epoch=h.supervisor_epoch AND h.status='ready'
            AND COALESCE(h.last_seen_at>clock_timestamp()-interval '30 seconds',false)
            FROM allocations a JOIN hosts h ON h.id=a.host_id WHERE a.id=$1 AND a.sandbox_id=$2 FOR UPDATE OF h")
            .bind(allocation).bind(r.sandbox_id.uuid()).bind(sandbox.try_get::<i64,_>("generation")?)
            .fetch_optional(&mut *tx).await?;
        if ready != Some((true,)) {
            return Ok(ExecuteAdmission::NotRunning);
        }
        if !has_capacity(&mut tx, allocation, Some(r.command.output_limit)).await? {
            return Ok(ExecuteAdmission::CapacityExhausted);
        }
        // A host lock wait may outlive the deadline or credential. Recheck both
        // using database time in the committing statement.
        let operation = crate::history::next_operation(&mut tx, allocation).await?;
        let payload = serde_json::to_value(&r.command).map_err(|_| DispatchError::InvalidData)?;
        let changed = sqlx::query("INSERT INTO operations(id,project_id,sandbox_id,kind,initiator_kind,initiator_key_id,
            idempotency_key,request_digest,digest_version,payload,status,phase,deadline,execution_allocation_id)
            SELECT $1,$2,$3,'execute','project',$4,$5,$6,$7,$8,'queued','admitted',$9,$10
            WHERE $9>clock_timestamp() AND EXISTS(SELECT 1 FROM projects p,
                LATERAL jsonb_array_elements(p.api_tokens) t WHERE p.id=$2 AND p.status='active' AND t->>'key_id'=$4
                AND (t->>'revoked_at' IS NULL OR (t->>'revoked_at')::timestamptz>clock_timestamp())
                AND (t->>'expires_at' IS NULL OR (t->>'expires_at')::timestamptz>clock_timestamp()))
            AND EXISTS(SELECT 1 FROM allocations a JOIN hosts h ON h.id=a.host_id
                WHERE a.id=$10 AND a.lease_expires_at>clock_timestamp() AND h.status='ready'
                AND h.last_seen_at>clock_timestamp()-interval '30 seconds')")
            .bind(operation.uuid()).bind(r.project_id.uuid()).bind(r.sandbox_id.uuid()).bind(r.key_id.as_str())
            .bind(r.idempotency_key.as_str()).bind(digest.as_bytes().as_slice()).bind(DIGEST_VERSION)
            .bind(payload).bind(deadline).bind(allocation).execute(&mut *tx).await?.rows_affected();
        if changed != 1 {
            return Err(DispatchError::Conflict);
        }
        tx.commit().await?;
        self.notify_work().await;
        Ok(ExecuteAdmission::Accepted {
            operation_id: operation,
            status: "queued".into(),
        })
    }

    /// Remember lost owner authentication without inventing an outcome for
    /// work with a dispatch intent. The normal prepare path rejects only work
    /// independently proven never dispatched, even after maintenance recovers.
    pub async fn fence_unavailable_execute(&self, claim: &Claim) -> Result<(), DispatchError> {
        let mut tx = self.pool().begin().await?;
        let ctx = context(&mut tx, claim).await?;
        if ctx.op.try_get::<i32, _>("attempt_count")? == 0
            && ctx.op.try_get::<String, _>("status")? != "unknown"
            && ctx.op.try_get::<serde_json::Value, _>("attempt_receipts")? == json!([])
        {
            dispatch::fence(&mut tx, claim).await?;
            sqlx::query("UPDATE operations SET phase='execution_owner_unavailable',updated_at=clock_timestamp() WHERE id=$1")
                .bind(claim.operation_id.uuid()).execute(&mut *tx).await?;
        }
        tx.commit().await?;
        Ok(())
    }

    /// Commit exactly one dispatch intent. Later calls inspect or cancel, including
    /// unknown outcomes and calls after revocation, expiry, destroy, or host loss.
    pub async fn prepare_execute(
        &self,
        claim: &Claim,
        host: HostId,
        epoch: i64,
    ) -> Result<ExecuteAction, DispatchError> {
        let mut tx = self.pool().begin().await?;
        let ctx = context(&mut tx, claim).await?;
        let Context {
            op,
            sandbox,
            allocation_row,
            host_row,
            allocation,
            allocation_host,
            owner,
            command,
            digest,
            expected_generation,
        } = ctx;
        let dispatched = op.try_get::<i32, _>("attempt_count")? > 0
            || op.try_get::<String, _>("status")? == "unknown";
        let cancellation: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM operations
            WHERE target_operation_id=$1 AND kind='cancel' AND phase='cancel_requested'
            AND status IN ('queued','running','unknown'))",
        )
        .bind(claim.operation_id.uuid())
        .fetch_one(&mut *tx)
        .await?;
        if dispatched {
            validate_intent(&op, &owner, &digest)?;
            dispatch::fence(&mut tx, claim).await?;
            tx.commit().await?;
            return Ok(if cancellation {
                ExecuteAction::Cancel { owner, digest }
            } else {
                ExecuteAction::Inspect { owner, digest }
            });
        }
        if cancellation {
            if op.try_get::<serde_json::Value, _>("attempt_receipts")? != json!([]) {
                return Err(DispatchError::InvalidData);
            }
            // The target row is locked and has no dispatch intent. No earlier
            // claimant could have received a Dispatch action without that intent.
            dispatch::fence(&mut tx, claim).await?;
            let changed = sqlx::query(
                r#"UPDATE operations SET status='cancelled',phase='cancelled_before_dispatch',
                completed_at=clock_timestamp(),lease_expires_at=NULL,next_retry_at=NULL,error=NULL,
                result='{"dispatch_intent_absent":true}',updated_at=clock_timestamp()
                WHERE id=$1 AND claim_revision=$2 AND lease_expires_at>clock_timestamp()"#,
            )
            .bind(claim.operation_id.uuid())
            .bind(claim.revision)
            .execute(&mut *tx)
            .await?
            .rows_affected();
            if changed != 1 {
                return Err(DispatchError::LostClaim);
            }
            tx.commit().await?;
            return Ok(ExecuteAction::Rejected);
        }
        if allocation_host != host.uuid() || owner.supervisor_epoch != epoch {
            return Err(DispatchError::HostUnavailable);
        }
        let authorized: (bool,) = sqlx::query_as("SELECT p.status='active' AND o.initiator_kind='project'
            AND o.deadline>clock_timestamp() AND (s.expires_at IS NULL OR s.expires_at>clock_timestamp())
            AND EXISTS(SELECT 1 FROM jsonb_array_elements(p.api_tokens) t WHERE t->>'key_id'=o.initiator_key_id
                AND (t->>'revoked_at' IS NULL OR (t->>'revoked_at')::timestamptz>clock_timestamp())
                AND (t->>'expires_at' IS NULL OR (t->>'expires_at')::timestamptz>clock_timestamp()))
            FROM operations o JOIN projects p ON p.id=o.project_id JOIN sandboxes s ON s.id=o.sandbox_id WHERE o.id=$1")
            .bind(claim.operation_id.uuid()).fetch_one(&mut *tx).await?;
        let current = expected_generation.is_none_or(|expected| expected == owner.generation)
            && sandbox.try_get::<String, _>("desired_state")? == "running"
            && sandbox.try_get::<String, _>("observed_state")? == "running"
            && sandbox
                .try_get::<Option<uuid::Uuid>, _>("active_transition_operation_id")?
                .is_none()
            && sandbox.try_get::<Option<uuid::Uuid>, _>("current_allocation_id")?
                == Some(allocation)
            && sandbox.try_get::<i64, _>("generation")? == owner.generation
            && allocation_row.try_get::<String, _>("status")? == "running"
            && host_row.try_get::<i64, _>("supervisor_epoch")? == owner.supervisor_epoch;
        // Old queued rows may predate capacity admission. Never write their
        // first dispatch intent if retained history already exceeds the bound.
        // Dispatched/unknown work took the inspection path above.
        let rejection = if !authorized.0 {
            Some("execution_authority_expired")
        } else if !current
            || op.try_get::<Option<String>, _>("phase")?.as_deref()
                == Some("execution_owner_unavailable")
        {
            Some("execution_target_changed")
        } else if !has_capacity(&mut tx, allocation, None).await? {
            Some("execution_capacity_exhausted")
        } else {
            None
        };
        if let Some(code) = rejection {
            dispatch::fence(&mut tx, claim).await?;
            let changed = sqlx::query("UPDATE operations SET status='failed',phase='rejected_before_dispatch',completed_at=clock_timestamp(),
                lease_expires_at=NULL,next_retry_at=NULL,error=$2,updated_at=clock_timestamp()
                WHERE id=$1 AND claim_revision=$3 AND lease_expires_at>clock_timestamp()")
                .bind(claim.operation_id.uuid()).bind(json!({"code":code,"dispatch_intent_absent":true})).bind(claim.revision)
                .execute(&mut *tx).await?.rows_affected();
            if changed != 1 {
                return Err(DispatchError::LostClaim);
            }
            tx.commit().await?;
            return Ok(ExecuteAction::Rejected);
        }
        let valid: (bool,) = sqlx::query_as("SELECT h.status='ready'
            AND COALESCE(h.last_seen_at>clock_timestamp()-interval '30 seconds',false)
            AND COALESCE(a.lease_expires_at>clock_timestamp(),false) FROM hosts h JOIN allocations a ON a.host_id=h.id WHERE a.id=$1")
            .bind(allocation).fetch_one(&mut *tx).await?;
        if !valid.0 {
            return Err(DispatchError::HostUnavailable);
        }
        let mut receipt = dispatch::evidence(&owner, None, "execute_dispatch_intent");
        receipt["command_digest"] = json!(hex::encode(digest));
        dispatch::fence(&mut tx, claim).await?;
        let changed = sqlx::query("UPDATE operations SET phase='execute_dispatched',attempt_count=1,
            attempt_receipts=jsonb_build_array($2::jsonb),updated_at=clock_timestamp()
            WHERE id=$1 AND claim_revision=$3 AND lease_expires_at>clock_timestamp()
            AND deadline>clock_timestamp() AND EXISTS(SELECT 1 FROM allocations WHERE id=$4 AND lease_expires_at>clock_timestamp())
            AND EXISTS(SELECT 1 FROM projects p, LATERAL jsonb_array_elements(p.api_tokens) t
                WHERE p.id=operations.project_id AND p.status='active' AND t->>'key_id'=operations.initiator_key_id
                AND (t->>'revoked_at' IS NULL OR (t->>'revoked_at')::timestamptz>clock_timestamp())
                AND (t->>'expires_at' IS NULL OR (t->>'expires_at')::timestamptz>clock_timestamp()))
            AND EXISTS(SELECT 1 FROM sandboxes WHERE id=operations.sandbox_id
                AND (expires_at IS NULL OR expires_at>clock_timestamp()))")
            .bind(claim.operation_id.uuid()).bind(receipt).bind(claim.revision).bind(allocation)
            .execute(&mut *tx).await?.rows_affected();
        if changed != 1 {
            return Err(DispatchError::Conflict);
        }
        tx.commit().await?;
        Ok(ExecuteAction::Dispatch { owner, command })
    }
}

/// Called under the project/sandbox locks shared by admission and dispatch.
/// Every admitted command conservatively reserves a journal slot and its full
/// output limit until verified history retirement, even if it never starts. Do not
/// lock operation rows here: reconciliation locks operation before project.
/// Compaction atomically replaces the payload with a descriptor of equal budget.
async fn has_capacity(
    db: &mut PgConnection,
    allocation: uuid::Uuid,
    additional_output: Option<u64>,
) -> Result<bool, DispatchError> {
    let rows = sqlx::query(
        "SELECT o.* FROM operations o LEFT JOIN completed_allocation_history h
        ON h.allocation_id=o.execution_allocation_id AND h.domain='commands'
        WHERE o.execution_allocation_id=$1 AND o.kind='execute'
        AND (h.completed_through IS NULL OR o.id>h.completed_through) ORDER BY o.id LIMIT $2",
    )
    .bind(allocation)
    .bind((MAX_COMMANDS + 1) as i64)
    .fetch_all(db)
    .await?;
    if rows.len() + usize::from(additional_output.is_some()) > MAX_COMMANDS {
        return Ok(false);
    }
    let mut reserved = additional_output.unwrap_or(0);
    for row in &rows {
        let summary = crate::compaction::command_summary(row).map_err(|error| match error {
            crate::output::OutputError::Query(error) => DispatchError::Query(error),
            _ => DispatchError::InvalidData,
        })?;
        if summary.output_limit == 0 {
            return Err(DispatchError::InvalidData);
        }
        reserved = reserved
            .checked_add(summary.output_limit)
            .ok_or(DispatchError::InvalidData)?;
    }
    Ok(reserved <= MAX_RESERVED_OUTPUT)
}

struct Context {
    op: sqlx::postgres::PgRow,
    sandbox: sqlx::postgres::PgRow,
    allocation_row: sqlx::postgres::PgRow,
    host_row: sqlx::postgres::PgRow,
    allocation: uuid::Uuid,
    allocation_host: uuid::Uuid,
    owner: Ownership,
    command: Execute,
    digest: [u8; 32],
    expected_generation: Option<i64>,
}
async fn context(db: &mut PgConnection, claim: &Claim) -> Result<Context, DispatchError> {
    let op = sqlx::query(
        "SELECT * FROM operations WHERE id=$1 AND claim_revision=$2
            AND lease_expires_at>clock_timestamp() AND status IN ('running','unknown') FOR UPDATE",
    )
    .bind(claim.operation_id.uuid())
    .bind(claim.revision)
    .fetch_optional(&mut *db)
    .await?
    .ok_or(DispatchError::LostClaim)?;
    if op.try_get::<String, _>("kind")? != "execute" {
        return Err(DispatchError::Conflict);
    }
    let project: uuid::Uuid = op.try_get("project_id")?;
    let sandbox_id: uuid::Uuid = op.try_get("sandbox_id")?;
    let allocation: uuid::Uuid = op
        .try_get::<Option<uuid::Uuid>, _>("execution_allocation_id")?
        .ok_or(DispatchError::InvalidData)?;
    sqlx::query("SELECT id FROM projects WHERE id=$1 FOR UPDATE")
        .bind(project)
        .fetch_one(&mut *db)
        .await?;
    let sandbox = sqlx::query("SELECT * FROM sandboxes WHERE id=$1 FOR UPDATE")
        .bind(sandbox_id)
        .fetch_one(&mut *db)
        .await?;
    // Read the pinned record even after destroy releases it. Never redirect
    // an old command onto the sandbox's current allocation or a new epoch.
    let allocation_row = sqlx::query("SELECT * FROM allocations WHERE id=$1 AND sandbox_id=$2")
        .bind(allocation)
        .bind(sandbox_id)
        .fetch_one(&mut *db)
        .await?;
    let allocation_host = allocation_row.try_get::<uuid::Uuid, _>("host_id")?;
    let host_row = sqlx::query("SELECT * FROM hosts WHERE id=$1 FOR UPDATE")
        .bind(allocation_host)
        .fetch_one(&mut *db)
        .await?;
    let command: CommandInput =
        serde_json::from_value(op.try_get("payload")?).map_err(|_| DispatchError::InvalidData)?;
    command.validate().map_err(|_| DispatchError::InvalidData)?;
    let expected_generation = command.expected_generation;
    let command = command.for_operation(claim.operation_id);
    let digest = command.digest().map_err(|_| DispatchError::InvalidData)?;
    let owner = Ownership {
        host_id: HostId::from_uuid(allocation_host).to_string(),
        project_id: ProjectId::from_uuid(project).to_string(),
        sandbox_id: SandboxId::from_uuid(sandbox_id).to_string(),
        allocation_id: AllocationId::from_uuid(allocation).to_string(),
        operation_id: claim.operation_id.to_string(),
        generation: allocation_row.try_get("generation")?,
        supervisor_epoch: allocation_row.try_get("supervisor_epoch")?,
        claim_revision: claim.revision,
        claim_expires_unix_ms: i64::try_from(
            op.try_get::<OffsetDateTime, _>("lease_expires_at")?
                .unix_timestamp_nanos()
                / 1_000_000,
        )
        .map_err(|_| DispatchError::InvalidData)?,
    };

    Ok(Context {
        op,
        sandbox,
        allocation_row,
        host_row,
        allocation,
        allocation_host,
        owner,
        command,
        digest,
        expected_generation,
    })
}

pub(crate) fn validate_intent(
    op: &sqlx::postgres::PgRow,
    owner: &Ownership,
    digest: &[u8; 32],
) -> Result<(), DispatchError> {
    // A changed persisted payload is corruption, never permission to inspect
    // another command under the same public operation ID.
    let receipts: serde_json::Value = op.try_get("attempt_receipts")?;
    let recorded = receipts
        .as_array()
        .and_then(|items| items.first())
        .ok_or(DispatchError::InvalidData)?;
    let expected = dispatch::evidence(owner, None, "execute_dispatch_intent");
    if op.try_get::<i32, _>("attempt_count")? != 1
        || recorded
            .get("command_digest")
            .and_then(serde_json::Value::as_str)
            != Some(hex::encode(digest).as_str())
        || [
            "phase",
            "host_id",
            "project_id",
            "sandbox_id",
            "operation_id",
            "allocation_id",
            "generation",
            "supervisor_epoch",
        ]
        .iter()
        .any(|key| recorded.get(*key) != expected.get(*key))
    {
        return Err(DispatchError::InvalidData);
    }
    Ok(())
}

impl Store {
    /// Transport loss has no execution meaning; preserve intent and inspect again.
    pub async fn record_execute_unknown(&self, claim: &Claim) -> Result<(), DispatchError> {
        self.finish_execute_observation(claim, None, false).await
    }
    pub async fn record_execute_observation(
        &self,
        claim: &Claim,
        observation: &sandbox_protocol::supervisor::CommandObservation,
        allow_simulated: bool,
    ) -> Result<(), DispatchError> {
        self.finish_execute_observation(claim, Some(observation), allow_simulated)
            .await
    }
    async fn finish_execute_observation(
        &self,
        claim: &Claim,
        observation: Option<&sandbox_protocol::supervisor::CommandObservation>,
        allow_simulated: bool,
    ) -> Result<(), DispatchError> {
        use sandbox_protocol::guest_model::{Receipt, State};
        let mut tx = self.pool().begin().await?;
        let ctx = context(&mut tx, claim).await?;
        validate_intent(&ctx.op, &ctx.owner, &ctx.digest)?;
        let old_receipts: serde_json::Value = ctx.op.try_get("attempt_receipts")?;
        let intent = old_receipts[0].clone();
        let previous = old_receipts.get(1).cloned();
        let mut receipt: Option<Receipt> = None;
        let mut not_started = false;
        let mut simulated = None;
        if let Some(o) = observation {
            if o.simulated && !allow_simulated {
                return Err(DispatchError::SimulationDenied);
            }
            if o.ownership.as_ref() != Some(&ctx.owner)
                || o.command_digest.as_slice() != ctx.digest
                || (o.not_started && o.receipt.is_some())
            {
                return Err(DispatchError::BadEvidence);
            }
            let fresh: (bool,) = sqlx::query_as(
                "SELECT abs(extract(epoch FROM clock_timestamp())*1000-$1::bigint)<=10000",
            )
            .bind(o.observed_unix_ms)
            .fetch_one(&mut *tx)
            .await?;
            if !fresh.0 {
                return Err(DispatchError::BadEvidence);
            }
            simulated = Some(o.simulated);
            not_started = o.not_started;
            if not_started
                && previous
                    .as_ref()
                    .is_some_and(|v| v.get("guest_receipt").is_some())
            {
                return Err(DispatchError::BadEvidence);
            }
            if let Some(wire) = &o.receipt {
                let r: Receipt = wire
                    .clone()
                    .try_into()
                    .map_err(|_| DispatchError::BadEvidence)?;
                if r.operation_id != claim.operation_id
                    || r.context.allocation_id.uuid() != ctx.allocation
                    || r.context.generation != ctx.owner.generation
                    || r.digest != ctx.digest
                    || r.deadline_unix_ms != ctx.command.deadline_unix_ms
                    || r.output_limit != ctx.command.output_limit
                {
                    return Err(DispatchError::BadEvidence);
                }
                if let Some(boot) = previous
                    .as_ref()
                    .and_then(|v| v.pointer("/guest_receipt/context/boot_id"))
                    .and_then(serde_json::Value::as_str)
                    && boot != r.context.boot_id
                {
                    return Err(DispatchError::BadEvidence);
                }
                receipt = Some(r);
            }
        }
        let cancellation:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM operations WHERE target_operation_id=$1
            AND kind='cancel' AND phase='cancel_requested' AND status IN ('queued','running','unknown'))")
            .bind(claim.operation_id.uuid()).fetch_one(&mut *tx).await?;
        let (status, phase, error) = match receipt.as_ref().map(|r| r.state) {
            Some(State::Exited) if receipt.as_ref().is_some_and(|r| r.exit_code == Some(0)) => {
                ("succeeded", "exited", None)
            }
            Some(State::Exited) => ("failed", "exited", Some("command_failed")),
            Some(State::TimedOut) => ("failed", "timed_out", Some("deadline_exceeded")),
            Some(State::Cancelled) => ("cancelled", "cancelled", None),
            Some(State::LaunchIntent) => ("running", "executing", None),
            _ if not_started && cancellation => ("cancelled", "cancelled_before_start", None),
            _ if not_started => ("failed", "not_started", Some("command_not_started")),
            _ => ("unknown", "reconciling", Some("outcome_unknown")),
        };
        let terminal = matches!(status, "succeeded" | "failed" | "cancelled");
        let archive = terminal && receipt.is_some();
        // Only bounded statistics/results cross the controller, never output bytes
        // or guest-controlled reason text. The receipt cannot release VM resources.
        let result = receipt.as_ref().map(|r| {
            json!({"simulated":simulated,"exit_code":r.exit_code,"signal":r.signal,
            "stdout":r.stdout,"stderr":r.stderr,"guest_reported":true})
        });
        let error = error.map(|code| json!({"code":code,"simulated":simulated}));
        let mut history = vec![intent];
        if let Some(receipt) = receipt {
            let mut evidence = dispatch::evidence(&ctx.owner, simulated, "command_observed");
            evidence["observed_unix_ms"] = json!(observation.map(|o| o.observed_unix_ms));
            evidence["guest_receipt"] =
                serde_json::to_value(receipt).map_err(|_| DispatchError::InvalidData)?;
            history.push(evidence);
        } else if not_started {
            let mut evidence =
                dispatch::evidence(&ctx.owner, simulated, "command_fenced_before_start");
            evidence["not_started"] = json!(true);
            evidence["observed_unix_ms"] = json!(observation.map(|o| o.observed_unix_ms));
            evidence["command_digest"] = json!(hex::encode(ctx.digest));
            history.push(evidence);
        } else if let Some(previous) = previous {
            history.push(previous);
        }
        dispatch::fence(&mut tx, claim).await?;
        let changed = sqlx::query("UPDATE operations SET status=$3,phase=$4,result=$5,error=$6,attempt_receipts=$7,
            completed_at=CASE WHEN $8 THEN clock_timestamp() ELSE NULL END,lease_expires_at=NULL,
            next_retry_at=CASE WHEN $8 THEN NULL ELSE clock_timestamp()+interval '1 second' END,updated_at=clock_timestamp(),
            output_status=CASE WHEN $10 THEN 'pending' ELSE output_status END
            WHERE id=$1 AND claim_revision=$2 AND lease_expires_at>clock_timestamp()
            AND ($9::bigint IS NULL OR abs(extract(epoch FROM clock_timestamp())*1000-$9::bigint)<=10000)")
            .bind(claim.operation_id.uuid()).bind(claim.revision).bind(status).bind(phase).bind(result).bind(error)
            .bind(json!(history)).bind(terminal).bind(observation.map(|o| o.observed_unix_ms)).bind(archive)
            .execute(&mut *tx).await?.rows_affected();
        if changed != 1 {
            return Err(DispatchError::LostClaim);
        }
        tx.commit().await?;
        Ok(())
    }
}
