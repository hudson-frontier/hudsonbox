//! Durable controller ownership. A claim permits reconciliation, not blind dispatch.
//!
//! Expired work retains its status, phase, payload, and receipts. A replacement
//! owner must inspect those facts before choosing its next external action.
//! PostgreSQL time is authoritative; controller clocks never decide ownership.

use sandbox_protocol::{Id, OperationId, ProjectId, SandboxId};
use sqlx::Row;
use time::OffsetDateTime;

use crate::Store;

/// A controller only claims operation kinds it knows how to reconcile.
#[derive(Debug, Clone, Copy)]
pub enum OperationKind {
    Create,
    Execute,
    Destroy,
    Cancel,
    FileWrite,
}

impl OperationKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::Create => "create",
            Self::Execute => "execute",
            Self::Destroy => "destroy",
            Self::Cancel => "cancel",
            Self::FileWrite => "file_write",
        }
    }
}

/// Ownership and the persisted evidence the new owner must reconcile.
#[derive(Clone)]
pub struct Claim {
    pub operation_id: OperationId,
    pub project_id: ProjectId,
    pub sandbox_id: SandboxId,
    pub revision: i64,
    pub lease_expires_at: OffsetDateTime,
    /// Status before claiming: unknown must remain unknown until reconciled.
    pub previous_status: String,
    pub phase: Option<String>,
    pub payload: serde_json::Value,
    pub receipts: serde_json::Value,
    pub deadline: Option<OffsetDateTime>,
}

// Payloads can contain customer argv/environment and receipts can contain guest
// data. Log ownership metadata only, including through derived caller Debug.
impl std::fmt::Debug for Claim {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Claim")
            .field("operation_id", &self.operation_id)
            .field("revision", &self.revision)
            .finish_non_exhaustive()
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ClaimError {
    #[error("lease must be between 1 and 300 seconds")]
    InvalidLease,
    #[error("retry delay must be between 1 and 3600 seconds")]
    InvalidRetryDelay,
    #[error("operation claim is no longer owned")]
    LostClaim,
    #[error("controller storage query failed: {0}")]
    Query(#[from] sqlx::Error),
}

fn lease_seconds(seconds: u32) -> Result<i32, ClaimError> {
    if !(1..=300).contains(&seconds) {
        return Err(ClaimError::InvalidLease);
    }
    Ok(seconds as i32)
}

impl Store {
    /// Claim the next eligible operation of a supported kind (oldest first;
    /// creates round-robin across projects) without waiting on another
    /// controller's row lock. The selection and revision increment
    /// are one statement. Claiming does not increment dispatch attempts.
    pub async fn claim_next(
        &self,
        kind: OperationKind,
        seconds: u32,
    ) -> Result<Option<Claim>, ClaimError> {
        let seconds = lease_seconds(seconds)?;
        // Only the file dispatcher depends on the new ownership column.
        let file_filter = if matches!(kind, OperationKind::FileWrite) {
            "AND file_allocation_id IS NOT NULL"
        } else {
            ""
        };
        let eligible = format!(
            r"kind = $1
               AND ($1 <> 'cancel' OR phase = 'cancel_requested')
               {file_filter}
               AND status IN ('queued', 'running', 'unknown')
               AND phase IS DISTINCT FROM 'cleanup_owned_by_destroy'
               AND (lease_expires_at IS NULL OR lease_expires_at <= clock_timestamp())
               AND (next_retry_at IS NULL OR next_retry_at <= clock_timestamp())"
        );
        // Creates take turns across projects: a create's turn is its position
        // among its project's unfinished creates, including ones already
        // claimed or waiting, so one project's backlog cannot delay another
        // project's first placement. Other kinds stay oldest-first. Eligibility
        // is checked on the locked row so a concurrent claim that commits
        // first is re-evaluated, not reused.
        let candidate = if matches!(kind, OperationKind::Create) {
            format!(
                r"
                ranked AS (
                    SELECT id, created_at,
                           row_number() OVER (PARTITION BY project_id ORDER BY created_at, id) AS turn
                      FROM operations
                     WHERE kind = $1 AND status IN ('queued', 'running', 'unknown')
                       AND phase IS DISTINCT FROM 'cleanup_owned_by_destroy'
                ),
                candidate AS (
                    SELECT o.id, o.status AS previous_status
                      FROM operations o JOIN ranked r ON r.id = o.id
                     WHERE {eligible}
                     ORDER BY r.turn, r.created_at, r.id
                     FOR UPDATE OF o SKIP LOCKED
                     LIMIT 1
                )"
            )
        } else {
            format!(
                r"
                candidate AS (
                    SELECT id, status AS previous_status
                      FROM operations
                     WHERE {eligible}
                     ORDER BY created_at, id
                     FOR UPDATE SKIP LOCKED
                     LIMIT 1
                )"
            )
        };
        let query = format!(
            r"
            WITH {candidate}
            UPDATE operations AS o
               SET claim_revision = o.claim_revision + 1,
                   lease_expires_at = clock_timestamp() + make_interval(secs => $2),
                   status = CASE WHEN o.status = 'queued' THEN 'running' ELSE o.status END,
                   next_retry_at = NULL,
                   updated_at = clock_timestamp()
              FROM candidate AS c
             WHERE o.id = c.id
            RETURNING o.*, c.previous_status
            ",
        );
        let row = sqlx::query(&query)
            .bind(kind.as_str())
            .bind(seconds)
            .fetch_optional(self.pool())
            .await?;

        row.map(|r| {
            Ok(Claim {
                operation_id: OperationId::from_uuid(r.try_get("id")?),
                project_id: ProjectId::from_uuid(r.try_get("project_id")?),
                sandbox_id: SandboxId::from_uuid(r.try_get("sandbox_id")?),
                revision: r.try_get("claim_revision")?,
                lease_expires_at: r.try_get("lease_expires_at")?,
                previous_status: r.try_get("previous_status")?,
                phase: r.try_get("phase")?,
                payload: r.try_get("payload")?,
                receipts: r.try_get("attempt_receipts")?,
                deadline: r.try_get("deadline")?,
            })
        })
        .transpose()
    }

    /// Renew only a currently live claim. An expired owner cannot resurrect
    /// itself even if no replacement has claimed the operation yet.
    pub async fn renew_claim(
        &self,
        claim: &Claim,
        seconds: u32,
    ) -> Result<OffsetDateTime, ClaimError> {
        let seconds = lease_seconds(seconds)?;
        let renewed: Option<(OffsetDateTime,)> = sqlx::query_as(
            r"
            UPDATE operations
               SET lease_expires_at = clock_timestamp() + make_interval(secs => $3),
                   updated_at = clock_timestamp()
             WHERE id = $1 AND claim_revision = $2
               AND status IN ('running', 'unknown')
               AND lease_expires_at > clock_timestamp()
            RETURNING lease_expires_at
            ",
        )
        .bind(claim.operation_id.uuid())
        .bind(claim.revision)
        .bind(seconds)
        .fetch_optional(self.pool())
        .await?;
        renewed.map(|(expiry,)| expiry).ok_or(ClaimError::LostClaim)
    }

    /// Release controller ownership until a bounded retry time. VM ownership,
    /// reservations, outcomes, and receipts are unchanged. This does not cancel
    /// work or make any capacity available.
    pub async fn defer_claim(&self, claim: &Claim, seconds: u32) -> Result<(), ClaimError> {
        if !(1..=3600).contains(&seconds) {
            return Err(ClaimError::InvalidRetryDelay);
        }
        let affected = sqlx::query(
            r"
            UPDATE operations
               SET lease_expires_at = NULL,
                   next_retry_at = clock_timestamp() + make_interval(secs => $3),
                   updated_at = clock_timestamp()
             WHERE id = $1 AND claim_revision = $2
               AND status IN ('running', 'unknown')
               AND lease_expires_at > clock_timestamp()
            ",
        )
        .bind(claim.operation_id.uuid())
        .bind(claim.revision)
        .bind(seconds as i32)
        .execute(self.pool())
        .await?
        .rows_affected();
        if affected == 0 {
            return Err(ClaimError::LostClaim);
        }
        Ok(())
    }
}
