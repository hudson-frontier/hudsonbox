//! Durable, fenced cleanup inventory and metadata-only completion receipts.
//! Trusted cleanup workers verify storage retirement; this module validates
//! its binding to the frozen attempt and preserves execution history.
use crate::{
    Store,
    output::{self, OutputError},
};
use sandbox_protocol::{
    Id, OperationId,
    output::{OutputPlans, OutputRefs, OutputRetirement, OutputTicket},
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sqlx::{PgConnection, Row, postgres::PgRow};
use time::OffsetDateTime;

#[derive(Debug, Clone)]
pub struct CleanupClaim {
    pub operation_id: OperationId,
    pub revision: i64,
    pub lease_expires_at: OffsetDateTime,
}

/// All known upload authority for one operation. Plans without references are
/// possible orphan objects; no plans means no upload was authorized. Neither
/// case is proof of absence in storage. This record is private service state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CleanupManifest {
    pub version: u32,
    pub ticket: OutputTicket,
    pub plans: Option<OutputPlans>,
    pub references: Option<OutputRefs>,
    pub simulated: bool,
}

/// Accepted only from a trusted cleanup worker after storage verification.
/// Never deserialize a customer request into cleanup authority.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum CleanupCompletion {
    NoUploadsAuthorized,
    Retired {
        stdout: Box<OutputRetirement>,
        stderr: Box<OutputRetirement>,
    },
}
impl CleanupCompletion {
    pub(crate) fn validate(&self, manifest: &CleanupManifest) -> Result<(), OutputError> {
        match (self, &manifest.plans) {
            (Self::NoUploadsAuthorized, None) if manifest.references.is_none() => Ok(()),
            (Self::Retired { stdout, stderr }, Some(plans)) => {
                stdout.validate(&plans.stdout)?;
                stderr.validate(&plans.stderr)?;
                if manifest.references.as_ref().is_some_and(|refs| {
                    stdout.previous.as_ref() != Some(&refs.stdout)
                        || stderr.previous.as_ref() != Some(&refs.stderr)
                }) {
                    return Err(OutputError::BadEvidence);
                }
                Ok(())
            }
            _ => Err(OutputError::BadEvidence),
        }
    }
}

#[derive(Debug)]
pub enum CleanupPreparation {
    /// Retention ended, but the persisted cleanup grace has not elapsed. The
    /// lease is released and this row cannot be claimed before this timestamp.
    Waiting { eligible_at: OffsetDateTime },
    /// Eligibility is not deletion proof. Reuse exactly this manifest after
    /// failures; never mint another upload attempt or execute the command.
    Ready { manifest: Box<CleanupManifest> },
}

fn lease(seconds: u32) -> Result<f64, OutputError> {
    if !(1..=300).contains(&seconds) {
        return Err(OutputError::InvalidPolicy);
    }
    Ok(f64::from(seconds))
}

pub(crate) async fn manifest(
    db: &mut PgConnection,
    row: &PgRow,
) -> Result<CleanupManifest, OutputError> {
    let status: String = row.try_get("output_status")?;
    if !matches!(status.as_str(), "uploading" | "published" | "expired") {
        return Err(OutputError::Corrupt);
    }
    let evidence = output::evidence(db, row).await?;
    let work = output::saved(row, &evidence)?.ok_or(OutputError::Corrupt)?;
    let raw: Value = row.try_get("output_refs")?;
    let items = raw.as_array().ok_or(OutputError::Corrupt)?;
    let references = match items.len() {
        0 if status != "published" => None,
        2 if status != "uploading" => {
            let refs = OutputRefs {
                stdout: serde_json::from_value(items[0].clone())
                    .map_err(|_| OutputError::Corrupt)?,
                stderr: serde_json::from_value(items[1].clone())
                    .map_err(|_| OutputError::Corrupt)?,
            };
            refs.validate(&evidence.owner, evidence.receipt.output_limit)
                .map_err(|_| OutputError::Corrupt)?;
            if work.plans.as_ref() != Some(&refs.plans()) {
                return Err(OutputError::Corrupt);
            }
            Some(refs)
        }
        _ => return Err(OutputError::Corrupt),
    };
    Ok(CleanupManifest {
        version: 1,
        ticket: work.ticket,
        plans: work.plans,
        references,
        simulated: work.simulated,
    })
}

/// Compaction may remove the original command only after revalidating the
/// completed retirement against all retained execution/publication evidence.
pub(crate) async fn validate_completed(
    db: &mut PgConnection,
    row: &PgRow,
) -> Result<(), OutputError> {
    let inventory=sqlx::query("SELECT manifest,receipt,eligible_at FROM output_cleanup WHERE operation_id=$1 AND completed_at IS NOT NULL FOR UPDATE")
        .bind(row.try_get::<uuid::Uuid,_>("id")?).fetch_optional(&mut *db).await?.ok_or(OutputError::Corrupt)?;
    let current = manifest(db, row).await?;
    let saved: CleanupManifest =
        serde_json::from_value(inventory.try_get("manifest")?).map_err(|_| OutputError::Corrupt)?;
    let receipt: CleanupCompletion =
        serde_json::from_value(inventory.try_get("receipt")?).map_err(|_| OutputError::Corrupt)?;
    let eligible = OffsetDateTime::from_unix_timestamp_nanos(
        i128::from(current.ticket.delete_after_unix_ms) * 1_000_000,
    )
    .map_err(|_| OutputError::Corrupt)?;
    if current != saved || inventory.try_get::<OffsetDateTime, _>("eligible_at")? != eligible {
        return Err(OutputError::Corrupt);
    }
    receipt.validate(&current)
}

impl Store {
    /// Record both streams under the same live cleanup claim. A lost commit
    /// acknowledgement can be reconciled by repeating this exact receipt with
    /// the same revision; a replacement worker otherwise recovers the markers.
    /// This does not verify S3 itself or mutate command/VM lifecycle state.
    pub async fn complete_output_cleanup(
        &self,
        claim: &CleanupClaim,
        receipt: &CleanupCompletion,
        allow_simulated: bool,
    ) -> Result<(), OutputError> {
        let mut tx = self.pool().begin().await?;
        let row = sqlx::query("SELECT * FROM operations WHERE id=$1 FOR UPDATE")
            .bind(claim.operation_id.uuid())
            .fetch_optional(&mut *tx)
            .await?
            .ok_or(OutputError::LostClaim)?;
        let inventory = sqlx::query("SELECT * FROM output_cleanup WHERE operation_id=$1
            AND claim_revision=$2 AND (lease_expires_at>clock_timestamp() OR completed_at IS NOT NULL) FOR UPDATE")
            .bind(claim.operation_id.uuid()).bind(claim.revision).fetch_optional(&mut *tx).await?.ok_or(OutputError::LostClaim)?;
        let current = manifest(&mut tx, &row).await?;
        let saved: CleanupManifest = serde_json::from_value(
            inventory
                .try_get::<Option<Value>, _>("manifest")?
                .ok_or(OutputError::BadEvidence)?,
        )
        .map_err(|_| OutputError::Corrupt)?;
        let eligible_at = OffsetDateTime::from_unix_timestamp_nanos(
            i128::from(current.ticket.delete_after_unix_ms) * 1_000_000,
        )
        .map_err(|_| OutputError::Corrupt)?;
        if saved != current
            || row.try_get::<String, _>("output_status")? != "expired"
            || inventory.try_get::<Option<OffsetDateTime>, _>("eligible_at")? != Some(eligible_at)
        {
            return Err(OutputError::Corrupt);
        }
        if current.simulated && !allow_simulated {
            return Err(OutputError::SimulationDenied);
        }
        receipt.validate(&current)?;
        if inventory
            .try_get::<Option<OffsetDateTime>, _>("completed_at")?
            .is_some()
        {
            if inventory.try_get::<Option<Value>, _>("receipt")? != Some(json!(receipt)) {
                return Err(OutputError::BadEvidence);
            }
            tx.commit().await?;
            return Ok(());
        }
        let n = sqlx::query("UPDATE output_cleanup SET completed_at=clock_timestamp(),receipt=$3,
            lease_expires_at=NULL,next_retry_at=NULL WHERE operation_id=$1 AND claim_revision=$2
            AND lease_expires_at>clock_timestamp() AND eligible_at<=clock_timestamp() AND completed_at IS NULL")
            .bind(claim.operation_id.uuid()).bind(claim.revision).bind(json!(receipt))
            .execute(&mut *tx).await?.rows_affected();
        if n != 1 {
            return Err(OutputError::LostClaim);
        }
        tx.commit().await?;
        Ok(())
    }

    /// Bounded discovery, including unpublished attempts and deleting projects.
    /// Queue identity is unique and durable, so retries cannot duplicate work.
    /// This does not parse or trust metadata, expire output, or delete bytes.
    pub async fn enqueue_expired_output(&self, limit: u32) -> Result<u64, OutputError> {
        if !(1..=100).contains(&limit) {
            return Err(OutputError::InvalidPolicy);
        }
        Ok(sqlx::query(
            "INSERT INTO output_cleanup(operation_id)
            SELECT o.id FROM operations o
            WHERE o.output_status IN ('uploading','published','expired')
            AND o.output_ticket IS NOT NULL AND o.output_expires_at<=clock_timestamp()
            AND NOT EXISTS(SELECT 1 FROM output_cleanup c WHERE c.operation_id=o.id)
            ORDER BY o.output_expires_at,o.id LIMIT $1
            ON CONFLICT(operation_id) DO NOTHING",
        )
        .bind(i64::from(limit))
        .execute(self.pool())
        .await?
        .rows_affected())
    }

    /// A claim authorizes metadata preparation only. Expired/replaced workers
    /// cannot change the saved inventory. Invalid records can be deferred
    /// independently so they do not starve other expired operations.
    pub async fn claim_output_cleanup(
        &self,
        seconds: u32,
    ) -> Result<Option<CleanupClaim>, OutputError> {
        let row = sqlx::query(
            "WITH candidate AS (SELECT operation_id FROM output_cleanup
            WHERE completed_at IS NULL AND (lease_expires_at IS NULL OR lease_expires_at<=clock_timestamp())
            AND (next_retry_at IS NULL OR next_retry_at<=clock_timestamp())
            AND (eligible_at IS NULL OR eligible_at<=clock_timestamp())
            ORDER BY created_at,operation_id FOR UPDATE SKIP LOCKED LIMIT 1)
            UPDATE output_cleanup c SET claim_revision=c.claim_revision+1,
            lease_expires_at=clock_timestamp()+make_interval(secs=>$1),next_retry_at=NULL
            FROM candidate q WHERE c.operation_id=q.operation_id
            RETURNING c.operation_id,c.claim_revision,c.lease_expires_at",
        )
        .bind(lease(seconds)?)
        .fetch_optional(self.pool())
        .await?;
        row.map(|r| {
            Ok(CleanupClaim {
                operation_id: OperationId::from_uuid(r.try_get("operation_id")?),
                revision: r.try_get("claim_revision")?,
                lease_expires_at: r.try_get("lease_expires_at")?,
            })
        })
        .transpose()
    }

    /// Atomically preserve the exact ticket, plans and selected references and
    /// revoke publication. Original execution ownership is independently
    /// reconstructed, even if the VM was destroyed or its host epoch changed.
    pub async fn prepare_output_cleanup(
        &self,
        claim: &CleanupClaim,
    ) -> Result<CleanupPreparation, OutputError> {
        let mut tx = self.pool().begin().await?;
        // Operation first, cleanup second. No project-status check: deletion
        // and suspension must not prevent reclaiming already-expired output.
        let row = sqlx::query("SELECT * FROM operations WHERE id=$1 FOR UPDATE")
            .bind(claim.operation_id.uuid())
            .fetch_optional(&mut *tx)
            .await?
            .ok_or(OutputError::LostClaim)?;
        let inventory = sqlx::query(
            "SELECT * FROM output_cleanup WHERE operation_id=$1
            AND claim_revision=$2 AND lease_expires_at>clock_timestamp() AND completed_at IS NULL FOR UPDATE",
        )
        .bind(claim.operation_id.uuid())
        .bind(claim.revision)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or(OutputError::LostClaim)?;
        let manifest = manifest(&mut tx, &row).await?;
        let eligible_at = OffsetDateTime::from_unix_timestamp_nanos(
            i128::from(manifest.ticket.delete_after_unix_ms) * 1_000_000,
        )
        .map_err(|_| OutputError::Corrupt)?;
        if let Some(saved) = inventory.try_get::<Option<Value>, _>("manifest")? {
            let saved: CleanupManifest =
                serde_json::from_value(saved).map_err(|_| OutputError::Corrupt)?;
            if saved != manifest
                || inventory.try_get::<Option<OffsetDateTime>, _>("eligible_at")?
                    != Some(eligible_at)
            {
                return Err(OutputError::Corrupt);
            }
        }
        // Recheck time after every lock wait and metadata lookup. These writes
        // roll back together if the cleanup claim expired while preparing.
        let n = sqlx::query(
            "UPDATE operations SET output_status='expired',
            output_claim_revision=output_claim_revision+CASE WHEN output_status<>'expired'
                OR output_lease_expires_at IS NOT NULL THEN 1 ELSE 0 END,
            output_lease_expires_at=NULL,output_next_retry_at=NULL
            WHERE id=$1 AND output_expires_at<=clock_timestamp()",
        )
        .bind(claim.operation_id.uuid())
        .execute(&mut *tx)
        .await?
        .rows_affected();
        if n != 1 {
            return Err(OutputError::BadEvidence);
        }
        let (waiting,): (bool,) = sqlx::query_as(
            "UPDATE output_cleanup SET manifest=$3,eligible_at=$4,
            lease_expires_at=CASE WHEN $4>clock_timestamp() THEN NULL ELSE lease_expires_at END
            WHERE operation_id=$1 AND claim_revision=$2 AND lease_expires_at>clock_timestamp()
            RETURNING lease_expires_at IS NULL",
        )
        .bind(claim.operation_id.uuid())
        .bind(claim.revision)
        .bind(json!(manifest))
        .bind(eligible_at)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or(OutputError::LostClaim)?;
        tx.commit().await?;
        Ok(if waiting {
            CleanupPreparation::Waiting { eligible_at }
        } else {
            CleanupPreparation::Ready {
                manifest: Box::new(manifest),
            }
        })
    }

    pub async fn defer_output_cleanup(
        &self,
        claim: &CleanupClaim,
        seconds: u32,
    ) -> Result<(), OutputError> {
        if !(1..=3600).contains(&seconds) {
            return Err(OutputError::InvalidPolicy);
        }
        let n = sqlx::query(
            "UPDATE output_cleanup SET lease_expires_at=NULL,
            next_retry_at=clock_timestamp()+make_interval(secs=>$3)
            WHERE operation_id=$1 AND claim_revision=$2 AND lease_expires_at>clock_timestamp()",
        )
        .bind(claim.operation_id.uuid())
        .bind(claim.revision)
        .bind(f64::from(seconds))
        .execute(self.pool())
        .await?
        .rows_affected();
        if n != 1 {
            return Err(OutputError::LostClaim);
        }
        Ok(())
    }
}
