//! Read-only, snapshot-consistent recovery inventory. Never repairs authority.
use crate::{
    Store, output,
    output_cleanup::{self, CleanupCompletion, CleanupManifest},
    uploads::{self, cleanup::SourceCleanupManifest},
};
use sandbox_protocol::{
    file_sources::{SourcePlan, SourceRef, SourceRetirement},
    output::{OutputPlan, OutputRef, OutputRetirement},
};
use serde_json::Value;
use sqlx::{PgConnection, Postgres, Row, Transaction, postgres::PgRow};
use time::OffsetDateTime;
use uuid::Uuid;

const PAGE_ROWS: i64 = 16;

// Avoid a nullable-cursor OR in subsequent pages: a generic prepared plan must
// retain the primary-key range as an index condition, not rescan old prefixes.
fn page_query(sources: bool, after: bool) -> &'static str {
    match (sources, after) {
        (true, false) => "SELECT * FROM file_uploads WHERE $1::uuid IS NULL ORDER BY operation_id LIMIT $2",
        (true, true) => "SELECT * FROM file_uploads WHERE operation_id>$1 ORDER BY operation_id LIMIT $2",
        (false, false) =>
            "SELECT o.*,c.manifest AS audit_manifest,c.receipt AS audit_receipt,c.completed_at AS audit_completed_at,c.eligible_at AS audit_eligible_at
             FROM operations o LEFT JOIN output_cleanup c ON c.operation_id=o.id
             WHERE $1::uuid IS NULL AND (o.output_status<>'none' OR c.operation_id IS NOT NULL) ORDER BY o.id LIMIT $2",
        (false, true) =>
            "SELECT o.*,c.manifest AS audit_manifest,c.receipt AS audit_receipt,c.completed_at AS audit_completed_at,c.eligible_at AS audit_eligible_at
             FROM operations o LEFT JOIN output_cleanup c ON c.operation_id=o.id
             WHERE o.id>$1 AND (o.output_status<>'none' OR c.operation_id IS NOT NULL) ORDER BY o.id LIMIT $2",
    }
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("storage audit requires the matching complete migration ledger")]
    Schema,
    #[error("storage audit database query failed")]
    Query,
    #[error("storage audit found invalid retained evidence")]
    Evidence,
    #[error("storage audit requires reconciliation of an unresolved upload")]
    Unresolved,
    #[error("simulated storage evidence requires explicit development opt-in")]
    SimulationDenied,
    #[error("storage audit must restart after a failed page")]
    Failed,
}
impl From<sqlx::Error> for Error {
    fn from(_: sqlx::Error) -> Self {
        Self::Query
    }
}

/// The owner in each entry was independently checked against allocation and
/// operation records. Entries remain private operator data, never access tokens.
#[derive(Debug)]
pub enum Entry {
    Output(OutputRef),
    RetiredOutput {
        plan: OutputPlan,
        receipt: OutputRetirement,
    },
    Source(SourceRef),
    RetiredSource {
        plan: SourcePlan,
        receipt: SourceRetirement,
    },
}

#[derive(Debug)]
pub struct Audit {
    transaction: Transaction<'static, Postgres>,
    cursor: Option<Uuid>,
    sources: bool,
    finished: bool,
    failed: bool,
    allow_simulated: bool,
}
impl Store {
    /// Start one repeatable-read, read-only snapshot. Keep the installation
    /// offline and storage quiescent; a database snapshot cannot fence writers
    /// or establish independent physical release. No migrations are applied.
    pub async fn storage_audit(&self, allow_simulated: bool) -> Result<Audit, Error> {
        let mut transaction = self.pool().begin().await?;
        sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
            .execute(&mut *transaction)
            .await?;
        sqlx::query("SET LOCAL statement_timeout='2s'")
            .execute(&mut *transaction)
            .await?;
        let expected: Vec<_> = crate::MIGRATOR.iter().collect();
        let ledger = sqlx::query(
            "SELECT version,checksum,success FROM _sqlx_migrations ORDER BY version LIMIT $1",
        )
        .bind(expected.len() as i64 + 1)
        .fetch_all(&mut *transaction)
        .await?;
        if ledger.len() != expected.len() {
            return Err(Error::Schema);
        }
        for (row, migration) in ledger.iter().zip(expected) {
            if row.try_get::<i64, _>("version")? != migration.version
                || row.try_get::<Vec<u8>, _>("checksum")?.as_slice() != migration.checksum.as_ref()
                || !row.try_get::<bool, _>("success")?
            {
                return Err(Error::Schema);
            }
        }
        Ok(Audit {
            transaction,
            cursor: None,
            sources: false,
            finished: false,
            failed: false,
            allow_simulated,
        })
    }
}
impl Audit {
    /// At most 32 typed objects per page. Keyset pagination never drops history
    /// because of a row limit. Empty means the complete snapshot was scanned.
    /// After any error this handle cannot skip the bad row and report success.
    pub async fn next_page(&mut self) -> Result<Vec<Entry>, Error> {
        if self.failed {
            return Err(Error::Failed);
        }
        self.failed = true;
        let result = self.page().await;
        if result.is_ok() {
            self.failed = false;
        }
        result
    }
    async fn page(&mut self) -> Result<Vec<Entry>, Error> {
        while !self.finished {
            let rows = sqlx::query(page_query(self.sources, self.cursor.is_some()))
                .bind(self.cursor)
                .bind(PAGE_ROWS)
                .fetch_all(&mut *self.transaction)
                .await?;
            if rows.is_empty() {
                if self.sources {
                    self.finished = true;
                } else {
                    self.sources = true;
                    self.cursor = None;
                }
                continue;
            }
            let mut entries = Vec::with_capacity(rows.len() * 2);
            for row in &rows {
                if self.sources {
                    entries.push(source(&mut self.transaction, row).await?);
                    self.cursor = Some(row.try_get("operation_id")?);
                } else {
                    entries
                        .extend(outputs(&mut self.transaction, row, self.allow_simulated).await?);
                    self.cursor = Some(row.try_get("id")?);
                }
            }
            if !entries.is_empty() {
                return Ok(entries);
            }
        }
        Ok(Vec::new())
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;
    use sandbox_protocol::Id;

    #[sqlx::test(migrator = "crate::MIGRATOR")]
    async fn storage_audit_generic_plans_bound_the_scan_by_the_cursor(pool: sqlx::PgPool) {
        let mut transaction = pool.begin().await.unwrap();
        for setting in [
            "SET LOCAL plan_cache_mode=force_generic_plan",
            "SET LOCAL enable_seqscan=off",
            "SET LOCAL enable_bitmapscan=off",
        ] {
            sqlx::query(setting)
                .execute(&mut *transaction)
                .await
                .unwrap();
        }
        for sources in [false, true] {
            let query = format!("EXPLAIN (COSTS OFF) {}", page_query(sources, true));
            let rows = sqlx::query(&query)
                .bind(sandbox_protocol::OperationId::generate().uuid())
                .bind(PAGE_ROWS)
                .fetch_all(&mut *transaction)
                .await
                .unwrap();
            let condition = if sources {
                "Index Cond: (operation_id >"
            } else {
                "Index Cond: (id >"
            };
            assert!(
                rows.iter()
                    .any(|row| row.get::<String, _>(0).contains(condition)),
                "cursor must constrain the index scan"
            );
        }
    }
}

fn parse<T: serde::de::DeserializeOwned>(value: Value) -> Result<T, Error> {
    serde_json::from_value(value).map_err(|_| Error::Evidence)
}
async fn outputs(
    db: &mut PgConnection,
    row: &PgRow,
    allow_simulated: bool,
) -> Result<Vec<Entry>, Error> {
    let evidence = output::evidence(db, row)
        .await
        .map_err(|_| Error::Evidence)?;
    if evidence.simulated && !allow_simulated {
        return Err(Error::SimulationDenied);
    }
    let completed = row
        .try_get::<Option<OffsetDateTime>, _>("audit_completed_at")?
        .is_some();
    let current = output::saved(row, &evidence).map_err(|_| Error::Evidence)?;
    if completed {
        // Compaction retains these immutable descriptors; it cannot turn a
        // completed cleanup into permission to omit its storage fence.
        let manifest: CleanupManifest = parse(row.try_get("audit_manifest")?)?;
        let receipt: CleanupCompletion = parse(row.try_get("audit_receipt")?)?;
        manifest.ticket.validate().map_err(|_| Error::Evidence)?;
        let eligible = OffsetDateTime::from_unix_timestamp_nanos(
            i128::from(manifest.ticket.delete_after_unix_ms) * 1_000_000,
        )
        .map_err(|_| Error::Evidence)?;
        if row.try_get::<Option<OffsetDateTime>, _>("audit_eligible_at")? != Some(eligible)
            || row.try_get::<OffsetDateTime, _>("audit_completed_at")? < eligible
            || manifest.version != 1
            || manifest.ticket.owner != evidence.owner
            || manifest.ticket.output_limit != evidence.receipt.output_limit
            || manifest.simulated != evidence.simulated
            || output_cleanup::manifest(db, row)
                .await
                .map_err(|_| Error::Evidence)?
                != manifest
        {
            return Err(Error::Evidence);
        }
        receipt.validate(&manifest).map_err(|_| Error::Evidence)?;
        return match (manifest.plans, receipt) {
            (None, CleanupCompletion::NoUploadsAuthorized) => Ok(Vec::new()),
            (Some(plans), CleanupCompletion::Retired { stdout, stderr }) => Ok(vec![
                Entry::RetiredOutput {
                    plan: plans.stdout,
                    receipt: *stdout,
                },
                Entry::RetiredOutput {
                    plan: plans.stderr,
                    receipt: *stderr,
                },
            ]),
            _ => Err(Error::Evidence),
        };
    }
    if row.try_get::<Option<Value>, _>("audit_receipt")?.is_some() {
        return Err(Error::Evidence);
    }
    if let Some(raw) = row.try_get::<Option<Value>, _>("audit_manifest")? {
        let saved: CleanupManifest = parse(raw)?;
        let eligible = OffsetDateTime::from_unix_timestamp_nanos(
            i128::from(saved.ticket.delete_after_unix_ms) * 1_000_000,
        )
        .map_err(|_| Error::Evidence)?;
        if row.try_get::<Option<OffsetDateTime>, _>("audit_eligible_at")? != Some(eligible)
            || output_cleanup::manifest(db, row)
                .await
                .map_err(|_| Error::Evidence)?
                != saved
        {
            return Err(Error::Evidence);
        }
    }
    let Some(work) = current else {
        // Pending archival (or a compacted pending response) never had a ticket
        // and therefore could not authorize any external storage upload.
        if row.try_get::<Value, _>("output_refs")? != serde_json::json!([])
            || row.try_get::<Option<Value>, _>("output_plan")?.is_some()
        {
            return Err(Error::Evidence);
        }
        return Ok(Vec::new());
    };
    if work.plans.is_none() {
        if row.try_get::<Value, _>("output_refs")? != serde_json::json!([]) {
            return Err(Error::Evidence);
        }
        return Ok(Vec::new());
    }
    let manifest = output_cleanup::manifest(db, row)
        .await
        .map_err(|_| Error::Evidence)?;
    let refs = manifest.references.ok_or(Error::Unresolved)?;
    Ok(vec![Entry::Output(refs.stdout), Entry::Output(refs.stderr)])
}

async fn source(db: &mut PgConnection, row: &PgRow) -> Result<Entry, Error> {
    let operation = sqlx::query("SELECT * FROM operations WHERE id=$1")
        .bind(row.try_get::<Uuid, _>("operation_id")?)
        .fetch_one(&mut *db)
        .await?;
    let (plan, reference) = uploads::source_identity_for_audit(db, &operation, row)
        .await
        .map_err(|_| Error::Evidence)?;
    let retired = row
        .try_get::<Option<OffsetDateTime>, _>("source_retired_at")?
        .is_some();
    if let Some(raw) = row.try_get::<Option<Value>, _>("source_cleanup_manifest")? {
        let saved: SourceCleanupManifest = parse(raw)?;
        if saved.version != 1 || saved.plan != plan || saved.selected != reference {
            return Err(Error::Evidence);
        }
    }
    if retired {
        let manifest: SourceCleanupManifest = parse(row.try_get("source_cleanup_manifest")?)?;
        let receipt: SourceRetirement = parse(row.try_get("source_retirement")?)?;
        receipt.validate(&plan).map_err(|_| Error::Evidence)?;
        let retired_at: OffsetDateTime = row.try_get("source_retired_at")?;
        let eligible = OffsetDateTime::from_unix_timestamp_nanos(
            i128::from(plan.delete_after_unix_ms) * 1_000_000,
        )
        .map_err(|_| Error::Evidence)?;
        if retired_at < eligible
            || operation.try_get::<OffsetDateTime, _>("deadline")?
                > retired_at - time::Duration::minutes(5)
            || manifest.version != 1
            || manifest.plan != plan
            || manifest.selected != reference
            || reference
                .as_ref()
                .is_some_and(|r| receipt.previous.as_ref() != Some(r))
        {
            return Err(Error::Evidence);
        }
        Ok(Entry::RetiredSource { plan, receipt })
    } else {
        if row
            .try_get::<Option<Value>, _>("source_retirement")?
            .is_some()
        {
            return Err(Error::Evidence);
        }
        Ok(Entry::Source(reference.ok_or(Error::Unresolved)?))
    }
}
