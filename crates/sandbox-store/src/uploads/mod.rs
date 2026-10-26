//! Durable single-PUT upload admission and fenced controller progress.
mod admission;
pub mod cleanup;
mod dispatch;
mod observe;
use crate::{claims::Claim, dispatch::DispatchError};
pub use admission::{UploadAdmission, UploadCommand};
pub use dispatch::UploadAction;
use sandbox_protocol::{
    Id, OperationId, RequestDigest, SandboxId,
    file_sources::{SourceOwner, SourcePlan, SourceRef},
    supervisor::{FileRequest, Ownership},
    supervisor_files::FileRecord,
};
use serde::{Deserialize, Serialize};
use sqlx::{PgConnection, Row, postgres::PgRow};
use time::OffsetDateTime;

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct UploadInput {
    pub path: String,
    pub size: u64,
    pub sha256: [u8; 32],
    pub mode: u32,
}
impl std::fmt::Debug for UploadInput {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UploadInput")
            .field("size", &self.size)
            .finish_non_exhaustive()
    }
}
impl UploadInput {
    pub fn for_operation(&self, id: OperationId) -> sandbox_protocol::files::Upload {
        sandbox_protocol::files::Upload {
            operation_id: id,
            path: self.path.clone(),
            size: self.size,
            sha256: self.sha256,
            mode: self.mode,
        }
    }
    pub fn validate(&self) -> bool {
        self.for_operation(OperationId::generate())
            .validate()
            .is_ok()
    }
    fn from_plan(p: &SourcePlan) -> Self {
        Self {
            path: p.upload.path.clone(),
            size: p.upload.size,
            sha256: p.upload.sha256,
            mode: p.upload.mode,
        }
    }
    fn digest(&self, id: SandboxId) -> Result<RequestDigest, DispatchError> {
        RequestDigest::compute("PUT", &format!("/v1/sandboxes/{id}/files"), self)
            .map_err(|_| DispatchError::InvalidData)
    }
}
struct Context {
    file: PgRow,
    plan: SourcePlan,
    source: Option<SourceRef>,
    record: Option<FileRecord>,
    owner: Ownership,
}
impl Context {
    fn request(&self) -> FileRequest {
        FileRequest {
            ownership: Some(self.owner.clone()),
            upload: Some((&self.plan.upload).into()),
        }
    }
    fn begun(&self) -> Result<bool, DispatchError> {
        Ok(self.file.try_get("begin_requested")?)
    }
    fn flag(&self, key: &str) -> Result<bool, DispatchError> {
        Ok(self.file.try_get(key)?)
    }
}
async fn context(db: &mut PgConnection, claim: &Claim) -> Result<Context, DispatchError> {
    // Recovery after a timed-out RPC must also have bounded database lock waits.
    sqlx::query("SET LOCAL statement_timeout='2s'")
        .execute(&mut *db)
        .await?;
    let op=sqlx::query("SELECT * FROM operations WHERE id=$1 AND claim_revision=$2 AND lease_expires_at>clock_timestamp() AND status IN ('running','unknown') FOR UPDATE")
 .bind(claim.operation_id.uuid()).bind(claim.revision).fetch_optional(&mut *db).await?.ok_or(DispatchError::LostClaim)?;
    if op.try_get::<String, _>("kind")? != "file_write" {
        return Err(DispatchError::Conflict);
    }
    let project: uuid::Uuid = op.try_get("project_id")?;
    let sandbox: uuid::Uuid = op.try_get("sandbox_id")?;
    sqlx::query("SELECT id FROM projects WHERE id=$1 FOR UPDATE")
        .bind(project)
        .fetch_one(&mut *db)
        .await?;
    sqlx::query("SELECT id FROM sandboxes WHERE id=$1 FOR UPDATE")
        .bind(sandbox)
        .fetch_one(&mut *db)
        .await?;
    let file=sqlx::query("SELECT * FROM file_uploads WHERE operation_id=$1 AND project_id=$2 AND sandbox_id=$3 FOR UPDATE")
 .bind(claim.operation_id.uuid()).bind(project).bind(sandbox).fetch_optional(&mut *db).await?.ok_or(DispatchError::InvalidData)?;
    let (plan, source) = source_identity(db, &op, &file).await?;
    let expected = &plan.owner;
    sqlx::query("SELECT id FROM hosts WHERE id=$1 FOR SHARE")
        .bind(expected.scope.host_id.uuid())
        .fetch_one(&mut *db)
        .await?;
    let record: Option<FileRecord> = file
        .try_get::<Option<serde_json::Value>, _>("record")?
        .map(serde_json::from_value)
        .transpose()
        .map_err(|_| DispatchError::InvalidData)?;
    if let Some(r) = &record {
        r.validate().map_err(|_| DispatchError::InvalidData)?;
        r.validate_identity(claim.operation_id, &plan.upload)
            .map_err(|_| DispatchError::InvalidData)?;
        if r.context.as_ref().is_some_and(|c| {
            c.allocation_id != expected.scope.allocation_id
                || c.generation != expected.scope.generation
        }) {
            return Err(DispatchError::InvalidData);
        }
    }
    let owner = Ownership {
        host_id: expected.scope.host_id.to_string(),
        project_id: expected.scope.project_id.to_string(),
        sandbox_id: expected.scope.sandbox_id.to_string(),
        allocation_id: expected.scope.allocation_id.to_string(),
        operation_id: claim.operation_id.to_string(),
        generation: expected.scope.generation,
        supervisor_epoch: expected.scope.host_epoch,
        claim_revision: claim.revision,
        claim_expires_unix_ms: ms(op.try_get("lease_expires_at")?)?,
    };
    Ok(Context {
        file,
        plan,
        source,
        record,
        owner,
    })
}
async fn source_identity(
    db: &mut PgConnection,
    op: &PgRow,
    file: &PgRow,
) -> Result<(SourcePlan, Option<SourceRef>), DispatchError> {
    source_identity_impl(db, op, file, true).await
}

pub(crate) async fn source_identity_for_audit(
    db: &mut PgConnection,
    op: &PgRow,
    file: &PgRow,
) -> Result<(SourcePlan, Option<SourceRef>), DispatchError> {
    source_identity_impl(db, op, file, false).await
}

async fn source_identity_impl(
    db: &mut PgConnection,
    op: &PgRow,
    file: &PgRow,
    share_lock: bool,
) -> Result<(SourcePlan, Option<SourceRef>), DispatchError> {
    let project: uuid::Uuid = op.try_get("project_id")?;
    let sandbox: uuid::Uuid = op.try_get("sandbox_id")?;
    if op.try_get::<String, _>("kind")? != "file_write"
        || file.try_get::<uuid::Uuid, _>("operation_id")? != op.try_get::<uuid::Uuid, _>("id")?
        || file.try_get::<uuid::Uuid, _>("project_id")? != project
        || file.try_get::<uuid::Uuid, _>("sandbox_id")? != sandbox
    {
        return Err(DispatchError::InvalidData);
    }
    let plan: SourcePlan =
        serde_json::from_value(file.try_get("plan")?).map_err(|_| DispatchError::InvalidData)?;
    plan.validate().map_err(|_| DispatchError::InvalidData)?;
    let allocation: uuid::Uuid = file.try_get("allocation_id")?;
    if op.try_get::<Option<uuid::Uuid>, _>("file_allocation_id")? != Some(allocation) {
        return Err(DispatchError::InvalidData);
    }
    let query = if share_lock {
        "SELECT * FROM allocations WHERE id=$1 AND project_id=$2 AND sandbox_id=$3 FOR SHARE"
    } else {
        "SELECT * FROM allocations WHERE id=$1 AND project_id=$2 AND sandbox_id=$3"
    };
    let a = sqlx::query(query)
        .bind(allocation)
        .bind(project)
        .bind(sandbox)
        .fetch_one(&mut *db)
        .await?;
    let expected = SourceOwner {
        operation_id: OperationId::from_uuid(op.try_get("id")?),
        scope: sandbox_protocol::file_downloads::ReadScope {
            version: 1,
            project_id: sandbox_protocol::ProjectId::from_uuid(project),
            sandbox_id: SandboxId::from_uuid(sandbox),
            allocation_id: sandbox_protocol::AllocationId::from_uuid(allocation),
            host_id: sandbox_protocol::HostId::from_uuid(a.try_get("host_id")?),
            generation: a.try_get("generation")?,
            host_epoch: a.try_get("supervisor_epoch")?,
        },
    };
    let input = UploadInput::from_plan(&plan);
    let payload: UploadInput =
        serde_json::from_value(op.try_get("payload")?).map_err(|_| DispatchError::InvalidData)?;
    if plan.owner != expected
        || input != payload
        || file.try_get::<i64, _>("size")? as u64 != plan.upload.size
        || op.try_get::<Vec<u8>, _>("request_digest")?.as_slice()
            != input.digest(expected.scope.sandbox_id)?.as_bytes()
        || op.try_get::<i32, _>("digest_version")? != sandbox_protocol::idempotency::DIGEST_VERSION
    {
        return Err(DispatchError::InvalidData);
    }
    let source: Option<SourceRef> = file
        .try_get::<Option<serde_json::Value>, _>("source_ref")?
        .map(serde_json::from_value)
        .transpose()
        .map_err(|_| DispatchError::InvalidData)?;
    if let Some(r) = &source {
        r.validate().map_err(|_| DispatchError::InvalidData)?;
        if r.plan != plan {
            return Err(DispatchError::InvalidData);
        }
    }
    let begun: bool = file.try_get("begin_requested")?;
    if op.try_get::<i32, _>("attempt_count")? != i32::from(begun) {
        return Err(DispatchError::InvalidData);
    }
    if begun {
        let receipts: serde_json::Value = op.try_get("attempt_receipts")?;
        if receipts
            != serde_json::json!([{"phase":"file_begin_intent","plan_sha256":plan.metadata_digest().map_err(|_|DispatchError::InvalidData)?}])
        {
            return Err(DispatchError::InvalidData);
        }
    }
    Ok((plan, source))
}
fn ms(t: OffsetDateTime) -> Result<i64, DispatchError> {
    i64::try_from(t.unix_timestamp_nanos() / 1_000_000).map_err(|_| DispatchError::InvalidData)
}
async fn eligible(db: &mut PgConnection, operation: OperationId) -> Result<bool, DispatchError> {
    Ok(sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM operations o JOIN file_uploads f ON f.operation_id=o.id JOIN projects p ON p.id=o.project_id JOIN sandboxes s ON s.id=o.sandbox_id JOIN allocations a ON a.id=f.allocation_id JOIN hosts h ON h.id=a.host_id WHERE o.id=$1 AND f.source_frozen_at IS NULL AND o.deadline>clock_timestamp() AND p.status='active' AND EXISTS(SELECT 1 FROM jsonb_array_elements(p.api_tokens) t WHERE t->>'key_id'=o.initiator_key_id AND decode(t->>'hash','hex')=f.token_hash AND (t->>'revoked_at' IS NULL OR (t->>'revoked_at')::timestamptz>clock_timestamp()) AND (t->>'expires_at' IS NULL OR (t->>'expires_at')::timestamptz>clock_timestamp())) AND s.desired_state='running' AND s.observed_state='running' AND s.destroyed_at IS NULL AND s.active_transition_operation_id IS NULL AND s.current_allocation_id=a.id AND s.generation=a.generation AND (s.expires_at IS NULL OR s.expires_at>clock_timestamp()) AND a.status='running' AND a.released_at IS NULL AND a.lease_expires_at>clock_timestamp() AND h.supervisor_epoch=a.supervisor_epoch AND h.status='ready' AND h.last_seen_at>clock_timestamp()-interval '30 seconds')")
 .bind(operation.uuid()).fetch_one(db).await?)
}
