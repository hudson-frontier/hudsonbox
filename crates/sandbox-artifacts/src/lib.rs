//! Private S3-compatible output and file-source objects. Successful storage I/O
//! does not authorize customer access, database publication or guest mutation.
mod config;
pub mod sources;
pub use config::S3Config;
mod retirement;
mod verification;
pub use retirement::ArtifactRetirer;

use futures_util::TryStreamExt;
use object_store::{
    Attribute, Attributes, GetOptions, ObjectStore, PutMode, PutOptions, path::Path,
};
use sandbox_protocol::output::{InvalidOutput, MAX_CHUNK, OutputOwner, OutputPlan, OutputRef};
use sha2::{Digest, Sha256};
use std::{fmt, sync::Arc, time::Duration};
use tokio::sync::Semaphore;

// Shared across handles, clones, and buckets. Fail fast rather than accumulating
// waiting uploads (which may each hold a 10 MiB caller buffer).
static TRANSFERS: Semaphore = Semaphore::const_new(4);
const TRANSFER_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum Error {
    #[error("invalid artifact storage configuration")]
    InvalidConfig,
    #[error("invalid artifact metadata")]
    InvalidMetadata,
    #[error("artifact owner mismatch")]
    OwnerMismatch,
    #[error("artifact retention expired")]
    Expired,
    #[error("artifact object missing")]
    Missing,
    #[error("artifact integrity check failed")]
    Corrupt,
    #[error("artifact upload attempt conflicts with existing object")]
    Conflict,
    #[error("invalid artifact range")]
    Bounds,
    #[error("artifact transfer capacity exhausted")]
    Busy,
    /// May mean a write succeeded but its acknowledgement was lost. Reconcile
    /// with a read of the SAME persisted plan; uncertainty is not permission to
    /// issue a new upload identity or release its reservation.
    #[error("artifact storage unavailable; outcome may be uncertain")]
    Unavailable,
}
impl From<InvalidOutput> for Error {
    fn from(_: InvalidOutput) -> Self {
        Self::InvalidMetadata
    }
}

#[derive(Clone)]
pub struct ArtifactStore {
    inner: Arc<dyn ObjectStore>,
}
impl fmt::Debug for ArtifactStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ArtifactStore").finish_non_exhaustive()
    }
}

/// Binary bytes are intentionally excluded from Debug. EOF means end of the
/// captured object; `truncated` may still report discarded process output.
pub struct OutputChunk {
    pub bytes: Vec<u8>,
    pub next_offset: u64,
    pub eof: bool,
    pub truncated: bool,
}
impl fmt::Debug for OutputChunk {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OutputChunk")
            .field("next_offset", &self.next_offset)
            .field("eof", &self.eof)
            .field("truncated", &self.truncated)
            .finish_non_exhaustive()
    }
}

fn metadata_key() -> Attribute {
    Attribute::Metadata("hudson-output-plan-sha256".into())
}
fn storage_error(error: object_store::Error) -> Error {
    match error {
        object_store::Error::NotFound { .. } => Error::Missing,
        object_store::Error::Precondition { .. } | object_store::Error::NotModified { .. } => {
            Error::Corrupt
        }
        _ => Error::Unavailable,
    }
}
// Even an uncertain PUT can have committed. Perform a verified read, never
// another PUT; a missing read cannot resolve a request that may still commit.
fn uncertain_put(result: &object_store::Result<object_store::PutResult>) -> bool {
    !matches!(
        result,
        Ok(_)
            | Err(object_store::Error::AlreadyExists { .. }
                | object_store::Error::Precondition { .. })
    )
}
fn upload_error(error: Error, uncertain: bool) -> Error {
    match error {
        Error::Corrupt => Error::Conflict,
        Error::Missing if uncertain => Error::Unavailable,
        other => other,
    }
}
// A reused HTTP connection can close before the confirming GET finishes.
// Retry only the read, once, within the caller's original transfer timeout and
// permit. Each attempt verifies the complete object from scratch. Missing or
// conflicting evidence is authoritative for that read and is never retried.
async fn confirm_upload<T, F, Fut>(mut read: F) -> Result<T, Error>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T, Error>>,
{
    match read().await {
        Err(Error::Unavailable) => read().await,
        result => result,
    }
}
fn check_owner(plan: &OutputPlan, expected: &OutputOwner, now: i64) -> Result<(), Error> {
    plan.validate()?;
    if &plan.owner != expected {
        return Err(Error::OwnerMismatch);
    }
    if now < plan.created_unix_ms {
        return Err(Error::InvalidMetadata);
    }
    if now >= plan.expires_unix_ms {
        return Err(Error::Expired);
    }
    Ok(())
}

impl ArtifactStore {
    /// Reconcile an upload from its persisted plan without access to the guest
    /// or original bytes. Missing is distinct from corrupt/uncertain; only a
    /// verified object yields a reference. This never creates or overwrites it.
    pub async fn reconcile(
        &self,
        plan: &OutputPlan,
        owner: &OutputOwner,
        now: i64,
    ) -> Result<OutputRef, Error> {
        check_owner(plan, owner, now)?;
        let _permit = TRANSFERS.try_acquire().map_err(|_| Error::Busy)?;
        tokio::time::timeout(TRANSFER_TIMEOUT, self.fetch(plan, None))
            .await
            .map_err(|_| Error::Unavailable)?
            .map(|(reference, _)| reference)
    }

    /// Create-only write. The expected owner must come from trusted operation
    /// and allocation state, never from the same untrusted reference being read.
    /// Caller must persist the plan before invoking this method. A returned ref
    /// still needs fenced database publication; it grants no customer access.
    pub async fn upload(
        &self,
        plan: &OutputPlan,
        owner: &OutputOwner,
        now: i64,
        bytes: &[u8],
    ) -> Result<OutputRef, Error> {
        check_owner(plan, owner, now)?;
        if bytes.len() as u64 != plan.size {
            return Err(Error::InvalidMetadata);
        }
        let _permit = TRANSFERS.try_acquire().map_err(|_| Error::Busy)?;
        if hex::encode(Sha256::digest(bytes)) != plan.sha256 {
            return Err(Error::Corrupt);
        }
        tokio::time::timeout(TRANSFER_TIMEOUT, async {
            let path = Path::from(plan.object_key()?);
            let attributes = Attributes::from_iter([
                (metadata_key(), plan.metadata_digest()?),
                (
                    Attribute::ContentType,
                    "application/octet-stream".to_owned(),
                ),
                (Attribute::CacheControl, "no-store".to_owned()),
            ]);
            let result = self
                .inner
                .put_opts(
                    &path,
                    bytes.to_vec().into(),
                    PutOptions {
                        mode: PutMode::Create,
                        attributes,
                        ..Default::default()
                    },
                )
                .await;
            let uncertain = uncertain_put(&result);
            // Do not infer success from AlreadyExists or an ETag (not a content
            // digest). Verify the complete object and bound metadata, including
            // after a successful PUT. Lost acknowledgements take this same path.
            let (reference, _) = confirm_upload(|| self.fetch(plan, None))
                .await
                .map_err(|error| upload_error(error, uncertain))?;
            Ok(reference)
        })
        .await
        .map_err(|_| Error::Unavailable)?
    }

    /// Verify the entire bounded object before returning any requested bytes.
    /// No raw key, URL, bucket, or host path is accepted. Authorization and
    /// periodic token revalidation belong to the API caller.
    pub async fn read(
        &self,
        reference: &OutputRef,
        owner: &OutputOwner,
        now: i64,
        offset: u64,
        limit: usize,
    ) -> Result<OutputChunk, Error> {
        reference.validate()?;
        check_owner(&reference.plan, owner, now)?;
        if limit == 0 || limit > MAX_CHUNK || offset > reference.plan.size {
            return Err(Error::Bounds);
        }
        let _permit = TRANSFERS.try_acquire().map_err(|_| Error::Busy)?;
        tokio::time::timeout(TRANSFER_TIMEOUT, async {
            let (_, bytes) = self.fetch(&reference.plan, Some(reference)).await?;
            let start = offset as usize;
            let end = bytes.len().min(start + limit);
            Ok(OutputChunk {
                bytes: bytes[start..end].to_vec(),
                next_offset: end as u64,
                eof: end == bytes.len(),
                truncated: reference.plan.truncated,
            })
        })
        .await
        .map_err(|_| Error::Unavailable)?
    }

    async fn fetch(
        &self,
        plan: &OutputPlan,
        pinned: Option<&OutputRef>,
    ) -> Result<(OutputRef, Vec<u8>), Error> {
        let path = Path::from(plan.object_key()?);
        let options = GetOptions {
            if_match: pinned.map(|r| r.etag.clone()),
            version: pinned.and_then(|r| r.object_version.clone()),
            ..Default::default()
        };
        let response = self
            .inner
            .get_opts(&path, options)
            .await
            .map_err(storage_error)?;
        Self::verify_response(plan, pinned, response).await
    }

    async fn verify_response(
        plan: &OutputPlan,
        pinned: Option<&OutputRef>,
        response: object_store::GetResult,
    ) -> Result<(OutputRef, Vec<u8>), Error> {
        if response.meta.size != plan.size
            || response.range != (0..plan.size)
            || response.attributes.get(&metadata_key()).map(|v| v.as_ref())
                != Some(plan.metadata_digest()?.as_str())
        {
            return Err(Error::Corrupt);
        }
        let reference = OutputRef {
            plan: plan.clone(),
            etag: response.meta.e_tag.clone().ok_or(Error::Corrupt)?,
            object_version: response.meta.version.clone(),
        };
        reference.validate().map_err(|_| Error::Corrupt)?;
        if pinned.is_some_and(|r| r != &reference) {
            return Err(Error::Corrupt);
        }
        let mut stream = response.into_stream();
        let mut bytes = Vec::with_capacity(plan.size as usize);
        let mut hash = Sha256::new();
        while let Some(chunk) = stream.try_next().await.map_err(storage_error)? {
            if chunk.len() > plan.size as usize - bytes.len() {
                return Err(Error::Corrupt);
            }
            hash.update(&chunk);
            bytes.extend_from_slice(&chunk);
        }
        if bytes.len() as u64 != plan.size || hex::encode(hash.finalize()) != plan.sha256 {
            return Err(Error::Corrupt);
        }
        Ok((reference, bytes))
    }
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod uncertain_tests;
