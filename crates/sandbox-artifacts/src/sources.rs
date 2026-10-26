//! Private file sources. These methods neither admit nor publish a guest mutation.
use crate::{ArtifactStore, Error, TRANSFER_TIMEOUT, TRANSFERS, storage_error};
use futures_util::TryStreamExt;
use object_store::{Attribute, Attributes, GetOptions, PutMode, PutOptions, path::Path};
use sandbox_protocol::file_sources::{InvalidSource, SourceOwner, SourcePlan, SourceRef};
use sha2::{Digest, Sha256};

mod retirement;
pub use retirement::SourceRetirer;

impl From<InvalidSource> for Error {
    fn from(_: InvalidSource) -> Self {
        Self::InvalidMetadata
    }
}
#[derive(Clone)]
pub struct SourceStore {
    pub(crate) store: ArtifactStore,
}
impl std::fmt::Debug for SourceStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SourceStore").finish_non_exhaustive()
    }
}
/// Complete, verified bytes. Keep caller-side reservations until this buffer is dropped.
pub struct SourceBytes {
    pub bytes: Vec<u8>,
}
impl std::fmt::Debug for SourceBytes {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SourceBytes")
            .field("size", &self.bytes.len())
            .finish_non_exhaustive()
    }
}
fn metadata_key() -> Attribute {
    Attribute::Metadata("hudson-file-source-sha256".into())
}
fn check(plan: &SourcePlan, owner: &SourceOwner, now: i64, writing: bool) -> Result<(), Error> {
    plan.validate()?;
    if &plan.owner != owner {
        return Err(Error::OwnerMismatch);
    }
    if now < plan.created_unix_ms {
        return Err(Error::InvalidMetadata);
    }
    if now
        >= if writing {
            plan.write_expires_unix_ms
        } else {
            plan.expires_unix_ms
        }
    {
        return Err(Error::Expired);
    }
    Ok(())
}
impl SourceStore {
    /// Persist the plan first. An uncertain acknowledgement requires the original
    /// plan; create-only writes never replace data or a retirement marker.
    pub async fn upload(
        &self,
        plan: &SourcePlan,
        owner: &SourceOwner,
        now: i64,
        bytes: &[u8],
    ) -> Result<SourceRef, Error> {
        check(plan, owner, now, true)?;
        if bytes.len() as u64 != plan.upload.size {
            return Err(Error::InvalidMetadata);
        }
        let _permit = TRANSFERS.try_acquire().map_err(|_| Error::Busy)?;
        if Sha256::digest(bytes).as_slice() != plan.upload.sha256 {
            return Err(Error::Corrupt);
        }
        tokio::time::timeout(TRANSFER_TIMEOUT, async {
            let result = self
                .store
                .inner
                .put_opts(
                    &Path::from(plan.object_key()?),
                    bytes.to_vec().into(),
                    PutOptions {
                        mode: PutMode::Create,
                        attributes: Attributes::from_iter([
                            (metadata_key(), plan.metadata_digest()?),
                            (Attribute::ContentType, "application/octet-stream".into()),
                            (Attribute::CacheControl, "no-store".into()),
                        ]),
                        ..Default::default()
                    },
                )
                .await;
            let uncertain = crate::uncertain_put(&result);
            crate::confirm_upload(|| self.fetch(plan, None))
                .await
                .map(|(reference, _)| reference)
                .map_err(|error| crate::upload_error(error, uncertain))
        })
        .await
        .map_err(|_| Error::Unavailable)?
    }
    /// Recover an existing object without original bytes, even after write admission
    /// expires. This does not create an object or extend read/retirement deadlines.
    pub async fn reconcile(
        &self,
        plan: &SourcePlan,
        owner: &SourceOwner,
        now: i64,
    ) -> Result<SourceRef, Error> {
        check(plan, owner, now, false)?;
        let _permit = TRANSFERS.try_acquire().map_err(|_| Error::Busy)?;
        tokio::time::timeout(TRANSFER_TIMEOUT, self.fetch(plan, None))
            .await
            .map_err(|_| Error::Unavailable)?
            .map(|(reference, _)| reference)
    }
    /// Fetch the pinned version and verify the entire object before returning bytes.
    pub async fn read(
        &self,
        reference: &SourceRef,
        owner: &SourceOwner,
        now: i64,
    ) -> Result<SourceBytes, Error> {
        reference.validate()?;
        check(&reference.plan, owner, now, false)?;
        let _permit = TRANSFERS.try_acquire().map_err(|_| Error::Busy)?;
        tokio::time::timeout(
            TRANSFER_TIMEOUT,
            self.fetch(&reference.plan, Some(reference)),
        )
        .await
        .map_err(|_| Error::Unavailable)?
        .map(|(_, bytes)| SourceBytes { bytes })
    }
    pub(crate) async fn fetch(
        &self,
        plan: &SourcePlan,
        pinned: Option<&SourceRef>,
    ) -> Result<(SourceRef, Vec<u8>), Error> {
        let result = self
            .store
            .inner
            .get_opts(
                &Path::from(plan.object_key()?),
                GetOptions {
                    if_match: pinned.map(|r| r.etag.clone()),
                    version: pinned.and_then(|r| r.object_version.clone()),
                    ..Default::default()
                },
            )
            .await
            .map_err(storage_error)?;
        Self::verify(plan, pinned, result).await
    }
    async fn verify(
        plan: &SourcePlan,
        pinned: Option<&SourceRef>,
        response: object_store::GetResult,
    ) -> Result<(SourceRef, Vec<u8>), Error> {
        if response.meta.size != plan.upload.size
            || response.range != (0..plan.upload.size)
            || response.attributes.get(&metadata_key()).map(|v| v.as_ref())
                != Some(plan.metadata_digest()?.as_str())
        {
            return Err(Error::Corrupt);
        }
        let reference = SourceRef {
            plan: plan.clone(),
            etag: response.meta.e_tag.clone().ok_or(Error::Corrupt)?,
            object_version: response.meta.version.clone(),
        };
        reference.validate().map_err(|_| Error::Corrupt)?;
        if pinned.is_some_and(|r| r != &reference) {
            return Err(Error::Corrupt);
        }
        let mut bytes = Vec::with_capacity(plan.upload.size as usize);
        let mut hash = Sha256::new();
        let mut stream = response.into_stream();
        while let Some(chunk) = stream.try_next().await.map_err(storage_error)? {
            if chunk.len() > plan.upload.size as usize - bytes.len() {
                return Err(Error::Corrupt);
            }
            hash.update(&chunk);
            bytes.extend_from_slice(&chunk);
        }
        if bytes.len() as u64 != plan.upload.size
            || hash.finalize().as_slice() != plan.upload.sha256
        {
            return Err(Error::Corrupt);
        }
        Ok((reference, bytes))
    }
}
#[cfg(test)]
mod tests;

/// Service boundary for API ingestion and controller recovery. Implementations
/// must preserve the same immutable-plan and full-object validation contract.
pub trait SourceBackend: std::fmt::Debug + Send + Sync {
    fn upload<'a>(
        &'a self,
        plan: &'a SourcePlan,
        owner: &'a SourceOwner,
        now: i64,
        bytes: &'a [u8],
    ) -> futures_util::future::BoxFuture<'a, Result<SourceRef, Error>>;
    fn reconcile<'a>(
        &'a self,
        plan: &'a SourcePlan,
        owner: &'a SourceOwner,
        now: i64,
    ) -> futures_util::future::BoxFuture<'a, Result<SourceRef, Error>>;
    fn read<'a>(
        &'a self,
        reference: &'a SourceRef,
        owner: &'a SourceOwner,
        now: i64,
    ) -> futures_util::future::BoxFuture<'a, Result<SourceBytes, Error>>;
}
impl SourceBackend for SourceStore {
    fn upload<'a>(
        &'a self,
        p: &'a SourcePlan,
        o: &'a SourceOwner,
        n: i64,
        b: &'a [u8],
    ) -> futures_util::future::BoxFuture<'a, Result<SourceRef, Error>> {
        Box::pin(self.upload(p, o, n, b))
    }
    fn reconcile<'a>(
        &'a self,
        p: &'a SourcePlan,
        o: &'a SourceOwner,
        n: i64,
    ) -> futures_util::future::BoxFuture<'a, Result<SourceRef, Error>> {
        Box::pin(self.reconcile(p, o, n))
    }
    fn read<'a>(
        &'a self,
        r: &'a SourceRef,
        o: &'a SourceOwner,
        n: i64,
    ) -> futures_util::future::BoxFuture<'a, Result<SourceBytes, Error>> {
        Box::pin(self.read(r, o, n))
    }
}
