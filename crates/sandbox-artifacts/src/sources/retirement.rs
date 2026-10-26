//! Retire an immutable upload attempt without reopening its create-only key.
//! Keep a compact current object forever; delete only verified old versions.
use super::SourceStore;
use crate::{Error, TRANSFER_TIMEOUT, TRANSFERS, storage_error};
use futures_util::{TryStreamExt, future::BoxFuture};
use object_store::{
    Attribute, Attributes, GetOptions, PutMode, PutOptions, UpdateVersion, path::Path,
};
use sandbox_protocol::file_sources::{SourceOwner, SourcePlan, SourceRef, SourceRetirement};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{fmt, sync::Arc};

const MAX_MARKER: u64 = 16384;
fn marker_key() -> Attribute {
    Attribute::Metadata("hudson-file-source-retirement-sha256".into())
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Marker {
    version: u32,
    plan_sha256: String,
    previous: Option<SourceRef>,
}

enum Current {
    Missing,
    Data(SourceRef),
    Retired(SourceRetirement),
}

pub(crate) trait SourceVersionDelete: Send + Sync {
    fn delete<'a>(
        &'a self,
        plan: &'a SourcePlan,
        version: &'a str,
    ) -> BoxFuture<'a, Result<(), Error>>;
}

/// Created only from trusted operator configuration. Runtime API readers and
/// controllers use SourceStore; cleanup uses this distinct capability.
#[derive(Clone)]
pub struct SourceRetirer {
    pub(crate) store: SourceStore,
    pub(crate) delete: Arc<dyn SourceVersionDelete>,
}
impl fmt::Debug for SourceRetirer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SourceRetirer").finish_non_exhaustive()
    }
}

impl SourceRetirer {
    /// The caller must first freeze this exact attempt in the database. Owner
    /// and time come from trusted service state. Selected refs, when present,
    /// must come from the frozen manifest, never from customer parameters.
    ///
    /// A failed/uncertain call must retry the same plan. Retains a small marker
    /// at the original key so even an old in-flight create-only PUT cannot
    /// recreate source bytes. Versioned payloads are deleted by exact version ID;
    /// the marker records that identity before any irreversible deletion.
    pub async fn retire(
        &self,
        plan: &SourcePlan,
        owner: &SourceOwner,
        selected: Option<&SourceRef>,
        now: i64,
    ) -> Result<SourceRetirement, Error> {
        plan.validate()?;
        if &plan.owner != owner {
            return Err(Error::OwnerMismatch);
        }
        if now < plan.delete_after_unix_ms {
            return Err(Error::InvalidMetadata);
        }
        if let Some(selected) = selected {
            selected.validate()?;
            if &selected.plan != plan {
                return Err(Error::InvalidMetadata);
            }
        }
        let _permit = TRANSFERS.try_acquire().map_err(|_| Error::Busy)?;
        tokio::time::timeout(TRANSFER_TIMEOUT, self.retire_inner(plan, selected))
            .await
            .map_err(|_| Error::Unavailable)?
    }

    async fn retire_inner(
        &self,
        plan: &SourcePlan,
        selected: Option<&SourceRef>,
    ) -> Result<SourceRetirement, Error> {
        let receipt = match self.store.current_retirement(plan, selected).await? {
            Current::Retired(receipt) => receipt,
            current => {
                let (previous, mode) = match current {
                    Current::Missing => (selected.cloned(), PutMode::Create),
                    Current::Data(reference) => {
                        let mode = PutMode::Update(UpdateVersion {
                            e_tag: Some(reference.etag.clone()),
                            version: reference.object_version.clone(),
                        });
                        (Some(reference), mode)
                    }
                    Current::Retired(_) => return Err(Error::Corrupt),
                };
                let marker = Marker {
                    version: 1,
                    plan_sha256: plan.metadata_digest()?,
                    previous,
                };
                let bytes = serde_json::to_vec(&marker).map_err(|_| Error::InvalidMetadata)?;
                if bytes.len() as u64 > MAX_MARKER {
                    return Err(Error::InvalidMetadata);
                }
                let attributes = Attributes::from_iter([
                    (marker_key(), hex::encode(Sha256::digest(&bytes))),
                    (Attribute::ContentType, "application/json".into()),
                    (Attribute::CacheControl, "no-store".into()),
                ]);
                let result = self
                    .store
                    .store
                    .inner
                    .put_opts(
                        &Path::from(plan.object_key()?),
                        bytes.into(),
                        PutOptions {
                            mode,
                            attributes,
                            ..Default::default()
                        },
                    )
                    .await;
                match result {
                    Ok(_)
                    | Err(
                        object_store::Error::AlreadyExists { .. }
                        | object_store::Error::Precondition { .. },
                    ) => {}
                    // The marker may have committed. Do not invent completion
                    // or write a different marker after a lost acknowledgement.
                    Err(error) => return Err(storage_error(error)),
                }
                match self.store.current_retirement(plan, selected).await? {
                    Current::Retired(receipt) => receipt,
                    _ => return Err(Error::Conflict),
                }
            }
        };
        self.remove_previous(plan, &receipt).await?;
        // Absence of the old version is insufficient if the marker vanished.
        match self.store.current_retirement(plan, selected).await? {
            Current::Retired(current) if current == receipt => Ok(receipt),
            _ => Err(Error::Conflict),
        }
    }
}

impl SourceStore {
    /// Read-only verification of an exact retirement receipt from a trusted
    /// offline recovery ledger. Missing/changed markers and reappeared retired
    /// versions fail without modifying storage or extending read authority.
    pub async fn verify_retirement(
        &self,
        plan: &SourcePlan,
        owner: &SourceOwner,
        expected: &SourceRetirement,
    ) -> Result<(), Error> {
        plan.validate()?;
        if &plan.owner != owner {
            return Err(Error::OwnerMismatch);
        }
        expected.validate(plan)?;
        let _permit = TRANSFERS.try_acquire().map_err(|_| Error::Busy)?;
        tokio::time::timeout(TRANSFER_TIMEOUT, async {
            match self
                .current_retirement(plan, expected.previous.as_ref())
                .await?
            {
                Current::Retired(current) if &current == expected => {}
                Current::Missing => return Err(Error::Missing),
                _ => return Err(Error::Conflict),
            }
            if let Some(previous) = &expected.previous
                && let Some(version) = &previous.object_version
                && Some(version) != expected.marker_version.as_ref()
            {
                match self
                    .store
                    .inner
                    .get_opts(
                        &Path::from(plan.object_key()?),
                        GetOptions {
                            version: Some(version.clone()),
                            head: true,
                            ..Default::default()
                        },
                    )
                    .await
                {
                    Err(object_store::Error::NotFound { .. }) => {}
                    Err(error) => return Err(storage_error(error)),
                    Ok(_) => return Err(Error::Conflict),
                }
            }
            Ok(())
        })
        .await
        .map_err(|_| Error::Unavailable)?
    }

    async fn current_retirement(
        &self,
        plan: &SourcePlan,
        selected: Option<&SourceRef>,
    ) -> Result<Current, Error> {
        let response = match self
            .store
            .inner
            .get_opts(&Path::from(plan.object_key()?), GetOptions::default())
            .await
        {
            Ok(response) => response,
            Err(object_store::Error::NotFound { .. }) => return Ok(Current::Missing),
            Err(error) => return Err(storage_error(error)),
        };
        let Some(digest) = response
            .attributes
            .get(&marker_key())
            .map(|v| v.as_ref().to_owned())
        else {
            let (reference, _) = SourceStore::verify(plan, selected, response).await?;
            return Ok(Current::Data(reference));
        };
        let size = response.meta.size;
        if size > MAX_MARKER || response.range != (0..size) {
            return Err(Error::Corrupt);
        }
        let marker_etag = response.meta.e_tag.clone().ok_or(Error::Corrupt)?;
        let marker_version = response.meta.version.clone();
        let mut bytes = Vec::with_capacity(size as usize);
        let mut stream = response.into_stream();
        while let Some(chunk) = stream.try_next().await.map_err(storage_error)? {
            if chunk.len() > size as usize - bytes.len() {
                return Err(Error::Corrupt);
            }
            bytes.extend_from_slice(&chunk);
        }
        if bytes.len() as u64 != size || hex::encode(Sha256::digest(&bytes)) != digest {
            return Err(Error::Corrupt);
        }
        let marker: Marker = serde_json::from_slice(&bytes).map_err(|_| Error::Corrupt)?;
        let receipt = SourceRetirement {
            version: marker.version,
            plan_sha256: marker.plan_sha256,
            previous: marker.previous,
            marker_etag,
            marker_version,
        };
        receipt.validate(plan).map_err(|_| Error::Corrupt)?;
        if selected.is_some_and(|r| receipt.previous.as_ref() != Some(r)) {
            return Err(Error::Corrupt);
        }
        Ok(Current::Retired(receipt))
    }
}

impl SourceRetirer {
    async fn remove_previous(
        &self,
        plan: &SourcePlan,
        receipt: &SourceRetirement,
    ) -> Result<(), Error> {
        let Some(previous) = &receipt.previous else {
            return Ok(());
        };
        let version = match (
            previous.object_version.as_deref(),
            receipt.marker_version.as_deref(),
        ) {
            // Unversioned replacement, or replacement of the mutable null
            // version in a suspended bucket, already removed those bytes.
            (None, None) | (Some("null"), Some("null")) => return Ok(()),
            (Some(old), Some(marker)) if old != marker => old,
            // A bucket changing its versioning mode mid-cleanup cannot turn
            // an unpinned object into permission to delete the current key.
            _ => return Err(Error::Conflict),
        };
        // Revalidate the entire old version before deleting it, including on
        // recovery from a persisted marker. Metadata alone is not ownership.
        match self.store.fetch(plan, Some(previous)).await {
            Ok(_) => {}
            Err(Error::Missing) => return Ok(()),
            Err(error) => return Err(error),
        }
        self.delete.delete(plan, version).await?;
        let options = GetOptions {
            version: Some(version.into()),
            head: true,
            ..Default::default()
        };
        match self
            .store
            .store
            .inner
            .get_opts(&Path::from(plan.object_key()?), options)
            .await
        {
            Err(object_store::Error::NotFound { .. }) => Ok(()),
            Err(error) => Err(storage_error(error)),
            Ok(_) => Err(Error::Unavailable),
        }
    }
}

impl SourceVersionDelete for crate::retirement::S3VersionDelete {
    fn delete<'a>(
        &'a self,
        plan: &'a SourcePlan,
        version: &'a str,
    ) -> BoxFuture<'a, Result<(), Error>> {
        Box::pin(async move { self.delete_object(&plan.object_key()?, version).await })
    }
}
#[cfg(test)]
mod tests;
