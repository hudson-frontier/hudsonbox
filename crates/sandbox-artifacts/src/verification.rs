//! Offline verification returns no payload or customer-access authority.
use crate::{ArtifactStore, Error, TRANSFER_TIMEOUT, TRANSFERS, sources::SourceStore};
use sandbox_protocol::{
    file_sources::{SourceOwner, SourceRef},
    output::{OutputOwner, OutputRef},
};

impl ArtifactStore {
    /// Verify the complete pinned object even if its retention deadline has
    /// passed. Only trusted offline recovery code should use this: it returns
    /// no customer bytes and never republishes, repairs or extends a reference.
    pub async fn verify_reference(
        &self,
        reference: &OutputRef,
        owner: &OutputOwner,
    ) -> Result<(), Error> {
        reference.validate()?;
        if &reference.plan.owner != owner {
            return Err(Error::OwnerMismatch);
        }
        let _permit = TRANSFERS.try_acquire().map_err(|_| Error::Busy)?;
        tokio::time::timeout(
            TRANSFER_TIMEOUT,
            self.fetch(&reference.plan, Some(reference)),
        )
        .await
        .map_err(|_| Error::Unavailable)?
        .map(|_| ())
    }
}

impl SourceStore {
    /// Verify retained source bytes against the exact ledger reference without
    /// returning them or extending write/read deadlines. Recovery must remain
    /// offline; success does not authorize replaying an upload into a guest.
    pub async fn verify_reference(
        &self,
        reference: &SourceRef,
        owner: &SourceOwner,
    ) -> Result<(), Error> {
        reference.validate()?;
        if &reference.plan.owner != owner {
            return Err(Error::OwnerMismatch);
        }
        let _permit = TRANSFERS.try_acquire().map_err(|_| Error::Busy)?;
        tokio::time::timeout(
            TRANSFER_TIMEOUT,
            self.fetch(&reference.plan, Some(reference)),
        )
        .await
        .map_err(|_| Error::Unavailable)?
        .map(|_| ())
    }
}
