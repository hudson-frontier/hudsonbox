//! Offline recovery verification: reads only, no cleanup or replay authority.
use futures_util::{StreamExt, TryStreamExt};
use sandbox_artifacts::{ArtifactStore, sources::SourceStore};
use sandbox_store::{Store, storage_audit::Entry};

#[derive(Debug, Default, serde::Serialize)]
pub struct Report {
    pub outputs_verified: u64,
    pub sources_verified: u64,
    pub output_markers_verified: u64,
    pub source_markers_verified: u64,
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("{0}")]
    Inventory(#[from] sandbox_store::storage_audit::Error),
    #[error("output storage configuration required for retained evidence")]
    MissingOutputConfig,
    #[error("source storage configuration required for retained evidence")]
    MissingSourceConfig,
    #[error("retained storage evidence could not be verified; keep recovery quarantined")]
    Storage,
}

enum Verified {
    Output,
    Source,
    OutputMarker,
    SourceMarker,
}

/// Verify every referenced payload and completed retirement in one database
/// snapshot. The caller must stop writers and enforce an overall deadline.
/// Four transfers share the existing bounded storage budget; no bytes, keys,
/// credentials or customer paths appear in the report or errors.
pub async fn verify(
    store: &Store,
    outputs: Option<&ArtifactStore>,
    sources: Option<&SourceStore>,
    allow_simulated: bool,
) -> Result<Report, Error> {
    let mut audit = store.storage_audit(allow_simulated).await?;
    let mut report = Report::default();
    loop {
        let entries = audit.next_page().await?;
        if entries.is_empty() {
            return Ok(report);
        }
        let verified: Vec<_> = futures_util::stream::iter(entries)
            .map(|entry| async move {
                match entry {
                    Entry::Output(reference) => {
                        outputs
                            .ok_or(Error::MissingOutputConfig)?
                            .verify_reference(&reference, &reference.plan.owner)
                            .await
                            .map_err(|_| Error::Storage)?;
                        Ok::<_, Error>(Verified::Output)
                    }
                    Entry::RetiredOutput { plan, receipt } => {
                        outputs
                            .ok_or(Error::MissingOutputConfig)?
                            .verify_retirement(&plan, &plan.owner, &receipt)
                            .await
                            .map_err(|_| Error::Storage)?;
                        Ok(Verified::OutputMarker)
                    }
                    Entry::Source(reference) => {
                        sources
                            .ok_or(Error::MissingSourceConfig)?
                            .verify_reference(&reference, &reference.plan.owner)
                            .await
                            .map_err(|_| Error::Storage)?;
                        Ok(Verified::Source)
                    }
                    Entry::RetiredSource { plan, receipt } => {
                        sources
                            .ok_or(Error::MissingSourceConfig)?
                            .verify_retirement(&plan, &plan.owner, &receipt)
                            .await
                            .map_err(|_| Error::Storage)?;
                        Ok(Verified::SourceMarker)
                    }
                }
            })
            .buffer_unordered(4)
            .try_collect()
            .await?;
        for kind in verified {
            match kind {
                Verified::Output => report.outputs_verified += 1,
                Verified::OutputMarker => report.output_markers_verified += 1,
                Verified::Source => report.sources_verified += 1,
                Verified::SourceMarker => report.source_markers_verified += 1,
            }
        }
    }
}
