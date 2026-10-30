//! Controller-only guest publication from one verified retained source.
use super::*;
use sandbox_artifacts::sources::{SourceBackend, SourceBytes};
use sandbox_protocol::{file_sources::SourceRef, supervisor::FileWriteRequest};
use sandbox_store::uploads::UploadAction;
use sha2::{Digest, Sha256};
use std::sync::Arc;

impl Controller {
    pub fn with_file_sources(mut self, sources: Arc<dyn SourceBackend>) -> Self {
        self.file_sources = Some(sources);
        self
    }
    pub(crate) async fn upload_tick(&mut self, claim: &Claim) -> Result<Tick, ControllerError> {
        match tokio::time::timeout(std::time::Duration::from_secs(10), self.upload_step(claim))
            .await
        {
            Ok(result) => result,
            Err(_) => {
                self.file_cache = None;
                self.upload_uncertain(claim).await
            }
        }
    }
    async fn upload_uncertain(&self, claim: &Claim) -> Result<Tick, ControllerError> {
        match self.store.upload_unknown(claim).await {
            Ok(()) => Ok(Tick::Unknown),
            Err(DispatchError::LostClaim) => Ok(Tick::LostOwnership),
            Err(error) => Err(error.into()),
        }
    }
    async fn upload_step(&mut self, claim: &Claim) -> Result<Tick, ControllerError> {
        let sources = self
            .file_sources
            .clone()
            .ok_or(ControllerError::InvalidConfig)?;
        let mut route = match self.owner(claim).await? {
            hosts::Owner::Ready(route) => Some(route),
            hosts::Owner::Unavailable => return self.defer(claim).await,
            hosts::Owner::None => None,
        };
        let (host, epoch) = route
            .as_ref()
            .map_or((self.config.host, self.config.epoch), |r| (r.host, r.epoch));
        let action = match self.store.prepare_upload(claim, host, epoch).await {
            Ok(a) => a,
            Err(DispatchError::LostClaim) => return Ok(Tick::LostOwnership),
            Err(
                DispatchError::HostUnavailable
                | DispatchError::Conflict
                | DispatchError::Unauthorized,
            ) => {
                return self.defer(claim).await;
            }
            Err(e) => return Err(e.into()),
        };
        let now = (time::OffsetDateTime::now_utc().unix_timestamp_nanos() / 1_000_000) as i64;
        if route.is_none() && !matches!(action, UploadAction::Rejected | UploadAction::Source(_)) {
            self.file_cache = None;
            return self.upload_uncertain(claim).await;
        }
        let route = route.as_mut().ok_or(ControllerError::Unroutable);
        let mut written = None;
        let result = match action {
            UploadAction::Rejected => {
                self.file_cache = None;
                return Ok(Tick::Rejected);
            }
            UploadAction::Source(plan) => match sources.reconcile(&plan, &plan.owner, now).await {
                Ok(reference) => match self.store.accept_upload_source(claim, &reference).await {
                    Ok(()) => return Ok(Tick::Confirmed),
                    Err(DispatchError::LostClaim) => return Ok(Tick::LostOwnership),
                    Err(DispatchError::BadEvidence | DispatchError::Conflict) => {
                        return self.upload_uncertain(claim).await;
                    }
                    Err(e) => return Err(e.into()),
                },
                Err(_) => return self.upload_uncertain(claim).await,
            },
            UploadAction::Begin(request) => route?.client.begin_file(request).await,
            UploadAction::Commit(request) => route?.client.commit_file(request).await,
            UploadAction::Abort(request) => {
                self.file_cache = None;
                route?.client.abort_file(request).await
            }
            UploadAction::Inspect(request) => route?.client.inspect_file(request).await,
            UploadAction::Write {
                request,
                source,
                offset,
                limit,
            } => {
                if now >= source.plan.expires_unix_ms {
                    self.file_cache = None;
                    return self.upload_uncertain(claim).await;
                }
                if !self
                    .file_cache
                    .as_ref()
                    .is_some_and(|(old, _)| old == source.as_ref())
                {
                    self.file_cache = None;
                    let bytes = match sources.read(&source, &source.plan.owner, now).await {
                        Ok(v) => v,
                        Err(_) => return self.upload_uncertain(claim).await,
                    };
                    if bytes.bytes.len() as u64 != source.plan.upload.size
                        || Sha256::digest(&bytes.bytes).as_slice() != source.plan.upload.sha256
                    {
                        return self.upload_uncertain(claim).await;
                    }
                    self.file_cache = Some((*source.clone(), bytes));
                }
                let bytes = &self
                    .file_cache
                    .as_ref()
                    .ok_or(ControllerError::InvalidConfig)?
                    .1
                    .bytes;
                let end = (offset as usize)
                    .checked_add(limit)
                    .ok_or(ControllerError::InvalidConfig)?;
                let chunk = bytes
                    .get(offset as usize..end)
                    .ok_or(ControllerError::InvalidConfig)?
                    .to_vec();
                if let Err(e) = self
                    .store
                    .confirm_upload_write(claim, &request, &source, offset)
                    .await
                {
                    self.file_cache = None;
                    return match e {
                        DispatchError::LostClaim => Ok(Tick::LostOwnership),
                        DispatchError::Unauthorized | DispatchError::Conflict => {
                            self.upload_uncertain(claim).await
                        }
                        _ => Err(e.into()),
                    };
                }
                written = Some(end as u64);
                route?
                    .client
                    .write_file(FileWriteRequest {
                        request: Some(request),
                        offset,
                        data: chunk,
                    })
                    .await
            }
        };
        let observation = match result {
            Ok(r) => r.into_inner(),
            Err(_) => return self.upload_uncertain(claim).await,
        };
        match self
            .store
            .observe_upload(claim, &observation, written, self.config.allow_simulated)
            .await
        {
            Ok(()) => {
                if observation.not_started || matches!(observation.state, 3..=5) {
                    self.file_cache = None;
                }
                Ok(Tick::Confirmed)
            }
            Err(DispatchError::LostClaim) => Ok(Tick::LostOwnership),
            Err(DispatchError::BadEvidence | DispatchError::SimulationDenied) => {
                self.upload_uncertain(claim).await
            }
            Err(e) => Err(e.into()),
        }
    }
}
pub(crate) type FileCache = Option<(SourceRef, SourceBytes)>;
