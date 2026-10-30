use super::*;
use sandbox_protocol::supervisor::PreviousAllocationRequest;
use sandbox_store::recovery::PreviousAction;

impl Controller {
    pub(super) async fn owner_unavailable(&self, claim: &Claim) -> Result<Tick, ControllerError> {
        match self.store.record_owner_unavailable(claim).await {
            Ok(()) => self.defer(claim).await,
            Err(DispatchError::LostClaim) => Ok(Tick::LostOwnership),
            Err(error) => Err(error.into()),
        }
    }

    async fn previous_unknown(
        &self,
        claim: &Claim,
        request: &PreviousAllocationRequest,
    ) -> Result<Tick, ControllerError> {
        match self.store.record_previous_unknown(claim, request).await {
            Ok(()) => Ok(Tick::Unknown),
            Err(DispatchError::LostClaim) => Ok(Tick::LostOwnership),
            Err(DispatchError::Conflict) => self.defer(claim).await,
            Err(e) => Err(e.into()),
        }
    }

    pub(super) async fn previous_tick(
        &mut self,
        claim: &Claim,
    ) -> Result<Option<Tick>, ControllerError> {
        // Includes database lock waits and the recovery RPC. Cancellation leaves
        // the existing durable operation claim to expire; it never releases capacity.
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(10),
            self.previous_inner(claim),
        )
        .await;
        match result {
            Ok(r) => r,
            Err(_) => Ok(Some(Tick::Deferred)),
        }
    }

    async fn previous_inner(&mut self, claim: &Claim) -> Result<Option<Tick>, ControllerError> {
        // Reconciliation is reported by the owner host's current incarnation.
        let mut route = match self.owner(claim).await? {
            hosts::Owner::Ready(route) => route,
            hosts::Owner::None => return Ok(None),
            hosts::Owner::Unavailable => return self.owner_unavailable(claim).await.map(Some),
        };
        let action = match self
            .store
            .prepare_previous_allocation(claim, route.host, route.epoch)
            .await
        {
            Ok(Some(a)) => a,
            Ok(None) => return Ok(None),
            Err(DispatchError::LostClaim) => return Ok(Some(Tick::LostOwnership)),
            Err(DispatchError::Conflict) => return self.defer(claim).await.map(Some),
            Err(e) => return Err(e.into()),
        };
        let PreviousAction::Reconcile(request) = action else {
            return self
                .reject(claim, CreateRejection::HostEpochChanged)
                .await
                .map(Some);
        };
        let response = match route
            .client
            .reconcile_previous_allocation(request.clone())
            .await
        {
            Ok(r) if r.get_ref().request.as_ref() == Some(&request) => r.into_inner(),
            _ => return self.previous_unknown(claim, &request).await.map(Some),
        };
        match self
            .store
            .record_previous_release(claim, &response, self.config.allow_simulated)
            .await
        {
            Ok(()) => Ok(Some(Tick::Confirmed)),
            Err(DispatchError::LostClaim) => Ok(Some(Tick::LostOwnership)),
            Err(DispatchError::Conflict) => self.defer(claim).await.map(Some),
            Err(DispatchError::BadEvidence | DispatchError::SimulationDenied) => {
                self.previous_unknown(claim, &request).await.map(Some)
            }
            Err(e) => Err(e.into()),
        }
    }
}
