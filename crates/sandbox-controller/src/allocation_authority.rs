use super::*;
use sandbox_protocol::supervisor::{AllocationAuthorityObservation, AllocationAuthorityRequest};
impl Controller {
    async fn accept_registration(
        &self,
        host: HostId,
        epoch: i64,
        observed: &AllocationAuthorityObservation,
    ) -> Result<(), ControllerError> {
        if observed.host_id != host.to_string() || observed.reporting_epoch != epoch {
            return Err(ControllerError::HostIdentity);
        }
        self.store
            .record_allocation_registration(host, epoch, true, observed.registered_through)
            .await?;
        Ok(())
    }
    pub(super) async fn sync_allocation_authority(
        &self,
        host: HostId,
        epoch: i64,
        required: bool,
        client: &mut SupervisorClient<Channel>,
        authority: &tokio::sync::Mutex<()>,
    ) -> Result<u64, ControllerError> {
        // Sibling workers must not advance host registration while another is
        // validating an acknowledgement against its bounded database batch.
        let _guard = authority.lock().await;
        if !required {
            self.store
                .record_allocation_registration(host, epoch, false, 0)
                .await?;
            return Ok(0);
        }
        let request = AllocationAuthorityRequest {
            host_id: host.to_string(),
            reporting_epoch: epoch,
            permits_json: Vec::new(),
        };
        let observed = client
            .allocation_authority(request.clone())
            .await
            .map_err(|_| ControllerError::Health)?
            .into_inner();
        self.accept_registration(host, epoch, &observed).await?;
        let batch = self
            .store
            .allocation_permits_after(host, observed.registered_through)
            .await?;
        if batch.has_unissued_allocations {
            return Err(ControllerError::HostIdentity);
        }
        if batch.permits.is_empty() {
            return Ok(observed.registered_through);
        }
        let expected = batch
            .permits
            .last()
            .ok_or(ControllerError::HostIdentity)?
            .serial;
        let mut register = request;
        register.permits_json =
            serde_json::to_vec(&batch.permits).map_err(|_| ControllerError::HostIdentity)?;
        // A lost reply ends this attempt. The next tick inspects progress first.
        let ack = client
            .allocation_authority(register)
            .await
            .map_err(|_| ControllerError::Health)?
            .into_inner();
        if ack.registered_through < expected || ack.registered_through > batch.issued_through {
            return Err(ControllerError::HostIdentity);
        }
        self.accept_registration(host, epoch, &ack).await?;
        Ok(ack.registered_through)
    }
    pub(super) async fn prepare_launch_permit(
        &self,
        claim: &Claim,
        allocation: &sandbox_store::placement::Allocation,
        client: &mut SupervisorClient<Channel>,
        authority: &tokio::sync::Mutex<()>,
    ) -> Result<Vec<u8>, ControllerError> {
        let through = self
            .sync_allocation_authority(
                allocation.host_id,
                allocation.supervisor_epoch,
                true,
                client,
                authority,
            )
            .await?;
        let owner = sandbox_protocol::supervisor::Ownership {
            host_id: allocation.host_id.to_string(),
            project_id: claim.project_id.to_string(),
            sandbox_id: claim.sandbox_id.to_string(),
            allocation_id: allocation.id.to_string(),
            operation_id: claim.operation_id.to_string(),
            generation: allocation.generation,
            supervisor_epoch: allocation.supervisor_epoch,
            claim_revision: claim.revision,
            claim_expires_unix_ms: 0,
        };
        let permit = self
            .store
            .allocation_launch_permit(&owner)
            .await?
            .ok_or(ControllerError::HostIdentity)?;
        if permit.serial > through {
            return Err(ControllerError::HostIdentity);
        }
        serde_json::to_vec(&permit).map_err(|_| ControllerError::HostIdentity)
    }
}
