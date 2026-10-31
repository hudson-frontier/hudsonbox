//! Simulated launch-permit registry. Opt-in with
//! [`FakeHost::require_launch_permits`] before the first registration.
//!
//! Matching every caller-supplied field is not proof of issuance: a forged but
//! self-consistent permit must also be denied. When required, Create and both
//! warm handoff RPCs accept only a permit that the controller registered
//! through `AllocationAuthority` (contiguous serials from the database issuer),
//! exactly as the real host's durable authority does. The registry is in
//! memory and per epoch; it is test evidence, not durable authority.
use super::{FakeHost, State};
use sandbox_protocol::{
    allocation_authority::Permit,
    supervisor::{AllocationAuthorityObservation, AllocationAuthorityRequest, Ownership},
};
use std::collections::BTreeMap;
use tonic::Status;

const MAX_BATCH: usize = 32;
const MAX_PERMITS: usize = 1024;

#[derive(Debug, Default)]
pub(crate) struct Registry {
    required: bool,
    through: u64,
    permits: BTreeMap<u64, Permit>,
}

impl Registry {
    pub(crate) fn required(&self) -> bool {
        self.required
    }

    /// Admit `permit` for `owner` only if it is the exact registered permit.
    /// `create` additionally binds the permit's create operation.
    pub(crate) fn admit(
        &self,
        permit_json: &[u8],
        owner: &Ownership,
        create: bool,
    ) -> Result<(), Status> {
        if !self.required {
            return Ok(());
        }
        let denied = || Status::permission_denied("launch permit is not registered");
        let permit: Permit = serde_json::from_slice(permit_json).map_err(|_| denied())?;
        if self.permits.get(&permit.serial) != Some(&permit)
            || permit.host.to_string() != owner.host_id
            || permit.project.to_string() != owner.project_id
            || permit.sandbox.to_string() != owner.sandbox_id
            || permit.allocation.to_string() != owner.allocation_id
            || permit.generation != owner.generation
            || permit.original_epoch != owner.supervisor_epoch
            || (create && permit.create_operation.to_string() != owner.operation_id)
        {
            return Err(denied());
        }
        Ok(())
    }
}

impl FakeHost {
    /// Require registered launch permits from now on. Cannot be undone.
    pub async fn require_launch_permits(&self) {
        self.state.lock().await.permits.required = true;
    }

    /// Permits registered by the controller (test observation).
    pub async fn registered_permits(&self) -> u64 {
        self.state.lock().await.permits.through
    }

    fn register(
        &self,
        state: &mut State,
        request: AllocationAuthorityRequest,
    ) -> Result<u64, Status> {
        let registry = &mut state.permits;
        if !registry.required {
            return Err(Status::failed_precondition(
                "launch permits are not required on this host",
            ));
        }
        if request.host_id != self.config.host.to_string()
            || request.reporting_epoch != self.config.epoch
        {
            return Err(Status::failed_precondition(
                "wrong host or supervisor epoch",
            ));
        }
        if request.permits_json.is_empty() {
            return Ok(registry.through);
        }
        let invalid = || Status::invalid_argument("invalid permit batch");
        let batch: Vec<Permit> =
            serde_json::from_slice(&request.permits_json).map_err(|_| invalid())?;
        if batch.is_empty() || batch.len() > MAX_BATCH {
            return Err(invalid());
        }
        // Wholly retained exact retry.
        if batch
            .iter()
            .all(|p| registry.permits.get(&p.serial) == Some(p))
        {
            return Ok(registry.through);
        }
        if registry.permits.len() + batch.len() > MAX_PERMITS {
            return Err(Status::resource_exhausted("fake permit registry full"));
        }
        for (offset, permit) in batch.iter().enumerate() {
            let allocation = permit.allocation;
            if permit.serial != registry.through + 1 + offset as u64
                || permit.host != self.config.host
                || permit.generation <= 0
                || permit.original_epoch <= 0
                || batch[..offset].iter().any(|p| p.allocation == allocation)
                || registry
                    .permits
                    .values()
                    .any(|p| p.allocation == allocation)
            {
                return Err(invalid());
            }
        }
        for permit in batch {
            registry.through = permit.serial;
            registry.permits.insert(permit.serial, permit);
        }
        Ok(registry.through)
    }

    pub(crate) async fn allocation_authority_inner(
        &self,
        request: AllocationAuthorityRequest,
    ) -> Result<AllocationAuthorityObservation, Status> {
        let mut state = self.state.lock().await;
        let through = self.register(&mut state, request)?;
        Ok(AllocationAuthorityObservation {
            host_id: self.config.host.to_string(),
            reporting_epoch: self.config.epoch,
            registered_through: through,
        })
    }
}
