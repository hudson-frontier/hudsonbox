//! Warm-pool RPCs on the real Firecracker host (#188): fail closed.
//!
//! Secure adoption is blocked by the guest boundary, not by this protocol.
//! The guardian writes the per-allocation bootstrap device (allocation ID,
//! generation, channel CA and certificates, `allocation-<UUID>` server name)
//! before launch, the guest agent reads it once at boot, and the guardian's
//! receipt, journal, lease watchdog and jail are keyed by allocation. A VM
//! booted before its allocation exists therefore cannot later receive that
//! identity without a guest late-binding protocol and a guardian re-key, which
//! do not exist. This binary never prepares a member, so:
//!
//! - Prepare is refused; nothing boots.
//! - Adopt and inspect answer a definitive `rejected/warm_unsupported` receipt
//!   for the exact scope at this epoch: no member exists to adopt.
//! - Retire of a member that never reported a VM name is truthful destruction
//!   evidence at this epoch; a named member was not created by this binary and
//!   is refused rather than attested.
use super::Host;
use sandbox_protocol::{
    HostId,
    supervisor::{WarmAdoptRequest, WarmObservation, WarmPrepareRequest, WarmRetireRequest},
    warm_pool::{
        self, CleanupReceipt, HandoffIntent, HandoffOutcome, HandoffReceipt, RetireIntent, VERSION,
        WarmReceipt,
    },
};
use tonic::Status;

const BLOCKED: &str =
    "warm adoption unavailable: guest bootstrap binds allocation identity at boot (#188)";

fn now_ms() -> Result<i64, Status> {
    i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|_| Status::internal("clock before epoch"))?
            .as_millis(),
    )
    .map_err(|_| Status::internal("clock out of range"))
}

fn reply(receipt: &WarmReceipt) -> Result<WarmObservation, Status> {
    Ok(WarmObservation {
        receipt_json: warm_pool::encode(receipt)
            .map_err(|_| Status::internal("warm receipt exceeds bound"))?,
        simulated: false,
        observed_unix_ms: now_ms()?,
    })
}

fn this_epoch(identity: (HostId, i64), host: HostId, epoch: i64) -> Result<(), Status> {
    if (host, epoch) != identity {
        return Err(Status::failed_precondition(
            "wrong host or supervisor epoch",
        ));
    }
    Ok(())
}

fn handoff(identity: (HostId, i64), request: &WarmAdoptRequest) -> Result<WarmObservation, Status> {
    let intent: HandoffIntent = warm_pool::decode(&request.intent_json)
        .map_err(|_| Status::invalid_argument("invalid handoff intent"))?;
    intent
        .validate()
        .map_err(|_| Status::invalid_argument("invalid handoff intent"))?;
    this_epoch(identity, intent.host_id, intent.supervisor_epoch)?;
    let receipt = HandoffReceipt {
        version: VERSION,
        member_id: intent.member_id,
        vm_ref: intent.vm_ref.clone(),
        allocation_id: intent.allocation_id,
        project_id: intent.project_id,
        sandbox_id: intent.sandbox_id,
        host_id: intent.host_id,
        supervisor_epoch: intent.supervisor_epoch,
        claim_revision: intent.claim_revision,
        intent_sha256: intent
            .digest()
            .map_err(|_| Status::invalid_argument("invalid handoff intent"))?,
        outcome: HandoffOutcome::Rejected {
            reason: "warm_unsupported".into(),
        },
    };
    reply(&WarmReceipt::Handoff { receipt })
}

fn retire(identity: (HostId, i64), request: &WarmRetireRequest) -> Result<WarmObservation, Status> {
    let intent: RetireIntent = warm_pool::decode(&request.intent_json)
        .map_err(|_| Status::invalid_argument("invalid retire intent"))?;
    if intent.version != VERSION {
        return Err(Status::invalid_argument("invalid retire intent"));
    }
    this_epoch(identity, intent.host_id, intent.supervisor_epoch)?;
    if intent.vm_ref.is_some() {
        return Err(Status::failed_precondition(BLOCKED));
    }
    let receipt = CleanupReceipt {
        version: VERSION,
        member_id: intent.member_id,
        vm_ref: None,
        host_id: intent.host_id,
        supervisor_epoch: intent.supervisor_epoch,
        destroyed: true,
    };
    reply(&WarmReceipt::Cleanup { receipt })
}

impl Host {
    fn identity(&self) -> (HostId, i64) {
        (self.inner.config.host, self.inner.config.epoch)
    }

    pub(super) fn warm_prepare_sync(
        &self,
        _: WarmPrepareRequest,
    ) -> Result<WarmObservation, Status> {
        Err(Status::failed_precondition(BLOCKED))
    }

    pub(super) fn warm_handoff_sync(
        &self,
        request: WarmAdoptRequest,
    ) -> Result<WarmObservation, Status> {
        handoff(self.identity(), &request)
    }

    pub(super) fn warm_retire_sync(
        &self,
        request: WarmRetireRequest,
    ) -> Result<WarmObservation, Status> {
        retire(self.identity(), &request)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use sandbox_protocol::{AllocationId, Id, OperationId, ProjectId, SandboxId};

    fn intent(host: HostId) -> HandoffIntent {
        HandoffIntent {
            version: VERSION,
            member_id: OperationId::generate().uuid(),
            vm_ref: "warm-1".into(),
            template_id: OperationId::generate().uuid(),
            allocation_id: AllocationId::generate(),
            project_id: ProjectId::generate(),
            sandbox_id: SandboxId::generate(),
            create_operation_id: OperationId::generate(),
            generation: 1,
            permit_serial: 1,
            host_id: host,
            supervisor_epoch: 4,
            claim_revision: 1,
        }
    }

    #[test]
    fn real_host_never_adopts_and_attests_only_unnamed_members() {
        let host = HostId::generate();
        let i = intent(host);
        let request = WarmAdoptRequest {
            ownership: None,
            intent_json: warm_pool::encode(&i).unwrap(),
            launch_permit_json: Vec::new(),
            allocation_expires_unix_ms: 0,
        };
        let observed = handoff((host, 4), &request).unwrap();
        assert!(!observed.simulated);
        let WarmReceipt::Handoff { receipt } = warm_pool::decode(&observed.receipt_json).unwrap()
        else {
            panic!()
        };
        receipt.verify(&i).unwrap();
        assert_eq!(
            receipt.outcome,
            HandoffOutcome::Rejected {
                reason: "warm_unsupported".into()
            }
        );
        assert_eq!(
            handoff((host, 5), &request).unwrap_err().code(),
            tonic::Code::FailedPrecondition
        );
        let mut retire_intent = RetireIntent {
            version: VERSION,
            member_id: i.member_id,
            vm_ref: None,
            host_id: host,
            supervisor_epoch: 4,
            operation_id: OperationId::generate().uuid(),
            operation_revision: 1,
        };
        let encode = |r: &RetireIntent| WarmRetireRequest {
            intent_json: warm_pool::encode(r).unwrap(),
        };
        assert!(retire((host, 4), &encode(&retire_intent)).is_ok());
        assert!(retire((host, 3), &encode(&retire_intent)).is_err());
        retire_intent.vm_ref = Some("warm-1".into());
        assert_eq!(
            retire((host, 4), &encode(&retire_intent))
                .unwrap_err()
                .code(),
            tonic::Code::FailedPrecondition
        );
    }
}
