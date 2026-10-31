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
    supervisor::{WarmAdoptRequest, WarmObservation, WarmPrepareRequest, WarmRetireRequest},
    warm_pool::{
        self, CleanupReceipt, HandoffIntent, HandoffOutcome, HandoffReceipt, RetireIntent, VERSION,
        WarmReceipt,
    },
};
use tonic::Status;

const BLOCKED: &str =
    "warm adoption unavailable: guest bootstrap binds allocation identity at boot (#188)";

fn reply(host: &Host, receipt: &WarmReceipt) -> Result<WarmObservation, Status> {
    Ok(WarmObservation {
        receipt_json: warm_pool::encode(receipt)
            .map_err(|_| Status::internal("warm receipt exceeds bound"))?,
        simulated: false,
        observed_unix_ms: host.now_ms()?,
    })
}

impl Host {
    fn now_ms(&self) -> Result<i64, Status> {
        i64::try_from(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_err(|_| Status::internal("clock before epoch"))?
                .as_millis(),
        )
        .map_err(|_| Status::internal("clock out of range"))
    }

    fn this_epoch(&self, host: sandbox_protocol::HostId, epoch: i64) -> Result<(), Status> {
        if host != self.inner.config.host || epoch != self.inner.config.epoch {
            return Err(Status::failed_precondition(
                "wrong host or supervisor epoch",
            ));
        }
        Ok(())
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
        let intent: HandoffIntent = warm_pool::decode(&request.intent_json)
            .map_err(|_| Status::invalid_argument("invalid handoff intent"))?;
        intent
            .validate()
            .map_err(|_| Status::invalid_argument("invalid handoff intent"))?;
        self.this_epoch(intent.host_id, intent.supervisor_epoch)?;
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
        reply(self, &WarmReceipt::Handoff { receipt })
    }

    pub(super) fn warm_retire_sync(
        &self,
        request: WarmRetireRequest,
    ) -> Result<WarmObservation, Status> {
        let intent: RetireIntent = warm_pool::decode(&request.intent_json)
            .map_err(|_| Status::invalid_argument("invalid retire intent"))?;
        if intent.version != VERSION {
            return Err(Status::invalid_argument("invalid retire intent"));
        }
        self.this_epoch(intent.host_id, intent.supervisor_epoch)?;
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
        reply(self, &WarmReceipt::Cleanup { receipt })
    }
}
