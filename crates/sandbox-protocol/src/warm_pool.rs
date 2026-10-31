//! Warm-pool intents and supervisor receipts (#188). Shared vocabulary only.
//!
//! The deployed supervisor does not implement prepared-VM preparation, adoption
//! or warm cleanup RPCs yet, and an existing allocation launch permit is not
//! adoption authority. These types define the exact identity a future receipt
//! must echo so the store can reject a receipt for any other member,
//! allocation, project, host, epoch or claim revision.
use crate::{AllocationId, HostId, ProjectId, SandboxId};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub const VERSION: u32 = 1;
const MAX_VM_REF: usize = 128;
const MAX_REASON: usize = 64;

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum Error {
    #[error("warm-pool receipt is malformed")]
    Invalid,
    #[error("warm-pool receipt does not match its durable intent")]
    Mismatch,
}

/// Opaque supervisor-local VM name. Never authority by itself.
pub fn valid_vm_ref(value: &str) -> bool {
    let bytes = value.as_bytes();
    !bytes.is_empty()
        && bytes.len() <= MAX_VM_REF
        && bytes[0].is_ascii_alphanumeric()
        && bytes
            .iter()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b':' | b'-'))
}

fn valid_reason(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= MAX_REASON
        && value
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
}

/// Boot one unused VM from an immutable template on a host at an epoch.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PrepareIntent {
    pub version: u32,
    pub member_id: Uuid,
    pub policy_id: Uuid,
    pub template_id: Uuid,
    pub host_id: HostId,
    pub supervisor_epoch: i64,
    pub image_digest: String,
    pub vcpu: u32,
    pub memory_mib: u64,
    pub disk_mib: u64,
    /// Capacity-operation claim that authorized this attempt.
    pub operation_id: Uuid,
    pub operation_revision: i64,
}

/// Supervisor evidence that an unused VM booted from the exact template image
/// with the exact reserved size and holds no project identity or secret.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PrepareReceipt {
    pub version: u32,
    pub member_id: Uuid,
    pub template_id: Uuid,
    pub host_id: HostId,
    pub supervisor_epoch: i64,
    pub image_digest: String,
    pub vcpu: u32,
    pub memory_mib: u64,
    pub disk_mib: u64,
    pub vm_ref: String,
}

impl PrepareReceipt {
    pub fn verify(&self, intent: &PrepareIntent) -> Result<(), Error> {
        if self.version != VERSION || !valid_vm_ref(&self.vm_ref) {
            return Err(Error::Invalid);
        }
        if self.member_id != intent.member_id
            || self.template_id != intent.template_id
            || self.host_id != intent.host_id
            || self.supervisor_epoch != intent.supervisor_epoch
            || self.image_digest != intent.image_digest
            || self.vcpu != intent.vcpu
            || self.memory_mib != intent.memory_mib
            || self.disk_mib != intent.disk_mib
        {
            return Err(Error::Mismatch);
        }
        Ok(())
    }
}

/// Durable handoff intent persisted with the claim, before any supervisor RPC.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HandoffIntent {
    pub version: u32,
    pub member_id: Uuid,
    pub vm_ref: String,
    pub allocation_id: AllocationId,
    pub project_id: ProjectId,
    pub sandbox_id: SandboxId,
    pub host_id: HostId,
    pub supervisor_epoch: i64,
    pub claim_revision: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum HandoffOutcome {
    /// The supervisor bound the prepared VM to the allocation identity. Not
    /// readiness: the allocation is still inspected before running.
    Adopted,
    /// The supervisor proved it did not adopt the VM for this allocation. The
    /// member is still retired; it is never offered again.
    Rejected { reason: String },
}

/// Supervisor evidence for one handoff attempt. Exact identity echo required.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HandoffReceipt {
    pub version: u32,
    pub member_id: Uuid,
    pub vm_ref: String,
    pub allocation_id: AllocationId,
    pub project_id: ProjectId,
    pub sandbox_id: SandboxId,
    pub host_id: HostId,
    pub supervisor_epoch: i64,
    pub claim_revision: i64,
    pub outcome: HandoffOutcome,
}

impl HandoffReceipt {
    pub fn verify(&self, intent: &HandoffIntent) -> Result<(), Error> {
        if self.version != VERSION
            || !valid_vm_ref(&self.vm_ref)
            || matches!(&self.outcome, HandoffOutcome::Rejected { reason } if !valid_reason(reason))
        {
            return Err(Error::Invalid);
        }
        if self.member_id != intent.member_id
            || self.vm_ref != intent.vm_ref
            || self.allocation_id != intent.allocation_id
            || self.project_id != intent.project_id
            || self.sandbox_id != intent.sandbox_id
            || self.host_id != intent.host_id
            || self.supervisor_epoch != intent.supervisor_epoch
            || self.claim_revision != intent.claim_revision
        {
            return Err(Error::Mismatch);
        }
        Ok(())
    }
}

/// Supervisor evidence that an unassigned member's VM and its disk/state are
/// destroyed. Missing local records or heartbeat expiry are never this receipt.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CleanupReceipt {
    pub version: u32,
    pub member_id: Uuid,
    /// Absent only when preparation never reported a VM name.
    pub vm_ref: Option<String>,
    pub host_id: HostId,
    pub supervisor_epoch: i64,
    pub destroyed: bool,
}

impl CleanupReceipt {
    pub fn verify(
        &self,
        member_id: Uuid,
        vm_ref: Option<&str>,
        host_id: HostId,
        supervisor_epoch: i64,
    ) -> Result<(), Error> {
        if self.version != VERSION
            || !self.destroyed
            || self.vm_ref.as_deref().is_some_and(|v| !valid_vm_ref(v))
        {
            return Err(Error::Invalid);
        }
        if self.member_id != member_id
            || self.vm_ref.as_deref() != vm_ref
            || self.host_id != host_id
            || self.supervisor_epoch != supervisor_epoch
        {
            return Err(Error::Mismatch);
        }
        Ok(())
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::Id;

    fn intent() -> HandoffIntent {
        HandoffIntent {
            version: VERSION,
            member_id: Uuid::now_v7(),
            vm_ref: "warm-1".into(),
            allocation_id: AllocationId::generate(),
            project_id: ProjectId::generate(),
            sandbox_id: SandboxId::generate(),
            host_id: HostId::generate(),
            supervisor_epoch: 3,
            claim_revision: 2,
        }
    }

    fn receipt(i: &HandoffIntent) -> HandoffReceipt {
        HandoffReceipt {
            version: VERSION,
            member_id: i.member_id,
            vm_ref: i.vm_ref.clone(),
            allocation_id: i.allocation_id,
            project_id: i.project_id,
            sandbox_id: i.sandbox_id,
            host_id: i.host_id,
            supervisor_epoch: i.supervisor_epoch,
            claim_revision: i.claim_revision,
            outcome: HandoffOutcome::Adopted,
        }
    }

    #[test]
    fn handoff_receipt_must_echo_every_identity() {
        let i = intent();
        receipt(&i).verify(&i).unwrap();
        let mutations: [fn(&mut HandoffReceipt); 8] = [
            |r| r.member_id = Uuid::now_v7(),
            |r| r.vm_ref = "warm-2".into(),
            |r| r.allocation_id = AllocationId::generate(),
            |r| r.project_id = ProjectId::generate(),
            |r| r.sandbox_id = SandboxId::generate(),
            |r| r.host_id = HostId::generate(),
            |r| r.supervisor_epoch += 1,
            |r| r.claim_revision -= 1,
        ];
        for mutate in mutations {
            let mut r = receipt(&i);
            mutate(&mut r);
            assert_eq!(r.verify(&i), Err(Error::Mismatch));
        }
        let mut r = receipt(&i);
        r.outcome = HandoffOutcome::Rejected {
            reason: "Bad Reason".into(),
        };
        assert_eq!(r.verify(&i), Err(Error::Invalid));
    }

    #[test]
    fn handoff_receipt_round_trips_with_tagged_outcome() {
        let i = intent();
        let mut r = receipt(&i);
        r.outcome = HandoffOutcome::Rejected {
            reason: "epoch_fenced".into(),
        };
        let value = serde_json::to_value(&r).unwrap();
        assert_eq!(value["outcome"]["kind"], "rejected");
        assert_eq!(value["outcome"]["reason"], "epoch_fenced");
        assert_eq!(serde_json::from_value::<HandoffReceipt>(value).unwrap(), r);
    }

    #[test]
    fn cleanup_requires_destruction_and_exact_member() {
        let host = HostId::generate();
        let member = Uuid::now_v7();
        let ok = CleanupReceipt {
            version: VERSION,
            member_id: member,
            vm_ref: Some("warm-1".into()),
            host_id: host,
            supervisor_epoch: 1,
            destroyed: true,
        };
        ok.verify(member, Some("warm-1"), host, 1).unwrap();
        assert_eq!(
            ok.verify(member, Some("warm-1"), host, 2),
            Err(Error::Mismatch)
        );
        assert_eq!(ok.verify(member, None, host, 1), Err(Error::Mismatch));
        let mut absent = ok.clone();
        absent.destroyed = false;
        assert_eq!(
            absent.verify(member, Some("warm-1"), host, 1),
            Err(Error::Invalid)
        );
        assert!(!valid_vm_ref("../escape"));
    }
}
