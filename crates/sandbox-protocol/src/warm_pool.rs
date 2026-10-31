//! Warm-pool intents, supervisor RPC payloads and receipts (#188).
//!
//! A fresh-create launch permit is not adoption authority. Adoption requires a
//! [`HandoffIntent`] persisted with the database claim and the registered
//! allocation [`Permit`] derived from it ([`HandoffIntent::permit`]); the
//! supervisor accepts the pair only if every field agrees, and its receipt
//! echoes the intent digest. Authority is the authenticated controller channel
//! plus permit registration, exactly as for Create: no detached signature or
//! new key is introduced. Receipts must echo the exact identity, so the store
//! rejects a receipt for any other member, allocation, project, host, epoch or
//! claim revision.
//!
//! The real Firecracker supervisor cannot adopt yet: its guest bootstrap device
//! binds the allocation identity at boot (see docs/warm-pools.md). It answers
//! these RPCs fail-closed; only the simulated fake host adopts.
use crate::{
    AllocationId, HostId, OperationId, ProjectId, SandboxId, allocation_authority::Permit,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
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

fn valid_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
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
/// Also the adoption scope: it names exactly one member VM, template, member
/// claim revision and the allocation permit (serial included) it may become.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HandoffIntent {
    pub version: u32,
    pub member_id: Uuid,
    pub vm_ref: String,
    pub template_id: Uuid,
    pub allocation_id: AllocationId,
    pub project_id: ProjectId,
    pub sandbox_id: SandboxId,
    pub create_operation_id: OperationId,
    pub generation: i64,
    pub permit_serial: u64,
    pub host_id: HostId,
    pub supervisor_epoch: i64,
    pub claim_revision: i64,
}

impl HandoffIntent {
    pub fn validate(&self) -> Result<(), Error> {
        if self.version != VERSION
            || !valid_vm_ref(&self.vm_ref)
            || self.generation != 1
            || self.supervisor_epoch <= 0
            || self.claim_revision <= 0
        {
            return Err(Error::Invalid);
        }
        self.permit().validate().map_err(|_| Error::Invalid)
    }

    /// The only allocation permit this intent may adopt into. The member was
    /// prepared at the same epoch, so the permit's original epoch is it.
    pub fn permit(&self) -> Permit {
        Permit {
            host: self.host_id,
            project: self.project_id,
            sandbox: self.sandbox_id,
            allocation: self.allocation_id,
            create_operation: self.create_operation_id,
            generation: self.generation,
            original_epoch: self.supervisor_epoch,
            serial: self.permit_serial,
        }
    }

    /// Hex SHA-256 of the canonical intent; echoed by every handoff receipt.
    pub fn digest(&self) -> Result<String, Error> {
        let bytes = serde_json::to_vec(self).map_err(|_| Error::Invalid)?;
        Ok(hex::encode(Sha256::digest(bytes)))
    }

    /// Check a supervisor request's permit and ownership fields against this
    /// intent. Any difference refuses adoption.
    pub fn admits(&self, permit: &Permit, owner: &crate::supervisor::Ownership) -> bool {
        *permit == self.permit()
            && owner.host_id == self.host_id.to_string()
            && owner.project_id == self.project_id.to_string()
            && owner.sandbox_id == self.sandbox_id.to_string()
            && owner.allocation_id == self.allocation_id.to_string()
            && owner.operation_id == self.create_operation_id.to_string()
            && owner.generation == self.generation
            && owner.supervisor_epoch == self.supervisor_epoch
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum HandoffOutcome {
    /// The supervisor bound the prepared VM to the allocation identity and
    /// issued its per-allocation guest channel identity before any customer
    /// work. Not readiness: the allocation is still inspected before running.
    Adopted {
        /// Hex SHA-256 of the per-allocation channel identity bound at adoption.
        /// Never a key; proves the identity was issued after the claim.
        identity_sha256: String,
    },
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
    /// [`HandoffIntent::digest`] of the scope the supervisor evaluated.
    pub intent_sha256: String,
    pub outcome: HandoffOutcome,
}

impl HandoffReceipt {
    pub fn verify(&self, intent: &HandoffIntent) -> Result<(), Error> {
        let bad_outcome = match &self.outcome {
            HandoffOutcome::Rejected { reason } => !valid_reason(reason),
            HandoffOutcome::Adopted { identity_sha256 } => !valid_sha256(identity_sha256),
        };
        if self.version != VERSION || !valid_vm_ref(&self.vm_ref) || bad_outcome {
            return Err(Error::Invalid);
        }
        if self.intent_sha256 != intent.digest()?
            || self.member_id != intent.member_id
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

/// Destroy one unassigned member. `vm_ref` is absent only when preparation
/// never reported a VM. Never names or touches an allocation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RetireIntent {
    pub version: u32,
    pub member_id: Uuid,
    pub vm_ref: Option<String>,
    pub host_id: HostId,
    pub supervisor_epoch: i64,
    pub operation_id: Uuid,
    pub operation_revision: i64,
}

/// Bounded supervisor answer for every warm RPC.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum WarmReceipt {
    Prepared { receipt: PrepareReceipt },
    Handoff { receipt: HandoffReceipt },
    Cleanup { receipt: CleanupReceipt },
}

/// Upper bound for any warm request or receipt JSON.
pub const MAX_JSON: usize = 16 * 1024;

pub fn decode<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> Result<T, Error> {
    if bytes.len() > MAX_JSON {
        return Err(Error::Invalid);
    }
    serde_json::from_slice(bytes).map_err(|_| Error::Invalid)
}

pub fn encode<T: Serialize>(value: &T) -> Result<Vec<u8>, Error> {
    let bytes = serde_json::to_vec(value).map_err(|_| Error::Invalid)?;
    if bytes.len() > MAX_JSON {
        return Err(Error::Invalid);
    }
    Ok(bytes)
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
            template_id: Uuid::now_v7(),
            allocation_id: AllocationId::generate(),
            project_id: ProjectId::generate(),
            sandbox_id: SandboxId::generate(),
            create_operation_id: OperationId::generate(),
            generation: 1,
            permit_serial: 7,
            host_id: HostId::generate(),
            supervisor_epoch: 3,
            claim_revision: 2,
        }
    }

    fn receipt(i: &HandoffIntent) -> HandoffReceipt {
        HandoffReceipt {
            intent_sha256: i.digest().unwrap(),
            version: VERSION,
            member_id: i.member_id,
            vm_ref: i.vm_ref.clone(),
            allocation_id: i.allocation_id,
            project_id: i.project_id,
            sandbox_id: i.sandbox_id,
            host_id: i.host_id,
            supervisor_epoch: i.supervisor_epoch,
            claim_revision: i.claim_revision,
            outcome: HandoffOutcome::Adopted {
                identity_sha256: "e".repeat(64),
            },
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
        // A receipt for a different scope (here another permit serial) fails
        // even when every echoed identity field matches.
        let mut other = i.clone();
        other.permit_serial += 1;
        assert_eq!(receipt(&other).verify(&i), Err(Error::Mismatch));
        let mut r = receipt(&i);
        r.outcome = HandoffOutcome::Adopted {
            identity_sha256: "not-hex".into(),
        };
        assert_eq!(r.verify(&i), Err(Error::Invalid));
    }

    #[test]
    fn adoption_scope_requires_matching_permit_and_owner() {
        let i = intent();
        i.validate().unwrap();
        let owner = crate::supervisor::Ownership {
            host_id: i.host_id.to_string(),
            project_id: i.project_id.to_string(),
            sandbox_id: i.sandbox_id.to_string(),
            allocation_id: i.allocation_id.to_string(),
            operation_id: i.create_operation_id.to_string(),
            generation: 1,
            supervisor_epoch: 3,
            claim_revision: 9,
            claim_expires_unix_ms: 1,
        };
        assert!(i.admits(&i.permit(), &owner));
        let mut permit = i.permit();
        permit.project = ProjectId::generate();
        assert!(!i.admits(&permit, &owner));
        let mut stale = owner.clone();
        stale.supervisor_epoch = 4;
        assert!(!i.admits(&i.permit(), &stale));
        let mut other = owner;
        other.project_id = ProjectId::generate().to_string();
        assert!(!i.admits(&i.permit(), &other));
        let mut bad = i.clone();
        bad.generation = 2;
        assert_eq!(bad.validate(), Err(Error::Invalid));
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
