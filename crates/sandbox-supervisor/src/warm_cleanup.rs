//! Host-local wire adapter for #216 while this PR stacks only on #217.
//! Keep these JSON shapes identical to #216; no guest/protocol edits here.
use crate::warm_member::{MemberRecord, MemberState, Refusal};
use sandbox_protocol::{
    HostId,
    warm_pool::{self, CleanupReceipt, RetireIntent, VERSION},
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PreviousRetireIntent {
    pub version: u32,
    pub original: RetireIntent,
    pub reporting_epoch: i64,
}

impl PreviousRetireIntent {
    pub(crate) fn validate(&self) -> Result<(), Refusal> {
        let i = &self.original;
        if self.version != VERSION
            || i.version != VERSION
            || i.supervisor_epoch <= 0
            || self.reporting_epoch <= i.supervisor_epoch
            || i.operation_revision <= 0
            || i.operation_id.is_nil()
            || i.member_id.is_nil()
            || i.vm_ref
                .as_deref()
                .is_some_and(|v| !warm_pool::valid_vm_ref(v))
        {
            return Err(Refusal::WrongScope);
        }
        Ok(())
    }

    /// Called under the member gate, persisted before touching the guardian.
    /// One cleanup operation per member; a new claim may advance its revision.
    pub(crate) fn admit(
        &self,
        record: &MemberRecord,
        saved: Option<&Self>,
        host: HostId,
        epoch: i64,
    ) -> Result<(), Refusal> {
        self.validate()?;
        let i = &self.original;
        if i.host_id != host
            || record.prepare.host_id != host
            || epoch != self.reporting_epoch
            || i.member_id != record.member()
            || i.supervisor_epoch != record.epoch()
            || i.vm_ref.as_ref().is_some_and(|v| v != &record.vm_ref)
        {
            return Err(Refusal::WrongScope);
        }
        if matches!(record.state, MemberState::Adopted { .. }) {
            return Err(Refusal::Adopted);
        }
        if let Some(saved) = saved {
            let mut retry = self.clone();
            retry.original.operation_revision = saved.original.operation_revision;
            retry.reporting_epoch = saved.reporting_epoch;
            if retry != *saved
                || self.original.operation_revision < saved.original.operation_revision
                || self.reporting_epoch < saved.reporting_epoch
            {
                return Err(Refusal::OtherScope);
            }
        }
        Ok(())
    }
}

#[derive(Debug, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub(crate) enum Reply {
    PreviousCleanup { receipt: PreviousCleanupReceipt },
}
#[derive(Debug, Serialize)]
pub(crate) struct PreviousCleanupReceipt {
    pub version: u32,
    pub request: PreviousRetireIntent,
    pub cleanup: CleanupReceipt,
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use sandbox_protocol::{AllocationId, Id, warm_pool::PrepareIntent};
    use uuid::Uuid;

    fn fixture() -> (MemberRecord, PreviousRetireIntent) {
        let host = HostId::generate();
        let r = MemberRecord::new(
            PrepareIntent {
                version: VERSION,
                member_id: Uuid::now_v7(),
                policy_id: Uuid::now_v7(),
                template_id: Uuid::now_v7(),
                host_id: host,
                supervisor_epoch: 1,
                image_digest: format!("sha256:{}", "a".repeat(64)),
                vcpu: 1,
                memory_mib: 512,
                disk_mib: 1024,
                operation_id: Uuid::now_v7(),
                operation_revision: 1,
            },
            AllocationId::generate(),
        );
        let p = PreviousRetireIntent {
            version: VERSION,
            reporting_epoch: 2,
            original: RetireIntent {
                version: VERSION,
                member_id: r.member(),
                host_id: host,
                supervisor_epoch: 1,
                vm_ref: None,
                operation_id: Uuid::now_v7(),
                operation_revision: 1,
            },
        };
        (r, p)
    }

    #[test]
    fn exact_retained_identity_and_reporting_epoch_required_before_effect() {
        let (r, p) = fixture();
        let host = r.prepare.host_id;
        p.admit(&r, None, host, 2).unwrap();
        let changes: [fn(&mut PreviousRetireIntent); 7] = [
            |p| p.original.member_id = Uuid::now_v7(),
            |p| p.original.host_id = HostId::generate(),
            |p| p.original.vm_ref = Some("warm-other".into()),
            |p| p.original.supervisor_epoch = 2,
            |p| p.original.operation_revision = 0,
            |p| p.original.operation_id = Uuid::nil(),
            |p| p.reporting_epoch = 3,
        ];
        for change in changes {
            let mut bad = p.clone();
            change(&mut bad);
            assert!(bad.admit(&r, None, host, 2).is_err());
        }
        let mut named = p.clone();
        named.original.vm_ref = Some(r.vm_ref.clone());
        named.admit(&r, None, host, 2).unwrap();
        assert!(p.admit(&r, None, HostId::generate(), 2).is_err());
    }

    #[test]
    fn lost_cleanup_reply_reclaim_and_restart_keep_claim_fence_and_capacity() {
        let (mut r, p) = fixture();
        let host = r.prepare.host_id;
        p.admit(&r, None, host, 2).unwrap();
        r.begin_retire(&p.original, host).unwrap();
        assert!(r.holds_capacity());
        assert!(r.cleanup_receipt().is_none());
        let saved: PreviousRetireIntent =
            serde_json::from_slice(&serde_json::to_vec(&p).unwrap()).unwrap();
        let mut r: MemberRecord = serde_json::from_slice(&serde_json::to_vec(&r).unwrap()).unwrap();
        r.validate(host).unwrap();
        let mut retry = saved.clone();
        retry.original.operation_revision += 1;
        retry.admit(&r, Some(&saved), host, 2).unwrap();
        assert!(saved.admit(&r, Some(&retry), host, 2).is_err());
        let mut other = retry.clone();
        other.original.operation_id = Uuid::now_v7();
        assert!(other.admit(&r, Some(&retry), host, 2).is_err());
        // A further host incarnation may echo a fresh reporting epoch, but the
        // original operation and exact retained VM ownership cannot change.
        let mut next_epoch = retry.clone();
        next_epoch.reporting_epoch = 3;
        next_epoch.admit(&r, Some(&retry), host, 3).unwrap();
        assert!(retry.admit(&r, Some(&next_epoch), host, 3).is_err());
        r.mark_retired().unwrap();
        assert!(!r.holds_capacity());
        assert_eq!(r.cleanup_receipt().unwrap().vm_ref, Some(r.vm_ref.clone()));
        assert!(r.mark_ready("late-prepare").is_err());
        retry.admit(&r, Some(&retry), host, 2).unwrap();
    }

    #[test]
    fn wire_shape_matches_216_and_rejects_unknown_or_oversized_envelopes() {
        let (mut r, p) = fixture();
        let bytes = warm_pool::encode(&p).unwrap();
        assert!(warm_pool::decode::<RetireIntent>(&bytes).is_err());
        let mut value = serde_json::to_value(&p).unwrap();
        value["extra"] = true.into();
        assert!(
            warm_pool::decode::<PreviousRetireIntent>(&serde_json::to_vec(&value).unwrap())
                .is_err()
        );
        assert!(
            warm_pool::decode::<PreviousRetireIntent>(&vec![b' '; warm_pool::MAX_JSON + 1])
                .is_err()
        );
        r.begin_retire(&p.original, r.prepare.host_id).unwrap();
        r.mark_retired().unwrap();
        let value = serde_json::to_value(Reply::PreviousCleanup {
            receipt: PreviousCleanupReceipt {
                version: VERSION,
                request: p.clone(),
                cleanup: r.cleanup_receipt().unwrap(),
            },
        })
        .unwrap();
        assert_eq!(value["kind"], "previous_cleanup");
        assert_eq!(
            value["receipt"]["request"],
            serde_json::to_value(p).unwrap()
        );
        assert_eq!(value["receipt"]["cleanup"]["supervisor_epoch"], 1);
    }
}
