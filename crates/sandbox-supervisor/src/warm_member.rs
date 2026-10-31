//! Durable ownership of one real-host warm member (#188). Portable and I/O
//! free: the Linux host persists [`MemberRecord`] in its journal before every
//! effect and only acts on the [`Step`] a transition returns.
//!
//! Ownership moves `member -> allocation` in one direction only:
//!
//! - `Preparing`: durable reservation written before the guardian launches.
//!   The VM booted from an inert member bootstrap (no project, sandbox,
//!   operation, token or customer secret) and runs no customer work.
//! - `Ready`: the member endpoint authenticated this exact boot.
//! - `Binding`: the exact [`HandoffIntent`] is fenced durably before any
//!   guardian re-key or guest bind call. No other scope can ever bind.
//! - `Adopted`: the guest acknowledged the allocation identity and the
//!   allocation channel authenticated the same boot. Only now may customer
//!   work start, and only through the ordinary allocation record.
//! - `Fenced`: inspect fenced an unadopted member against one scope.
//! - `Retiring` / `Retired`: destruction requested / proven by the guardian.
//!
//! A member that ever left `Ready` never returns to it (no repooling), and
//! every state except `Retired` and `Adopted` holds its capacity reservation.
//! `Adopted` capacity is held by the allocation record instead.
use sandbox_protocol::{
    AllocationId, HostId,
    warm_pool::{
        CleanupReceipt, HandoffIntent, HandoffOutcome, HandoffReceipt, PrepareIntent,
        PrepareReceipt, RetireIntent, VERSION,
    },
};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Version of a persisted [`MemberRecord`]. Unknown versions fail closed.
pub const RECORD_VERSION: u32 = 1;
const MAX_REASON: usize = 64;
const MAX_BOOT_ID: usize = 64;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case", deny_unknown_fields)]
pub enum MemberState {
    Preparing,
    Ready {
        boot_id: String,
    },
    Binding {
        intent: HandoffIntent,
        intent_sha256: String,
        boot_id: String,
    },
    Adopted {
        intent: HandoffIntent,
        intent_sha256: String,
        identity_sha256: String,
        boot_id: String,
    },
    Fenced {
        intent: HandoffIntent,
        intent_sha256: String,
        reason: String,
    },
    Retiring {
        reason: String,
    },
    Retired,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MemberRecord {
    pub version: u32,
    pub prepare: PrepareIntent,
    /// Host-chosen key of the guardian directory, jail, cgroup and channel.
    /// Never an allocation the controller issued.
    pub slot: AllocationId,
    pub vm_ref: String,
    pub state: MemberState,
    /// Incremented by every durable transition.
    pub revision: u64,
}

/// What the host must do after durably saving the record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Step {
    /// Answer with this receipt; no effect is required.
    Answer(HandoffReceipt),
    /// Re-key the guardian and bind the guest for this exact intent, then call
    /// [`MemberRecord::finish_bind`] or [`MemberRecord::fail_bind`].
    Bind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum Refusal {
    #[error("warm request names another host, epoch, member or template")]
    WrongScope,
    #[error("warm prepare retry changed its input")]
    Changed,
    #[error("warm member is bound to another scope")]
    OtherScope,
    #[error("warm member was adopted; destroy it through its allocation")]
    Adopted,
    #[error("warm member state does not permit this transition")]
    State,
}

fn reason(value: &str) -> String {
    debug_assert!(
        !value.is_empty()
            && value.len() <= MAX_REASON
            && value
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
    );
    value.into()
}

/// The prepare fields a retry must repeat exactly. The capacity-operation
/// claim (`operation_id`, `operation_revision`) may change between attempts.
fn same_prepare(a: &PrepareIntent, b: &PrepareIntent) -> bool {
    a.version == b.version
        && a.member_id == b.member_id
        && a.policy_id == b.policy_id
        && a.template_id == b.template_id
        && a.host_id == b.host_id
        && a.supervisor_epoch == b.supervisor_epoch
        && a.image_digest == b.image_digest
        && a.vcpu == b.vcpu
        && a.memory_mib == b.memory_mib
        && a.disk_mib == b.disk_mib
}

impl MemberRecord {
    /// A durable reservation for `prepare` keyed by the fresh `slot`.
    pub fn new(prepare: PrepareIntent, slot: AllocationId) -> Self {
        use sandbox_protocol::Id as _;
        Self {
            version: RECORD_VERSION,
            vm_ref: format!("warm-{}", slot.uuid()),
            prepare,
            slot,
            state: MemberState::Preparing,
            revision: 1,
        }
    }

    pub fn member(&self) -> Uuid {
        self.prepare.member_id
    }

    pub fn epoch(&self) -> i64 {
        self.prepare.supervisor_epoch
    }

    /// Structural check for a record loaded from the journal.
    pub fn validate(&self, host: HostId) -> Result<(), Refusal> {
        use sandbox_protocol::Id as _;
        let boot = |b: &str| !b.is_empty() && b.len() <= MAX_BOOT_ID;
        let scoped = |i: &HandoffIntent, d: &str| {
            i.validate().is_ok()
                && i.digest().is_ok_and(|x| x == d)
                && i.member_id == self.prepare.member_id
                && i.vm_ref == self.vm_ref
                && i.template_id == self.prepare.template_id
                && i.host_id == host
                && i.supervisor_epoch == self.prepare.supervisor_epoch
        };
        let ok = self.version == RECORD_VERSION
            && self.prepare.version == VERSION
            && self.prepare.host_id == host
            && self.prepare.supervisor_epoch > 0
            && !self.slot.uuid().is_nil()
            && self.vm_ref == format!("warm-{}", self.slot.uuid())
            && self.revision > 0
            && match &self.state {
                MemberState::Preparing | MemberState::Retired => true,
                MemberState::Ready { boot_id } => boot(boot_id),
                MemberState::Binding {
                    intent,
                    intent_sha256,
                    boot_id,
                } => boot(boot_id) && scoped(intent, intent_sha256),
                MemberState::Adopted {
                    intent,
                    intent_sha256,
                    identity_sha256,
                    boot_id,
                } => {
                    boot(boot_id)
                        && scoped(intent, intent_sha256)
                        && identity_sha256.len() == 64
                        && identity_sha256
                            .bytes()
                            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
                }
                MemberState::Fenced {
                    intent,
                    intent_sha256,
                    reason,
                } => scoped(intent, intent_sha256) && !reason.is_empty() && reason.len() <= 64,
                MemberState::Retiring { reason } => !reason.is_empty() && reason.len() <= 64,
            };
        if ok { Ok(()) } else { Err(Refusal::State) }
    }

    /// Whether this record still reserves host capacity for the member VM.
    pub fn holds_capacity(&self) -> bool {
        !matches!(
            self.state,
            MemberState::Retired | MemberState::Adopted { .. }
        )
    }

    /// Whether the guardian may still own a VM for this member.
    pub fn needs_cleanup(&self) -> bool {
        !matches!(
            self.state,
            MemberState::Retired | MemberState::Adopted { .. }
        )
    }

    fn advance(&mut self, state: MemberState) {
        self.state = state;
        self.revision += 1;
    }

    /// Exact retry check. Prepare never relaunches an existing member.
    pub fn check_prepare_retry(&self, intent: &PrepareIntent) -> Result<(), Refusal> {
        if same_prepare(&self.prepare, intent) {
            Ok(())
        } else {
            Err(Refusal::Changed)
        }
    }

    /// The receipt the store may record. Issued only for an authenticated,
    /// still-unbound boot; preparation in flight answers `None` (uncertain).
    pub fn prepare_receipt(&self) -> Result<Option<PrepareReceipt>, Refusal> {
        match &self.state {
            MemberState::Preparing => Ok(None),
            MemberState::Ready { .. } => Ok(Some(PrepareReceipt {
                version: VERSION,
                member_id: self.prepare.member_id,
                template_id: self.prepare.template_id,
                host_id: self.prepare.host_id,
                supervisor_epoch: self.prepare.supervisor_epoch,
                image_digest: self.prepare.image_digest.clone(),
                vcpu: self.prepare.vcpu,
                memory_mib: self.prepare.memory_mib,
                disk_mib: self.prepare.disk_mib,
                vm_ref: self.vm_ref.clone(),
            })),
            _ => Err(Refusal::State),
        }
    }

    /// The inert member endpoint authenticated `boot_id`.
    pub fn mark_ready(&mut self, boot_id: &str) -> Result<bool, Refusal> {
        match &self.state {
            MemberState::Preparing if !boot_id.is_empty() && boot_id.len() <= MAX_BOOT_ID => {
                self.advance(MemberState::Ready {
                    boot_id: boot_id.into(),
                });
                Ok(true)
            }
            MemberState::Ready { boot_id: b } if b == boot_id => Ok(false),
            _ => Err(Refusal::State),
        }
    }

    fn check_scope(&self, intent: &HandoffIntent, host: HostId) -> Result<String, Refusal> {
        intent.validate().map_err(|_| Refusal::WrongScope)?;
        if intent.member_id != self.prepare.member_id
            || intent.vm_ref != self.vm_ref
            || intent.template_id != self.prepare.template_id
            || intent.host_id != host
            || intent.host_id != self.prepare.host_id
            || intent.supervisor_epoch != self.prepare.supervisor_epoch
        {
            return Err(Refusal::WrongScope);
        }
        intent.digest().map_err(|_| Refusal::WrongScope)
    }

    fn receipt(intent: &HandoffIntent, digest: &str, outcome: HandoffOutcome) -> HandoffReceipt {
        HandoffReceipt {
            version: VERSION,
            member_id: intent.member_id,
            vm_ref: intent.vm_ref.clone(),
            allocation_id: intent.allocation_id,
            project_id: intent.project_id,
            sandbox_id: intent.sandbox_id,
            host_id: intent.host_id,
            supervisor_epoch: intent.supervisor_epoch,
            claim_revision: intent.claim_revision,
            intent_sha256: digest.into(),
            outcome,
        }
    }

    fn rejected(intent: &HandoffIntent, digest: &str, why: &str) -> Step {
        Step::Answer(Self::receipt(
            intent,
            digest,
            HandoffOutcome::Rejected {
                reason: reason(why),
            },
        ))
    }

    /// Recorded answer for `intent`, if this scope already has a final one.
    fn recorded(&self, intent: &HandoffIntent, digest: &str) -> Result<Option<Step>, Refusal> {
        match &self.state {
            MemberState::Adopted {
                intent_sha256,
                identity_sha256,
                ..
            } => {
                if intent_sha256 != digest {
                    return Err(Refusal::OtherScope);
                }
                Ok(Some(Step::Answer(Self::receipt(
                    intent,
                    digest,
                    HandoffOutcome::Adopted {
                        identity_sha256: identity_sha256.clone(),
                    },
                ))))
            }
            MemberState::Fenced {
                intent_sha256,
                reason,
                ..
            } => {
                if intent_sha256 != digest {
                    return Err(Refusal::OtherScope);
                }
                Ok(Some(Self::rejected(intent, digest, reason)))
            }
            MemberState::Binding { intent_sha256, .. } if intent_sha256 != digest => {
                Err(Refusal::OtherScope)
            }
            _ => Ok(None),
        }
    }

    /// Adopt. The caller has already verified the registered permit and the
    /// operation ownership against `intent`. Returns whether the record
    /// changed (save before acting) and the step.
    pub fn begin_bind(
        &mut self,
        intent: &HandoffIntent,
        host: HostId,
    ) -> Result<(bool, Step), Refusal> {
        let digest = self.check_scope(intent, host)?;
        if let Some(step) = self.recorded(intent, &digest)? {
            return Ok((false, step));
        }
        match &self.state {
            MemberState::Ready { boot_id } => {
                let boot_id = boot_id.clone();
                self.advance(MemberState::Binding {
                    intent: intent.clone(),
                    intent_sha256: digest,
                    boot_id,
                });
                Ok((true, Step::Bind))
            }
            // Same scope after a lost acknowledgement within this epoch: the
            // guest accepts an exact retry and refuses any other scope.
            MemberState::Binding { .. } => Ok((false, Step::Bind)),
            // A member that never authenticated or is being destroyed cannot be
            // bound. Fence the scope so a delayed retry cannot apply later.
            MemberState::Preparing => {
                self.advance(MemberState::Fenced {
                    intent: intent.clone(),
                    intent_sha256: digest.clone(),
                    reason: reason("member_not_ready"),
                });
                Ok((true, Self::rejected(intent, &digest, "member_not_ready")))
            }
            MemberState::Retiring { .. } | MemberState::Retired => {
                Ok((false, Self::rejected(intent, &digest, "member_retired")))
            }
            MemberState::Adopted { .. } | MemberState::Fenced { .. } => Err(Refusal::State),
        }
    }

    /// The guest acknowledged `identity_sha256` and the allocation channel
    /// authenticated the member's boot.
    pub fn finish_bind(&mut self, identity_sha256: &str) -> Result<HandoffReceipt, Refusal> {
        let MemberState::Binding {
            intent,
            intent_sha256,
            boot_id,
        } = &self.state
        else {
            return Err(Refusal::State);
        };
        let (intent, digest, boot_id) = (intent.clone(), intent_sha256.clone(), boot_id.clone());
        self.advance(MemberState::Adopted {
            intent: intent.clone(),
            intent_sha256: digest.clone(),
            identity_sha256: identity_sha256.into(),
            boot_id,
        });
        Ok(Self::receipt(
            &intent,
            &digest,
            HandoffOutcome::Adopted {
                identity_sha256: identity_sha256.into(),
            },
        ))
    }

    /// Definitive refusal while binding (the guest refused, the permit was
    /// revoked, or the guardian stopped). The VM may hold a partial binding,
    /// so it is destroyed, never repooled. Uncertain failures must not call
    /// this: they leave `Binding` for inspect to fence.
    pub fn fail_bind(&mut self, why: &str) -> Result<HandoffReceipt, Refusal> {
        let MemberState::Binding {
            intent,
            intent_sha256,
            ..
        } = &self.state
        else {
            return Err(Refusal::State);
        };
        let (intent, digest) = (intent.clone(), intent_sha256.clone());
        self.advance(MemberState::Fenced {
            intent: intent.clone(),
            intent_sha256: digest.clone(),
            reason: reason(why),
        });
        Ok(Self::receipt(
            &intent,
            &digest,
            HandoffOutcome::Rejected {
                reason: reason(why),
            },
        ))
    }

    /// InspectWarmHandoff never adopts. It answers a recorded outcome or
    /// fences the member against this exact scope.
    pub fn inspect(
        &mut self,
        intent: &HandoffIntent,
        host: HostId,
    ) -> Result<(bool, HandoffReceipt), Refusal> {
        let digest = self.check_scope(intent, host)?;
        if let Some(Step::Answer(receipt)) = self.recorded(intent, &digest)? {
            return Ok((false, receipt));
        }
        match &self.state {
            MemberState::Retiring { .. } | MemberState::Retired => Ok((
                false,
                Self::receipt(
                    intent,
                    &digest,
                    HandoffOutcome::Rejected {
                        reason: reason("member_retired"),
                    },
                ),
            )),
            // Ready, Preparing, or an interrupted Binding of this scope: never
            // completed, so fence it. A guest that did install the identity is
            // destroyed with the member before any customer work.
            _ => {
                self.advance(MemberState::Fenced {
                    intent: intent.clone(),
                    intent_sha256: digest.clone(),
                    reason: reason("fenced_by_inspect"),
                });
                Ok((
                    true,
                    Self::receipt(
                        intent,
                        &digest,
                        HandoffOutcome::Rejected {
                            reason: reason("fenced_by_inspect"),
                        },
                    ),
                ))
            }
        }
    }

    /// Retire. Returns whether the record changed. Adopted members are only
    /// destroyed through their allocation.
    pub fn begin_retire(&mut self, intent: &RetireIntent, host: HostId) -> Result<bool, Refusal> {
        if intent.version != VERSION
            || intent.member_id != self.prepare.member_id
            || intent.host_id != host
            || intent.supervisor_epoch != self.prepare.supervisor_epoch
            || intent.vm_ref.as_ref().is_some_and(|v| *v != self.vm_ref)
        {
            return Err(Refusal::WrongScope);
        }
        match &self.state {
            MemberState::Adopted { .. } => Err(Refusal::Adopted),
            MemberState::Retiring { .. } | MemberState::Retired => Ok(false),
            _ => {
                self.advance(MemberState::Retiring {
                    reason: reason("retire_requested"),
                });
                Ok(true)
            }
        }
    }

    /// Host-initiated destruction (expired TTL, failed boot, previous epoch).
    pub fn retire_locally(&mut self, why: &str) -> bool {
        match &self.state {
            MemberState::Adopted { .. } | MemberState::Retiring { .. } | MemberState::Retired => {
                false
            }
            _ => {
                self.advance(MemberState::Retiring {
                    reason: reason(why),
                });
                true
            }
        }
    }

    /// Record guardian-proven destruction (empty cgroup, removed runtime files).
    pub fn mark_retired(&mut self) -> Result<bool, Refusal> {
        match &self.state {
            MemberState::Retiring { .. } => {
                self.advance(MemberState::Retired);
                Ok(true)
            }
            MemberState::Retired => Ok(false),
            _ => Err(Refusal::State),
        }
    }

    /// Cleanup evidence, available only once destruction is proven.
    pub fn cleanup_receipt(&self) -> Option<CleanupReceipt> {
        matches!(self.state, MemberState::Retired).then(|| CleanupReceipt {
            version: VERSION,
            member_id: self.prepare.member_id,
            vm_ref: Some(self.vm_ref.clone()),
            host_id: self.prepare.host_id,
            supervisor_epoch: self.prepare.supervisor_epoch,
            destroyed: true,
        })
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use sandbox_protocol::{Id, OperationId, ProjectId, SandboxId};

    fn prepare(host: HostId) -> PrepareIntent {
        PrepareIntent {
            version: VERSION,
            member_id: Uuid::now_v7(),
            policy_id: Uuid::now_v7(),
            template_id: Uuid::now_v7(),
            host_id: host,
            supervisor_epoch: 4,
            image_digest: format!("sha256:{}", "a".repeat(64)),
            vcpu: 1,
            memory_mib: 512,
            disk_mib: 1024,
            operation_id: Uuid::now_v7(),
            operation_revision: 1,
        }
    }

    fn ready(host: HostId) -> MemberRecord {
        let mut r = MemberRecord::new(prepare(host), AllocationId::generate());
        assert!(r.mark_ready("boot-1").unwrap());
        r
    }

    fn intent(r: &MemberRecord) -> HandoffIntent {
        HandoffIntent {
            version: VERSION,
            member_id: r.member(),
            vm_ref: r.vm_ref.clone(),
            template_id: r.prepare.template_id,
            allocation_id: AllocationId::generate(),
            project_id: ProjectId::generate(),
            sandbox_id: SandboxId::generate(),
            create_operation_id: OperationId::generate(),
            generation: 1,
            permit_serial: 7,
            host_id: r.prepare.host_id,
            supervisor_epoch: r.epoch(),
            claim_revision: 1,
        }
    }

    fn outcome(step: Step) -> HandoffOutcome {
        match step {
            Step::Answer(r) => r.outcome,
            Step::Bind => panic!("expected answer"),
        }
    }

    #[test]
    fn prepare_receipt_only_after_authenticated_boot_and_retry_is_exact() {
        let host = HostId::generate();
        let mut r = MemberRecord::new(prepare(host), AllocationId::generate());
        r.validate(host).unwrap();
        assert!(r.holds_capacity());
        assert_eq!(r.prepare_receipt().unwrap(), None);
        let mut retry = r.prepare.clone();
        retry.operation_id = Uuid::now_v7();
        retry.operation_revision = 2;
        r.check_prepare_retry(&retry).unwrap();
        retry.memory_mib += 1;
        assert_eq!(r.check_prepare_retry(&retry), Err(Refusal::Changed));
        assert!(r.mark_ready("boot-1").unwrap());
        assert!(!r.mark_ready("boot-1").unwrap());
        assert_eq!(r.mark_ready("boot-2"), Err(Refusal::State));
        let receipt = r.prepare_receipt().unwrap().unwrap();
        receipt.verify(&r.prepare).unwrap();
    }

    #[test]
    fn adoption_is_fenced_before_effects_idempotent_and_exclusive() {
        let host = HostId::generate();
        let mut r = ready(host);
        let i = intent(&r);
        let (changed, step) = r.begin_bind(&i, host).unwrap();
        assert!(changed && step == Step::Bind);
        r.validate(host).unwrap();
        // Lost acknowledgement of the same scope resumes, never re-fences.
        assert_eq!(r.begin_bind(&i, host).unwrap(), (false, Step::Bind));
        let mut other = intent(&r);
        other.claim_revision = 2;
        assert_eq!(r.begin_bind(&other, host), Err(Refusal::OtherScope));
        assert_eq!(r.inspect(&other, host), Err(Refusal::OtherScope));
        let receipt = r.finish_bind(&"b".repeat(64)).unwrap();
        receipt.verify(&i).unwrap();
        assert!(!r.holds_capacity() && !r.needs_cleanup());
        // Retries and inspection return the recorded receipt.
        let (changed, step) = r.begin_bind(&i, host).unwrap();
        assert!(!changed);
        assert_eq!(step, Step::Answer(receipt.clone()));
        assert_eq!(r.inspect(&i, host).unwrap(), (false, receipt));
        assert_eq!(r.begin_bind(&other, host), Err(Refusal::OtherScope));
        // Never retired or repooled as a member.
        let retire = RetireIntent {
            version: VERSION,
            member_id: r.member(),
            vm_ref: None,
            host_id: host,
            supervisor_epoch: r.epoch(),
            operation_id: Uuid::now_v7(),
            operation_revision: 1,
        };
        assert_eq!(r.begin_retire(&retire, host), Err(Refusal::Adopted));
        assert!(!r.retire_locally("idle_ttl_expired"));
        assert_eq!(r.mark_ready("boot-1"), Err(Refusal::State));
        assert_eq!(r.prepare_receipt(), Err(Refusal::State));
    }

    #[test]
    fn inspect_fences_unadopted_members_and_a_late_adopt_cannot_apply() {
        let host = HostId::generate();
        for interrupted in [false, true] {
            let mut r = ready(host);
            let i = intent(&r);
            if interrupted {
                r.begin_bind(&i, host).unwrap();
            }
            let (changed, receipt) = r.inspect(&i, host).unwrap();
            assert!(changed);
            receipt.verify(&i).unwrap();
            assert_eq!(
                receipt.outcome,
                HandoffOutcome::Rejected {
                    reason: "fenced_by_inspect".into()
                }
            );
            let (changed, step) = r.begin_bind(&i, host).unwrap();
            assert!(!changed);
            assert_eq!(outcome(step), receipt.outcome);
            assert_eq!(r.finish_bind(&"c".repeat(64)), Err(Refusal::State));
            assert!(r.holds_capacity() && r.needs_cleanup());
            assert_eq!(r.mark_ready("boot-1"), Err(Refusal::State));
            assert_eq!(r.mark_retired(), Err(Refusal::State));
            assert!(r.retire_locally("fenced"));
            assert!(r.holds_capacity());
            assert!(r.cleanup_receipt().is_none());
            assert!(r.mark_retired().unwrap());
            assert!(!r.holds_capacity());
            r.cleanup_receipt()
                .unwrap()
                .verify(r.member(), Some(&r.vm_ref), host, r.epoch())
                .unwrap();
        }
    }

    #[test]
    fn definitive_bind_failure_destroys_rather_than_repools() {
        let host = HostId::generate();
        let mut r = ready(host);
        let i = intent(&r);
        r.begin_bind(&i, host).unwrap();
        let receipt = r.fail_bind("guest_refused_binding").unwrap();
        receipt.verify(&i).unwrap();
        assert_eq!(r.prepare_receipt(), Err(Refusal::State));
        assert!(r.holds_capacity());
        let (_, again) = r.inspect(&i, host).unwrap();
        assert_eq!(again, receipt);
    }

    #[test]
    fn scope_must_name_this_member_host_epoch_and_template() {
        let host = HostId::generate();
        let mut r = ready(host);
        let base = intent(&r);
        let mut cases = Vec::new();
        for f in [
            |i: &mut HandoffIntent| i.member_id = Uuid::now_v7(),
            |i: &mut HandoffIntent| i.vm_ref = "warm-other".into(),
            |i: &mut HandoffIntent| i.template_id = Uuid::now_v7(),
            |i: &mut HandoffIntent| i.supervisor_epoch += 1,
            |i: &mut HandoffIntent| i.host_id = HostId::generate(),
            |i: &mut HandoffIntent| i.generation = 2,
        ] {
            let mut i = base.clone();
            f(&mut i);
            cases.push(i);
        }
        for i in cases {
            assert_eq!(r.begin_bind(&i, host), Err(Refusal::WrongScope));
            assert_eq!(r.inspect(&i, host), Err(Refusal::WrongScope));
        }
        assert_eq!(
            r.begin_bind(&base, HostId::generate()),
            Err(Refusal::WrongScope)
        );
        assert!(matches!(r.state, MemberState::Ready { .. }));
    }

    #[test]
    fn unauthenticated_or_retiring_members_never_bind() {
        let host = HostId::generate();
        let mut r = MemberRecord::new(prepare(host), AllocationId::generate());
        let i = intent(&r);
        let (changed, step) = r.begin_bind(&i, host).unwrap();
        assert!(changed);
        assert_eq!(
            outcome(step),
            HandoffOutcome::Rejected {
                reason: "member_not_ready".into()
            }
        );
        // A boot that authenticates after the fence stays unbindable.
        assert_eq!(r.mark_ready("boot-1"), Err(Refusal::State));

        let mut r = ready(host);
        let i = intent(&r);
        assert!(r.retire_locally("idle_ttl_expired"));
        let (changed, step) = r.begin_bind(&i, host).unwrap();
        assert!(!changed);
        assert_eq!(
            outcome(step),
            HandoffOutcome::Rejected {
                reason: "member_retired".into()
            }
        );
    }

    #[test]
    fn retire_is_exact_and_idempotent_and_holds_capacity_until_proof() {
        let host = HostId::generate();
        let mut r = ready(host);
        let mut retire = RetireIntent {
            version: VERSION,
            member_id: r.member(),
            vm_ref: Some("warm-other".into()),
            host_id: host,
            supervisor_epoch: r.epoch(),
            operation_id: Uuid::now_v7(),
            operation_revision: 1,
        };
        assert_eq!(r.begin_retire(&retire, host), Err(Refusal::WrongScope));
        retire.vm_ref = Some(r.vm_ref.clone());
        retire.supervisor_epoch += 1;
        assert_eq!(r.begin_retire(&retire, host), Err(Refusal::WrongScope));
        retire.supervisor_epoch -= 1;
        assert!(r.begin_retire(&retire, host).unwrap());
        assert!(!r.begin_retire(&retire, host).unwrap());
        retire.vm_ref = None;
        assert!(!r.begin_retire(&retire, host).unwrap());
        assert!(r.holds_capacity() && r.cleanup_receipt().is_none());
        assert!(r.mark_retired().unwrap());
        assert!(!r.mark_retired().unwrap());
        assert!(r.cleanup_receipt().is_some());
        assert!(!r.retire_locally("again"));
    }

    #[test]
    fn persisted_record_round_trips_and_rejects_unknown_shapes() {
        let host = HostId::generate();
        let mut r = ready(host);
        let i = intent(&r);
        r.begin_bind(&i, host).unwrap();
        let bytes = serde_json::to_vec(&r).unwrap();
        let back: MemberRecord = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(back, r);
        back.validate(host).unwrap();
        assert_eq!(back.validate(HostId::generate()), Err(Refusal::State));
        let mut value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        value["unexpected"] = serde_json::json!(true);
        assert!(serde_json::from_value::<MemberRecord>(value).is_err());
        let mut future = r.clone();
        future.version = RECORD_VERSION + 1;
        assert_eq!(future.validate(host), Err(Refusal::State));
        // A tampered intent no longer matches its fenced digest.
        let mut tampered = r.clone();
        if let MemberState::Binding { intent, .. } = &mut tampered.state {
            intent.permit_serial += 1;
        }
        assert_eq!(tampered.validate(host), Err(Refusal::State));
    }
    #[test]
    fn previous_cleanup_cannot_destroy_adopted_member_even_with_exact_claim() {
        let host = HostId::generate();
        let mut r = ready(host);
        let i = intent(&r);
        r.begin_bind(&i, host).unwrap();
        r.finish_bind(&"a".repeat(64)).unwrap();
        let previous = crate::warm_cleanup::PreviousRetireIntent {
            version: VERSION,
            reporting_epoch: r.epoch() + 1,
            original: RetireIntent {
                version: VERSION,
                member_id: r.member(),
                vm_ref: Some(r.vm_ref.clone()),
                host_id: host,
                supervisor_epoch: r.epoch(),
                operation_id: Uuid::now_v7(),
                operation_revision: 1,
            },
        };
        assert_eq!(
            previous.admit(&r, None, host, r.epoch() + 1),
            Err(Refusal::Adopted)
        );
        let recovered: MemberRecord =
            serde_json::from_slice(&serde_json::to_vec(&r).unwrap()).unwrap();
        assert_eq!(
            previous.admit(&recovered, None, host, r.epoch() + 1),
            Err(Refusal::Adopted)
        );
    }
}
