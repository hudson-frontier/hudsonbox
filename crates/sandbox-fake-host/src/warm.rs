//! Simulated warm-pool members (#188). Nothing boots: a member is an in-memory
//! record that holds no project identity. Adoption binds it to exactly one
//! allocation scope and creates the allocation record that ordinary Inspect
//! then reports. Records are per epoch, so a restart (new epoch) refuses every
//! older-epoch request and can supply no cleanup evidence for it.
use super::{FakeHost, MAX_RECORDS, Record, State, bounded_deadline, unix_ms};
use sandbox_protocol::{
    Id, OperationId,
    allocation_authority::Permit,
    supervisor::{
        AllocationState, CreateRequest, Resources, WarmAdoptRequest, WarmObservation,
        WarmPrepareRequest, WarmRetireRequest,
    },
    warm_pool::{
        self, CleanupReceipt, HandoffIntent, HandoffOutcome, HandoffReceipt, PrepareIntent,
        PrepareReceipt, RetireIntent, VERSION, WarmReceipt,
    },
};
use sha2::{Digest, Sha256};
use tokio::time::Instant;
use tonic::Status;

const MAX_MEMBERS: usize = 1024;

#[derive(Debug, Clone)]
pub(crate) enum Member {
    /// Booted (simulated), unused, no identity. Holds capacity.
    Ready(PrepareReceipt),
    /// Bound to one scope; capacity now belongs to the allocation record.
    Adopted {
        digest: String,
        receipt: HandoffReceipt,
    },
    /// Never adoptable again; the unused VM still holds capacity until retired.
    Refused {
        prepared: Option<PrepareReceipt>,
        digest: String,
        receipt: HandoffReceipt,
    },
    /// Destroyed in this epoch; never prepared again.
    Destroyed(CleanupReceipt),
}

impl Member {
    /// Capacity held outside allocation records.
    pub(crate) fn held(&self) -> Option<(u64, u64, u64)> {
        let prepared = match self {
            Self::Ready(p) => p,
            Self::Refused {
                prepared: Some(p), ..
            } => p,
            _ => return None,
        };
        Some((
            u64::from(prepared.vcpu),
            prepared.memory_mib,
            prepared.disk_mib,
        ))
    }
}

fn reply(receipt: &WarmReceipt) -> Result<WarmObservation, Status> {
    Ok(WarmObservation {
        receipt_json: warm_pool::encode(receipt)
            .map_err(|_| Status::internal("warm receipt exceeds bound"))?,
        simulated: true,
        observed_unix_ms: unix_ms()?,
    })
}

fn handoff(intent: &HandoffIntent, digest: &str, outcome: HandoffOutcome) -> HandoffReceipt {
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
        intent_sha256: digest.to_owned(),
        outcome,
    }
}

fn rejected(reason: &str) -> HandoffOutcome {
    HandoffOutcome::Rejected {
        reason: reason.into(),
    }
}

impl FakeHost {
    fn this_epoch(&self, host: sandbox_protocol::HostId, epoch: i64) -> Result<(), Status> {
        if host != self.config.host || epoch != self.config.epoch {
            return Err(Status::failed_precondition(
                "wrong host or supervisor epoch",
            ));
        }
        Ok(())
    }

    /// Simulated warm members currently holding capacity outside allocations.
    pub(crate) fn warm_held(state: &State) -> (u64, u64, u64) {
        state
            .warm
            .values()
            .filter_map(Member::held)
            .fold((0, 0, 0), |a, b| (a.0 + b.0, a.1 + b.1, a.2 + b.2))
    }

    /// Fault injection: apply one adoption, then lose its acknowledgement.
    pub async fn lose_next_adopt_reply(&self) {
        self.state.lock().await.lose_next_adopt_reply = true;
    }

    /// Fault injection: apply the next `n` prepares (or exact retries) and
    /// lose each acknowledgement.
    pub async fn lose_prepare_replies(&self, n: u32) {
        self.state.lock().await.lose_prepare_replies = n;
    }

    /// Simulated members holding capacity outside allocations (test observation).
    pub async fn warm_holding(&self) -> usize {
        let state = self.state.lock().await;
        state.warm.values().filter(|m| m.held().is_some()).count()
    }

    /// Successful simulated adoptions (each is one allocation record).
    pub async fn warm_adoptions(&self) -> u64 {
        self.state.lock().await.warm_adoptions
    }

    /// Simulated members that are not destroyed, adopted or refused.
    pub async fn warm_ready(&self) -> usize {
        let state = self.state.lock().await;
        state
            .warm
            .values()
            .filter(|m| matches!(m, Member::Ready(_)))
            .count()
    }

    pub(crate) async fn warm_prepare(
        &self,
        request: WarmPrepareRequest,
    ) -> Result<WarmObservation, Status> {
        let intent: PrepareIntent = warm_pool::decode(&request.intent_json)
            .map_err(|_| Status::invalid_argument("invalid prepare intent"))?;
        self.this_epoch(intent.host_id, intent.supervisor_epoch)?;
        if intent.version != VERSION
            || !self.config.images.contains(&intent.image_digest)
            || !sandbox_protocol::resources::supported(
                intent.vcpu.into(),
                intent.memory_mib.into(),
                intent.disk_mib.into(),
            )
        {
            return Err(Status::invalid_argument("unsupported warm template"));
        }
        let receipt = PrepareReceipt {
            version: VERSION,
            member_id: intent.member_id,
            template_id: intent.template_id,
            host_id: intent.host_id,
            supervisor_epoch: intent.supervisor_epoch,
            image_digest: intent.image_digest.clone(),
            vcpu: intent.vcpu,
            memory_mib: intent.memory_mib,
            disk_mib: intent.disk_mib,
            vm_ref: format!("warm-{}", intent.member_id),
        };
        let mut state = self.state.lock().await;
        Self::expire(&mut state);
        match state.warm.get(&intent.member_id) {
            // Idempotent on member identity; any changed input is refused.
            Some(Member::Ready(existing)) if *existing == receipt => {
                if let Some(n) = state.lose_prepare_replies.checked_sub(1) {
                    state.lose_prepare_replies = n;
                    return Err(Status::unavailable("injected lost prepare acknowledgement"));
                }
                return reply(&WarmReceipt::Prepared { receipt });
            }
            Some(_) => {
                return Err(Status::already_exists(
                    "warm member exists with other state or input",
                ));
            }
            None => {}
        }
        if state.warm.len() >= MAX_MEMBERS {
            return Err(Status::resource_exhausted("fake warm store full"));
        }
        let (mut cpu, mut memory, mut disk) = Self::warm_held(&state);
        for r in state
            .allocations
            .values()
            .filter(|r| r.state == AllocationState::Ready)
        {
            if let Some(r) = &r.create.resources {
                cpu += u64::from(r.vcpu);
                memory += r.memory_mib;
                disk += r.disk_mib;
            }
        }
        let capacity = &self.config.capacity;
        if u64::from(intent.vcpu) > u64::from(capacity.vcpu).saturating_sub(cpu)
            || intent.memory_mib > capacity.memory_mib.saturating_sub(memory)
            || intent.disk_mib > capacity.disk_mib.saturating_sub(disk)
        {
            return Err(Status::resource_exhausted("fake host capacity full"));
        }
        state
            .warm
            .insert(intent.member_id, Member::Ready(receipt.clone()));
        state.warm_prepares += 1;
        if let Some(n) = state.lose_prepare_replies.checked_sub(1) {
            state.lose_prepare_replies = n;
            return Err(Status::unavailable("injected lost prepare acknowledgement"));
        }
        reply(&WarmReceipt::Prepared { receipt })
    }

    /// Adopt (`inspect == false`) or inspect-and-fence (`inspect == true`).
    pub(crate) async fn warm_handoff(
        &self,
        request: WarmAdoptRequest,
        inspect: bool,
    ) -> Result<WarmObservation, Status> {
        let intent: HandoffIntent = warm_pool::decode(&request.intent_json)
            .map_err(|_| Status::invalid_argument("invalid handoff intent"))?;
        intent
            .validate()
            .map_err(|_| Status::invalid_argument("invalid handoff intent"))?;
        let permit: Permit = warm_pool::decode(&request.launch_permit_json)
            .map_err(|_| Status::invalid_argument("invalid adoption permit"))?;
        let now = unix_ms()?;
        let owner = self.ownership(request.ownership, now)?;
        // Wrong epoch is refused before any state is read or changed.
        self.this_epoch(intent.host_id, intent.supervisor_epoch)?;
        if !intent.admits(&permit, &owner) {
            return Err(Status::permission_denied(
                "adoption permit or ownership does not match the handoff intent",
            ));
        }
        let digest = intent
            .digest()
            .map_err(|_| Status::invalid_argument("invalid handoff intent"))?;
        let mut state = self.state.lock().await;
        // Agreement with the intent is not proof of issuance: on a host that
        // requires permits, only the registered permit may bind or fence.
        state
            .permits
            .admit(&request.launch_permit_json, &owner, true)?;
        Self::expire(&mut state);
        Self::fence(&mut state, &owner)?;
        let current = state.warm.get(&intent.member_id).cloned();
        let receipt = match current {
            Some(Member::Adopted { digest: d, receipt })
            | Some(Member::Refused {
                digest: d, receipt, ..
            }) if d == digest => receipt,
            Some(Member::Adopted { .. } | Member::Refused { .. }) => {
                return Err(Status::already_exists(
                    "warm member is bound to another handoff scope",
                ));
            }
            Some(Member::Destroyed(_)) => handoff(&intent, &digest, rejected("member_retired")),
            None => {
                // Complete in-epoch memory: an unknown member was never
                // prepared here. Fence it so it cannot be prepared later.
                if state.warm.len() >= MAX_MEMBERS {
                    return Err(Status::resource_exhausted("fake warm store full"));
                }
                let receipt = handoff(&intent, &digest, rejected("member_absent"));
                state.warm.insert(
                    intent.member_id,
                    Member::Refused {
                        prepared: None,
                        digest: digest.clone(),
                        receipt: receipt.clone(),
                    },
                );
                receipt
            }
            Some(Member::Ready(prepared)) => {
                let reason = if prepared.vm_ref != intent.vm_ref
                    || prepared.template_id != intent.template_id
                {
                    Some("member_mismatch")
                } else if inspect {
                    Some("fenced_by_inspect")
                } else if state.allocations.contains_key(&owner.allocation_id)
                    || state.sandboxes.contains_key(&owner.sandbox_id)
                {
                    Some("allocation_exists")
                } else {
                    bounded_deadline(request.allocation_expires_unix_ms, now)
                        .err()
                        .map(|_| "allocation_expired")
                };
                if let Some(reason) = reason {
                    let receipt = handoff(&intent, &digest, rejected(reason));
                    state.warm.insert(
                        intent.member_id,
                        Member::Refused {
                            prepared: Some(prepared),
                            digest: digest.clone(),
                            receipt: receipt.clone(),
                        },
                    );
                    receipt
                } else {
                    if state.allocations.len() >= MAX_RECORDS {
                        return Err(Status::resource_exhausted("fake receipt store full"));
                    }
                    let duration = bounded_deadline(request.allocation_expires_unix_ms, now)?;
                    // Per-allocation channel identity issued only now, after the
                    // claim; the unused member never held one. Simulated digest.
                    let identity =
                        hex::encode(Sha256::digest(OperationId::generate().uuid().as_bytes()));
                    let receipt = handoff(
                        &intent,
                        &digest,
                        HandoffOutcome::Adopted {
                            identity_sha256: identity,
                        },
                    );
                    state.sandboxes.insert(
                        owner.sandbox_id.clone(),
                        (
                            owner.project_id.clone(),
                            owner.allocation_id.clone(),
                            owner.generation,
                        ),
                    );
                    state.allocations.insert(
                        owner.allocation_id.clone(),
                        Record {
                            lease_until: request.allocation_expires_unix_ms,
                            create: CreateRequest {
                                launch_permit_json: request.launch_permit_json,
                                ownership: Some(owner.clone()),
                                image_digest: prepared.image_digest.clone(),
                                resources: Some(Resources {
                                    vcpu: prepared.vcpu,
                                    memory_mib: prepared.memory_mib,
                                    disk_mib: prepared.disk_mib,
                                }),
                                allocation_expires_unix_ms: request.allocation_expires_unix_ms,
                            },
                            state: AllocationState::Ready,
                            expires: Instant::now() + duration,
                            reason: "simulated warm adoption; no VM or process started",
                        },
                    );
                    state.warm.insert(
                        intent.member_id,
                        Member::Adopted {
                            digest: digest.clone(),
                            receipt: receipt.clone(),
                        },
                    );
                    state.warm_adoptions += 1;
                    if std::mem::take(&mut state.lose_next_adopt_reply) {
                        return Err(Status::unavailable("injected lost adopt acknowledgement"));
                    }
                    receipt
                }
            }
        };
        reply(&WarmReceipt::Handoff { receipt })
    }

    pub(crate) async fn warm_retire(
        &self,
        request: WarmRetireRequest,
    ) -> Result<WarmObservation, Status> {
        let intent: RetireIntent = warm_pool::decode(&request.intent_json)
            .map_err(|_| Status::invalid_argument("invalid retire intent"))?;
        if intent.version != VERSION
            || intent
                .vm_ref
                .as_deref()
                .is_some_and(|v| !warm_pool::valid_vm_ref(v))
        {
            return Err(Status::invalid_argument("invalid retire intent"));
        }
        self.this_epoch(intent.host_id, intent.supervisor_epoch)?;
        let mut state = self.state.lock().await;
        let prepared_ref = match state.warm.get(&intent.member_id) {
            Some(Member::Adopted { .. }) => {
                return Err(Status::failed_precondition(
                    "adopted member belongs to its allocation",
                ));
            }
            Some(Member::Destroyed(existing)) => {
                // Exact retry, including an unnamed retry of a named destroy.
                return if existing.vm_ref == intent.vm_ref || intent.vm_ref.is_none() {
                    reply(&WarmReceipt::Cleanup {
                        receipt: existing.clone(),
                    })
                } else {
                    Err(Status::already_exists(
                        "member destroyed under another name",
                    ))
                };
            }
            Some(Member::Ready(p)) => Some(p.vm_ref.clone()),
            Some(Member::Refused { prepared, .. }) => prepared.as_ref().map(|p| p.vm_ref.clone()),
            None => None,
        };
        // A named retire must name what this epoch prepared. An unnamed retire
        // (prepare acknowledgement lost) destroys the exact member's prepared,
        // never-adopted VM and reports its name; complete in-epoch memory makes
        // an unknown member a truthful "never prepared" answer.
        if intent.vm_ref.is_some() && prepared_ref != intent.vm_ref {
            return Err(Status::failed_precondition("warm member name mismatch"));
        }
        if !state.warm.contains_key(&intent.member_id) && state.warm.len() >= MAX_MEMBERS {
            return Err(Status::resource_exhausted("fake warm store full"));
        }
        let receipt = CleanupReceipt {
            version: VERSION,
            member_id: intent.member_id,
            vm_ref: prepared_ref,
            host_id: intent.host_id,
            supervisor_epoch: intent.supervisor_epoch,
            destroyed: true,
        };
        state
            .warm
            .insert(intent.member_id, Member::Destroyed(receipt.clone()));
        reply(&WarmReceipt::Cleanup { receipt })
    }
}
