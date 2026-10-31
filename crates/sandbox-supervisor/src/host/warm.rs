//! Warm-pool RPCs on the real Firecracker host (#188).
//!
//! A member is one unused VM launched by its own guardian from an inert
//! version-2 bootstrap (no project, sandbox, operation, token or customer
//! secret). Its directory, jail, cgroup and vsock channel are keyed by a
//! host-chosen slot for the VM's whole life. [`MemberRecord`] is persisted in
//! the host journal before every effect:
//!
//! - Prepare reserves capacity durably, stages and launches the guardian, and
//!   answers a receipt only after the member endpoint authenticated the boot.
//! - Adopt verifies operation ownership and the exact registered permit
//!   ([`HandoffIntent::admits`], plus the launch-authority ledger when permits
//!   are required), fences the intent durably, then asks the member's guardian
//!   to re-key: it persists the allocation identity and binding before calling
//!   the guest, and confirms only after the guest acknowledged the identity and
//!   the allocation channel authenticated the same boot. The same guardian
//!   keeps its lifecycle lock, watchdog and cgroup throughout. Only then does
//!   the allocation record exist with a manifest, so no customer RPC reaches
//!   the VM before binding.
//! - Inspect never adopts: a recorded outcome or a durable fence.
//! - Retire destroys an unadopted member and answers only with guardian-proven
//!   cleanup. Capacity stays held until then, including for members retained
//!   from a previous epoch.
use super::*;
use crate::warm_member::{MemberRecord, MemberState, Refusal, Step};
use sandbox_protocol::{
    supervisor::{WarmAdoptRequest, WarmObservation, WarmPrepareRequest, WarmRetireRequest},
    warm_pool::{
        self, CleanupReceipt, HandoffIntent, HandoffOutcome, HandoffReceipt, PrepareIntent,
        RetireIntent, VERSION, WarmReceipt,
    },
};

/// Retained member records (all states). Bounds journal growth.
pub(super) const MAX_MEMBERS: usize = 256;
/// Inert member channel life; the store's idle TTL retires members earlier.
const MEMBER_VALIDITY_MS: i64 = 23 * 3_600_000;
/// Guardian lease step for an inert member; renewed by maintenance.
const MEMBER_LEASE_MS: i64 = 120_000;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Entry {
    pub record: MemberRecord,
    /// Inert guardian manifest keyed by `record.slot`.
    pub manifest: Manifest,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub previous_retire: Option<crate::warm_cleanup::PreviousRetireIntent>,
    #[serde(skip)]
    pub gate: Arc<Mutex<()>>,
}

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

fn refusal(error: Refusal) -> Status {
    match error {
        Refusal::OtherScope => Status::already_exists(error.to_string()),
        Refusal::WrongScope | Refusal::Changed => Status::invalid_argument(error.to_string()),
        Refusal::Adopted | Refusal::State => Status::failed_precondition(error.to_string()),
    }
}

fn handoff_reply(receipt: HandoffReceipt) -> Result<WarmObservation, Status> {
    reply(&WarmReceipt::Handoff { receipt })
}

fn rejected(intent: &HandoffIntent, reason: &str) -> Result<WarmObservation, Status> {
    handoff_reply(HandoffReceipt {
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
            reason: reason.into(),
        },
    })
}

/// Validate a member record and its inert manifest loaded from the journal.
pub(super) fn validate_retained(config: &Config, key: &str, entry: &Entry) -> anyhow::Result<()> {
    let r = &entry.record;
    let m = &entry.manifest;
    r.validate(config.host)
        .map_err(|_| anyhow::anyhow!("invalid retained warm member"))?;
    let member = m
        .member
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("warm manifest lacks member key"))?;
    anyhow::ensure!(
        key == r.member().to_string()
            && m.is_inert()
            && member.member == r.member()
            && member.slot == r.slot
            && m.start.owner.host == config.host
            && m.start.owner.epoch == r.epoch()
            && r.epoch() <= config.epoch
            && m.config.state_root == config.state_root.join("a")
            && m.start.vcpu == r.prepare.vcpu
            && m.start.memory_mib == r.prepare.memory_mib
            && m.start.disk_mib == r.prepare.disk_mib,
        "retained warm member identity mismatch"
    );
    if let Some(previous) = &entry.previous_retire {
        previous
            .admit(r, None, config.host, previous.reporting_epoch)
            .map_err(|_| anyhow::anyhow!("invalid retained previous cleanup fence"))?;
        anyhow::ensure!(
            previous.reporting_epoch <= config.epoch
                && matches!(r.state, MemberState::Retiring { .. } | MemberState::Retired),
            "invalid previous cleanup state"
        );
    }
    m.validate()
}

impl Host {
    fn warm_identity(&self) -> (HostId, i64) {
        (self.inner.config.host, self.inner.config.epoch)
    }

    fn this_epoch(&self, host: HostId, epoch: i64) -> Result<(), Status> {
        if (host, epoch) != self.warm_identity() {
            return Err(Status::failed_precondition(
                "wrong host or supervisor epoch",
            ));
        }
        Ok(())
    }

    fn member_entry(&self, member: &uuid::Uuid) -> Result<Option<Entry>, Status> {
        Ok(self.journal()?.warm.get(&member.to_string()).cloned())
    }

    /// Persist `record` for `member`, re-reading under the journal lock.
    fn save_member(&self, record: &MemberRecord) -> Result<(), Status> {
        let mut j = self.journal()?;
        let entry = j
            .warm
            .get_mut(&record.member().to_string())
            .ok_or_else(|| uncertain("warm member record missing"))?;
        if entry.record.revision >= record.revision && entry.record != *record {
            return Err(uncertain("concurrent warm member change"));
        }
        entry.record = record.clone();
        self.save(&mut j)
    }

    /// Resources held by unadopted members, charged like allocations.
    pub(super) fn warm_held(j: &Journal) -> (u64, u64, u64) {
        j.warm
            .values()
            .filter(|e| e.record.holds_capacity())
            .fold((0, 0, 0), |(c, m, d), e| {
                (
                    c.saturating_add(u64::from(e.record.prepare.vcpu)),
                    m.saturating_add(e.record.prepare.memory_mib + MEMORY_OVERHEAD),
                    d.saturating_add(e.record.prepare.disk_mib + DISK_OVERHEAD),
                )
            })
    }

    fn member_manifest(
        &self,
        intent: &PrepareIntent,
        slot: AllocationId,
    ) -> Result<Manifest, Status> {
        let c = &self.inner.config;
        let image = c
            .images
            .get(&intent.image_digest)
            .ok_or_else(|| Status::permission_denied("image not allowed"))?;
        if !sandbox_protocol::resources::supported(
            intent.vcpu.into(),
            intent.memory_mib.into(),
            intent.disk_mib.into(),
        ) {
            return Err(Status::invalid_argument(
                "resources outside real VM envelope",
            ));
        }
        if let Some(capabilities) = &c.capabilities
            && !capabilities.supports(
                &intent.image_digest,
                u64::from(intent.vcpu),
                intent.memory_mib,
                intent.disk_mib,
            )
        {
            return Err(Status::failed_precondition(
                "host image/resource compatibility mismatch",
            ));
        }
        let now = now_ms()?;
        Ok(Manifest {
            launch_permit: None,
            config: c.guardian_config(image),
            start: guardian::Start {
                owner: guardian::Owner::inert(c.host, intent.member_id, slot, c.epoch),
                vcpu: intent.vcpu,
                memory_mib: intent.memory_mib,
                disk_mib: intent.disk_mib,
                expires_unix_ms: now + MEMBER_LEASE_MS,
            },
            member: Some(guardian::Member {
                member: intent.member_id,
                slot,
                valid_until_unix_ms: now + MEMBER_VALIDITY_MS,
                bound_from: None,
            }),
        })
    }

    /// Reserve a new member durably, or return the existing one unchanged.
    fn reserve_member(&self, intent: &PrepareIntent) -> Result<(Entry, bool), Status> {
        let mut j = self.journal()?;
        if j.warm_refusals.contains(&intent.member_id) {
            return Err(Status::failed_precondition(
                "warm member durably retired before prepare",
            ));
        }
        if let Some(entry) = j.warm.get(&intent.member_id.to_string()) {
            entry.record.check_prepare_retry(intent).map_err(refusal)?;
            return Ok((entry.clone(), false));
        }
        if j.warm.len().saturating_add(j.warm_refusals.len()) >= MAX_MEMBERS {
            return Err(Status::resource_exhausted(
                "warm member lifetime budget exhausted (256 retained identities); drain and reconcile this host before replacing its identity; never delete journal authority",
            ));
        }
        let slot = AllocationId::generate();
        let manifest = self.member_manifest(intent, slot)?;
        let (mut cpu, mut memory, mut disk) = Self::warm_held(&j);
        for r in j.records.values() {
            if let Some(m) = &r.manifest
                && !r.released
            {
                cpu = cpu.saturating_add(u64::from(m.start.vcpu));
                memory = memory.saturating_add(m.start.memory_mib + MEMORY_OVERHEAD);
                disk = disk.saturating_add(m.start.disk_mib + DISK_OVERHEAD);
            }
        }
        let cap = &self.inner.config.capacity;
        if u64::from(intent.vcpu) > cap.vcpu.saturating_sub(cpu)
            || intent.memory_mib + MEMORY_OVERHEAD > cap.memory_mib.saturating_sub(memory)
            || intent.disk_mib + DISK_OVERHEAD > cap.disk_mib.saturating_sub(disk)
        {
            return Err(Status::resource_exhausted("host capacity reserved"));
        }
        let entry = Entry {
            record: MemberRecord::new(intent.clone(), slot),
            manifest,
            previous_retire: None,
            gate: Arc::default(),
        };
        j.warm.insert(intent.member_id.to_string(), entry.clone());
        self.save(&mut j)?;
        Ok((entry, true))
    }

    /// Spawn the single-threaded guardian executable for a staged manifest.
    pub(super) fn spawn_guardian(&self, manifest: &Manifest, label: String) -> bool {
        match Command::new(&self.inner.config.guardian_binary)
            .arg("--manifest")
            .arg(manifest.directory().join("manifest.json"))
            .arg("run")
            .env_clear()
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
        {
            Ok(mut child) => {
                std::thread::spawn(move || match child.wait() {
                    Ok(status) if !status.success() => {
                        eprintln!("sandbox guardian exited unexpectedly for {label}: {status:?}");
                    }
                    Ok(_) => {}
                    Err(error) => {
                        eprintln!("could not wait for sandbox guardian for {label}: {error}");
                    }
                });
                true
            }
            Err(error) => {
                eprintln!("could not launch sandbox guardian for {label}: {error}");
                false
            }
        }
    }

    /// Authenticate the inert member endpoint and record its boot.
    fn member_ready(&self, entry: &mut Entry, wait: Duration) -> Result<bool, Status> {
        let until = Instant::now() + wait;
        loop {
            if let Ok(reply) = guardian::control(&entry.manifest, Action::MemberHello)
                && reply.error.is_none()
                && let Some(r) = reply.receipt
                && r.state == GuardianState::Running
                && r.binding.is_none()
                && let Some(boot) = r.guest_boot_id
            {
                if entry.record.mark_ready(&boot).map_err(refusal)? {
                    self.save_member(&entry.record)?;
                }
                return Ok(true);
            }
            if Instant::now() >= until {
                return Ok(false);
            }
            std::thread::sleep(Duration::from_millis(50));
        }
    }

    pub(super) fn warm_prepare_sync(
        &self,
        request: WarmPrepareRequest,
    ) -> Result<WarmObservation, Status> {
        let intent: PrepareIntent = warm_pool::decode(&request.intent_json)
            .map_err(|_| Status::invalid_argument("invalid prepare intent"))?;
        if intent.version != VERSION || intent.member_id.is_nil() {
            return Err(Status::invalid_argument("invalid prepare intent"));
        }
        self.this_epoch(intent.host_id, intent.supervisor_epoch)?;
        let (mut entry, _) = self.reserve_member(&intent)?;
        let gate = entry.gate.clone();
        let _gate = lock(&gate)?;
        // Re-read under the member gate.
        entry = self
            .member_entry(&intent.member_id)?
            .ok_or_else(|| uncertain("warm member record missing"))?;
        // Retirement may have won the member gate after this call reserved
        // a fresh record. The current durable state alone permits staging.
        if matches!(entry.record.state, MemberState::Preparing) {
            match entry.manifest.receipt() {
                // Never staged: stage and launch exactly once under the gate.
                Err(_) if !entry.manifest.directory().join("receipt.json").exists() => {
                    if entry.manifest.prepare().is_err()
                        || !self.spawn_guardian(
                            &entry.manifest,
                            format!("warm member {}", intent.member_id),
                        )
                    {
                        if entry.record.retire_locally("prepare_failed") {
                            self.save_member(&entry.record)?;
                        }
                        let _ = self.cleanup_member(&mut entry);
                        return Err(Status::unavailable(
                            "warm member launch failed; reservation held until cleanup",
                        ));
                    }
                }
                Ok(r) if r.state == GuardianState::Stopped => {
                    if entry.record.retire_locally("prepare_failed") {
                        self.save_member(&entry.record)?;
                    }
                    return Err(Status::unavailable("warm member stopped before ready"));
                }
                _ => {}
            }
            if !self.member_ready(&mut entry, Duration::from_secs(4))? {
                return Err(uncertain("warm member readiness pending"));
            }
        }
        match entry.record.prepare_receipt().map_err(refusal)? {
            Some(receipt) => reply(&WarmReceipt::Prepared { receipt }),
            None => Err(uncertain("warm member readiness pending")),
        }
    }

    /// Validate an adopt/inspect request: operation ownership (claim revision
    /// fence), exact permit and intent agreement, and permit registration.
    fn handoff_scope(
        &self,
        request: &WarmAdoptRequest,
    ) -> Result<(Ownership, HandoffIntent, Permit), Status> {
        let intent: HandoffIntent = warm_pool::decode(&request.intent_json)
            .map_err(|_| Status::invalid_argument("invalid handoff intent"))?;
        intent
            .validate()
            .map_err(|_| Status::invalid_argument("invalid handoff intent"))?;
        self.this_epoch(intent.host_id, intent.supervisor_epoch)?;
        let o = self.owner(request.ownership.clone())?;
        let permit: Permit = serde_json::from_slice(&request.launch_permit_json)
            .map_err(|_| Status::invalid_argument("invalid launch permit"))?;
        if !intent.admits(&permit, &o) {
            return Err(Status::permission_denied(
                "permit or ownership does not match handoff intent",
            ));
        }
        Ok((o, intent, permit))
    }

    pub(super) fn warm_handoff_sync(
        &self,
        request: WarmAdoptRequest,
        inspect_only: bool,
    ) -> Result<WarmObservation, Status> {
        let (o, intent, permit) = self.handoff_scope(&request)?;
        // gate() admits a missing allocation record only for the exact
        // registered active permit when permits are required; fence() rejects
        // a superseded operation claim before it reaches the member.
        let allocation_gate = self.gate(&o)?;
        let _allocation_gate = lock(&allocation_gate)?;
        let record = self.fence(&o)?;
        if self.inner.authority.is_some() {
            let checkpoint = self
                .journal()?
                .launch_authority
                .clone()
                .ok_or_else(|| uncertain("missing authority checkpoint"))?;
            let (registered, _guard) = crate::launch_authority::authorize_registered_allocation(
                &self.inner.config.state_root.join("a"),
                self.inner.config.host,
                self.inner.config.epoch,
                checkpoint.registered_through,
                intent.allocation_id,
            )
            .map_err(|_| Status::failed_precondition("registered active allocation required"))?;
            if registered != permit {
                return Err(Status::permission_denied(
                    "permit is not the registered permit",
                ));
            }
        }
        let Some(entry) = self.member_entry(&intent.member_id)? else {
            // The journal records every member before its guardian launches,
            // so no VM exists for this member at this epoch.
            return rejected(&intent, "member_unknown");
        };
        let gate = entry.gate.clone();
        let _gate = lock(&gate)?;
        let mut entry = self
            .member_entry(&intent.member_id)?
            .ok_or_else(|| uncertain("warm member record missing"))?;
        let host = self.inner.config.host;
        if inspect_only {
            let (changed, receipt) = entry.record.inspect(&intent, host).map_err(refusal)?;
            if changed {
                self.save_member(&entry.record)?;
            }
            return handoff_reply(receipt);
        }
        if matches!(entry.record.state, MemberState::Ready { .. })
            && (record.create.is_some() || record.stopped)
        {
            // A cold create or stop already owns this allocation.
            return Err(Status::failed_precondition(
                "allocation already dispatched or stop-fenced",
            ));
        }
        let (changed, step) = entry.record.begin_bind(&intent, host).map_err(refusal)?;
        if changed {
            self.save_member(&entry.record)?;
        }
        match step {
            Step::Answer(receipt) => handoff_reply(receipt),
            Step::Bind if record.create.is_some() || record.stopped => {
                Err(uncertain("allocation changed during warm adoption"))
            }
            Step::Bind => self.bind_member(&mut entry, &o, &intent, permit, &request),
        }
    }

    fn bind_member(
        &self,
        entry: &mut Entry,
        o: &Ownership,
        intent: &HandoffIntent,
        permit: Permit,
        request: &WarmAdoptRequest,
    ) -> Result<WarmObservation, Status> {
        deadline(request.allocation_expires_unix_ms)?;
        let owner = guardian::Owner {
            host: intent.host_id,
            project: intent.project_id,
            sandbox: intent.sandbox_id,
            allocation: intent.allocation_id,
            create_operation: intent.create_operation_id,
            generation: intent.generation,
            epoch: intent.supervisor_epoch,
        };
        let adoption = guardian::Adoption {
            owner,
            // Bound to the binding only when permits are enforced, so cleanup
            // and renewal of the adopted VM use the same authority as Create.
            permit: self.inner.authority.as_ref().map(|_| permit),
            intent: intent.clone(),
            expires_unix_ms: request.allocation_expires_unix_ms,
        };
        let response = guardian::control(&entry.manifest, Action::Adopt(Box::new(adoption)))
            .map_err(uncertain)?;
        if response.error.as_deref() == Some(guardian::ADOPTION_REFUSED) {
            let receipt = entry
                .record
                .fail_bind("guest_refused_binding")
                .map_err(refusal)?;
            self.save_member(&entry.record)?;
            return handoff_reply(receipt);
        }
        if response.error.is_some() {
            return Err(uncertain("warm adoption unconfirmed"));
        }
        let binding = response
            .receipt
            .and_then(|r| r.binding)
            .filter(|b| b.confirmed && b.intent_sha256 == entry_digest(intent))
            .ok_or_else(|| uncertain("warm adoption unconfirmed"))?;
        let view = entry.manifest.bound(&binding).map_err(uncertain)?;
        // The allocation record gains its manifest only now: no command, file
        // or lease RPC can reach the VM before the binding is confirmed.
        {
            let mut j = self.journal()?;
            let r = j
                .records
                .get_mut(&o.allocation_id)
                .ok_or_else(|| uncertain("record missing"))?;
            r.create = Some(CreateRequest {
                ownership: Some(o.clone()),
                image_digest: entry.record.prepare.image_digest.clone(),
                resources: Some(sandbox_protocol::supervisor::Resources {
                    vcpu: entry.record.prepare.vcpu,
                    memory_mib: entry.record.prepare.memory_mib,
                    disk_mib: entry.record.prepare.disk_mib,
                }),
                allocation_expires_unix_ms: request.allocation_expires_unix_ms,
                launch_permit_json: request.launch_permit_json.clone(),
            });
            r.manifest = Some(view);
            r.dispatched = true;
            let entry_record = j
                .warm
                .get_mut(&intent.member_id.to_string())
                .ok_or_else(|| uncertain("warm member record missing"))?;
            let receipt = entry_record
                .record
                .finish_bind(&binding.guest_identity_sha256)
                .map_err(refusal)?;
            entry.record = entry_record.record.clone();
            // Capacity moves from the member to the allocation in one write.
            self.save(&mut j)?;
            handoff_reply(receipt)
        }
    }

    /// Stop and clean the member's guardian state. Returns proof of cleanup.
    fn cleanup_member(&self, entry: &mut Entry) -> Result<bool, Status> {
        if !matches!(entry.record.state, MemberState::Retiring { .. }) {
            return Ok(matches!(entry.record.state, MemberState::Retired));
        }
        let m = &entry.manifest;
        // A timeout is uncertain. The lifecycle lock prevents racing a live
        // guardian; fence_unstarted proves absence of unstaged state too.
        let _ = guardian::control(m, Action::Stop);
        let receipt = match m.fence_unstarted() {
            Ok(r) => r,
            Err(error)
                if crate::launch_authority::is_lock_contended(&error)
                    || guardian::is_ownership_busy(&error) =>
            {
                return Ok(false);
            }
            Err(error) => return Err(uncertain(error)),
        };
        if receipt.state == GuardianState::Stopped && receipt.cleanup_confirmed {
            // Check present runtime absence under a fresh lifecycle lock before
            // releasing even the host-local reservation.
            m.prove_member_absent().map_err(uncertain)?;
            entry.record.mark_retired().map_err(refusal)?;
            self.save_member(&entry.record)?;
            return Ok(true);
        }
        Ok(false)
    }

    pub(super) fn warm_retire_sync(
        &self,
        request: WarmRetireRequest,
    ) -> Result<WarmObservation, Status> {
        if let Ok(previous) =
            warm_pool::decode::<crate::warm_cleanup::PreviousRetireIntent>(&request.intent_json)
        {
            return self.warm_previous_retire(previous);
        }
        let intent: RetireIntent = warm_pool::decode(&request.intent_json)
            .map_err(|_| Status::invalid_argument("invalid retire intent"))?;
        if intent.version != VERSION
            || intent.member_id.is_nil()
            || intent.operation_id.is_nil()
            || intent.operation_revision <= 0
            || intent
                .vm_ref
                .as_deref()
                .is_some_and(|v| !warm_pool::valid_vm_ref(v))
        {
            return Err(Status::invalid_argument("invalid retire intent"));
        }
        // Ordinary retirement cannot acknowledge a previous incarnation.
        self.this_epoch(intent.host_id, intent.supervisor_epoch)?;
        let Some(entry) = self.member_entry(&intent.member_id)? else {
            if intent.vm_ref.is_some() {
                return Err(Status::failed_precondition("unknown named warm member"));
            }
            let mut j = self.journal()?;
            if j.warm.contains_key(&intent.member_id.to_string()) {
                return Err(uncertain("concurrent warm prepare; retry retirement"));
            }
            if !j.warm_refusals.contains(&intent.member_id) {
                if j.warm.len().saturating_add(j.warm_refusals.len()) >= MAX_MEMBERS {
                    return Err(Status::resource_exhausted(
                        "warm member lifetime budget exhausted",
                    ));
                }
                j.warm_refusals.insert(intent.member_id);
                self.save(&mut j)?;
            }
            // The durable refusal prevents every late prepare from launching.
            return reply(&WarmReceipt::Cleanup {
                receipt: CleanupReceipt {
                    version: VERSION,
                    member_id: intent.member_id,
                    vm_ref: None,
                    host_id: intent.host_id,
                    supervisor_epoch: intent.supervisor_epoch,
                    destroyed: true,
                },
            });
        };
        let gate = entry.gate.clone();
        let _gate = lock(&gate)?;
        let mut entry = self
            .member_entry(&intent.member_id)?
            .ok_or_else(|| uncertain("warm member record missing"))?;
        if entry
            .record
            .begin_retire(&intent, self.inner.config.host)
            .map_err(refusal)?
        {
            self.save_member(&entry.record)?;
        }
        if !self.cleanup_member(&mut entry)? {
            return Err(Status::unavailable("warm member cleanup pending"));
        }
        let receipt = entry
            .record
            .cleanup_receipt()
            .ok_or_else(|| uncertain("warm member cleanup unconfirmed"))?;
        reply(&WarmReceipt::Cleanup { receipt })
    }

    fn warm_previous_retire(
        &self,
        previous: crate::warm_cleanup::PreviousRetireIntent,
    ) -> Result<WarmObservation, Status> {
        previous.validate().map_err(refusal)?;
        self.this_epoch(previous.original.host_id, previous.reporting_epoch)?;
        let entry = self
            .member_entry(&previous.original.member_id)?
            .ok_or_else(|| Status::failed_precondition("retained warm ownership unavailable"))?;
        let gate = entry.gate.clone();
        let _gate = lock(&gate)?;
        let mut entry = self
            .member_entry(&previous.original.member_id)?
            .ok_or_else(|| uncertain("retained warm ownership unavailable"))?;
        previous
            .admit(
                &entry.record,
                entry.previous_retire.as_ref(),
                self.inner.config.host,
                self.inner.config.epoch,
            )
            .map_err(refusal)?;
        entry
            .record
            .begin_retire(&previous.original, self.inner.config.host)
            .map_err(refusal)?;
        // Save claim and retirement together, before stop/absence proof. This
        // also fences a stale callback after a lost reply and host restart.
        entry.previous_retire = Some(previous.clone());
        {
            let mut j = self.journal()?;
            j.warm
                .insert(entry.record.member().to_string(), entry.clone());
            self.save(&mut j)?;
        }
        if !self.cleanup_member(&mut entry)? {
            return Err(Status::unavailable("warm member cleanup pending"));
        }
        // Recheck guardian evidence even for an already-Retired retry. Missing
        // or partial metadata is never converted to refund authority.
        let proof = entry.manifest.prove_member_absent().map_err(uncertain)?;
        if proof.state != GuardianState::Stopped || !proof.cleanup_confirmed {
            return Err(uncertain("warm member absence unconfirmed"));
        }
        let cleanup = entry
            .record
            .cleanup_receipt()
            .ok_or_else(|| uncertain("warm member cleanup unconfirmed"))?;
        Ok(WarmObservation {
            receipt_json: warm_pool::encode(&crate::warm_cleanup::Reply::PreviousCleanup {
                receipt: crate::warm_cleanup::PreviousCleanupReceipt {
                    version: VERSION,
                    request: previous,
                    cleanup,
                },
            })
            .map_err(|_| Status::internal("warm receipt exceeds bound"))?,
            simulated: false,
            observed_unix_ms: now_ms()?,
        })
    }

    /// One bounded maintenance step: clean a retiring member, retire an
    /// expired one, or renew an idle member's guardian lease.
    pub(super) fn reconcile_member(&self) -> Result<(), Status> {
        let candidates: Vec<Entry> = {
            let j = self.journal()?;
            j.warm
                .values()
                .filter(|e| e.record.needs_cleanup())
                .cloned()
                .collect()
        };
        if candidates.is_empty() {
            return Ok(());
        }
        let index = self.inner.cursor.fetch_add(1, Ordering::Relaxed) % candidates.len();
        let entry = &candidates[index];
        let Ok(_gate) = entry.gate.try_lock() else {
            return Ok(());
        };
        let Some(mut entry) = self.member_entry(&entry.record.member())? else {
            return Ok(());
        };
        let now = now_ms()?;
        let member = entry
            .manifest
            .member
            .clone()
            .ok_or_else(|| uncertain("warm manifest lacks member key"))?;
        let previous = entry.record.epoch() < self.inner.config.epoch;
        if (previous || now + MEMBER_LEASE_MS >= member.valid_until_unix_ms)
            && entry.record.retire_locally(if previous {
                "previous_epoch"
            } else {
                "member_expired"
            })
        {
            self.save_member(&entry.record)?;
        }
        match &entry.record.state {
            MemberState::Retiring { .. } => {
                self.cleanup_member(&mut entry)?;
            }
            MemberState::Preparing => {
                self.member_ready(&mut entry, Duration::ZERO)?;
            }
            MemberState::Ready { .. } => {
                let Ok(reply) = guardian::control(&entry.manifest, Action::Inspect) else {
                    return Ok(());
                };
                let Some(r) = reply.receipt else {
                    return Ok(());
                };
                if r.state != GuardianState::Running {
                    if entry.record.retire_locally("member_stopped") {
                        self.save_member(&entry.record)?;
                    }
                    return Ok(());
                }
                // Renew well before expiry; the guardian refuses renewal once
                // any binding exists, so this never extends an allocation.
                if r.expires_unix_ms - now < MEMBER_LEASE_MS / 2 {
                    let _ = guardian::control(
                        &entry.manifest,
                        Action::Renew {
                            revision: r.renewal_revision + 1,
                            expires_unix_ms: (now + MEMBER_LEASE_MS)
                                .min(member.valid_until_unix_ms - 1),
                        },
                    );
                }
            }
            // Binding or fenced members are never renewed: their inert lease
            // lapses and the store retires them.
            _ => {}
        }
        Ok(())
    }
}

fn entry_digest(intent: &HandoffIntent) -> String {
    intent.digest().unwrap_or_default()
}

use sandbox_protocol::allocation_authority::Permit;

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    fn artifact() -> Artifact {
        Artifact {
            path: "/nonexistent".into(),
            sha256: "0".repeat(64),
        }
    }

    fn inert(host: HostId, member: uuid::Uuid, slot: AllocationId) -> Manifest {
        Manifest {
            launch_permit: None,
            config: guardian::Config {
                state_root: "/var/lib/hudson/a".into(),
                cgroup_parent: "/sys/fs/cgroup/hudson".into(),
                firecracker: artifact(),
                jailer: artifact(),
                kernel: artifact(),
                rootfs: artifact(),
                jail_uid: 65534,
                jail_gid: 65534,
            },
            start: guardian::Start {
                owner: guardian::Owner::inert(host, member, slot, 3),
                vcpu: 1,
                memory_mib: 512,
                disk_mib: 1024,
                expires_unix_ms: 1_000,
            },
            member: Some(guardian::Member {
                member,
                slot,
                valid_until_unix_ms: 2_000,
                bound_from: None,
            }),
        }
    }

    fn prepare(host: HostId) -> PrepareIntent {
        PrepareIntent {
            version: VERSION,
            member_id: uuid::Uuid::now_v7(),
            policy_id: uuid::Uuid::now_v7(),
            template_id: uuid::Uuid::now_v7(),
            host_id: host,
            supervisor_epoch: 3,
            image_digest: format!("sha256:{}", "a".repeat(64)),
            vcpu: 1,
            memory_mib: 512,
            disk_mib: 1024,
            operation_id: uuid::Uuid::now_v7(),
            operation_revision: 1,
        }
    }

    #[test]
    fn journals_and_guardian_files_without_warm_fields_load_unchanged() {
        let host = HostId::generate();
        let journal = Journal {
            launch_authority: None,
            version: 1,
            host,
            epoch: 2,
            records: BTreeMap::new(),
            warm: BTreeMap::new(),
            warm_refusals: Default::default(),
            poisoned: false,
        };
        let value = serde_json::to_value(&journal).unwrap();
        assert!(value.get("warm").is_none());
        let legacy = serde_json::json!({"version": 1, "host": host, "epoch": 2, "records": {}});
        let loaded: Journal = serde_json::from_value(legacy).unwrap();
        assert!(loaded.warm.is_empty());
        // Cold manifests keep their pre-#188 serialization, so retained
        // receipt digests still verify after upgrade.
        let mut cold = inert(host, uuid::Uuid::now_v7(), AllocationId::generate());
        cold.member = None;
        let encoded = serde_json::to_value(&cold).unwrap();
        assert!(encoded.get("member").is_none());
        // Inert placeholders round-trip internally without admitting nil or
        // non-v7 identities through the public identifier parser.
        assert!(
            cold.start
                .owner
                .project
                .to_string()
                .parse::<ProjectId>()
                .is_err()
        );
        let mut invalid = encoded.clone();
        invalid["start"]["owner"]["project"] =
            serde_json::json!(format!("prj_{}", uuid::Uuid::max()));
        assert!(serde_json::from_value::<Manifest>(invalid).is_err());
        let reloaded: Manifest = serde_json::from_value(encoded.clone()).unwrap();
        assert_eq!(serde_json::to_value(&reloaded).unwrap(), encoded);
        let receipt = serde_json::json!({
            "version": 1, "owner": cold.start.owner, "digest": "0".repeat(64),
            "host_boot_id": "b", "state": "Stopped", "cleanup_confirmed": true,
            "renewal_revision": 0, "expires_unix_ms": 1, "guardian_host_pid": null,
            "guardian_start_ticks": null, "firecracker_namespace_pid": null,
            "cgroup_inode": null, "reason": null
        });
        let receipt: Receipt = serde_json::from_value(receipt).unwrap();
        assert!(receipt.binding.is_none());
        assert!(
            serde_json::to_value(&receipt)
                .unwrap()
                .get("binding")
                .is_none()
        );
        let mut unknown = serde_json::to_value(&receipt).unwrap();
        unknown["binding"] = serde_json::json!({"owner": cold.start.owner});
        assert!(serde_json::from_value::<Receipt>(unknown).is_err());
    }

    #[test]
    fn adopted_view_keeps_slot_paths_and_guardian_receipt_identity() {
        let host = HostId::generate();
        let member = uuid::Uuid::now_v7();
        let slot = AllocationId::generate();
        let m = inert(host, member, slot);
        assert!(m.is_inert());
        assert_eq!(m.key(), slot);
        let owner = guardian::Owner {
            host,
            project: ProjectId::generate(),
            sandbox: SandboxId::generate(),
            allocation: AllocationId::generate(),
            create_operation: OperationId::generate(),
            generation: 1,
            epoch: 3,
        };
        let mut binding = guardian::Binding {
            owner: owner.clone(),
            permit: None,
            intent_sha256: "1".repeat(64),
            identity_digest: "2".repeat(64),
            guest_identity_sha256: "3".repeat(64),
            identity_expires_unix_ms: 5_000,
            confirmed: false,
        };
        // An unconfirmed binding never yields an allocation view.
        assert!(m.bound(&binding).is_err());
        binding.confirmed = true;
        let view = m.bound(&binding).unwrap();
        assert!(!view.is_inert());
        assert_eq!(view.start.owner, owner);
        assert_eq!(view.key(), slot);
        assert_eq!(view.directory(), m.directory());
        assert_eq!(view.group(), m.group());
        assert_eq!(view.jail_root(), m.jail_root());
        let (receipt_owner, digest) = view.receipt_identity().unwrap();
        assert_eq!(
            (receipt_owner, digest),
            m.receipt_identity().unwrap(),
            "the running guardian's receipt identity never changes"
        );
        assert!(view.bound(&binding).is_err());
    }

    #[test]
    fn only_unadopted_unretired_members_hold_capacity() {
        let host = HostId::generate();
        let mut journal = Journal {
            launch_authority: None,
            version: 1,
            host,
            epoch: 3,
            records: BTreeMap::new(),
            warm: BTreeMap::new(),
            warm_refusals: Default::default(),
            poisoned: false,
        };
        let mut add = |record: MemberRecord| {
            let manifest = inert(host, record.member(), record.slot);
            journal.warm.insert(
                record.member().to_string(),
                Entry {
                    record,
                    manifest,
                    previous_retire: None,
                    gate: Arc::default(),
                },
            );
        };
        let preparing = MemberRecord::new(prepare(host), AllocationId::generate());
        let mut retired = MemberRecord::new(prepare(host), AllocationId::generate());
        assert!(retired.retire_locally("prepare_failed"));
        let mut retiring = retired.clone();
        retiring.prepare.member_id = uuid::Uuid::now_v7();
        assert!(retired.mark_retired().unwrap());
        add(preparing);
        add(retiring);
        add(retired);
        let (cpu, memory, disk) = Host::warm_held(&journal);
        assert_eq!(cpu, 2);
        assert_eq!(memory, 2 * (512 + MEMORY_OVERHEAD));
        assert_eq!(disk, 2 * (1024 + DISK_OVERHEAD));
    }
    fn source_fixture(temp: &Path) -> (Host, Entry) {
        let host = HostId::generate();
        let record = MemberRecord::new(prepare(host), AllocationId::generate());
        let mut manifest = inert(host, record.member(), record.slot);
        manifest.config.state_root = temp.join("a");
        manifest.config.cgroup_parent = temp.join("cgroups");
        let entry = Entry {
            record,
            manifest,
            previous_retire: None,
            gate: Arc::default(),
        };
        let config = Config {
            capabilities: None,
            launch_permits_required: false,
            host,
            epoch: 4,
            state_root: temp.into(),
            cgroup_parent: temp.join("cgroups"),
            guardian_binary: temp.join("unused"),
            firecracker: artifact(),
            jailer: artifact(),
            images: BTreeMap::new(),
            jail_uid: 65534,
            jail_gid: 65534,
            capacity: Capacity {
                vcpu: 1024,
                memory_mib: 1_000_000,
                disk_mib: 1_000_000,
            },
        };
        let journal = Journal {
            launch_authority: None,
            version: 1,
            host,
            epoch: 4,
            records: BTreeMap::new(),
            warm: BTreeMap::from([(entry.record.member().to_string(), entry.clone())]),
            warm_refusals: Default::default(),
            poisoned: false,
        };
        let host = Host {
            inner: Arc::new(Inner {
                authority: None,
                config,
                journal: Mutex::new(journal),
                _lock: fs::File::create(temp.join("test.lock")).unwrap(),
                workers: Arc::new(tokio::sync::Semaphore::new(1)),
                artifacts: None,
                archive_workers: tokio::sync::Semaphore::new(1),
                output_readers: tokio::sync::Semaphore::new(1),
                file_readers: crate::file_downloads::Workers::default(),
                downloads: Mutex::new(crate::file_downloads::Registry::default()),
                cursor: AtomicUsize::new(0),
            }),
        };
        (host, entry)
    }

    fn previous(entry: &Entry) -> crate::warm_cleanup::PreviousRetireIntent {
        crate::warm_cleanup::PreviousRetireIntent {
            version: VERSION,
            reporting_epoch: 4,
            original: RetireIntent {
                version: VERSION,
                member_id: entry.record.member(),
                vm_ref: Some(entry.record.vm_ref.clone()),
                host_id: entry.record.prepare.host_id,
                supervisor_epoch: 3,
                operation_id: uuid::Uuid::now_v7(),
                operation_revision: 2,
            },
        }
    }

    #[test]
    fn previous_cleanup_persists_claim_but_never_refunds_missing_guardian_evidence() {
        let temp = tempfile::tempdir().unwrap();
        let (host, entry) = source_fixture(temp.path());
        let request = previous(&entry);
        // No guardian/manifest exists. Failure must retain capacity and claim.
        assert!(host.warm_previous_retire(request.clone()).is_err());
        let bytes = fs::read(temp.path().join("host.json")).unwrap();
        let recovered: Journal = serde_json::from_slice(&bytes).unwrap();
        let saved = &recovered.warm[&entry.record.member().to_string()];
        assert_eq!(saved.previous_retire.as_ref(), Some(&request));
        assert!(matches!(saved.record.state, MemberState::Retiring { .. }));
        assert!(saved.record.holds_capacity());
        assert!(saved.record.cleanup_receipt().is_none());
        let mut stale = request.clone();
        stale.original.operation_revision = 1;
        assert!(host.warm_previous_retire(stale).is_err());
        assert_eq!(fs::read(temp.path().join("host.json")).unwrap(), bytes);
        let mut unknown = request;
        unknown.original.member_id = uuid::Uuid::now_v7();
        assert!(host.warm_previous_retire(unknown).is_err());
        assert_eq!(host.journal().unwrap().warm.len(), 1);
    }

    #[test]
    fn failed_cleanup_intent_write_poison_fences_and_keeps_original_authority() {
        let temp = tempfile::tempdir().unwrap();
        let (host, entry) = source_fixture(temp.path());
        host.save(&mut host.journal().unwrap()).unwrap();
        let original = fs::read(temp.path().join("host.json")).unwrap();
        fs::rename(
            temp.path().join("host.json"),
            temp.path().join("before.json"),
        )
        .unwrap();
        fs::create_dir(temp.path().join("host.json")).unwrap();
        assert!(host.warm_previous_retire(previous(&entry)).is_err());
        assert!(host.inner.journal.lock().unwrap().poisoned);
        assert!(host.member_entry(&entry.record.member()).is_err());
        let recovered: Journal = serde_json::from_slice(&original).unwrap();
        let before = &recovered.warm[&entry.record.member().to_string()];
        assert!(before.previous_retire.is_none());
        assert!(before.record.holds_capacity());
        assert_eq!(fs::read(temp.path().join("before.json")).unwrap(), original);
    }

    #[test]
    fn retired_lifetime_budget_keeps_tombstones_and_refuses_new_identity() {
        let temp = tempfile::tempdir().unwrap();
        let (host, mut entry) = source_fixture(temp.path());
        entry.record.retire_locally("test");
        entry.record.mark_retired().unwrap();
        {
            let mut j = host.journal().unwrap();
            j.warm.clear();
            for _ in 0..MAX_MEMBERS {
                let mut e = entry.clone();
                e.record.prepare.member_id = uuid::Uuid::now_v7();
                j.warm.insert(e.record.member().to_string(), e);
            }
        }
        let error = host
            .reserve_member(&prepare(host.inner.config.host))
            .unwrap_err();
        assert_eq!(error.code(), tonic::Code::ResourceExhausted);
        assert_eq!(host.journal().unwrap().warm.len(), MAX_MEMBERS);
        assert_eq!(Host::warm_held(&host.journal().unwrap()), (0, 0, 0));
        let i = RetireIntent {
            version: VERSION,
            member_id: uuid::Uuid::now_v7(),
            vm_ref: None,
            host_id: host.inner.config.host,
            supervisor_epoch: 4,
            operation_id: uuid::Uuid::now_v7(),
            operation_revision: 1,
        };
        assert!(
            host.warm_retire_sync(WarmRetireRequest {
                intent_json: warm_pool::encode(&i).unwrap()
            })
            .is_err()
        );
    }
    #[test]
    fn unnamed_retire_before_prepare_persists_bounded_no_resurrection_fence() {
        let temp = tempfile::tempdir().unwrap();
        let (host, _) = source_fixture(temp.path());
        let mut prepare = prepare(host.inner.config.host);
        prepare.supervisor_epoch = 4;
        let retire = RetireIntent {
            version: VERSION,
            member_id: prepare.member_id,
            vm_ref: None,
            host_id: prepare.host_id,
            supervisor_epoch: 4,
            operation_id: uuid::Uuid::now_v7(),
            operation_revision: 1,
        };
        let reply = host
            .warm_retire_sync(WarmRetireRequest {
                intent_json: warm_pool::encode(&retire).unwrap(),
            })
            .unwrap();
        let WarmReceipt::Cleanup { receipt } = warm_pool::decode(&reply.receipt_json).unwrap()
        else {
            panic!("cleanup expected")
        };
        assert!(receipt.destroyed && receipt.vm_ref.is_none());
        let bytes = fs::read(temp.path().join("host.json")).unwrap();
        let recovered: Journal = serde_json::from_slice(&bytes).unwrap();
        assert!(recovered.warm_refusals.contains(&prepare.member_id));
        // Denial occurs before image staging or guardian launch, including on
        // restart when only the persisted journal survives.
        assert_eq!(
            host.reserve_member(&prepare).unwrap_err().code(),
            tonic::Code::FailedPrecondition
        );
        *host.inner.journal.lock().unwrap() = recovered;
        assert!(host.reserve_member(&prepare).is_err());
        assert_eq!(fs::read(temp.path().join("host.json")).unwrap(), bytes);
    }
}
