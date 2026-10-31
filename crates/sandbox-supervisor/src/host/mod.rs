//! Durable, root-operated lifecycle adapter. Guest readiness and cleanup remain observations.
mod archive;
mod authority;
mod capabilities;
mod commands;
mod executable;
mod file_downloads;
mod files;
mod forgetting;
mod history;
mod image_admission;
mod journal;
mod live_output;
mod metadata_retirement;
mod previous;
mod readers;
mod released_history;
mod retirement;
mod warm;
use crate::guardian::{self, Action, Artifact, Manifest, Receipt, State as GuardianState};
use journal::{Journal, Record};
use sandbox_protocol::{
    AllocationId, HostId, Id, OperationId, ProjectId, SandboxId,
    supervisor::{
        AllocationState, CreateRequest, HealthRequest, HostInfo, InspectRequest, LeaseInspection,
        LeaseObservation, LeaseOwnership, LeaseRequest, Observation, Ownership, StopRequest,
        supervisor_server::Supervisor,
    },
};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
    process::{Command, Stdio},
    sync::{
        Arc, Mutex, MutexGuard,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};
use tonic::{Request, Response, Status};

// Shared with database placement so both layers charge identical overhead.
const MEMORY_OVERHEAD: u64 = sandbox_protocol::resources::VMM_MEMORY_OVERHEAD_MIB as u64;
const DISK_OVERHEAD: u64 = sandbox_protocol::resources::ARTIFACT_DISK_OVERHEAD_MIB as u64;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Image {
    pub kernel: Artifact,
    pub rootfs: Artifact,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Capacity {
    pub vcpu: u64,
    pub memory_mib: u64,
    pub disk_mib: u64,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// Opt-in versioned operator contract; enroll matching database policy first.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub capabilities: Option<sandbox_protocol::host_capabilities::Capabilities>,
    /// Enable only during fresh-host provisioning; persisted mode cannot downgrade.
    #[serde(default)]
    pub launch_permits_required: bool,
    pub host: HostId,
    /// Fresh externally issued epoch for every server start, strictly above the retained value.
    pub epoch: i64,
    pub state_root: PathBuf,
    pub cgroup_parent: PathBuf,
    pub guardian_binary: PathBuf,
    pub firecracker: Artifact,
    pub jailer: Artifact,
    pub images: BTreeMap<String, Image>,
    pub jail_uid: u32,
    pub jail_gid: u32,
    /// Includes guest RAM plus 128 MiB VMM allowance and disk plus 401 MiB artifact allowance.
    pub capacity: Capacity,
}
impl Config {
    fn guardian_config(&self, image: &Image) -> guardian::Config {
        guardian::Config {
            state_root: self.state_root.join("a"),
            cgroup_parent: self.cgroup_parent.clone(),
            firecracker: self.firecracker.clone(),
            jailer: self.jailer.clone(),
            kernel: image.kernel.clone(),
            rootfs: image.rootfs.clone(),
            jail_uid: self.jail_uid,
            jail_gid: self.jail_gid,
        }
    }
}
#[derive(Debug)]
struct Inner {
    authority: Option<crate::launch_authority::AuthorityFile>,
    config: Config,
    journal: Mutex<Journal>,
    // Never inherited by exec children; one service owns this state directory.
    _lock: fs::File,
    workers: Arc<tokio::sync::Semaphore>,
    artifacts: Option<sandbox_artifacts::ArtifactStore>,
    archive_workers: tokio::sync::Semaphore,
    output_readers: tokio::sync::Semaphore,
    file_readers: crate::file_downloads::Workers,
    downloads: Mutex<crate::file_downloads::Registry>,
    cursor: AtomicUsize,
}
#[derive(Debug, Clone)]
pub struct Host {
    inner: Arc<Inner>,
}
fn uncertain(error: impl std::fmt::Display) -> Status {
    eprintln!("sandbox host uncertain operation: {error}");
    Status::unavailable("host operation uncertain; inspect retained allocation before retry")
}
fn lock<T>(value: &Mutex<T>) -> Result<MutexGuard<'_, T>, Status> {
    value.lock().map_err(uncertain)
}
fn deadline(until: i64) -> Result<(), Status> {
    let remaining = until.checked_sub(guardian::wall_ms()).unwrap_or(0);
    if !(1..=300_000).contains(&remaining) {
        return Err(Status::failed_precondition(
            "expired or unbounded claim/lease",
        ));
    }
    Ok(())
}
fn same_allocation(a: &Ownership, b: &Ownership) -> bool {
    a.host_id == b.host_id
        && a.project_id == b.project_id
        && a.sandbox_id == b.sandbox_id
        && a.allocation_id == b.allocation_id
        && a.generation == b.generation
        && a.supervisor_epoch == b.supervisor_epoch
}
impl Host {
    pub fn open(config: Config) -> anyhow::Result<Self> {
        Self::open_with_artifacts(config, None)
    }
    pub fn open_with_artifacts(
        config: Config,
        artifacts: Option<sandbox_artifacts::ArtifactStore>,
    ) -> anyhow::Result<Self> {
        anyhow::ensure!(
            rustix::process::geteuid().is_root(),
            "real supervisor requires root"
        );
        anyhow::ensure!(
            config.epoch > 0
                && !config.images.is_empty()
                && config.images.len() <= 64
                && config
                    .images
                    .keys()
                    .all(|s| sandbox_protocol::images::valid_image_digest(s))
                && config.capacity.vcpu > 0
                && config.capacity.memory_mib > 0
                && config.capacity.disk_mib > 0
                && config.state_root.is_absolute()
                && config.guardian_binary.is_absolute(),
            "invalid real host configuration"
        );
        image_admission::validate(&config.images)?;
        capabilities::preflight(&config)?;
        for image in config.images.values() {
            Manifest {
                launch_permit: None,
                config: config.guardian_config(image),
                start: guardian::Start {
                    owner: guardian::Owner {
                        host: config.host,
                        project: ProjectId::generate(),
                        sandbox: SandboxId::generate(),
                        allocation: AllocationId::generate(),
                        create_operation: OperationId::generate(),
                        generation: 1,
                        epoch: config.epoch,
                    },
                    vcpu: 1,
                    memory_mib: 128,
                    disk_mib: 64,
                    expires_unix_ms: guardian::wall_ms() + 1000,
                },
                member: None,
            }
            .validate()?;
        }
        executable::validate(&config.guardian_binary, 0)?;
        let (file, mut journal) = journal::open(&config)?;
        let authority = authority::open(&config, &mut journal)?;
        Ok(Self {
            inner: Arc::new(Inner {
                authority,
                config,
                journal: Mutex::new(journal),
                _lock: file,
                workers: Arc::new(tokio::sync::Semaphore::new(8)),
                artifacts,
                archive_workers: tokio::sync::Semaphore::new(2),
                output_readers: tokio::sync::Semaphore::new(4),
                file_readers: crate::file_downloads::Workers::default(),
                downloads: Mutex::new(crate::file_downloads::Registry::default()),
                cursor: AtomicUsize::new(0),
            }),
        })
    }
    fn journal(&self) -> Result<MutexGuard<'_, Journal>, Status> {
        let j = lock(&self.inner.journal)?;
        if j.poisoned {
            return Err(Status::unavailable("host journal requires recovery"));
        }
        Ok(j)
    }
    fn save(&self, j: &mut Journal) -> Result<(), Status> {
        journal::save(&self.inner.config, j).map_err(uncertain)
    }
    fn owner(&self, owner: Option<Ownership>) -> Result<Ownership, Status> {
        let o = owner.ok_or_else(|| Status::invalid_argument("ownership required"))?;
        let invalid = || Status::invalid_argument("invalid ownership");
        o.host_id.parse::<HostId>().map_err(|_| invalid())?;
        o.project_id.parse::<ProjectId>().map_err(|_| invalid())?;
        o.sandbox_id.parse::<SandboxId>().map_err(|_| invalid())?;
        o.allocation_id
            .parse::<AllocationId>()
            .map_err(|_| invalid())?;
        o.operation_id
            .parse::<OperationId>()
            .map_err(|_| invalid())?;
        if o.generation <= 0 || o.claim_revision <= 0 {
            return Err(invalid());
        }
        if o.host_id != self.inner.config.host.to_string()
            || o.supervisor_epoch != self.inner.config.epoch
        {
            return Err(Status::failed_precondition(
                "wrong host or supervisor epoch",
            ));
        }
        deadline(o.claim_expires_unix_ms)?;
        Ok(o)
    }
    // Called before the per-allocation gate to prevent unbounded lock creation.
    // The revision is validated again under that gate before any action.
    fn gate(&self, o: &Ownership) -> Result<Arc<Mutex<()>>, Status> {
        let mut j = self.journal()?;
        if let Some(r) = j.records.get(&o.allocation_id) {
            if !same_allocation(&r.owner, o) {
                return Err(Status::failed_precondition("allocation identity mismatch"));
            }
            retirement::check(r)?;
            return Ok(r.gate.clone());
        }
        if j.records.len() >= journal::MAX_RECORDS {
            return Err(Status::resource_exhausted("host receipt capacity full"));
        }
        // A missing journal row must not recreate an unregistered or forgotten
        // allocation, even through Inspect/Stop rather than Create.
        let _authority = if self.inner.authority.is_some() {
            let checkpoint = j
                .launch_authority
                .as_ref()
                .ok_or_else(|| uncertain("missing authority checkpoint"))?;
            let allocation = o.allocation_id.parse().map_err(uncertain)?;
            let (permit, guard) = crate::launch_authority::authorize_registered_allocation(
                &self.inner.config.state_root.join("a"),
                self.inner.config.host,
                self.inner.config.epoch,
                checkpoint.registered_through,
                allocation,
            )
            .map_err(|_| Status::failed_precondition("registered active allocation required"))?;
            if permit.project.to_string() != o.project_id
                || permit.sandbox.to_string() != o.sandbox_id
                || permit.generation != o.generation
            {
                return Err(Status::failed_precondition(
                    "registered allocation ownership mismatch",
                ));
            }
            Some(guard)
        } else {
            None
        };
        let gate = Arc::new(Mutex::new(()));
        j.records.insert(
            o.allocation_id.clone(),
            Record {
                owner: o.clone(),
                retirement: None,
                metadata_retirement: None,
                revisions: BTreeMap::new(),
                create: None,
                manifest: None,
                dispatched: false,
                stopped: false,
                released: false,
                commands: BTreeMap::new(),
                files: BTreeMap::new(),
                archives: BTreeMap::new(),
                command_history: None,
                released_commands: None,
                released_files: None,
                file_history: None,
                lease_revision: 0,
                lease_request: None,
                gate: gate.clone(),
                file_io: journal::file_io(),
                readers: Arc::default(),
            },
        );
        self.save(&mut j)?;
        Ok(gate)
    }
    fn fence(&self, o: &Ownership) -> Result<Record, Status> {
        deadline(o.claim_expires_unix_ms)?;
        let mut j = self.journal()?;
        let r = j
            .records
            .get_mut(&o.allocation_id)
            .ok_or_else(|| uncertain("record missing"))?;
        if !same_allocation(&r.owner, o) {
            return Err(Status::failed_precondition("allocation identity mismatch"));
        }
        retirement::check(r)?;
        if !r.revisions.contains_key(&o.operation_id) && r.revisions.len() >= 64 {
            return Err(Status::resource_exhausted("operation fence capacity full"));
        }
        let revision = r.revisions.entry(o.operation_id.clone()).or_default();
        if o.claim_revision < *revision {
            return Err(Status::failed_precondition("stale controller claim"));
        }
        *revision = o.claim_revision;
        let result = r.clone();
        self.save(&mut j)?;
        Ok(result)
    }
    fn stopped(&self, id: &str) -> Result<(), Status> {
        let mut j = self.journal()?;
        j.records
            .get_mut(id)
            .ok_or_else(|| uncertain("record missing"))?
            .stopped = true;
        self.save(&mut j)
    }
    fn released(&self, id: &str) -> Result<(), Status> {
        let mut j = self.journal()?;
        let r = j
            .records
            .get_mut(id)
            .ok_or_else(|| uncertain("record missing"))?;
        if r.released {
            return Ok(());
        }
        r.stopped = true;
        r.released = true;
        self.save(&mut j)
    }
    fn manifest(&self, o: &Ownership, request: &CreateRequest) -> Result<Manifest, Status> {
        let c = &self.inner.config;
        let image = c
            .images
            .get(&request.image_digest)
            .ok_or_else(|| Status::permission_denied("image not allowed"))?;
        let r = request
            .resources
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("resources required"))?;
        if !sandbox_protocol::resources::supported(
            r.vcpu.into(),
            r.memory_mib.into(),
            r.disk_mib.into(),
        ) {
            return Err(Status::invalid_argument(
                "resources outside real VM envelope",
            ));
        }
        if let Some(capabilities) = &c.capabilities
            && !capabilities.supports(
                &request.image_digest,
                u64::from(r.vcpu),
                r.memory_mib,
                r.disk_mib,
            )
        {
            return Err(Status::failed_precondition(
                "host image/resource compatibility mismatch",
            ));
        }
        deadline(request.allocation_expires_unix_ms)?;
        Ok(Manifest {
            launch_permit: if self.inner.authority.is_some() {
                Some(
                    serde_json::from_slice(&request.launch_permit_json)
                        .map_err(|_| Status::invalid_argument("invalid launch permit"))?,
                )
            } else {
                None
            },
            config: c.guardian_config(image),
            start: guardian::Start {
                owner: guardian::Owner {
                    host: c.host,
                    project: o.project_id.parse().map_err(uncertain)?,
                    sandbox: o.sandbox_id.parse().map_err(uncertain)?,
                    allocation: o.allocation_id.parse().map_err(uncertain)?,
                    create_operation: o.operation_id.parse().map_err(uncertain)?,
                    generation: o.generation,
                    epoch: o.supervisor_epoch,
                },
                vcpu: r.vcpu,
                memory_mib: r.memory_mib,
                disk_mib: r.disk_mib,
                expires_unix_ms: request.allocation_expires_unix_ms,
            },
            member: None,
        })
    }
    fn admit(
        &self,
        o: &Ownership,
        request: &CreateRequest,
        manifest: Manifest,
    ) -> Result<Record, Status> {
        let mut j = self.journal()?;
        // Unadopted warm members reserve capacity exactly like allocations.
        let (mut cpu, mut memory, mut disk) = Self::warm_held(&j);
        for (id, r) in &j.records {
            if id != &o.allocation_id
                && r.owner.sandbox_id == o.sandbox_id
                && (r.owner.project_id != o.project_id
                    || r.owner.generation >= o.generation
                    || (r.manifest.is_some() && !r.released)
                    || !r.stopped)
            {
                return Err(Status::failed_precondition(
                    "previous sandbox generation not released",
                ));
            }
            if let Some(m) = &r.manifest
                && !r.released
            {
                cpu = cpu.saturating_add(u64::from(m.start.vcpu));
                memory = memory.saturating_add(m.start.memory_mib + MEMORY_OVERHEAD);
                disk = disk.saturating_add(m.start.disk_mib + DISK_OVERHEAD);
            }
        }
        let cap = &self.inner.config.capacity;
        if u64::from(manifest.start.vcpu) > cap.vcpu.saturating_sub(cpu)
            || manifest.start.memory_mib + MEMORY_OVERHEAD > cap.memory_mib.saturating_sub(memory)
            || manifest.start.disk_mib + DISK_OVERHEAD > cap.disk_mib.saturating_sub(disk)
        {
            return Err(Status::resource_exhausted("host capacity reserved"));
        }
        let r = j
            .records
            .get_mut(&o.allocation_id)
            .ok_or_else(|| uncertain("record missing"))?;
        r.create = Some(request.clone());
        r.manifest = Some(manifest);
        let result = r.clone();
        self.save(&mut j)?;
        Ok(result)
    }
    fn cleanup(&self, record: &Record) -> Result<Option<Receipt>, Status> {
        let Some(m) = &record.manifest else {
            let id: AllocationId = record.owner.allocation_id.parse().map_err(uncertain)?;
            if self
                .inner
                .config
                .state_root
                .join("a")
                .join(id.to_string())
                .try_exists()
                .map_err(uncertain)?
                || self
                    .inner
                    .config
                    .cgroup_parent
                    .join(id.uuid().to_string())
                    .try_exists()
                    .map_err(uncertain)?
            {
                return Err(uncertain("unowned allocation state prevents absence proof"));
            }
            return Ok(None);
        };
        // A timeout is uncertain. The lifecycle lock prevents racing a live owner;
        // fence_unstarted also fences a delayed wrapper that has not taken the lock.
        let _ = guardian::control(m, Action::Stop);
        let r = match m.fence_unstarted() {
            Ok(receipt) => receipt,
            Err(error)
                if crate::launch_authority::is_lock_contended(&error)
                    || guardian::is_ownership_busy(&error) =>
            {
                return Err(Status::unavailable("allocation cleanup busy"));
            }
            Err(error) => return Err(uncertain(error)),
        };
        if r.state != GuardianState::Stopped || !r.cleanup_confirmed {
            return Err(uncertain("cleanup pending"));
        }
        self.released(&record.owner.allocation_id)?;
        Ok(Some(r))
    }
    fn observe_record(
        &self,
        record: &Record,
    ) -> Result<(AllocationState, Option<Receipt>), Status> {
        retirement::check(record)?;
        if record.stopped {
            let receipt = self.cleanup(record)?;
            return Ok((
                if record.manifest.is_some() {
                    AllocationState::Released
                } else {
                    AllocationState::FencedAbsent
                },
                receipt,
            ));
        }
        let Some(m) = &record.manifest else {
            return Ok((AllocationState::Absent, None));
        };
        // BindGuest includes authenticated hello and persists the exact boot ID.
        if let Ok(reply) = guardian::control(m, Action::BindGuest)
            && reply.error.is_none()
            && let Some(r) = reply.receipt
            && r.state == GuardianState::Running
            && r.guest_boot_id.is_some()
            && r.expires_unix_ms > guardian::wall_ms()
        {
            return Ok((AllocationState::Ready, Some(r)));
        }
        // No live readiness evidence. A free ownership lock permits fencing, never relaunch.
        let r = m.fence_unstarted().map_err(|error| {
            if crate::launch_authority::is_lock_contended(&error)
                || guardian::is_ownership_busy(&error)
            {
                Status::unavailable("allocation guardian is still active")
            } else {
                uncertain(error)
            }
        })?;
        self.released(&record.owner.allocation_id)?;
        Ok((AllocationState::Released, Some(r)))
    }
    fn observation(
        &self,
        o: Ownership,
        record: &Record,
        state: AllocationState,
        r: Option<Receipt>,
    ) -> Observation {
        Observation {
            ownership: Some(o),
            state: state as i32,
            simulated: false,
            start_count: u64::from(
                r.as_ref()
                    .is_some_and(|r| r.firecracker_namespace_pid.is_some()),
            ),
            create_operation_id: record
                .manifest
                .as_ref()
                .map(|m| m.start.owner.create_operation.to_string())
                .unwrap_or_default(),
            observed_unix_ms: guardian::wall_ms(),
            reason: match state {
                AllocationState::Ready => "authenticated guest boot bound",
                AllocationState::Released => "owned cgroup and runtime files cleaned",
                AllocationState::FencedAbsent => "durable stop before create",
                _ => "no allocation execution evidence",
            }
            .into(),
        }
    }
    fn create_sync(&self, request: CreateRequest) -> Result<Observation, Status> {
        let o = self.owner(request.ownership.clone())?;
        let _authority = self.authorize_create(&o, &request)?;
        let gate = self.gate(&o)?;
        let _gate = lock(&gate)?;
        let mut record = self.fence(&o)?;
        if let Some(original) = &record.create {
            if original.ownership.as_ref().map(|o| &o.operation_id) != Some(&o.operation_id)
                || original.launch_permit_json != request.launch_permit_json
                || original.image_digest != request.image_digest
                || original.resources != request.resources
                || original.allocation_expires_unix_ms != request.allocation_expires_unix_ms
            {
                return Err(Status::already_exists(
                    "allocation retry changed create input",
                ));
            }
            let (state, r) = self.observe_record(&record)?;
            return Ok(self.observation(o, &record, state, r));
        }
        if record.stopped {
            return Err(Status::failed_precondition("allocation is stop-fenced"));
        }
        let manifest = self.manifest(&o, &request)?;
        record = self.admit(&o, &request, manifest.clone())?;
        if manifest.prepare().is_err() || deadline(o.claim_expires_unix_ms).is_err() {
            self.stopped(&o.allocation_id)?;
            record.stopped = true;
            let (state, r) = self.observe_record(&record)?;
            return Ok(self.observation(o, &record, state, r));
        }
        {
            let mut j = self.journal()?;
            j.records
                .get_mut(&o.allocation_id)
                .ok_or_else(|| uncertain("record missing"))?
                .dispatched = true;
            self.save(&mut j)?;
        }
        // Never unshare a multithreaded server. The separate single-threaded
        // executable owns namespace setup and the child guardian.
        if !self.spawn_guardian(&manifest, format!("allocation {}", o.allocation_id)) {
            self.stopped(&o.allocation_id)?;
            record.stopped = true;
            let (s, r) = self.observe_record(&record)?;
            return Ok(self.observation(o, &record, s, r));
        }
        // Give the child time to acquire ownership. Do not reconcile Prepared here:
        // a delayed spawn is still an in-flight effect of this accepted request.
        let until = Instant::now() + Duration::from_secs(4);
        loop {
            if let Ok(reply) = guardian::control(&manifest, Action::BindGuest)
                && reply.error.is_none()
                && let Some(r) = reply.receipt
                && r.state == GuardianState::Running
                && r.guest_boot_id.is_some()
                && r.expires_unix_ms > guardian::wall_ms()
            {
                return Ok(self.observation(o, &record, AllocationState::Ready, Some(r)));
            }
            if Instant::now() >= until {
                return Err(uncertain("guest readiness pending"));
            }
            std::thread::sleep(Duration::from_millis(25));
        }
    }
    fn inspect_sync(&self, owner: Option<Ownership>, stop: bool) -> Result<Observation, Status> {
        let o = self.owner(owner)?;
        let gate = self.gate(&o)?;
        let _gate = lock(&gate)?;
        let mut record = self.fence(&o)?;
        if stop {
            self.stopped(&o.allocation_id)?;
            record.stopped = true;
        }
        let (state, r) = self.observe_record(&record)?;
        Ok(self.observation(o, &record, state, r))
    }
    fn lease_sync(
        &self,
        owner: Option<LeaseOwnership>,
        until: Option<i64>,
    ) -> Result<LeaseObservation, Status> {
        let owner = owner.ok_or_else(|| Status::invalid_argument("lease ownership required"))?;
        // Reuse strict identity/deadline validation without inventing an operation fence.
        let o = self.owner(Some(Ownership {
            host_id: owner.host_id.clone(),
            project_id: owner.project_id.clone(),
            sandbox_id: owner.sandbox_id.clone(),
            allocation_id: owner.allocation_id.clone(),
            operation_id: OperationId::generate().to_string(),
            generation: owner.generation,
            supervisor_epoch: owner.supervisor_epoch,
            claim_revision: owner.revision,
            claim_expires_unix_ms: owner.claim_expires_unix_ms,
        }))?;
        if let Some(until) = until {
            deadline(until)?;
        }
        let gate = {
            let j = self.journal()?;
            j.records.get(&o.allocation_id).map(|r| r.gate.clone())
        };
        let Some(gate) = gate else {
            return Ok(LeaseObservation {
                ownership: Some(owner),
                state: AllocationState::Absent as i32,
                simulated: false,
                allocation_expires_unix_ms: 0,
                observed_unix_ms: guardian::wall_ms(),
            });
        };
        let _gate = lock(&gate)?;
        deadline(owner.claim_expires_unix_ms)?;
        let record = {
            let mut j = self.journal()?;
            let r = j
                .records
                .get_mut(&o.allocation_id)
                .ok_or_else(|| uncertain("record missing"))?;
            if !same_allocation(&r.owner, &o) || owner.revision < r.lease_revision {
                return Err(Status::failed_precondition(
                    "stale or mismatched lease ownership",
                ));
            }
            if let Some(until) = until {
                if r.lease_request
                    .is_some_and(|(rev, expiry)| rev == owner.revision && expiry != until)
                {
                    return Err(Status::already_exists("lease retry changed deadline"));
                }
                r.lease_request = Some((owner.revision, until));
            }
            r.lease_revision = owner.revision;
            let result = r.clone();
            self.save(&mut j)?;
            result
        };
        if let Some(until) = until
            && !record.stopped
            && let Some(m) = &record.manifest
        {
            let response = guardian::control(
                m,
                Action::Renew {
                    revision: owner.revision as u64,
                    expires_unix_ms: until,
                },
            )
            .map_err(uncertain)?;
            if response.error.is_some() {
                return Err(uncertain("renewal unconfirmed"));
            }
        }
        let (state, receipt) = self.observe_record(&record)?;
        Ok(LeaseObservation {
            ownership: Some(owner),
            state: state as i32,
            simulated: false,
            allocation_expires_unix_ms: receipt.map_or(0, |r| r.expires_unix_ms),
            observed_unix_ms: guardian::wall_ms(),
        })
    }
    async fn work<T: Send + 'static>(
        &self,
        f: impl FnOnce(Host) -> Result<T, Status> + Send + 'static,
    ) -> Result<T, Status> {
        let permit = self
            .inner
            .workers
            .clone()
            .try_acquire_owned()
            .map_err(|_| Status::resource_exhausted("host workers busy"))?;
        let host = self.clone();
        // Dropping an RPC does not cancel a committed operation or release its worker slot.
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            f(host)
        })
        .await
        .map_err(uncertain)?
    }
    /// Bounded maintenance pass. Old epochs can only be stopped; no recovery path starts a VM.
    pub async fn reconcile_one(&self) -> Result<(), Status> {
        self.work(|host| {
            let record = {
                let j = host.journal()?;
                let records: Vec<_> = j
                    .records
                    .values()
                    .filter(|r| {
                        if r.released || r.manifest.is_none() {
                            return false;
                        }
                        // A running, unexpired receipt is not a recovery target.
                        // Rebinding its guest on every 100 ms maintenance tick
                        // continually takes the shared launch-authority lock and
                        // can starve an exclusive retirement fence. Explicit host
                        // inspections still authenticate the live guardian.
                        if !r.stopped
                            && r.manifest.as_ref().is_some_and(|manifest| {
                                manifest.receipt().is_ok_and(|receipt| {
                                    receipt.state == GuardianState::Running
                                        && receipt.expires_unix_ms > guardian::wall_ms()
                                })
                            })
                        {
                            return false;
                        }
                        true
                    })
                    .collect();
                if records.is_empty() {
                    None
                } else {
                    let index = host.inner.cursor.fetch_add(1, Ordering::Relaxed) % records.len();
                    Some(records[index].clone())
                }
            };
            if let Some(record) = record
                && let Ok(_gate) = record.gate.try_lock()
            {
                let _ = host.observe_record(&record);
            }
            let _ = host.reconcile_member();
            Ok(())
        })
        .await
    }
}
#[tonic::async_trait]
impl Supervisor for Host {
    async fn prepare_warm_member(
        &self,
        request: Request<sandbox_protocol::supervisor::WarmPrepareRequest>,
    ) -> Result<Response<sandbox_protocol::supervisor::WarmObservation>, Status> {
        let request = request.into_inner();
        self.work(move |host| host.warm_prepare_sync(request))
            .await
            .map(Response::new)
    }

    async fn adopt_warm_member(
        &self,
        request: Request<sandbox_protocol::supervisor::WarmAdoptRequest>,
    ) -> Result<Response<sandbox_protocol::supervisor::WarmObservation>, Status> {
        let request = request.into_inner();
        self.work(move |host| host.warm_handoff_sync(request, false))
            .await
            .map(Response::new)
    }

    async fn inspect_warm_handoff(
        &self,
        request: Request<sandbox_protocol::supervisor::WarmAdoptRequest>,
    ) -> Result<Response<sandbox_protocol::supervisor::WarmObservation>, Status> {
        let request = request.into_inner();
        self.work(move |host| host.warm_handoff_sync(request, true))
            .await
            .map(Response::new)
    }

    async fn retire_warm_member(
        &self,
        request: Request<sandbox_protocol::supervisor::WarmRetireRequest>,
    ) -> Result<Response<sandbox_protocol::supervisor::WarmObservation>, Status> {
        let request = request.into_inner();
        self.work(move |host| host.warm_retire_sync(request))
            .await
            .map(Response::new)
    }

    async fn forget_allocation(
        &self,
        request: Request<sandbox_protocol::supervisor::AllocationForgetRequest>,
    ) -> Result<Response<sandbox_protocol::supervisor::AllocationForgetObservation>, Status> {
        self.work(move |host| host.forget_allocation_sync(request.into_inner()))
            .await
            .map(Response::new)
    }

    async fn retire_allocation_metadata(
        &self,
        request: Request<sandbox_protocol::supervisor::AllocationMetadataRequest>,
    ) -> Result<Response<sandbox_protocol::supervisor::AllocationMetadataObservation>, Status> {
        self.work(move |host| host.retire_allocation_metadata_sync(request.into_inner()))
            .await
            .map(Response::new)
    }

    async fn fence_allocation(
        &self,
        request: Request<sandbox_protocol::supervisor::AllocationFenceRequest>,
    ) -> Result<Response<sandbox_protocol::supervisor::AllocationFenceObservation>, Status> {
        self.work(move |host| host.fence_allocation_sync(request.into_inner()))
            .await
            .map(Response::new)
    }

    async fn allocation_authority(
        &self,
        request: Request<sandbox_protocol::supervisor::AllocationAuthorityRequest>,
    ) -> Result<Response<sandbox_protocol::supervisor::AllocationAuthorityObservation>, Status>
    {
        self.work(move |host| host.allocation_authority_sync(request.into_inner()))
            .await
            .map(Response::new)
    }

    async fn retire_released_history(
        &self,
        r: Request<sandbox_protocol::supervisor::ReleasedHistoryRequest>,
    ) -> Result<Response<sandbox_protocol::supervisor::ReleasedHistoryObservation>, Status> {
        self.work(move |h| h.retire_released_history_sync(r.into_inner()))
            .await
            .map(Response::new)
    }

    async fn history_binding(
        &self,
        r: Request<LeaseInspection>,
    ) -> Result<Response<sandbox_protocol::supervisor::HistoryBindingObservation>, Status> {
        self.work(move |h| h.history_binding_sync(r.into_inner()))
            .await
            .map(Response::new)
    }

    async fn retire_history(
        &self,
        r: Request<sandbox_protocol::supervisor::HistoryRequest>,
    ) -> Result<Response<sandbox_protocol::supervisor::HistoryObservation>, Status> {
        self.work(move |h| h.retire_history_sync(r.into_inner()))
            .await
            .map(Response::new)
    }

    async fn reconcile_previous_allocation(
        &self,
        r: Request<sandbox_protocol::supervisor::PreviousAllocationRequest>,
    ) -> Result<Response<sandbox_protocol::supervisor::PreviousAllocationObservation>, Status> {
        self.work(move |h| h.previous_allocation(r.into_inner()))
            .await
            .map(Response::new)
    }
    async fn begin_file(
        &self,
        r: Request<sandbox_protocol::supervisor::FileRequest>,
    ) -> Result<Response<sandbox_protocol::supervisor::FileObservation>, Status> {
        self.work(move |h| h.file_sync(r.into_inner(), files::FileAction::Begin))
            .await
            .map(Response::new)
    }

    async fn inspect_file(
        &self,
        r: Request<sandbox_protocol::supervisor::FileRequest>,
    ) -> Result<Response<sandbox_protocol::supervisor::FileObservation>, Status> {
        self.work(move |h| h.file_sync(r.into_inner(), files::FileAction::Inspect))
            .await
            .map(Response::new)
    }

    async fn commit_file(
        &self,
        r: Request<sandbox_protocol::supervisor::FileRequest>,
    ) -> Result<Response<sandbox_protocol::supervisor::FileObservation>, Status> {
        self.work(move |h| h.file_sync(r.into_inner(), files::FileAction::Commit))
            .await
            .map(Response::new)
    }

    async fn abort_file(
        &self,
        r: Request<sandbox_protocol::supervisor::FileRequest>,
    ) -> Result<Response<sandbox_protocol::supervisor::FileObservation>, Status> {
        self.work(move |h| h.file_sync(r.into_inner(), files::FileAction::Abort))
            .await
            .map(Response::new)
    }
    async fn write_file(
        &self,
        r: Request<sandbox_protocol::supervisor::FileWriteRequest>,
    ) -> Result<Response<sandbox_protocol::supervisor::FileObservation>, Status> {
        self.work(move |h| h.file_write_sync(r.into_inner()))
            .await
            .map(Response::new)
    }

    async fn prepare_output(
        &self,
        r: Request<sandbox_protocol::supervisor::OutputRequest>,
    ) -> Result<Response<sandbox_protocol::supervisor::OutputObservation>, Status> {
        self.prepare_output_inner(r.into_inner())
            .await
            .map(Response::new)
    }
    async fn archive_output(
        &self,
        r: Request<sandbox_protocol::supervisor::OutputRequest>,
    ) -> Result<Response<sandbox_protocol::supervisor::OutputObservation>, Status> {
        self.archive_output_inner(r.into_inner())
            .await
            .map(Response::new)
    }
    async fn execute_command(
        &self,
        r: Request<sandbox_protocol::supervisor::CommandRequest>,
    ) -> Result<Response<sandbox_protocol::supervisor::CommandObservation>, Status> {
        self.work(move |h| h.execute_command_sync(r.into_inner()))
            .await
            .map(Response::new)
    }
    async fn inspect_command(
        &self,
        r: Request<sandbox_protocol::supervisor::CommandInspection>,
    ) -> Result<Response<sandbox_protocol::supervisor::CommandObservation>, Status> {
        self.work(move |h| h.inspect_command_sync(r.into_inner()))
            .await
            .map(Response::new)
    }
    async fn cancel_command(
        &self,
        r: Request<sandbox_protocol::supervisor::CommandInspection>,
    ) -> Result<Response<sandbox_protocol::supervisor::CommandObservation>, Status> {
        self.work(move |h| h.cancel_command_sync(r.into_inner()))
            .await
            .map(Response::new)
    }

    async fn health(&self, _: Request<HealthRequest>) -> Result<Response<HostInfo>, Status> {
        self.work(|h| {
            drop(h.journal()?);
            capabilities::preflight(&h.inner.config)
                .map_err(|_| Status::unavailable("host capability preflight failed"))?;
            if h.inner.authority.is_some() {
                h.allocation_authority_sync(
                    sandbox_protocol::supervisor::AllocationAuthorityRequest {
                        host_id: h.inner.config.host.to_string(),
                        reporting_epoch: h.inner.config.epoch,
                        permits_json: Vec::new(),
                    },
                )?;
            }
            Ok(HostInfo {
                capabilities_json: h
                    .inner
                    .config
                    .capabilities
                    .as_ref()
                    .map(sandbox_protocol::host_capabilities::Capabilities::to_json)
                    .transpose()
                    .map_err(|_| Status::unavailable("host capability contract failed"))?
                    .unwrap_or_default(),
                launch_permits_required: h.inner.authority.is_some(),
                host_id: h.inner.config.host.to_string(),
                supervisor_epoch: h.inner.config.epoch,
                simulated: false,
            })
        })
        .await
        .map(Response::new)
    }
    async fn create(&self, r: Request<CreateRequest>) -> Result<Response<Observation>, Status> {
        self.work(move |h| h.create_sync(r.into_inner()))
            .await
            .map(Response::new)
    }
    async fn inspect(&self, r: Request<InspectRequest>) -> Result<Response<Observation>, Status> {
        self.work(move |h| h.inspect_sync(r.into_inner().ownership, false))
            .await
            .map(Response::new)
    }
    async fn stop(&self, r: Request<StopRequest>) -> Result<Response<Observation>, Status> {
        self.work(move |h| h.inspect_sync(r.into_inner().ownership, true))
            .await
            .map(Response::new)
    }
    async fn renew_lease(
        &self,
        r: Request<LeaseRequest>,
    ) -> Result<Response<LeaseObservation>, Status> {
        let r = r.into_inner();
        self.work(move |h| h.lease_sync(r.ownership, Some(r.allocation_expires_unix_ms)))
            .await
            .map(Response::new)
    }
    async fn inspect_lease(
        &self,
        r: Request<LeaseInspection>,
    ) -> Result<Response<LeaseObservation>, Status> {
        self.work(move |h| h.lease_sync(r.into_inner().ownership, None))
            .await
            .map(Response::new)
    }
}
