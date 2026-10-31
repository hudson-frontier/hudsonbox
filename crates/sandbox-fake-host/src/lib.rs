//! Bounded in-memory supervisor for control-plane tests. It never executes code.
//! Every observation is explicitly simulated. Restart loses evidence and requires
//! a new externally issued supervisor epoch; absence never proves old VM release.

mod archive;
mod commands;
mod file_downloads;
mod files;
mod live_output;
mod permits;
mod warm;
use sandbox_protocol::{
    AllocationId, HostId, OperationId, ProjectId, SandboxId,
    supervisor::{
        AllocationState, CreateRequest, HealthRequest, HostInfo, InspectRequest, LeaseInspection,
        LeaseObservation, LeaseOwnership, LeaseRequest, Observation, Ownership, Resources,
        StopRequest, supervisor_server::Supervisor,
    },
};
use std::{
    collections::{BTreeSet, HashMap},
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::{sync::Mutex, time::Instant};
use tonic::{Request, Response, Status};

const MAX_RECORDS: usize = 10_000;
const MAX_LEASE_MS: i64 = 300_000;

#[derive(Debug, Clone)]
pub struct FakeConfig {
    pub host: HostId,
    /// Must be freshly issued externally after every process restart.
    pub epoch: i64,
    pub images: BTreeSet<String>,
    pub capacity: Resources,
}

#[derive(Debug, Clone)]
pub struct FakeHost {
    config: Arc<FakeConfig>,
    state: Arc<Mutex<State>>,
    artifacts: Option<sandbox_artifacts::ArtifactStore>,
    archive_workers: Arc<tokio::sync::Semaphore>,
    output_readers: Arc<tokio::sync::Semaphore>,
    file_readers: sandbox_supervisor::file_downloads::Workers,
}

#[derive(Default)]
struct State {
    capabilities_json: Vec<u8>,
    allocations: HashMap<String, Record>,
    /// Retained after release so old generations cannot start again.
    sandboxes: HashMap<String, (String, String, i64)>,
    fences: HashMap<String, Fence>,
    lose_next_create_reply: bool,
    lose_next_stop_reply: bool,
    lose_next_renew_reply: bool,
    fail_next_health: bool,
    total_starts: u64,
    total_commands: u64,
    file_commits: u64,
    lose_next_file_reply: bool,
    published_files: HashMap<(String, String), Vec<u8>>,
    downloads: sandbox_supervisor::file_downloads::Registry,
    captured_files: HashMap<OperationId, (String, Vec<u8>)>,
    lose_next_download_reply: bool,
    file_captures: u64,
    hold_next_command: bool,
    lose_next_command_reply: bool,
    lose_next_cancel_reply: bool,
    lose_next_archive_reply: bool,
    archive_delay: Duration,
    archives_started: u64,
    warm: HashMap<uuid::Uuid, warm::Member>,
    warm_epoch_fenced: bool,
    warm_prepares: u64,
    warm_adoptions: u64,
    lose_next_adopt_reply: bool,
    lose_prepare_replies: u32,
    permits: permits::Registry,
}

impl std::fmt::Debug for State {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FakeState")
            .field("allocations", &self.allocations.len())
            .field("file_captures", &self.file_captures)
            .finish_non_exhaustive()
    }
}

#[derive(Debug)]
struct Record {
    create: CreateRequest,
    state: AllocationState,
    expires: Instant,
    lease_until: i64,
    reason: &'static str,
}

#[derive(Debug)]
struct Fence {
    owner: Ownership,
    revisions: HashMap<String, i64>,
    stopped: bool,
    lease_revision: i64,
    lease_request: Option<(i64, i64)>,
    commands: HashMap<String, sandbox_protocol::command::CommandRecord>,
    archives: HashMap<String, sandbox_supervisor::archive::ArchiveRecord>,
    files: HashMap<String, sandbox_protocol::supervisor_files::FileRecord>,
    file_data: HashMap<String, Vec<u8>>,
}

/// Database/controller deadlines and the supervisor wall clock must be synchronized.
/// The watchdog converts a validated wall deadline into a monotonic local deadline.
pub fn unix_ms() -> Result<i64, Status> {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| Status::internal("clock predates Unix epoch"))?
        .as_millis();
    i64::try_from(millis).map_err(|_| Status::internal("clock out of range"))
}

fn bounded_deadline(deadline: i64, now: i64) -> Result<Duration, Status> {
    let remaining = deadline
        .checked_sub(now)
        .filter(|v| *v > 0 && *v <= MAX_LEASE_MS)
        .ok_or_else(|| Status::failed_precondition("expired or unbounded lease"))?;
    Ok(Duration::from_millis(remaining as u64))
}

impl FakeHost {
    /// Development-only advertisements remain explicitly simulated.
    pub async fn set_capabilities(
        &self,
        value: sandbox_protocol::host_capabilities::Capabilities,
    ) -> Result<(), sandbox_protocol::host_capabilities::Error> {
        if !value.simulated {
            return Err(sandbox_protocol::host_capabilities::Error);
        }
        self.state.lock().await.capabilities_json = value.to_json()?;
        Ok(())
    }
    pub fn new(config: FakeConfig) -> Result<Self, Status> {
        if config.epoch <= 0
            || config.images.is_empty()
            || config
                .images
                .iter()
                .any(|s| !sandbox_protocol::images::valid_image_digest(s))
            || config.capacity.vcpu == 0
            || config.capacity.memory_mib == 0
            || config.capacity.disk_mib == 0
        {
            return Err(Status::invalid_argument("invalid fake host configuration"));
        }
        Ok(Self {
            config: Arc::new(config),
            state: Arc::new(Mutex::new(State::default())),
            artifacts: None,
            archive_workers: Arc::new(tokio::sync::Semaphore::new(2)),
            output_readers: Arc::new(tokio::sync::Semaphore::new(4)),
            file_readers: sandbox_supervisor::file_downloads::Workers::default(),
        })
    }

    /// Count applied simulated starts, including later released incarnations.
    pub async fn total_starts(&self) -> u64 {
        self.state.lock().await.total_starts
    }

    /// Fault injection: apply one create, then lose its acknowledgement.
    pub async fn lose_next_create_reply(&self) {
        self.state.lock().await.lose_next_create_reply = true;
    }

    /// Fault injection: apply a stop/fence, then lose its acknowledgement.
    pub async fn lose_next_stop_reply(&self) {
        self.state.lock().await.lose_next_stop_reply = true;
    }

    pub async fn fail_next_health_check(&self) {
        self.state.lock().await.fail_next_health = true;
    }

    pub async fn lose_next_renew_reply(&self) {
        self.state.lock().await.lose_next_renew_reply = true;
    }

    /// Also called by the binary's independent watchdog when no RPCs arrive.
    pub async fn expire_leases(&self) {
        let mut state = self.state.lock().await;
        Self::expire(&mut state);
    }

    fn expire(state: &mut State) {
        let now = Instant::now();
        for record in state.allocations.values_mut() {
            if record.state == AllocationState::Ready && record.expires <= now {
                record.state = AllocationState::Released;
                record.reason = "simulated lease expiry";
            }
        }
        state.downloads.prune(Instant::now().into_std());
        state.published_files.retain(|(id, _), _| {
            state
                .allocations
                .get(id)
                .is_some_and(|r| r.state != AllocationState::Released)
        });
        state.captured_files.retain(|ticket, (id, _)| {
            state.downloads.contains(ticket)
                && state
                    .allocations
                    .get(id)
                    .is_some_and(|r| r.state != AllocationState::Released)
        });
        for (id, record) in &state.allocations {
            if record.state == AllocationState::Released
                && let Some(fence) = state.fences.get_mut(id)
            {
                fence.file_data.clear();
            }
        }
    }

    fn ownership(&self, value: Option<Ownership>, now: i64) -> Result<Ownership, Status> {
        let value = value.ok_or_else(|| Status::invalid_argument("ownership required"))?;
        let invalid = || Status::invalid_argument("invalid ownership identity");
        value.host_id.parse::<HostId>().map_err(|_| invalid())?;
        value
            .project_id
            .parse::<ProjectId>()
            .map_err(|_| invalid())?;
        value
            .sandbox_id
            .parse::<SandboxId>()
            .map_err(|_| invalid())?;
        value
            .allocation_id
            .parse::<AllocationId>()
            .map_err(|_| invalid())?;
        value
            .operation_id
            .parse::<OperationId>()
            .map_err(|_| invalid())?;
        if value.host_id != self.config.host.to_string()
            || value.supervisor_epoch != self.config.epoch
        {
            return Err(Status::failed_precondition(
                "wrong host or supervisor epoch",
            ));
        }
        if value.generation <= 0 || value.claim_revision <= 0 {
            return Err(invalid());
        }
        bounded_deadline(value.claim_expires_unix_ms, now)?;
        Ok(value)
    }

    fn fence(state: &mut State, owner: &Ownership) -> Result<(), Status> {
        if !state.fences.contains_key(&owner.allocation_id) && state.fences.len() >= MAX_RECORDS {
            return Err(Status::resource_exhausted("fake fence store full"));
        }
        let fence = state
            .fences
            .entry(owner.allocation_id.clone())
            .or_insert_with(|| Fence {
                owner: owner.clone(),
                revisions: HashMap::new(),
                stopped: false,
                lease_revision: 0,
                lease_request: None,
                commands: HashMap::new(),
                archives: HashMap::new(),
                files: HashMap::new(),
                file_data: HashMap::new(),
            });
        if fence.owner.project_id != owner.project_id
            || fence.owner.sandbox_id != owner.sandbox_id
            || fence.owner.generation != owner.generation
        {
            return Err(Status::failed_precondition("allocation identity mismatch"));
        }
        if !fence.revisions.contains_key(&owner.operation_id) && fence.revisions.len() >= 64 {
            return Err(Status::resource_exhausted(
                "fake operation fence store full",
            ));
        }
        let revision = fence
            .revisions
            .entry(owner.operation_id.clone())
            .or_default();
        if owner.claim_revision < *revision {
            return Err(Status::failed_precondition("stale controller claim"));
        }
        *revision = owner.claim_revision;
        Ok(())
    }

    fn observation(owner: Ownership, record: Option<&Record>, now: i64) -> Observation {
        Observation {
            ownership: Some(owner),
            state: record.map_or(AllocationState::Absent, |r| r.state) as i32,
            simulated: true,
            start_count: u64::from(record.is_some()),
            create_operation_id: record
                .and_then(|r| r.create.ownership.as_ref())
                .map_or_else(String::new, |o| o.operation_id.clone()),
            observed_unix_ms: now,
            reason: record
                .map_or("no in-memory evidence", |r| r.reason)
                .to_owned(),
        }
    }

    async fn observe(&self, owner: Option<Ownership>, stop: bool) -> Result<Observation, Status> {
        let mut state = self.state.lock().await;
        Self::expire(&mut state);
        let now = unix_ms()?;
        let owner = self.ownership(owner, now)?;
        Self::fence(&mut state, &owner)?;
        if stop && let Some(fence) = state.fences.get_mut(&owner.allocation_id) {
            fence.stopped = true;
            fence.file_data.clear();
        }
        if stop {
            state
                .published_files
                .retain(|(id, _), _| id != &owner.allocation_id);
            state
                .captured_files
                .retain(|_, (id, _)| id != &owner.allocation_id);
            state
                .sandboxes
                .entry(owner.sandbox_id.clone())
                .or_insert_with(|| {
                    (
                        owner.project_id.clone(),
                        owner.allocation_id.clone(),
                        owner.generation,
                    )
                });
        }
        let fenced = state
            .fences
            .get(&owner.allocation_id)
            .is_some_and(|f| f.stopped);
        let record = state.allocations.get_mut(&owner.allocation_id);
        let observation = if let Some(record) = record {
            if stop {
                record.state = AllocationState::Released;
                record.reason = "simulated stop";
            }
            Self::observation(owner, Some(record), now)
        } else {
            let mut observation = Self::observation(owner, None, now);
            if fenced {
                observation.state = AllocationState::FencedAbsent as i32;
                observation.reason =
                    "simulated absence confirmed and future starts fenced in this epoch".into();
            }
            observation
        };
        if stop && std::mem::take(&mut state.lose_next_stop_reply) {
            return Err(Status::unavailable("injected lost stop acknowledgement"));
        }
        Ok(observation)
    }

    async fn lease_observation(
        &self,
        owner: Option<LeaseOwnership>,
        until: Option<i64>,
    ) -> Result<LeaseObservation, Status> {
        let mut state = self.state.lock().await;
        Self::expire(&mut state);
        let now = unix_ms()?;
        let owner = owner.ok_or_else(|| Status::invalid_argument("lease ownership required"))?;
        let invalid = || Status::invalid_argument("invalid lease ownership");
        owner.host_id.parse::<HostId>().map_err(|_| invalid())?;
        owner
            .project_id
            .parse::<ProjectId>()
            .map_err(|_| invalid())?;
        owner
            .sandbox_id
            .parse::<SandboxId>()
            .map_err(|_| invalid())?;
        owner
            .allocation_id
            .parse::<AllocationId>()
            .map_err(|_| invalid())?;
        if owner.revision <= 0 || owner.generation <= 0 {
            return Err(invalid());
        }
        if owner.host_id != self.config.host.to_string()
            || owner.supervisor_epoch != self.config.epoch
        {
            return Err(Status::failed_precondition(
                "wrong host or supervisor epoch",
            ));
        }
        bounded_deadline(owner.claim_expires_unix_ms, now)?;
        if let Some(until) = until {
            bounded_deadline(until, now)?;
        }
        // Maintenance can never create evidence for an allocation this epoch
        // has not seen. In particular, an empty restart cannot prove release.
        let live_until = state
            .allocations
            .get(&owner.allocation_id)
            .filter(|record| record.state == AllocationState::Ready)
            .map(|record| record.lease_until);
        let Some(fence) = state.fences.get_mut(&owner.allocation_id) else {
            return Ok(LeaseObservation {
                ownership: Some(owner),
                state: AllocationState::Absent as i32,
                simulated: true,
                allocation_expires_unix_ms: 0,
                observed_unix_ms: now,
            });
        };
        if fence.owner.project_id != owner.project_id
            || fence.owner.sandbox_id != owner.sandbox_id
            || fence.owner.generation != owner.generation
            || owner.revision < fence.lease_revision
        {
            return Err(Status::failed_precondition(
                "stale or mismatched lease ownership",
            ));
        }
        if let Some(until) = until {
            if fence
                .lease_request
                .is_some_and(|(revision, deadline)| revision == owner.revision && deadline != until)
            {
                return Err(Status::already_exists(
                    "lease retry changed the requested deadline",
                ));
            }
            if live_until.is_some_and(|prior| until < prior) {
                // Match the real guardian instead of silently keeping the old
                // lease and masking a controller regression.
                return Err(Status::failed_precondition("renewal cannot shorten lease"));
            }
            fence.lease_request = Some((owner.revision, until));
        }
        fence.lease_revision = owner.revision;
        let stopped = fence.stopped;
        let record = state.allocations.get_mut(&owner.allocation_id);
        let (allocation_state, lease_until) = if let Some(record) = record {
            if let Some(until) = until
                && !stopped
                && record.state == AllocationState::Ready
                && until > record.lease_until
            {
                // Earlier/lost/reordered requests cannot shorten a newer lease.
                record.expires = Instant::now() + bounded_deadline(until, now)?;
                record.lease_until = until;
            }
            (record.state, record.lease_until)
        } else {
            (
                if stopped {
                    AllocationState::FencedAbsent
                } else {
                    AllocationState::Absent
                },
                0,
            )
        };
        if until.is_some() && std::mem::take(&mut state.lose_next_renew_reply) {
            return Err(Status::unavailable("injected lost renewal acknowledgement"));
        }
        Ok(LeaseObservation {
            ownership: Some(owner),
            state: allocation_state as i32,
            simulated: true,
            allocation_expires_unix_ms: lease_until,
            observed_unix_ms: now,
        })
    }
}

#[tonic::async_trait]
impl Supervisor for FakeHost {
    async fn prepare_warm_member(
        &self,
        request: Request<sandbox_protocol::supervisor::WarmPrepareRequest>,
    ) -> Result<Response<sandbox_protocol::supervisor::WarmObservation>, Status> {
        self.warm_prepare(request.into_inner())
            .await
            .map(Response::new)
    }

    async fn adopt_warm_member(
        &self,
        request: Request<sandbox_protocol::supervisor::WarmAdoptRequest>,
    ) -> Result<Response<sandbox_protocol::supervisor::WarmObservation>, Status> {
        self.warm_handoff(request.into_inner(), false)
            .await
            .map(Response::new)
    }

    async fn inspect_warm_handoff(
        &self,
        request: Request<sandbox_protocol::supervisor::WarmAdoptRequest>,
    ) -> Result<Response<sandbox_protocol::supervisor::WarmObservation>, Status> {
        self.warm_handoff(request.into_inner(), true)
            .await
            .map(Response::new)
    }

    async fn retire_warm_member(
        &self,
        request: Request<sandbox_protocol::supervisor::WarmRetireRequest>,
    ) -> Result<Response<sandbox_protocol::supervisor::WarmObservation>, Status> {
        self.warm_retire(request.into_inner())
            .await
            .map(Response::new)
    }

    async fn forget_allocation(
        &self,
        _: Request<sandbox_protocol::supervisor::AllocationForgetRequest>,
    ) -> Result<Response<sandbox_protocol::supervisor::AllocationForgetObservation>, Status> {
        Err(Status::unimplemented(
            "fake host cannot forget physical allocations",
        ))
    }

    async fn retire_allocation_metadata(
        &self,
        _: Request<sandbox_protocol::supervisor::AllocationMetadataRequest>,
    ) -> Result<Response<sandbox_protocol::supervisor::AllocationMetadataObservation>, Status> {
        Err(Status::unimplemented(
            "fake host does not provide physical metadata retirement",
        ))
    }

    async fn fence_allocation(
        &self,
        _: Request<sandbox_protocol::supervisor::AllocationFenceRequest>,
    ) -> Result<Response<sandbox_protocol::supervisor::AllocationFenceObservation>, Status> {
        Err(Status::unimplemented(
            "fake host does not provide durable retirement fencing",
        ))
    }

    async fn allocation_authority(
        &self,
        request: Request<sandbox_protocol::supervisor::AllocationAuthorityRequest>,
    ) -> Result<Response<sandbox_protocol::supervisor::AllocationAuthorityObservation>, Status>
    {
        self.allocation_authority_inner(request.into_inner())
            .await
            .map(Response::new)
    }

    async fn retire_released_history(
        &self,
        _r: Request<sandbox_protocol::supervisor::ReleasedHistoryRequest>,
    ) -> Result<Response<sandbox_protocol::supervisor::ReleasedHistoryObservation>, Status> {
        Err(Status::unimplemented(
            "simulator does not provide durable destruction retirement",
        ))
    }

    async fn history_binding(
        &self,
        _r: Request<LeaseInspection>,
    ) -> Result<Response<sandbox_protocol::supervisor::HistoryBindingObservation>, Status> {
        Err(Status::unimplemented(
            "simulator does not provide durable history binding",
        ))
    }

    async fn retire_history(
        &self,
        _r: Request<sandbox_protocol::supervisor::HistoryRequest>,
    ) -> Result<Response<sandbox_protocol::supervisor::HistoryObservation>, Status> {
        Err(Status::unimplemented(
            "simulator does not provide durable history retirement",
        ))
    }

    async fn reconcile_previous_allocation(
        &self,
        _r: Request<sandbox_protocol::supervisor::PreviousAllocationRequest>,
    ) -> Result<Response<sandbox_protocol::supervisor::PreviousAllocationObservation>, Status> {
        // Restart discards this simulator's journal. A new epoch cannot invent
        // release evidence for ownership that was never durably retained.
        Err(Status::failed_precondition(
            "prior ownership evidence unavailable",
        ))
    }
    async fn begin_file(
        &self,
        r: Request<sandbox_protocol::supervisor::FileRequest>,
    ) -> Result<Response<sandbox_protocol::supervisor::FileObservation>, Status> {
        self.file_inner(r.into_inner(), files::FileAction::Begin)
            .await
            .map(Response::new)
    }

    async fn inspect_file(
        &self,
        r: Request<sandbox_protocol::supervisor::FileRequest>,
    ) -> Result<Response<sandbox_protocol::supervisor::FileObservation>, Status> {
        self.file_inner(r.into_inner(), files::FileAction::Inspect)
            .await
            .map(Response::new)
    }

    async fn commit_file(
        &self,
        r: Request<sandbox_protocol::supervisor::FileRequest>,
    ) -> Result<Response<sandbox_protocol::supervisor::FileObservation>, Status> {
        self.file_inner(r.into_inner(), files::FileAction::Commit)
            .await
            .map(Response::new)
    }

    async fn abort_file(
        &self,
        r: Request<sandbox_protocol::supervisor::FileRequest>,
    ) -> Result<Response<sandbox_protocol::supervisor::FileObservation>, Status> {
        self.file_inner(r.into_inner(), files::FileAction::Abort)
            .await
            .map(Response::new)
    }
    async fn write_file(
        &self,
        r: Request<sandbox_protocol::supervisor::FileWriteRequest>,
    ) -> Result<Response<sandbox_protocol::supervisor::FileObservation>, Status> {
        self.file_write_inner(r.into_inner())
            .await
            .map(Response::new)
    }

    async fn prepare_output(
        &self,
        r: Request<sandbox_protocol::supervisor::OutputRequest>,
    ) -> Result<Response<sandbox_protocol::supervisor::OutputObservation>, Status> {
        self.output_inner(r.into_inner(), false)
            .await
            .map(Response::new)
    }
    async fn archive_output(
        &self,
        r: Request<sandbox_protocol::supervisor::OutputRequest>,
    ) -> Result<Response<sandbox_protocol::supervisor::OutputObservation>, Status> {
        self.output_inner(r.into_inner(), true)
            .await
            .map(Response::new)
    }
    async fn execute_command(
        &self,
        r: Request<sandbox_protocol::supervisor::CommandRequest>,
    ) -> Result<Response<sandbox_protocol::supervisor::CommandObservation>, Status> {
        self.execute_command_inner(r.into_inner())
            .await
            .map(Response::new)
    }
    async fn inspect_command(
        &self,
        r: Request<sandbox_protocol::supervisor::CommandInspection>,
    ) -> Result<Response<sandbox_protocol::supervisor::CommandObservation>, Status> {
        self.inspect_command_inner(r.into_inner())
            .await
            .map(Response::new)
    }
    async fn cancel_command(
        &self,
        r: Request<sandbox_protocol::supervisor::CommandInspection>,
    ) -> Result<Response<sandbox_protocol::supervisor::CommandObservation>, Status> {
        self.cancel_command_inner(r.into_inner())
            .await
            .map(Response::new)
    }

    async fn health(&self, _: Request<HealthRequest>) -> Result<Response<HostInfo>, Status> {
        let mut state = self.state.lock().await;
        if std::mem::take(&mut state.fail_next_health) {
            return Err(Status::unavailable("injected health failure"));
        }
        Ok(Response::new(HostInfo {
            capabilities_json: state.capabilities_json.clone(),
            launch_permits_required: state.permits.required(),
            host_id: self.config.host.to_string(),
            supervisor_epoch: self.config.epoch,
            simulated: true,
        }))
    }

    async fn create(
        &self,
        request: Request<CreateRequest>,
    ) -> Result<Response<Observation>, Status> {
        let request = request.into_inner();
        let mut state = self.state.lock().await;
        Self::expire(&mut state);
        let now = unix_ms()?;
        let owner = self.ownership(request.ownership.clone(), now)?;
        Self::fence(&mut state, &owner)?;
        if let Some(record) = state.allocations.get_mut(&owner.allocation_id) {
            let original = record
                .create
                .ownership
                .as_ref()
                .ok_or_else(|| Status::internal("missing stored owner"))?;
            if original.operation_id != owner.operation_id
                || record.create.image_digest != request.image_digest
                || record.create.resources != request.resources
                || record.create.allocation_expires_unix_ms != request.allocation_expires_unix_ms
            {
                return Err(Status::already_exists(
                    "allocation retry changed the create request",
                ));
            }
            // Do not restart a released allocation or extend its lease on a retry.
            return Ok(Response::new(Self::observation(owner, Some(record), now)));
        }
        if state
            .fences
            .get(&owner.allocation_id)
            .is_some_and(|f| f.stopped)
        {
            return Err(Status::failed_precondition(
                "allocation was stopped before create",
            ));
        }
        // A new allocation receipt requires the exact registered permit.
        state
            .permits
            .admit(&request.launch_permit_json, &owner, true)?;
        let duration = bounded_deadline(request.allocation_expires_unix_ms, now)?;
        if !self.config.images.contains(&request.image_digest) {
            return Err(Status::permission_denied("image not allowed"));
        }
        let resources = request
            .resources
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("resources required"))?;
        if !sandbox_protocol::resources::supported(
            resources.vcpu.into(),
            resources.memory_mib.into(),
            resources.disk_mib.into(),
        ) {
            return Err(Status::invalid_argument(
                "resources outside supported envelope",
            ));
        }
        if let Some((project, previous, generation)) = state.sandboxes.get(&owner.sandbox_id)
            && (owner.project_id != *project
                || owner.generation <= *generation
                || state.allocations.get(previous).map_or_else(
                    || !state.fences.get(previous).is_some_and(|f| f.stopped),
                    |r| r.state != AllocationState::Released,
                ))
        {
            return Err(Status::failed_precondition(
                "previous generation not released",
            ));
        }
        if state.allocations.len() >= MAX_RECORDS {
            return Err(Status::resource_exhausted("fake receipt store full"));
        }
        // Unadopted warm members hold capacity exactly like allocations.
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
        if u64::from(resources.vcpu) > u64::from(self.config.capacity.vcpu).saturating_sub(cpu)
            || resources.memory_mib > self.config.capacity.memory_mib.saturating_sub(memory)
            || resources.disk_mib > self.config.capacity.disk_mib.saturating_sub(disk)
        {
            return Err(Status::resource_exhausted("fake host capacity full"));
        }
        state.sandboxes.insert(
            owner.sandbox_id.clone(),
            (
                owner.project_id.clone(),
                owner.allocation_id.clone(),
                owner.generation,
            ),
        );
        let record = Record {
            lease_until: request.allocation_expires_unix_ms,
            create: request,
            state: AllocationState::Ready,
            expires: Instant::now() + duration,
            reason: "simulated readiness; no VM or process started",
        };
        state.total_starts += 1;
        let observation = Self::observation(owner.clone(), Some(&record), now);
        state.allocations.insert(owner.allocation_id, record);
        if std::mem::take(&mut state.lose_next_create_reply) {
            return Err(Status::unavailable("injected lost create acknowledgement"));
        }
        Ok(Response::new(observation))
    }

    async fn inspect(
        &self,
        request: Request<InspectRequest>,
    ) -> Result<Response<Observation>, Status> {
        self.observe(request.into_inner().ownership, false)
            .await
            .map(Response::new)
    }

    async fn stop(&self, request: Request<StopRequest>) -> Result<Response<Observation>, Status> {
        self.observe(request.into_inner().ownership, true)
            .await
            .map(Response::new)
    }

    async fn renew_lease(
        &self,
        request: Request<LeaseRequest>,
    ) -> Result<Response<LeaseObservation>, Status> {
        let request = request.into_inner();
        self.lease_observation(request.ownership, Some(request.allocation_expires_unix_ms))
            .await
            .map(Response::new)
    }

    async fn inspect_lease(
        &self,
        request: Request<LeaseInspection>,
    ) -> Result<Response<LeaseObservation>, Status> {
        self.lease_observation(request.into_inner().ownership, None)
            .await
            .map(Response::new)
    }
}
