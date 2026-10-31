//! Real warm-pool lifecycle RPCs (#188) against the authenticated host
//! supervisor, its per-member guardians and bootstrapped Firecracker guests.
//!
//! Every test is ignored: it needs root, KVM and the controlled runtime
//! artifacts described in `docs/evidence/2026-10-31-real-warm-e2e-procedure.md`.
//! Hosted CI only proves this suite compiles.
#![cfg(target_os = "linux")]
#![allow(clippy::unwrap_used, clippy::expect_used)]
#[path = "../../sandbox-protocol/tests/support/guest_tls.rs"]
mod tls;
#[path = "support/vm.rs"]
mod vm;
use sandbox_protocol::{
    AllocationId, Id, OperationId, SandboxId,
    allocation_authority::Permit,
    guest_model as gm,
    supervisor::{
        AllocationAuthorityRequest, AllocationState, CommandInspection, CommandRequest,
        CreateRequest, HealthRequest, InspectRequest, Observation, Ownership, Resources,
        StopRequest, WarmAdoptRequest, WarmObservation, WarmPrepareRequest, WarmRetireRequest,
        supervisor_client::SupervisorClient,
    },
    warm_pool::{
        self, CleanupReceipt, HandoffIntent, HandoffOutcome, HandoffReceipt, PrepareIntent,
        PrepareReceipt, PreviousCleanupReceipt, PreviousRetireIntent, RetireIntent, VERSION,
        WarmReceipt,
    },
};
use sandbox_supervisor::{
    guardian::{self, Action, Manifest, State as GuardianState},
    host::{Capacity, Config, Image},
    transport,
};
use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    time::{Duration, Instant},
};
use tonic::{Code, Status, transport::Channel};
use uuid::Uuid;

const VCPU: u32 = 1;
const MEMORY_MIB: u64 = 128;
const DISK_MIB: u64 = 64;

/// One permit-enforcing host whose capacity admits exactly one VM (one vCPU),
/// so a refused second reservation observes a member's capacity hold.
struct Fixture {
    vm: vm::Fixture,
    config: Config,
    config_path: PathBuf,
    tls: tls::Fixture,
    reader: tls::Leaf,
    file_reader: tls::Leaf,
    child: Child,
    url: String,
    address: String,
    image: String,
    template: Uuid,
    policy: Uuid,
    serial: u64,
}

impl Fixture {
    async fn new() -> Self {
        let vm = vm::Fixture::build(60000, true);
        let c = &vm.manifest.config;
        let image = format!("sha256:{}", c.rootfs.sha256);
        let config = Config {
            capabilities: None,
            host: vm.manifest.start.owner.host,
            epoch: 1,
            state_root: vm.temp.path().join("h"),
            cgroup_parent: c.cgroup_parent.clone(),
            launch_permits_required: true,
            guardian_binary: PathBuf::from(env!("CARGO_BIN_EXE_sandbox-supervisor")),
            firecracker: c.firecracker.clone(),
            jailer: c.jailer.clone(),
            images: BTreeMap::from([(
                image.clone(),
                Image {
                    kernel: c.kernel.clone(),
                    rootfs: c.rootfs.clone(),
                },
            )]),
            jail_uid: c.jail_uid,
            jail_gid: c.jail_gid,
            capacity: Capacity {
                vcpu: u64::from(VCPU),
                memory_mib: 512,
                disk_mib: 1024,
            },
        };
        let tls = tls::Fixture::new();
        let reader = tls::Leaf::new(&tls.ca, "reader.sandbox.internal".into(), true);
        let file_reader = tls::Leaf::new(&tls.ca, "file-reader.sandbox.internal".into(), true);
        let server = tls::Leaf::new(&tls.ca, transport::host_server_name(config.host), false);
        for (name, value) in [
            ("ca.pem", tls.ca.pem()),
            ("server.pem", server.cert.pem()),
            ("server.key", server.key.serialize_pem()),
        ] {
            fs::write(vm.temp.path().join(name), value).unwrap();
        }
        let config_path = vm.temp.path().join("host-config.json");
        fs::write(&config_path, serde_json::to_vec(&config).unwrap()).unwrap();
        let address = std::net::TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .to_string();
        let child = Self::spawn(&vm, &config_path, &address, &tls, &reader, &file_reader);
        let f = Self {
            vm,
            config,
            config_path,
            tls,
            reader,
            file_reader,
            child,
            url: format!("https://{address}"),
            address,
            image,
            template: Uuid::now_v7(),
            policy: Uuid::now_v7(),
            serial: 0,
        };
        assert!(
            f.client()
                .await
                .health(HealthRequest {})
                .await
                .unwrap()
                .get_ref()
                .launch_permits_required
        );
        f
    }

    fn spawn(
        vm: &vm::Fixture,
        config: &Path,
        address: &str,
        tls: &tls::Fixture,
        reader: &tls::Leaf,
        file_reader: &tls::Leaf,
    ) -> Child {
        Command::new(env!("CARGO_BIN_EXE_sandbox-host"))
            .arg("--config")
            .arg(config)
            .arg("--listen")
            .arg(address)
            .arg("--ca-cert")
            .arg(vm.temp.path().join("ca.pem"))
            .arg("--server-cert")
            .arg(vm.temp.path().join("server.pem"))
            .arg("--server-key")
            .arg(vm.temp.path().join("server.key"))
            .arg("--controller-cert-sha256")
            .arg(hex::encode(tls.host.pin()))
            .arg("--output-reader-cert-sha256")
            .arg(hex::encode(reader.pin()))
            .arg("--file-reader-cert-sha256")
            .arg(hex::encode(file_reader.pin()))
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(fs::File::create(vm.temp.path().join("host.stderr")).unwrap())
            .spawn()
            .unwrap()
    }

    async fn client(&self) -> SupervisorClient<Channel> {
        let until = Instant::now() + Duration::from_secs(10);
        loop {
            if let Ok(mut c) = transport::connect(
                &self.url,
                self.config.host,
                self.tls.ca.pem().as_bytes(),
                self.tls.host.cert.pem().as_bytes(),
                self.tls.host.key.serialize_pem().as_bytes(),
            )
            .await
                && c.health(HealthRequest {}).await.is_ok()
            {
                return c;
            }
            assert!(Instant::now() < until, "host never healthy");
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }

    /// Kill the host process (member guardians keep running) and start the
    /// same state root at the next epoch.
    async fn restart(&mut self) {
        self.child.kill().unwrap();
        self.child.wait().unwrap();
        assert!(
            sandbox_supervisor::host::Host::open(self.config.clone()).is_err(),
            "same epoch cannot restart"
        );
        self.config.epoch += 1;
        fs::write(&self.config_path, serde_json::to_vec(&self.config).unwrap()).unwrap();
        self.child = Self::spawn(
            &self.vm,
            &self.config_path,
            &self.address,
            &self.tls,
            &self.reader,
            &self.file_reader,
        );
        self.client().await;
    }

    fn journal(&self) -> serde_json::Value {
        guardian::read_json(&self.config.state_root.join("host.json")).unwrap()
    }

    fn member_state(&self, member: Uuid) -> String {
        self.journal()["warm"][member.to_string()]["record"]["state"]["state"]
            .as_str()
            .expect("member record retained")
            .to_owned()
    }

    fn member_boot(&self, member: Uuid) -> String {
        self.journal()["warm"][member.to_string()]["record"]["state"]["boot_id"]
            .as_str()
            .expect("member boot recorded")
            .to_owned()
    }

    /// The inert guardian manifest keyed by the member's host-chosen slot.
    fn member_manifest(&self, member: Uuid) -> Manifest {
        serde_json::from_value(self.journal()["warm"][member.to_string()]["manifest"].clone())
            .unwrap()
    }

    /// The allocation record's guardian view; absent before confirmed binding.
    fn allocation_manifest(&self, o: &Ownership) -> Option<Manifest> {
        let value = self.journal()["records"][&o.allocation_id]["manifest"].clone();
        (!value.is_null()).then(|| serde_json::from_value(value).unwrap())
    }

    fn owner(&self) -> Ownership {
        Ownership {
            host_id: self.config.host.to_string(),
            project_id: self.vm.manifest.start.owner.project.to_string(),
            sandbox_id: SandboxId::generate().to_string(),
            allocation_id: AllocationId::generate().to_string(),
            operation_id: OperationId::generate().to_string(),
            generation: 1,
            supervisor_epoch: self.config.epoch,
            claim_revision: 1,
            claim_expires_unix_ms: guardian::wall_ms() + 120000,
        }
    }

    /// Issue and register the next permit for `o` at the current epoch.
    async fn register(&mut self, c: &mut SupervisorClient<Channel>, o: &Ownership) -> Permit {
        self.serial += 1;
        let permit = Permit {
            host: o.host_id.parse().unwrap(),
            project: o.project_id.parse().unwrap(),
            sandbox: o.sandbox_id.parse().unwrap(),
            allocation: o.allocation_id.parse().unwrap(),
            create_operation: o.operation_id.parse().unwrap(),
            generation: o.generation,
            original_epoch: o.supervisor_epoch,
            serial: self.serial,
        };
        let mut registration = AllocationAuthorityRequest {
            host_id: self.config.host.to_string(),
            reporting_epoch: self.config.epoch,
            permits_json: serde_json::to_vec(std::slice::from_ref(&permit)).unwrap(),
        };
        c.allocation_authority(registration.clone()).await.unwrap();
        registration.permits_json.clear();
        assert_eq!(
            c.allocation_authority(registration)
                .await
                .unwrap()
                .get_ref()
                .registered_through,
            self.serial
        );
        permit
    }

    fn prepare_intent(&self) -> PrepareIntent {
        PrepareIntent {
            version: VERSION,
            member_id: Uuid::now_v7(),
            policy_id: self.policy,
            template_id: self.template,
            host_id: self.config.host,
            supervisor_epoch: self.config.epoch,
            image_digest: self.image.clone(),
            vcpu: VCPU,
            memory_mib: MEMORY_MIB,
            disk_mib: DISK_MIB,
            operation_id: Uuid::now_v7(),
            operation_revision: 1,
        }
    }

    async fn prepare_once(
        c: &mut SupervisorClient<Channel>,
        intent: &PrepareIntent,
    ) -> Result<WarmObservation, Status> {
        c.prepare_warm_member(WarmPrepareRequest {
            intent_json: warm_pool::encode(intent).unwrap(),
        })
        .await
        .map(|r| r.into_inner())
    }

    /// Retry the exact prepare until the authenticated-boot receipt arrives.
    async fn prepare(
        &self,
        c: &mut SupervisorClient<Channel>,
        intent: &PrepareIntent,
    ) -> PrepareReceipt {
        let until = Instant::now() + Duration::from_secs(30);
        loop {
            match Self::prepare_once(c, intent).await {
                Ok(obs) => {
                    assert!(!obs.simulated);
                    let WarmReceipt::Prepared { receipt } =
                        warm_pool::decode(&obs.receipt_json).unwrap()
                    else {
                        panic!("prepare answered a non-prepare receipt")
                    };
                    receipt.verify(intent).unwrap();
                    return receipt;
                }
                Err(e)
                    if matches!(
                        e.code(),
                        Code::Unavailable | Code::Cancelled | Code::DeadlineExceeded
                    ) || (e.code() == Code::ResourceExhausted
                        && e.message().contains("workers busy")) => {}
                Err(e) => panic!("prepare refused: {e}"),
            }
            assert!(Instant::now() < until, "warm member never ready");
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    /// Probe free host capacity with a fresh one-vCPU member. `None` means the
    /// host refused the reservation for capacity; `Some` is a prepared member
    /// the caller must retire.
    async fn capacity_probe(
        &self,
        c: &mut SupervisorClient<Channel>,
    ) -> Option<(PrepareIntent, PrepareReceipt)> {
        let intent = self.prepare_intent();
        loop {
            match Self::prepare_once(c, &intent).await {
                Err(e)
                    if e.code() == Code::ResourceExhausted
                        && e.message().contains("capacity reserved") =>
                {
                    return None;
                }
                Err(e)
                    if e.code() == Code::ResourceExhausted
                        && e.message().contains("workers busy") =>
                {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
                _ => {
                    let receipt = self.prepare(c, &intent).await;
                    return Some((intent, receipt));
                }
            }
        }
    }

    fn handoff(&self, o: &Ownership, prepared: &PrepareReceipt, permit: &Permit) -> HandoffIntent {
        let intent = HandoffIntent {
            version: VERSION,
            member_id: prepared.member_id,
            vm_ref: prepared.vm_ref.clone(),
            template_id: prepared.template_id,
            allocation_id: permit.allocation,
            project_id: permit.project,
            sandbox_id: permit.sandbox,
            create_operation_id: permit.create_operation,
            generation: permit.generation,
            permit_serial: permit.serial,
            host_id: prepared.host_id,
            supervisor_epoch: prepared.supervisor_epoch,
            claim_revision: o.claim_revision,
        };
        assert_eq!(intent.permit(), *permit);
        assert!(intent.admits(permit, o));
        intent
    }

    fn adopt_request(o: &Ownership, intent: &HandoffIntent) -> WarmAdoptRequest {
        WarmAdoptRequest {
            ownership: Some(o.clone()),
            intent_json: warm_pool::encode(intent).unwrap(),
            launch_permit_json: serde_json::to_vec(&intent.permit()).unwrap(),
            allocation_expires_unix_ms: guardian::wall_ms() + 120000,
        }
    }

    fn handoff_receipt(obs: WarmObservation, intent: &HandoffIntent) -> HandoffReceipt {
        assert!(!obs.simulated);
        let WarmReceipt::Handoff { receipt } = warm_pool::decode(&obs.receipt_json).unwrap() else {
            panic!("handoff answered a non-handoff receipt")
        };
        receipt.verify(intent).unwrap();
        receipt
    }

    async fn adopt(
        c: &mut SupervisorClient<Channel>,
        o: &Ownership,
        intent: &HandoffIntent,
    ) -> HandoffReceipt {
        let reply = c
            .adopt_warm_member(Self::adopt_request(o, intent))
            .await
            .unwrap()
            .into_inner();
        Self::handoff_receipt(reply, intent)
    }

    async fn inspect_handoff(
        c: &mut SupervisorClient<Channel>,
        o: &Ownership,
        intent: &HandoffIntent,
    ) -> HandoffReceipt {
        let reply = c
            .inspect_warm_handoff(Self::adopt_request(o, intent))
            .await
            .unwrap()
            .into_inner();
        Self::handoff_receipt(reply, intent)
    }

    fn retire_intent(&self, prepared: &PrepareReceipt, revision: i64) -> RetireIntent {
        RetireIntent {
            version: VERSION,
            member_id: prepared.member_id,
            vm_ref: Some(prepared.vm_ref.clone()),
            host_id: prepared.host_id,
            supervisor_epoch: prepared.supervisor_epoch,
            operation_id: Uuid::now_v7(),
            operation_revision: revision,
        }
    }

    /// Retire until guardian-proven cleanup. While the answer is pending the
    /// member must still hold its capacity.
    async fn retire(
        &self,
        c: &mut SupervisorClient<Channel>,
        intent: &RetireIntent,
    ) -> CleanupReceipt {
        let until = Instant::now() + Duration::from_secs(20);
        loop {
            match c
                .retire_warm_member(WarmRetireRequest {
                    intent_json: warm_pool::encode(intent).unwrap(),
                })
                .await
            {
                Ok(obs) => {
                    let obs = obs.into_inner();
                    assert!(!obs.simulated);
                    let WarmReceipt::Cleanup { receipt } =
                        warm_pool::decode(&obs.receipt_json).unwrap()
                    else {
                        panic!("retire answered a non-cleanup receipt")
                    };
                    receipt
                        .verify(
                            intent.member_id,
                            intent.vm_ref.as_deref(),
                            intent.host_id,
                            intent.supervisor_epoch,
                        )
                        .unwrap();
                    self.assert_member_destroyed(intent.member_id);
                    return receipt;
                }
                Err(e) if e.code() == Code::Unavailable => {
                    assert_ne!(self.member_state(intent.member_id), "retired");
                    assert!(
                        self.capacity_probe(c).await.is_none(),
                        "capacity released before cleanup proof"
                    );
                }
                Err(e) => panic!("retire refused: {e}"),
            }
            assert!(Instant::now() < until, "warm member cleanup never proven");
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    fn assert_member_destroyed(&self, member: Uuid) {
        assert_eq!(self.member_state(member), "retired");
        let m = self.member_manifest(member);
        let r = m.receipt().unwrap();
        assert_eq!(r.state, GuardianState::Stopped);
        assert!(r.cleanup_confirmed);
        assert!(r.binding.is_none(), "an unadopted member was never bound");
        assert!(!m.group().exists());
        assert!(!m.directory().join("run").exists());
    }

    async fn ready(&self, c: &mut SupervisorClient<Channel>, o: &Ownership) -> Observation {
        let until = Instant::now() + Duration::from_secs(20);
        loop {
            if let Ok(r) = c
                .inspect(InspectRequest {
                    ownership: Some(o.clone()),
                })
                .await
            {
                let r = r.into_inner();
                assert_ne!(
                    r.state,
                    AllocationState::Released as i32,
                    "allocation stopped before readiness"
                );
                if r.state == AllocationState::Ready as i32 {
                    assert!(!r.simulated);
                    return r;
                }
            }
            assert!(Instant::now() < until, "allocation never ready through RPC");
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    async fn destroy(&self, c: &mut SupervisorClient<Channel>, o: &Ownership) {
        let m = self.allocation_manifest(o).expect("allocation manifest");
        let mut stop = o.clone();
        stop.operation_id = OperationId::generate().to_string();
        let _ = c
            .stop(StopRequest {
                ownership: Some(stop.clone()),
            })
            .await;
        let until = Instant::now() + Duration::from_secs(15);
        loop {
            if let Ok(r) = c
                .inspect(InspectRequest {
                    ownership: Some(stop.clone()),
                })
                .await
                && r.get_ref().state == AllocationState::Released as i32
            {
                assert!(m.receipt().unwrap().cleanup_confirmed);
                assert!(!m.group().exists());
                assert!(!m.directory().join("run").exists());
                return;
            }
            assert!(Instant::now() < until, "allocation cleanup never confirmed");
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    fn create_request(&self, o: &Ownership, permit: &Permit) -> CreateRequest {
        CreateRequest {
            launch_permit_json: serde_json::to_vec(permit).unwrap(),
            ownership: Some(o.clone()),
            image_digest: self.image.clone(),
            resources: Some(Resources {
                vcpu: VCPU,
                memory_mib: MEMORY_MIB,
                disk_mib: DISK_MIB,
            }),
            allocation_expires_unix_ms: guardian::wall_ms() + 120000,
        }
    }

    /// Run one command through the ordinary allocation command RPC.
    async fn exec(&self, c: &mut SupervisorClient<Channel>, o: &Ownership) {
        let mut owner = o.clone();
        owner.operation_id = OperationId::generate().to_string();
        let command = gm::Execute {
            operation_id: owner.operation_id.parse().unwrap(),
            argv: vec![
                "/bin/busybox".into(),
                "sh".into(),
                "-c".into(),
                "echo adopted; exit 3".into(),
            ],
            env: BTreeMap::new(),
            cwd: "/".into(),
            deadline_unix_ms: guardian::wall_ms() + 15000,
            output_limit: 1024,
        };
        let reply = c
            .execute_command(CommandRequest {
                ownership: Some(owner.clone()),
                command: Some((&command).into()),
            })
            .await
            .unwrap()
            .into_inner();
        assert!(!reply.simulated);
        assert!(!reply.not_started);
        let until = Instant::now() + Duration::from_secs(10);
        loop {
            let r = c
                .inspect_command(CommandInspection {
                    ownership: Some(owner.clone()),
                    command_digest: command.digest().unwrap().to_vec(),
                })
                .await
                .unwrap()
                .into_inner();
            if let Some(receipt) = r.receipt {
                let receipt: gm::Receipt = receipt.try_into().unwrap();
                if receipt.state == gm::State::Exited {
                    assert_eq!(receipt.exit_code, Some(3));
                    assert!(receipt.cleanup_confirmed);
                    return;
                }
            }
            assert!(Instant::now() < until, "command did not finish");
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let Ok(entries) = fs::read_dir(self.config.state_root.join("a")) else {
            return;
        };
        for entry in entries.flatten() {
            if entry.file_type().is_ok_and(|t| t.is_file())
                && matches!(
                    entry.file_name().to_str(),
                    Some("launch.lock" | "launch.required" | "launch.json" | "launch.next")
                )
            {
                continue;
            }
            // Member slot directories hold their inert manifest; adopted
            // members keep the same directory.
            let Ok(m) = guardian::read_json::<Manifest>(&entry.path().join("manifest.json")) else {
                self.vm.temp.disable_cleanup(true);
                return;
            };
            let _ = guardian::control(&m, Action::Stop);
            let until = Instant::now() + Duration::from_secs(8);
            while m.fence_unstarted().is_err() {
                if Instant::now() > until {
                    self.vm.temp.disable_cleanup(true);
                    eprintln!(
                        "warm fixture cleanup unconfirmed; retained {:?}",
                        entry.path()
                    );
                    return;
                }
                std::thread::sleep(Duration::from_millis(20));
            }
        }
    }
}

fn rejected(receipt: &HandoffReceipt) -> &str {
    match &receipt.outcome {
        HandoffOutcome::Rejected { reason } => reason,
        HandoffOutcome::Adopted { .. } => panic!("expected a rejected handoff"),
    }
}

/// Scenarios 1 and 2: prepare answers only after the inert member endpoint
/// (vsock port 53, ALPN `hudson-warm/1`) authenticated the boot; adoption with
/// the registered permit re-keys the same guardian and VM, after which the
/// allocation channel (port 52, ALPN `hudson-guest/1`) answers the same boot,
/// ordinary Inspect reports ready, a command runs and destroy cleans up.
#[tokio::test]
#[ignore = "requires root, HUDSON_GUARDIAN_TEST_VM=1 and native KVM artifacts"]
async fn real_warm_prepare_adopt_exec_and_destroy() {
    assert_eq!(sandbox_protocol::warm_bind::MEMBER_PORT, 53);
    assert_eq!(sandbox_protocol::guest_wire::MEMBER_ALPN, b"hudson-warm/1");
    assert_eq!(sandbox_protocol::guest_wire::ALPN, b"hudson-guest/1");
    let mut f = Fixture::new().await;
    let mut c = f.client().await;
    let intent = f.prepare_intent();
    let prepared = f.prepare(&mut c, &intent).await;
    assert_eq!(f.member_state(intent.member_id), "ready");
    let boot = f.member_boot(intent.member_id);
    let member = f.member_manifest(intent.member_id);
    assert!(member.is_inert());
    assert_eq!(prepared.vm_ref, format!("warm-{}", member.key().uuid()));
    // The member endpoint authenticates the same recorded boot, unbound.
    let hello = guardian::control(&member, Action::MemberHello).unwrap();
    assert!(hello.error.is_none());
    let r = hello.receipt.unwrap();
    assert_eq!(r.state, GuardianState::Running);
    assert!(r.binding.is_none());
    assert_eq!(r.guest_boot_id.as_deref(), Some(boot.as_str()));
    // An exact retry answers the same receipt and launches nothing new.
    assert_eq!(f.prepare(&mut c, &intent).await, prepared);
    assert!(
        f.capacity_probe(&mut c).await.is_none(),
        "member holds capacity"
    );

    let o = f.owner();
    let permit = f.register(&mut c, &o).await;
    let handoff = f.handoff(&o, &prepared, &permit);
    // Unregistered or mismatched permits never adopt.
    let mut wrong = Fixture::adopt_request(&o, &handoff);
    let mut other = permit.clone();
    other.serial += 100;
    wrong.launch_permit_json = serde_json::to_vec(&other).unwrap();
    assert_eq!(
        c.adopt_warm_member(wrong).await.unwrap_err().code(),
        Code::PermissionDenied
    );
    assert!(f.allocation_manifest(&o).is_none());
    let adopted = Fixture::adopt(&mut c, &o, &handoff).await;
    let HandoffOutcome::Adopted { identity_sha256 } = &adopted.outcome else {
        panic!("member not adopted: {adopted:?}")
    };
    assert_eq!(f.member_state(intent.member_id), "adopted");
    let view = f
        .allocation_manifest(&o)
        .expect("allocation gains manifest");
    assert!(!view.is_inert());
    assert_eq!(view.directory(), member.directory(), "same slot and VM");
    assert_eq!(view.group(), member.group());
    let receipt = view.receipt().unwrap();
    let binding = receipt.binding.unwrap();
    assert!(binding.confirmed);
    assert_eq!(&binding.guest_identity_sha256, identity_sha256);
    assert_eq!(binding.intent_sha256, handoff.digest().unwrap());
    // Allocation-channel hello authenticates the same boot.
    let guest = view.guest_client().unwrap();
    assert_eq!(guest.hello().await.unwrap().boot_id, boot);
    f.ready(&mut c, &o).await;
    f.exec(&mut c, &o).await;
    f.destroy(&mut c, &o).await;
    // Capacity moved to the allocation and is released by its cleanup.
    let (probe, probe_receipt) = f
        .capacity_probe(&mut c)
        .await
        .expect("capacity released after allocation cleanup");
    let retire = f.retire_intent(&probe_receipt, probe.operation_revision);
    f.retire(&mut c, &retire).await;
}

/// Scenario 3: a lost Adopt reply is recovered by InspectWarmHandoff from the
/// persisted outcome. Inspect never adopts: on an unadopted member it fences
/// the scope, and a late Adopt for that scope is refused.
#[tokio::test]
#[ignore = "requires root, HUDSON_GUARDIAN_TEST_VM=1 and native KVM artifacts"]
async fn real_warm_lost_adopt_reply_and_inspect_fence() {
    let mut f = Fixture::new().await;
    let mut c = f.client().await;

    // Lost reply: the answer is discarded by the caller; only its digest-bound
    // content is kept to compare against reconciliation.
    let first = f.prepare_intent();
    let prepared = f.prepare(&mut c, &first).await;
    let o = f.owner();
    let permit = f.register(&mut c, &o).await;
    let handoff = f.handoff(&o, &prepared, &permit);
    let lost = Fixture::adopt(&mut c, &o, &handoff).await;
    assert!(matches!(lost.outcome, HandoffOutcome::Adopted { .. }));
    let revision = f.journal()["warm"][first.member_id.to_string()]["record"]["revision"].clone();
    assert_eq!(Fixture::inspect_handoff(&mut c, &o, &handoff).await, lost);
    // Inspect of a recorded outcome is read-only; Adopt retry is idempotent.
    assert_eq!(
        f.journal()["warm"][first.member_id.to_string()]["record"]["revision"],
        revision
    );
    assert_eq!(Fixture::adopt(&mut c, &o, &handoff).await, lost);
    f.ready(&mut c, &o).await;
    f.destroy(&mut c, &o).await;

    // Inspect before adoption fences instead of adopting.
    let second = f.prepare_intent();
    let prepared = f.prepare(&mut c, &second).await;
    let member = f.member_manifest(second.member_id);
    let o = f.owner();
    let permit = f.register(&mut c, &o).await;
    let handoff = f.handoff(&o, &prepared, &permit);
    let fenced = Fixture::inspect_handoff(&mut c, &o, &handoff).await;
    assert_eq!(rejected(&fenced), "fenced_by_inspect");
    assert_eq!(f.member_state(second.member_id), "fenced");
    assert!(f.allocation_manifest(&o).is_none(), "inspect never adopts");
    let unbound = guardian::control(&member, Action::Inspect)
        .unwrap()
        .receipt
        .unwrap();
    assert_eq!(unbound.state, GuardianState::Running);
    assert!(unbound.binding.is_none());
    if let Ok(r) = c
        .inspect(InspectRequest {
            ownership: Some(o.clone()),
        })
        .await
    {
        assert_ne!(r.get_ref().state, AllocationState::Ready as i32);
    }
    // The late Adopt answers the recorded refusal and binds nothing.
    let late = Fixture::adopt(&mut c, &o, &handoff).await;
    assert_eq!(late, fenced);
    assert!(f.allocation_manifest(&o).is_none());
    assert!(
        guardian::control(&member, Action::Inspect)
            .unwrap()
            .receipt
            .unwrap()
            .binding
            .is_none()
    );
    // Another registered scope cannot take the fenced member either.
    let other = f.owner();
    let other_permit = f.register(&mut c, &other).await;
    let other_handoff = f.handoff(&other, &prepared, &other_permit);
    assert_eq!(
        c.adopt_warm_member(Fixture::adopt_request(&other, &other_handoff))
            .await
            .unwrap_err()
            .code(),
        Code::AlreadyExists
    );
    // The fenced member is never repooled; it is destroyed.
    let retire = f.retire_intent(&prepared, 1);
    f.retire(&mut c, &retire).await;
}

/// Scenario 4: retiring an unadopted member answers only with guardian-proven
/// cleanup and keeps its capacity reserved until then.
#[tokio::test]
#[ignore = "requires root, HUDSON_GUARDIAN_TEST_VM=1 and native KVM artifacts"]
async fn real_warm_retire_holds_capacity_until_cleanup_proof() {
    let mut f = Fixture::new().await;
    let mut c = f.client().await;
    let intent = f.prepare_intent();
    let prepared = f.prepare(&mut c, &intent).await;
    assert!(
        f.capacity_probe(&mut c).await.is_none(),
        "member holds capacity"
    );
    let retire = f.retire_intent(&prepared, 1);
    let cleanup = f.retire(&mut c, &retire).await;
    assert!(cleanup.destroyed);
    assert_eq!(cleanup.vm_ref.as_deref(), Some(prepared.vm_ref.as_str()));
    // A retry answers the same proof.
    assert_eq!(f.retire(&mut c, &retire).await, cleanup);
    // Adoption after retirement is a definitive rejection, never a VM.
    let o = f.owner();
    let permit = f.register(&mut c, &o).await;
    let handoff = f.handoff(&o, &prepared, &permit);
    assert_eq!(
        rejected(&Fixture::adopt(&mut c, &o, &handoff).await),
        "member_retired"
    );
    assert!(f.allocation_manifest(&o).is_none());
    let (probe, probe_receipt) = f
        .capacity_probe(&mut c)
        .await
        .expect("capacity released by cleanup proof");
    let retire = f.retire_intent(&probe_receipt, probe.operation_revision);
    f.retire(&mut c, &retire).await;
}

/// Scenario 5: after a supervisor restart at a new epoch, a member retained
/// from the previous epoch is cleaned through the previous-epoch retire
/// envelope. Capacity stays held until guardian evidence verifies cleanup.
#[tokio::test]
#[ignore = "requires root, HUDSON_GUARDIAN_TEST_VM=1 and native KVM artifacts"]
async fn real_warm_previous_epoch_member_cleanup_after_restart() {
    let mut f = Fixture::new().await;
    let mut c = f.client().await;
    let intent = f.prepare_intent();
    let prepared = f.prepare(&mut c, &intent).await;
    let member = f.member_manifest(intent.member_id);
    assert_eq!(prepared.supervisor_epoch, 1);
    f.restart().await;
    assert_eq!(f.config.epoch, 2);
    let mut c = f.client().await;
    // Ordinary retirement cannot acknowledge a previous incarnation.
    let original = f.retire_intent(&prepared, 1);
    assert_eq!(
        c.retire_warm_member(WarmRetireRequest {
            intent_json: warm_pool::encode(&original).unwrap(),
        })
        .await
        .unwrap_err()
        .code(),
        Code::FailedPrecondition
    );
    let previous = PreviousRetireIntent {
        version: VERSION,
        original,
        reporting_epoch: f.config.epoch,
    };
    let until = Instant::now() + Duration::from_secs(30);
    let receipt: PreviousCleanupReceipt = loop {
        // Maintenance may also retire the member locally; either way a held
        // reservation must refuse the probe until cleanup is recorded.
        let held_before = f.member_state(intent.member_id) != "retired";
        let probe = f.capacity_probe(&mut c).await;
        let held_after = f.member_state(intent.member_id) != "retired";
        if let Some((probe, probe_receipt)) = probe {
            assert!(
                !held_before || !held_after,
                "capacity released before previous-epoch cleanup proof"
            );
            let retire = f.retire_intent(&probe_receipt, probe.operation_revision);
            f.retire(&mut c, &retire).await;
        }
        match c
            .retire_warm_member(WarmRetireRequest {
                intent_json: warm_pool::encode(&previous).unwrap(),
            })
            .await
        {
            Ok(obs) => {
                let obs = obs.into_inner();
                assert!(!obs.simulated);
                let WarmReceipt::PreviousCleanup { receipt } =
                    warm_pool::decode(&obs.receipt_json).unwrap()
                else {
                    panic!("previous retire answered another receipt")
                };
                break receipt;
            }
            Err(e) if e.code() == Code::Unavailable => {}
            Err(e) => panic!("previous-epoch retire refused: {e}"),
        }
        assert!(
            Instant::now() < until,
            "previous-epoch cleanup never proven"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    receipt.verify(&previous).unwrap();
    assert_eq!(receipt.cleanup.supervisor_epoch, 1);
    f.assert_member_destroyed(intent.member_id);
    assert_eq!(
        f.journal()["warm"][intent.member_id.to_string()]["previous_retire"],
        serde_json::to_value(&previous).unwrap()
    );
    assert!(!member.group().exists());
    let (probe, probe_receipt) = f
        .capacity_probe(&mut c)
        .await
        .expect("capacity released after verified cleanup");
    assert_eq!(probe_receipt.supervisor_epoch, 2);
    let retire = f.retire_intent(&probe_receipt, probe.operation_revision);
    f.retire(&mut c, &retire).await;
}

/// Scenario 6: cold create-to-ready versus warm adopt-to-ready. Prints
/// measurements only; latency is environment-dependent and never asserted.
#[tokio::test]
#[ignore = "requires root, HUDSON_GUARDIAN_TEST_VM=1 and native KVM artifacts"]
async fn real_cold_versus_warm_time_to_ready() {
    const SAMPLES: usize = 3;
    let mut f = Fixture::new().await;
    let mut c = f.client().await;
    let mut cold = Vec::new();
    let mut prepare = Vec::new();
    let mut warm = Vec::new();
    for _ in 0..SAMPLES {
        let o = f.owner();
        let permit = f.register(&mut c, &o).await;
        let request = f.create_request(&o, &permit);
        let start = Instant::now();
        // A timed-out Create remains unknown until exact-owner inspection.
        let _ = c.create(request).await;
        f.ready(&mut c, &o).await;
        cold.push(start.elapsed());
        f.destroy(&mut c, &o).await;

        let intent = f.prepare_intent();
        let start = Instant::now();
        let prepared = f.prepare(&mut c, &intent).await;
        prepare.push(start.elapsed());
        let o = f.owner();
        let permit = f.register(&mut c, &o).await;
        let handoff = f.handoff(&o, &prepared, &permit);
        let start = Instant::now();
        let adopted = Fixture::adopt(&mut c, &o, &handoff).await;
        assert!(matches!(adopted.outcome, HandoffOutcome::Adopted { .. }));
        f.ready(&mut c, &o).await;
        warm.push(start.elapsed());
        f.destroy(&mut c, &o).await;
    }
    let ms = |d: &[Duration]| {
        d.iter()
            .map(|d| format!("{:.1}", d.as_secs_f64() * 1000.0))
            .collect::<Vec<_>>()
            .join(", ")
    };
    println!("warm_e2e cold create-to-ready ms: [{}]", ms(&cold));
    println!(
        "warm_e2e warm prepare-to-receipt ms (off the request path): [{}]",
        ms(&prepare)
    );
    println!("warm_e2e warm adopt-to-ready ms: [{}]", ms(&warm));
}
