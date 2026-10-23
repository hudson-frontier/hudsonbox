//! Real mTLS lifecycle RPCs against a guardian and bootstrapped Firecracker guest.
#![cfg(target_os = "linux")]
#![allow(clippy::unwrap_used, clippy::expect_used)]
#[path = "../../sandbox-protocol/tests/support/guest_tls.rs"]
mod tls;
#[path = "support/vm.rs"]
mod vm;
use sandbox_protocol::{
    AllocationId, Id, OperationId, SandboxId,
    supervisor::{
        AllocationState, CreateRequest, HealthRequest, InspectRequest, LeaseInspection,
        LeaseOwnership, LeaseRequest, Ownership, Resources, StopRequest,
        supervisor_client::SupervisorClient,
    },
};
use sandbox_supervisor::{
    guardian::{self, Action, Manifest},
    host::{Capacity, Config, Image},
    transport,
};
use std::{
    collections::BTreeMap,
    fs,
    path::PathBuf,
    process::{Child, Command, Stdio},
    time::{Duration, Instant},
};
use tonic::{Code, transport::Channel};

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
}
impl Fixture {
    async fn new() -> Self {
        Self::with_agent(true).await
    }
    async fn with_agent(agent: bool) -> Self {
        Self::configured(agent, false).await
    }
    async fn configured(agent: bool, output: bool) -> Self {
        Self::configured_permits(agent, output, false).await
    }
    async fn configured_permits(agent: bool, output: bool, permits: bool) -> Self {
        let vm = vm::Fixture::build(60000, agent);
        let c = &vm.manifest.config;
        let config = Config {
            capabilities: None,
            host: vm.manifest.start.owner.host,
            epoch: 1,
            state_root: vm.temp.path().join("h"),
            cgroup_parent: c.cgroup_parent.clone(),
            launch_permits_required: permits,
            guardian_binary: PathBuf::from(env!("CARGO_BIN_EXE_sandbox-supervisor")),
            firecracker: c.firecracker.clone(),
            jailer: c.jailer.clone(),
            images: BTreeMap::from([(
                format!("sha256:{}", c.rootfs.sha256),
                Image {
                    kernel: c.kernel.clone(),
                    rootfs: c.rootfs.clone(),
                },
            )]),
            jail_uid: c.jail_uid,
            jail_gid: c.jail_gid,
            capacity: Capacity {
                vcpu: 2,
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
        if output {
            use std::os::unix::fs::PermissionsExt;
            let path = vm.temp.path().join("output.json");
            fs::write(
                &path,
                serde_json::to_vec(&serde_json::json!({
                    "endpoint":std::env::var("HUDSON_TEST_S3_ENDPOINT").unwrap(),
                    "region":"us-east-1",
                    "bucket":std::env::var("HUDSON_TEST_S3_BUCKET").unwrap(),
                    "access_key":std::env::var("HUDSON_TEST_S3_ACCESS_KEY").unwrap(),
                    "secret_key":std::env::var("HUDSON_TEST_S3_SECRET_KEY").unwrap(),
                    "allow_loopback_http":true
                }))
                .unwrap(),
            )
            .unwrap();
            fs::set_permissions(path, fs::Permissions::from_mode(0o600)).unwrap();
        }
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
        };
        f.client().await;
        f
    }
    fn spawn(
        vm: &vm::Fixture,
        config: &PathBuf,
        address: &str,
        tls: &tls::Fixture,
        reader: &tls::Leaf,
        file_reader: &tls::Leaf,
    ) -> Child {
        let mut command = Command::new(env!("CARGO_BIN_EXE_sandbox-host"));
        let output = vm.temp.path().join("output.json");
        if output.exists() {
            command.arg("--output-config").arg(output);
        }
        command
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
    fn request(&self) -> CreateRequest {
        let owner = &self.vm.manifest.start.owner;
        CreateRequest {
            launch_permit_json: Vec::new(),
            ownership: Some(Ownership {
                host_id: self.config.host.to_string(),
                project_id: owner.project.to_string(),
                sandbox_id: SandboxId::generate().to_string(),
                allocation_id: AllocationId::generate().to_string(),
                operation_id: OperationId::generate().to_string(),
                generation: 1,
                supervisor_epoch: self.config.epoch,
                claim_revision: 1,
                claim_expires_unix_ms: guardian::wall_ms() + 120000,
            }),
            image_digest: self.config.images.keys().next().unwrap().clone(),
            resources: Some(Resources {
                vcpu: 1,
                memory_mib: 128,
                disk_mib: 64,
            }),
            allocation_expires_unix_ms: guardian::wall_ms() + 60000,
        }
    }
    fn manifest(&self, o: &Ownership) -> Manifest {
        guardian::read_json(
            &self
                .config
                .state_root
                .join("a")
                .join(&o.allocation_id)
                .join("manifest.json"),
        )
        .unwrap()
    }
    async fn ready(
        &self,
        c: &mut SupervisorClient<Channel>,
        o: &Ownership,
    ) -> sandbox_protocol::supervisor::Observation {
        let until = Instant::now() + Duration::from_secs(15);
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
            assert!(Instant::now() < until, "guest never ready through RPC");
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
    async fn released(
        &self,
        c: &mut SupervisorClient<Channel>,
        o: &Ownership,
    ) -> sandbox_protocol::supervisor::Observation {
        let until = Instant::now() + Duration::from_secs(10);
        loop {
            if let Ok(r) = c
                .inspect(InspectRequest {
                    ownership: Some(o.clone()),
                })
                .await
                && r.get_ref().state == AllocationState::Released as i32
            {
                let m = self.manifest(o);
                assert!(!m.group().exists());
                assert!(!m.directory().join("run").exists());
                assert!(m.receipt().unwrap().cleanup_confirmed);
                return r.into_inner();
            }
            assert!(
                Instant::now() < until,
                "cleanup never confirmed through RPC"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
    async fn restart(&mut self) {
        self.restart_with(|_| {}).await;
    }
    async fn restart_with(&mut self, edit: impl FnOnce(&Self)) {
        self.child.kill().unwrap();
        self.child.wait().unwrap();
        edit(self);
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
}
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let root = self.config.state_root.join("a");
        let authority_valid = !self.config.launch_permits_required
            || sandbox_supervisor::launch_authority::AuthorityFile::open(
                root.clone(),
                self.config.host,
                self.config.epoch,
                0,
            )
            .is_ok();
        if !authority_valid {
            self.vm.temp.disable_cleanup(true);
            return;
        }
        if let Ok(entries) = fs::read_dir(root) {
            for entry in entries.flatten() {
                // These validated root files are not allocation directories.
                // Treating launch.lock as a missing manifest retained every
                // registered fixture, eventually filling the controlled VM.
                if self.config.launch_permits_required
                    && entry.file_type().is_ok_and(|t| t.is_file())
                    && matches!(
                        entry.file_name().to_str(),
                        Some("launch.lock" | "launch.required" | "launch.json" | "launch.next")
                    )
                {
                    continue;
                }
                if let Ok(m) = guardian::read_json::<Manifest>(&entry.path().join("manifest.json"))
                {
                    let _ = guardian::control(&m, Action::Stop);
                    let until = Instant::now() + Duration::from_secs(8);
                    while m.fence_unstarted().is_err() {
                        if Instant::now() > until {
                            self.vm.temp.disable_cleanup(true);
                            return;
                        }
                        std::thread::sleep(Duration::from_millis(20));
                    }
                } else {
                    self.vm.temp.disable_cleanup(true);
                    return;
                }
            }
        }
    }
}
fn lease(o: &Ownership, revision: i64) -> LeaseOwnership {
    LeaseOwnership {
        host_id: o.host_id.clone(),
        project_id: o.project_id.clone(),
        sandbox_id: o.sandbox_id.clone(),
        allocation_id: o.allocation_id.clone(),
        generation: o.generation,
        supervisor_epoch: o.supervisor_epoch,
        revision,
        claim_expires_unix_ms: guardian::wall_ms() + 60000,
    }
}
#[tokio::test]
#[ignore = "requires root, HUDSON_GUARDIAN_TEST_VM=1 and native KVM artifacts"]
async fn real_rpc_duplicate_create_lease_stop_and_fences() {
    let f = Fixture::new().await;
    let mut c = f.client().await;
    let req = f.request();
    let o = req.ownership.as_ref().unwrap().clone();
    let mut concurrent = c.clone();
    let (a, b) = tokio::join!(concurrent.create(req.clone()), c.create(req.clone()));
    for reply in [a, b].into_iter().flatten() {
        assert!(!reply.get_ref().simulated);
    }
    let ready = f.ready(&mut c, &o).await;
    assert_eq!(ready.start_count, 1);
    assert_eq!(ready.create_operation_id, o.operation_id);
    let m = f.manifest(&o);
    let before = m.receipt().unwrap();
    assert!(before.guest_boot_id.is_some());
    // Readiness comes from a functioning authenticated guest, not process presence.
    let guest = m.guest_client().unwrap();
    assert_eq!(
        guest.hello().await.unwrap().boot_id,
        before.guest_boot_id.unwrap()
    );
    let mut changed = req.clone();
    changed.resources.as_mut().unwrap().vcpu = 2;
    assert_eq!(
        c.create(changed).await.unwrap_err().code(),
        Code::AlreadyExists
    );
    let owner = lease(&o, 1);
    let expiry = guardian::wall_ms() + 90000;
    let renewed = c
        .renew_lease(LeaseRequest {
            ownership: Some(owner.clone()),
            allocation_expires_unix_ms: expiry,
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(renewed.allocation_expires_unix_ms, expiry);
    assert!(!renewed.simulated);
    assert_eq!(
        c.renew_lease(LeaseRequest {
            ownership: Some(owner.clone()),
            allocation_expires_unix_ms: expiry + 1
        })
        .await
        .unwrap_err()
        .code(),
        Code::AlreadyExists
    );
    assert_eq!(
        c.inspect_lease(LeaseInspection {
            ownership: Some(owner)
        })
        .await
        .unwrap()
        .get_ref()
        .allocation_expires_unix_ms,
        expiry
    );
    let mut inspect = o.clone();
    inspect.claim_revision = 2;
    c.inspect(InspectRequest {
        ownership: Some(inspect.clone()),
    })
    .await
    .unwrap();
    assert_eq!(
        c.create(req.clone()).await.unwrap_err().code(),
        Code::FailedPrecondition
    );
    let _ = c
        .stop(StopRequest {
            ownership: Some(inspect.clone()),
        })
        .await;
    f.released(&mut c, &inspect).await;
    let mut retry = req;
    retry.ownership = Some(inspect);
    assert_eq!(
        c.create(retry).await.unwrap().get_ref().state,
        AllocationState::Released as i32
    );
    let absent = f.request();
    let absent_owner = absent.ownership.clone();
    let unknown_id: AllocationId = absent_owner
        .as_ref()
        .unwrap()
        .allocation_id
        .parse()
        .unwrap();
    let unknown_group = f.config.cgroup_parent.join(unknown_id.uuid().to_string());
    fs::create_dir(&unknown_group).unwrap();
    let refused = c
        .stop(StopRequest {
            ownership: absent_owner.clone(),
        })
        .await;
    fs::remove_dir(&unknown_group).unwrap();
    assert_eq!(
        refused.unwrap_err().code(),
        Code::Unavailable,
        "unknown cgroup cannot supply fenced absence"
    );
    assert_eq!(
        c.stop(StopRequest {
            ownership: absent_owner.clone()
        })
        .await
        .unwrap()
        .get_ref()
        .state,
        AllocationState::FencedAbsent as i32
    );
    assert_eq!(
        c.create(absent).await.unwrap_err().code(),
        Code::FailedPrecondition
    );
    assert_eq!(
        c.inspect(InspectRequest {
            ownership: absent_owner
        })
        .await
        .unwrap()
        .get_ref()
        .state,
        AllocationState::FencedAbsent as i32
    );
}
#[tokio::test]
#[ignore = "requires root, HUDSON_GUARDIAN_TEST_VM=1 and native KVM artifacts"]
async fn restart_advances_epoch_stops_old_owner_and_preserves_capacity() {
    let mut f = Fixture::new().await;
    let mut c = f.client().await;
    let req = f.request();
    let o = req.ownership.as_ref().unwrap().clone();
    let _ = c.create(req.clone()).await;
    f.ready(&mut c, &o).await;
    let m = f.manifest(&o);
    assert!(
        sandbox_supervisor::host::Host::open(f.config.clone()).is_err(),
        "duplicate service must not own journal"
    );
    let mut command_owner = o.clone();
    command_owner.operation_id = OperationId::generate().to_string();
    let command = sandbox_protocol::guest_model::Execute {
        operation_id: command_owner.operation_id.parse().unwrap(),
        argv: vec!["/bin/busybox".into(), "sleep".into(), "20".into()],
        env: BTreeMap::new(),
        cwd: "/".into(),
        deadline_unix_ms: guardian::wall_ms() + 25000,
        output_limit: 1024,
    };
    let command_request = sandbox_protocol::supervisor::CommandRequest {
        ownership: Some(command_owner.clone()),
        command: Some((&command).into()),
    };
    let command_reply = c
        .execute_command(command_request.clone())
        .await
        .unwrap()
        .into_inner();
    assert!(!command_reply.not_started);
    assert_eq!(
        command_reply.receipt.unwrap().state,
        sandbox_protocol::guest::State::LaunchIntent as i32
    );
    f.restart().await;
    let mut c = f.client().await;
    assert_eq!(
        c.execute_command(command_request).await.unwrap_err().code(),
        Code::FailedPrecondition
    );
    let retained: serde_json::Value =
        guardian::read_json(&f.config.state_root.join("host.json")).unwrap();
    let command_record =
        &retained["records"][&o.allocation_id]["commands"][&command_owner.operation_id];
    assert_eq!(
        command_record["digest"],
        serde_json::json!(command.digest().unwrap())
    );
    assert_eq!(command_record["not_started"], false);
    assert_eq!(command_record["receipt"]["state"], "launch_intent");
    assert_eq!(
        c.create(req.clone()).await.unwrap_err().code(),
        Code::FailedPrecondition
    );
    let mut new_epoch = req;
    new_epoch.ownership.as_mut().unwrap().supervisor_epoch = f.config.epoch;
    assert_eq!(
        c.create(new_epoch).await.unwrap_err().code(),
        Code::FailedPrecondition
    );
    let until = Instant::now() + Duration::from_secs(10);
    while !m.receipt().unwrap().cleanup_confirmed {
        assert!(Instant::now() < until);
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(!m.group().exists());
    assert!(!m.directory().join("run").exists());
    let next = f.request();
    let _ = c.create(next.clone()).await;
    f.ready(&mut c, next.ownership.as_ref().unwrap()).await;
}
#[tokio::test]
#[ignore = "requires root, HUDSON_GUARDIAN_TEST_VM=1 and native KVM artifacts"]
async fn lost_create_caller_reconciles_one_vm_and_rejects_other_controller() {
    let f = Fixture::new().await;
    let mut c = f.client().await;
    let request = f.request();
    let o = request.ownership.as_ref().unwrap().clone();
    let mut caller = c.clone();
    let call = tokio::spawn(async move { caller.create(request).await });
    let until = Instant::now() + Duration::from_secs(5);
    while !f
        .config
        .state_root
        .join("a")
        .join(&o.allocation_id)
        .join("receipt.json")
        .exists()
    {
        assert!(Instant::now() < until);
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    call.abort();
    let _ = call.await;
    assert_eq!(f.ready(&mut c, &o).await.start_count, 1);
    let rogue = tls::Leaf::new(&f.tls.ca, "host.sandbox.internal".into(), true);
    let mut rogue = transport::connect(
        &f.url,
        f.config.host,
        f.tls.ca.pem().as_bytes(),
        rogue.cert.pem().as_bytes(),
        rogue.key.serialize_pem().as_bytes(),
    )
    .await
    .unwrap();
    assert_eq!(
        rogue.health(HealthRequest {}).await.unwrap_err().code(),
        Code::PermissionDenied
    );
}

#[tokio::test]
#[ignore = "requires root, HUDSON_GUARDIAN_TEST_VM=1 and native KVM artifacts"]
async fn capacity_remains_reserved_until_real_cleanup() {
    let f = Fixture::new().await;
    let mut c = f.client().await;
    let first = f.request();
    let second = f.request();
    let third = f.request();
    for r in [&first, &second] {
        let _ = c.create(r.clone()).await;
        f.ready(&mut c, r.ownership.as_ref().unwrap()).await;
    }
    assert_eq!(
        c.create(third.clone()).await.unwrap_err().code(),
        Code::ResourceExhausted
    );
    let o = first.ownership.unwrap();
    let _ = c
        .stop(StopRequest {
            ownership: Some(o.clone()),
        })
        .await;
    f.released(&mut c, &o).await;
    let _ = c.create(third.clone()).await;
    f.ready(&mut c, third.ownership.as_ref().unwrap()).await;
    assert!(
        f.manifest(second.ownership.as_ref().unwrap())
            .group()
            .exists()
    );
}
#[tokio::test]
#[ignore = "requires root, HUDSON_GUARDIAN_TEST_VM=1 and native KVM artifacts"]
async fn running_vmm_without_guest_handshake_never_becomes_ready() {
    let f = Fixture::with_agent(false).await;
    let mut c = f.client().await;
    let mut r = f.request();
    r.allocation_expires_unix_ms = guardian::wall_ms() + 12000;
    let o = r.ownership.as_ref().unwrap().clone();
    let result = c.create(r).await;
    if let Ok(reply) = result {
        assert_ne!(reply.get_ref().state, AllocationState::Ready as i32);
    }
    let until = Instant::now() + Duration::from_secs(5);
    while Instant::now() < until {
        if let Ok(reply) = c
            .inspect(InspectRequest {
                ownership: Some(o.clone()),
            })
            .await
        {
            assert_ne!(reply.get_ref().state, AllocationState::Ready as i32);
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let _ = c
        .stop(StopRequest {
            ownership: Some(o.clone()),
        })
        .await;
    f.released(&mut c, &o).await;
}

async fn http(
    app: &axum::Router,
    token: &str,
    method: &str,
    path: &str,
    key: &str,
    body: serde_json::Value,
) -> (http::StatusCode, serde_json::Value) {
    use tower::ServiceExt;
    let request = http::Request::builder()
        .method(method)
        .uri(path)
        .header("authorization", format!("Bearer {token}"))
        .header("content-type", "application/json")
        .header("idempotency-key", key)
        .body(axum::body::Body::from(body.to_string()))
        .unwrap();
    let response = app.clone().oneshot(request).await.unwrap();
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), 65536)
        .await
        .unwrap();
    (status, serde_json::from_slice(&bytes).unwrap())
}
#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
#[ignore = "requires root, HUDSON_GUARDIAN_TEST_VM=1, local PostgreSQL and native KVM artifacts"]
async fn authenticated_api_controller_creates_renews_and_destroys_real_vm(pool: sqlx::PgPool) {
    api_lifecycle(pool, false, false).await;
}
#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
#[ignore = "requires root, HUDSON_GUARDIAN_TEST_VM=1, local PostgreSQL and native KVM artifacts"]
async fn authenticated_controller_automatically_retires_real_allocation(pool: sqlx::PgPool) {
    api_lifecycle(pool, false, true).await;
}
#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
#[ignore = "requires root, HUDSON_GUARDIAN_TEST_VM=1 and a sustained native KVM run"]
async fn sustained_real_allocation_reuse_keeps_an_older_vm_live(pool: sqlx::PgPool) {
    use sandbox_protocol::{ProjectId, ProjectToken};
    use serde_json::json;
    let mut f = Fixture::configured_permits(true, false, true).await;
    let project = ProjectId::generate();
    let token = ProjectToken::generate().unwrap();
    sqlx::query("INSERT INTO projects(id,name,status,limits,api_tokens) VALUES($1,'sustained-reuse','active','{}',$2)")
        .bind(project.uuid()).bind(json!([{"key_id":token.key_id().as_str(),"hash":hex::encode(token.hash().as_bytes())}]))
        .execute(&pool).await.unwrap();
    sqlx::query("INSERT INTO hosts(id,status,cpu_capacity,memory_capacity_mib,disk_capacity_mib,supervisor_epoch) VALUES($1,'ready',2,256,128,1)")
        .bind(f.config.host.uuid()).execute(&pool).await.unwrap();
    let store = sandbox_store::Store::from_pool(pool.clone());
    let image = f.config.images.keys().next().unwrap().clone();
    let app = sandbox_api::router(sandbox_api::AppState {
        store: store.clone(),
        images: sandbox_protocol::images::ImageAllowlist::new([image.clone()]).unwrap(),
    });
    let mut controller = sandbox_controller::Controller::connect(
        store.clone(),
        sandbox_controller::ControllerConfig {
            endpoint: f.url.clone(),
            host: f.config.host,
            epoch: 1,
            allowed_images: std::collections::BTreeSet::from([image.clone()]),
            allow_simulated: false,
        },
        f.tls.ca.pem().as_bytes(),
        f.tls.host.cert.pem().as_bytes(),
        f.tls.host.key.serialize_pem().as_bytes(),
    )
    .await
    .unwrap();
    let token = token.render_once();
    let resources =
        json!({"image_digest":image,"resources":{"vcpu":1,"memory_mib":128,"disk_mib":64}});
    let keeper_key = OperationId::generate().to_string();
    let (_, keeper) = http(
        &app,
        &token,
        "POST",
        "/v1/sandboxes",
        &keeper_key,
        resources.clone(),
    )
    .await;
    let keeper_sandbox = keeper["sandbox_id"].as_str().unwrap().to_string();
    let keeper_operation = keeper["operation_id"].as_str().unwrap().to_string();
    wait_operation_preserving(
        &app,
        &token,
        &mut controller,
        &keeper_operation,
        "succeeded",
        &mut f,
    )
    .await;
    let live: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM allocations WHERE sandbox_id=$1 AND status='running')",
    )
    .bind(keeper_sandbox.parse::<SandboxId>().unwrap().uuid())
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(live);
    let cycles = std::env::var("HUDSON_SUSTAINED_REUSE_CYCLES")
        .ok()
        .map(|v| v.parse().unwrap())
        .unwrap_or(1025usize);
    assert!(cycles >= 2);
    if cycles <= 1024 {
        eprintln!("sustained_reuse_calibration_cycles={cycles}; full acceptance requires 1025");
    }
    let mut history = controller.history_retirer();
    let mut retire = controller.allocation_retirer();
    for cycle in 0..cycles {
        let key = OperationId::generate().to_string();
        let (_, admitted) = http(
            &app,
            &token,
            "POST",
            "/v1/sandboxes",
            &key,
            resources.clone(),
        )
        .await;
        let sandbox = admitted["sandbox_id"].as_str().unwrap().to_string();
        let create_op = admitted["operation_id"].as_str().unwrap().to_string();
        wait_operation_preserving(
            &app,
            &token,
            &mut controller,
            &create_op,
            "succeeded",
            &mut f,
        )
        .await;
        let execute_key = OperationId::generate().to_string();
        let (_, execute) = http(&app, &token, "POST", &format!("/v1/sandboxes/{sandbox}/execute"), &execute_key,
            json!({"argv":["/bin/busybox","true"],"deadline_unix_ms":guardian::wall_ms()+10000,"output_limit":1024})).await;
        wait_operation_preserving(
            &app,
            &token,
            &mut controller,
            execute["operation_id"].as_str().unwrap(),
            "succeeded",
            &mut f,
        )
        .await;
        let destroy_key = OperationId::generate().to_string();
        let (_, destroy) = http(
            &app,
            &token,
            "POST",
            &format!("/v1/sandboxes/{sandbox}/destroy"),
            &destroy_key,
            json!({}),
        )
        .await;
        wait_operation_preserving(
            &app,
            &token,
            &mut controller,
            destroy["operation_id"].as_str().unwrap(),
            "succeeded",
            &mut f,
        )
        .await;
        let allocation: sqlx::types::Uuid = sqlx::query_scalar(
            "SELECT id FROM allocations WHERE sandbox_id=$1 AND status='released'",
        )
        .bind(sandbox.parse::<SandboxId>().unwrap().uuid())
        .fetch_one(&pool)
        .await
        .unwrap();
        sqlx::query("UPDATE operations SET response_expires_at=clock_timestamp()-interval '1 second' WHERE sandbox_id=$1 AND kind='execute' AND payload_compacted_at IS NULL")
            .bind(sandbox.parse::<SandboxId>().unwrap().uuid())
            .execute(&pool)
            .await
            .unwrap();
        loop {
            match store.compact_expired_response().await.unwrap() {
                sandbox_store::compaction::Compaction::Completed(_) => {}
                sandbox_store::compaction::Compaction::Deferred(_) => {}
                sandbox_store::compaction::Compaction::Idle => break,
            }
        }
        // A real forgetting claim is valid for 120 seconds. A guardian can still
        // be releasing its lifecycle lock after destroy has recorded a durable
        // release, so allow the bounded reconciler to use that protocol window.
        let until = Instant::now() + Duration::from_secs(110);
        let mut last_retirement_rpc = None;
        loop {
            for _ in 0..4 {
                match history.tick().await {
                    Ok(_) | Err(sandbox_controller::history::HistoryError::Rpc) => {}
                    Err(error) => panic!("cycle {cycle} history retirement failed: {error}"),
                }
            }
            match retire.tick().await {
                Ok(sandbox_controller::retirement::RetirementTick::Completed)
                    if sqlx::query_scalar::<_, bool>(
                        "SELECT EXISTS(SELECT 1 FROM allocation_retirements WHERE allocation_id=$1 AND forget_completion IS NOT NULL)",
                    )
                    .bind(allocation)
                    .fetch_one(&pool)
                    .await
                    .unwrap() =>
                {
                    break;
                }
                Ok(sandbox_controller::retirement::RetirementTick::Completed) => {}
                Ok(
                    sandbox_controller::retirement::RetirementTick::Idle
                    | sandbox_controller::retirement::RetirementTick::Deferred,
                ) => {}
                Err(error @ sandbox_controller::retirement::RetirementError::Rpc(_)) => {
                    last_retirement_rpc = Some(error.to_string());
                }
                Err(error) => panic!("cycle {cycle} allocation retirement failed: {error}"),
            }
            if Instant::now() >= until {
                // Preserve the exact guardian directory and journal on a long
                // real-VM failure so a retirement refusal can be diagnosed
                // from the entry name and metadata, rather than only its log.
                f.vm.temp.disable_cleanup(true);
                panic!(
                    "cycle {cycle} retirement did not converge; last RPC error: {last_retirement_rpc:?}; retained artifacts: {}; host stderr: {}",
                    f.vm.temp.path().display(),
                    fs::read_to_string(f.vm.temp.path().join("host.stderr")).unwrap_or_default(),
                );
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let running: i64 =
            sqlx::query_scalar("SELECT count(*) FROM allocations WHERE status='running'")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(
            running, 1,
            "cycle {cycle} disturbed the older live allocation"
        );
        if cycle % 64 == 63 {
            eprintln!("sustained_reuse_cycle={}", cycle + 1);
        }
    }
    let records: usize = guardian::read_json::<serde_json::Value>(
        &f.config.state_root.join("host.json"),
    )
    .unwrap()["records"]
        .as_object()
        .unwrap()
        .len();
    assert_eq!(records, 1);
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM allocations WHERE status='running'")
            .fetch_one(&pool)
            .await
            .unwrap(),
        1
    );
    eprintln!(
        "real_sustained_reuse_observation {}",
        json!({"cycles":cycles,"older_live_allocation":true,"host_records":records,"allocation_rows":cycles+1})
    );
}

async fn wait_operation_preserving(
    app: &axum::Router,
    token: &str,
    controller: &mut sandbox_controller::Controller,
    operation: &str,
    expected: &str,
    fixture: &mut Fixture,
) {
    let until = Instant::now() + Duration::from_secs(65);
    loop {
        controller.tick().await.unwrap();
        let (_, result) = http(
            app,
            token,
            "GET",
            &format!("/v1/operations/{operation}"),
            operation,
            serde_json::Value::Null,
        )
        .await;
        if result["status"] == expected {
            return;
        }
        if Instant::now() >= until {
            fixture.vm.temp.disable_cleanup(true);
            panic!(
                "operation {operation} did not reach {expected}: {result}; retained test artifacts: {}",
                fixture.vm.temp.path().display()
            );
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}
#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
#[ignore = "requires root, KVM artifacts, PostgreSQL and HUDSON_TEST_S3_* MinIO"]
async fn real_output_minio_archives_binary_bytes_and_reconciles_after_epoch_restart(
    pool: sqlx::PgPool,
) {
    api_lifecycle(pool, true, false).await;
}
async fn api_lifecycle(pool: sqlx::PgPool, output: bool, automatic_retirement: bool) {
    use sandbox_protocol::{ProjectId, ProjectToken};
    use serde_json::{Value, json};
    let mut f = Fixture::configured_permits(true, output, true).await;
    let project = ProjectId::generate();
    let token = ProjectToken::generate().unwrap();
    sqlx::query("INSERT INTO projects(id,name,status,limits,api_tokens) VALUES($1,'real-vm-test','active','{}',$2)")
        .bind(project.uuid()).bind(json!([{"key_id":token.key_id().as_str(),"hash":hex::encode(token.hash().as_bytes())}])).execute(&pool).await.unwrap();
    sqlx::query("INSERT INTO hosts(id,status,cpu_capacity,memory_capacity_mib,disk_capacity_mib,supervisor_epoch) VALUES($1,'ready',2,256,128,1)")
        .bind(f.config.host.uuid()).execute(&pool).await.unwrap();
    let store = sandbox_store::Store::from_pool(pool.clone());
    let image = f.config.images.keys().next().unwrap().clone();
    let artifacts = output.then(|| {
        sandbox_artifacts::S3Config::read_private(&f.vm.temp.path().join("output.json"))
            .unwrap()
            .build()
            .unwrap()
    });
    let sources = output.then(|| {
        std::sync::Arc::new(
            sandbox_artifacts::S3Config::read_private(&f.vm.temp.path().join("output.json"))
                .unwrap()
                .build_sources()
                .unwrap(),
        )
    });
    let app = sandbox_api::router_with_uploads(
        sandbox_api::AppState {
            store: store.clone(),
            images: sandbox_protocol::images::ImageAllowlist::new([image.clone()]).unwrap(),
        },
        artifacts.clone().map(|s| {
            std::sync::Arc::new(s) as std::sync::Arc<dyn sandbox_api::outputs::OutputReader>
        }),
        Some(std::sync::Arc::new(
            sandbox_api::streams::live::LiveClient::new(
                f.config.host,
                f.url.clone(),
                f.tls.ca.pem().into_bytes(),
                f.reader.cert.pem().into_bytes(),
                f.reader.key.serialize_pem().into_bytes(),
                false,
            )
            .unwrap(),
        )),
        Some(std::sync::Arc::new(
            sandbox_api::files::client::FileClient::new(
                f.config.host,
                f.url.clone(),
                f.tls.ca.pem().into_bytes(),
                f.file_reader.cert.pem().into_bytes(),
                f.file_reader.key.serialize_pem().into_bytes(),
                false,
            )
            .unwrap(),
        )),
        sources
            .clone()
            .map(|s| s as std::sync::Arc<dyn sandbox_artifacts::sources::SourceBackend>),
    );
    let mut controller = sandbox_controller::Controller::connect(
        store.clone(),
        sandbox_controller::ControllerConfig {
            endpoint: f.url.clone(),
            host: f.config.host,
            epoch: 1,
            allowed_images: std::collections::BTreeSet::from([image.clone()]),
            allow_simulated: false,
        },
        f.tls.ca.pem().as_bytes(),
        f.tls.host.cert.pem().as_bytes(),
        f.tls.host.key.serialize_pem().as_bytes(),
    )
    .await
    .unwrap();
    if let Some(sources) = sources {
        controller = controller.with_file_sources(sources);
    }
    let token = token.render_once();
    let key = OperationId::generate().to_string();
    let body = json!({"image_digest":image,"resources":{"vcpu":1,"memory_mib":128,"disk_mib":64}});
    let (status, admission) = http(&app, &token, "POST", "/v1/sandboxes", &key, body.clone()).await;
    assert_eq!(status, http::StatusCode::ACCEPTED);
    let (_, repeated) = http(&app, &token, "POST", "/v1/sandboxes", &key, body).await;
    assert_eq!(repeated, admission);
    let operation = admission["operation_id"].as_str().unwrap();
    let sandbox = admission["sandbox_id"].as_str().unwrap();
    let until = Instant::now() + Duration::from_secs(25);
    loop {
        controller.tick().await.unwrap();
        let (_, result) = http(
            &app,
            &token,
            "GET",
            &format!("/v1/operations/{operation}"),
            &key,
            Value::Null,
        )
        .await;
        if result["status"] == "succeeded" {
            assert_eq!(result["result"]["simulated"], false);
            break;
        }
        assert!(
            Instant::now() < until,
            "create failed to converge: {result}"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    let (_, state) = http(
        &app,
        &token,
        "GET",
        &format!("/v1/sandboxes/{sandbox}"),
        &key,
        Value::Null,
    )
    .await;
    assert_eq!(state["observed_state"], "running");
    assert_eq!(state["observation_simulated"], false);
    let (allocation, initial): (String, i64) = sqlx::query_as(
        "SELECT id::text,(extract(epoch FROM lease_expires_at)*1000)::bigint FROM allocations",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    let m: Manifest = guardian::read_json(
        &f.config
            .state_root
            .join("a")
            .join(format!("{}_{allocation}", AllocationId::PREFIX))
            .join("manifest.json"),
    )
    .unwrap();
    assert!(m.receipt().unwrap().guest_boot_id.is_some());
    assert_eq!(
        m.guest_client()
            .unwrap()
            .hello()
            .await
            .unwrap()
            .allocation_id,
        m.start.owner.allocation
    );
    let until = Instant::now() + Duration::from_secs(20);
    loop {
        controller.tick().await.unwrap();
        let (renewed,):(bool,)=sqlx::query_as("SELECT (extract(epoch FROM lease_expires_at)*1000)::bigint>$1 AND maintenance_revision>0 AND NOT renewal_pending FROM allocations").bind(initial).fetch_one(&pool).await.unwrap();
        if renewed {
            break;
        }
        assert!(
            Instant::now() < until,
            "controller never renewed execution lease"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    let mut archived = Vec::new();
    // Authenticated public execute is owned by the runtime after admission.
    for (argv, expected_status, expected_exit) in [
        (
            vec![
                "/bin/busybox",
                "sh",
                "-c",
                "echo once >> /execution-marker; /bin/busybox printf 'a\\000b\\377'; /bin/busybox printf 'err\\000' >&2; /bin/busybox sleep 3; exit 0",
            ],
            "succeeded",
            Some(0),
        ),
        (
            vec!["/bin/busybox", "sh", "-c", "exit 7"],
            "failed",
            Some(7),
        ),
        (vec!["/bin/busybox", "sleep", "10"], "failed", None),
    ] {
        let execute_key = OperationId::generate().to_string();
        let command = json!({"argv":argv,"deadline_unix_ms":guardian::wall_ms()+if expected_exit.is_none(){1500}else{10000},"output_limit":1024});
        let (status, admitted) = http(
            &app,
            &token,
            "POST",
            &format!("/v1/sandboxes/{sandbox}/execute"),
            &execute_key,
            command.clone(),
        )
        .await;
        assert_eq!(status, http::StatusCode::ACCEPTED, "{admitted}");
        let (_, retry) = http(
            &app,
            &token,
            "POST",
            &format!("/v1/sandboxes/{sandbox}/execute"),
            &execute_key,
            command,
        )
        .await;
        assert_eq!(admitted, retry);
        let id = admitted["operation_id"].as_str().unwrap();
        if output && expected_exit == Some(0) {
            use base64::Engine;
            use tokio_stream::StreamExt;
            use tower::ServiceExt;
            controller.tick().await.unwrap();
            let request = http::Request::builder()
                .uri(format!("/v1/operations/{id}/stream"))
                .header("authorization", format!("Bearer {token}"))
                .body(axum::body::Body::empty())
                .unwrap();
            let response = app.clone().oneshot(request).await.unwrap();
            assert_eq!(response.status(), http::StatusCode::OK);
            let mut stream = response.into_body().into_data_stream();
            let first = tokio::time::timeout(Duration::from_secs(2), stream.next())
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            let first = std::str::from_utf8(&first).unwrap();
            let data: Value = serde_json::from_str(
                first
                    .lines()
                    .find_map(|l| l.strip_prefix("data: "))
                    .unwrap(),
            )
            .unwrap();
            assert_eq!(data["simulated"], false);
            assert_eq!(data["complete"], false);
            let mut observed = std::collections::BTreeMap::<String, Vec<u8>>::new();
            let name = data["stream"].as_str().unwrap().to_string();
            observed.insert(
                name,
                base64::engine::general_purpose::STANDARD
                    .decode(data["data_base64"].as_str().unwrap())
                    .unwrap(),
            );
            let cursor = first
                .lines()
                .find_map(|l| l.strip_prefix("id: "))
                .unwrap()
                .to_string();
            drop(stream);
            let request = http::Request::builder()
                .uri(format!("/v1/operations/{id}/stream"))
                .header("authorization", format!("Bearer {token}"))
                .header("last-event-id", cursor)
                .body(axum::body::Body::empty())
                .unwrap();
            let response = app.clone().oneshot(request).await.unwrap();
            assert_eq!(response.status(), http::StatusCode::OK);
            let bytes = tokio::time::timeout(
                Duration::from_secs(8),
                axum::body::to_bytes(response.into_body(), 100000),
            )
            .await
            .unwrap()
            .unwrap();
            let text = std::str::from_utf8(&bytes).unwrap();
            assert!(text.contains("event: end"));
            for line in text.lines().filter_map(|l| l.strip_prefix("data: ")) {
                let frame: Value = serde_json::from_str(line).unwrap();
                if let Some(name) = frame["stream"].as_str() {
                    let captured = observed.entry(name.to_string()).or_default();
                    assert_eq!(frame["offset"].as_u64().unwrap(), captured.len() as u64);
                    captured.extend(
                        base64::engine::general_purpose::STANDARD
                            .decode(frame["data_base64"].as_str().unwrap())
                            .unwrap(),
                    );
                }
            }
            assert_eq!(observed["stdout"], b"a\0b\xff");
            assert_eq!(observed["stderr"], b"err\0");
            eprintln!(
                "real_sse_observation {{\"live_binary_before_completion\":true,\"reconnected_same_operation\":true,\"final_end\":true,\"simulated\":false}}"
            );
        }
        let until = Instant::now() + Duration::from_secs(15);
        loop {
            controller.tick().await.unwrap();
            let (_, result) = http(
                &app,
                &token,
                "GET",
                &format!("/v1/operations/{id}"),
                &execute_key,
                Value::Null,
            )
            .await;
            if result["status"] == expected_status {
                assert_eq!(result["result"]["simulated"], false);
                assert_eq!(result["result"]["exit_code"], json!(expected_exit));
                if expected_exit.is_none() {
                    assert_eq!(result["phase"], "timed_out");
                }
                break;
            }
            assert!(
                Instant::now() < until,
                "execute failed to converge: {result}"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        if let Some(artifacts) = &artifacts {
            assert_eq!(
                controller
                    .output_archiver(3600, 60)
                    .unwrap()
                    .tick()
                    .await
                    .unwrap(),
                sandbox_controller::archive::ArchiveTick::Published
            );
            let view = store
                .output_for_project(project, id.parse().unwrap())
                .await
                .unwrap()
                .unwrap();
            assert_eq!(view.status, "published");
            let refs = view.references.unwrap();
            let owner = view.owner.unwrap();
            let stdout = artifacts
                .read(&refs.stdout, &owner, guardian::wall_ms(), 0, 1024)
                .await
                .unwrap();
            let stderr = artifacts
                .read(&refs.stderr, &owner, guardian::wall_ms(), 0, 1024)
                .await
                .unwrap();
            assert_eq!(
                stdout.bytes,
                if expected_exit == Some(0) {
                    b"a\x00b\xff".as_slice()
                } else {
                    b""
                }
            );
            assert_eq!(
                stderr.bytes,
                if expected_exit == Some(0) {
                    b"err\x00".as_slice()
                } else {
                    b""
                }
            );
            assert!(stdout.eof && stderr.eof);
            for (name, expected) in [
                ("stdout", stdout.bytes.as_slice()),
                ("stderr", stderr.bytes.as_slice()),
            ] {
                public_output(&app, &token, id, name, expected).await;
            }
            let (ticket,plans,revision):(Value,Value,i64) = sqlx::query_as("SELECT output_ticket,output_plan,output_claim_revision FROM operations WHERE id=$1").bind(owner.operation_id.uuid()).fetch_one(&pool).await.unwrap();
            archived.push((refs, owner, ticket, plans, revision));
        }
    }
    public_cancel_case(&pool, &app, &token, &mut controller, sandbox, &m).await;
    let file_capture =
        public_files::round_trip(&app, &token, &mut controller, sandbox, output).await;
    if !output {
        history::public_capacity_retirement(&pool, &store, &app, &token, &mut controller, sandbox)
            .await;
    }
    if output {
        // Archive retries may not replay a command with side effects.
        let guest = m.guest_client().unwrap();
        let command = sandbox_protocol::guest_model::Execute {
            operation_id: OperationId::generate(),
            argv: vec![
                "/bin/busybox".into(),
                "cat".into(),
                "/execution-marker".into(),
            ],
            env: BTreeMap::new(),
            cwd: "/".into(),
            deadline_unix_ms: guardian::wall_ms() + 5000,
            output_limit: 1024,
        };
        guest.execute(&command).await.unwrap();
        let until = Instant::now() + Duration::from_secs(5);
        while !guest
            .inspect(command.operation_id)
            .await
            .unwrap()
            .cleanup_confirmed
        {
            assert!(Instant::now() < until);
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        assert_eq!(
            guest
                .output(sandbox_protocol::guest::ReadOutput {
                    operation_id: command.operation_id.to_string(),
                    stream: sandbox_protocol::guest::Stream::Stdout as i32,
                    offset: 0,
                    limit: 1024
                })
                .await
                .unwrap()
                .data,
            b"once\n"
        );
    }
    let (status, destroy) = http(
        &app,
        &token,
        "POST",
        &format!("/v1/sandboxes/{sandbox}/destroy"),
        &OperationId::generate().to_string(),
        json!({}),
    )
    .await;
    assert_eq!(status, http::StatusCode::ACCEPTED, "{destroy}");
    let destroy = destroy["operation_id"].as_str().unwrap();
    let until = Instant::now() + Duration::from_secs(15);
    loop {
        controller.tick().await.unwrap();
        let (_, result) = http(
            &app,
            &token,
            "GET",
            &format!("/v1/operations/{destroy}"),
            &key,
            Value::Null,
        )
        .await;
        if result["status"] == "succeeded" {
            break;
        }
        assert!(
            Instant::now() < until,
            "destroy failed to converge: {result}"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    let (_, state) = http(
        &app,
        &token,
        "GET",
        &format!("/v1/sandboxes/{sandbox}"),
        &key,
        Value::Null,
    )
    .await;
    public_files::after_destroy(&app, &token, sandbox, &file_capture).await;
    assert_eq!(state["observed_state"], "destroyed");
    assert_eq!(state["observation_simulated"], false);
    assert!(!m.group().exists());
    assert!(!m.directory().join("run").exists());
    assert!(m.receipt().unwrap().cleanup_confirmed);
    let (released,):(bool,)=sqlx::query_as("SELECT released_at IS NOT NULL AND release_evidence->>'simulated'='false' FROM allocations").fetch_one(&pool).await.unwrap();
    assert!(released);
    if !output {
        history::public_released_retirement(&pool, &store, &mut f, &image).await;
        if automatic_retirement {
            forgetting::automatic_handoff(&pool, &store, &mut f, &image).await;
        } else {
            forgetting::public_handoff(&pool, &store, &mut f, &image).await;
        }
    }
    if let Some(artifacts) = &artifacts {
        f.restart().await;
        let mut client = transport::connect_archiver(
            &f.url,
            f.config.host,
            f.tls.ca.pem().as_bytes(),
            f.tls.host.cert.pem().as_bytes(),
            f.tls.host.key.serialize_pem().as_bytes(),
        )
        .await
        .unwrap();
        for (refs, owner, ticket, plans, revision) in archived {
            let response = client
                .archive_output(sandbox_protocol::supervisor::OutputRequest {
                    ticket_json: serde_json::to_vec(&ticket).unwrap(),
                    plans_json: serde_json::to_vec(&plans).unwrap(),
                    publication_revision: revision + 1,
                    claim_expires_unix_ms: guardian::wall_ms() + 120000,
                })
                .await
                .unwrap()
                .into_inner();
            assert!(!response.simulated);
            assert_eq!(response.supervisor_epoch, 2);
            assert_eq!(
                serde_json::from_slice::<sandbox_protocol::output::OutputRefs>(
                    &response.references_json
                )
                .unwrap(),
                refs
            );
            assert_eq!(owner.host_epoch, 1);
            let retained = artifacts
                .read(&refs.stdout, &owner, guardian::wall_ms(), 0, 1024)
                .await
                .unwrap();
            public_output(
                &app,
                &token,
                &owner.operation_id.to_string(),
                "stdout",
                &retained.bytes,
            )
            .await;
            public_stream(
                &app,
                &token,
                &owner.operation_id.to_string(),
                &retained.bytes,
            )
            .await;
        }
        assert!(!m.group().exists());
        eprintln!(
            "real_output_archive_observation {}",
            json!({"public_sse_after_destroy_and_epoch_restart":true,"public_binary_output_verified":true,"public_output_after_destroy_and_epoch_restart":true,"binary_stdout_stderr_verified":true,"empty_streams_verified":true,"controller_published":true,"execution_marker_once":true,"destroy_confirmed":true,"epoch_2_reconciles_epoch_1_objects_without_guest":true})
        );
    }
    eprintln!(
        "real_api_lifecycle_observation {}",
        json!({"create_succeeded":true,"duplicate_handles_match":true,"simulated":false,"guest_boot_bound":true,"lease_renewed":true,"destroy_succeeded":true,"database_release_confirmed":true,"cgroup_removed":true,"runtime_files_removed":true})
    );
}

async fn public_cancel_case(
    pool: &sqlx::PgPool,
    app: &axum::Router,
    token: &str,
    controller: &mut sandbox_controller::Controller,
    sandbox: &str,
    manifest: &Manifest,
) {
    use sandbox_protocol::guest_model as m;
    use serde_json::{Value, json};
    let execution_key = OperationId::generate().to_string();
    let body = json!({"argv":["/bin/busybox","sh","-c","echo once >> /cancel-marker; /bin/busybox printf 'started\\n'; /bin/busybox sleep 30; echo too-late >> /cancel-marker"],"deadline_unix_ms":guardian::wall_ms()+45000,"output_limit":1024});
    let (status, admission) = http(
        app,
        token,
        "POST",
        &format!("/v1/sandboxes/{sandbox}/execute"),
        &execution_key,
        body.clone(),
    )
    .await;
    assert_eq!(status, http::StatusCode::ACCEPTED, "{admission}");
    let target = admission["operation_id"].as_str().unwrap();
    let target_id: OperationId = target.parse().unwrap();
    let guest = manifest.guest_client().unwrap();
    let until = Instant::now() + Duration::from_secs(10);
    loop {
        controller.tick().await.unwrap();
        if let Ok(receipt) = guest.inspect(target_id).await
            && receipt.stdout.stored == 8
        {
            assert_eq!(receipt.state, m::State::LaunchIntent);
            break;
        }
        assert!(
            Instant::now() < until,
            "real cancellation target did not start"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let cancel_key = OperationId::generate().to_string();
    let (status, cancellation) = http(
        app,
        token,
        "POST",
        &format!("/v1/operations/{target}/cancel"),
        &cancel_key,
        json!({}),
    )
    .await;
    assert_eq!(status, http::StatusCode::ACCEPTED);
    let (_, retry) = http(
        app,
        token,
        "POST",
        &format!("/v1/operations/{target}/cancel"),
        &cancel_key,
        json!({}),
    )
    .await;
    assert_eq!(retry, cancellation);
    let id = cancellation["operation_id"].as_str().unwrap();
    let until = Instant::now() + Duration::from_secs(12);
    loop {
        controller.tick().await.unwrap();
        let (_, state) = http(
            app,
            token,
            "GET",
            &format!("/v1/operations/{id}"),
            &cancel_key,
            Value::Null,
        )
        .await;
        if state["status"] == "succeeded" {
            assert_eq!(state["result"]["cancelled"], true);
            assert_eq!(state["result"]["target_status"], "cancelled");
            assert_eq!(state["target_operation_id"], target);
            break;
        }
        assert!(
            Instant::now() < until,
            "real cancellation did not settle: {state}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let final_receipt = guest.inspect(target_id).await.unwrap();
    assert_eq!(final_receipt.state, m::State::Cancelled);
    assert!(final_receipt.cancel_requested && final_receipt.cleanup_confirmed);
    let (_, state) = http(
        app,
        token,
        "GET",
        &format!("/v1/operations/{target}"),
        &execution_key,
        Value::Null,
    )
    .await;
    assert_eq!(state["status"], "cancelled");
    assert_eq!(state["result"]["simulated"], false);
    assert_eq!(state["output_status"], "pending");
    let (_, retry) = http(
        app,
        token,
        "POST",
        &format!("/v1/sandboxes/{sandbox}/execute"),
        &execution_key,
        body,
    )
    .await;
    assert_eq!(retry["operation_id"], target);
    let attempts: i32 = sqlx::query_scalar("SELECT attempt_count FROM operations WHERE id=$1")
        .bind(target_id.uuid())
        .fetch_one(pool)
        .await
        .unwrap();
    assert_eq!(attempts, 1);
    let verify = m::Execute {
        operation_id: OperationId::generate(),
        argv: vec!["/bin/busybox".into(), "cat".into(), "/cancel-marker".into()],
        env: BTreeMap::new(),
        cwd: "/".into(),
        deadline_unix_ms: guardian::wall_ms() + 5000,
        output_limit: 1024,
    };
    guest.execute(&verify).await.unwrap();
    let until = Instant::now() + Duration::from_secs(5);
    while !guest
        .inspect(verify.operation_id)
        .await
        .unwrap()
        .cleanup_confirmed
    {
        assert!(Instant::now() < until);
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    let output = guest
        .output(sandbox_protocol::guest::ReadOutput {
            operation_id: verify.operation_id.to_string(),
            stream: sandbox_protocol::guest::Stream::Stdout as i32,
            offset: 0,
            limit: 1024,
        })
        .await
        .unwrap();
    assert_eq!(output.data, b"once\n");
    println!(
        "real_cancel_observation {}",
        json!({"public_request_durable":true,"target_cancelled":true,"guest_cleanup_confirmed":true,"one_execution_marker":true,"execution_attempts":1,"retained_output_pending":true,"simulated":false})
    );
}

#[tokio::test]
#[ignore = "requires root, HUDSON_GUARDIAN_TEST_VM=1 and native KVM artifacts"]
async fn real_command_rpc_retains_results_fences_absence_and_survives_request_end() {
    use sandbox_protocol::{
        guest_model as m,
        supervisor::{CommandInspection, CommandRequest},
    };
    let f = Fixture::new().await;
    let mut c = f.client().await;
    let request = f.request();
    let allocation = request.ownership.as_ref().unwrap().clone();
    let _ = c.create(request).await;
    f.ready(&mut c, &allocation).await;
    let mut owner = allocation.clone();
    owner.operation_id = OperationId::generate().to_string();
    let command = m::Execute {
        operation_id: owner.operation_id.parse().unwrap(),
        argv: vec![
            "/bin/busybox".into(),
            "sh".into(),
            "-c".into(),
            "echo once >> /execution-marker; /bin/busybox sleep 1; exit 7".into(),
        ],
        env: BTreeMap::new(),
        cwd: "/".into(),
        deadline_unix_ms: guardian::wall_ms() + 15000,
        output_limit: 1024,
    };
    let request = CommandRequest {
        ownership: Some(owner.clone()),
        command: Some((&command).into()),
    };
    let reply = c
        .execute_command(request.clone())
        .await
        .unwrap()
        .into_inner();
    assert!(!reply.simulated);
    assert!(!reply.not_started);
    // The command continues after this RPC response. Retry observes the same intent.
    let _ = c.execute_command(request.clone()).await.unwrap();
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
            let receipt: m::Receipt = receipt.try_into().unwrap();
            if receipt.state == m::State::Exited {
                assert_eq!(receipt.exit_code, Some(7));
                assert!(receipt.cleanup_confirmed);
                break;
            }
        }
        assert!(Instant::now() < until, "command did not finish");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let replay = c
        .execute_command(request.clone())
        .await
        .unwrap()
        .into_inner();
    assert_eq!(replay.receipt.unwrap().exit_code, Some(7));
    let cancelled_after_exit = c
        .cancel_command(CommandInspection {
            ownership: Some(owner.clone()),
            command_digest: command.digest().unwrap().to_vec(),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(cancelled_after_exit.receipt.unwrap().exit_code, Some(7));
    let mut changed = request;
    changed
        .command
        .as_mut()
        .unwrap()
        .argv
        .push("changed".into());
    assert_eq!(
        c.execute_command(changed).await.unwrap_err().code(),
        Code::AlreadyExists
    );
    // Private output read verifies the side effect occurred once; public output is separate work.
    let client = f.manifest(&allocation).guest_client().unwrap();
    let verify = m::Execute {
        operation_id: OperationId::generate(),
        argv: vec![
            "/bin/busybox".into(),
            "cat".into(),
            "/execution-marker".into(),
        ],
        env: BTreeMap::new(),
        cwd: "/".into(),
        deadline_unix_ms: guardian::wall_ms() + 5000,
        output_limit: 1024,
    };
    client.execute(&verify).await.unwrap();
    let until = Instant::now() + Duration::from_secs(5);
    while !client
        .inspect(verify.operation_id)
        .await
        .unwrap()
        .cleanup_confirmed
    {
        assert!(Instant::now() < until);
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    let output = client
        .output(sandbox_protocol::guest::ReadOutput {
            operation_id: verify.operation_id.to_string(),
            stream: sandbox_protocol::guest::Stream::Stdout as i32,
            offset: 0,
            limit: 1024,
        })
        .await
        .unwrap();
    assert_eq!(output.data, b"once\n");
    let mut absent_owner = owner.clone();
    absent_owner.operation_id = OperationId::generate().to_string();
    let mut absent = command.clone();
    absent.operation_id = absent_owner.operation_id.parse().unwrap();
    assert!(
        c.cancel_command(CommandInspection {
            ownership: Some(absent_owner.clone()),
            command_digest: absent.digest().unwrap().to_vec()
        })
        .await
        .unwrap()
        .into_inner()
        .not_started
    );
    assert!(
        c.inspect_command(CommandInspection {
            ownership: Some(absent_owner.clone()),
            command_digest: absent.digest().unwrap().to_vec()
        })
        .await
        .unwrap()
        .into_inner()
        .not_started
    );
    assert!(
        c.execute_command(CommandRequest {
            ownership: Some(absent_owner),
            command: Some((&absent).into())
        })
        .await
        .unwrap()
        .into_inner()
        .not_started
    );
    let mut active_owner = owner.clone();
    active_owner.operation_id = OperationId::generate().to_string();
    let mut active = command.clone();
    active.operation_id = active_owner.operation_id.parse().unwrap();
    active.argv = vec!["/bin/busybox".into(), "sleep".into(), "20".into()];
    active.deadline_unix_ms = guardian::wall_ms() + 25000;
    let response = c
        .execute_command(CommandRequest {
            ownership: Some(active_owner.clone()),
            command: Some((&active).into()),
        })
        .await
        .unwrap()
        .into_inner();
    assert!(!response.not_started);
    let mut stop = allocation.clone();
    stop.operation_id = OperationId::generate().to_string();
    let _ = c
        .stop(StopRequest {
            ownership: Some(stop.clone()),
        })
        .await;
    f.released(&mut c, &stop).await;
    let uncertain = c
        .inspect_command(CommandInspection {
            ownership: Some(active_owner),
            command_digest: active.digest().unwrap().to_vec(),
        })
        .await
        .unwrap()
        .into_inner();
    assert!(!uncertain.not_started);
    assert!(uncertain.receipt.is_none());
    // A previously retained exit survives cleanup; the active command's result does not get invented.
    let completed = c
        .inspect_command(CommandInspection {
            ownership: Some(owner),
            command_digest: command.digest().unwrap().to_vec(),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(completed.receipt.unwrap().exit_code, Some(7));
}

async fn public_output(
    app: &axum::Router,
    token: &str,
    operation: &str,
    name: &str,
    expected: &[u8],
) {
    use tower::ServiceExt;
    let request = http::Request::builder()
        .uri(format!("/v1/operations/{operation}/outputs/{name}"))
        .header("authorization", format!("Bearer {token}"))
        .body(axum::body::Body::empty())
        .unwrap();
    let response = app.clone().oneshot(request).await.unwrap();
    assert_eq!(response.status(), http::StatusCode::OK);
    assert_eq!(response.headers()["x-output-simulated"], "false");
    assert_eq!(response.headers()["x-output-eof"], "true");
    assert_eq!(
        axum::body::to_bytes(response.into_body(), 65536)
            .await
            .unwrap()
            .as_ref(),
        expected
    );
}

#[tokio::test]
#[ignore = "requires root, HUDSON_GUARDIAN_TEST_VM=1 and native KVM artifacts"]
async fn real_live_output_reads_binary_reconnects_without_journal_mutation_or_reexecution() {
    use sandbox_protocol::{
        guest as w, guest_model as m,
        live_output::LiveOutputScope,
        output::OutputOwner,
        supervisor::{CommandRequest, LiveOutputRequest},
    };
    let mut f = Fixture::new().await;
    let mut controller = f.client().await;
    let create = f.request();
    let allocation = create.ownership.as_ref().unwrap().clone();
    let _ = controller.create(create).await;
    f.ready(&mut controller, &allocation).await;
    let mut owner = allocation.clone();
    owner.operation_id = OperationId::generate().to_string();
    let command=m::Execute {operation_id:owner.operation_id.parse().unwrap(),argv:vec![
        "/bin/busybox".into(),"sh".into(),"-c".into(),
        "echo once >> /live-marker; printf '\\000\\377a'; printf '\\376e' >&2; /bin/busybox sleep 3; printf z; exit 7".into()],
        env:Default::default(),cwd:"/".into(),deadline_unix_ms:guardian::wall_ms()+20000,output_limit:1024};
    let reply = controller
        .execute_command(CommandRequest {
            ownership: Some(owner.clone()),
            command: Some((&command).into()),
        })
        .await
        .unwrap()
        .into_inner();
    let receipt: m::Receipt = reply.receipt.unwrap().try_into().unwrap();
    let scope = LiveOutputScope {
        version: 1,
        owner: OutputOwner {
            project_id: owner.project_id.parse().unwrap(),
            sandbox_id: owner.sandbox_id.parse().unwrap(),
            operation_id: command.operation_id,
            allocation_id: receipt.context.allocation_id,
            generation: receipt.context.generation,
            boot_id: receipt.context.boot_id,
            host_id: f.config.host,
            host_epoch: f.config.epoch,
        },
        command_digest: command.digest().unwrap(),
        output_limit: command.output_limit,
        deadline_unix_ms: command.deadline_unix_ms,
    };
    let mut request = LiveOutputRequest {
        scope_json: serde_json::to_vec(&scope).unwrap(),
        output: Some(w::ReadOutput {
            operation_id: command.operation_id.to_string(),
            stream: w::Stream::Stdout as i32,
            offset: 0,
            limit: 32,
        }),
        expires_unix_ms: guardian::wall_ms() + 30000,
    };
    // A controller certificate cannot read even though it can execute.
    let mut denied = transport::connect_output_reader(
        &f.url,
        f.config.host,
        f.tls.ca.pem().as_bytes(),
        f.tls.host.cert.pem().as_bytes(),
        f.tls.host.key.serialize_pem().as_bytes(),
    )
    .await
    .unwrap();
    assert_eq!(
        denied.read(request.clone()).await.unwrap_err().code(),
        Code::PermissionDenied
    );
    let mut reader = transport::connect_output_reader(
        &f.url,
        f.config.host,
        f.tls.ca.pem().as_bytes(),
        f.reader.cert.pem().as_bytes(),
        f.reader.key.serialize_pem().as_bytes(),
    )
    .await
    .unwrap();
    let journal = fs::read(f.config.state_root.join("host.json")).unwrap();
    let until = Instant::now() + Duration::from_secs(2);
    loop {
        let observed = reader.read(request.clone()).await.unwrap().into_inner();
        assert!(!observed.simulated);
        assert_eq!(observed.request, Some(request.clone()));
        let chunk = observed.chunk.unwrap();
        assert!(!chunk.complete);
        if chunk.data == [0, 255, b'a'] {
            assert!(chunk.at_end);
            break;
        }
        assert!(Instant::now() < until, "first live bytes never arrived");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    // Reading the captured EOF while running is not final completion.
    request.output.as_mut().unwrap().offset = 3;
    let chunk = reader
        .read(request.clone())
        .await
        .unwrap()
        .into_inner()
        .chunk
        .unwrap();
    assert!(chunk.data.is_empty() && chunk.at_end && !chunk.complete);
    drop(reader);
    let mut reader = transport::connect_output_reader(
        &f.url,
        f.config.host,
        f.tls.ca.pem().as_bytes(),
        f.reader.cert.pem().as_bytes(),
        f.reader.key.serialize_pem().as_bytes(),
    )
    .await
    .unwrap();
    let until = Instant::now() + Duration::from_secs(8);
    loop {
        let observed = reader.read(request.clone()).await.unwrap().into_inner();
        let chunk = observed.chunk.unwrap();
        if chunk.complete {
            assert_eq!(chunk.data, b"z");
            assert!(chunk.at_end);
            assert_eq!(chunk.next_offset, 4);
            assert_eq!(observed.receipt.unwrap().exit_code, Some(7));
            break;
        }
        assert!(Instant::now() < until, "final output never arrived");
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    request.output.as_mut().unwrap().offset = 4;
    let end = reader
        .read(request.clone())
        .await
        .unwrap()
        .into_inner()
        .chunk
        .unwrap();
    assert!(end.data.is_empty() && end.at_end && end.complete);
    request.output.as_mut().unwrap().offset = 0;
    request.output.as_mut().unwrap().stream = w::Stream::Stderr as i32;
    let stderr = reader
        .read(request.clone())
        .await
        .unwrap()
        .into_inner()
        .chunk
        .unwrap();
    assert_eq!(stderr.data, [254, b'e']);
    assert!(stderr.complete && stderr.at_end);
    assert_eq!(
        journal,
        fs::read(f.config.state_root.join("host.json")).unwrap(),
        "read changed host journal"
    );
    // A direct guest read verifies the side effect stayed one line across reconnects.
    let guest = f.manifest(&allocation).guest_client().unwrap();
    let verify = m::Execute {
        operation_id: OperationId::generate(),
        argv: vec!["/bin/busybox".into(), "cat".into(), "/live-marker".into()],
        env: Default::default(),
        cwd: "/".into(),
        deadline_unix_ms: guardian::wall_ms() + 5000,
        output_limit: 1024,
    };
    guest.execute(&verify).await.unwrap();
    let until = Instant::now() + Duration::from_secs(5);
    while !guest
        .inspect(verify.operation_id)
        .await
        .unwrap()
        .cleanup_confirmed
    {
        assert!(Instant::now() < until);
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert_eq!(
        guest
            .output(w::ReadOutput {
                operation_id: verify.operation_id.to_string(),
                stream: w::Stream::Stdout as i32,
                offset: 0,
                limit: 32
            })
            .await
            .unwrap()
            .data,
        b"once\n"
    );
    let mut stop = allocation.clone();
    stop.operation_id = OperationId::generate().to_string();
    // Stop can acknowledge uncertainty while the guardian is still cleaning up.
    // Only subsequent release evidence establishes completion.
    let _ = controller
        .stop(StopRequest {
            ownership: Some(stop.clone()),
        })
        .await;
    f.released(&mut controller, &stop).await;
    assert_eq!(
        reader.read(request.clone()).await.unwrap_err().code(),
        Code::Unavailable
    );
    f.restart().await;
    let mut reader = transport::connect_output_reader(
        &f.url,
        f.config.host,
        f.tls.ca.pem().as_bytes(),
        f.reader.cert.pem().as_bytes(),
        f.reader.key.serialize_pem().as_bytes(),
    )
    .await
    .unwrap();
    request.expires_unix_ms = guardian::wall_ms() + 5000;
    assert_eq!(
        reader.read(request).await.unwrap_err().code(),
        Code::FailedPrecondition
    );
    eprintln!(
        "real_live_output_observation {{\"binary_stdout_stderr\":true,\"pending_eof_distinct\":true,\"reconnect_no_reexecution\":true,\"journal_unchanged\":true,\"destroy_and_old_epoch_rejected\":true}}"
    );
}

async fn public_stream(app: &axum::Router, token: &str, operation: &str, expected: &[u8]) {
    use base64::Engine;
    use tower::ServiceExt;
    let request = http::Request::builder()
        .uri(format!("/v1/operations/{operation}/stream"))
        .header("authorization", format!("Bearer {token}"))
        .body(axum::body::Body::empty())
        .unwrap();
    let response = app.clone().oneshot(request).await.unwrap();
    assert_eq!(response.status(), http::StatusCode::OK);
    let bytes = tokio::time::timeout(
        Duration::from_secs(10),
        axum::body::to_bytes(response.into_body(), 100000),
    )
    .await
    .unwrap()
    .unwrap();
    let text = std::str::from_utf8(&bytes).unwrap();
    assert!(text.contains("event: end"));
    let mut stdout = Vec::new();
    for line in text.lines().filter_map(|l| l.strip_prefix("data: ")) {
        let frame: serde_json::Value = serde_json::from_str(line).unwrap();
        if frame["stream"] == "stdout" {
            assert_eq!(frame["simulated"], false);
            assert_eq!(frame["offset"].as_u64().unwrap(), stdout.len() as u64);
            stdout.extend(
                base64::engine::general_purpose::STANDARD
                    .decode(frame["data_base64"].as_str().unwrap())
                    .unwrap(),
            );
        }
    }
    assert_eq!(stdout, expected);
}

#[tokio::test]
#[ignore = "requires root, HUDSON_GUARDIAN_TEST_VM=1 and native KVM artifacts"]
async fn real_guest_file_upload_execute_and_captured_download_round_trip() {
    use sandbox_protocol::{files as files_model, guest_model as m};
    use sha2::{Digest, Sha256};
    let f = Fixture::new().await;
    let mut c = f.client().await;
    let request = f.request();
    let owner = request.ownership.as_ref().unwrap().clone();
    let _ = c.create(request).await;
    f.ready(&mut c, &owner).await;
    let guest = f.manifest(&owner).guest_client().unwrap();
    let bytes: Vec<u8> = (0..(96 * 1024 + 3)).map(|n| (n % 251) as u8).collect();
    let input = files_model::Upload {
        operation_id: OperationId::generate(),
        path: "input.bin".into(),
        size: bytes.len() as u64,
        sha256: Sha256::digest(&bytes).into(),
        mode: 0o644,
    };
    guest.begin_upload(&input).await.unwrap();
    for (i, data) in bytes.chunks(files_model::MAX_CHUNK_BYTES).enumerate() {
        guest
            .write_file(&input, (i * files_model::MAX_CHUNK_BYTES) as u64, data)
            .await
            .unwrap();
    }
    assert_eq!(
        guest.commit_upload(&input).await.unwrap().state,
        files_model::State::Committed
    );
    let script=b"#!/bin/busybox sh\n/bin/busybox cp input.bin result.bin\nprintf mutated > input.bin\nprintf 'once\\n' >> marker\n";
    let code = files_model::Upload {
        operation_id: OperationId::generate(),
        path: "transform.sh".into(),
        size: script.len() as u64,
        sha256: Sha256::digest(script).into(),
        mode: 0o755,
    };
    guest.begin_upload(&code).await.unwrap();
    guest.write_file(&code, 0, script).await.unwrap();
    guest.commit_upload(&code).await.unwrap();
    let command = m::Execute {
        operation_id: OperationId::generate(),
        argv: vec!["/workspace/transform.sh".into()],
        env: BTreeMap::new(),
        cwd: "/workspace".into(),
        deadline_unix_ms: guardian::wall_ms() + 10000,
        output_limit: 1024,
    };
    guest.execute(&command).await.unwrap();
    let until = Instant::now() + Duration::from_secs(10);
    loop {
        let receipt = guest.inspect(command.operation_id).await.unwrap();
        if receipt.cleanup_confirmed {
            assert_eq!(receipt.exit_code, Some(0));
            break;
        }
        assert!(Instant::now() < until);
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert_eq!(guest.execute(&command).await.unwrap().exit_code, Some(0));
    // Retrying a completed upload must not undo the executed script's mutation.
    guest.begin_upload(&input).await.unwrap();
    guest.commit_upload(&input).await.unwrap();
    let changed = guest.capture_file("input.bin").await.unwrap();
    assert_eq!(
        guest.read_file(&changed, 0, 32).await.unwrap().data,
        b"mutated"
    );
    guest.release_file(&changed).await.unwrap();
    let marker = guest.capture_file("marker").await.unwrap();
    assert_eq!(
        guest.read_file(&marker, 0, 32).await.unwrap().data,
        b"once\n"
    );
    guest.release_file(&marker).await.unwrap();
    let captured = guest.capture_file("result.bin").await.unwrap();
    let mut retrieved = Vec::new();
    while (retrieved.len() as u64) < captured.size {
        let chunk = guest
            .read_file(
                &captured,
                retrieved.len() as u64,
                files_model::MAX_CHUNK_BYTES as u32,
            )
            .await
            .unwrap();
        retrieved.extend_from_slice(&chunk.data);
    }
    assert_eq!(retrieved, bytes);
    assert_eq!(Sha256::digest(&retrieved).as_slice(), captured.sha256);
    guest.release_file(&captured).await.unwrap();
    assert!(guest.read_file(&captured, 0, 32).await.is_err());
    let mut conflict = input.clone();
    conflict.mode = 0o755;
    assert!(guest.begin_upload(&conflict).await.is_err());
    assert!(
        guest
            .capture_file("../run/hudson/agent/context.json")
            .await
            .is_err()
    );
    let _ = c
        .stop(StopRequest {
            ownership: Some(owner.clone()),
        })
        .await;
    f.released(&mut c, &owner).await;
    println!(
        "real_file_transfer_observation={}",
        serde_json::json!({"simulated":false,"round_trip_bytes":bytes.len(),"sha256_verified":true,"uploaded_script_executed":true,"execution_marker_once":true,"upload_retry_preserved_later_change":true,"released_capture_not_recreated":true,"vm_cleanup_confirmed":true})
    );
}

#[path = "support/supervisor_files.rs"]
mod supervisor_files;

#[path = "support/file_downloads.rs"]
mod file_downloads;

#[path = "support/public_files.rs"]
mod public_files;

#[path = "support/previous_epoch.rs"]
mod previous_epoch;

#[path = "support/history.rs"]
mod history;

#[tokio::test]
#[ignore = "requires root, HUDSON_GUARDIAN_TEST_VM=1 and controlled KVM artifacts"]
async fn real_host_registers_permits_and_rejects_replay_and_downgrade() {
    use sandbox_protocol::{allocation_authority::Permit, supervisor::AllocationAuthorityRequest};
    let mut f = Fixture::configured_permits(true, false, true).await;
    let mut c = f.client().await;
    assert!(
        c.health(HealthRequest {})
            .await
            .unwrap()
            .get_ref()
            .launch_permits_required
    );
    let mut request = f.request();
    let owner = request.ownership.as_ref().unwrap().clone();
    assert!(c.create(request.clone()).await.is_err());
    let permit = Permit {
        host: owner.host_id.parse().unwrap(),
        project: owner.project_id.parse().unwrap(),
        sandbox: owner.sandbox_id.parse().unwrap(),
        allocation: owner.allocation_id.parse().unwrap(),
        create_operation: owner.operation_id.parse().unwrap(),
        generation: owner.generation,
        original_epoch: owner.supervisor_epoch,
        serial: 1,
    };
    request.launch_permit_json = serde_json::to_vec(&permit).unwrap();
    assert!(c.create(request.clone()).await.is_err());
    let mut registration = AllocationAuthorityRequest {
        host_id: f.config.host.to_string(),
        reporting_epoch: 1,
        permits_json: serde_json::to_vec(&[permit]).unwrap(),
    };
    c.allocation_authority(registration.clone()).await.unwrap(); // discard acknowledgement
    registration.permits_json.clear();
    assert_eq!(
        c.allocation_authority(registration.clone())
            .await
            .unwrap()
            .get_ref()
            .registered_through,
        1
    );
    let mut changed = request.clone();
    changed.ownership.as_mut().unwrap().project_id =
        sandbox_protocol::ProjectId::generate().to_string();
    assert!(c.create(changed).await.is_err());
    if let Err(error) = c.create(request.clone()).await {
        assert!(
            matches!(error.code(), Code::Cancelled | Code::DeadlineExceeded),
            "{error}"
        );
    }
    // A timed-out Create remains unknown until exact-owner inspection proves it.
    assert_eq!(f.ready(&mut c, &owner).await.start_count, 1);
    assert!(f.manifest(&owner).launch_permit.is_some());
    c.create(request.clone()).await.unwrap();
    let mut stop = owner.clone();
    stop.operation_id = OperationId::generate().to_string();
    if let Err(error) = c
        .stop(StopRequest {
            ownership: Some(stop.clone()),
        })
        .await
    {
        assert!(
            matches!(
                error.code(),
                Code::Unavailable | Code::Cancelled | Code::DeadlineExceeded
            ),
            "{error}"
        );
    }
    f.released(&mut c, &stop).await;
    f.restart().await;
    let mut c = f.client().await;
    assert!(c.allocation_authority(registration.clone()).await.is_err());
    registration.reporting_epoch = 2;
    assert_eq!(
        c.allocation_authority(registration)
            .await
            .unwrap()
            .get_ref()
            .registered_through,
        1
    );
    assert!(c.create(request).await.is_err());
    let mut reader = transport::connect(
        &f.url,
        f.config.host,
        f.tls.ca.pem().as_bytes(),
        f.reader.cert.pem().as_bytes(),
        f.reader.key.serialize_pem().as_bytes(),
    )
    .await
    .unwrap();
    assert_eq!(
        reader
            .allocation_authority(AllocationAuthorityRequest {
                host_id: f.config.host.to_string(),
                reporting_epoch: 2,
                permits_json: Vec::new()
            })
            .await
            .unwrap_err()
            .code(),
        Code::PermissionDenied
    );
    let authority_path = f.config.state_root.join("a/launch.json");
    let saved_path = f.config.state_root.join("a/fixture-saved.json");
    fs::rename(&authority_path, &saved_path).unwrap();
    assert!(c.health(HealthRequest {}).await.is_err());
    fs::rename(&saved_path, &authority_path).unwrap();
    c.health(HealthRequest {}).await.unwrap();
    f.child.kill().unwrap();
    f.child.wait().unwrap();
    let mut config = f.config.clone();
    config.epoch += 1;
    config.launch_permits_required = false;
    assert!(sandbox_supervisor::host::Host::open(config).is_err());
}

#[tokio::test]
#[ignore = "requires root, HUDSON_GUARDIAN_TEST_VM=1 and controlled host artifacts"]
async fn real_host_receipts_require_active_registered_owner() {
    use sandbox_protocol::{allocation_authority::Permit, supervisor::AllocationAuthorityRequest};
    let f = Fixture::configured_permits(true, false, true).await;
    let mut client = f.client().await;
    let request = f.request();
    let owner = request.ownership.unwrap();
    let records = || {
        let saved: serde_json::Value =
            serde_json::from_slice(&fs::read(f.config.state_root.join("host.json")).unwrap())
                .unwrap();
        saved["records"].as_object().unwrap().len()
    };
    for _ in 0..3 {
        assert_eq!(
            client
                .inspect(InspectRequest {
                    ownership: Some(owner.clone())
                })
                .await
                .unwrap_err()
                .code(),
            Code::FailedPrecondition
        );
        assert_eq!(
            client
                .stop(StopRequest {
                    ownership: Some(owner.clone())
                })
                .await
                .unwrap_err()
                .code(),
            Code::FailedPrecondition
        );
    }
    assert_eq!(records(), 0);
    let p = Permit {
        host: owner.host_id.parse().unwrap(),
        project: owner.project_id.parse().unwrap(),
        sandbox: owner.sandbox_id.parse().unwrap(),
        allocation: owner.allocation_id.parse().unwrap(),
        create_operation: owner.operation_id.parse().unwrap(),
        generation: owner.generation,
        original_epoch: owner.supervisor_epoch,
        serial: 1,
    };
    client
        .allocation_authority(AllocationAuthorityRequest {
            host_id: f.config.host.to_string(),
            reporting_epoch: 1,
            permits_json: serde_json::to_vec(std::slice::from_ref(&p)).unwrap(),
        })
        .await
        .unwrap();
    let mut wrong = owner.clone();
    wrong.project_id = sandbox_protocol::ProjectId::generate().to_string();
    assert!(
        client
            .inspect(InspectRequest {
                ownership: Some(wrong)
            })
            .await
            .is_err()
    );
    assert_eq!(records(), 0);
    // Simulate trusted coordinator transitions for an unused permit. This is
    // admission-denial evidence, not a production deletion/absence protocol.
    let authority = sandbox_supervisor::launch_authority::AuthorityFile::open(
        f.config.state_root.join("a"),
        f.config.host,
        1,
        1,
    )
    .unwrap();
    let retirement = OperationId::generate();
    authority.fence(1, &p, retirement).unwrap();
    assert!(
        client
            .inspect(InspectRequest {
                ownership: Some(owner.clone())
            })
            .await
            .is_err()
    );
    let scope = sandbox_protocol::allocation_retirement::Intent {
        version: 1,
        retirement,
        permit: p.clone(),
        commands: sandbox_protocol::allocation_retirement::DomainClosure::Empty {},
        files: sandbox_protocol::allocation_retirement::DomainClosure::Empty {},
        // Synthetic database binding in this component test, not a DB receipt.
        release_evidence_sha256: "a".repeat(64),
        simulated: false,
    };
    authority.complete(1, &scope).unwrap();
    authority.forget(1, &scope).unwrap();
    assert!(
        client
            .inspect(InspectRequest {
                ownership: Some(owner.clone())
            })
            .await
            .is_err()
    );
    assert!(
        client
            .stop(StopRequest {
                ownership: Some(owner)
            })
            .await
            .is_err()
    );
    assert_eq!(records(), 0);
    let next_request = f.request();
    let next_owner = next_request.ownership.unwrap();
    let next = Permit {
        host: next_owner.host_id.parse().unwrap(),
        project: next_owner.project_id.parse().unwrap(),
        sandbox: next_owner.sandbox_id.parse().unwrap(),
        allocation: next_owner.allocation_id.parse().unwrap(),
        create_operation: next_owner.operation_id.parse().unwrap(),
        generation: next_owner.generation,
        original_epoch: next_owner.supervisor_epoch,
        serial: 2,
    };
    client
        .allocation_authority(AllocationAuthorityRequest {
            host_id: f.config.host.to_string(),
            reporting_epoch: 1,
            permits_json: serde_json::to_vec(std::slice::from_ref(&next)).unwrap(),
        })
        .await
        .unwrap();
    assert_eq!(
        client
            .stop(StopRequest {
                ownership: Some(next_owner.clone())
            })
            .await
            .unwrap()
            .get_ref()
            .state,
        AllocationState::FencedAbsent as i32
    );
    assert_eq!(records(), 1);
    authority.fence(1, &next, OperationId::generate()).unwrap();
    // Retained metadata still permits original-owner inspection and cleanup.
    assert_eq!(
        client
            .inspect(InspectRequest {
                ownership: Some(next_owner)
            })
            .await
            .unwrap()
            .get_ref()
            .state,
        AllocationState::FencedAbsent as i32
    );
    assert_eq!(records(), 1);
}

#[path = "support/allocation_retirement.rs"]
mod allocation_retirement;

#[path = "support/metadata_retirement.rs"]
mod metadata_retirement;

#[path = "support/forgetting.rs"]
mod forgetting;
