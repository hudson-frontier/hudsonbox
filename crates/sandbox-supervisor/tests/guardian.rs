//! Controlled root-only Firecracker tests on the dedicated development host.
#![cfg(target_os = "linux")]
#![allow(clippy::unwrap_used, clippy::expect_used)]
#[path = "support/vm.rs"]
mod vm;
use sandbox_protocol::{Id, OperationId, ProjectId};
use sandbox_supervisor::guardian::{self, Action, Receipt, State};
use std::{
    fs,
    path::{Path, PathBuf},
    process::Command,
    time::{Duration, Instant},
};
use vm::{Fixture, artifact};

#[test]
#[ignore = "requires root, HUDSON_GUARDIAN_TEST_VM=1 and verified native Firecracker artifacts"]
fn real_vm_stop_renewal_fences_and_exact_retry() {
    let f = Fixture::new(5000);
    let mut child = f.spawn();
    let running = f.running(&mut child);
    assert!(f.populated());
    assert!(f.manifest.reconcile("live_recovery_must_fail").is_err());
    assert_eq!(
        fs::read_to_string(f.manifest.group().join("cpu.max"))
            .unwrap()
            .trim(),
        "100000 100000"
    );
    assert_eq!(
        fs::read_to_string(f.manifest.group().join("memory.max"))
            .unwrap()
            .trim(),
        "268435456"
    );
    assert_eq!(
        fs::read_to_string(f.manifest.group().join("pids.max"))
            .unwrap()
            .trim(),
        "64"
    );
    assert_eq!(
        fs::metadata(f.manifest.jail_root().join("rootfs.ext4"))
            .unwrap()
            .len(),
        64 << 20
    );
    let repeated = Command::new(env!("CARGO_BIN_EXE_sandbox-supervisor"))
        .arg("--manifest")
        .arg(&f.path)
        .arg("run")
        .output()
        .unwrap();
    assert!(
        repeated.status.success(),
        "{}",
        String::from_utf8_lossy(&repeated.stderr)
    );
    let receipt: Receipt = serde_json::from_slice(&repeated.stdout).unwrap();
    assert_eq!(receipt.guardian_host_pid, running.guardian_host_pid);
    let until = guardian::wall_ms() + 8000;
    let renewed = guardian::control(
        &f.manifest,
        Action::Renew {
            revision: 1,
            expires_unix_ms: until,
        },
    )
    .unwrap();
    assert!(renewed.error.is_none());
    assert_eq!(renewed.receipt.unwrap().expires_unix_ms, until);
    assert!(
        guardian::control(
            &f.manifest,
            Action::Renew {
                revision: 1,
                expires_unix_ms: until
            }
        )
        .unwrap()
        .error
        .is_none()
    );
    assert!(
        guardian::control(
            &f.manifest,
            Action::Renew {
                revision: 1,
                expires_unix_ms: until + 1000
            }
        )
        .unwrap()
        .error
        .is_some()
    );
    assert!(
        guardian::control(
            &f.manifest,
            Action::Renew {
                revision: 0,
                expires_unix_ms: until
            }
        )
        .unwrap()
        .error
        .is_some()
    );
    let mut wrong = f.manifest.clone();
    wrong.start.owner.project = ProjectId::generate();
    assert!(
        guardian::control(&wrong, Action::Stop)
            .unwrap()
            .error
            .is_some()
    );
    let wait = (f.manifest.start.expires_unix_ms - guardian::wall_ms() + 100).max(0) as u64;
    std::thread::sleep(Duration::from_millis(wait));
    assert!(f.populated(), "renewed VM died at original deadline");
    let _ = guardian::control(&f.manifest, Action::Stop);
    assert!(child.wait().unwrap().success());
    let stopped = f.stopped();
    assert_eq!(stopped.reason.as_deref(), Some("guardian_stopped"));
    assert_eq!(f.manifest.prepare().unwrap().state, State::Stopped);
    assert!(
        guardian::control(
            &f.manifest,
            Action::Renew {
                revision: 2,
                expires_unix_ms: guardian::wall_ms() + 10000
            }
        )
        .is_err()
    );
}
#[test]
#[ignore = "requires root, HUDSON_GUARDIAN_TEST_VM=1 and verified native Firecracker artifacts"]
fn supervisor_loss_does_not_disable_lease_expiry() {
    let f = Fixture::new(4000);
    let mut child = f.spawn();
    f.running(&mut child);
    child.kill().unwrap();
    child.wait().unwrap();
    std::thread::sleep(Duration::from_millis(100));
    assert!(f.populated());
    f.wait_empty();
    assert!(
        f.manifest.group().exists(),
        "test must observe kernel cleanup before reconciliation"
    );
    let r = f
        .manifest
        .reconcile("lost_supervisor_after_expiry")
        .unwrap();
    assert_eq!(r.state, State::Stopped);
    f.stopped();
}
#[test]
#[ignore = "requires root, HUDSON_GUARDIAN_TEST_VM=1 and verified native Firecracker artifacts"]
fn killing_namespace_guardian_kills_vm_without_a_surviving_supervisor() {
    let f = Fixture::new(20000);
    let mut child = f.spawn();
    let r = f.running(&mut child);
    child.kill().unwrap();
    child.wait().unwrap();
    let pid = r.guardian_host_pid.unwrap();
    let stat = fs::read_to_string(format!("/proc/{pid}/stat")).unwrap();
    let ticks = stat
        .rsplit_once(") ")
        .unwrap()
        .1
        .split_whitespace()
        .nth(19)
        .unwrap()
        .parse::<u64>()
        .unwrap();
    assert_eq!(Some(ticks), r.guardian_start_ticks);
    assert_eq!(
        fs::read_link(format!("/proc/{pid}/exe")).unwrap(),
        PathBuf::from(env!("CARGO_BIN_EXE_sandbox-supervisor"))
    );
    rustix::process::kill_process(
        rustix::process::Pid::from_raw(pid as i32).unwrap(),
        rustix::process::Signal::KILL,
    )
    .unwrap();
    f.wait_empty();
    assert!(f.manifest.group().exists());
    assert_eq!(
        f.manifest.reconcile("guardian_sigkill").unwrap().state,
        State::Stopped
    );
    f.stopped();
}
#[test]
#[ignore = "requires root, HUDSON_GUARDIAN_TEST_VM=1 and verified native Firecracker artifacts"]
fn staging_digest_failure_and_recovery_fence_prevent_delayed_launch() {
    let mut f = Fixture::new(20000);
    f.manifest.config.rootfs.sha256 = "0".repeat(64);
    assert!(f.manifest.prepare().is_err());
    assert_eq!(f.record().state, State::Stopped);
    assert!(!f.manifest.directory().join("run").exists());
    let f = Fixture::new(20000);
    assert_eq!(f.manifest.prepare().unwrap().state, State::Prepared);
    assert_eq!(
        f.manifest
            .reconcile("recovery_before_launch")
            .unwrap()
            .state,
        State::Stopped
    );
    let mut child = f.spawn();
    assert!(child.wait().unwrap().success());
    assert!(f.record().guardian_host_pid.is_none());
    assert!(!f.manifest.group().exists());
}

#[test]
#[ignore = "requires root, HUDSON_GUARDIAN_TEST_VM=1 and verified native Firecracker artifacts"]
fn stalled_control_clients_do_not_disable_expiry() {
    use std::io::Write;
    use std::os::unix::net::UnixStream;
    let f = Fixture::new(4000);
    let mut child = f.spawn();
    f.running(&mut child);
    let mut clients = Vec::new();
    for _ in 0..8 {
        let mut client = UnixStream::connect(f.manifest.socket()).unwrap();
        client.write_all(&65536u32.to_be_bytes()).unwrap();
        clients.push(client);
    }
    // Retaining incomplete frames cannot extend the host deadline. The outer
    // wrapper is still present here and must persist expiry plus cleanup.
    assert!(child.wait().unwrap().success());
    let stopped = f.stopped();
    assert_eq!(stopped.reason.as_deref(), Some("lease_expired"));
    assert!(guardian::wall_ms() >= f.manifest.start.expires_unix_ms);
    assert!(guardian::wall_ms() < f.manifest.start.expires_unix_ms + 5000);
}
#[test]
#[ignore = "requires root, HUDSON_GUARDIAN_TEST_VM=1 and verified native Firecracker artifacts"]
fn concurrent_stop_and_renewal_never_revive_the_vm() {
    let f = Fixture::new(20000);
    let mut child = f.spawn();
    f.running(&mut child);
    let barrier = std::sync::Arc::new(std::sync::Barrier::new(3));
    std::thread::scope(|scope| {
        for action in [
            Action::Stop,
            Action::Renew {
                revision: 1,
                expires_unix_ms: guardian::wall_ms() + 30000,
            },
        ] {
            let barrier = barrier.clone();
            let manifest = &f.manifest;
            scope.spawn(move || {
                barrier.wait();
                // Transport loss after stop is uncertain, never proof of renewal.
                let _ = guardian::control(manifest, action);
            });
        }
        barrier.wait();
    });
    f.stopped();
    assert!(child.wait().unwrap().success());
    assert_eq!(f.manifest.prepare().unwrap().state, State::Stopped);
}
#[test]
#[ignore = "requires root, HUDSON_GUARDIAN_TEST_VM=1 and verified native Firecracker artifacts"]
fn recovery_of_launch_intent_fences_before_any_new_spawn() {
    let f = Fixture::new(20000);
    let mut receipt = f.manifest.prepare().unwrap();
    receipt.state = State::LaunchIntent;
    // Simulate a crash after durable intent, before starting the jailer.
    fs::write(
        f.manifest.directory().join("receipt.json"),
        serde_json::to_vec(&receipt).unwrap(),
    )
    .unwrap();
    let mut child = f.spawn();
    assert!(child.wait().unwrap().success());
    let stopped = f.stopped();
    assert!(stopped.guardian_host_pid.is_none());
    assert_eq!(stopped.reason.as_deref(), Some("guardian_recovery_fence"));
}

#[test]
#[ignore = "requires root, HUDSON_GUARDIAN_TEST_VM=1, release guest binary and native runtime artifacts"]
fn bootstrapped_guest_executes_only_after_durable_binding() {
    use sandbox_protocol::guest::Stream;
    use sandbox_protocol::guest_model::Execute;
    let f = Fixture::build(30000, true);
    let source_digest = f.manifest.config.rootfs.sha256.clone();
    let prepared = f.manifest.prepare().unwrap();
    assert_eq!(
        f.manifest.prepare().unwrap().identity_digest,
        prepared.identity_digest
    );
    let mut child = f.spawn();
    let running = f.running(&mut child);
    assert!(running.guest_boot_id.is_none());
    assert!(f.manifest.guest_client().is_err());
    let ready = f.ready();
    assert_eq!(f.record().guest_boot_id, ready.guest_boot_id);
    assert_eq!(
        guardian::control(&f.manifest, Action::BindGuest)
            .unwrap()
            .receipt
            .unwrap()
            .guest_boot_id,
        ready.guest_boot_id
    );
    assert_eq!(f.record().identity_digest, running.identity_digest);
    assert_eq!(
        artifact(f.manifest.config.rootfs.path.clone()).sha256,
        source_digest
    );
    let client = f.manifest.guest_client().unwrap();
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        let request = Execute {
            operation_id: OperationId::generate(),
            argv: vec![
                "/bin/busybox".into(),
                "sh".into(),
                "-c".into(),
                "if printf nope >/dev/vdb; then exit 99; fi; echo once >> /root/once; /bin/busybox id -u; /bin/busybox cat /root/once; exit 9"
                    .into(),
            ],
            env: Default::default(),
            cwd: "/root".into(),
            deadline_unix_ms: guardian::wall_ms() + 10000,
            output_limit: 4096,
        };
        client.execute(&request).await.unwrap();
        let until = Instant::now() + Duration::from_secs(8);
        let receipt = loop {
            let receipt = client.inspect(request.operation_id).await.unwrap();
            if receipt.state.terminal() {
                break receipt;
            }
            assert!(Instant::now() < until);
            tokio::time::sleep(Duration::from_millis(20)).await;
        };
        assert_eq!(receipt.exit_code, Some(9));
        assert!(receipt.cleanup_confirmed);
        assert_eq!(client.execute(&request).await.unwrap().exit_code, Some(9));
        let output = client
            .output(sandbox_protocol::guest::ReadOutput {
                operation_id: request.operation_id.to_string(),
                stream: Stream::Stdout as i32,
                offset: 0,
                limit: 4096,
            })
            .await
            .unwrap();
        assert_eq!(output.data, b"0\nonce\n");
        assert!(output.complete);
        let mut changed = request.clone();
        changed.argv.push("different".into());
        assert!(client.execute(&changed).await.is_err());
    });
    let _ = guardian::control(&f.manifest, Action::Stop);
    assert!(child.wait().unwrap().success());
    f.stopped();
    assert!(f.manifest.guest_client().is_err());
}
#[test]
#[ignore = "requires root, HUDSON_GUARDIAN_TEST_VM=1 and verified native Firecracker artifacts"]
fn changed_bootstrap_is_fenced_before_launch() {
    let f = Fixture::new(20000);
    let prepared = f.manifest.prepare().unwrap();
    assert!(prepared.identity_digest.is_some());
    let path = f.manifest.jail_root().join("bootstrap.img");
    let mut changed = fs::read(&path).unwrap();
    changed[16] ^= 1; // Preserve device length; exercise content verification.
    fs::write(path, changed).unwrap();
    let mut child = f.spawn();
    assert!(child.wait().unwrap().success());
    let stopped = f.stopped();
    assert!(stopped.firecracker_namespace_pid.is_none());
}

#[test]
#[ignore = "requires root, HUDSON_GUARDIAN_TEST_VM=1, release guest binary and native runtime artifacts"]
fn shared_base_image_keeps_allocation_credentials_separate() {
    use sandbox_supervisor::{guest::GuestClient, identity::Identity};
    let first = Fixture::build(30000, true);
    let mut first_process = first.spawn();
    first.running(&mut first_process);
    let first_ready = first.ready();
    let first_identity: Identity =
        guardian::read_json(&first.manifest.directory().join("run/identity.json")).unwrap();
    let mut second = Fixture::new(30000);
    second.manifest.config.rootfs = first.manifest.config.rootfs.clone();
    fs::write(&second.path, serde_json::to_vec(&second.manifest).unwrap()).unwrap();
    let mut second_process = second.spawn();
    second.running(&mut second_process);
    let second_ready = second.ready();
    assert_ne!(first_ready.identity_digest, second_ready.identity_digest);
    assert_ne!(first_ready.guest_boot_id, second_ready.guest_boot_id);
    assert_eq!(
        first.manifest.config.rootfs.sha256,
        second.manifest.config.rootfs.sha256
    );
    let context = sandbox_protocol::guest_model::Context {
        allocation_id: second.manifest.start.owner.allocation,
        generation: 1,
        boot_id: second_ready.guest_boot_id.unwrap(),
    };
    let wrong = GuestClient::from_firecracker_directory(
        second.manifest.jail_root(),
        52,
        first_identity.client_tls(guardian::wall_ms()).unwrap(),
        context.clone(),
    )
    .unwrap();
    let right = second.manifest.guest_client().unwrap();
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            assert!(wrong.hello().await.is_err());
            assert_eq!(right.hello().await.unwrap(), context);
        });
    let _ = guardian::control(&second.manifest, Action::Stop);
    assert!(second_process.wait().unwrap().success());
    second.stopped();
    assert!(first.populated());
    let _ = guardian::control(&first.manifest, Action::Stop);
    assert!(first_process.wait().unwrap().success());
    first.stopped();
}

#[path = "support/frozen_fs.rs"]
mod frozen_fs;

fn cpu_usage(group: &Path) -> u64 {
    fs::read_to_string(group.join("cpu.stat"))
        .unwrap()
        .lines()
        .find_map(|s| s.strip_prefix("usage_usec "))
        .unwrap()
        .parse()
        .unwrap()
}
fn task_identity(pid: u32) -> Option<(String, u64, u64)> {
    let stat = fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
    let fields: Vec<_> = stat.rsplit_once(") ")?.1.split_whitespace().collect();
    Some((
        fields[0].into(),
        fields[6].parse().ok()?,
        fields[19].parse().ok()?,
    ))
}
#[test]
#[ignore = "requires root, HUDSON_GUARDIAN_TEST_VM=1, loopback ext4/fsfreeze and native runtime artifacts"]
fn journal_stall_cannot_keep_vm_executing_past_expiry() {
    let disk = frozen_fs::Filesystem::new();
    let mut f = Fixture::new(15000);
    f.manifest.config.state_root = disk.path().join("s");
    fs::write(&f.path, serde_json::to_vec(&f.manifest).unwrap()).unwrap();
    let mut child = f.spawn();
    let running = f.running(&mut child);
    // The fixed init spins indefinitely; allow boot to finish before faulting metadata.
    std::thread::sleep(Duration::from_secs(3));
    let busy_before = cpu_usage(&f.manifest.group());
    std::thread::sleep(Duration::from_millis(250));
    let busy_delta = cpu_usage(&f.manifest.group()) - busy_before;
    assert!(busy_delta > 50000, "VM must be executing before the fault");
    let vcpus: Vec<_> = fs::read_to_string(f.manifest.group().join("cgroup.threads"))
        .unwrap()
        .lines()
        .filter_map(|s| s.parse::<u32>().ok())
        .filter_map(|pid| {
            let name = fs::read_to_string(format!("/proc/{pid}/comm")).ok()?;
            if name.starts_with("fc_vcpu") {
                Some((pid, task_identity(pid)?.2))
            } else {
                None
            }
        })
        .collect();
    assert!(!vcpus.is_empty());
    let mut frozen = disk.freeze();
    let manifest = f.manifest.clone();
    let request = std::thread::spawn(move || {
        guardian::control(
            &manifest,
            Action::Renew {
                revision: 1,
                expires_unix_ms: guardian::wall_ms() + 30000,
            },
        )
    });
    let guardian_pid = running.guardian_host_pid.unwrap();
    let until = Instant::now() + Duration::from_secs(3);
    let blocked = loop {
        let stalled = fs::read_dir(format!("/proc/{guardian_pid}/task"))
            .unwrap()
            .filter_map(Result::ok)
            .filter_map(|entry| entry.file_name().to_str()?.parse::<u32>().ok())
            .find(|pid| task_identity(*pid).is_some_and(|(state, _, _)| state == "D"));
        if let Some(pid) = stalled {
            break pid;
        }
        assert!(
            Instant::now() < until,
            "journal write did not enter uninterruptible wait"
        );
        std::thread::sleep(Duration::from_millis(10));
    };
    assert!(
        request.join().unwrap().is_err(),
        "stalled renewal must not be acknowledged"
    );
    let remaining = (f.manifest.start.expires_unix_ms - guardian::wall_ms() + 500).max(0) as u64;
    std::thread::sleep(Duration::from_millis(remaining));
    let after_start = cpu_usage(&f.manifest.group());
    std::thread::sleep(Duration::from_millis(250));
    let after_delta = cpu_usage(&f.manifest.group()) - after_start;
    // PF_EXITING (Linux sched.h) means the original task cannot return to KVM.
    // A recycled PID is not the original vCPU. Cleanup can still be pending.
    let live_vcpus = vcpus
        .iter()
        .filter(|(pid, ticks)| {
            task_identity(*pid)
                .is_some_and(|(_, flags, current)| current == *ticks && flags & 4 == 0)
        })
        .count();
    let retained = f.record();
    eprintln!(
        "journal_stall_observation {}",
        serde_json::json!({
            "blocked_guardian_thread":blocked,"busy_cpu_delta_us":busy_delta,
            "post_expiry_cpu_delta_us":after_delta,"live_original_vcpus":live_vcpus,
            "cleanup_confirmed_before_thaw":retained.cleanup_confirmed,
        })
    );
    // Restore the exclusively owned filesystem before any assertion can unwind
    // through allocation cleanup. The independent helper is still armed here.
    frozen.thaw();
    assert!(child.wait().unwrap().success());
    f.stopped();
    assert_eq!(f.manifest.prepare().unwrap().state, State::Stopped);
    assert!(!retained.cleanup_confirmed);
    assert_eq!(
        live_vcpus, 0,
        "vCPU survived expiry while guardian journal was blocked"
    );
    assert!(after_delta < 10000, "VM still consumed CPU after expiry");
}
