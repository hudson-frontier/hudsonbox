//! Guest-only init and allocation bootstrap. Never invoke on a host.
use anyhow::{Result, ensure};
use sandbox_protocol::bootstrap::Bootstrap;
use std::{
    fs,
    os::unix::fs::{FileTypeExt, OpenOptionsExt},
    path::Path,
    process::{Command, Stdio},
};

fn mount(source: &str, target: &str, kind: &str) -> Result<()> {
    fs::create_dir_all(target)?;
    rustix::mount::mount(
        source,
        target,
        kind,
        rustix::mount::MountFlags::NOSUID | rustix::mount::MountFlags::NOEXEC,
        None,
    )?;
    Ok(())
}
/// Our init owns the guest boot. Customer commands remain UID 0 in writable userspace.
pub fn init() -> Result<()> {
    ensure!(
        std::process::id() == 1 && rustix::process::geteuid().is_root(),
        "guest boot requires PID 1"
    );
    mount("proc", "/proc", "proc")?;
    mount("sysfs", "/sys", "sysfs")?;
    mount("devtmpfs", "/dev", "devtmpfs")?;
    mount("cgroup2", "/sys/fs/cgroup", "cgroup2")?;
    for path in [
        "/run/hudson",
        "/root",
        "/tmp",
        "/workspace",
        "/sys/fs/cgroup/system",
        "/sys/fs/cgroup/workloads",
    ] {
        fs::create_dir_all(path)?;
    }
    fs::write("/sys/fs/cgroup/cgroup.subtree_control", "+pids")?;
    fs::write("/sys/fs/cgroup/workloads/cgroup.subtree_control", "+pids")?;
    let status = Command::new(std::env::current_exe()?)
        .arg("__boot-namespace")
        .env_clear()
        .stdin(Stdio::null())
        .status()?;
    ensure!(status.success(), "guest agent exited unsuccessfully");
    // The host guardian remains authoritative for VM teardown, including agent death.
    anyhow::bail!("guest agent exited")
}
/// A single-threaded helper separates the agent PID namespace before starting its runtime.
pub fn namespace() -> Result<()> {
    ensure!(
        rustix::process::geteuid().is_root(),
        "guest agent requires root"
    );
    fs::write(
        "/sys/fs/cgroup/system/cgroup.procs",
        std::process::id().to_string(),
    )?;
    // SAFETY: called by the binary before creating threads; FILES is not requested.
    #[allow(unsafe_code)]
    unsafe {
        rustix::thread::unshare_unsafe(
            rustix::thread::UnshareFlags::NEWPID | rustix::thread::UnshareFlags::NEWNS,
        )?;
    }
    rustix::mount::mount_change(
        "/",
        rustix::mount::MountPropagationFlags::PRIVATE | rustix::mount::MountPropagationFlags::REC,
    )?;
    let status = Command::new(std::env::current_exe()?)
        .arg("__serve-bootstrap")
        .env_clear()
        .stdin(Stdio::null())
        .status()?;
    ensure!(status.success(), "guest bootstrap agent failed");
    Ok(())
}
pub fn serve() -> Result<()> {
    ensure!(
        std::process::id() == 1 && rustix::process::geteuid().is_root(),
        "guest agent must be namespace init"
    );
    mount("proc", "/proc", "proc")?;
    let device = fs::OpenOptions::new()
        .read(true)
        .custom_flags((rustix::fs::OFlags::NOFOLLOW | rustix::fs::OFlags::NONBLOCK).bits() as i32)
        .open("/dev/vdb")?;
    ensure!(
        device.metadata()?.file_type().is_block_device(),
        "bootstrap must be the dedicated block device"
    );
    let device = sandbox_protocol::warm_bind::read_device(device)?;
    rustix::process::set_dumpable_behavior(rustix::process::DumpableBehavior::NotDumpable)?;
    let boot_id = fs::read_to_string("/proc/sys/kernel/random/boot_id")?
        .trim()
        .to_owned();
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(async move {
            let mut member_task = None;
            let bootstrap: Bootstrap = match device {
                sandbox_protocol::warm_bind::Device::Allocation(b) => b,
                // An unbound warm member (#188) is inert: no runner, file
                // service or customer process exists until one allocation
                // identity is bound over the pinned member channel.
                sandbox_protocol::warm_bind::Device::Member(member) => {
                    match serve_member(member, boot_id.clone()).await? {
                        Some((b, task)) => { member_task = Some(task); b },
                        None => return Ok(()),
                    }
                }
            };
            let tls = bootstrap.server_tls(crate::runner::now_ms())?;
            let runner = crate::runner::Runner::open(crate::runner::Config {
                state_dir: Path::new("/run/hudson/agent").into(),
                cgroup_root: Path::new("/sys/fs/cgroup/workloads").into(),
                launcher: std::env::current_exe()?,
                context: crate::model::Context {
                    allocation_id: bootstrap.allocation,
                    generation: bootstrap.generation,
                    boot_id,
                },
            })
            .await?;
            let files = crate::file_service::FileService::open(Path::new("/workspace").into(), runner.context().clone()).await?;
            use tokio::signal::unix::{SignalKind, signal};
            let mut stop = signal(SignalKind::terminate())?;
            let mut interrupt = signal(SignalKind::interrupt())?;
            let remaining = bootstrap
                .valid_until_unix_ms
                .saturating_sub(crate::runner::now_ms());
            ensure!(remaining > 0, "guest identity expired during boot");
            let served = tokio::select! {
                result=crate::server::serve_vsock_with_files(runner.clone(),Some(files.clone()),tls,52)=>result,
                _=stop.recv()=>Ok(()),
                _=interrupt.recv()=>Ok(()),
                _=tokio::time::sleep(std::time::Duration::from_millis(remaining as u64))=>Ok(()),
            };
            let files_stopped = files.shutdown().await;
            let commands_stopped = runner.shutdown().await;
            if let Some(task) = member_task { task.abort(); }
            files_stopped?;
            commands_stopped?;
            served
        })
}

/// Start the authenticated member listener before opening any workload service.
/// Keep it alive after activation so lost acknowledgements permit exact retries.
async fn serve_member(
    member: sandbox_protocol::warm_bind::MemberBootstrap,
    boot_id: String,
) -> Result<Option<(Bootstrap, tokio::task::JoinHandle<Result<()>>)>> {
    use sandbox_protocol::{guest_wire, warm_bind};
    use std::sync::Arc;
    use tokio::signal::unix::{SignalKind, signal};
    use tokio_vsock::{VMADDR_CID_ANY, VMADDR_CID_HOST, VsockAddr, VsockListener};
    let tls = member.server_tls(crate::runner::now_ms())?;
    let remaining = member
        .valid_until_unix_ms
        .saturating_sub(crate::runner::now_ms());
    ensure!(remaining > 0, "member identity expired during boot");
    fs::create_dir_all("/var/lib/hudson")?;
    let guest = Arc::new(warm_bind::MemberGuest::open(
        member,
        boot_id,
        Path::new("/var/lib/hudson/member"),
    )?);
    let listener = VsockListener::bind(VsockAddr::new(VMADDR_CID_ANY, warm_bind::MEMBER_PORT))?;
    let capacity = Arc::new(tokio::sync::Semaphore::new(guest_wire::MAX_CONNECTIONS));
    let serving = guest.clone();
    let mut task = tokio::spawn(async move {
        let mut tasks = tokio::task::JoinSet::new();
        let expiry = tokio::time::sleep(std::time::Duration::from_millis(remaining as u64));
        tokio::pin!(expiry);
        loop {
            tokio::select! {
                _ = &mut expiry => return Ok(()),
                _ = tasks.join_next(), if !tasks.is_empty() => {},
                accepted = listener.accept() => {
                    let (stream, peer) = accepted?;
                    if peer.cid() != VMADDR_CID_HOST { continue; }
                    let Ok(permit) = capacity.clone().try_acquire_owned() else { continue };
                    let (guest, tls) = (serving.clone(), tls.clone());
                    tasks.spawn(async move {
                        let _permit = permit;
                        let _ = warm_bind::serve_member_connection(&guest, &tls, stream,
                            crate::runner::now_ms).await;
                    });
                }
            }
        }
    });
    let mut stop = signal(SignalKind::terminate())?;
    let mut interrupt = signal(SignalKind::interrupt())?;
    tokio::select! {
        b = guest.wait_bound() => Ok(Some((b, task))),
        result = &mut task => { result??; Ok(None) },
        _ = stop.recv() => { task.abort(); Ok(None) },
        _ = interrupt.recv() => { task.abort(); Ok(None) },
    }
}
