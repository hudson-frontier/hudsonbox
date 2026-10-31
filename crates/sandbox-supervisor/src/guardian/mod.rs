//! Root-owned Linux allocation staging, fencing and cleanup. No customer host commands.
mod process;
pub(crate) mod retirement;
use anyhow::{Context as _, Result, ensure};
pub use process::{control, launch, namespace_init};
use sandbox_protocol::{AllocationId, HostId, Id, OperationId, ProjectId, SandboxId};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use sha2::{Digest, Sha256};
use std::{
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt, PermissionsExt},
    path::{Path, PathBuf},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Owner {
    pub host: HostId,
    pub project: ProjectId,
    pub sandbox: SandboxId,
    pub allocation: AllocationId,
    pub create_operation: OperationId,
    pub generation: i64,
    pub epoch: i64,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Artifact {
    pub path: PathBuf,
    pub sha256: String,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub state_root: PathBuf,
    pub cgroup_parent: PathBuf,
    pub firecracker: Artifact,
    pub jailer: Artifact,
    pub kernel: Artifact,
    pub rootfs: Artifact,
    pub jail_uid: u32,
    pub jail_gid: u32,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Start {
    pub owner: Owner,
    pub vcpu: u32,
    pub memory_mib: u64,
    pub disk_mib: u64,
    pub expires_unix_ms: i64,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Manifest {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub launch_permit: Option<sandbox_protocol::allocation_authority::Permit>,
    pub config: Config,
    pub start: Start,
    /// Present for a warm member (#188). The guardian directory, jail, cgroup
    /// and vsock channel stay keyed by `slot` for the VM's whole life.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub member: Option<Member>,
}
/// Warm member key. An inert manifest has `bound_from: None` and an owner
/// derived by [`Owner::inert`]; the host's view of an adopted member is the
/// same manifest re-owned by the confirmed [`Binding`] (see [`Manifest::bound`]).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Member {
    pub member: uuid::Uuid,
    pub slot: AllocationId,
    /// Idle life of the inert member; renewals never pass it.
    pub valid_until_unix_ms: i64,
    /// Digest of the inert manifest the guardian runs, for an adopted view.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bound_from: Option<String>,
}
/// Durable allocation ownership of an adopted warm member, recorded by the
/// member's own guardian. `confirmed` only after the guest acknowledged the
/// exact identity and the allocation channel authenticated the member boot.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Binding {
    pub owner: Owner,
    pub permit: Option<sandbox_protocol::allocation_authority::Permit>,
    pub intent_sha256: String,
    /// SHA-256 of the host-side allocation identity file.
    pub identity_digest: String,
    /// [`sandbox_protocol::warm_bind::identity_digest`] echoed by the guest.
    pub guest_identity_sha256: String,
    pub identity_expires_unix_ms: i64,
    pub confirmed: bool,
}
/// Host request to bind a running inert member to one allocation.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Adoption {
    pub owner: Owner,
    pub permit: Option<sandbox_protocol::allocation_authority::Permit>,
    pub intent: sandbox_protocol::warm_pool::HandoffIntent,
    pub expires_unix_ms: i64,
}
impl Owner {
    /// Placeholder owner of an unbound member: no project, sandbox or
    /// operation exists, so they are nil and the allocation is the slot.
    pub fn inert(host: HostId, member: uuid::Uuid, slot: AllocationId, epoch: i64) -> Self {
        Self {
            host,
            project: ProjectId::from_uuid(uuid::Uuid::nil()),
            sandbox: SandboxId::from_uuid(uuid::Uuid::nil()),
            allocation: slot,
            create_operation: OperationId::from_uuid(member),
            generation: 1,
            epoch,
        }
    }
}
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum State {
    Staging,
    Prepared,
    LaunchIntent,
    Running,
    Stopping,
    Fenced,
    Stopped,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Receipt {
    pub version: u32,
    pub owner: Owner,
    pub digest: String,
    pub host_boot_id: String,
    pub state: State,
    pub cleanup_confirmed: bool,
    pub renewal_revision: u64,
    pub expires_unix_ms: i64,
    pub guardian_host_pid: Option<u32>,
    pub guardian_start_ticks: Option<u64>,
    pub firecracker_namespace_pid: Option<u32>,
    pub cgroup_inode: Option<u64>,
    pub reason: Option<String>,
    #[serde(default)]
    pub identity_digest: Option<String>,
    #[serde(default)]
    pub identity_expires_unix_ms: Option<i64>,
    #[serde(default)]
    pub guest_boot_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub binding: Option<Binding>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub enum Action {
    Inspect,
    BindGuest,
    Renew {
        revision: u64,
        expires_unix_ms: i64,
    },
    Stop,
    /// Authenticate an unbound member's endpoint and persist its boot.
    MemberHello,
    /// Durably fence, then bind the member to one allocation (#188).
    Adopt(Box<Adoption>),
}
/// Guardian error text for a definitive adoption refusal. Any other error is
/// uncertain and leaves the durable binding fence in place.
pub const ADOPTION_REFUSED: &str = "warm_adoption_refused";
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Request {
    pub owner: Owner,
    pub action: Action,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Response {
    pub receipt: Option<Receipt>,
    pub error: Option<String>,
}

#[derive(Debug, thiserror::Error)]
#[error("allocation guardian is still active")]
struct OwnershipBusy;

pub(crate) fn is_ownership_busy(error: &anyhow::Error) -> bool {
    error.chain().any(|cause| cause.is::<OwnershipBusy>())
}

pub fn wall_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|t| i64::try_from(t.as_millis()).unwrap_or(i64::MAX))
        .unwrap_or(0)
}
pub fn boot_ms() -> u64 {
    let t = rustix::time::clock_gettime(rustix::time::ClockId::Boottime);
    (t.tv_sec as u64)
        .saturating_mul(1000)
        .saturating_add((t.tv_nsec as u64) / 1_000_000)
}
fn boot_id() -> Result<String> {
    Ok(fs::read_to_string("/proc/sys/kernel/random/boot_id")?
        .trim()
        .into())
}
fn root() -> Result<()> {
    ensure!(
        rustix::process::geteuid().is_root(),
        "allocation guardian requires root"
    );
    Ok(())
}
pub fn read_json<T: DeserializeOwned>(path: &Path) -> Result<T> {
    let mut data = Vec::new();
    let file = OpenOptions::new()
        .read(true)
        .custom_flags((rustix::fs::OFlags::NOFOLLOW | rustix::fs::OFlags::NONBLOCK).bits() as i32)
        .open(path)?;
    let metadata = file.metadata()?;
    ensure!(
        metadata.is_file() && metadata.len() <= 65536,
        "invalid guardian metadata file"
    );
    file.take(65537).read_to_end(&mut data)?;
    ensure!(data.len() <= 65536, "guardian metadata too large");
    serde_json::from_slice(&data).map_err(|_| anyhow::anyhow!("invalid guardian metadata"))
}
fn write_json(path: &Path, value: &impl Serialize) -> Result<()> {
    let parent = path.parent().context("missing metadata parent")?;
    let temp = parent.join(format!("{}.tmp", OperationId::generate()));
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&temp)?;
    let bytes = serde_json::to_vec(value)?;
    ensure!(bytes.len() <= 65536, "guardian metadata too large");
    file.write_all(&bytes)?;
    file.sync_all()?;
    fs::rename(&temp, path)?;
    File::open(parent)?.sync_all()?;
    Ok(())
}
fn private_dir(path: &Path) -> Result<()> {
    match fs::DirBuilder::new().mode(0o700).create(path) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {}
        Err(e) => return Err(e.into()),
    }
    let m = fs::symlink_metadata(path)?;
    ensure!(
        m.is_dir() && m.uid() == 0 && m.mode() & 0o077 == 0,
        "guardian directory must be private and root-owned"
    );
    Ok(())
}
impl Manifest {
    fn authorize_launch(&self) -> Result<crate::launch_authority::LaunchGuard> {
        if self.is_inert() {
            // An inert member holds no allocation permit. The shared gate still
            // serializes its launch with exclusive retirement fences.
            return crate::launch_authority::authorize_member(&self.config.state_root);
        }
        if let Some(p) = &self.launch_permit {
            let o = &self.start.owner;
            ensure!(
                p.host == o.host
                    && p.project == o.project
                    && p.sandbox == o.sandbox
                    && p.allocation == o.allocation
                    && p.create_operation == o.create_operation
                    && p.generation == o.generation
                    && p.original_epoch == o.epoch,
                "manifest launch permit ownership mismatch"
            );
        }
        crate::launch_authority::authorize(&self.config.state_root, self.launch_permit.as_ref())
    }
    /// Key of the guardian directory, cgroup and jail: the member slot for a
    /// warm member (inert or adopted), else the allocation.
    pub fn key(&self) -> AllocationId {
        self.member
            .as_ref()
            .map_or(self.start.owner.allocation, |m| m.slot)
    }
    pub fn is_inert(&self) -> bool {
        self.member.as_ref().is_some_and(|m| m.bound_from.is_none())
    }
    fn is_bound_view(&self) -> bool {
        self.member.as_ref().is_some_and(|m| m.bound_from.is_some())
    }
    /// Owner and digest the running guardian's receipt carries.
    pub(crate) fn receipt_identity(&self) -> Result<(Owner, String)> {
        match &self.member {
            Some(Member {
                member,
                slot,
                bound_from: Some(digest),
                ..
            }) => Ok((
                Owner::inert(
                    self.start.owner.host,
                    *member,
                    *slot,
                    self.start.owner.epoch,
                ),
                digest.clone(),
            )),
            _ => Ok((self.start.owner.clone(), self.digest()?)),
        }
    }
    /// The host's allocation view of an adopted inert member.
    pub fn bound(&self, binding: &Binding) -> Result<Self> {
        ensure!(
            self.is_inert() && binding.confirmed,
            "not a confirmed member binding"
        );
        let mut view = self.clone();
        view.launch_permit = binding.permit.clone();
        view.start.owner = binding.owner.clone();
        if let Some(m) = view.member.as_mut() {
            m.bound_from = Some(self.digest()?);
        }
        Ok(view)
    }
    fn authorize_cleanup(&self) -> Result<(crate::launch_authority::LaunchGuard, bool)> {
        if self.is_inert() {
            return Ok((
                crate::launch_authority::authorize_member(&self.config.state_root)?,
                true,
            ));
        }
        crate::launch_authority::authorize_cleanup(
            &self.config.state_root,
            self.launch_permit.as_ref(),
        )
    }
    pub fn directory(&self) -> PathBuf {
        self.config.state_root.join(self.key().to_string())
    }
    pub fn group(&self) -> PathBuf {
        self.config
            .cgroup_parent
            .join(self.key().uuid().to_string())
    }
    pub fn jail_root(&self) -> PathBuf {
        self.directory()
            .join("run/chroots/firecracker")
            .join(self.key().uuid().to_string())
            .join("root")
    }
    pub fn socket(&self) -> PathBuf {
        self.directory().join("control.sock")
    }
    fn record_path(&self) -> PathBuf {
        self.directory().join("receipt.json")
    }
    fn digest(&self) -> Result<String> {
        Ok(hex::encode(Sha256::digest(serde_json::to_vec(self)?)))
    }
    pub(crate) fn validate(&self) -> Result<()> {
        root()?;
        if let Some(p) = &self.launch_permit {
            let o = &self.start.owner;
            ensure!(
                p.host == o.host
                    && p.project == o.project
                    && p.sandbox == o.sandbox
                    && p.allocation == o.allocation
                    && p.create_operation == o.create_operation
                    && p.generation == o.generation
                    && p.original_epoch == o.epoch,
                "manifest launch permit ownership mismatch"
            );
        }
        ensure!(
            self.start.owner.generation > 0 && self.start.owner.epoch > 0,
            "invalid allocation ownership"
        );
        if let Some(m) = &self.member {
            let o = &self.start.owner;
            ensure!(
                !m.member.is_nil() && !m.slot.uuid().is_nil() && m.valid_until_unix_ms > 0,
                "invalid warm member key"
            );
            match &m.bound_from {
                None => ensure!(
                    self.launch_permit.is_none()
                        && *o == Owner::inert(o.host, m.member, m.slot, o.epoch),
                    "inert member must not name allocation ownership"
                ),
                Some(digest) => ensure!(
                    digest.len() == 64
                        && digest
                            .bytes()
                            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
                        && o.allocation != m.slot
                        && !o.project.uuid().is_nil()
                        && !o.sandbox.uuid().is_nil(),
                    "invalid adopted member view"
                ),
            }
        }
        ensure!(
            sandbox_protocol::resources::supported(
                self.start.vcpu.into(),
                self.start.memory_mib.into(),
                self.start.disk_mib.into()
            ),
            "invalid VM resource bounds"
        );
        ensure!(
            self.config.jail_uid >= 65534 && self.config.jail_gid >= 65534,
            "jailer must use an unprivileged UID/GID"
        );
        ensure!(
            self.config.state_root.is_absolute()
                && self
                    .config
                    .state_root
                    .components()
                    .all(|c| !matches!(c, std::path::Component::ParentDir)),
            "invalid state root"
        );
        ensure!(
            self.config.cgroup_parent.starts_with("/sys/fs/cgroup/")
                && self.config.cgroup_parent != Path::new("/sys/fs/cgroup")
                && self
                    .config
                    .cgroup_parent
                    .components()
                    .all(|c| !matches!(c, std::path::Component::ParentDir)),
            "dedicated cgroup parent required"
        );
        ensure!(
            self.socket().as_os_str().len() < 100,
            "guardian Unix socket path too long"
        );
        for artifact in [
            &self.config.firecracker,
            &self.config.jailer,
            &self.config.kernel,
            &self.config.rootfs,
        ] {
            ensure!(
                artifact.path.is_absolute()
                    && artifact.sha256.len() == 64
                    && artifact
                        .sha256
                        .bytes()
                        .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)),
                "invalid artifact path or digest"
            );
        }
        ensure!(
            serde_json::to_vec(self)?.len() <= 65536,
            "manifest too large"
        );
        Ok(())
    }
    fn lock(&self) -> Result<File> {
        self.lifecycle_lock(true)
    }
    fn lifecycle_lock(&self, create: bool) -> Result<File> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(create)
            .truncate(false)
            .mode(0o600)
            .custom_flags(
                (rustix::fs::OFlags::NOFOLLOW | rustix::fs::OFlags::NONBLOCK).bits() as i32,
            )
            .open(self.directory().join("lifecycle.lock"))?;
        match rustix::fs::flock(&file, rustix::fs::FlockOperation::NonBlockingLockExclusive) {
            Ok(()) => {}
            Err(rustix::io::Errno::WOULDBLOCK) => return Err(OwnershipBusy.into()),
            Err(e) => return Err(e.into()),
        }
        Ok(file)
    }
    pub fn receipt(&self) -> Result<Receipt> {
        let r: Receipt = read_json(&self.record_path())?;
        self.validate_receipt(&r)?;
        Ok(r)
    }
    pub(crate) fn validate_receipt(&self, r: &Receipt) -> Result<()> {
        let (owner, digest) = self.receipt_identity()?;
        ensure!(
            self.member.is_some() || r.binding.is_none(),
            "binding on a cold allocation receipt"
        );
        if self.is_bound_view() {
            ensure!(
                r.binding
                    .as_ref()
                    .is_some_and(|b| b.confirmed && b.owner == self.start.owner),
                "adopted member binding mismatch"
            );
        }
        ensure!(
            r.version == 1
                && r.owner == owner
                && r.digest == digest
                && r.reason.as_ref().is_none_or(|s| s.len() < 256)
                && r.cleanup_confirmed == (r.state == State::Stopped),
            "guardian receipt ownership or shape mismatch"
        );
        Ok(())
    }
    /// Stage once under ownership. A returned existing receipt never causes a second launch.
    pub fn prepare(&self) -> Result<Receipt> {
        self.validate()?;
        ensure!(
            !self.is_bound_view(),
            "an adopted member view never launches"
        );
        let _authority = self.authorize_launch()?;
        private_dir(&self.config.state_root)?;
        private_dir(&self.directory())?;
        let _lock = self.lock()?;
        if self.record_path().exists() {
            return self.receipt();
        }
        ensure!(
            !self.group().exists(),
            "unowned allocation cgroup already exists"
        );
        ensure!(
            fs::read_dir(self.directory())?.count() == 1,
            "unowned allocation files already exist"
        );
        crate::lease::Deadline::new(self.start.expires_unix_ms, wall_ms(), boot_ms())?;
        let mut record = Receipt {
            version: 1,
            owner: self.start.owner.clone(),
            digest: self.digest()?,
            host_boot_id: boot_id()?,
            state: State::Staging,
            cleanup_confirmed: false,
            renewal_revision: 0,
            expires_unix_ms: self.start.expires_unix_ms,
            guardian_host_pid: None,
            guardian_start_ticks: None,
            firecracker_namespace_pid: None,
            cgroup_inode: None,
            reason: None,
            identity_digest: None,
            identity_expires_unix_ms: None,
            guest_boot_id: None,
            binding: None,
        };
        write_json(&self.record_path(), &record)?;
        write_json(&self.directory().join("manifest.json"), self)?;
        if self
            .stage()
            .and_then(|()| self.stage_identity(&mut record))
            .is_err()
        {
            record.state = State::Fenced;
            record.reason = Some("artifact_staging_failed".into());
            write_json(&self.record_path(), &record)?;
            self.finish_cleanup(&mut record)?;
            anyhow::bail!("artifact staging failed; allocation fenced")
        }
        record.state = State::Prepared;
        write_json(&self.record_path(), &record)?;
        Ok(record)
    }
    fn stage(&self) -> Result<()> {
        private_dir(&self.directory().join("run"))?;
        let run = self.directory().join("run");
        let jail = self.jail_root();
        fs::create_dir_all(&jail)?;
        fn copy(source: &Artifact, dest: &Path, max: u64, executable: bool) -> Result<()> {
            let mut input = OpenOptions::new()
                .read(true)
                .custom_flags(
                    (rustix::fs::OFlags::NOFOLLOW | rustix::fs::OFlags::NONBLOCK).bits() as i32,
                )
                .open(&source.path)?;
            let m = input.metadata()?;
            ensure!(
                m.is_file() && m.len() > 0 && m.len() <= max,
                "artifact outside size bound"
            );
            let mut output = OpenOptions::new()
                .write(true)
                .create_new(true)
                .mode(0o600)
                .open(dest)?;
            let mut digest = Sha256::new();
            let mut total = 0u64;
            let mut bytes = [0u8; 65536];
            loop {
                let n = input.read(&mut bytes)?;
                if n == 0 {
                    break;
                }
                total += n as u64;
                ensure!(total <= max, "artifact grew past bound");
                digest.update(&bytes[..n]);
                output.write_all(&bytes[..n])?;
            }
            ensure!(
                hex::encode(digest.finalize()) == source.sha256,
                "artifact digest mismatch"
            );
            output.sync_all()?;
            fs::set_permissions(
                dest,
                fs::Permissions::from_mode(if executable { 0o755 } else { 0o600 }),
            )?;
            Ok(())
        }
        copy(
            &self.config.firecracker,
            &run.join("firecracker"),
            128 << 20,
            true,
        )?;
        copy(&self.config.jailer, &run.join("jailer"), 16 << 20, true)?;
        copy(&self.config.kernel, &jail.join("vmlinux"), 256 << 20, false)?;
        let disk_bytes = self.start.disk_mib << 20;
        copy(
            &self.config.rootfs,
            &jail.join("rootfs.ext4"),
            disk_bytes,
            false,
        )?;
        let disk = OpenOptions::new()
            .write(true)
            .open(jail.join("rootfs.ext4"))?;
        disk.set_len(disk_bytes)?;
        rustix::fs::fallocate(&disk, rustix::fs::FallocateFlags::empty(), 0, disk_bytes)?;
        disk.sync_all()?;
        let config = serde_json::json!({"boot-source":{"kernel_image_path":"/vmlinux","boot_args":"keep_bootcon console=ttyS0 reboot=k panic=1 pci=off root=/dev/vda rw init=/init"},"drives":[{"drive_id":"rootfs","path_on_host":"/rootfs.ext4","is_root_device":true,"is_read_only":false},{"drive_id":"bootstrap","path_on_host":"/bootstrap.img","is_root_device":false,"is_read_only":true}],"machine-config":{"vcpu_count":self.start.vcpu,"mem_size_mib":self.start.memory_mib,"smt":false},"network-interfaces":[],"vsock":{"guest_cid":3,"uds_path":"/vsock.sock"}});
        write_json(&jail.join("config.json"), &config)?;
        for name in ["vmlinux", "rootfs.ext4", "config.json"] {
            std::os::unix::fs::chown(
                jail.join(name),
                Some(self.config.jail_uid),
                Some(self.config.jail_gid),
            )?;
        }
        fs::set_permissions(jail.join("vmlinux"), fs::Permissions::from_mode(0o400))?;
        fs::set_permissions(jail.join("config.json"), fs::Permissions::from_mode(0o400))?;
        std::os::unix::fs::chown(
            &jail,
            Some(self.config.jail_uid),
            Some(self.config.jail_gid),
        )?;
        for file in [
            run.join("firecracker"),
            run.join("jailer"),
            jail.join("vmlinux"),
            jail.join("rootfs.ext4"),
            jail.join("config.json"),
        ] {
            File::open(file)?.sync_all()?;
        }
        let mut directory = jail.as_path();
        loop {
            File::open(directory)?.sync_all()?;
            if directory == self.directory() {
                break;
            }
            directory = directory.parent().context("missing staging parent")?;
        }
        File::open(&run)?.sync_all()?;
        File::open(self.directory())?.sync_all()?;
        Ok(())
    }
    fn stage_identity(&self, receipt: &mut Receipt) -> Result<()> {
        if let Some(m) = &self.member {
            let identity = crate::identity::MemberIdentity::issue(
                m.member,
                m.slot,
                m.valid_until_unix_ms,
                wall_ms(),
            )?;
            ensure!(
                self.start.expires_unix_ms <= m.valid_until_unix_ms,
                "initial member lease exceeds member validity"
            );
            let bytes = serde_json::to_vec(&identity)?;
            receipt.identity_digest = Some(hex::encode(Sha256::digest(&bytes)));
            receipt.identity_expires_unix_ms = Some(m.valid_until_unix_ms);
            write_json(&self.directory().join("run/identity.json"), &identity)?;
            return self.write_device(&identity.guest.encode_device()?);
        }
        let identity = crate::identity::Identity::issue(
            self.start.owner.allocation,
            self.start.owner.generation,
            wall_ms(),
        )?;
        ensure!(
            self.start.expires_unix_ms < identity.guest.valid_until_unix_ms,
            "initial lease exceeds channel validity"
        );
        let bytes = serde_json::to_vec(&identity)?;
        receipt.identity_digest = Some(hex::encode(Sha256::digest(&bytes)));
        receipt.identity_expires_unix_ms = Some(identity.guest.valid_until_unix_ms);
        write_json(&self.directory().join("run/identity.json"), &identity)?;
        self.write_device(&identity.guest.encode_device()?)
    }
    fn write_device(&self, device: &[u8]) -> Result<()> {
        let path = self.jail_root().join("bootstrap.img");
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&path)?;
        file.write_all(device)?;
        std::os::unix::fs::chown(
            &path,
            Some(self.config.jail_uid),
            Some(self.config.jail_gid),
        )?;
        file.set_permissions(fs::Permissions::from_mode(0o400))?;
        file.sync_all()?;
        File::open(self.jail_root())?.sync_all()?;
        Ok(())
    }
    fn member_identity(&self, receipt: &Receipt) -> Result<crate::identity::MemberIdentity> {
        let m = self.member.as_ref().context("not a warm member manifest")?;
        let identity: crate::identity::MemberIdentity =
            read_json(&self.directory().join("run/identity.json"))?;
        ensure!(
            receipt.identity_digest.as_deref()
                == Some(&hex::encode(Sha256::digest(serde_json::to_vec(&identity)?)))
                && identity.guest.member == m.member
                && identity.guest.slot == m.slot
                && identity.guest.valid_until_unix_ms == m.valid_until_unix_ms
                && receipt.identity_expires_unix_ms == Some(m.valid_until_unix_ms),
            "member channel identity mismatch"
        );
        identity.guest.validate()?;
        ensure!(
            wall_ms() < identity.guest.valid_until_unix_ms,
            "member channel identity expired"
        );
        Ok(identity)
    }
    fn allocation_identity_path(&self) -> PathBuf {
        self.directory().join("run/allocation-identity.json")
    }
    /// Allocation identity of an adopted member, checked against its binding.
    fn bound_identity(&self, binding: &Binding) -> Result<crate::identity::Identity> {
        let identity: crate::identity::Identity = read_json(&self.allocation_identity_path())?;
        ensure!(
            binding.identity_digest == hex::encode(Sha256::digest(serde_json::to_vec(&identity)?))
                && identity.guest.allocation == binding.owner.allocation
                && identity.guest.generation == binding.owner.generation
                && identity.guest.valid_until_unix_ms == binding.identity_expires_unix_ms
                && identity.guest_digest()? == binding.guest_identity_sha256,
            "adopted member identity mismatch"
        );
        identity.guest.validate()?;
        ensure!(
            wall_ms() < identity.guest.valid_until_unix_ms,
            "allocation channel identity expired"
        );
        Ok(identity)
    }
    fn identity(&self, receipt: &Receipt) -> Result<crate::identity::Identity> {
        if self.member.is_some() {
            let binding = receipt
                .binding
                .as_ref()
                .filter(|b| b.confirmed)
                .context("warm member is not adopted")?;
            return self.bound_identity(binding);
        }
        let identity: crate::identity::Identity =
            read_json(&self.directory().join("run/identity.json"))?;
        ensure!(
            receipt.identity_digest.as_deref()
                == Some(&hex::encode(Sha256::digest(serde_json::to_vec(&identity)?)))
                && identity.guest.allocation == self.start.owner.allocation
                && identity.guest.generation == self.start.owner.generation
                && receipt.identity_expires_unix_ms == Some(identity.guest.valid_until_unix_ms),
            "allocation channel identity mismatch"
        );
        identity.guest.validate()?;
        ensure!(
            wall_ms() < identity.guest.valid_until_unix_ms,
            "allocation channel identity expired"
        );
        Ok(identity)
    }
    fn verify_bootstrap(&self, receipt: &Receipt) -> Result<()> {
        let (device, valid_until) = if self.member.is_some() {
            let identity = self.member_identity(receipt)?;
            (
                identity.guest.encode_device()?,
                identity.guest.valid_until_unix_ms,
            )
        } else {
            let identity = self.identity(receipt)?;
            (
                identity.guest.encode_device()?,
                identity.guest.valid_until_unix_ms,
            )
        };
        let file = OpenOptions::new()
            .read(true)
            .custom_flags(
                (rustix::fs::OFlags::NOFOLLOW | rustix::fs::OFlags::NONBLOCK).bits() as i32,
            )
            .open(self.jail_root().join("bootstrap.img"))?;
        let metadata = file.metadata()?;
        ensure!(
            metadata.is_file()
                && metadata.len() == sandbox_protocol::bootstrap::DEVICE_BYTES as u64,
            "invalid bootstrap artifact"
        );
        let mut bytes = Vec::new();
        file.take(sandbox_protocol::bootstrap::DEVICE_BYTES as u64 + 1)
            .read_to_end(&mut bytes)?;
        ensure!(bytes == device, "bootstrap artifact changed");
        // A member lease may run to its idle limit; an allocation lease must
        // end strictly before its certificate validity.
        ensure!(
            receipt.expires_unix_ms < valid_until
                || (self.member.is_some() && receipt.expires_unix_ms == valid_until),
            "lease exceeds channel validity"
        );
        Ok(())
    }
    /// A fresh live-owner observation and durable boot binding are required before guest calls.
    /// This is a host component interface, not public API admission or controller fencing.
    pub fn guest_client(&self) -> Result<crate::guest::GuestClient> {
        let response = control(self, Action::Inspect)?;
        ensure!(response.error.is_none(), "guardian inspection uncertain");
        let receipt = response.receipt.context("guardian receipt missing")?;
        ensure!(
            receipt.state == State::Running && receipt.expires_unix_ms > wall_ms(),
            "allocation is not live"
        );
        let boot_id = receipt
            .guest_boot_id
            .clone()
            .context("guest boot not bound")?;
        let identity = self.identity(&receipt)?;
        crate::guest::GuestClient::from_firecracker_directory(
            self.jail_root(),
            52,
            identity.client_tls(wall_ms())?,
            sandbox_protocol::guest_model::Context {
                allocation_id: self.start.owner.allocation,
                generation: self.start.owner.generation,
                boot_id,
            },
        )
    }
    /// Permanently fence a request reserved by the host but not yet staged.
    /// A partial or unknown directory is never treated as confirmed absence.
    pub fn fence_unstarted(&self) -> Result<Receipt> {
        self.validate()?;
        let (_authority, may_create) = self.authorize_cleanup()?;
        if may_create {
            private_dir(&self.config.state_root)?;
            private_dir(&self.directory())?;
        }
        let _lock = self.lifecycle_lock(may_create)?;
        if self.record_path().exists() {
            let mut r = self.receipt()?;
            if r.state != State::Stopped {
                r.state = State::Fenced;
                r.reason = Some("host_stop_fence".into());
                write_json(&self.record_path(), &r)?;
                self.finish_cleanup(&mut r)?;
            }
            return Ok(r);
        }
        ensure!(
            may_create,
            "fenced cleanup cannot recreate missing metadata"
        );
        ensure!(
            !self.group().exists(),
            "unowned cgroup prevents absence proof"
        );
        ensure!(
            fs::read_dir(self.directory())?.count() == 1,
            "unknown files prevent absence proof"
        );
        let record = Receipt {
            version: 1,
            owner: self.start.owner.clone(),
            digest: self.digest()?,
            host_boot_id: boot_id()?,
            state: State::Stopped,
            cleanup_confirmed: true,
            renewal_revision: 0,
            expires_unix_ms: self.start.expires_unix_ms,
            guardian_host_pid: None,
            guardian_start_ticks: None,
            firecracker_namespace_pid: None,
            cgroup_inode: None,
            reason: Some("host_stop_before_launch".into()),
            identity_digest: None,
            identity_expires_unix_ms: None,
            guest_boot_id: None,
            binding: None,
        };
        write_json(&self.directory().join("manifest.json"), self)?;
        write_json(&self.record_path(), &record)?;
        Ok(record)
    }
    /// Fresh absence proof for retained unused-member cleanup. A historical
    /// stopped receipt alone is insufficient if runtime paths have reappeared.
    pub(crate) fn prove_member_absent(&self) -> Result<Receipt> {
        ensure!(self.is_inert(), "absence proof requires inert ownership");
        self.validate()?;
        let (_authority, _) = self.authorize_cleanup()?;
        let _lock = self.lifecycle_lock(false)?;
        let receipt = self.receipt()?;
        ensure!(
            receipt.state == State::Stopped && receipt.cleanup_confirmed,
            "member cleanup not confirmed"
        );
        for path in [self.group(), self.directory().join("run"), self.socket()] {
            match fs::symlink_metadata(path) {
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                _ => anyhow::bail!("retained member runtime absence not proven"),
            }
        }
        Ok(receipt)
    }

    /// Only a free lifecycle lock permits fencing and cleanup. Never signal a saved arbitrary PID.
    pub fn reconcile(&self, reason: &str) -> Result<Receipt> {
        self.validate()?;
        let (_authority, _) = self.authorize_cleanup()?;
        let _lock = self.lifecycle_lock(false)?;
        let mut r = self.receipt()?;
        if r.state == State::Stopped {
            return Ok(r);
        }
        r.state = State::Fenced;
        r.cleanup_confirmed = false;
        r.reason = Some(reason.into());
        write_json(&self.record_path(), &r)?;
        self.finish_cleanup(&mut r)?;
        Ok(r)
    }
    fn finish_cleanup(&self, r: &mut Receipt) -> Result<()> {
        let group = self.group();
        match fs::metadata(&group) {
            Ok(m) => {
                ensure!(m.is_dir(), "invalid owned cgroup");
                let populated = fs::read_to_string(group.join("cgroup.events"))?
                    .lines()
                    .any(|s| s == "populated 1");
                if populated {
                    ensure!(
                        r.host_boot_id == boot_id()? && r.cgroup_inode == Some(m.ino()),
                        "cgroup ownership cannot be proven"
                    );
                    fs::write(group.join("cgroup.kill"), "1")?;
                }
                let deadline = std::time::Instant::now() + Duration::from_secs(5);
                loop {
                    if fs::read_to_string(group.join("cgroup.events"))?
                        .lines()
                        .any(|s| s == "populated 0")
                    {
                        break;
                    }
                    ensure!(
                        std::time::Instant::now() < deadline,
                        "VM cleanup unconfirmed"
                    );
                    std::thread::sleep(Duration::from_millis(20));
                }
                fs::remove_dir(group)?;
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(e.into()),
        }
        let run = self.directory().join("run");
        if run.exists() {
            fs::remove_dir_all(run)?;
        }
        match fs::remove_file(self.socket()) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(e.into()),
        }
        r.state = State::Stopped;
        r.cleanup_confirmed = true;
        write_json(&self.record_path(), r)?;
        Ok(())
    }
}
