use super::*;
use serde::{Deserialize, Serialize};
use std::{
    fs::File,
    io::{Read, Write},
    os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt},
};

pub(super) const MAX_BYTES: u64 = 16 * 1024 * 1024;
pub(super) const MAX_RECORDS: usize = 1024;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Record {
    pub owner: Ownership,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retirement: Option<sandbox_protocol::allocation_retirement::Request>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata_retirement: Option<super::metadata_retirement::Saved>,
    pub revisions: BTreeMap<String, i64>,
    pub create: Option<CreateRequest>,
    pub manifest: Option<Manifest>,
    pub dispatched: bool,
    pub stopped: bool,
    pub released: bool,
    #[serde(default)]
    pub commands: BTreeMap<String, sandbox_protocol::command::CommandRecord>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub files: BTreeMap<String, sandbox_protocol::supervisor_files::FileRecord>,
    #[serde(default)]
    pub archives: BTreeMap<String, crate::archive::ArchiveRecord>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub command_history: Option<super::history::Retirement>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub file_history: Option<super::history::Retirement>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub released_commands: Option<super::released_history::Retirement>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub released_files: Option<super::released_history::Retirement>,
    pub lease_revision: i64,
    pub lease_request: Option<(i64, i64)>,
    #[serde(skip)]
    pub gate: Arc<Mutex<()>>,
    #[serde(skip, default = "file_io")]
    pub file_io: Arc<tokio::sync::Semaphore>,
    #[serde(skip)]
    pub readers: Arc<tokio::sync::RwLock<()>>,
}
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Journal {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub launch_authority: Option<crate::launch_authority::Checkpoint>,
    pub version: u32,
    pub host: HostId,
    pub epoch: i64,
    pub records: BTreeMap<String, Record>,
    /// Warm members keyed by member ID (#188). Absent in journals written
    /// before warm members existed; such journals load unchanged.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub warm: BTreeMap<String, super::warm::Entry>,
    #[serde(skip)]
    pub poisoned: bool,
}

pub(super) fn private_dir(path: &Path) -> anyhow::Result<()> {
    match fs::DirBuilder::new().mode(0o700).create(path) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {}
        Err(e) => return Err(e.into()),
    }
    let m = fs::symlink_metadata(path)?;
    anyhow::ensure!(
        m.is_dir() && m.uid() == 0 && m.mode() & 0o077 == 0,
        "host state directory must be private and root-owned"
    );
    anyhow::ensure!(
        fs::canonicalize(path)? == path,
        "host state directory must be canonical"
    );
    Ok(())
}
pub(super) fn open(config: &Config) -> anyhow::Result<(File, Journal)> {
    private_dir(&config.state_root)?;
    let lock = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .mode(0o600)
        .custom_flags(rustix::fs::OFlags::NOFOLLOW.bits() as i32)
        .open(config.state_root.join("host.lock"))?;
    rustix::fs::flock(&lock, rustix::fs::FlockOperation::NonBlockingLockExclusive)?;
    let path = config.state_root.join("host.json");
    let mut journal = match fs::OpenOptions::new()
        .read(true)
        .custom_flags((rustix::fs::OFlags::NOFOLLOW | rustix::fs::OFlags::NONBLOCK).bits() as i32)
        .open(&path)
    {
        Ok(file) => {
            let m = file.metadata()?;
            anyhow::ensure!(
                m.is_file() && m.uid() == 0 && m.mode() & 0o077 == 0 && m.len() <= MAX_BYTES,
                "invalid host journal"
            );
            let mut bytes = Vec::new();
            file.take(MAX_BYTES + 1).read_to_end(&mut bytes)?;
            anyhow::ensure!(bytes.len() as u64 <= MAX_BYTES, "host journal too large");
            let journal: Journal = serde_json::from_slice(&bytes)?;
            anyhow::ensure!(
                journal.version == 1
                    && journal.host == config.host
                    && config.epoch > journal.epoch
                    && journal.records.len() <= MAX_RECORDS,
                "host identity mismatch or epoch not advanced"
            );
            journal
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            anyhow::ensure!(
                fs::read_dir(&config.state_root)?.count() == 1,
                "missing journal with retained host state"
            );
            Journal {
                launch_authority: if config.launch_permits_required {
                    let authority = crate::launch_authority::AuthorityFile::initialize(
                        config.state_root.join("a"),
                        config.host,
                        config.epoch,
                    )?;
                    Some(authority.checkpoint(config.epoch)?)
                } else {
                    None
                },
                version: 1,
                host: config.host,
                epoch: config.epoch,
                records: BTreeMap::new(),
                warm: BTreeMap::new(),
                poisoned: false,
            }
        }
        Err(e) => return Err(e.into()),
    };
    anyhow::ensure!(
        journal
            .records
            .values()
            .map(|r| r.files.len())
            .sum::<usize>()
            <= sandbox_protocol::supervisor_files::MAX_HOST_FILES,
        "too many host file records"
    );
    anyhow::ensure!(
        journal.warm.len() <= super::warm::MAX_MEMBERS,
        "too many retained warm members"
    );
    // Retained members never become adoptable at a new epoch: maintenance
    // retires them and keeps their reservation until cleanup is proven.
    for (key, entry) in &journal.warm {
        super::warm::validate_retained(config, key, entry)?;
        anyhow::ensure!(
            entry.record.epoch() < config.epoch,
            "retained warm member from current epoch"
        );
    }
    journal.epoch = config.epoch;
    // New epochs never revive an old owner, even when its guardian survived.
    for (key, record) in &mut journal.records {
        anyhow::ensure!(
            key == &record.owner.allocation_id
                && record.owner.host_id == config.host.to_string()
                && record.owner.supervisor_epoch < config.epoch
                && record.revisions.len() <= 64,
            "invalid retained allocation ownership"
        );
        anyhow::ensure!(
            !record.released || (record.stopped && record.manifest.is_some()),
            "invalid release fence"
        );
        anyhow::ensure!(
            record.create.is_some() == record.manifest.is_some()
                && (!record.dispatched || record.manifest.is_some()),
            "invalid retained launch intent"
        );
        super::retirement::validate_retained(record, config.epoch)?;
        super::metadata_retirement::validate_retained(
            config,
            journal.launch_authority.as_ref(),
            record,
        )?;
        if let Some(manifest) = &record.manifest {
            anyhow::ensure!(
                manifest.start.owner.allocation.to_string() == *key
                    && manifest.start.owner.host == config.host
                    && manifest.config.state_root == config.state_root.join("a"),
                "invalid retained guardian ownership"
            );
        }
        if let Some(manifest) = &record.manifest {
            manifest.validate()?;
            anyhow::ensure!(
                manifest.start.owner.project.to_string() == record.owner.project_id
                    && manifest.start.owner.sandbox.to_string() == record.owner.sandbox_id
                    && manifest.start.owner.generation == record.owner.generation
                    && manifest.start.owner.epoch == record.owner.supervisor_epoch,
                "retained manifest identity mismatch"
            );
            if record.released {
                let receipt = super::metadata_retirement::receipt(record)?;
                anyhow::ensure!(
                    receipt.cleanup_confirmed && receipt.state == GuardianState::Stopped,
                    "retained release lacks cleanup evidence"
                );
            }
        }
        anyhow::ensure!(
            record.commands.len() <= sandbox_protocol::command::MAX_COMMANDS,
            "retained command journal too large"
        );
        anyhow::ensure!(
            record.archives.len() <= record.commands.len(),
            "invalid archive journal count"
        );
        for (id, archive) in &record.archives {
            let command = record
                .commands
                .get(id)
                .ok_or_else(|| anyhow::anyhow!("archive command missing"))?;
            archive.ticket.validate()?;
            anyhow::ensure!(
                archive.revision > 0 && archive.ticket.owner.operation_id.to_string() == *id,
                "invalid archive revision or operation"
            );
            super::archive::validate_owner(&record.owner, &archive.ticket)?;
            let receipt = command
                .receipt
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("archive receipt missing"))?;
            crate::archive::validate_receipt(&archive.ticket, receipt)?;
            if let Some(plans) = &archive.plans {
                crate::archive::validate_plans(&archive.ticket, plans, receipt)?;
            }
        }
        for (id, command) in &record.commands {
            let id: OperationId = id.parse()?;
            if command.not_started {
                anyhow::ensure!(
                    command.context.is_none()
                        && command.receipt.is_none()
                        && command.output_limit == 0
                        && command.deadline_unix_ms == 0,
                    "invalid command fence"
                );
            } else {
                let context = command
                    .context
                    .as_ref()
                    .ok_or_else(|| anyhow::anyhow!("missing command context"))?;
                anyhow::ensure!(
                    context.allocation_id.to_string() == record.owner.allocation_id
                        && context.generation == record.owner.generation
                        && !context.boot_id.is_empty()
                        && context.boot_id.len() <= 64
                        && (1..=sandbox_protocol::command::MAX_OUTPUT)
                            .contains(&command.output_limit),
                    "invalid retained command ownership"
                );
                if let Some(receipt) = &command.receipt {
                    command.validate_receipt(id, receipt)?;
                }
            }
        }
        anyhow::ensure!(
            record.files.len() <= sandbox_protocol::supervisor_files::MAX_FILES
                && record.files.values().map(|f| f.size).sum::<u64>()
                    <= sandbox_protocol::files::MAX_RESERVED_BYTES,
            "invalid retained file capacity"
        );
        for (id, file) in &record.files {
            id.parse::<OperationId>()?;
            file.validate()?;
            anyhow::ensure!(
                record.revisions.contains_key(id) && !record.commands.contains_key(id),
                "invalid file operation ownership"
            );
            if let Some(context) = &file.context {
                anyhow::ensure!(
                    context.allocation_id.to_string() == record.owner.allocation_id
                        && context.generation == record.owner.generation,
                    "retained file allocation mismatch"
                );
                anyhow::ensure!(
                    super::metadata_retirement::receipt(record)?
                        .guest_boot_id
                        .as_deref()
                        == Some(context.boot_id.as_str()),
                    "retained file boot mismatch"
                );
            }
        }
        super::history::validate_retained(record)?;
        super::released_history::validate_retained(record, config.epoch)?;
        record.stopped = true;
    }
    save(config, &mut journal)?;
    Ok((lock, journal))
}
pub(super) fn save(config: &Config, journal: &mut Journal) -> anyhow::Result<()> {
    anyhow::ensure!(
        !journal.poisoned,
        "host journal requires restart after uncertain write"
    );
    journal.poisoned = true;
    let bytes = serde_json::to_vec(journal)?;
    anyhow::ensure!(bytes.len() as u64 <= MAX_BYTES, "host journal full");
    // One staging name bounds interrupted writes across repeated recoveries.
    // Startup has already validated the authoritative journal under host.lock;
    // never promote staging bytes or initialize an empty missing journal.
    let temp = config.state_root.join("journal.next");
    match fs::OpenOptions::new()
        .read(true)
        .custom_flags((rustix::fs::OFlags::NOFOLLOW | rustix::fs::OFlags::NONBLOCK).bits() as i32)
        .open(&temp)
    {
        Ok(file) => {
            let m = file.metadata()?;
            anyhow::ensure!(
                m.is_file()
                    && m.uid() == 0
                    && m.mode() & 0o077 == 0
                    && m.nlink() == 1
                    && m.len() <= MAX_BYTES,
                "invalid host journal staging file"
            );
            let original = fs::symlink_metadata(config.state_root.join("host.json"))?;
            anyhow::ensure!(
                original.is_file() && original.uid() == 0 && original.mode() & 0o077 == 0,
                "missing authoritative host journal"
            );
            fs::remove_file(&temp)?;
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => return Err(e.into()),
    }
    let mut file = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&temp)?;
    file.write_all(&bytes)?;
    file.sync_all()?;
    fs::rename(temp, config.state_root.join("host.json"))?;
    File::open(&config.state_root)?.sync_all()?;
    journal.poisoned = false;
    Ok(())
}

pub(super) fn file_io() -> Arc<tokio::sync::Semaphore> {
    Arc::new(tokio::sync::Semaphore::new(1))
}
