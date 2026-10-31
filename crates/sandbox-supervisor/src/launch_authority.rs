//! Root-owned launch authority, independent of per-allocation directories.
//!
//! This component does not verify database closure or delete allocation files.
//! Its caller must supply those proofs before completing/forgetting an owner.
use anyhow::{Context as _, Result, ensure};
use sandbox_protocol::{
    HostId, OperationId,
    allocation_authority::{Authority, Permit},
    allocation_retirement::Intent,
};
use serde::{Deserialize, Serialize};
use std::{
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt},
    path::{Path, PathBuf},
};
const LOCK: &str = "launch.lock";
const REQUIRED: &str = "launch.required";
const STATE: &str = "launch.json";
const NEXT: &str = "launch.next";
const MAX_BYTES: u64 = (sandbox_protocol::allocation_authority::MAX_BYTES * 2 + 4096) as u64;

#[derive(Debug, thiserror::Error)]
#[error("launch authority lock is busy")]
pub(crate) struct LockContended;

pub(crate) fn is_lock_contended(error: &anyhow::Error) -> bool {
    error.chain().any(|cause| cause.is::<LockContended>())
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Required {
    version: u32,
    host: HostId,
    lock_device: u64,
    lock_inode: u64,
}
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Saved {
    version: u32,
    host: HostId,
    epoch: i64,
    // Preserve inner duplicate-field validation; a serde_json::Value would
    // silently collapse duplicates before the model could inspect them.
    ledger_json: String,
}
#[derive(Debug)]
pub struct LaunchGuard {
    _lock: File,
}
/// Durable registration progress, not an exact-owner grant or release proof.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Checkpoint {
    pub host: HostId,
    pub epoch: i64,
    pub registered_through: u64,
}
#[derive(Debug, Clone)]
pub struct AuthorityFile {
    root: PathBuf,
    host: HostId,
    minimum_epoch: i64,
    minimum_through: u64,
}
fn private_root(root: &Path) -> Result<()> {
    ensure!(
        rustix::process::geteuid().is_root(),
        "launch authority requires root"
    );
    ensure!(root.is_absolute(), "absolute launch root required");
    match fs::DirBuilder::new().mode(0o700).create(root) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {}
        Err(e) => return Err(e.into()),
    }
    let m = fs::symlink_metadata(root)?;
    ensure!(
        m.is_dir() && m.uid() == 0 && m.mode() & 0o077 == 0 && fs::canonicalize(root)? == root,
        "launch root must be canonical, private and root-owned"
    );
    Ok(())
}
fn exists(path: &Path) -> Result<bool> {
    match fs::symlink_metadata(path) {
        Ok(_) => Ok(true),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(e) => Err(e.into()),
    }
}
fn private_file(file: &File) -> Result<()> {
    let m = file.metadata()?;
    ensure!(
        m.is_file() && m.uid() == 0 && m.mode() & 0o077 == 0 && m.nlink() == 1,
        "launch metadata must be a private, singly linked root-owned file"
    );
    Ok(())
}
fn read<T: serde::de::DeserializeOwned>(path: &Path, limit: u64) -> Result<T> {
    let file = OpenOptions::new()
        .read(true)
        .custom_flags((rustix::fs::OFlags::NOFOLLOW | rustix::fs::OFlags::NONBLOCK).bits() as i32)
        .open(path)?;
    private_file(&file)?;
    ensure!(file.metadata()?.len() <= limit, "launch metadata too large");
    let mut bytes = Vec::new();
    file.take(limit + 1).read_to_end(&mut bytes)?;
    ensure!(bytes.len() as u64 <= limit, "launch metadata too large");
    serde_json::from_slice(&bytes).context("invalid launch metadata")
}
fn gate(root: &Path, exclusive: bool) -> Result<File> {
    private_root(root)?;
    // After activation, a missing lock is corruption, never permission to make
    // another inode while a process may still hold the original one.
    let active =
        exists(&root.join(REQUIRED))? || exists(&root.join(STATE))? || exists(&root.join(NEXT))?;
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(!active)
        .truncate(false)
        .mode(0o600)
        .custom_flags((rustix::fs::OFlags::NOFOLLOW | rustix::fs::OFlags::NONBLOCK).bits() as i32)
        .open(root.join(LOCK))?;
    private_file(&file)?;
    match rustix::fs::flock(
        &file,
        if exclusive {
            rustix::fs::FlockOperation::NonBlockingLockExclusive
        } else {
            rustix::fs::FlockOperation::NonBlockingLockShared
        },
    ) {
        Ok(()) => {}
        Err(rustix::io::Errno::WOULDBLOCK) => return Err(LockContended.into()),
        Err(error) => return Err(error.into()),
    }
    let retained = fs::symlink_metadata(root.join(LOCK))?;
    let held = file.metadata()?;
    ensure!(
        retained.dev() == held.dev() && retained.ino() == held.ino(),
        "launch lock replaced"
    );
    if exists(&root.join(REQUIRED))? {
        let required: Required = read(&root.join(REQUIRED), 4096)?;
        ensure!(
            required.version == 1
                && required.lock_device == held.dev()
                && required.lock_inode == held.ino(),
            "launch lock identity mismatch"
        );
    }
    Ok(file)
}
impl AuthorityFile {
    /// Explicit initialization of an empty allocation root. Never use as an
    /// open-or-create recovery path, and quiesce old binaries before activation.
    pub fn initialize(root: PathBuf, host: HostId, epoch: i64) -> Result<Self> {
        ensure!(epoch > 0, "positive launch epoch required");
        let ledger = Authority::new(host)?;
        let _lock = gate(&root, true)?;
        ensure!(
            fs::read_dir(&root)?.count() == 1,
            "launch root is not fresh"
        );
        let mut required = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(root.join(REQUIRED))?;
        let identity = _lock.metadata()?;
        required.write_all(&serde_json::to_vec(&Required {
            version: 1,
            host,
            lock_device: identity.dev(),
            lock_inode: identity.ino(),
        })?)?;
        required.sync_all()?;
        File::open(&root)?.sync_all()?;
        let store = Self {
            root,
            host,
            minimum_epoch: epoch,
            minimum_through: 0,
        };
        store.save(epoch, &ledger)?;
        Ok(store)
    }
    /// Open with independently retained lower bounds, never an empty fallback.
    pub fn open(
        root: PathBuf,
        host: HostId,
        minimum_epoch: i64,
        minimum_through: u64,
    ) -> Result<Self> {
        ensure!(minimum_epoch > 0, "positive launch epoch required");
        let store = Self {
            root,
            host,
            minimum_epoch,
            minimum_through,
        };
        let _lock = gate(&store.root, false)?;
        store.load()?;
        Ok(store)
    }
    fn load(&self) -> Result<(i64, Authority)> {
        let required: Required = read(&self.root.join(REQUIRED), 4096)?;
        ensure!(
            required.version == 1 && required.host == self.host,
            "launch activation mismatch"
        );
        let saved: Saved = read(&self.root.join(STATE), MAX_BYTES)?;
        ensure!(
            saved.version == 1 && saved.host == self.host && saved.epoch >= self.minimum_epoch,
            "launch authority scope or epoch mismatch"
        );
        Ok((
            saved.epoch,
            Authority::decode(
                saved.ledger_json.as_bytes(),
                self.host,
                self.minimum_through,
            )?,
        ))
    }
    fn save(&self, epoch: i64, ledger: &Authority) -> Result<()> {
        let saved = Saved {
            version: 1,
            host: self.host,
            epoch,
            ledger_json: String::from_utf8(ledger.encode()?)?,
        };
        let bytes = serde_json::to_vec(&saved)?;
        ensure!(bytes.len() as u64 <= MAX_BYTES, "launch metadata too large");
        let next = self.root.join(NEXT);
        if exists(&next)? {
            // Under the stable exclusive gate, only a valid old main record
            // permits removal of a leftover uncommitted staging file.
            self.load()?;
            let old = OpenOptions::new()
                .read(true)
                .custom_flags(
                    (rustix::fs::OFlags::NOFOLLOW | rustix::fs::OFlags::NONBLOCK).bits() as i32,
                )
                .open(&next)?;
            private_file(&old)?;
            fs::remove_file(&next)?;
        }
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&next)?;
        file.write_all(&bytes)?;
        file.sync_all()?;
        fs::rename(&next, self.root.join(STATE))?;
        File::open(&self.root)?.sync_all()?;
        Ok(())
    }
    fn update(
        &self,
        reporting_epoch: i64,
        change: impl FnOnce(&mut Authority) -> Result<()>,
    ) -> Result<()> {
        let _lock = gate(&self.root, true)?;
        let (epoch, mut ledger) = self.load()?;
        ensure!(epoch == reporting_epoch, "stale launch authority writer");
        change(&mut ledger)?;
        self.save(epoch, &ledger)
    }
    /// Inspect persisted state subject to the independently supplied lower bounds.
    /// Startup uses this to reconcile an interrupted epoch advance.
    pub fn retained_checkpoint(&self) -> Result<Checkpoint> {
        let _lock = gate(&self.root, false)?;
        let (epoch, ledger) = self.load()?;
        File::open(&self.root)?.sync_all()?;
        Ok(Checkpoint {
            host: self.host,
            epoch,
            registered_through: ledger.through(),
        })
    }
    /// Read-only reconciliation after a lost registration acknowledgement.
    /// A frontier can cover fenced or forgotten owners; it never grants launch.
    pub fn checkpoint(&self, reporting_epoch: i64) -> Result<Checkpoint> {
        let _lock = gate(&self.root, false)?;
        let (epoch, ledger) = self.load()?;
        ensure!(epoch == reporting_epoch, "stale launch authority reader");
        // A previous writer may have renamed its synced file but failed the
        // directory sync. Reassert that persistence before acknowledging the
        // visible frontier; never promote an uncommitted staging file.
        File::open(&self.root)?.sync_all()?;
        Ok(Checkpoint {
            host: self.host,
            epoch,
            registered_through: ledger.through(),
        })
    }
    /// Acknowledge only after the batch is durable, while holding the same gate.
    pub fn register(&self, reporting_epoch: i64, permits: &[Permit]) -> Result<Checkpoint> {
        let _lock = gate(&self.root, true)?;
        let (epoch, mut ledger) = self.load()?;
        ensure!(epoch == reporting_epoch, "stale launch authority writer");
        ensure!(
            permits.iter().all(|p| p.original_epoch <= reporting_epoch),
            "future permit epoch"
        );
        ledger.register(permits)?;
        self.save(epoch, &ledger)?;
        Ok(Checkpoint {
            host: self.host,
            epoch,
            registered_through: ledger.through(),
        })
    }
    pub fn advance_epoch(&self, previous_epoch: i64, next_epoch: i64) -> Result<()> {
        let _lock = gate(&self.root, true)?;
        let (epoch, ledger) = self.load()?;
        ensure!(
            epoch == previous_epoch && next_epoch > epoch,
            "launch epoch did not advance"
        );
        self.save(next_epoch, &ledger)
    }
    pub fn fence(&self, epoch: i64, permit: &Permit, retirement: OperationId) -> Result<()> {
        self.update(epoch, |ledger| {
            ledger.fence(permit, retirement)?;
            Ok(())
        })
    }
    /// Caller must have verified exact-owner physical cleanup and synced deletion.
    pub fn complete(&self, epoch: i64, intent: &Intent) -> Result<()> {
        self.update(epoch, |ledger| {
            ledger.complete(intent)?;
            Ok(())
        })
    }
    /// Exact-scope reconciliation, not fresh cleanup evidence. Keep the guard
    /// through any dependent journal removal so another writer cannot forget the
    /// proof in between. The caller independently validates metadata absence.
    pub fn completed(&self, epoch: i64, intent: &Intent) -> Result<LaunchGuard> {
        let lock = gate(&self.root, false)?;
        let (current, ledger) = self.load()?;
        ensure!(current == epoch, "stale completion reader");
        ledger.completed(intent)?;
        // Reassert persistence after a rename whose directory sync lost its ACK.
        File::open(&self.root)?.sync_all()?;
        Ok(LaunchGuard { _lock: lock })
    }
    /// Caller must have verified durable database completion; this deletes no files.
    pub fn forget(&self, epoch: i64, intent: &Intent) -> Result<()> {
        self.update(epoch, |ledger| {
            ledger.forget(intent)?;
            Ok(())
        })
    }
}

/// One root-exclusive transaction spanning completion, host-journal removal and
/// forgetting. The caller independently proves cleanup and database completion.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RetirementPhase {
    Fenced,
    Complete,
    Closed,
}
pub(crate) struct RetirementGuard {
    _lock: File,
    store: AuthorityFile,
    ledger: Authority,
    epoch: i64,
    intent: Intent,
    pub(crate) phase: RetirementPhase,
}
impl AuthorityFile {
    pub(crate) fn retirement_guard(&self, epoch: i64, intent: &Intent) -> Result<RetirementGuard> {
        intent.validate()?;
        ensure!(
            !intent.simulated && intent.permit.original_epoch <= epoch,
            "physical retirement required"
        );
        let lock = gate(&self.root, true)?;
        let (current, ledger) = self.load()?;
        ensure!(current == epoch, "stale forgetting epoch");
        use sandbox_protocol::allocation_authority::{Error, State};
        let phase = match ledger.state(&intent.permit) {
            Ok(State::Fenced { retirement }) if *retirement == intent.retirement => {
                RetirementPhase::Fenced
            }
            Ok(State::Complete { .. }) => {
                ledger.completed(intent)?;
                RetirementPhase::Complete
            }
            Err(Error::Closed) => RetirementPhase::Closed,
            _ => anyhow::bail!("forgetting authority conflict"),
        };
        File::open(&self.root)?.sync_all()?;
        Ok(RetirementGuard {
            _lock: lock,
            store: self.clone(),
            ledger,
            epoch,
            intent: intent.clone(),
            phase,
        })
    }
}
impl RetirementGuard {
    pub(crate) fn complete(&mut self) -> Result<()> {
        self.ledger.complete(&self.intent)?;
        self.store.save(self.epoch, &self.ledger)?;
        self.phase = RetirementPhase::Complete;
        Ok(())
    }
    pub(crate) fn forget(&mut self) -> Result<()> {
        self.ledger.forget(&self.intent)?;
        self.store.save(self.epoch, &self.ledger)?;
        self.phase = RetirementPhase::Closed;
        Ok(())
    }
}

/// Resolve a registered active allocation while holding the persistent gate.
/// The caller must compare project/sandbox/generation before creating metadata
/// and keep the guard until its journal write is durable.
pub fn authorize_registered_allocation(
    root: &Path,
    host: HostId,
    epoch: i64,
    minimum_through: u64,
    allocation: sandbox_protocol::AllocationId,
) -> Result<(Permit, LaunchGuard)> {
    let lock = gate(root, false)?;
    let store = AuthorityFile {
        root: root.into(),
        host,
        minimum_epoch: epoch,
        minimum_through,
    };
    let (current, ledger) = store.load()?;
    ensure!(current == epoch, "allocation epoch rejected");
    let permit = ledger.active_allocation(allocation)?.clone();
    ensure!(
        permit.original_epoch == epoch,
        "old allocation epoch rejected"
    );
    Ok((permit, LaunchGuard { _lock: lock }))
}

/// Hold through the final launch decision. Stop/inspection may still proceed
/// after fencing; this guard authorizes neither cleanup nor outcome reporting.
pub fn authorize(root: &Path, permit: Option<&Permit>) -> Result<LaunchGuard> {
    let lock = gate(root, false)?;
    let required = exists(&root.join(REQUIRED))?;
    let state = exists(&root.join(STATE))?;
    if let Some(permit) = permit {
        let store = AuthorityFile {
            root: root.into(),
            host: permit.host,
            minimum_epoch: permit.original_epoch,
            minimum_through: permit.serial,
        };
        let (epoch, ledger) = store.load()?;
        ensure!(epoch == permit.original_epoch, "old launch epoch rejected");
        ledger.authorize(permit)?;
    } else {
        ensure!(
            !required && !state && !exists(&root.join(NEXT))?,
            "launch permit required"
        );
    }
    Ok(LaunchGuard { _lock: lock })
}

/// Launch or clean an inert warm member (#188). It names no allocation, so no
/// permit applies; the shared gate still orders it against exclusive fences.
/// Adoption separately requires the exact registered allocation permit.
pub fn authorize_member(root: &Path) -> Result<LaunchGuard> {
    Ok(LaunchGuard {
        _lock: gate(root, false)?,
    })
}
/// Serialize cleanup with retirement. Fenced owners can inspect/clean existing
/// state but cannot create an allocation directory or replacement lifecycle lock.
pub(crate) fn authorize_cleanup(
    root: &Path,
    permit: Option<&Permit>,
) -> Result<(LaunchGuard, bool)> {
    let lock = gate(root, false)?;
    let may_create = if let Some(permit) = permit {
        let store = AuthorityFile {
            root: root.into(),
            host: permit.host,
            minimum_epoch: permit.original_epoch,
            minimum_through: permit.serial,
        };
        let (_, ledger) = store.load()?;
        match ledger.state(permit)? {
            sandbox_protocol::allocation_authority::State::Active {} => true,
            sandbox_protocol::allocation_authority::State::Fenced { .. } => false,
            _ => anyhow::bail!("cleanup authority retired"),
        }
    } else {
        ensure!(
            !exists(&root.join(REQUIRED))?
                && !exists(&root.join(STATE))?
                && !exists(&root.join(NEXT))?,
            "cleanup permit required"
        );
        true
    };
    Ok((LaunchGuard { _lock: lock }, may_create))
}
/// Exclusive against launch and cleanup metadata writers. This proves only
/// durable launch denial; the deletion caller must independently verify cleanup.
pub(crate) fn authorize_deletion(
    root: &Path,
    epoch: i64,
    minimum_through: u64,
    intent: &sandbox_protocol::allocation_retirement::Intent,
) -> Result<LaunchGuard> {
    intent.validate()?;
    ensure!(
        !intent.simulated && intent.permit.original_epoch <= epoch,
        "physical retirement required"
    );
    let lock = gate(root, true)?;
    let store = AuthorityFile {
        root: root.into(),
        host: intent.permit.host,
        minimum_epoch: epoch,
        minimum_through,
    };
    let (current, ledger) = store.load()?;
    ensure!(current == epoch, "deletion epoch mismatch");
    ensure!(
        matches!(ledger.state(&intent.permit)?,
        sandbox_protocol::allocation_authority::State::Fenced { retirement }
        if *retirement == intent.retirement),
        "exact retirement fence required"
    );
    Ok(LaunchGuard { _lock: lock })
}

/// Check an original registered retirement owner under the persistent gate.
/// This permits prior epochs but grants no cleanup or absence authority.
pub fn authorize_retirement(
    root: &Path,
    host: HostId,
    epoch: i64,
    minimum_through: u64,
    intent: &sandbox_protocol::allocation_retirement::Intent,
) -> Result<LaunchGuard> {
    intent.validate()?;
    ensure!(
        intent.permit.host == host && intent.permit.original_epoch <= epoch,
        "retirement owner mismatch"
    );
    let guard = gate(root, false)?;
    let store = AuthorityFile {
        root: root.to_path_buf(),
        host,
        minimum_epoch: epoch,
        minimum_through,
    };
    let (current, ledger) = store.load()?;
    ensure!(current == epoch, "retirement epoch mismatch");
    match ledger.state(&intent.permit)? {
        sandbox_protocol::allocation_authority::State::Active {} => {}
        sandbox_protocol::allocation_authority::State::Fenced { retirement }
            if *retirement == intent.retirement => {}
        _ => anyhow::bail!("retirement authority conflict"),
    }
    Ok(LaunchGuard { _lock: guard })
}

#[cfg(test)]
#[path = "launch_authority_tests.rs"]
mod tests;
