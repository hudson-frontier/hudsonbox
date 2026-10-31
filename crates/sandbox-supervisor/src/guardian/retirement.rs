//! Recoverable removal of stopped guardian metadata and abandoned atomic-write
//! stages. The caller MUST durably retain Plan before calling remove. Never recursive.
use super::*;
use sandbox_protocol::allocation_retirement::Intent;
use std::collections::BTreeMap;

/// Nonblocking flock contention is a transient lifecycle state, not evidence
/// that the retirement request is invalid. Callers may retry only this error.
const NAMES: [&str; 3] = ["receipt.json", "manifest.json", "lifecycle.lock"];
const MAX_STAGED_FILES: usize = 16;
const MAX_STAGED_BYTES: u64 = 1 << 20;
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct Identity {
    device: u64,
    inode: u64,
}
impl Identity {
    fn of(m: &fs::Metadata) -> Self {
        Self {
            device: m.dev(),
            inode: m.ino(),
        }
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Entry {
    identity: Identity,
    len: u64,
    sha256: String,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum Metadata {
    Absent {},
    Stopped {
        directory: Identity,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        original_manifest: Option<Box<Manifest>>,
        receipt: Box<Receipt>,
        files: BTreeMap<String, Entry>,
        #[serde(default)]
        staged: BTreeMap<String, Entry>,
    },
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Plan {
    version: u32,
    intent_sha256: String,
    metadata: Metadata,
}
impl Plan {
    pub(crate) fn receipt(&self) -> Option<&Receipt> {
        match &self.metadata {
            Metadata::Absent {} => None,
            Metadata::Stopped { receipt, .. } => Some(receipt),
        }
    }
    fn validate(&self, intent: &Intent, manifest: Option<&Manifest>) -> Result<()> {
        ensure!(
            (1..=3).contains(&self.version) && self.intent_sha256 == hex::encode(intent.digest()?),
            "deletion scope mismatch"
        );
        match (&self.metadata, manifest) {
            (Metadata::Absent {}, None) => {}
            (
                Metadata::Stopped {
                    directory,
                    original_manifest,
                    receipt,
                    files,
                    staged,
                },
                Some(manifest),
            ) => {
                let original = retirement_manifest(manifest, receipt)?;
                ensure!(
                    match (original_manifest, manifest.is_bound_view()) {
                        (Some(retained), true) =>
                            self.version == 3 && retained.digest()? == original.digest()?,
                        (None, false) => true,
                        _ => false,
                    },
                    "missing or changed original retirement manifest"
                );
                ensure!(
                    receipt.state == State::Stopped && receipt.cleanup_confirmed,
                    "unverified deletion cleanup"
                );
                ensure!(
                    files.len() == NAMES.len() && NAMES.iter().all(|n| files.contains_key(*n)),
                    "invalid deletion inventory"
                );
                let staged_bytes = staged
                    .values()
                    .try_fold(0u64, |total, entry| total.checked_add(entry.len));
                ensure!(
                    (self.version >= 2 || staged.is_empty())
                        && staged.len() <= MAX_STAGED_FILES
                        && staged_bytes.is_some_and(|n| n <= MAX_STAGED_BYTES)
                        && staged.keys().all(|name| is_staging_name(name)),
                    "invalid staged metadata inventory"
                );
                ensure!(directory.inode > 0, "invalid deletion directory identity");
                let receipt_bytes = serde_json::to_vec(receipt)?;
                let manifest_bytes = serde_json::to_vec(&original)?;
                for (name, bytes) in [
                    ("receipt.json", receipt_bytes),
                    ("manifest.json", manifest_bytes),
                    ("lifecycle.lock", Vec::new()),
                ] {
                    ensure!(
                        files[name].len == bytes.len() as u64
                            && files[name].sha256 == hex::encode(Sha256::digest(bytes)),
                        "retained deletion evidence differs from original file digest"
                    );
                }
                for (name, file) in files {
                    ensure!(
                        file.identity.inode > 0
                            && file.len <= 65536
                            && file.sha256.len() == 64
                            && file
                                .sha256
                                .bytes()
                                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)),
                        "invalid deletion file digest"
                    );
                    ensure!(
                        name != "lifecycle.lock" || file.len == 0,
                        "invalid lifecycle lock contents"
                    );
                }
                for file in staged.values() {
                    ensure!(
                        file.identity.inode > 0
                            && file.len <= 65536
                            && file.sha256.len() == 64
                            && file
                                .sha256
                                .bytes()
                                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)),
                        "invalid staged metadata digest"
                    );
                }
            }
            _ => anyhow::bail!("deletion manifest mismatch"),
        }
        Ok(())
    }
}
/// Reconstruct the immutable inert manifest from the retained allocation view.
/// Its digest and the guardian's confirmed permit-bearing binding independently
/// pin the slot and ownership mapping; a caller-selected path is never authority.
fn retirement_manifest(view: &Manifest, receipt: &Receipt) -> Result<Manifest> {
    view.validate_receipt(receipt)?;
    let mut original = view.clone();
    if view.is_bound_view() {
        let binding = receipt
            .binding
            .as_ref()
            .context("missing adoption binding")?;
        ensure!(
            binding.permit == view.launch_permit,
            "adoption permit changed"
        );
        let (owner, digest) = view.receipt_identity()?;
        original.start.owner = owner;
        original.launch_permit = None;
        original
            .member
            .as_mut()
            .context("missing member")?
            .bound_from = None;
        ensure!(
            original.digest()? == digest,
            "original member manifest changed"
        );
        ensure!(
            original.bound(binding)?.digest()? == view.digest()?,
            "adoption mapping changed"
        );
    }
    Ok(original)
}
fn absent(path: &Path) -> Result<()> {
    match fs::symlink_metadata(path) {
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e.into()),
        Ok(_) => anyhow::bail!("retirement resource still present"),
    }
}
/// Absence under independently held root authority. This supplies no proof on
/// its own; Closed serials must never turn it into an original-owner receipt.
pub(crate) fn verify_absent(root: &Path, cgroup_parent: &Path, intent: &Intent) -> Result<()> {
    intent.validate()?;
    ensure!(!intent.simulated, "physical retirement required");
    ensure!(
        cgroup_parent.starts_with("/sys/fs/cgroup/")
            && cgroup_parent != Path::new("/sys/fs/cgroup")
            && !cgroup_parent
                .components()
                .any(|c| matches!(c, std::path::Component::ParentDir)),
        "invalid retirement cgroup parent"
    );
    absent(&root.join(intent.permit.allocation.to_string()))?;
    absent(&cgroup_parent.join(intent.permit.allocation.uuid().to_string()))?;
    File::open(root)?.sync_all()?;
    Ok(())
}
impl Plan {
    pub(crate) fn verify_removed(
        &self,
        root: &Path,
        cgroup_parent: &Path,
        intent: &Intent,
        manifest: Option<&Manifest>,
    ) -> Result<()> {
        self.validate(intent, manifest)?;
        if let Some(m) = manifest {
            m.validate()?;
            ensure!(
                m.launch_permit.as_ref() == Some(&intent.permit)
                    && m.config.state_root == root
                    && m.config.cgroup_parent == cgroup_parent,
                "retirement guardian scope mismatch"
            );
        }
        if let Some(m) = manifest {
            absent(&m.directory())?;
            absent(&m.group())?;
            File::open(root)?.sync_all()?;
            Ok(())
        } else {
            verify_absent(root, cgroup_parent, intent)
        }
    }
}
fn entry(path: &Path) -> Result<Entry> {
    let file = OpenOptions::new()
        .read(true)
        .custom_flags((rustix::fs::OFlags::NOFOLLOW | rustix::fs::OFlags::NONBLOCK).bits() as i32)
        .open(path)?;
    let m = file.metadata()?;
    ensure!(
        m.is_file() && m.uid() == 0 && m.mode() & 0o077 == 0 && m.nlink() == 1 && m.len() <= 65536,
        "unowned retirement metadata"
    );
    let mut bytes = Vec::new();
    file.take(65537).read_to_end(&mut bytes)?;
    ensure!(bytes.len() as u64 == m.len(), "retirement file changed");
    Ok(Entry {
        identity: Identity::of(&m),
        len: m.len(),
        sha256: hex::encode(Sha256::digest(bytes)),
    })
}
fn same_entry(path: &Path, expected: &Entry) -> Result<()> {
    let actual = entry(path)?;
    ensure!(
        actual.identity == expected.identity
            && actual.len == expected.len
            && actual.sha256 == expected.sha256,
        "retirement metadata replaced or changed"
    );
    Ok(())
}
fn is_staging_name(name: &str) -> bool {
    name.strip_suffix(".tmp")
        .and_then(|stem| stem.parse::<OperationId>().ok())
        .is_some_and(|id| format!("{id}.tmp") == name)
}
/// Holds the persistent exclusive authority lock and the original lifecycle lock.
/// Neither lock can be replaced or recreated to authorize this deletion.
pub(crate) struct Session {
    _authority: crate::launch_authority::LaunchGuard,
    _lifecycle: Option<File>,
    directory: Option<File>,
    path: PathBuf,
    root: PathBuf,
    pub(crate) plan: Plan,
}
impl Session {
    pub(crate) fn open(
        root: &Path,
        cgroup_parent: &Path,
        epoch: i64,
        frontier: u64,
        intent: &Intent,
        manifest: Option<&Manifest>,
        saved: Option<&Plan>,
    ) -> Result<Self> {
        let authority = crate::launch_authority::authorize_deletion(root, epoch, frontier, intent)?;
        ensure!(
            cgroup_parent.starts_with("/sys/fs/cgroup/")
                && cgroup_parent != Path::new("/sys/fs/cgroup")
                && !cgroup_parent
                    .components()
                    .any(|c| matches!(c, std::path::Component::ParentDir)),
            "invalid retirement cgroup parent"
        );
        let p = &intent.permit;
        let key = manifest.map_or(p.allocation, Manifest::key);
        let path = root.join(key.to_string());
        absent(&cgroup_parent.join(key.uuid().to_string()))?;
        if let Some(m) = manifest {
            m.validate()?;
            ensure!(
                m.launch_permit.as_ref() == Some(p)
                    && m.config.state_root == root
                    && m.config.cgroup_parent == cgroup_parent,
                "retirement guardian scope mismatch"
            );
        }
        if let Some(plan) = saved {
            plan.validate(intent, manifest)?;
        }
        let directory = match fs::symlink_metadata(&path) {
            Ok(m) => {
                ensure!(
                    m.is_dir()
                        && m.uid() == 0
                        && m.mode() & 0o077 == 0
                        && fs::canonicalize(&path)? == path,
                    "unowned retirement directory"
                );
                if let Some(plan) = saved {
                    match &plan.metadata {
                        Metadata::Stopped { directory, .. } => ensure!(
                            Identity::of(&m) == *directory,
                            "retirement directory replaced"
                        ),
                        Metadata::Absent {} => {
                            anyhow::bail!("unexpected directory for unused permit")
                        }
                    }
                }
                Some(File::open(&path)?)
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                ensure!(
                    saved.is_some() || manifest.is_none(),
                    "missing original guardian metadata"
                );
                None
            }
            Err(e) => return Err(e.into()),
        };
        let mut lifecycle = None;
        let mut actual = BTreeMap::new();
        let mut staged = BTreeMap::new();
        if directory.is_some() {
            ensure!(manifest.is_some(), "unknown allocation directory");
            let lock_path = path.join("lifecycle.lock");
            match OpenOptions::new()
                .read(true)
                .write(true)
                .custom_flags(
                    (rustix::fs::OFlags::NOFOLLOW | rustix::fs::OFlags::NONBLOCK).bits() as i32,
                )
                .open(&lock_path)
            {
                Ok(file) => {
                    // Validate type/ownership before flock; do not block on FIFOs or devices.
                    let identity = entry(&lock_path)?;
                    ensure!(
                        identity.len == 0 && Identity::of(&file.metadata()?) == identity.identity,
                        "invalid lifecycle identity"
                    );
                    match rustix::fs::flock(
                        &file,
                        rustix::fs::FlockOperation::NonBlockingLockExclusive,
                    ) {
                        Ok(()) => {}
                        Err(rustix::io::Errno::WOULDBLOCK) => {
                            return Err(crate::launch_authority::LockContended.into());
                        }
                        Err(error) => return Err(error.into()),
                    }
                    lifecycle = Some(file);
                }
                Err(e) if e.kind() == std::io::ErrorKind::NotFound && saved.is_some() => {}
                Err(e) => return Err(e.into()),
            }
            for item in fs::read_dir(&path)? {
                let item = item?;
                let name = item
                    .file_name()
                    .into_string()
                    .map_err(|_| anyhow::anyhow!("invalid metadata name"))?;
                if NAMES.contains(&name.as_str()) {
                    actual.insert(name, entry(&item.path())?);
                } else if is_staging_name(&name) {
                    ensure!(
                        staged.len() < MAX_STAGED_FILES,
                        "too many staged allocation metadata files"
                    );
                    let staged_entry = entry(&item.path())?;
                    let staged_bytes = staged
                        .values()
                        .try_fold(staged_entry.len, |total, e: &Entry| {
                            total.checked_add(e.len)
                        });
                    ensure!(
                        staged_bytes.is_some_and(|n| n <= MAX_STAGED_BYTES),
                        "staged allocation metadata exceeds retirement bound"
                    );
                    staged.insert(name, staged_entry);
                } else {
                    anyhow::bail!("unknown allocation metadata prevents deletion: {name:?}");
                }
            }
            ensure!(
                lifecycle.is_some() || actual.is_empty(),
                "missing lifecycle lock before metadata removal"
            );
            if let Some(plan) = saved {
                if let Metadata::Stopped { files, .. } = &plan.metadata {
                    for name in actual.keys() {
                        same_entry(
                            &path.join(name),
                            files
                                .get(name)
                                .ok_or_else(|| anyhow::anyhow!("unplanned allocation metadata"))?,
                        )?;
                    }
                    if let Metadata::Stopped {
                        staged: expected, ..
                    } = &plan.metadata
                    {
                        for name in staged.keys() {
                            same_entry(
                                &path.join(name),
                                expected.get(name).ok_or_else(|| {
                                    anyhow::anyhow!("unplanned staged allocation metadata")
                                })?,
                            )?;
                        }
                    }
                }
            } else {
                ensure!(
                    actual.len() == NAMES.len(),
                    "incomplete original guardian metadata"
                );
            }
        }
        let plan = if let Some(plan) = saved {
            plan.clone()
        } else {
            let metadata = if let Some(manifest) = manifest {
                let disk: Manifest = read_json(&path.join("manifest.json"))?;
                let receipt = manifest.receipt()?;
                let original = retirement_manifest(manifest, &receipt)?;
                ensure!(
                    disk.digest()? == original.digest()?,
                    "retirement manifest changed"
                );
                ensure!(
                    receipt.cleanup_confirmed && receipt.state == State::Stopped,
                    "guardian cleanup unconfirmed"
                );
                Metadata::Stopped {
                    original_manifest: manifest.is_bound_view().then(|| Box::new(original)),
                    directory: Identity::of(
                        &directory
                            .as_ref()
                            .context("missing directory")?
                            .metadata()?,
                    ),
                    receipt: Box::new(receipt),
                    files: actual,
                    staged,
                }
            } else {
                Metadata::Absent {}
            };
            Plan {
                version: if manifest.is_some_and(Manifest::is_bound_view) {
                    3
                } else {
                    2
                },
                intent_sha256: hex::encode(intent.digest()?),
                metadata,
            }
        };
        plan.validate(intent, manifest)?;
        Ok(Self {
            _authority: authority,
            _lifecycle: lifecycle,
            directory,
            path,
            root: root.into(),
            plan,
        })
    }
    pub(crate) fn is_removed(&self) -> bool {
        self.directory.is_none()
    }
    /// One bounded, synced deletion step; safe to restart from the retained plan.
    pub(crate) fn remove_next(&mut self) -> Result<bool> {
        if let Some(dir) = &self.directory {
            if let Metadata::Stopped { files, .. } = &self.plan.metadata {
                for name in NAMES {
                    match fs::symlink_metadata(self.path.join(name)) {
                        Ok(_) => {
                            same_entry(&self.path.join(name), &files[name])?;
                            fs::remove_file(self.path.join(name))?;
                            dir.sync_all()?;
                            return Ok(false);
                        }
                        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                        Err(e) => return Err(e.into()),
                    }
                }
            }
            if let Metadata::Stopped { staged, .. } = &self.plan.metadata {
                for (name, expected) in staged {
                    match fs::symlink_metadata(self.path.join(name)) {
                        Ok(_) => {
                            same_entry(&self.path.join(name), expected)?;
                            fs::remove_file(self.path.join(name))?;
                            dir.sync_all()?;
                            return Ok(false);
                        }
                        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                        Err(e) => return Err(e.into()),
                    }
                }
            }
            fs::remove_dir(&self.path)?;
            self.directory = None;
        }
        File::open(&self.root)?.sync_all()?;
        Ok(true)
    }
}

#[cfg(test)]
#[path = "retirement_tests.rs"]
mod tests;
