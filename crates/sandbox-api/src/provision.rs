//! Local operator credential delivery. Files are never printed, overwritten, or regenerated on retry.
use anyhow::{Context, ensure};
use sandbox_protocol::{Id, ProjectId, ProjectToken};
use sandbox_store::{Store, provision::ProjectProvision};
use serde::{Deserialize, Serialize};
use std::{
    fs::File,
    io::{Read, Write},
    path::Path,
};
use time::OffsetDateTime;

// Deliberately no Debug: this is the only durable plaintext credential copy.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Credential {
    version: u8,
    project_id: ProjectId,
    name: String,
    token: String,
    created_at: i64,
    expires_at: i64,
}

/// Provision via a private operator-owned directory on Unix. An existing file resumes the same
/// intent; after any uncertain DB result the operator must retry this file, not mint another one.
pub async fn provision(store: &Store, name: &str, path: &Path) -> anyhow::Result<ProjectId> {
    ensure!(
        !name.trim().is_empty() && name.len() <= 200 && !name.chars().any(char::is_control),
        "project name must contain 1-200 bytes without control characters"
    );
    let credential = credential(name, path)?;
    ensure!(
        credential.version == 1 && credential.name == name,
        "credential file does not match this provisioning request"
    );
    let token = ProjectToken::parse(&credential.token)
        .map_err(|_| anyhow::anyhow!("invalid credential file token"))?;
    let created_at = OffsetDateTime::from_unix_timestamp(credential.created_at)
        .context("invalid credential creation time")?;
    let expires_at = OffsetDateTime::from_unix_timestamp(credential.expires_at)
        .context("invalid credential expiry time")?;
    ensure!(
        expires_at > OffsetDateTime::now_utc()
            && expires_at - created_at == time::Duration::days(30),
        "credential has expired or has invalid lifetime"
    );
    let matches = store
        .provision_project(&ProjectProvision {
            project: credential.project_id,
            name: credential.name,
            key: token.key_id().clone(),
            hash: token.hash(),
            created_at,
            expires_at,
        })
        .await
        .map_err(|_| {
            anyhow::anyhow!(
                "project provisioning was not confirmed; retry with the same credential file"
            )
        })?;
    ensure!(
        matches,
        "existing project differs from the credential file; no existing data was changed"
    );
    Ok(credential.project_id)
}

#[cfg(unix)]
fn credential(name: &str, path: &Path) -> anyhow::Result<Credential> {
    credential_for_owner(name, path, rustix::process::geteuid().as_raw())
}

#[cfg(unix)]
fn credential_for_owner(name: &str, path: &Path, owner: u32) -> anyhow::Result<Credential> {
    use rustix::fs::{Mode, OFlags, openat};
    use std::os::unix::ffi::OsStrExt;
    let parent = path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    let filename = path
        .file_name()
        .context("credential path requires a filename")?;
    ensure!(
        path.as_os_str()
            .as_bytes()
            .rsplit(|byte| *byte == b'/')
            .next()
            == Some(filename.as_bytes()),
        "credential path must end with a filename"
    );
    let directory = private_directory(parent, owner)?;
    let mut file = match openat(
        &directory,
        filename,
        OFlags::WRONLY | OFlags::CREATE | OFlags::EXCL | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        Mode::from_bits_truncate(0o600),
    ) {
        Ok(file) => File::from(file),
        Err(rustix::io::Errno::EXIST) => return read_existing(&directory, filename, owner),
        Err(_) => anyhow::bail!("cannot create credential file"),
    };
    private_file(&file, owner)?;
    let created_at = OffsetDateTime::now_utc().unix_timestamp();
    let token = ProjectToken::generate()
        .map_err(|_| anyhow::anyhow!("cannot generate project credential"))?;
    let credential = Credential {
        version: 1,
        project_id: ProjectId::generate(),
        name: name.to_owned(),
        token: token.render_once(),
        created_at,
        expires_at: created_at + 30 * 86400,
    };
    let bytes = serde_json::to_vec(&credential).context("encoding credential file")?;
    file.write_all(&bytes).context("writing credential file")?;
    file.write_all(b"\n").context("finishing credential file")?;
    file.sync_all().context("syncing credential file")?;
    directory
        .sync_all()
        .context("syncing credential directory")?;
    Ok(credential)
}

#[cfg(unix)]
fn private_directory(path: &Path, owner: u32) -> anyhow::Result<File> {
    use rustix::fs::{Mode, OFlags, open};
    use std::os::unix::fs::MetadataExt;
    let directory = File::from(
        open(
            path,
            OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
            Mode::empty(),
        )
        .context("opening credential directory")?,
    );
    let metadata = directory
        .metadata()
        .context("reading credential directory")?;
    ensure!(
        metadata.is_dir() && metadata.uid() == owner && metadata.mode() & 0o077 == 0,
        "credential directory must be operator-owned, private (mode 0700), and not a symbolic link"
    );
    Ok(directory)
}

#[cfg(unix)]
fn private_file(file: &File, owner: u32) -> anyhow::Result<()> {
    use std::os::unix::fs::MetadataExt;
    let metadata = file.metadata().context("reading credential metadata")?;
    ensure!(
        metadata.is_file()
            && metadata.uid() == owner
            && metadata.mode() & 0o077 == 0
            && metadata.nlink() == 1
            && metadata.len() <= 4096,
        "credential must be an operator-owned private regular file with one link and at most 4096 bytes"
    );
    Ok(())
}

#[cfg(unix)]
fn read_existing(
    directory: &File,
    filename: &std::ffi::OsStr,
    owner: u32,
) -> anyhow::Result<Credential> {
    use rustix::fs::{Mode, OFlags, openat};
    let file = File::from(
        openat(
            directory,
            filename,
            OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::NONBLOCK | OFlags::CLOEXEC,
            Mode::empty(),
        )
        .context("opening credential file")?,
    );
    private_file(&file, owner)?;
    let mut bytes = Vec::new();
    (&file)
        .take(4097)
        .read_to_end(&mut bytes)
        .context("reading credential file")?;
    ensure!(bytes.len() <= 4096, "credential file is too large");
    file.sync_all()
        .context("syncing existing credential file")?;
    directory
        .sync_all()
        .context("syncing credential directory")?;
    serde_json::from_slice(&bytes)
        .map_err(|_| anyhow::anyhow!("invalid credential file; it was not overwritten"))
}
#[cfg(not(unix))]
fn credential(_name: &str, _path: &Path) -> anyhow::Result<Credential> {
    anyhow::bail!("offline provisioning requires a Unix filesystem with private permissions")
}

#[cfg(all(test, unix))]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;
    use std::os::unix::fs::{MetadataExt, PermissionsExt, symlink};

    fn directory() -> tempfile::TempDir {
        let directory = tempfile::tempdir().unwrap();
        std::fs::set_permissions(directory.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        directory
    }

    #[test]
    fn operator_owned_credential_is_preserved_on_retry() {
        let directory = directory();
        let path = directory.path().join("credential.json");
        let first = credential("fixture", &path).unwrap();
        let saved = std::fs::read(&path).unwrap();
        let second = credential("fixture", &path).unwrap();
        assert_eq!(first.project_id, second.project_id);
        assert_eq!(first.token, second.token);
        assert_eq!(std::fs::read(&path).unwrap(), saved);
    }

    #[test]
    fn private_mode_cannot_substitute_for_operator_ownership() {
        let directory = directory();
        let path = directory.path().join("credential.json");
        let _ = credential("fixture", &path).unwrap();
        let owner = std::fs::metadata(directory.path()).unwrap().uid();
        assert!(credential_for_owner("fixture", &path, owner.wrapping_add(1)).is_err());
    }

    #[test]
    fn credential_directory_cannot_be_a_symbolic_link() {
        let directory = directory();
        let real = directory.path().join("private");
        std::fs::create_dir(&real).unwrap();
        std::fs::set_permissions(&real, std::fs::Permissions::from_mode(0o700)).unwrap();
        let _ = credential("fixture", &real.join("credential.json")).unwrap();
        let alias = directory.path().join("alias");
        symlink(&real, &alias).unwrap();
        assert!(credential("fixture", &alias.join("credential.json")).is_err());
    }

    #[test]
    fn existing_credential_cannot_be_a_hard_link() {
        let directory = directory();
        let path = directory.path().join("credential.json");
        let _ = credential("fixture", &path).unwrap();
        let alias = directory.path().join("alias.json");
        std::fs::hard_link(&path, &alias).unwrap();
        let saved = std::fs::read(&path).unwrap();
        assert!(credential("fixture", &alias).is_err());
        assert_eq!(std::fs::read(&path).unwrap(), saved);
    }

    #[test]
    fn open_directory_remains_bound_after_its_path_is_replaced() {
        let outer = directory();
        let path = outer.path().join("private");
        std::fs::create_dir(&path).unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o700)).unwrap();
        let first = credential("fixture", &path.join("credential.json")).unwrap();
        let owner = rustix::process::geteuid().as_raw();
        let opened = private_directory(&path, owner).unwrap();
        std::fs::rename(&path, outer.path().join("moved")).unwrap();
        std::fs::create_dir(&path).unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o700)).unwrap();
        let replacement = credential("fixture", &path.join("credential.json")).unwrap();
        let recovered =
            read_existing(&opened, std::ffi::OsStr::new("credential.json"), owner).unwrap();
        assert_eq!(recovered.token, first.token);
        assert_ne!(recovered.token, replacement.token);
    }

    #[test]
    fn opened_file_must_belong_to_the_operator() {
        let directory = directory();
        let path = directory.path().join("credential.json");
        let _ = credential("fixture", &path).unwrap();
        let file = File::open(path).unwrap();
        let owner = file.metadata().unwrap().uid();
        assert!(private_file(&file, owner.wrapping_add(1)).is_err());
    }

    #[test]
    fn links_special_files_and_oversized_credentials_are_rejected() {
        let directory = directory();
        let target = directory.path().join("original.json");
        let _ = credential("fixture", &target).unwrap();
        let link = directory.path().join("link.json");
        symlink(&target, &link).unwrap();
        assert!(credential("fixture", &link).is_err());
        let fifo = directory.path().join("fifo.json");
        assert!(
            std::process::Command::new("mkfifo")
                .args(["-m", "600"])
                .arg(&fifo)
                .status()
                .unwrap()
                .success()
        );
        assert!(credential("fixture", &fifo).is_err());
        let large = directory.path().join("large.json");
        std::fs::write(&large, vec![b'x'; 4097]).unwrap();
        std::fs::set_permissions(&large, std::fs::Permissions::from_mode(0o600)).unwrap();
        assert!(credential("fixture", &large).is_err());
    }

    #[test]
    fn directory_syntax_cannot_create_a_different_output_file() {
        let directory = directory();
        for name in ["new/", "new/."] {
            assert!(credential("fixture", &directory.path().join(name)).is_err());
            assert!(!directory.path().join("new").exists());
        }
    }
}
