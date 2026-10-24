//! Validate the operator-controlled executable path before retaining it for dispatch.
use std::{fs, os::unix::fs::MetadataExt, path::Path};

pub(super) fn validate(path: &Path, owner: u32) -> anyhow::Result<()> {
    anyhow::ensure!(
        path.is_absolute() && fs::canonicalize(path)?.as_path() == path,
        "guardian binary requires a canonical absolute path"
    );
    let binary = fs::symlink_metadata(path)?;
    anyhow::ensure!(
        binary.is_file() && binary.uid() == owner && binary.mode() & 0o022 == 0,
        "guardian binary must be root-owned and not group/world writable"
    );
    // Production startup fixes owner to root. The parameter also lets ordinary
    // unprivileged tests exercise the same ownership policy with temporary files.
    for parent in path.ancestors().skip(1) {
        let metadata = fs::symlink_metadata(parent)?;
        let trusted_owner = metadata.uid() == 0 || metadata.uid() == owner;
        let protected =
            metadata.mode() & 0o022 == 0 || (metadata.uid() == 0 && metadata.mode() & 0o1000 != 0);
        anyhow::ensure!(
            metadata.is_dir() && trusted_owner && protected,
            "guardian binary ancestors must be trusted directories protected from replacement"
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used)]
    use super::*;
    use std::os::unix::fs::{PermissionsExt, symlink};
    use std::path::PathBuf;

    fn fixture() -> (tempfile::TempDir, PathBuf, u32) {
        let directory = tempfile::tempdir().expect("temporary fixture");
        fs::set_permissions(directory.path(), fs::Permissions::from_mode(0o755))
            .expect("fixture traversal");
        let parent = directory.path().join("protected");
        fs::create_dir(&parent).expect("parent");
        fs::set_permissions(&parent, fs::Permissions::from_mode(0o755)).expect("parent mode");
        let binary = parent.join("guardian");
        fs::write(&binary, b"metadata fixture; never executed").expect("fixture file");
        fs::set_permissions(&binary, fs::Permissions::from_mode(0o755)).expect("file mode");
        let owner = fs::metadata(&binary).expect("metadata").uid();
        (directory, binary, owner)
    }

    #[test]
    fn accepts_a_regular_executable_below_protected_parents() {
        let (_directory, binary, owner) = fixture();
        validate(&binary, owner).expect("protected executable");
    }

    #[test]
    fn rejects_an_executable_in_a_replaceable_directory() {
        let (_directory, binary, owner) = fixture();
        fs::set_permissions(
            binary.parent().expect("parent"),
            fs::Permissions::from_mode(0o777),
        )
        .expect("replaceable parent");
        assert!(validate(&binary, owner).is_err());
    }

    #[test]
    fn rejects_a_symbolic_link_in_an_ancestor() {
        let (directory, binary, owner) = fixture();
        let alias = directory.path().join("alias");
        symlink(binary.parent().expect("parent"), &alias).expect("ancestor alias");
        assert!(validate(&alias.join("guardian"), owner).is_err());
    }
}
