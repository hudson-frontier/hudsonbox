//! Local preflight and artifact binding for an operator-declared contract.
use super::Config;
use anyhow::{Result, ensure};
use std::{fs, os::fd::AsRawFd};

pub(super) fn preflight(config: &Config) -> Result<()> {
    let Some(capabilities) = &config.capabilities else {
        return Ok(());
    };
    capabilities.validate()?;
    ensure!(
        !capabilities.simulated,
        "real host cannot advertise simulated capabilities"
    );
    ensure!(
        capabilities.architecture == std::env::consts::ARCH,
        "host capability architecture mismatch"
    );
    let release = fs::read_to_string("/proc/sys/kernel/osrelease")?;
    ensure!(
        capabilities.host_kernel_release == release.trim(),
        "host capability kernel mismatch"
    );
    let kvm = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open("/dev/kvm")?;
    // KVM_GET_API_VERSION (_IO(KVMIO, 0)) accepts no pointer and creates no VM.
    // The FD is held throughout the ioctl. Only the scalar API version is read.
    #[allow(unsafe_code)]
    let version = unsafe { libc::ioctl(kvm.as_raw_fd(), 0xAE00, 0 as libc::c_ulong) };
    ensure!(version == 12, "host requires KVM API 12");
    let controllers = fs::read_to_string(config.cgroup_parent.join("cgroup.controllers"))?;
    let enabled = fs::read_to_string(config.cgroup_parent.join("cgroup.subtree_control"))?;
    for required in ["cpu", "memory", "pids"] {
        ensure!(
            controllers.split_whitespace().any(|name| name == required)
                && enabled.split_whitespace().any(|name| name == required),
            "host requires enabled cgroup v2 cpu, memory and pids controllers"
        );
    }
    validate_artifacts(config)?;
    Ok(())
}

pub(super) fn validate_artifacts(config: &Config) -> Result<()> {
    let Some(capabilities) = &config.capabilities else {
        return Ok(());
    };
    capabilities.validate()?;
    ensure!(
        capabilities.firecracker_digest == format!("sha256:{}", config.firecracker.sha256)
            && capabilities.jailer_digest == format!("sha256:{}", config.jailer.sha256),
        "host runtime artifact contract mismatch"
    );
    ensure!(
        capabilities.images.len() == config.images.len(),
        "host image contract mismatch"
    );
    for image in &capabilities.images {
        let artifact = config
            .images
            .get(&image.rootfs_digest)
            .ok_or_else(|| anyhow::anyhow!("host image contract mismatch"))?;
        ensure!(
            image.kernel_digest == format!("sha256:{}", artifact.kernel.sha256)
                && image.rootfs_digest == format!("sha256:{}", artifact.rootfs.sha256),
            "host image artifact contract mismatch"
        );
        let size = fs::symlink_metadata(&artifact.rootfs.path)?;
        ensure!(
            size.is_file() && size.len() > 0 && size.len() <= image.minimum_disk_mib * 1024 * 1024,
            "host image disk contract mismatch"
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;
    use crate::{
        guardian::Artifact,
        host::{Capacity, Image},
    };
    use sandbox_protocol::{
        HostId, Id,
        host_capabilities::{Capabilities, Image as ImageContract, Limits},
    };
    use std::collections::BTreeMap;

    #[test]
    fn declared_images_are_bound_to_configured_artifacts_and_disk_size() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("rootfs");
        fs::write(&path, [0; 64]).unwrap();
        let artifact = Artifact {
            path: path.clone(),
            sha256: "a".repeat(64),
        };
        let digest = format!("sha256:{}", artifact.sha256);
        let caps = Capabilities {
            version: 1,
            simulated: false,
            architecture: std::env::consts::ARCH.into(),
            host_kernel_release: "6.8.0-test".into(),
            kvm_api_version: 12,
            cgroup_version: 2,
            supervisor_version: env!("CARGO_PKG_VERSION").into(),
            firecracker_version: "1.17.0".into(),
            firecracker_digest: digest.clone(),
            jailer_digest: digest.clone(),
            limits: Limits {
                vcpu: 4,
                memory_mib: 8192,
                disk_mib: 65536,
            },
            images: vec![ImageContract {
                rootfs_digest: digest.clone(),
                kernel_digest: digest.clone(),
                minimum_disk_mib: 64,
                guest_version: env!("CARGO_PKG_VERSION").into(),
                bootstrap_version: 1,
                guest_protocol_version: 1,
            }],
        };
        let mut config = Config {
            capabilities: Some(caps),
            launch_permits_required: false,
            host: HostId::generate(),
            epoch: 1,
            state_root: temp.path().into(),
            cgroup_parent: temp.path().into(),
            guardian_binary: path.clone(),
            firecracker: artifact.clone(),
            jailer: artifact.clone(),
            images: BTreeMap::from([(
                digest,
                Image {
                    kernel: artifact.clone(),
                    rootfs: artifact,
                },
            )]),
            jail_uid: 65534,
            jail_gid: 65534,
            capacity: Capacity {
                vcpu: 4,
                memory_mib: 8192,
                disk_mib: 65536,
            },
        };
        assert!(validate_artifacts(&config).is_ok());
        config.firecracker.sha256 = "b".repeat(64);
        assert!(validate_artifacts(&config).is_err());
        config.firecracker.sha256 = "a".repeat(64);
        config.images.values_mut().next().unwrap().kernel.sha256 = "b".repeat(64);
        assert!(validate_artifacts(&config).is_err());
        config.images.values_mut().next().unwrap().kernel.sha256 = "a".repeat(64);
        fs::OpenOptions::new()
            .write(true)
            .open(&path)
            .unwrap()
            .set_len(64 * 1024 * 1024 + 1)
            .unwrap();
        assert!(validate_artifacts(&config).is_err());
        fs::remove_file(&path).unwrap();
        std::os::unix::fs::symlink(temp.path(), &path).unwrap();
        assert!(validate_artifacts(&config).is_err());
        config.images.clear();
        assert!(validate_artifacts(&config).is_err());
    }
}
