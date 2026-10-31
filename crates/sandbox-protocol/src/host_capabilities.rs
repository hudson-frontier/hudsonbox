//! Bounded compatibility metadata, not evidence of workload isolation or cleanup.
use crate::{images::valid_image_digest, resources};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

pub const MAX_BYTES: usize = 65_536;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Image {
    pub rootfs_digest: String,
    pub kernel_digest: String,
    pub minimum_disk_mib: u64,
    pub guest_version: String,
    pub bootstrap_version: u32,
    pub guest_protocol_version: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Limits {
    pub vcpu: u32,
    pub memory_mib: u64,
    pub disk_mib: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Capabilities {
    pub version: u32,
    pub simulated: bool,
    pub architecture: String,
    pub host_kernel_release: String,
    pub kvm_api_version: u32,
    pub cgroup_version: u32,
    pub supervisor_version: String,
    pub firecracker_version: String,
    pub firecracker_digest: String,
    pub jailer_digest: String,
    pub limits: Limits,
    pub images: Vec<Image>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("invalid or unsupported host capability contract")]
pub struct Error;

impl Capabilities {
    pub fn from_json(bytes: &[u8]) -> Result<Self, Error> {
        if bytes.is_empty() || bytes.len() > MAX_BYTES {
            return Err(Error);
        }
        let value: Self = serde_json::from_slice(bytes).map_err(|_| Error)?;
        value.validate()?;
        Ok(value)
    }

    pub fn to_json(&self) -> Result<Vec<u8>, Error> {
        self.validate()?;
        let bytes = serde_json::to_vec(self).map_err(|_| Error)?;
        if bytes.len() > MAX_BYTES {
            return Err(Error);
        }
        Ok(bytes)
    }

    pub fn validate(&self) -> Result<(), Error> {
        let release = &self.host_kernel_release;
        if self.version != 1
            || !matches!(self.architecture.as_str(), "x86_64" | "aarch64")
            || release.is_empty()
            || release.len() > 128
            || !release
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"._+-".contains(&b))
            || self.kvm_api_version != 12
            || self.cgroup_version != 2
            || self.supervisor_version != env!("CARGO_PKG_VERSION")
            || self.firecracker_version != "1.17.0"
            || !valid_image_digest(&self.firecracker_digest)
            || !valid_image_digest(&self.jailer_digest)
            || !resources::supported(
                self.limits.vcpu.into(),
                self.limits.memory_mib.into(),
                self.limits.disk_mib.into(),
            )
            || self.images.is_empty()
            || self.images.len() > 64
        {
            return Err(Error);
        }
        let mut seen = BTreeSet::new();
        for image in &self.images {
            if !valid_image_digest(&image.rootfs_digest)
                || !valid_image_digest(&image.kernel_digest)
                || !seen.insert(&image.rootfs_digest)
                || !(64..=self.limits.disk_mib).contains(&image.minimum_disk_mib)
                || image.guest_version != env!("CARGO_PKG_VERSION")
                || image.bootstrap_version != 1
                || image.guest_protocol_version != 1
            {
                return Err(Error);
            }
        }
        Ok(())
    }

    pub fn supports(&self, digest: &str, vcpu: u64, memory_mib: u64, disk_mib: u64) -> bool {
        self.validate().is_ok()
            && resources::supported(vcpu.into(), memory_mib.into(), disk_mib.into())
            && vcpu <= u64::from(self.limits.vcpu)
            && memory_mib <= self.limits.memory_mib
            && disk_mib <= self.limits.disk_mib
            && self
                .images
                .iter()
                .any(|image| image.rootfs_digest == digest && disk_mib >= image.minimum_disk_mib)
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;

    fn fixture() -> Capabilities {
        Capabilities {
            version: 1,
            simulated: true,
            architecture: "x86_64".into(),
            host_kernel_release: "6.8.0-test".into(),
            kvm_api_version: 12,
            cgroup_version: 2,
            supervisor_version: env!("CARGO_PKG_VERSION").into(),
            firecracker_version: "1.17.0".into(),
            firecracker_digest: format!("sha256:{}", "f".repeat(64)),
            jailer_digest: format!("sha256:{}", "b".repeat(64)),
            limits: Limits {
                vcpu: 4,
                memory_mib: 8192,
                disk_mib: 65536,
            },
            images: vec![Image {
                rootfs_digest: format!("sha256:{}", "a".repeat(64)),
                kernel_digest: format!("sha256:{}", "c".repeat(64)),
                minimum_disk_mib: 256,
                guest_version: env!("CARGO_PKG_VERSION").into(),
                bootstrap_version: 1,
                guest_protocol_version: 1,
            }],
        }
    }

    #[test]
    fn contract_rejects_unknown_versions_duplicate_images_and_unbounded_data() {
        let value = fixture();
        assert_eq!(
            Capabilities::from_json(&value.to_json().unwrap()).unwrap(),
            value
        );
        for mutate in [
            |c: &mut Capabilities| c.version = 2,
            |c: &mut Capabilities| c.architecture = "riscv64".into(),
            |c: &mut Capabilities| c.cgroup_version = 1,
            |c: &mut Capabilities| c.kvm_api_version = 0,
            |c: &mut Capabilities| c.images.push(c.images[0].clone()),
            |c: &mut Capabilities| c.images[0].guest_protocol_version = 2,
            |c: &mut Capabilities| c.images[0].kernel_digest = "latest".into(),
            |c: &mut Capabilities| c.host_kernel_release = "private\ntext".into(),
            |c: &mut Capabilities| c.limits.vcpu = 5,
        ] {
            let mut changed = value.clone();
            mutate(&mut changed);
            assert!(changed.to_json().is_err());
        }
        let mut object = serde_json::to_value(value).unwrap();
        object["secret"] = "must-not-be-accepted".into();
        assert!(Capabilities::from_json(&serde_json::to_vec(&object).unwrap()).is_err());
        assert!(Capabilities::from_json(&vec![b' '; MAX_BYTES + 1]).is_err());
        assert!(Capabilities::from_json(br#"{"version":1,"version":1}"#).is_err());
    }

    #[test]
    fn image_and_resource_limits_are_jointly_required() {
        let value = fixture();
        let digest = &value.images[0].rootfs_digest;
        assert!(value.supports(digest, 1, 128, 256));
        assert!(!value.supports(digest, 1, 128, 255));
        assert!(!value.supports(digest, 5, 128, 256));
        assert!(!value.supports(digest, 1, 8193, 256));
        assert!(!value.supports(digest, 1, 128, 65537));
        assert!(!value.supports(&format!("sha256:{}", "d".repeat(64)), 1, 128, 256));
    }
}
