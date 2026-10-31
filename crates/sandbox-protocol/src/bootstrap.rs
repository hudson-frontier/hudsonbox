//! Bounded allocation bootstrap data. Contains the guest key, never a host or CA signing key.
use crate::{AllocationId, guest_wire::ServerTls};
use anyhow::{Result, ensure};
use serde::{Deserialize, Serialize};
use std::io::Read;

pub const DEVICE_BYTES: usize = 128 * 1024;
pub const MAX_BODY: usize = 64 * 1024;
const MAGIC: &[u8; 8] = b"HDSBOOT1";
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Bootstrap {
    pub version: u32,
    pub allocation: AllocationId,
    pub generation: i64,
    pub valid_until_unix_ms: i64,
    pub ca_pem: String,
    pub guest_cert_pem: String,
    pub guest_key_pem: String,
    pub host_pin: [u8; 32],
}
impl std::fmt::Debug for Bootstrap {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Bootstrap")
            .field("allocation", &self.allocation)
            .field("generation", &self.generation)
            .finish_non_exhaustive()
    }
}
impl Bootstrap {
    pub fn validate(&self) -> Result<()> {
        ensure!(
            self.version == 1 && self.generation > 0 && self.valid_until_unix_ms > 0,
            "invalid bootstrap identity"
        );
        for value in [&self.ca_pem, &self.guest_cert_pem, &self.guest_key_pem] {
            ensure!(
                !value.is_empty() && value.len() <= 16384,
                "invalid bootstrap credential size"
            );
        }
        ensure!(self.host_pin != [0; 32], "missing bootstrap peer identity");
        Ok(())
    }
    pub fn server_tls(&self, now_ms: i64) -> Result<ServerTls> {
        self.validate()?;
        ensure!(
            now_ms < self.valid_until_unix_ms,
            "bootstrap identity expired"
        );
        ServerTls::new(
            self.ca_pem.as_bytes(),
            self.guest_cert_pem.as_bytes(),
            self.guest_key_pem.as_bytes(),
            self.host_pin,
        )
    }
    pub fn encode_device(&self) -> Result<Vec<u8>> {
        self.validate()?;
        frame(&serde_json::to_vec(self)?)
    }
    /// Version 1 only. A warm member device (version 2) is refused here, so a
    /// guest without late-binding support never boots as a member.
    pub fn read_device(reader: impl Read) -> Result<Self> {
        let body = read_body(reader)?;
        let bootstrap: Self =
            serde_json::from_slice(&body).map_err(|_| anyhow::anyhow!("invalid bootstrap body"))?;
        bootstrap.validate()?;
        Ok(bootstrap)
    }
}
/// Device framing shared by every bootstrap body version.
pub(crate) fn frame(body: &[u8]) -> Result<Vec<u8>> {
    ensure!(
        !body.is_empty() && body.len() <= MAX_BODY,
        "bootstrap body too large"
    );
    let mut bytes = Vec::with_capacity(DEVICE_BYTES);
    bytes.extend_from_slice(MAGIC);
    bytes.extend_from_slice(&(body.len() as u32).to_be_bytes());
    bytes.extend_from_slice(body);
    bytes.resize(DEVICE_BYTES, 0);
    Ok(bytes)
}
pub(crate) fn read_body(mut reader: impl Read) -> Result<Vec<u8>> {
    let mut header = [0u8; 12];
    reader.read_exact(&mut header)?;
    ensure!(&header[..8] == MAGIC, "unsupported bootstrap device");
    let length = u32::from_be_bytes([header[8], header[9], header[10], header[11]]) as usize;
    ensure!(
        length > 0 && length <= MAX_BODY,
        "invalid bootstrap body size"
    );
    let mut body = vec![0; length];
    reader.read_exact(&mut body)?;
    Ok(body)
}
#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;
    use crate::Id;
    #[test]
    fn bootstrap_is_bounded_versioned_and_redacted() {
        let mut b = Bootstrap {
            version: 1,
            allocation: AllocationId::generate(),
            generation: 1,
            valid_until_unix_ms: 1000,
            ca_pem: "CA".into(),
            guest_cert_pem: "CERT".into(),
            guest_key_pem: "PRIVATE-KEY-CANARY".into(),
            host_pin: [1; 32],
        };
        let bytes = b.encode_device().unwrap();
        assert_eq!(bytes.len(), DEVICE_BYTES);
        assert_eq!(
            Bootstrap::read_device(bytes.as_slice()).unwrap().allocation,
            b.allocation
        );
        assert!(!format!("{b:?}").contains("PRIVATE-KEY-CANARY"));
        assert!(Bootstrap::read_device(&bytes[..20]).is_err());
        let mut bad = bytes.clone();
        bad[0] = 0;
        assert!(Bootstrap::read_device(bad.as_slice()).is_err());
        bad = bytes.clone();
        bad[8..12].copy_from_slice(&((MAX_BODY + 1) as u32).to_be_bytes());
        assert!(Bootstrap::read_device(bad.as_slice()).is_err());
        b.version = 2;
        assert!(b.encode_device().is_err());
        b.version = 1;
        assert!(b.server_tls(1000).is_err());
    }
}
