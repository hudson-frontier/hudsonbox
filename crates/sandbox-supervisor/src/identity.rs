//! Allocation-specific channel credentials. Ephemeral issuer keys are never persisted.
use anyhow::{Result, ensure};
use rcgen::{
    BasicConstraints, CertificateParams, CertifiedIssuer, ExtendedKeyUsagePurpose, IsCa, KeyPair,
    KeyUsagePurpose,
};
use sandbox_protocol::{
    AllocationId,
    bootstrap::Bootstrap,
    guest_wire::{ClientTls, guest_name},
};
#[cfg(unix)]
use sandbox_protocol::{
    guest_wire::member_name,
    warm_bind::{self, MemberBootstrap},
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use time::{Duration, OffsetDateTime};

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Identity {
    pub guest: Bootstrap,
    host_cert_pem: String,
    host_key_pem: String,
    guest_pin: [u8; 32],
}
impl std::fmt::Debug for Identity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Identity")
            .field("allocation", &self.guest.allocation)
            .finish_non_exhaustive()
    }
}
/// One ephemeral issuer with a host client leaf and a guest server leaf.
struct Issued {
    valid_until_unix_ms: i64,
    ca_pem: String,
    host_cert_pem: String,
    host_key_pem: String,
    guest_cert_pem: String,
    guest_key_pem: String,
    host_pin: [u8; 32],
    guest_pin: [u8; 32],
}
fn issue_pair(server_name: String, now_ms: i64) -> Result<Issued> {
    ensure!(now_ms > 0, "invalid channel identity clock");
    let now = OffsetDateTime::from_unix_timestamp(now_ms / 1000)?;
    let until = now
        .checked_add(Duration::hours(24))
        .ok_or_else(|| anyhow::anyhow!("certificate clock overflow"))?;
    let mut params = CertificateParams::new(Vec::new())?;
    params.not_before = now - Duration::minutes(1);
    params.not_after = until;
    params.is_ca = IsCa::Ca(BasicConstraints::Constrained(0));
    params.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
    let ca = CertifiedIssuer::self_signed(params, KeyPair::generate()?)?;
    let leaf = |name: String, client: bool| -> Result<_> {
        let key = KeyPair::generate()?;
        let mut p = CertificateParams::new(vec![name])?;
        p.not_before = now - Duration::minutes(1);
        p.not_after = until;
        p.key_usages = vec![KeyUsagePurpose::DigitalSignature];
        p.extended_key_usages = vec![if client {
            ExtendedKeyUsagePurpose::ClientAuth
        } else {
            ExtendedKeyUsagePurpose::ServerAuth
        }];
        Ok((p.signed_by(&key, &ca)?, key))
    };
    let (host, host_key) = leaf(format!("host-{server_name}"), true)?;
    let (guest, guest_key) = leaf(server_name, false)?;
    Ok(Issued {
        valid_until_unix_ms: until.unix_timestamp() * 1000,
        ca_pem: ca.pem(),
        host_cert_pem: host.pem(),
        host_key_pem: host_key.serialize_pem(),
        guest_cert_pem: guest.pem(),
        guest_key_pem: guest_key.serialize_pem(),
        host_pin: Sha256::digest(host.der()).into(),
        guest_pin: Sha256::digest(guest.der()).into(),
    })
}
impl Identity {
    pub fn issue(allocation: AllocationId, generation: i64, now_ms: i64) -> Result<Self> {
        ensure!(generation > 0 && now_ms > 0, "invalid allocation identity");
        let issued = issue_pair(guest_name(allocation), now_ms)?;
        let identity = Self {
            guest: Bootstrap {
                version: 1,
                allocation,
                generation,
                valid_until_unix_ms: issued.valid_until_unix_ms,
                ca_pem: issued.ca_pem,
                guest_cert_pem: issued.guest_cert_pem,
                guest_key_pem: issued.guest_key_pem,
                host_pin: issued.host_pin,
            },
            host_cert_pem: issued.host_cert_pem,
            host_key_pem: issued.host_key_pem,
            guest_pin: issued.guest_pin,
        };
        // Construct both endpoints now; issuance must never persist unusable key pairs.
        identity.guest.server_tls(now_ms)?;
        identity.client_tls(now_ms)?;
        Ok(identity)
    }
    /// Hex SHA-256 of the guest bootstrap's canonical JSON, as echoed by a
    /// warm member that installed it ([`warm_bind::identity_digest`]).
    #[cfg(unix)]
    pub fn guest_digest(&self) -> Result<String> {
        warm_bind::identity_digest(&self.guest)
    }
    pub fn client_tls(&self, now_ms: i64) -> Result<ClientTls> {
        self.guest.validate()?;
        ensure!(
            now_ms < self.guest.valid_until_unix_ms,
            "allocation channel identity expired"
        );
        ensure!(
            self.host_cert_pem.len() <= 16384 && self.host_key_pem.len() <= 16384,
            "invalid host credential size"
        );
        ClientTls::new(
            self.guest.ca_pem.as_bytes(),
            self.host_cert_pem.as_bytes(),
            self.host_key_pem.as_bytes(),
            self.guest_pin,
            self.guest.allocation,
        )
    }
}
/// Inert channel identity of an unbound warm member (#188). Names no
/// allocation, project or sandbox; serves only the member bind endpoint.
#[cfg(unix)]
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MemberIdentity {
    pub guest: MemberBootstrap,
    host_cert_pem: String,
    host_key_pem: String,
    guest_pin: [u8; 32],
}
#[cfg(unix)]
impl std::fmt::Debug for MemberIdentity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MemberIdentity")
            .field("member", &self.guest.member)
            .field("slot", &self.guest.slot)
            .finish_non_exhaustive()
    }
}
#[cfg(unix)]
impl MemberIdentity {
    /// `valid_until_unix_ms` bounds the member's idle life and must not exceed
    /// the 24-hour certificate validity.
    pub fn issue(
        member: uuid::Uuid,
        slot: AllocationId,
        valid_until_unix_ms: i64,
        now_ms: i64,
    ) -> Result<Self> {
        let issued = issue_pair(member_name(member), now_ms)?;
        ensure!(
            now_ms < valid_until_unix_ms && valid_until_unix_ms <= issued.valid_until_unix_ms,
            "member validity outside channel validity"
        );
        let identity = Self {
            guest: MemberBootstrap {
                version: warm_bind::MEMBER_VERSION,
                member,
                slot,
                valid_until_unix_ms,
                ca_pem: issued.ca_pem,
                guest_cert_pem: issued.guest_cert_pem,
                guest_key_pem: issued.guest_key_pem,
                host_pin: issued.host_pin,
            },
            host_cert_pem: issued.host_cert_pem,
            host_key_pem: issued.host_key_pem,
            guest_pin: issued.guest_pin,
        };
        identity.guest.server_tls(now_ms)?;
        identity.client_tls(now_ms)?;
        Ok(identity)
    }
    pub fn client_tls(&self, now_ms: i64) -> Result<ClientTls> {
        self.guest.validate()?;
        ensure!(
            now_ms < self.guest.valid_until_unix_ms,
            "member channel identity expired"
        );
        ensure!(
            self.host_cert_pem.len() <= 16384 && self.host_key_pem.len() <= 16384,
            "invalid host credential size"
        );
        ClientTls::member(
            self.guest.ca_pem.as_bytes(),
            self.host_cert_pem.as_bytes(),
            self.host_key_pem.as_bytes(),
            self.guest_pin,
            self.guest.member,
        )
    }
}
#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;
    use sandbox_protocol::Id;
    #[tokio::test]
    async fn issued_channel_authenticates_and_rejects_other_allocations() {
        let now = OffsetDateTime::now_utc().unix_timestamp() * 1000;
        let identity = Identity::issue(AllocationId::generate(), 1, now).unwrap();
        let other = Identity::issue(AllocationId::generate(), 1, now).unwrap();
        let server = identity.guest.server_tls(now).unwrap();
        let client = identity.client_tls(now).unwrap();
        let (a, b) = tokio::io::duplex(65536);
        let (a, b) = tokio::join!(server.accept(a), client.connect(b));
        assert!(a.is_ok() && b.is_ok());
        let wrong = other.client_tls(now).unwrap();
        let (a, b) = tokio::io::duplex(65536);
        let (a, b) = tokio::join!(server.accept(a), wrong.connect(b));
        assert!(a.is_err() && b.is_err());
        let until = identity.guest.valid_until_unix_ms;
        assert!(identity.client_tls(until).is_err());
        assert!(identity.guest.server_tls(until).is_err());
        let serialized = serde_json::to_value(&identity.guest).unwrap();
        assert!(serialized.get("host_key_pem").is_none());
        assert_ne!(identity.guest.guest_key_pem, identity.host_key_pem);
        assert!(!format!("{identity:?}").contains("PRIVATE KEY"));
    }
    #[cfg(unix)]
    #[tokio::test]
    async fn member_identity_is_distinct_and_never_serves_the_allocation_channel() {
        let now = OffsetDateTime::now_utc().unix_timestamp() * 1000;
        let member = uuid::Uuid::now_v7();
        let slot = AllocationId::generate();
        let identity = MemberIdentity::issue(member, slot, now + 60_000, now).unwrap();
        assert!(MemberIdentity::issue(member, slot, now + 25 * 3_600_000, now).is_err());
        assert!(MemberIdentity::issue(member, slot, now, now).is_err());
        let server = identity.guest.server_tls(now).unwrap();
        let client = identity.client_tls(now).unwrap();
        let (a, b) = tokio::io::duplex(65536);
        let (a, b) = tokio::join!(server.accept(a), client.connect(b));
        assert!(a.is_ok() && b.is_ok());
        // An allocation client (other ALPN and name) cannot reach the member endpoint.
        let allocation = Identity::issue(slot, 1, now).unwrap();
        let wrong = allocation.client_tls(now).unwrap();
        let (a, b) = tokio::io::duplex(65536);
        let (a, b) = tokio::join!(server.accept(a), wrong.connect(b));
        assert!(a.is_err() && b.is_err());
        assert_ne!(allocation.guest.host_pin, identity.guest.host_pin);
        assert_ne!(allocation.guest_digest().unwrap(), "");
        assert!(identity.client_tls(now + 60_000).is_err());
        assert!(!format!("{identity:?}").contains("PRIVATE KEY"));
    }
}
