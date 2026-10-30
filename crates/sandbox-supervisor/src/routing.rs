//! Bounded per-host supervisor channels for one controller or API process.
//!
//! Every TLS handshake verifies the operator CA chain, the per-host DNS
//! subjectAltName and, for registered hosts, the exact SHA-256 of the presented
//! server leaf certificate. A load-balanced address therefore cannot answer
//! for an allocation owner: any replica other than the pinned host fails the
//! handshake. Channels connect lazily, perform no transparent retries, and are
//! replaced when a host's registered endpoint, certificate or epoch changes.

use std::{
    collections::HashMap,
    fmt,
    sync::{Arc, Mutex},
    time::Duration,
};

use hyper_util::rt::TokioIo;
use sandbox_protocol::HostId;
use sha2::{Digest, Sha256};
use tokio::{
    net::TcpStream,
    sync::{OwnedSemaphorePermit, Semaphore},
    time::Instant,
};
use tokio_rustls::{
    TlsConnector,
    rustls::{
        self, ClientConfig, DigitallySignedStruct, RootCertStore, SignatureScheme,
        client::{
            WebPkiServerVerifier,
            danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier},
        },
        crypto::CryptoProvider,
        pki_types::{CertificateDer, PrivateKeyDer, ServerName, UnixTime, pem::PemObject},
    },
};
use tonic::transport::{Channel, Endpoint, Uri};

use crate::transport::host_server_name;

/// Lifecycle, inspection and reader RPCs.
pub const SHORT_RPC_TIMEOUT: Duration = Duration::from_secs(5);
/// Output publication, history and retirement RPCs.
pub const LONG_RPC_TIMEOUT: Duration = Duration::from_secs(80);
pub const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PoolLimits {
    /// Cached host entries; idle entries are evicted least-recently-used first.
    pub max_hosts: usize,
    /// Concurrent requests one process may have outstanding to one host.
    pub max_in_flight_per_host: usize,
    /// First delay after consecutive reported failures; doubles to `max_backoff`.
    pub base_backoff: Duration,
    pub max_backoff: Duration,
}

impl Default for PoolLimits {
    fn default() -> Self {
        Self {
            max_hosts: 1024,
            max_in_flight_per_host: 8,
            base_backoff: Duration::from_millis(250),
            max_backoff: Duration::from_secs(30),
        }
    }
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum RouteError {
    #[error("host transport registration is invalid")]
    Invalid,
    #[error("host has no routable transport")]
    Unaddressed,
    #[error("host request limit reached")]
    Busy,
    #[error("host is backing off after failures")]
    Backoff,
    #[error("connection cache is full of hosts with requests in flight")]
    Saturated,
}

/// Exactly where and as whom one supervisor incarnation must answer.
#[derive(Clone, PartialEq, Eq)]
pub struct HostAddress {
    pub host: HostId,
    pub epoch: i64,
    pub endpoint: String,
    pub server_name: String,
    /// Required server leaf SHA-256. `None` only for the legacy host configured
    /// on the command line, which keeps CA plus subjectAltName verification.
    pub certificate_sha256: Option<[u8; 32]>,
}

impl fmt::Debug for HostAddress {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HostAddress")
            .field("host", &self.host)
            .field("epoch", &self.epoch)
            .field("endpoint", &self.endpoint)
            .field("pinned", &self.certificate_sha256.is_some())
            .finish()
    }
}

fn authority(endpoint: &str) -> Result<String, RouteError> {
    let uri: Uri = endpoint.parse().map_err(|_| RouteError::Invalid)?;
    let authority = uri.authority().ok_or(RouteError::Invalid)?;
    if uri.scheme_str() != Some("https")
        || authority.as_str().contains('@')
        || authority.port_u16().is_none()
        || !matches!(uri.path(), "" | "/")
        || uri.query().is_some()
    {
        return Err(RouteError::Invalid);
    }
    Ok(authority.as_str().to_owned())
}

impl HostAddress {
    /// A registry transport identity: endpoint, required server name and leaf pin.
    pub fn registered(
        host: HostId,
        epoch: i64,
        endpoint: &str,
        server_name: &str,
        certificate_sha256_hex: &str,
    ) -> Result<Self, RouteError> {
        let mut pin = [0_u8; 32];
        hex::decode_to_slice(certificate_sha256_hex, &mut pin).map_err(|_| RouteError::Invalid)?;
        if epoch <= 0 || server_name != host_server_name(host) {
            return Err(RouteError::Invalid);
        }
        authority(endpoint)?;
        Ok(Self {
            host,
            epoch,
            endpoint: endpoint.to_owned(),
            server_name: server_name.to_owned(),
            certificate_sha256: Some(pin),
        })
    }

    /// The single host configured by the operator for a host without a registry
    /// transport (NULL endpoint). CA and per-host subjectAltName still apply.
    pub fn legacy(host: HostId, epoch: i64, endpoint: &str) -> Result<Self, RouteError> {
        if epoch <= 0 {
            return Err(RouteError::Invalid);
        }
        authority(endpoint)?;
        Ok(Self {
            host,
            epoch,
            endpoint: endpoint.to_owned(),
            server_name: host_server_name(host),
            certificate_sha256: None,
        })
    }
}

/// CA-chain and subjectAltName verification, then the exact leaf pin.
#[derive(Debug)]
struct PinnedVerifier {
    inner: Arc<WebPkiServerVerifier>,
    pin: Option<[u8; 32]>,
}

impl ServerCertVerifier for PinnedVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        server_name: &ServerName<'_>,
        ocsp_response: &[u8],
        now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        let verified = self.inner.verify_server_cert(
            end_entity,
            intermediates,
            server_name,
            ocsp_response,
            now,
        )?;
        match self.pin {
            Some(pin) if <[u8; 32]>::from(Sha256::digest(end_entity.as_ref())) != pin => {
                Err(rustls::Error::InvalidCertificate(
                    rustls::CertificateError::ApplicationVerificationFailure,
                ))
            }
            _ => Ok(verified),
        }
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        self.inner.verify_tls12_signature(message, cert, dss)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        self.inner.verify_tls13_signature(message, cert, dss)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.inner.supported_verify_schemes()
    }
}

/// Client mutual-TLS material shared by every host connection of one role.
#[derive(Clone)]
pub struct ClientTls {
    provider: Arc<CryptoProvider>,
    roots: Arc<RootCertStore>,
    certs: Vec<CertificateDer<'static>>,
    key: Arc<PrivateKeyDer<'static>>,
}

impl fmt::Debug for ClientTls {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ClientTls").finish_non_exhaustive()
    }
}

impl ClientTls {
    pub fn from_pem(ca_pem: &[u8], cert_pem: &[u8], key_pem: &[u8]) -> Result<Self, RouteError> {
        let mut roots = RootCertStore::empty();
        for cert in CertificateDer::pem_slice_iter(ca_pem) {
            roots
                .add(cert.map_err(|_| RouteError::Invalid)?)
                .map_err(|_| RouteError::Invalid)?;
        }
        let certs = CertificateDer::pem_slice_iter(cert_pem)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|_| RouteError::Invalid)?;
        let key = PrivateKeyDer::from_pem_slice(key_pem).map_err(|_| RouteError::Invalid)?;
        if roots.is_empty() || certs.is_empty() {
            return Err(RouteError::Invalid);
        }
        let tls = Self {
            provider: Arc::new(rustls::crypto::ring::default_provider()),
            roots: Arc::new(roots),
            certs,
            key: Arc::new(key),
        };
        tls.config(None)?;
        Ok(tls)
    }

    fn config(&self, pin: Option<[u8; 32]>) -> Result<Arc<ClientConfig>, RouteError> {
        let inner =
            WebPkiServerVerifier::builder_with_provider(self.roots.clone(), self.provider.clone())
                .build()
                .map_err(|_| RouteError::Invalid)?;
        let mut config = ClientConfig::builder_with_provider(self.provider.clone())
            .with_safe_default_protocol_versions()
            .map_err(|_| RouteError::Invalid)?
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(PinnedVerifier { inner, pin }))
            .with_client_auth_cert(self.certs.clone(), self.key.clone_key())
            .map_err(|_| RouteError::Invalid)?;
        config.alpn_protocols = vec![b"h2".to_vec()];
        Ok(Arc::new(config))
    }

    /// A lazily connecting channel bound to exactly this host identity.
    pub fn channel(&self, address: &HostAddress, timeout: Duration) -> Result<Channel, RouteError> {
        let target = authority(&address.endpoint)?;
        let server_name =
            ServerName::try_from(address.server_name.clone()).map_err(|_| RouteError::Invalid)?;
        let connector = TlsConnector::from(self.config(address.certificate_sha256)?);
        let dial = target.clone();
        let connect = tower::service_fn(move |_: Uri| {
            let (connector, dial, server_name) =
                (connector.clone(), dial.clone(), server_name.clone());
            async move {
                let tcp = TcpStream::connect(dial.as_str()).await?;
                tcp.set_nodelay(true)?;
                let tls = connector.connect(server_name, tcp).await?;
                if tls.get_ref().1.alpn_protocol() != Some(b"h2".as_slice()) {
                    return Err(std::io::Error::other("supervisor did not negotiate h2"));
                }
                Ok::<_, std::io::Error>(TokioIo::new(tls))
            }
        });
        // The connector performs TLS itself; the URI only labels HTTP/2 requests.
        Ok(Endpoint::from_shared(format!("http://{target}"))
            .map_err(|_| RouteError::Invalid)?
            .connect_timeout(CONNECT_TIMEOUT)
            .timeout(timeout)
            .connect_with_connector_lazy(connect))
    }
}

#[derive(Debug)]
struct Entry {
    address: HostAddress,
    short: Option<Channel>,
    long: Option<Channel>,
    permits: Arc<Semaphore>,
    failures: u32,
    retry_at: Option<Instant>,
    used: u64,
}

/// One acquired request slot on one host. Dropping it releases the slot.
#[derive(Debug)]
pub struct Lease {
    pub channel: Channel,
    pub address: HostAddress,
    _permit: OwnedSemaphorePermit,
}

/// Bounded host → channel cache. Holds no lock across I/O.
#[derive(Debug)]
pub struct ChannelPool {
    tls: ClientTls,
    limits: PoolLimits,
    entries: Mutex<(u64, HashMap<HostId, Entry>)>,
}

impl ChannelPool {
    pub fn new(tls: ClientTls, limits: PoolLimits) -> Result<Self, RouteError> {
        if limits.max_hosts == 0
            || limits.max_in_flight_per_host == 0
            || limits.max_in_flight_per_host > Semaphore::MAX_PERMITS
            || limits.base_backoff > limits.max_backoff
        {
            return Err(RouteError::Invalid);
        }
        Ok(Self {
            tls,
            limits,
            entries: Mutex::new((0, HashMap::new())),
        })
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, (u64, HashMap<HostId, Entry>)> {
        self.entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn entry(&self, address: &HostAddress) -> Entry {
        Entry {
            address: address.clone(),
            short: None,
            long: None,
            permits: Arc::new(Semaphore::new(self.limits.max_in_flight_per_host)),
            failures: 0,
            retry_at: None,
            used: 0,
        }
    }

    /// Reserve one request slot. A changed endpoint, certificate pin or epoch
    /// replaces the cached channels; requests already holding the old channel
    /// finish against the identity they were issued for.
    pub fn acquire(&self, address: &HostAddress, long: bool) -> Result<Lease, RouteError> {
        let mut guard = self.lock();
        let (clock, entries) = &mut *guard;
        *clock += 1;
        let now = *clock;
        if !entries.contains_key(&address.host) && entries.len() >= self.limits.max_hosts {
            let idle = entries
                .iter()
                .filter(|(_, e)| {
                    e.permits.available_permits() == self.limits.max_in_flight_per_host
                })
                .min_by_key(|(_, e)| e.used)
                .map(|(host, _)| *host)
                .ok_or(RouteError::Saturated)?;
            entries.remove(&idle);
        }
        let entry = entries
            .entry(address.host)
            .or_insert_with(|| self.entry(address));
        if entry.address != *address {
            *entry = self.entry(address);
        }
        entry.used = now;
        if entry.retry_at.is_some_and(|at| at > Instant::now()) {
            return Err(RouteError::Backoff);
        }
        let permit = entry
            .permits
            .clone()
            .try_acquire_owned()
            .map_err(|_| RouteError::Busy)?;
        let slot = if long {
            &mut entry.long
        } else {
            &mut entry.short
        };
        let channel = match slot {
            Some(channel) => channel.clone(),
            None => {
                let timeout = if long {
                    LONG_RPC_TIMEOUT
                } else {
                    SHORT_RPC_TIMEOUT
                };
                slot.insert(self.tls.channel(address, timeout)?).clone()
            }
        };
        Ok(Lease {
            channel,
            address: address.clone(),
            _permit: permit,
        })
    }

    /// Record a transport or identity failure for exactly this registration.
    /// The first failure is retried immediately; later ones back off.
    pub fn report_failure(&self, address: &HostAddress) {
        let mut guard = self.lock();
        if let Some(entry) = guard.1.get_mut(&address.host)
            && entry.address == *address
        {
            entry.failures = entry.failures.saturating_add(1);
            // Drop channels so the next attempt performs a fresh verified handshake.
            entry.short = None;
            entry.long = None;
            if entry.failures > 1 {
                let shift = (entry.failures - 2).min(16);
                let delay = self
                    .limits
                    .base_backoff
                    .saturating_mul(1 << shift)
                    .min(self.limits.max_backoff);
                entry.retry_at = Some(Instant::now() + delay);
            }
        }
    }

    pub fn report_success(&self, address: &HostAddress) {
        let mut guard = self.lock();
        if let Some(entry) = guard.1.get_mut(&address.host)
            && entry.address == *address
        {
            entry.failures = 0;
            entry.retry_at = None;
        }
    }

    /// Forget a host's channels, e.g. after its registration disappears.
    pub fn invalidate(&self, host: HostId) {
        self.lock().1.remove(&host);
    }

    #[must_use]
    pub fn cached_hosts(&self) -> usize {
        self.lock().1.len()
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use sandbox_protocol::Id;

    fn tls() -> ClientTls {
        let mut ca = rcgen::CertificateParams::new(Vec::new()).unwrap();
        ca.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        let ca =
            rcgen::CertifiedIssuer::self_signed(ca, rcgen::KeyPair::generate().unwrap()).unwrap();
        let key = rcgen::KeyPair::generate().unwrap();
        let cert = rcgen::CertificateParams::new(vec!["controller.sandbox.internal".into()])
            .unwrap()
            .signed_by(&key, &ca)
            .unwrap();
        ClientTls::from_pem(
            ca.pem().as_bytes(),
            cert.pem().as_bytes(),
            key.serialize_pem().as_bytes(),
        )
        .unwrap()
    }

    fn address(host: HostId, port: u16, pin: u8) -> HostAddress {
        HostAddress::registered(
            host,
            1,
            &format!("https://127.0.0.1:{port}"),
            &host_server_name(host),
            &format!("{pin:02x}").repeat(32),
        )
        .unwrap()
    }

    #[test]
    fn registered_addresses_bind_server_name_and_pin_to_the_host() {
        let host = HostId::generate();
        let other = HostId::generate();
        let pin = "ab".repeat(32);
        assert!(
            HostAddress::registered(host, 1, "https://h:1", &host_server_name(host), &pin).is_ok()
        );
        for (endpoint, name, pin, epoch) in [
            ("https://h:1", host_server_name(other), pin.clone(), 1),
            ("https://h:1", host_server_name(host), "ab".repeat(31), 1),
            ("http://h:1", host_server_name(host), pin.clone(), 1),
            ("https://h", host_server_name(host), pin.clone(), 1),
            ("https://u@h:1", host_server_name(host), pin.clone(), 1),
            ("https://h:1/x", host_server_name(host), pin.clone(), 1),
            ("https://h:1", host_server_name(host), pin.clone(), 0),
        ] {
            assert_eq!(
                HostAddress::registered(host, epoch, endpoint, &name, &pin),
                Err(RouteError::Invalid),
                "{endpoint} {name}"
            );
        }
    }

    #[tokio::test]
    async fn pool_bounds_requests_hosts_and_invalidates_changed_identity() {
        let limits = PoolLimits {
            max_hosts: 2,
            max_in_flight_per_host: 2,
            base_backoff: Duration::from_secs(60),
            max_backoff: Duration::from_secs(60),
        };
        let pool = ChannelPool::new(tls(), limits).unwrap();
        let (a, b, c) = (HostId::generate(), HostId::generate(), HostId::generate());
        let first = pool.acquire(&address(a, 1, 1), false).unwrap();
        let _second = pool.acquire(&address(a, 1, 1), true).unwrap();
        assert_eq!(
            pool.acquire(&address(a, 1, 1), false).unwrap_err(),
            RouteError::Busy
        );
        drop(first);
        let _b = pool.acquire(&address(b, 2, 2), false).unwrap();
        // Both cached hosts have requests in flight; nothing is evictable.
        assert_eq!(
            pool.acquire(&address(c, 3, 3), false).unwrap_err(),
            RouteError::Saturated
        );
        drop(_b);
        pool.acquire(&address(c, 3, 3), false).unwrap();
        assert_eq!(pool.cached_hosts(), 2);
        // A new endpoint for a busy host gets a fresh slot budget and channels.
        let moved = address(a, 9, 1);
        let _m1 = pool.acquire(&moved, false).unwrap();
        let _m2 = pool.acquire(&moved, false).unwrap();
        assert_eq!(pool.acquire(&moved, false).unwrap_err(), RouteError::Busy);
        // Failures for a superseded registration do not penalize the new one.
        pool.report_failure(&address(a, 1, 1));
        pool.report_failure(&address(a, 1, 1));
        drop((_m1, _m2));
        pool.acquire(&moved, false).unwrap();
        pool.report_failure(&moved);
        pool.acquire(&moved, false).unwrap();
        pool.report_failure(&moved);
        assert_eq!(
            pool.acquire(&moved, false).unwrap_err(),
            RouteError::Backoff
        );
        pool.report_success(&moved);
        pool.acquire(&moved, false).unwrap();
    }
}
