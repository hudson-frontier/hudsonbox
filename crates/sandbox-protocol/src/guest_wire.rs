//! Framing, strict model conversion and mutually authenticated guest transport.
use crate::{AllocationId, Id, guest as w, guest_model as m};
use anyhow::{Context as _, Result, ensure};
use prost::Message;
use sha2::{Digest, Sha256};
use std::{sync::Arc, time::Duration};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio_rustls::{
    TlsAcceptor, TlsConnector,
    rustls::{
        self,
        pki_types::{CertificateDer, PrivateKeyDer, ServerName, pem::PemObject},
    },
};

pub const VERSION: u32 = 1;
pub const MAX_FRAME: usize = 128 * 1024;
pub const MAX_CHUNK: u32 = 32 * 1024;
pub const IO_TIMEOUT: Duration = Duration::from_secs(5);
pub const CONNECTION_TIMEOUT: Duration = Duration::from_secs(15);
pub const MAX_CONNECTIONS: usize = 32;
pub const ALPN: &[u8] = b"hudson-guest/1";
/// Distinct protocol for an unbound warm member's bind channel (#188). Never
/// negotiated by allocation endpoints, so neither side can be confused.
pub const MEMBER_ALPN: &[u8] = b"hudson-warm/1";

pub async fn read_frame<T: Message + Default>(io: &mut (impl AsyncRead + Unpin)) -> Result<T> {
    tokio::time::timeout(IO_TIMEOUT, async {
        let len = io.read_u32().await? as usize;
        ensure!(len > 0 && len <= MAX_FRAME, "invalid guest frame size");
        let mut bytes = vec![0; len];
        io.read_exact(&mut bytes).await?;
        T::decode(bytes.as_slice()).map_err(|_| anyhow::anyhow!("invalid guest protobuf"))
    })
    .await
    .context("guest frame timeout")?
}
pub async fn write_frame<T: Message>(io: &mut (impl AsyncWrite + Unpin), value: &T) -> Result<()> {
    let len = value.encoded_len();
    ensure!(len > 0 && len <= MAX_FRAME, "invalid guest frame size");
    tokio::time::timeout(IO_TIMEOUT, async {
        io.write_u32(len as u32).await?;
        io.write_all(&value.encode_to_vec()).await?;
        io.flush().await?;
        Ok::<_, anyhow::Error>(())
    })
    .await
    .context("guest write timeout")?
}
pub fn guest_name(allocation: AllocationId) -> String {
    format!("allocation-{}.sandbox.internal", allocation.uuid())
}
/// Server name of an unbound warm member's bind endpoint.
pub fn member_name(member: uuid::Uuid) -> String {
    format!("warm-{member}.sandbox.internal")
}
fn certificates(bytes: &[u8]) -> Result<Vec<CertificateDer<'static>>> {
    let certs = CertificateDer::pem_slice_iter(bytes)
        .collect::<std::result::Result<Vec<_>, _>>()
        .context("invalid certificate PEM")?;
    ensure!(!certs.is_empty(), "empty certificate chain");
    Ok(certs)
}
fn roots(pem: &[u8]) -> Result<rustls::RootCertStore> {
    let mut roots = rustls::RootCertStore::empty();
    for cert in certificates(pem)? {
        roots.add(cert)?;
    }
    Ok(roots)
}
fn provider() -> Arc<rustls::crypto::CryptoProvider> {
    Arc::new(rustls::crypto::ring::default_provider())
}
/// Check a newly installed allocation certificate's CA, validity, TLS server
/// purpose and exact allocation SAN before the guest commits its identity.
pub(crate) fn validate_server_identity(
    ca: &[u8],
    cert: &[u8],
    allocation: AllocationId,
    now_ms: i64,
) -> Result<()> {
    use rustls::{client::danger::ServerCertVerifier, pki_types::UnixTime};
    let verifier = rustls::client::WebPkiServerVerifier::builder_with_provider(
        Arc::new(roots(ca)?),
        provider(),
    )
    .build()?;
    let chain = certificates(cert)?;
    let now = if now_ms == 0 {
        UnixTime::now()
    } else {
        ensure!(now_ms > 0, "invalid identity time");
        UnixTime::since_unix_epoch(Duration::from_millis(now_ms as u64))
    };
    verifier.verify_server_cert(
        &chain[0],
        &chain[1..],
        &ServerName::try_from(guest_name(allocation))?,
        &[],
        now,
    )?;
    Ok(())
}

#[derive(Clone)]
pub struct ServerTls {
    acceptor: TlsAcceptor,
    peer_pin: [u8; 32],
    alpn: &'static [u8],
}
#[derive(Clone)]
pub struct ClientTls {
    connector: TlsConnector,
    peer_pin: [u8; 32],
    name: ServerName<'static>,
    alpn: &'static [u8],
}
fn peer(
    certs: Option<&[CertificateDer<'_>]>,
    expected: &[u8; 32],
    alpn: Option<&[u8]>,
    required: &[u8],
) -> Result<()> {
    let first = certs
        .and_then(|v| v.first())
        .context("missing peer certificate")?;
    let actual: [u8; 32] = Sha256::digest(first.as_ref()).into();
    ensure!(
        actual == *expected && alpn == Some(required),
        "guest peer identity or ALPN mismatch"
    );
    Ok(())
}
impl ServerTls {
    pub fn new(ca: &[u8], cert: &[u8], key: &[u8], host_pin: [u8; 32]) -> Result<Self> {
        Self::with_alpn(ca, cert, key, host_pin, ALPN)
    }
    /// An unbound warm member's bind endpoint ([`MEMBER_ALPN`]).
    pub fn member(ca: &[u8], cert: &[u8], key: &[u8], host_pin: [u8; 32]) -> Result<Self> {
        Self::with_alpn(ca, cert, key, host_pin, MEMBER_ALPN)
    }
    fn with_alpn(
        ca: &[u8],
        cert: &[u8],
        key: &[u8],
        host_pin: [u8; 32],
        alpn: &'static [u8],
    ) -> Result<Self> {
        let verifier = rustls::server::WebPkiClientVerifier::builder_with_provider(
            Arc::new(roots(ca)?),
            provider(),
        )
        .build()?;
        let mut config = rustls::ServerConfig::builder_with_provider(provider())
            .with_protocol_versions(&[&rustls::version::TLS13])?
            .with_client_cert_verifier(verifier)
            .with_single_cert(
                certificates(cert)?,
                PrivateKeyDer::from_pem_slice(key).context("invalid private key")?,
            )?;
        config.alpn_protocols = vec![alpn.to_vec()];
        config.max_early_data_size = 0;
        Ok(Self {
            acceptor: TlsAcceptor::from(Arc::new(config)),
            peer_pin: host_pin,
            alpn,
        })
    }
    pub async fn accept<S: AsyncRead + AsyncWrite + Unpin>(
        &self,
        stream: S,
    ) -> Result<tokio_rustls::server::TlsStream<S>> {
        let tls = tokio::time::timeout(IO_TIMEOUT, self.acceptor.accept(stream))
            .await
            .context("guest TLS timeout")??;
        peer(
            tls.get_ref().1.peer_certificates(),
            &self.peer_pin,
            tls.get_ref().1.alpn_protocol(),
            self.alpn,
        )?;
        Ok(tls)
    }
}
impl ClientTls {
    pub fn new(
        ca: &[u8],
        cert: &[u8],
        key: &[u8],
        guest_pin: [u8; 32],
        allocation: AllocationId,
    ) -> Result<Self> {
        Self::with_alpn(ca, cert, key, guest_pin, guest_name(allocation), ALPN)
    }
    /// Connect to an unbound warm member's bind endpoint.
    pub fn member(
        ca: &[u8],
        cert: &[u8],
        key: &[u8],
        guest_pin: [u8; 32],
        member: uuid::Uuid,
    ) -> Result<Self> {
        Self::with_alpn(ca, cert, key, guest_pin, member_name(member), MEMBER_ALPN)
    }
    fn with_alpn(
        ca: &[u8],
        cert: &[u8],
        key: &[u8],
        guest_pin: [u8; 32],
        name: String,
        alpn: &'static [u8],
    ) -> Result<Self> {
        let mut config = rustls::ClientConfig::builder_with_provider(provider())
            .with_protocol_versions(&[&rustls::version::TLS13])?
            .with_root_certificates(roots(ca)?)
            .with_client_auth_cert(
                certificates(cert)?,
                PrivateKeyDer::from_pem_slice(key).context("invalid private key")?,
            )?;
        config.alpn_protocols = vec![alpn.to_vec()];
        config.enable_early_data = false;
        Ok(Self {
            connector: TlsConnector::from(Arc::new(config)),
            peer_pin: guest_pin,
            name: ServerName::try_from(name)?,
            alpn,
        })
    }
    pub async fn connect<S: AsyncRead + AsyncWrite + Unpin>(
        &self,
        stream: S,
    ) -> Result<tokio_rustls::client::TlsStream<S>> {
        let tls = tokio::time::timeout(
            IO_TIMEOUT,
            self.connector.connect(self.name.clone(), stream),
        )
        .await
        .context("guest TLS timeout")??;
        peer(
            tls.get_ref().1.peer_certificates(),
            &self.peer_pin,
            tls.get_ref().1.alpn_protocol(),
            self.alpn,
        )?;
        Ok(tls)
    }
}
impl From<&m::Context> for w::Context {
    fn from(v: &m::Context) -> Self {
        Self {
            allocation_id: v.allocation_id.to_string(),
            generation: v.generation,
            boot_id: v.boot_id.clone(),
        }
    }
}
impl TryFrom<w::Context> for m::Context {
    type Error = anyhow::Error;
    fn try_from(v: w::Context) -> Result<Self> {
        ensure!(
            v.generation > 0 && v.boot_id.len() <= 64,
            "invalid guest context"
        );
        Ok(Self {
            allocation_id: v
                .allocation_id
                .parse()
                .map_err(|_| anyhow::anyhow!("invalid allocation identity"))?,
            generation: v.generation,
            boot_id: v.boot_id,
        })
    }
}
impl From<&m::Execute> for w::Execute {
    fn from(v: &m::Execute) -> Self {
        Self {
            operation_id: v.operation_id.to_string(),
            argv: v.argv.clone(),
            env: v.env.clone().into_iter().collect(),
            cwd: v.cwd.clone(),
            deadline_unix_ms: v.deadline_unix_ms,
            output_limit: v.output_limit,
        }
    }
}
impl TryFrom<w::Execute> for m::Execute {
    type Error = anyhow::Error;
    fn try_from(v: w::Execute) -> Result<Self> {
        let result = Self {
            operation_id: v
                .operation_id
                .parse()
                .map_err(|_| anyhow::anyhow!("invalid operation identity"))?,
            argv: v.argv,
            env: v.env.into_iter().collect(),
            cwd: v.cwd,
            deadline_unix_ms: v.deadline_unix_ms,
            output_limit: v.output_limit,
        };
        result.validate()?;
        Ok(result)
    }
}
impl From<&m::Output> for w::OutputStats {
    fn from(v: &m::Output) -> Self {
        Self {
            seen: v.seen,
            stored: v.stored,
            truncated: v.truncated,
        }
    }
}
impl From<w::OutputStats> for m::Output {
    fn from(v: w::OutputStats) -> Self {
        Self {
            seen: v.seen,
            stored: v.stored,
            truncated: v.truncated,
        }
    }
}
impl From<&m::Receipt> for w::Receipt {
    fn from(v: &m::Receipt) -> Self {
        Self {
            version: v.version,
            context: Some((&v.context).into()),
            operation_id: v.operation_id.to_string(),
            digest: v.digest.to_vec(),
            state: match v.state {
                m::State::LaunchIntent => w::State::LaunchIntent,
                m::State::Exited => w::State::Exited,
                m::State::TimedOut => w::State::TimedOut,
                m::State::Cancelled => w::State::Cancelled,
                m::State::Unknown => w::State::Unknown,
            } as i32,
            deadline_unix_ms: v.deadline_unix_ms,
            output_limit: v.output_limit,
            cancel_requested: v.cancel_requested,
            cleanup_confirmed: v.cleanup_confirmed,
            exit_code: v.exit_code,
            signal: v.signal,
            stdout: Some((&v.stdout).into()),
            stderr: Some((&v.stderr).into()),
            reason: v.reason.clone(),
        }
    }
}
impl TryFrom<w::Receipt> for m::Receipt {
    type Error = anyhow::Error;
    fn try_from(v: w::Receipt) -> Result<Self> {
        let result = Self {
            version: v.version,
            context: v.context.context("missing receipt context")?.try_into()?,
            operation_id: v
                .operation_id
                .parse()
                .map_err(|_| anyhow::anyhow!("invalid operation identity"))?,
            digest: v
                .digest
                .try_into()
                .map_err(|_| anyhow::anyhow!("invalid receipt digest"))?,
            state: match w::State::try_from(v.state)? {
                w::State::LaunchIntent => m::State::LaunchIntent,
                w::State::Exited => m::State::Exited,
                w::State::TimedOut => m::State::TimedOut,
                w::State::Cancelled => m::State::Cancelled,
                w::State::Unknown => m::State::Unknown,
                w::State::Unspecified => anyhow::bail!("unspecified receipt state"),
            },
            deadline_unix_ms: v.deadline_unix_ms,
            output_limit: v.output_limit,
            cancel_requested: v.cancel_requested,
            cleanup_confirmed: v.cleanup_confirmed,
            exit_code: v.exit_code,
            signal: v.signal,
            stdout: v.stdout.context("missing stdout stats")?.into(),
            stderr: v.stderr.context("missing stderr stats")?.into(),
            reason: v.reason,
        };
        result.validate()?;
        Ok(result)
    }
}

impl std::fmt::Debug for ServerTls {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ServerTls").finish_non_exhaustive()
    }
}
impl std::fmt::Debug for ClientTls {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClientTls")
            .field("name", &self.name)
            .finish_non_exhaustive()
    }
}
impl std::fmt::Debug for w::Execute {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Execute").finish_non_exhaustive()
    }
}
impl std::fmt::Debug for w::OutputChunk {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OutputChunk").finish_non_exhaustive()
    }
}
