//! Late binding of an unused warm member VM to one allocation (#188).
//!
//! Contract (docs/warm-late-binding.md):
//!
//! - A member boots from a version-2 bootstrap device ([`MemberBootstrap`]):
//!   its member ID, its pre-assigned allocation slot, and a member-only channel
//!   identity. It carries no project, sandbox, generation, operation, token or
//!   customer secret. Version-1 guests refuse the device, so a guest without
//!   this protocol never runs as a member.
//! - The inert member serves only [`MemberRequest`] on the distinct
//!   [`MEMBER_ALPN`](crate::guest_wire::MEMBER_ALPN) channel, pinned to the
//!   host's member client certificate. It runs no runner, file service or
//!   customer process before binding.
//! - [`BindRequest`] installs a freshly issued version-1 allocation
//!   [`Bootstrap`] for exactly the member's slot, at a positive generation, for
//!   the exact scope digest, in the exact boot. It is accepted once; an exact
//!   retry is acknowledged again and any other scope is refused.
//! - After durable binding the ordinary allocation agent starts. The member
//!   endpoint remains for exact retries; the host proves execution readiness by an
//!   authenticated allocation hello reporting the member's boot ID.
use crate::{
    AllocationId,
    bootstrap::{self, Bootstrap},
    guest_wire::{ClientTls, IO_TIMEOUT, ServerTls},
};
use anyhow::{Context as _, Result, ensure};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};
use std::{
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    path::{Path, PathBuf},
    sync::Mutex,
};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio_rustls::rustls::pki_types::{CertificateDer, PrivateKeyDer, pem::PemObject};
use uuid::Uuid;

/// Bootstrap body version of an unbound warm member.
pub const MEMBER_VERSION: u32 = 2;
/// Version of [`MemberRequest`] and [`MemberResponse`].
pub const BIND_VERSION: u32 = 1;
pub const MEMBER_PORT: u32 = 53;
const MAX_FRAME: usize = 64 * 1024;
const MAX_BOOT_ID: usize = 64;
const MAX_REASON: usize = 64;

fn valid_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

fn valid_boot(value: &str) -> bool {
    !value.is_empty() && value.len() <= MAX_BOOT_ID
}

/// Member bootstrap device body. Never contains allocation scope or secrets
/// other than the member's own channel key.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MemberBootstrap {
    pub version: u32,
    pub member: Uuid,
    /// Allocation ID this member may become; fixed when it was reserved.
    pub slot: AllocationId,
    pub valid_until_unix_ms: i64,
    pub ca_pem: String,
    pub guest_cert_pem: String,
    pub guest_key_pem: String,
    pub host_pin: [u8; 32],
}

impl std::fmt::Debug for MemberBootstrap {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MemberBootstrap")
            .field("member", &self.member)
            .field("slot", &self.slot)
            .finish_non_exhaustive()
    }
}

impl MemberBootstrap {
    pub fn validate(&self) -> Result<()> {
        ensure!(
            self.version == MEMBER_VERSION
                && self.valid_until_unix_ms > 0
                && !self.member.is_nil()
                && !crate::Id::uuid(&self.slot).is_nil(),
            "invalid member bootstrap"
        );
        for value in [&self.ca_pem, &self.guest_cert_pem, &self.guest_key_pem] {
            ensure!(
                !value.is_empty() && value.len() <= 16384,
                "invalid member credential size"
            );
        }
        ensure!(self.host_pin != [0; 32], "missing member peer identity");
        Ok(())
    }

    pub fn server_tls(&self, now_ms: i64) -> Result<ServerTls> {
        self.validate()?;
        ensure!(now_ms < self.valid_until_unix_ms, "member identity expired");
        ServerTls::member(
            self.ca_pem.as_bytes(),
            self.guest_cert_pem.as_bytes(),
            self.guest_key_pem.as_bytes(),
            self.host_pin,
        )
    }

    pub fn encode_device(&self) -> Result<Vec<u8>> {
        self.validate()?;
        bootstrap::frame(&serde_json::to_vec(self)?)
    }
}

/// A bootstrap device as read by a late-binding-capable guest.
#[derive(Debug, Clone)]
pub enum Device {
    Allocation(Bootstrap),
    Member(MemberBootstrap),
}

/// Read either body version strictly. Unknown versions and fields fail.
pub fn read_device(reader: impl Read) -> Result<Device> {
    #[derive(Deserialize)]
    struct Peek {
        version: u32,
    }
    let body = bootstrap::read_body(reader)?;
    let peek: Peek =
        serde_json::from_slice(&body).map_err(|_| anyhow::anyhow!("invalid bootstrap body"))?;
    let invalid = || anyhow::anyhow!("invalid bootstrap body");
    match peek.version {
        1 => {
            let b: Bootstrap = serde_json::from_slice(&body).map_err(|_| invalid())?;
            b.validate()?;
            Ok(Device::Allocation(b))
        }
        MEMBER_VERSION => {
            let b: MemberBootstrap = serde_json::from_slice(&body).map_err(|_| invalid())?;
            b.validate()?;
            Ok(Device::Member(b))
        }
        _ => anyhow::bail!("unsupported bootstrap version"),
    }
}

/// Hex SHA-256 of an allocation bootstrap's canonical JSON. Echoed by the
/// guest so the host knows exactly which credentials it installed.
pub fn identity_digest(allocation: &Bootstrap) -> Result<String> {
    Ok(hex::encode(Sha256::digest(serde_json::to_vec(allocation)?)))
}

/// Install one allocation identity into an unbound member.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BindRequest {
    pub version: u32,
    pub request_id: Uuid,
    pub member: Uuid,
    pub slot: AllocationId,
    /// The member boot the host authenticated before binding.
    pub boot_id: String,
    /// Digest of the durable handoff scope this binding serves.
    pub scope_sha256: String,
    /// Exact host-authorized handoff; guest checks consistency, not issuer authority.
    pub scope: crate::warm_pool::HandoffIntent,
    /// Freshly issued version-1 allocation bootstrap for `slot`.
    pub allocation: Bootstrap,
}

impl std::fmt::Debug for BindRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BindRequest")
            .field("member", &self.member)
            .field("slot", &self.slot)
            .field("scope_sha256", &self.scope_sha256)
            .field("allocation", &self.allocation)
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum MemberRequest {
    Hello { version: u32, request_id: Uuid },
    Bind(Box<BindRequest>),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum MemberResponse {
    Hello {
        version: u32,
        request_id: Uuid,
        member: Uuid,
        slot: AllocationId,
        boot_id: String,
    },
    Bound {
        version: u32,
        request_id: Uuid,
        member: Uuid,
        slot: AllocationId,
        boot_id: String,
        scope_sha256: String,
        generation: i64,
        identity_sha256: String,
    },
    Rejected {
        version: u32,
        request_id: Uuid,
        reason: String,
    },
}

impl MemberResponse {
    fn rejected(request_id: Uuid, reason: &str) -> Self {
        Self::Rejected {
            version: BIND_VERSION,
            request_id,
            reason: reason.into(),
        }
    }

    /// Host-side check of a hello reply for an exact member/slot.
    pub fn verify_hello(
        &self,
        request_id: Uuid,
        member: Uuid,
        slot: AllocationId,
    ) -> Result<String> {
        match self {
            Self::Hello {
                version,
                request_id: r,
                member: m,
                slot: s,
                boot_id,
            } if *version == BIND_VERSION
                && *r == request_id
                && *m == member
                && *s == slot
                && valid_boot(boot_id) =>
            {
                Ok(boot_id.clone())
            }
            _ => anyhow::bail!("member hello mismatch"),
        }
    }

    /// Host-side check that the guest installed exactly `request`.
    pub fn verify_bound(&self, request: &BindRequest) -> Result<()> {
        match self {
            Self::Bound {
                version,
                request_id,
                member,
                slot,
                boot_id,
                scope_sha256,
                generation,
                identity_sha256,
            } if *version == BIND_VERSION
                && *request_id == request.request_id
                && *member == request.member
                && *slot == request.slot
                && *boot_id == request.boot_id
                && *scope_sha256 == request.scope_sha256
                && *generation == request.allocation.generation
                && *identity_sha256 == identity_digest(&request.allocation)? =>
            {
                Ok(())
            }
            Self::Rejected { reason, .. } => anyhow::bail!("member refused binding: {reason}"),
            _ => anyhow::bail!("member binding acknowledgement mismatch"),
        }
    }
}

#[derive(Clone)]
struct Binding {
    scope_sha256: String,
    identity_sha256: String,
    allocation: Bootstrap,
}

/// Guest-side member state. Unbound until exactly one binding is accepted.
pub struct MemberGuest {
    bootstrap: MemberBootstrap,
    boot_id: String,
    bound: Mutex<Option<Binding>>,
    changed: tokio::sync::Notify,
    directory: PathBuf,
    // Stable inode lock survives atomic binding-file replacement.
    _lock: File,
    persistence_failed: std::sync::atomic::AtomicBool,
}

impl std::fmt::Debug for MemberGuest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MemberGuest")
            .field("bootstrap", &self.bootstrap)
            .field("bound", &self.bound().is_some())
            .finish_non_exhaustive()
    }
}

impl MemberGuest {
    /// Open durable guest-private state. Never use tmpfs (/run) for this directory.
    /// A partial write or a previous binding for another boot fails closed.
    pub fn open(bootstrap: MemberBootstrap, boot_id: String, directory: &Path) -> Result<Self> {
        bootstrap.validate()?;
        ensure!(valid_boot(&boot_id), "invalid guest boot ID");
        match fs::create_dir(directory) {
            Ok(()) => {
                fs::set_permissions(directory, fs::Permissions::from_mode(0o700))?;
                File::open(directory.parent().context("missing state parent")?)?.sync_all()?;
            }
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(e) => return Err(e.into()),
        }
        let metadata = fs::symlink_metadata(directory)?;
        ensure!(
            metadata.is_dir()
                && metadata.permissions().mode() & 0o777 == 0o700
                && metadata.uid() == rustix::process::geteuid().as_raw(),
            "unsafe member state directory"
        );
        let fd = rustix::fs::open(
            directory.join("lock"),
            rustix::fs::OFlags::RDWR
                | rustix::fs::OFlags::CREATE
                | rustix::fs::OFlags::NOFOLLOW
                | rustix::fs::OFlags::NONBLOCK
                | rustix::fs::OFlags::CLOEXEC,
            rustix::fs::Mode::from_raw_mode(0o600),
        )?;
        let lock = File::from(fd);
        private_regular(&lock)?;
        lock.try_lock().context("member state already open")?;
        File::open(directory)?.sync_all()?;
        // The first file is a permanent incarnation marker, created before serving.
        // Missing binding after any interrupted attempt is uncertainty, never Unbound.
        let incarnation = directory.join("incarnation.json");
        let marker = serde_json::to_vec(&(
            bootstrap.member,
            bootstrap.slot,
            &boot_id,
            hex::encode(Sha256::digest(serde_json::to_vec(&bootstrap)?)),
        ))?;
        match OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&incarnation)
        {
            Ok(mut f) => {
                f.write_all(&marker)?;
                f.sync_all()?;
                File::open(directory)?.sync_all()?;
            }
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                ensure!(
                    read_retained(&incarnation)? == marker,
                    "member incarnation changed"
                );
            }
            Err(e) => return Err(e.into()),
        }
        ensure!(
            !directory.join("binding.pending").try_exists()?,
            "uncertain member binding"
        );
        let attempted = directory.join("binding.attempted").try_exists()?;
        let committed = directory.join("binding.json").try_exists()?;
        ensure!(
            attempted == committed,
            "missing retained binding or attempt marker"
        );
        let bound = if committed {
            ensure!(
                read_retained(&directory.join("binding.attempted"))? == b"attempted-v1",
                "invalid attempt marker"
            );
            let bytes = read_retained(&directory.join("binding.json"))?;
            ensure!(bytes.len() <= MAX_FRAME, "oversized retained binding");
            let request: BindRequest = serde_json::from_slice(&bytes)
                .map_err(|_| anyhow::anyhow!("invalid retained binding"))?;
            validate_binding(&bootstrap, &boot_id, &request, 0)?;
            Some(Binding {
                scope_sha256: request.scope_sha256.clone(),
                identity_sha256: identity_digest(&request.allocation)?,
                allocation: request.allocation,
            })
        } else {
            None
        };
        Ok(Self {
            bootstrap,
            boot_id,
            bound: Mutex::new(bound),
            changed: tokio::sync::Notify::new(),
            directory: directory.into(),
            _lock: lock,
            persistence_failed: std::sync::atomic::AtomicBool::new(false),
        })
    }

    pub fn bootstrap(&self) -> &MemberBootstrap {
        &self.bootstrap
    }

    /// The installed allocation bootstrap once bound.
    pub fn bound(&self) -> Option<Bootstrap> {
        self.bound
            .lock()
            .ok()
            .and_then(|b| b.as_ref().map(|b| b.allocation.clone()))
    }

    /// Resolves after the first accepted binding.
    pub async fn wait_bound(&self) -> Bootstrap {
        loop {
            let notified = self.changed.notified();
            if let Some(b) = self.bound() {
                return b;
            }
            notified.await;
        }
    }

    /// Evaluate one request. Never panics on hostile input.
    pub fn handle(&self, request: MemberRequest, now_ms: i64) -> MemberResponse {
        match request {
            MemberRequest::Hello {
                version,
                request_id,
            } => {
                if version != BIND_VERSION {
                    return MemberResponse::rejected(request_id, "unsupported_version");
                }
                MemberResponse::Hello {
                    version: BIND_VERSION,
                    request_id,
                    member: self.bootstrap.member,
                    slot: self.bootstrap.slot,
                    boot_id: self.boot_id.clone(),
                }
            }
            MemberRequest::Bind(request) => self.bind(*request, now_ms),
        }
    }

    fn bind(&self, request: BindRequest, now_ms: i64) -> MemberResponse {
        let id = request.request_id;
        if request.version != BIND_VERSION {
            return MemberResponse::rejected(id, "unsupported_version");
        }
        if now_ms >= self.bootstrap.valid_until_unix_ms {
            return MemberResponse::rejected(id, "member_expired");
        }
        if request.member != self.bootstrap.member || request.slot != self.bootstrap.slot {
            return MemberResponse::rejected(id, "wrong_member");
        }
        if request.boot_id != self.boot_id {
            return MemberResponse::rejected(id, "wrong_boot");
        }
        let a = &request.allocation;
        if validate_binding(&self.bootstrap, &self.boot_id, &request, now_ms).is_err() {
            return MemberResponse::rejected(id, "invalid_allocation_identity");
        }
        let Ok(identity_sha256) = identity_digest(a) else {
            return MemberResponse::rejected(id, "invalid_allocation_identity");
        };
        let Ok(mut bound) = self.bound.lock() else {
            return MemberResponse::rejected(id, "member_state_poisoned");
        };
        match bound.as_ref() {
            Some(b)
                if b.scope_sha256 != request.scope_sha256
                    || b.identity_sha256 != identity_sha256 =>
            {
                return MemberResponse::rejected(id, "already_bound");
            }
            Some(_) => {}
            None => {
                // Pending is deliberately retained on every error: even the same
                // process cannot treat uncertain persistence as a new member.
                if self
                    .persistence_failed
                    .load(std::sync::atomic::Ordering::SeqCst)
                {
                    return MemberResponse::rejected(id, "binding_persistence_uncertain");
                }
                if persist_binding(&self.directory, &request).is_err() {
                    self.persistence_failed
                        .store(true, std::sync::atomic::Ordering::SeqCst);
                    return MemberResponse::rejected(id, "binding_persistence_uncertain");
                }
                *bound = Some(Binding {
                    scope_sha256: request.scope_sha256.clone(),
                    identity_sha256: identity_sha256.clone(),
                    allocation: a.clone(),
                });
                self.changed.notify_waiters();
            }
        }
        MemberResponse::Bound {
            version: BIND_VERSION,
            request_id: id,
            member: self.bootstrap.member,
            slot: self.bootstrap.slot,
            boot_id: self.boot_id.clone(),
            scope_sha256: request.scope_sha256,
            generation: a.generation,
            identity_sha256,
        }
    }
}

fn validate_binding(
    member: &MemberBootstrap,
    boot: &str,
    request: &BindRequest,
    now_ms: i64,
) -> Result<()> {
    let a = &request.allocation;
    request.scope.validate()?;
    ensure!(
        request.version == BIND_VERSION
            && request.member == member.member
            && request.slot == member.slot
            && request.boot_id == boot
            && request.scope.member_id == member.member
            && request.scope.allocation_id == member.slot
            && request.scope.generation == a.generation
            && a.allocation == member.slot
            && valid_sha256(&request.scope_sha256)
            && request.scope_sha256 == request.scope.digest()?,
        "binding scope mismatch"
    );
    let allocation_roots = CertificateDer::pem_slice_iter(a.ca_pem.as_bytes())
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let member_roots = CertificateDer::pem_slice_iter(member.ca_pem.as_bytes())
        .collect::<std::result::Result<Vec<_>, _>>()?;
    ensure!(
        allocation_roots.len() == 1
            && !member_roots
                .iter()
                .any(|root| allocation_roots.contains(root)),
        "allocation roots reused member trust"
    );
    // New member and allocation channels must have distinct credentials.
    ensure!(
        first_cert(&a.ca_pem)? != first_cert(&member.ca_pem)?
            && first_cert(&a.guest_cert_pem)? != first_cert(&member.guest_cert_pem)?
            && key_identity(&a.guest_key_pem)? != key_identity(&member.guest_key_pem)?
            && a.host_pin != member.host_pin,
        "allocation identity reused member credentials"
    );
    a.server_tls(now_ms)?;
    crate::guest_wire::validate_server_identity(
        a.ca_pem.as_bytes(),
        a.guest_cert_pem.as_bytes(),
        a.allocation,
        now_ms,
    )?;
    Ok(())
}
fn key_identity(pem: &str) -> Result<Vec<u8>> {
    let key = PrivateKeyDer::from_pem_slice(pem.as_bytes())?;
    let signer = tokio_rustls::rustls::crypto::ring::sign::any_supported_type(&key)?;
    Ok(signer
        .public_key()
        .context("private key has no public identity")?
        .as_ref()
        .to_vec())
}
fn first_cert(pem: &str) -> Result<CertificateDer<'static>> {
    CertificateDer::pem_slice_iter(pem.as_bytes())
        .next()
        .context("missing certificate")?
        .context("invalid certificate")
}
fn private_regular(file: &File) -> Result<()> {
    let metadata = file.metadata()?;
    ensure!(
        metadata.is_file()
            && metadata.permissions().mode() & 0o777 == 0o600
            && metadata.uid() == rustix::process::geteuid().as_raw(),
        "unsafe retained member state"
    );
    Ok(())
}
fn read_retained(path: &Path) -> Result<Vec<u8>> {
    let fd = rustix::fs::open(
        path,
        rustix::fs::OFlags::RDONLY
            | rustix::fs::OFlags::NOFOLLOW
            | rustix::fs::OFlags::NONBLOCK
            | rustix::fs::OFlags::CLOEXEC,
        rustix::fs::Mode::empty(),
    )?;
    let file = File::from(fd);
    private_regular(&file)?;
    let mut bytes = Vec::new();
    file.take((MAX_FRAME + 1) as u64).read_to_end(&mut bytes)?;
    ensure!(bytes.len() <= MAX_FRAME, "oversized retained member state");
    Ok(bytes)
}
fn persist_binding(directory: &Path, request: &BindRequest) -> Result<()> {
    let bytes = serde_json::to_vec(request)?;
    ensure!(bytes.len() <= MAX_FRAME, "binding too large");
    let mut attempted = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(directory.join("binding.attempted"))?;
    attempted.write_all(b"attempted-v1")?;
    attempted.sync_all()?;
    File::open(directory)?.sync_all()?;
    let pending = directory.join("binding.pending");
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&pending)?;
    file.write_all(&bytes)?;
    file.sync_all()?;
    File::open(directory)?.sync_all()?;
    fs::rename(pending, directory.join("binding.json"))?;
    File::open(directory)?.sync_all()?;
    Ok(())
}

async fn read_json<T: serde::de::DeserializeOwned>(io: &mut (impl AsyncRead + Unpin)) -> Result<T> {
    tokio::time::timeout(IO_TIMEOUT, async {
        let len = io.read_u32().await? as usize;
        ensure!(len > 0 && len <= MAX_FRAME, "invalid member frame size");
        let mut bytes = vec![0; len];
        io.read_exact(&mut bytes).await?;
        serde_json::from_slice(&bytes).map_err(|_| anyhow::anyhow!("invalid member frame"))
    })
    .await
    .context("member frame timeout")?
}

async fn write_json(io: &mut (impl AsyncWrite + Unpin), value: &impl Serialize) -> Result<()> {
    let bytes = serde_json::to_vec(value)?;
    ensure!(
        !bytes.is_empty() && bytes.len() <= MAX_FRAME,
        "invalid member frame size"
    );
    tokio::time::timeout(IO_TIMEOUT, async {
        io.write_u32(bytes.len() as u32).await?;
        io.write_all(&bytes).await?;
        io.flush().await?;
        Ok::<_, anyhow::Error>(())
    })
    .await
    .context("member write timeout")?
}

/// Serve one member connection: one request, one response.
pub async fn serve_member_connection<S: AsyncRead + AsyncWrite + Unpin>(
    guest: &MemberGuest,
    tls: &ServerTls,
    stream: S,
    now_ms: impl Fn() -> i64,
) -> Result<()> {
    tokio::time::timeout(crate::guest_wire::CONNECTION_TIMEOUT, async {
        let mut stream = tls.accept(stream).await?;
        let request: MemberRequest = read_json(&mut stream).await?;
        let response = guest.handle(request, now_ms());
        write_json(&mut stream, &response).await
    })
    .await
    .context("member connection deadline")?
}

/// Host side: one authenticated member call. No automatic retry.
pub async fn member_call<S: AsyncRead + AsyncWrite + Unpin>(
    tls: &ClientTls,
    stream: S,
    request: &MemberRequest,
) -> Result<MemberResponse> {
    tokio::time::timeout(crate::guest_wire::CONNECTION_TIMEOUT, async {
        let mut stream = tls.connect(stream).await?;
        write_json(&mut stream, request).await?;
        let response: MemberResponse = read_json(&mut stream).await?;
        if let MemberResponse::Rejected { reason, .. } = &response {
            ensure!(
                !reason.is_empty() && reason.len() <= MAX_REASON,
                "invalid member rejection"
            );
        }
        Ok(response)
    })
    .await
    .context("member call deadline")?
}
