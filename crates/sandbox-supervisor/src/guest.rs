//! Authenticated host-to-guest calls over Firecracker's operator-owned Unix socket.
//! No automatic retry: a lost response cannot determine whether a command ran.
mod files;
use anyhow::{Context as _, Result, ensure};
use sandbox_protocol::{
    Id, OperationId, guest as w, guest_model as m,
    guest_wire::{self as wire, ClientTls},
};
use std::path::PathBuf;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::UnixStream,
};

/// Open one stream to a guest vsock port through Firecracker's hybrid
/// Unix-socket CONNECT handshake. Authentication is the caller's TLS.
async fn connect_vsock(socket: &std::path::Path, port: u32) -> Result<UnixStream> {
    use std::os::unix::fs::FileTypeExt;
    ensure!(
        std::fs::symlink_metadata(socket)?.file_type().is_socket(),
        "guest endpoint is not a Unix socket"
    );
    let mut socket = UnixStream::connect(socket).await?;
    socket
        .write_all(format!("CONNECT {port}\n").as_bytes())
        .await?;
    let mut reply = Vec::new();
    for _ in 0..16 {
        let b = socket.read_u8().await?;
        reply.push(b);
        if b == b'\n' {
            break;
        }
    }
    let reply = std::str::from_utf8(&reply)?;
    let assigned = reply
        .strip_prefix("OK ")
        .and_then(|s| s.strip_suffix('\n'))
        .context("invalid Firecracker CONNECT acknowledgement")?;
    ensure!(
        !assigned.is_empty()
            && assigned.bytes().all(|b| b.is_ascii_digit())
            && assigned.parse::<u32>()? > 0,
        "invalid assigned vsock port"
    );
    Ok(socket)
}

/// One authenticated call to an unbound warm member's bind endpoint
/// ([`MEMBER_PORT`](sandbox_protocol::warm_bind::MEMBER_PORT)) through the
/// operator-owned jail directory. No automatic retry.
#[cfg(target_os = "linux")]
pub async fn member_call(
    directory: PathBuf,
    tls: &ClientTls,
    request: &sandbox_protocol::warm_bind::MemberRequest,
) -> Result<sandbox_protocol::warm_bind::MemberResponse> {
    use std::os::{fd::AsRawFd, unix::fs::OpenOptionsExt};
    ensure!(directory.is_absolute(), "absolute guest directory required");
    let file = std::fs::OpenOptions::new()
        .read(true)
        .custom_flags((rustix::fs::OFlags::DIRECTORY | rustix::fs::OFlags::NOFOLLOW).bits() as i32)
        .open(directory)?;
    let path = PathBuf::from(format!("/proc/self/fd/{}/vsock.sock", file.as_raw_fd()));
    tokio::time::timeout(wire::CONNECTION_TIMEOUT, async {
        let socket = connect_vsock(&path, sandbox_protocol::warm_bind::MEMBER_PORT).await?;
        sandbox_protocol::warm_bind::member_call(tls, socket, request).await
    })
    .await
    .context("member call uncertain: connection deadline")?
}

#[derive(Debug, Clone)]
pub struct GuestClient {
    socket: PathBuf,
    port: u32,
    tls: ClientTls,
    context: m::Context,
    _socket_directory: Option<std::sync::Arc<std::fs::File>>,
}
impl GuestClient {
    pub fn context(&self) -> &m::Context {
        &self.context
    }
    pub fn new(socket: PathBuf, port: u32, tls: ClientTls, context: m::Context) -> Result<Self> {
        ensure!(
            socket.is_absolute() && port > 0 && port < u32::MAX && context.generation > 0,
            "invalid guest endpoint"
        );
        Ok(Self {
            socket,
            port,
            tls,
            context,
            _socket_directory: None,
        })
    }
    /// Pin the operator-owned jail directory and keep a short Unix socket path.
    /// Allocation paths can exceed sockaddr_un even when the control path fits.
    #[cfg(target_os = "linux")]
    pub fn from_firecracker_directory(
        directory: PathBuf,
        port: u32,
        tls: ClientTls,
        context: m::Context,
    ) -> Result<Self> {
        use std::os::{fd::AsRawFd, unix::fs::OpenOptionsExt};
        ensure!(directory.is_absolute(), "absolute guest directory required");
        let file = std::fs::OpenOptions::new()
            .read(true)
            .custom_flags(
                (rustix::fs::OFlags::DIRECTORY | rustix::fs::OFlags::NOFOLLOW).bits() as i32,
            )
            .open(directory)?;
        let path = PathBuf::from(format!("/proc/self/fd/{}/vsock.sock", file.as_raw_fd()));
        let mut client = Self::new(path, port, tls, context)?;
        client._socket_directory = Some(std::sync::Arc::new(file));
        Ok(client)
    }
    async fn call(&self, action: w::request::Action) -> Result<(m::Context, w::response::Result)> {
        use w::response::Result as R;
        let hello = matches!(action, w::request::Action::Hello(_));
        ensure!(
            hello || !self.context.boot_id.is_empty(),
            "discover and bind guest boot identity before commands"
        );
        tokio::time::timeout(wire::CONNECTION_TIMEOUT, async {
            let socket = connect_vsock(&self.socket, self.port).await?;
            let mut tls = self.tls.connect(socket).await?;
            let request_id = OperationId::generate().to_string();
            let request = w::Request {
                version: wire::VERSION,
                request_id: request_id.clone(),
                context: Some((&self.context).into()),
                action: Some(action),
            };
            wire::write_frame(&mut tls, &request).await?;
            let response: w::Response = wire::read_frame(&mut tls).await?;
            ensure!(
                response.version == wire::VERSION && response.request_id == request_id,
                "guest response mismatch"
            );
            let context: m::Context = response
                .context
                .context("missing response context")?
                .try_into()?;
            ensure!(
                context.allocation_id == self.context.allocation_id
                    && context.generation == self.context.generation
                    && !context.boot_id.is_empty(),
                "guest response ownership mismatch"
            );
            ensure!(
                context.boot_id == self.context.boot_id
                    || (hello && self.context.boot_id.is_empty()),
                "guest boot changed"
            );
            let result = response.result.context("missing response result")?;
            if let R::Error(error) = result {
                let code = w::ErrorCode::try_from(error.code)?;
                ensure!(code != w::ErrorCode::Unspecified, "unspecified guest error");
                anyhow::bail!("guest reported request error: {code:?}; inspect before any retry");
            }
            Ok((context, result))
        })
        .await
        .context("guest call uncertain: connection deadline")?
    }
    /// Discover boot identity without implicitly authorizing commands in a new boot.
    /// The host must persist its independent floor before calling this method.
    /// A lost reply is reconciled by repeating retirement, never by a floor-only read.
    pub async fn retire_history(
        &self,
        barrier: &sandbox_protocol::history::Barrier,
    ) -> Result<sandbox_protocol::history::Barrier> {
        barrier.validate(&self.context, barrier.domain)?;
        let w::response::Result::HistoryRetired(value) = self
            .call(w::request::Action::RetireHistory(barrier.into()))
            .await?
            .1
        else {
            anyhow::bail!("unexpected history response")
        };
        let acknowledged: sandbox_protocol::history::Barrier = value.try_into()?;
        // A compromised or stale guest cannot expand the controller-approved prefix.
        ensure!(&acknowledged == barrier, "history acknowledgement changed");
        Ok(acknowledged)
    }
    pub async fn hello(&self) -> Result<m::Context> {
        let (context, result) = self.call(w::request::Action::Hello(w::Hello {})).await?;
        ensure!(
            matches!(result, w::response::Result::Hello(_)),
            "unexpected hello response"
        );
        Ok(context)
    }
    fn receipt(&self, receipt: w::Receipt, id: OperationId) -> Result<m::Receipt> {
        let receipt: m::Receipt = receipt.try_into()?;
        ensure!(
            receipt.operation_id == id && receipt.context == self.context,
            "guest receipt ownership mismatch"
        );
        Ok(receipt)
    }
    pub async fn execute(&self, request: &m::Execute) -> Result<m::Receipt> {
        request.validate()?;
        let w::response::Result::Receipt(receipt) = self
            .call(w::request::Action::Execute(request.into()))
            .await?
            .1
        else {
            anyhow::bail!("unexpected execute response")
        };
        let receipt = self.receipt(receipt, request.operation_id)?;
        ensure!(
            receipt.digest == request.digest()?
                && receipt.deadline_unix_ms == request.deadline_unix_ms
                && receipt.output_limit == request.output_limit,
            "guest receipt payload mismatch"
        );
        Ok(receipt)
    }
    pub async fn inspect(&self, id: OperationId) -> Result<m::Receipt> {
        let w::response::Result::Receipt(receipt) = self
            .call(w::request::Action::Inspect(w::Operation {
                operation_id: id.to_string(),
            }))
            .await?
            .1
        else {
            anyhow::bail!("unexpected inspect response")
        };
        self.receipt(receipt, id)
    }
    pub async fn cancel(&self, id: OperationId) -> Result<m::Receipt> {
        let w::response::Result::Receipt(receipt) = self
            .call(w::request::Action::Cancel(w::Operation {
                operation_id: id.to_string(),
            }))
            .await?
            .1
        else {
            anyhow::bail!("unexpected cancel response")
        };
        self.receipt(receipt, id)
    }
    pub async fn output(&self, request: w::ReadOutput) -> Result<w::OutputChunk> {
        request.operation_id.parse::<OperationId>()?;
        ensure!(
            request.limit > 0
                && request.limit <= wire::MAX_CHUNK
                && request.offset <= m::MAX_OUTPUT
                && matches!(
                    w::Stream::try_from(request.stream)?,
                    w::Stream::Stdout | w::Stream::Stderr
                ),
            "invalid output request"
        );
        let w::response::Result::Output(output) = self
            .call(w::request::Action::Output(request.clone()))
            .await?
            .1
        else {
            anyhow::bail!("unexpected output response")
        };
        ensure!(
            output.operation_id == request.operation_id
                && output.stream == request.stream
                && output.offset == request.offset
                && output.data.len() <= request.limit as usize
                && request.offset.checked_add(output.data.len() as u64) == Some(output.next_offset)
                && output.next_offset <= m::MAX_OUTPUT,
            "invalid output response"
        );
        Ok(output)
    }
}
