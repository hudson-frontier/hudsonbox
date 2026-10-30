//! Read-only transport. All endpoints and TLS material are selected by the operator.
use crate::problem::Problem;
use sandbox_protocol::{
    HostId,
    supervisor::{
        FileAccessObservation, FileCaptureRequest, FileDownloadRequest, FileReleaseRequest,
    },
};
use std::{fmt, future::Future, pin::Pin};
pub type Reply<'a> =
    Pin<Box<dyn Future<Output = Result<FileAccessObservation, Problem>> + Send + 'a>>;
pub trait FileReader: fmt::Debug + Send + Sync {
    fn capture(&self, host: HostId, request: FileCaptureRequest) -> Reply<'_>;
    fn read(&self, host: HostId, request: FileDownloadRequest) -> Reply<'_>;
    fn release(&self, host: HostId, request: FileReleaseRequest) -> Reply<'_>;
}
pub struct FileClient {
    routes: crate::owners::OwnerRoutes,
    allow_simulated: bool,
}
impl fmt::Debug for FileClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FileClient")
            .field("routes", &self.routes)
            .finish_non_exhaustive()
    }
}
impl FileClient {
    /// Legacy single-host reader.
    pub fn new(
        host: HostId,
        endpoint: String,
        ca: Vec<u8>,
        cert: Vec<u8>,
        key: Vec<u8>,
        allow_simulated: bool,
    ) -> anyhow::Result<Self> {
        Self::routed(
            None,
            Some((host, endpoint)),
            &ca,
            &cert,
            &key,
            allow_simulated,
        )
    }
    /// Registry routing to each allocation owner, optionally keeping one legacy host.
    pub fn routed(
        store: Option<sandbox_store::Store>,
        legacy: Option<(HostId, String)>,
        ca: &[u8],
        cert: &[u8],
        key: &[u8],
        allow_simulated: bool,
    ) -> anyhow::Result<Self> {
        Ok(Self {
            routes: crate::owners::OwnerRoutes::new(store, legacy, ca, cert, key)
                .map_err(|e| anyhow::anyhow!("file reader: {e}"))?,
            allow_simulated,
        })
    }
    async fn call<F, Fut>(&self, host: HostId, call: F) -> Result<FileAccessObservation, Problem>
    where
        F: FnOnce(
            sandbox_protocol::supervisor::file_downloads_client::FileDownloadsClient<
                tonic::transport::Channel,
            >,
        ) -> Fut,
        Fut: Future<Output = Result<tonic::Response<FileAccessObservation>, tonic::Status>>,
    {
        let route = self.routes.route(host).await?;
        let client = sandbox_protocol::supervisor::file_downloads_client::FileDownloadsClient::new(
            route.lease.channel.clone(),
        )
        .max_decoding_message_size(sandbox_supervisor::transport::MAX_MESSAGE_BYTES)
        .max_encoding_message_size(sandbox_supervisor::transport::MAX_MESSAGE_BYTES);
        let response = call(client).await;
        self.routes.observe(&route, &response);
        self.response(response)
    }
    fn response(
        &self,
        response: Result<tonic::Response<FileAccessObservation>, tonic::Status>,
    ) -> Result<FileAccessObservation, Problem> {
        let reply = response
            .map_err(|e| match e.code() {
                tonic::Code::NotFound | tonic::Code::FailedPrecondition => Problem::FileMissing,
                _ => Problem::Unavailable,
            })?
            .into_inner();
        if reply.simulated && !self.allow_simulated {
            return Err(Problem::Unavailable);
        }
        // The echoed scope (host, epoch, allocation) is compared by the caller.
        Ok(reply)
    }
}
impl FileReader for FileClient {
    fn capture(&self, host: HostId, r: FileCaptureRequest) -> Reply<'_> {
        Box::pin(self.call(host, |mut c| async move { c.capture(r).await }))
    }
    fn read(&self, host: HostId, r: FileDownloadRequest) -> Reply<'_> {
        Box::pin(self.call(host, |mut c| async move { c.read(r).await }))
    }
    fn release(&self, host: HostId, r: FileReleaseRequest) -> Reply<'_> {
        Box::pin(self.call(host, |mut c| async move { c.release(r).await }))
    }
}

/// Optional operator wiring. API callers never supply an endpoint or TLS material.
#[derive(Debug, clap::Args)]
pub struct FileReaderArgs {
    /// Legacy host addressed by `--file-endpoint` while its registry transport is NULL.
    #[arg(long, requires_all=["file_endpoint","file_ca_cert","file_client_cert","file_client_key"])]
    file_host_id: Option<HostId>,
    #[arg(long, requires = "file_host_id")]
    file_endpoint: Option<String>,
    /// Route file reads to each allocation owner through the host registry.
    #[arg(long, requires_all=["file_ca_cert","file_client_cert","file_client_key"])]
    file_registry_routing: bool,
    #[arg(long)]
    file_ca_cert: Option<std::path::PathBuf>,
    #[arg(long)]
    file_client_cert: Option<std::path::PathBuf>,
    #[arg(long)]
    file_client_key: Option<std::path::PathBuf>,
    #[arg(long)]
    allow_simulated_files: bool,
}
impl FileReaderArgs {
    pub fn build(
        self,
        store: &sandbox_store::Store,
    ) -> anyhow::Result<Option<std::sync::Arc<dyn FileReader>>> {
        if self.file_host_id.is_none() && !self.file_registry_routing {
            anyhow::ensure!(
                self.file_ca_cert.is_none()
                    && self.file_client_cert.is_none()
                    && self.file_client_key.is_none()
                    && !self.allow_simulated_files,
                "file reader options require --file-host-id or --file-registry-routing"
            );
            return Ok(None);
        }
        let read = |path: Option<std::path::PathBuf>| -> anyhow::Result<Vec<u8>> {
            use std::io::Read;
            let path = path.ok_or_else(|| anyhow::anyhow!("file reader TLS path required"))?;
            let mut bytes = Vec::new();
            std::fs::File::open(path)?
                .take(65537)
                .read_to_end(&mut bytes)?;
            anyhow::ensure!(bytes.len() <= 65536, "file reader TLS material too large");
            Ok(bytes)
        };
        let legacy = match self.file_host_id {
            Some(host) => Some((
                host,
                self.file_endpoint
                    .ok_or_else(|| anyhow::anyhow!("file endpoint required"))?,
            )),
            None => None,
        };
        Ok(Some(std::sync::Arc::new(FileClient::routed(
            self.file_registry_routing.then(|| store.clone()),
            legacy,
            &read(self.file_ca_cert)?,
            &read(self.file_client_cert)?,
            &read(self.file_client_key)?,
            self.allow_simulated_files,
        )?)))
    }
}
