//! Loopback-only, authenticated development supervisor. No customer code is run.
use anyhow::{Context, bail};
use clap::Parser;
use sandbox_fake_host::{FakeConfig, FakeHost};
use sandbox_protocol::{
    HostId,
    supervisor::{Resources, supervisor_server::SupervisorServer},
};
use sandbox_supervisor::transport::{ControllerIdentity, MAX_MESSAGE_BYTES, server_tls};
use std::{collections::BTreeSet, net::SocketAddr, path::PathBuf, time::Duration};
use tonic::transport::Server;

#[derive(Debug, Parser)]
#[command(
    display_name = "sandbox-fake-host (hudsonbox)",
    version,
    about = "Simulated supervisor over mTLS; never starts a VM or process"
)]
struct Args {
    #[arg(long, default_value = "127.0.0.1:7443")]
    listen: SocketAddr,
    #[arg(long)]
    host_id: HostId,
    /// Obtain a new epoch from the control plane after every restart.
    #[arg(long)]
    supervisor_epoch: i64,
    #[arg(long)]
    ca_cert: PathBuf,
    #[arg(long)]
    server_cert: PathBuf,
    #[arg(long)]
    server_key: PathBuf,
    /// SHA-256 of the DER-encoded controller leaf certificate. Repeat for rotation.
    #[arg(long, required = true)]
    controller_cert_sha256: Vec<String>,
    /// SHA-256 of a distinct read-only API client certificate. Repeat for rotation.
    #[arg(long)]
    output_reader_cert_sha256: Vec<String>,
    /// SHA-256 of an authorized file-reader certificate, distinct from the controller.
    #[arg(long)]
    file_reader_cert_sha256: Vec<String>,
    #[arg(long, required = true)]
    image_digest: Vec<String>,
    #[arg(long, default_value_t = 4)]
    vcpu: u32,
    #[arg(long, default_value_t = 8192)]
    memory_mib: u64,
    #[arg(long, default_value_t = 65536)]
    disk_mib: u64,
    #[arg(long)]
    output_config: Option<PathBuf>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    if !args.listen.ip().is_loopback() {
        bail!("the fake supervisor only listens on loopback");
    }
    let pins = args
        .controller_cert_sha256
        .iter()
        .map(|p| {
            let mut bytes = [0u8; 32];
            hex::decode_to_slice(p, &mut bytes)
                .context("invalid controller certificate fingerprint")?;
            Ok(bytes)
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    let identity = ControllerIdentity::new(pins)?;
    let reader_identity = if args.output_reader_cert_sha256.is_empty() {
        None
    } else {
        let pins = args
            .output_reader_cert_sha256
            .iter()
            .map(|p| {
                let mut bytes = [0u8; 32];
                hex::decode_to_slice(p, &mut bytes)?;
                Ok(bytes)
            })
            .collect::<anyhow::Result<Vec<_>>>()?;
        Some(ControllerIdentity::output_reader(pins, &identity)?)
    };
    let file_identity = if args.file_reader_cert_sha256.is_empty() {
        None
    } else {
        let pins = args
            .file_reader_cert_sha256
            .iter()
            .map(|p| {
                let mut bytes = [0u8; 32];
                hex::decode_to_slice(p, &mut bytes)?;
                Ok(bytes)
            })
            .collect::<anyhow::Result<Vec<_>>>()?;
        Some(ControllerIdentity::output_reader(pins, &identity)?)
    };
    let mut host = FakeHost::new(FakeConfig {
        host: args.host_id,
        epoch: args.supervisor_epoch,
        images: BTreeSet::from_iter(args.image_digest),
        capacity: Resources {
            vcpu: args.vcpu,
            memory_mib: args.memory_mib,
            disk_mib: args.disk_mib,
        },
    })?;
    if let Some(path) = args.output_config {
        host = host.with_artifacts(sandbox_artifacts::S3Config::read_private(&path)?.build()?);
    }
    let reader_service = reader_identity.map(|identity| {
        let service =
            sandbox_protocol::supervisor::live_output_server::LiveOutputServer::new(host.clone())
                .max_decoding_message_size(MAX_MESSAGE_BYTES)
                .max_encoding_message_size(MAX_MESSAGE_BYTES);
        tonic::service::interceptor::InterceptedService::new(service, identity)
    });
    let file_service = file_identity.map(|identity| {
        let service =
            sandbox_protocol::supervisor::file_downloads_server::FileDownloadsServer::new(
                host.clone(),
            )
            .max_decoding_message_size(MAX_MESSAGE_BYTES)
            .max_encoding_message_size(MAX_MESSAGE_BYTES);
        tonic::service::interceptor::InterceptedService::new(service, identity)
    });
    let service = SupervisorServer::new(host.clone())
        .max_decoding_message_size(MAX_MESSAGE_BYTES)
        .max_encoding_message_size(MAX_MESSAGE_BYTES);
    let service = tonic::service::interceptor::InterceptedService::new(service, identity);
    let tls = server_tls(
        &std::fs::read(args.ca_cert)?,
        &std::fs::read(args.server_cert)?,
        &std::fs::read(args.server_key)?,
    );
    let watchdog = tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_millis(100));
        loop {
            interval.tick().await;
            host.expire_leases().await;
        }
    });
    eprintln!(
        "simulated supervisor listening on {}; no VM or process execution",
        args.listen
    );
    let result = Server::builder()
        .tls_config(tls)?
        .add_service(service)
        .add_optional_service(reader_service)
        .add_optional_service(file_service)
        .serve_with_shutdown(args.listen, async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await;
    watchdog.abort();
    result.context("fake supervisor stopped with an error")
}
