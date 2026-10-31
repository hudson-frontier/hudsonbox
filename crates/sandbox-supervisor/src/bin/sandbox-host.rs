//! Root-operated real Linux lifecycle service. Every network call requires mTLS.
#[cfg(not(target_os = "linux"))]
fn main() {
    eprintln!("sandbox-host requires Linux");
    std::process::exit(1);
}
#[cfg(target_os = "linux")]
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    use clap::Parser;
    use sandbox_protocol::supervisor::supervisor_server::SupervisorServer;
    use sandbox_supervisor::{
        guardian,
        host::{Config, Host},
        transport::{ControllerIdentity, MAX_MESSAGE_BYTES, server_tls},
    };
    use std::{net::SocketAddr, path::PathBuf, time::Duration};
    #[derive(Parser)]
    #[command(display_name = "sandbox-host (hudsonbox)", version)]
    struct Args {
        #[arg(long)]
        config: PathBuf,
        #[arg(long, default_value = "127.0.0.1:7443")]
        listen: SocketAddr,
        #[arg(long)]
        ca_cert: PathBuf,
        #[arg(long)]
        server_cert: PathBuf,
        #[arg(long)]
        server_key: PathBuf,
        /// SHA-256 of an authorized controller's DER leaf certificate. Repeat for rotation.
        #[arg(long, required = true)]
        controller_cert_sha256: Vec<String>,
        /// SHA-256 of a distinct read-only API client certificate. Repeat for rotation.
        #[arg(long)]
        output_reader_cert_sha256: Vec<String>,
        /// SHA-256 of an authorized file-reader certificate, distinct from the controller.
        #[arg(long)]
        file_reader_cert_sha256: Vec<String>,
        /// Private service-owned JSON file with operator S3 credentials/configuration.
        #[arg(long)]
        output_config: Option<PathBuf>,
    }
    let args = Args::parse();
    let pins = args
        .controller_cert_sha256
        .iter()
        .map(|p| {
            let mut bytes = [0u8; 32];
            hex::decode_to_slice(p, &mut bytes)?;
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
    let tls = server_tls(
        &std::fs::read(args.ca_cert)?,
        &std::fs::read(args.server_cert)?,
        &std::fs::read(args.server_key)?,
    );
    let config: Config = guardian::read_json(&args.config)?;
    let artifacts = args
        .output_config
        .as_deref()
        .map(sandbox_artifacts::S3Config::read_private)
        .transpose()?
        .map(sandbox_artifacts::S3Config::build)
        .transpose()?;
    let host = Host::open_with_artifacts(config, artifacts)?;
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
    let maintenance = tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_millis(100));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            interval.tick().await;
            let _ = host.reconcile_one().await;
        }
    });
    eprintln!("real supervisor listening on {}", args.listen);
    let result = tonic::transport::Server::builder()
        .tls_config(tls)?
        .add_service(service)
        .add_optional_service(reader_service)
        .add_optional_service(file_service)
        .serve_with_shutdown(args.listen, async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await;
    maintenance.abort();
    result.map_err(Into::into)
}
