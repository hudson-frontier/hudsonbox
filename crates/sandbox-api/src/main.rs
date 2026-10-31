//! Operator CLI and HTTPS entry point. There is no public bootstrap endpoint.
use anyhow::Context;
use clap::{Parser, Subcommand};
use sandbox_api::{
    AppState,
    provision::provision,
    router_with_uploads,
    server::{ServerLimits, serve, tls_acceptor},
};
use sandbox_protocol::images::ImageAllowlist;
use sandbox_store::Store;
use std::{net::SocketAddr, path::PathBuf};

#[derive(Parser)]
#[command(
    display_name = "sandbox-api (hudsonbox)",
    version,
    about = "Hudsonbox HTTPS API and offline operator setup"
)]
struct Args {
    #[command(subcommand)]
    command: Command,
}
#[derive(Subcommand)]
enum Command {
    /// Serve existing routes. Requires DATABASE_URL in the environment.
    Serve(Box<ServeArgs>),
    /// Create or resume one project via a private credential file. Requires direct DATABASE_URL access.
    ProvisionProject {
        #[arg(long)]
        name: String,
        #[arg(long)]
        credential_file: PathBuf,
    },
    /// Apply embedded migrations without opening an HTTP listener.
    Migrate,
}
#[derive(clap::Args)]
struct ServeArgs {
    #[arg(long, default_value = "127.0.0.1:8443")]
    bind: SocketAddr,
    /// Optional loopback-only HTTP liveness and database-readiness probes.
    #[arg(long, value_parser = sandbox_api::health::loopback_address)]
    health_bind: Option<SocketAddr>,
    #[arg(long)]
    tls_cert: PathBuf,
    #[arg(long)]
    tls_key: PathBuf,
    #[arg(long, required = true)]
    image_digest: Vec<String>,
    /// Private service-owned S3 JSON configuration; provision read-only credentials.
    #[arg(long)]
    output_config: Option<PathBuf>,
    /// Private source-store credentials for file ingestion.
    #[arg(long)]
    file_source_config: Option<PathBuf>,
    /// Operator-selected live-output host; never accepted from API callers.
    #[arg(long, requires_all = ["live_endpoint", "live_ca_cert", "live_client_cert", "live_client_key"])]
    live_host_id: Option<sandbox_protocol::HostId>,
    #[arg(long, requires = "live_host_id")]
    live_endpoint: Option<String>,
    /// Route live reads to each allocation owner through the host registry.
    #[arg(long, requires_all = ["live_ca_cert", "live_client_cert", "live_client_key"])]
    live_registry_routing: bool,
    #[arg(long)]
    live_ca_cert: Option<PathBuf>,
    #[arg(long)]
    live_client_cert: Option<PathBuf>,
    #[arg(long)]
    live_client_key: Option<PathBuf>,
    /// Explicit development opt-in for a configured fake reader host.
    #[arg(long)]
    allow_simulated_live: bool,
    #[command(flatten)]
    files: Box<sandbox_api::files::client::FileReaderArgs>,
}
async fn store() -> anyhow::Result<Store> {
    let database_url =
        std::env::var("DATABASE_URL").map_err(|_| anyhow::anyhow!("DATABASE_URL is required"))?;
    let store = Store::connect(&database_url, 16)
        .await
        .map_err(|_| anyhow::anyhow!("invalid database configuration"))?;
    store
        .migrate()
        .await
        .map_err(|_| anyhow::anyhow!("database migration failed"))?;
    Ok(store)
}
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    // Fixed filters deliberately avoid wire-level debug logs containing request headers or SQL binds.
    tracing_subscriber::fmt()
        .with_target(false)
        .with_ansi(false)
        .with_max_level(tracing::Level::INFO)
        .with_writer(std::io::stderr)
        .init();
    match args.command {
        Command::Serve(args) => {
            let ServeArgs {
                bind,
                health_bind,
                tls_cert,
                tls_key,
                image_digest,
                output_config,
                file_source_config,
                live_host_id,
                live_endpoint,
                live_registry_routing,
                live_ca_cert,
                live_client_cert,
                live_client_key,
                allow_simulated_live,
                files,
            } = *args;
            let images = ImageAllowlist::new(image_digest)?;
            let acceptor = tls_acceptor(
                &std::fs::read(tls_cert).context("reading TLS certificate")?,
                &std::fs::read(tls_key).context("reading TLS key")?,
            )?;
            let shutdown = shutdown_signal()?;
            let store = store().await?;
            let output = load_output(output_config.as_deref())?;
            let live = if live_host_id.is_none() && !live_registry_routing {
                anyhow::ensure!(
                    live_ca_cert.is_none()
                        && live_client_cert.is_none()
                        && live_client_key.is_none()
                        && !allow_simulated_live,
                    "live reader options require --live-host-id or --live-registry-routing"
                );
                None
            } else {
                let read = |path: Option<PathBuf>| -> anyhow::Result<Vec<u8>> {
                    use std::io::Read;
                    let path = path.context("live TLS file required")?;
                    let mut bytes = Vec::new();
                    std::fs::File::open(path)
                        .context("opening live TLS file")?
                        .take(65537)
                        .read_to_end(&mut bytes)
                        .context("reading live TLS file")?;
                    anyhow::ensure!(bytes.len() <= 65536, "live TLS file too large");
                    Ok(bytes)
                };
                let legacy = match live_host_id {
                    Some(host) => Some((host, live_endpoint.context("live endpoint required")?)),
                    None => None,
                };
                Some(
                    std::sync::Arc::new(sandbox_api::streams::live::LiveClient::routed(
                        live_registry_routing.then(|| store.clone()),
                        legacy,
                        &read(live_ca_cert)?,
                        &read(live_client_cert)?,
                        &read(live_client_key)?,
                        allow_simulated_live,
                    )?)
                        as std::sync::Arc<dyn sandbox_api::streams::live::LiveReader>,
                )
            };
            let files = (*files).build(&store)?;
            let sources = file_source_config
                .map(|p| sandbox_artifacts::S3Config::read_private(&p)?.build_sources())
                .transpose()?
                .map(|v| {
                    std::sync::Arc::new(v)
                        as std::sync::Arc<dyn sandbox_artifacts::sources::SourceBackend>
                });
            let listener = tokio::net::TcpListener::bind(bind)
                .await
                .context("binding HTTPS listener")?;
            let health_listener = match health_bind {
                Some(address) => Some(
                    tokio::net::TcpListener::bind(address)
                        .await
                        .context("binding health listener")?,
                ),
                None => None,
            };
            tracing::info!(address = %listener.local_addr()?, "sandbox API listening over HTTPS");
            let app = router_with_uploads(
                AppState {
                    store: store.clone(),
                    images,
                },
                output,
                live,
                files,
                sources,
            );
            match health_listener {
                Some(health_listener) => {
                    tracing::info!(health_address = %health_listener.local_addr()?, "local API health listener ready");
                    sandbox_api::health::serve(
                        listener,
                        acceptor,
                        app,
                        ServerLimits::default(),
                        health_listener,
                        store,
                        shutdown,
                    )
                    .await?;
                }
                None => serve(listener, acceptor, app, ServerLimits::default(), shutdown).await?,
            }
        }
        Command::ProvisionProject {
            name,
            credential_file,
        } => {
            let store = store().await?;
            let project = provision(&store, &name, &credential_file).await?;
            // Only nonsecret metadata reaches stdout; the token lives exclusively in the private file.
            println!(
                "{}",
                serde_json::json!({"project_id":project,"credential_file":credential_file})
            );
        }
        Command::Migrate => {
            store().await?;
        }
    }
    Ok(())
}
#[cfg(unix)]
fn shutdown_signal() -> anyhow::Result<impl Future<Output = ()>> {
    use tokio::signal::unix::{SignalKind, signal};
    let mut interrupt = signal(SignalKind::interrupt()).context("registering SIGINT")?;
    let mut terminate = signal(SignalKind::terminate()).context("registering SIGTERM")?;
    Ok(async move {
        tokio::select! { _ = interrupt.recv() => {}, _ = terminate.recv() => {} }
    })
}
#[cfg(not(unix))]
fn shutdown_signal() -> anyhow::Result<impl Future<Output = ()>> {
    Ok(async {
        let _ = tokio::signal::ctrl_c().await;
    })
}

fn load_output(
    path: Option<&std::path::Path>,
) -> anyhow::Result<Option<std::sync::Arc<dyn sandbox_api::outputs::OutputReader>>> {
    let Some(path) = path else {
        return Ok(None);
    };
    #[cfg(unix)]
    {
        Ok(Some(std::sync::Arc::new(
            sandbox_artifacts::S3Config::read_private(path)?.build()?,
        )))
    }
    #[cfg(not(unix))]
    {
        let _ = path;
        anyhow::bail!("private output configuration requires Unix")
    }
}
