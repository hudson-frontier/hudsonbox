//! Finite read-only audit of an offline database/storage recovery set.
#[cfg(unix)]
mod unix {
    use clap::Parser;
    use sandbox_artifacts::S3Config;
    use sandbox_store::Store;
    use std::{
        io::Read,
        os::unix::fs::{MetadataExt, OpenOptionsExt},
        path::{Path, PathBuf},
        time::Duration,
    };

    #[derive(Parser)]
    #[command(
        display_name = "sandbox-storage-audit (hudsonbox)",
        version,
        about = "Verify offline recovery storage evidence without modifying the database or storage"
    )]
    pub(super) struct Args {
        /// Private owned JSON {"version":1,"database_url":"..."} for the quarantined database.
        #[arg(long)]
        database_config: PathBuf,
        /// Private output bucket configuration with read-only GET/HEAD credentials.
        #[arg(long, required_unless_present = "file_source_config")]
        output_config: Option<PathBuf>,
        /// Private source bucket configuration with read-only GET/HEAD credentials.
        #[arg(long)]
        file_source_config: Option<PathBuf>,
        /// Bound the complete snapshot and all storage requests; timeout is audit failure.
        #[arg(long, default_value_t = 300, value_parser = clap::value_parser!(u32).range(1..=3600))]
        timeout_seconds: u32,
        /// Development only: allow explicitly simulated output metadata.
        #[arg(long)]
        allow_simulated: bool,
    }
    #[derive(serde::Deserialize)]
    #[serde(deny_unknown_fields)]
    struct DatabaseConfig {
        version: u32,
        database_url: String,
    }

    fn database_config(path: &Path) -> anyhow::Result<DatabaseConfig> {
        let invalid = || anyhow::anyhow!("private database configuration required");
        let file = std::fs::OpenOptions::new()
            .read(true)
            .custom_flags(
                (rustix::fs::OFlags::NOFOLLOW | rustix::fs::OFlags::NONBLOCK).bits() as i32,
            )
            .open(path)
            .map_err(|_| invalid())?;
        let meta = file.metadata().map_err(|_| invalid())?;
        anyhow::ensure!(
            meta.is_file()
                && meta.len() <= 65536
                && meta.uid() == rustix::process::geteuid().as_raw()
                && meta.mode() & 0o077 == 0,
            "private database configuration required"
        );
        let mut bytes = Vec::new();
        file.take(65537)
            .read_to_end(&mut bytes)
            .map_err(|_| invalid())?;
        anyhow::ensure!(
            bytes.len() <= 65536,
            "private database configuration required"
        );
        let config: DatabaseConfig = serde_json::from_slice(&bytes).map_err(|_| invalid())?;
        anyhow::ensure!(
            config.version == 1 && !config.database_url.is_empty(),
            "private database configuration required"
        );
        Ok(config)
    }

    async fn run(args: &Args) -> anyhow::Result<sandbox_cleanup::audit::Report> {
        let config = database_config(&args.database_config)?;
        let outputs = args
            .output_config
            .as_deref()
            .map(|p| S3Config::read_private(p)?.build())
            .transpose()?;
        let sources = args
            .file_source_config
            .as_deref()
            .map(|p| S3Config::read_private(p)?.build_sources())
            .transpose()?;
        let store = Store::connect(&config.database_url, 1)
            .await
            .map_err(|_| anyhow::anyhow!("invalid database configuration"))?;
        Ok(sandbox_cleanup::audit::verify(
            &store,
            outputs.as_ref(),
            sources.as_ref(),
            args.allow_simulated,
        )
        .await?)
    }

    pub(super) async fn main() {
        let args = Args::parse();
        let result = tokio::time::timeout(
            Duration::from_secs(u64::from(args.timeout_seconds)),
            run(&args),
        )
        .await;
        match result {
            Ok(Ok(report)) => println!(
                "{}",
                serde_json::json!({"version":1,"scope":"storage_evidence_only","verified":report})
            ),
            error => {
                match error {
                    Ok(Err(error)) => eprintln!("storage audit failed: {error}"),
                    _ => eprintln!(
                        "storage audit failed: overall deadline exceeded; keep recovery quarantined"
                    ),
                }
                std::process::exit(1);
            }
        }
    }
}

#[cfg(unix)]
#[tokio::main]
async fn main() {
    unix::main().await;
}

#[cfg(not(unix))]
fn main() {
    eprintln!("storage audit requires Unix private operator configuration");
    std::process::exit(1);
}
