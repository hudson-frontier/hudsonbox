//! Opt-in cleanup service. Credentials and deployment are operator-owned.
use clap::Parser;
use sandbox_artifacts::S3Config;
use sandbox_cleanup::{
    Cleaner, CleanupTick,
    sources::{SourceCleaner, SourceCleanupTick},
};
use sandbox_store::Store;
use sandbox_store::retention::ResponseRetention;
use std::{path::PathBuf, time::Duration};

#[derive(Parser)]
#[command(display_name = "sandbox-cleanup (hudsonbox)", version)]
struct Args {
    #[arg(long, env = "DATABASE_URL", hide_env_values = true)]
    database_url: String,
    /// Private operator credential file for the retained-output bucket.
    #[arg(long, required_unless_present = "file_source_config")]
    output_config: Option<PathBuf>,
    /// Opt in to retirement of expired file sources in this private bucket.
    #[arg(long)]
    file_source_config: Option<PathBuf>,
    /// Perform one bounded discovery/cleanup tick, then exit.
    #[arg(long)]
    once: bool,
    /// Development only: permit retirement of explicitly simulated output.
    #[arg(long, default_value_t = false)]
    allow_simulated: bool,
    /// Opt in to terminal response expiry, measured from completion. Applies
    /// to existing completed work too; never changes already assigned deadlines.
    #[arg(long, requires="output_config", value_parser = clap::value_parser!(u32).range(1..=31_536_000))]
    response_retention_seconds: Option<u32>,
    /// Remove eligible expired request/result bodies, preserving retry and
    /// recovery evidence. Disabled by default; removal cannot be undone.
    #[arg(long, requires = "output_config", default_value_t = false)]
    compact_payloads: bool,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let retirer = args
        .output_config
        .as_ref()
        .map(|p| S3Config::read_private(p)?.build_retirer())
        .transpose()?;
    let source_retirer = args
        .file_source_config
        .as_ref()
        .map(|p| S3Config::read_private(p)?.build_source_retirer())
        .transpose()?;
    let store = Store::connect(&args.database_url, 4)
        .await
        .map_err(|_| anyhow::anyhow!("invalid database configuration"))?;
    store
        .migrate()
        .await
        .map_err(|_| anyhow::anyhow!("database migration failed"))?;
    let mut cleaner = retirer.map(|r| Cleaner::new(store.clone(), r, args.allow_simulated));
    if let Some(worker) = cleaner.take() {
        let worker = if let Some(seconds) = args.response_retention_seconds {
            worker.with_response_retention(
                ResponseRetention::new(seconds)
                    .ok_or_else(|| anyhow::anyhow!("invalid response retention policy"))?,
            )
        } else {
            worker
        };
        cleaner = Some(if args.compact_payloads {
            worker.with_payload_compaction()
        } else {
            worker
        });
    }
    let sources = source_retirer.map(|r| SourceCleaner::new(store, r));
    if args.once {
        let (a, b) = tokio::join!(output_tick(&cleaner, true), source_tick(&sources, true));
        a?;
        b?;
        return Ok(());
    }
    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => break,
            results = async {tokio::join!(output_tick(&cleaner,false),source_tick(&sources,false))} => {
                for error in [results.0.err(),results.1.err()].into_iter().flatten() {eprintln!("sandbox cleanup: {error}");}
            }
        }
        tokio::select! {
            _=tokio::signal::ctrl_c()=>break,
            _=tokio::time::sleep(Duration::from_millis(500))=>{},
        }
    }
    Ok(())
}
async fn output_tick(worker: &Option<Cleaner>, verbose: bool) -> anyhow::Result<()> {
    if let Some(worker) = worker {
        let tick = worker.tick().await.map_err(|e| anyhow::anyhow!("{e}"))?;
        if verbose || tick != CleanupTick::Idle {
            eprintln!("sandbox output cleanup: {tick:?}");
        }
    }
    Ok(())
}
async fn source_tick(worker: &Option<SourceCleaner>, verbose: bool) -> anyhow::Result<()> {
    if let Some(worker) = worker {
        let tick = worker.tick().await.map_err(|e| anyhow::anyhow!("{e}"))?;
        if verbose || tick != SourceCleanupTick::Idle {
            eprintln!("sandbox file source cleanup: {tick:?}");
        }
    }
    Ok(())
}
