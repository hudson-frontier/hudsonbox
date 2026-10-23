//! Offline host maintenance; database access is operator authority, no public route.
use clap::{Parser, Subcommand};
use sandbox_protocol::HostId;
use sandbox_store::Store;
use std::{io::Read, path::PathBuf};

#[derive(Parser)]
struct Args {
    /// Private owned JSON {"version":1,"database_url":"..."}; never a tool payload.
    #[arg(long)]
    database_config: PathBuf,
    #[arg(long)]
    host_id: HostId,
    #[arg(long)]
    host_epoch: i64,
    #[command(subcommand)]
    action: Action,
}
#[derive(Subcommand)]
enum Action {
    /// Provision a new draining host or inspect an exact retry; never refresh or reopen it.
    Provision {
        /// Private owned JSON containing version, total capacity and approved capabilities.
        #[arg(long)]
        host_config: PathBuf,
    },
    /// Report counts only; absence is not independent physical cleanup proof.
    Status,
    /// Prevent new placements; preserves work, capacity and epoch.
    Drain,
    /// Explicitly reopen only with no reservations and fresh host observation.
    FinishDrain,
    /// Approve a bounded capability contract on an empty draining host.
    SetCapabilities {
        #[arg(long)]
        capabilities_config: PathBuf,
    },
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct DatabaseConfig {
    version: u32,
    database_url: String,
}

fn private_bytes(path: &std::path::Path) -> anyhow::Result<Vec<u8>> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
        let file = std::fs::OpenOptions::new()
            .read(true)
            .custom_flags(
                (rustix::fs::OFlags::NOFOLLOW | rustix::fs::OFlags::NONBLOCK).bits() as i32,
            )
            .open(path)?;
        let meta = file.metadata()?;
        anyhow::ensure!(
            meta.is_file()
                && meta.len() <= 65536
                && meta.uid() == rustix::process::geteuid().as_raw()
                && meta.mode() & 0o077 == 0,
            "private configuration required"
        );
        let mut bytes = Vec::new();
        file.take(65537).read_to_end(&mut bytes)?;
        anyhow::ensure!(bytes.len() <= 65536, "configuration too large");
        Ok(bytes)
    }
    #[cfg(not(unix))]
    {
        let _ = path;
        anyhow::bail!("private configuration requires Unix");
    }
}

fn config(path: &std::path::Path) -> anyhow::Result<DatabaseConfig> {
    let value: DatabaseConfig = serde_json::from_slice(&private_bytes(path)?)?;
    anyhow::ensure!(
        value.version == 1 && !value.database_url.is_empty(),
        "unsupported configuration"
    );
    Ok(value)
}

async fn run(args: Args) -> anyhow::Result<()> {
    let cfg = config(&args.database_config)?;
    let store = Store::connect(&cfg.database_url, 2).await?;
    // Maintenance never migrates or resets a user's database.
    let value = match args.action {
        Action::Provision { host_config } => {
            let request = sandbox_store::host_maintenance::HostProvision::from_json(
                &private_bytes(&host_config)?,
            )?;
            store
                .provision_host(args.host_id, args.host_epoch, &request)
                .await?
        }
        Action::Status => {
            store
                .host_maintenance(args.host_id, args.host_epoch)
                .await?
        }
        Action::Drain => store.drain_host(args.host_id, args.host_epoch).await?,
        Action::FinishDrain => {
            store
                .finish_host_drain(args.host_id, args.host_epoch)
                .await?
        }
        Action::SetCapabilities {
            capabilities_config,
        } => {
            let value = sandbox_protocol::host_capabilities::Capabilities::from_json(
                &private_bytes(&capabilities_config)?,
            )?;
            store
                .set_host_capability_policy(args.host_id, args.host_epoch, &value)
                .await?
        }
    };
    println!("{}", serde_json::to_string(&value)?);
    Ok(())
}

#[tokio::main]
async fn main() {
    if run(Args::parse()).await.is_err() {
        eprintln!(
            "Host maintenance failed: check private configuration, expected epoch, remaining reservations and fresh service observation. No capacity was released."
        );
        std::process::exit(1);
    }
}

#[cfg(all(test, unix))]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;
    #[test]
    fn private_configuration_rejects_permissions_symlinks_duplicates_and_oversize() {
        let root = tempfile::tempdir().expect("temp");
        let path = root.path().join("database.json");
        let valid = br#"{"version":1,"database_url":"postgres://fixture:private-test-password@localhost/test"}"#;
        std::fs::write(&path, valid).expect("write");
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).expect("mode");
        assert!(config(&path).is_ok());
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).expect("mode");
        assert!(config(&path).is_err());
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).expect("mode");
        let linked = root.path().join("link");
        std::os::unix::fs::symlink(&path, &linked).expect("link");
        assert!(config(&linked).is_err());
        for invalid in [
            br#"{"version":1,"version":1,"database_url":"x"}"#.to_vec(),
            vec![b'x'; 65537],
            br#"{"version":2,"database_url":"x"}"#.to_vec(),
        ] {
            std::fs::write(&path, invalid).expect("write");
            assert!(config(&path).is_err());
        }
    }
}
