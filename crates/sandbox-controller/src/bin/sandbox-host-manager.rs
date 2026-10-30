//! Offline host maintenance; database access is operator authority, no public route.
use clap::{Parser, Subcommand};
use sandbox_controller::host_discovery::{ProviderRouter, StandaloneStaticDiscovery, reconcile};
use sandbox_protocol::{HostId, ProjectId};
use sandbox_store::{
    Store,
    host_registry::{HostPoolSpec, HostTransport},
};
use std::{io::Read, path::PathBuf};

#[derive(Parser)]
struct Args {
    /// Private owned JSON {"version":1,"database_url":"..."}; never a tool payload.
    #[arg(long)]
    database_config: PathBuf,
    /// Required by host actions; refused by pool/registry actions.
    #[arg(long)]
    host_id: Option<HostId>,
    #[arg(long)]
    host_epoch: Option<i64>,
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
    /// Advance one database epoch on a drained/unhealthy host; retain all reservations.
    AdvanceEpoch,
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
    /// Set endpoint/transport identity; a change requires a draining host at its epoch.
    SetTransport {
        #[arg(long)]
        transport_config: PathBuf,
    },
    /// Create or update a host pool; type changes are refused while hosts are enrolled.
    PutPool {
        #[arg(long)]
        pool_config: PathBuf,
    },
    /// Idempotently permit one project to place work in a pool.
    GrantPoolAccess {
        #[arg(long)]
        pool_id: uuid::Uuid,
        #[arg(long)]
        project_id: ProjectId,
    },
    /// Print the registry read model, optionally for one pool.
    Registry {
        #[arg(long)]
        pool_id: Option<uuid::Uuid>,
    },
    /// Compare operator-configured standalone addresses with one pool's registry.
    Discover {
        #[arg(long)]
        pool_id: uuid::Uuid,
        #[arg(long)]
        discovery_config: PathBuf,
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
    let host = || -> anyhow::Result<(HostId, i64)> {
        match (args.host_id, args.host_epoch) {
            (Some(id), Some(epoch)) => Ok((id, epoch)),
            _ => anyhow::bail!("host action requires host ID and epoch"),
        }
    };
    let pool_action = matches!(
        args.action,
        Action::PutPool { .. }
            | Action::GrantPoolAccess { .. }
            | Action::Registry { .. }
            | Action::Discover { .. }
    );
    anyhow::ensure!(
        !pool_action || (args.host_id.is_none() && args.host_epoch.is_none()),
        "pool action takes no host"
    );
    let value = match args.action {
        Action::PutPool { pool_config } => serde_json::to_value(
            store
                .put_host_pool(&HostPoolSpec::from_json(&private_bytes(&pool_config)?)?)
                .await?,
        )?,
        Action::GrantPoolAccess {
            pool_id,
            project_id,
        } => serde_json::json!({
            "access_id": store.grant_host_pool_access(project_id, pool_id).await?,
            "project_id": project_id,
            "host_pool_id": pool_id,
        }),
        Action::Registry { pool_id } => {
            serde_json::to_value(store.registered_hosts(pool_id).await?)?
        }
        Action::Discover {
            pool_id,
            discovery_config,
        } => {
            let pool = store
                .host_pool(pool_id)
                .await?
                .ok_or_else(|| anyhow::anyhow!("pool missing"))?;
            let router = ProviderRouter::new().with(std::sync::Arc::new(
                StandaloneStaticDiscovery::from_json(&private_bytes(&discovery_config)?)?,
            ));
            let discovered = router.discover(&pool).await?;
            serde_json::to_value(reconcile(
                &discovered,
                &store.registered_hosts(Some(pool_id)).await?,
            ))?
        }
        Action::SetTransport { transport_config } => {
            let (id, epoch) = host()?;
            let request = HostTransport::from_json(&private_bytes(&transport_config)?)?;
            serde_json::to_value(store.set_host_transport(id, epoch, &request).await?)?
        }
        action => serde_json::to_value(host_action(&store, host()?, action).await?)?,
    };
    println!("{}", serde_json::to_string(&value)?);
    Ok(())
}

async fn host_action(
    store: &Store,
    (host_id, host_epoch): (HostId, i64),
    action: Action,
) -> anyhow::Result<sandbox_store::host_maintenance::HostMaintenance> {
    Ok(match action {
        Action::Provision { host_config } => {
            let request = sandbox_store::host_maintenance::HostProvision::from_json(
                &private_bytes(&host_config)?,
            )?;
            store.provision_host(host_id, host_epoch, &request).await?
        }
        Action::AdvanceEpoch => store.advance_host_epoch(host_id, host_epoch).await?,
        Action::Status => store.host_maintenance(host_id, host_epoch).await?,
        Action::Drain => store.drain_host(host_id, host_epoch).await?,
        Action::FinishDrain => store.finish_host_drain(host_id, host_epoch).await?,
        Action::SetCapabilities {
            capabilities_config,
        } => {
            let value = sandbox_protocol::host_capabilities::Capabilities::from_json(
                &private_bytes(&capabilities_config)?,
            )?;
            store
                .set_host_capability_policy(host_id, host_epoch, &value)
                .await?
        }
        _ => anyhow::bail!("not a host maintenance action"),
    })
}

#[tokio::main]
async fn main() {
    if run(Args::parse()).await.is_err() {
        eprintln!(
            "Host maintenance failed: check private configuration, expected epoch, remaining reservations, fresh service observation and registry uniqueness. No capacity was released."
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
