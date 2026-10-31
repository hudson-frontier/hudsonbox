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
    /// Operator-only fleet snapshot; no public administration route.
    FleetStatus {
        #[arg(long)]
        prometheus: bool,
    },
    /// Retain a durable static capacity intent from a private Request JSON file.
    CapacityEnqueue {
        #[arg(long)]
        pool_id: uuid::Uuid,
        #[arg(long)]
        idempotency_key: String,
        #[arg(long)]
        request_config: PathBuf,
    },
    /// Claim and apply at most one durable intent. Retry after uncertainty.
    CapacityTick {
        #[arg(long)]
        pool_id: uuid::Uuid,
        /// Private explicit HTTPS cluster/CA/token file references. No ambient kubeconfig.
        #[arg(long)]
        k8s_config: Option<PathBuf>,
    },
    /// Cancel a stale, unclaimed static intent that has no receipt.
    CapacityCancel {
        #[arg(long)]
        pool_id: uuid::Uuid,
        #[arg(long)]
        operation_id: uuid::Uuid,
    },
    /// Read a durable intent, status, lease and receipt by ID.
    CapacityStatus {
        #[arg(long)]
        operation_id: uuid::Uuid,
    },
    /// Advisory worker demand bounded by pool inventory and operator limits.
    CapacityPlan {
        #[arg(long)]
        pool_id: uuid::Uuid,
        #[arg(long)]
        waiting_sandboxes: u32,
        #[arg(long)]
        sandboxes_per_host: u32,
        #[arg(long)]
        max_additional_hosts: u32,
    },
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
    /// Print one page of registry history (retired tombstones included),
    /// optionally for one pool. Continue with `--after <next_after>`.
    Registry {
        #[arg(long)]
        pool_id: Option<uuid::Uuid>,
        #[arg(long)]
        after: Option<HostId>,
        #[arg(long, default_value_t = 500)]
        limit: i64,
    },
    /// Compare operator-configured standalone addresses with one pool's registry.
    Discover {
        #[arg(long)]
        pool_id: uuid::Uuid,
        #[arg(long)]
        discovery_config: Option<PathBuf>,
        #[arg(long)]
        k8s_config: Option<PathBuf>,
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
        Action::FleetStatus { .. }
            | Action::PutPool { .. }
            | Action::GrantPoolAccess { .. }
            | Action::Registry { .. }
            | Action::Discover { .. }
            | Action::CapacityEnqueue { .. }
            | Action::CapacityTick { .. }
            | Action::CapacityCancel { .. }
            | Action::CapacityStatus { .. }
            | Action::CapacityPlan { .. }
    );
    anyhow::ensure!(
        !pool_action || (args.host_id.is_none() && args.host_epoch.is_none()),
        "pool action takes no host"
    );
    let value = match args.action {
        Action::FleetStatus { prometheus } => {
            let status = store.fleet_status().await?;
            if prometheus {
                print!("{}", sandbox_store::fleet::prometheus(&status));
                return Ok(());
            }
            status
        }
        Action::CapacityEnqueue {
            pool_id,
            idempotency_key,
            request_config,
        } => {
            let request = serde_json::from_slice(&private_bytes(&request_config)?)?;
            serde_json::json!({"operation_id": store.enqueue_capacity(pool_id, &idempotency_key, &request).await?})
        }
        Action::CapacityTick {
            pool_id,
            k8s_config,
        } => {
            let mut router = sandbox_controller::capacity::ProviderRouter::default().with(
                std::sync::Arc::new(sandbox_controller::capacity::StandaloneStaticProvider),
            );
            if let Some(path) = k8s_config {
                router = router.with(std::sync::Arc::new(k8s_provider(&path)?));
            }
            match store.claim_capacity(pool_id, 30).await? {
                Some(claim) => router.apply(&store, &claim).await?,
                None => serde_json::json!({"idle":true}),
            }
        }
        Action::CapacityCancel {
            pool_id,
            operation_id,
        } => {
            store.cancel_capacity(pool_id, operation_id).await?;
            serde_json::json!({"operation_id": operation_id, "status": "cancelled"})
        }
        Action::CapacityStatus { operation_id } => store
            .capacity_operation(operation_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("capacity operation missing"))?,
        Action::CapacityPlan {
            pool_id,
            waiting_sandboxes,
            sandboxes_per_host,
            max_additional_hosts,
        } => {
            let pool = store
                .host_pool(pool_id)
                .await?
                .ok_or_else(|| anyhow::anyhow!("pool missing"))?;
            serde_json::to_value(
                sandbox_controller::capacity::plan(
                    &store,
                    &pool,
                    sandbox_controller::capacity::Demand {
                        waiting_sandboxes,
                        sandboxes_per_host,
                        max_additional_hosts,
                    },
                )
                .await?,
            )?
        }
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
        Action::Registry {
            pool_id,
            after,
            limit,
        } => {
            let hosts = store.registered_host_history(pool_id, after, limit).await?;
            let next = (hosts.len() as i64 == limit)
                .then(|| hosts.last().map(|h| h.host_id))
                .flatten();
            serde_json::json!({"hosts": hosts, "next_after": next})
        }
        Action::Discover {
            pool_id,
            discovery_config,
            k8s_config,
        } => {
            let pool = store
                .host_pool(pool_id)
                .await?
                .ok_or_else(|| anyhow::anyhow!("pool missing"))?;
            anyhow::ensure!(
                discovery_config.is_some() != k8s_config.is_some(),
                "select one discovery config"
            );
            let mut router = ProviderRouter::new();
            if let Some(path) = discovery_config {
                router = router.with(std::sync::Arc::new(StandaloneStaticDiscovery::from_json(
                    &private_bytes(&path)?,
                )?));
            }
            if let Some(path) = k8s_config {
                router = router.with(std::sync::Arc::new(k8s_provider(&path)?));
            }
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

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct K8sConfig {
    version: u32,
    server: String,
    credential_secret_ref: String,
    ca_file: PathBuf,
    token_file: PathBuf,
}
fn k8s_provider(
    path: &std::path::Path,
) -> anyhow::Result<sandbox_controller::k8s_capacity::KubernetesCapacityProvider> {
    use sandbox_controller::k8s_capacity::{HttpKubernetesApi, KubernetesCapacityProvider};
    let config: K8sConfig = serde_json::from_slice(&private_bytes(path)?)?;
    anyhow::ensure!(config.version == 1, "unsupported cluster configuration");
    let token = String::from_utf8(private_bytes(&config.token_file)?)?;
    let api = HttpKubernetesApi::new(
        &config.server,
        &private_bytes(&config.ca_file)?,
        token.trim().into(),
        config.credential_secret_ref,
    )?;
    Ok(KubernetesCapacityProvider::new(std::sync::Arc::new(api)))
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
