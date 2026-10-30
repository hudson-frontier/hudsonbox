//! Controller-visible registry of operator-enrolled supervisor hosts and pools.
//!
//! Database access is operator authority; there is no public registration route.
//! Authenticated health refreshes only freshness/capability observation and can
//! never change a host's pool, endpoint, transport identity, capacity or epoch.

use sandbox_protocol::{HostId, Id, ProjectId};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use sqlx::Row;
use uuid::Uuid;

use crate::{
    Store,
    host_maintenance::{self, HostCapacity, HostMaintenance},
};

/// Backfilled shared standalone pool holding every pre-registry host (migration 0025).
pub const LEGACY_POOL_ID: Uuid = Uuid::from_u128(0x0000_0000_0000_4000_8000_0000_0000_0001);
/// Same freshness window `finish-drain` requires.
pub const HEARTBEAT_FRESHNESS_SECONDS: i64 = 30;
/// Bound on one registry read; larger fleets need paging first.
pub const MAX_REGISTRY_HOSTS: i64 = 10_000;

#[derive(Debug, thiserror::Error)]
pub enum RegistryError {
    #[error("registry input is invalid")]
    Invalid,
    #[error("registry record missing, epoch changed or state incompatible")]
    Conflict,
    #[error("another host already uses this endpoint, certificate or provider reference")]
    Duplicate,
    #[error("host pool still has enrolled hosts")]
    InUse,
    #[error("supervisor transport does not match its registration")]
    TransportMismatch,
    #[error("host registry storage query failed")]
    Query(sqlx::Error),
}

impl From<sqlx::Error> for RegistryError {
    fn from(error: sqlx::Error) -> Self {
        match error.as_database_error().and_then(|e| e.code()).as_deref() {
            Some("23505") => Self::Duplicate,
            Some("23514" | "23503") => Self::Invalid,
            _ => Self::Query(error),
        }
    }
}

/// Selects a deployment adapter; the router is application code, not a table.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum PoolType {
    #[serde(rename = "k8s")]
    K8s,
    #[serde(rename = "standalone")]
    Standalone,
}

impl PoolType {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::K8s => "k8s",
            Self::Standalone => "standalone",
        }
    }
    fn parse(value: &str) -> Result<Self, RegistryError> {
        match value {
            "k8s" => Ok(Self::K8s),
            "standalone" => Ok(Self::Standalone),
            _ => Err(RegistryError::Invalid),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Tenancy {
    /// Any project with an explicit access grant.
    Shared,
    /// At most one project can hold an access grant.
    Dedicated,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PlacementPolicy {
    pub version: u32,
    pub tenancy: Tenancy,
}

/// Operator-registered Linux hosts; addresses live on each host registration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StandaloneProviderConfig {
    pub version: u32,
}

/// Seam for the Kubernetes worker adapter (#197). No Kubernetes client is used here.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct K8sProviderConfig {
    pub version: u32,
    /// Namespace holding supervisor worker Pods (DNS label).
    pub namespace: String,
    /// Label selector identifying supervisor worker Pods.
    pub worker_label_selector: String,
}

/// `provider_config` decoded against its pool type. Unknown fields are refused,
/// so secret values cannot be smuggled into the stored configuration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(untagged)]
pub enum ProviderConfig {
    Standalone(StandaloneProviderConfig),
    K8s(K8sProviderConfig),
}

fn dns_label(value: &str) -> bool {
    let bytes = value.as_bytes();
    (1..=63).contains(&bytes.len())
        && bytes
            .iter()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || *b == b'-')
        && bytes.first() != Some(&b'-')
        && bytes.last() != Some(&b'-')
}

fn reference(value: &str) -> bool {
    let bytes = value.as_bytes();
    (1..=253).contains(&bytes.len())
        && bytes[0].is_ascii_alphanumeric()
        && bytes
            .iter()
            .all(|b| b.is_ascii_alphanumeric() || b"._/:-".contains(b))
}

impl ProviderConfig {
    pub fn decode(pool_type: PoolType, value: &serde_json::Value) -> Result<Self, RegistryError> {
        let config = match pool_type {
            PoolType::Standalone => {
                let config: StandaloneProviderConfig =
                    serde_json::from_value(value.clone()).map_err(|_| RegistryError::Invalid)?;
                (config.version == 1).then_some(Self::Standalone(config))
            }
            PoolType::K8s => {
                let config: K8sProviderConfig =
                    serde_json::from_value(value.clone()).map_err(|_| RegistryError::Invalid)?;
                let selector = &config.worker_label_selector;
                (config.version == 1
                    && dns_label(&config.namespace)
                    && (1..=1024).contains(&selector.len())
                    && selector.bytes().all(|b| b.is_ascii_graphic() || b == b' '))
                .then_some(Self::K8s(config))
            }
        };
        config.ok_or(RegistryError::Invalid)
    }

    #[must_use]
    pub fn pool_type(&self) -> PoolType {
        match self {
            Self::Standalone(_) => PoolType::Standalone,
            Self::K8s(_) => PoolType::K8s,
        }
    }

    fn to_value(&self) -> Result<serde_json::Value, RegistryError> {
        match self {
            Self::Standalone(config) => serde_json::to_value(config),
            Self::K8s(config) => serde_json::to_value(config),
        }
        .map_err(|_| RegistryError::Invalid)
    }
}

/// Retained operator input for `put-pool`. Exact retries are no-ops.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HostPoolSpec {
    pub version: u32,
    pub id: Uuid,
    pub name: String,
    #[serde(rename = "type")]
    pub pool_type: PoolType,
    pub min_ready_hosts: u32,
    pub max_hosts: u32,
    pub placement_policy: PlacementPolicy,
    pub provider_config: serde_json::Value,
    #[serde(default)]
    pub credential_secret_ref: Option<String>,
}

impl HostPoolSpec {
    pub fn from_json(bytes: &[u8]) -> Result<Self, RegistryError> {
        if bytes.is_empty() || bytes.len() > 65536 {
            return Err(RegistryError::Invalid);
        }
        let value: Self = serde_json::from_slice(bytes).map_err(|_| RegistryError::Invalid)?;
        value.validate()?;
        Ok(value)
    }

    fn validate(&self) -> Result<ProviderConfig, RegistryError> {
        if self.version != 1
            || self.id.is_nil()
            || !dns_label(&self.name)
            || self.max_hosts < self.min_ready_hosts
            || self.max_hosts > 100_000
            || self.placement_policy.version != 1
            || self
                .credential_secret_ref
                .as_deref()
                .is_some_and(|r| !reference(r))
        {
            return Err(RegistryError::Invalid);
        }
        ProviderConfig::decode(self.pool_type, &self.provider_config)
    }
}

/// Pool read model.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct HostPool {
    pub id: Uuid,
    pub name: String,
    #[serde(rename = "type")]
    pub pool_type: PoolType,
    pub min_ready_hosts: i32,
    pub max_hosts: i32,
    pub placement_policy: PlacementPolicy,
    pub provider_config: ProviderConfig,
    pub credential_secret_ref: Option<String>,
    pub enrolled_hosts: i64,
}

/// Retained operator input for `set-transport`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HostTransport {
    pub version: u32,
    /// `https://HOST:PORT`, no path, query or credentials.
    pub endpoint: String,
    /// SHA-256 of the host server leaf certificate DER, hex.
    pub certificate_sha256: String,
    #[serde(default)]
    pub provider_ref: Option<String>,
}

impl HostTransport {
    pub fn from_json(bytes: &[u8]) -> Result<Self, RegistryError> {
        if bytes.is_empty() || bytes.len() > 65536 {
            return Err(RegistryError::Invalid);
        }
        let value: Self = serde_json::from_slice(bytes).map_err(|_| RegistryError::Invalid)?;
        value.normalized()?;
        Ok(value)
    }

    /// Canonical stored form; equivalent spellings collide on uniqueness.
    pub fn normalized(&self) -> Result<Self, RegistryError> {
        if self.version != 1 || self.provider_ref.as_deref().is_some_and(|r| !reference(r)) {
            return Err(RegistryError::Invalid);
        }
        Ok(Self {
            version: 1,
            endpoint: normalize_endpoint(&self.endpoint)?,
            certificate_sha256: normalize_fingerprint(&self.certificate_sha256)?,
            provider_ref: self.provider_ref.clone(),
        })
    }
}

/// Accept only `https://host:port` with an explicit port.
pub fn normalize_endpoint(value: &str) -> Result<String, RegistryError> {
    let value = value.to_ascii_lowercase();
    let authority = value
        .strip_prefix("https://")
        .ok_or(RegistryError::Invalid)?;
    let (host, port) = authority.rsplit_once(':').ok_or(RegistryError::Invalid)?;
    let port: u16 = port
        .parse()
        .ok()
        .filter(|p| *p != 0)
        .ok_or(RegistryError::Invalid)?;
    let host_ok = if let Some(inner) = host.strip_prefix('[').and_then(|h| h.strip_suffix(']')) {
        inner.parse::<std::net::Ipv6Addr>().is_ok()
    } else {
        host.parse::<std::net::Ipv4Addr>().is_ok()
            || (host.len() <= 253 && !host.is_empty() && host.split('.').all(dns_label))
    };
    if !host_ok {
        return Err(RegistryError::Invalid);
    }
    let host = match host.strip_prefix('[').and_then(|h| h.strip_suffix(']')) {
        Some(inner) => format!(
            "[{}]",
            inner
                .parse::<std::net::Ipv6Addr>()
                .map_err(|_| RegistryError::Invalid)?
        ),
        None => host.to_owned(),
    };
    Ok(format!("https://{host}:{port}"))
}

fn normalize_fingerprint(value: &str) -> Result<String, RegistryError> {
    let value = value.to_ascii_lowercase();
    if value.len() != 64 || !value.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err(RegistryError::Invalid);
    }
    Ok(value)
}

/// Stable per-host DNS subjectAltName; equals `sandbox_supervisor::transport::host_server_name`.
#[must_use]
pub fn host_server_name(host: HostId) -> String {
    format!("host-{}.sandbox.internal", host.uuid())
}

/// What the controller must verify before trusting a supervisor connection.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct TransportIdentity {
    pub endpoint: String,
    /// Required DNS subjectAltName of the host server certificate.
    pub server_name: String,
    /// Required SHA-256 of the presented host server leaf certificate DER.
    pub certificate_sha256: String,
}

/// Registry read model consumed by routing (#193) and placement (#194).
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RegisteredHost {
    pub host_id: HostId,
    pub host_pool_id: Uuid,
    pub pool_type: PoolType,
    pub status: String,
    pub supervisor_epoch: i64,
    /// `None` for a legacy host still addressed by single-host controller config.
    pub transport: Option<TransportIdentity>,
    pub provider_ref: Option<String>,
    pub capacity: HostCapacity,
    pub capability_policy: serde_json::Value,
    pub observed_capabilities: Option<serde_json::Value>,
    pub observed_capabilities_epoch: Option<i64>,
    #[serde(skip)]
    pub last_seen_at: Option<time::OffsetDateTime>,
    /// Milliseconds since the last authenticated observation, by database clock.
    pub heartbeat_age_ms: Option<i64>,
    /// Authenticated observation within [`HEARTBEAT_FRESHNESS_SECONDS`].
    pub heartbeat_fresh: bool,
}

impl RegisteredHost {
    /// Check a connection against the registration: exact epoch, endpoint and
    /// server leaf certificate. CA-chain and subjectAltName verification against
    /// [`TransportIdentity::server_name`] remain the TLS client's job.
    pub fn verify_transport(
        &self,
        epoch: i64,
        endpoint: &str,
        server_leaf_der: &[u8],
    ) -> Result<(), RegistryError> {
        let transport = self
            .transport
            .as_ref()
            .ok_or(RegistryError::TransportMismatch)?;
        let presented = hex::encode(Sha256::digest(server_leaf_der));
        let endpoint =
            normalize_endpoint(endpoint).map_err(|_| RegistryError::TransportMismatch)?;
        if epoch <= 0
            || epoch != self.supervisor_epoch
            || endpoint != transport.endpoint
            || presented != transport.certificate_sha256
        {
            return Err(RegistryError::TransportMismatch);
        }
        Ok(())
    }

    /// Eligible for new placement consideration; existing allocations route by
    /// owner identity and epoch regardless of this flag.
    #[must_use]
    pub fn admits_new_work(&self) -> bool {
        self.status == "ready" && self.heartbeat_fresh && self.transport.is_some()
    }
}

async fn bounded(tx: &mut sqlx::Transaction<'_, sqlx::Postgres>) -> Result<(), sqlx::Error> {
    sqlx::query("SET LOCAL statement_timeout='2s'")
        .execute(&mut **tx)
        .await?;
    sqlx::query("SET LOCAL lock_timeout='1s'")
        .execute(&mut **tx)
        .await?;
    Ok(())
}

fn pool_from_row(row: &sqlx::postgres::PgRow) -> Result<HostPool, RegistryError> {
    let pool_type = PoolType::parse(row.try_get("type")?)?;
    let policy: serde_json::Value = row.try_get("placement_policy")?;
    let config: serde_json::Value = row.try_get("provider_config")?;
    Ok(HostPool {
        id: row.try_get("id")?,
        name: row.try_get("name")?,
        pool_type,
        min_ready_hosts: row.try_get("min_ready_hosts")?,
        max_hosts: row.try_get("max_hosts")?,
        placement_policy: serde_json::from_value(policy).map_err(|_| RegistryError::Invalid)?,
        provider_config: ProviderConfig::decode(pool_type, &config)?,
        credential_secret_ref: row.try_get("credential_secret_ref")?,
        enrolled_hosts: row.try_get("enrolled_hosts")?,
    })
}

const POOL_SELECT: &str = "SELECT p.id,p.name,p.type,p.min_ready_hosts,p.max_hosts,p.placement_policy,
    p.provider_config,p.credential_secret_ref,(SELECT count(*) FROM hosts h WHERE h.host_pool_id=p.id) enrolled_hosts
    FROM host_pools p";

fn host_from_row(row: &sqlx::postgres::PgRow) -> Result<RegisteredHost, RegistryError> {
    let endpoint: Option<String> = row.try_get("endpoint")?;
    let server_name: Option<String> = row.try_get("transport_server_name")?;
    let certificate: Option<String> = row.try_get("transport_certificate_sha256")?;
    let transport = match (endpoint, server_name, certificate) {
        (Some(endpoint), Some(server_name), Some(certificate_sha256)) => Some(TransportIdentity {
            endpoint,
            server_name,
            certificate_sha256,
        }),
        (None, None, None) => None,
        _ => return Err(RegistryError::Invalid),
    };
    let capacity = |name: &str| -> Result<u64, RegistryError> {
        let value: i64 = row.try_get(name)?;
        u64::try_from(value).map_err(|_| RegistryError::Invalid)
    };
    let vcpu: i32 = row.try_get("cpu_capacity")?;
    Ok(RegisteredHost {
        host_id: HostId::from_uuid(row.try_get("id")?),
        host_pool_id: row.try_get("host_pool_id")?,
        pool_type: PoolType::parse(row.try_get("type")?)?,
        status: row.try_get("status")?,
        supervisor_epoch: row.try_get("supervisor_epoch")?,
        transport,
        provider_ref: row.try_get("provider_ref")?,
        capacity: HostCapacity {
            vcpu: u32::try_from(vcpu).map_err(|_| RegistryError::Invalid)?,
            memory_mib: capacity("memory_capacity_mib")?,
            disk_mib: capacity("disk_capacity_mib")?,
        },
        capability_policy: row.try_get("compatibility")?,
        observed_capabilities: row.try_get("runtime_capabilities")?,
        observed_capabilities_epoch: row.try_get("runtime_capabilities_epoch")?,
        last_seen_at: row.try_get("last_seen_at")?,
        heartbeat_age_ms: row.try_get("heartbeat_age_ms")?,
        heartbeat_fresh: row.try_get("heartbeat_fresh")?,
    })
}

const HOST_SELECT: &str = "SELECT h.id,h.host_pool_id,p.type,h.status,h.supervisor_epoch,h.endpoint,
    h.transport_server_name,h.transport_certificate_sha256,h.provider_ref,h.cpu_capacity,h.memory_capacity_mib,h.disk_capacity_mib,h.compatibility,h.runtime_capabilities,
    h.runtime_capabilities_epoch,h.last_seen_at,
    (extract(epoch FROM clock_timestamp()-h.last_seen_at)*1000)::bigint AS heartbeat_age_ms,
    COALESCE(h.last_seen_at>clock_timestamp()-make_interval(secs=>$1),false) AS heartbeat_fresh
    FROM hosts h JOIN host_pools p ON p.id=h.host_pool_id";

impl Store {
    /// Create or update a pool under operator authority. A type change is
    /// refused while any host is enrolled: hosts carry every reservation and
    /// unfinished operation, and are never deleted or moved between pools.
    pub async fn put_host_pool(&self, spec: &HostPoolSpec) -> Result<HostPool, RegistryError> {
        let config = spec.validate()?.to_value()?;
        let policy =
            serde_json::to_value(&spec.placement_policy).map_err(|_| RegistryError::Invalid)?;
        let min = i32::try_from(spec.min_ready_hosts).map_err(|_| RegistryError::Invalid)?;
        let max = i32::try_from(spec.max_hosts).map_err(|_| RegistryError::Invalid)?;
        let mut tx = self.pool().begin().await?;
        bounded(&mut tx).await?;
        sqlx::query("INSERT INTO host_pools(id,name,type,min_ready_hosts,max_hosts,placement_policy,provider_config,credential_secret_ref)
            VALUES($1,$2,$3,$4,$5,$6,$7,$8) ON CONFLICT(id) DO NOTHING")
            .bind(spec.id).bind(&spec.name).bind(spec.pool_type.as_str()).bind(min).bind(max)
            .bind(&policy).bind(&config).bind(&spec.credential_secret_ref)
            .execute(&mut *tx).await?;
        // Serialize with provisioning, which locks the pool before enrolling.
        let row = sqlx::query(&format!("{POOL_SELECT} WHERE p.id=$1 FOR UPDATE OF p"))
            .bind(spec.id)
            .fetch_one(&mut *tx)
            .await?;
        let current = pool_from_row(&row)?;
        let unfinished: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM capacity_operations WHERE host_pool_id=$1 AND status<>'succeeded')")
            .bind(spec.id).fetch_one(&mut *tx).await?;
        if current.pool_type != spec.pool_type && (current.enrolled_hosts != 0 || unfinished) {
            return Err(RegistryError::InUse);
        }
        if i64::from(max) < current.enrolled_hosts {
            return Err(RegistryError::Conflict);
        }
        if spec.placement_policy.tenancy == Tenancy::Dedicated {
            let grants: i64 = sqlx::query_scalar(
                "SELECT count(*) FROM project_host_pool_access WHERE host_pool_id=$1",
            )
            .bind(spec.id)
            .fetch_one(&mut *tx)
            .await?;
            if grants > 1 {
                return Err(RegistryError::Conflict);
            }
        }
        sqlx::query("UPDATE host_pools SET name=$2,type=$3,min_ready_hosts=$4,max_hosts=$5,placement_policy=$6,
            provider_config=$7,credential_secret_ref=$8,updated_at=clock_timestamp()
            WHERE id=$1 AND (name,type,min_ready_hosts,max_hosts,placement_policy,provider_config,credential_secret_ref)
            IS DISTINCT FROM ($2,$3,$4,$5,$6,$7,$8)")
            .bind(spec.id).bind(&spec.name).bind(spec.pool_type.as_str()).bind(min).bind(max)
            .bind(&policy).bind(&config).bind(&spec.credential_secret_ref)
            .execute(&mut *tx).await?;
        let row = sqlx::query(&format!("{POOL_SELECT} WHERE p.id=$1"))
            .bind(spec.id)
            .fetch_one(&mut *tx)
            .await?;
        let value = pool_from_row(&row)?;
        tx.commit().await?;
        Ok(value)
    }

    pub async fn host_pool(&self, id: Uuid) -> Result<Option<HostPool>, RegistryError> {
        let row = sqlx::query(&format!("{POOL_SELECT} WHERE p.id=$1"))
            .bind(id)
            .fetch_optional(self.pool())
            .await?;
        row.as_ref().map(pool_from_row).transpose()
    }

    /// Idempotently permit a project to place work in a pool. A dedicated pool
    /// accepts exactly one project. Grants confer no authority over hosts.
    pub async fn grant_host_pool_access(
        &self,
        project: ProjectId,
        pool: Uuid,
    ) -> Result<Uuid, RegistryError> {
        let mut tx = self.pool().begin().await?;
        bounded(&mut tx).await?;
        let policy: serde_json::Value =
            sqlx::query_scalar("SELECT placement_policy FROM host_pools WHERE id=$1 FOR UPDATE")
                .bind(pool)
                .fetch_optional(&mut *tx)
                .await?
                .ok_or(RegistryError::Conflict)?;
        let policy: PlacementPolicy =
            serde_json::from_value(policy).map_err(|_| RegistryError::Invalid)?;
        sqlx::query(
            "INSERT INTO project_host_pool_access(id,project_id,host_pool_id) VALUES($1,$2,$3)
            ON CONFLICT ON CONSTRAINT project_host_pool_access_pair DO NOTHING",
        )
        .bind(Uuid::now_v7())
        .bind(project.uuid())
        .bind(pool)
        .execute(&mut *tx)
        .await
        .map_err(|e| match RegistryError::from(e) {
            // Missing project.
            RegistryError::Invalid => RegistryError::Conflict,
            other => other,
        })?;
        let projects: Vec<(Uuid, Uuid)> = sqlx::query_as(
            "SELECT id,project_id FROM project_host_pool_access WHERE host_pool_id=$1",
        )
        .bind(pool)
        .fetch_all(&mut *tx)
        .await?;
        if policy.tenancy == Tenancy::Dedicated && projects.len() > 1 {
            return Err(RegistryError::Conflict);
        }
        let id = projects
            .iter()
            .find(|(_, p)| *p == project.uuid())
            .map(|(id, _)| *id)
            .ok_or(RegistryError::Conflict)?;
        tx.commit().await?;
        Ok(id)
    }

    /// Pools a project may place work in. Absence of a grant is denial.
    pub async fn project_host_pools(&self, project: ProjectId) -> Result<Vec<Uuid>, RegistryError> {
        Ok(sqlx::query_scalar(
            "SELECT host_pool_id FROM project_host_pool_access WHERE project_id=$1 ORDER BY host_pool_id",
        )
        .bind(project.uuid())
        .fetch_all(self.pool())
        .await?)
    }

    /// Set or replace a host's endpoint and transport identity. An exact retry
    /// reports current state in any status. A change requires the expected
    /// epoch on a draining host and clears freshness, so `finish-drain` waits
    /// for authenticated health through the new registration.
    pub async fn set_host_transport(
        &self,
        host: HostId,
        expected_epoch: i64,
        request: &HostTransport,
    ) -> Result<HostMaintenance, RegistryError> {
        if expected_epoch <= 0 {
            return Err(RegistryError::Conflict);
        }
        let request = request.normalized()?;
        let mut tx = self.pool().begin().await?;
        bounded(&mut tx).await?;
        let row = sqlx::query(
            "SELECT status,endpoint,transport_certificate_sha256,provider_ref FROM hosts
            WHERE id=$1 AND supervisor_epoch=$2 FOR UPDATE",
        )
        .bind(host.uuid())
        .bind(expected_epoch)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or(RegistryError::Conflict)?;
        let status: String = row.try_get("status")?;
        let unchanged = row.try_get::<Option<String>, _>("endpoint")?.as_deref()
            == Some(request.endpoint.as_str())
            && row
                .try_get::<Option<String>, _>("transport_certificate_sha256")?
                .as_deref()
                == Some(request.certificate_sha256.as_str())
            && row.try_get::<Option<String>, _>("provider_ref")? == request.provider_ref;
        if !unchanged {
            if status != "draining" {
                return Err(RegistryError::Conflict);
            }
            sqlx::query("UPDATE hosts SET endpoint=$3,transport_server_name=$4,transport_certificate_sha256=$5,
                provider_ref=$6,last_seen_at=NULL,updated_at=clock_timestamp() WHERE id=$1 AND supervisor_epoch=$2")
                .bind(host.uuid()).bind(expected_epoch).bind(&request.endpoint).bind(host_server_name(host))
                .bind(&request.certificate_sha256).bind(&request.provider_ref)
                .execute(&mut *tx).await?;
        }
        let value = host_maintenance::report(&mut tx, host, expected_epoch)
            .await
            .map_err(|e| match e {
                host_maintenance::Error::Query(e) => RegistryError::from(e),
                _ => RegistryError::Conflict,
            })?;
        tx.commit().await?;
        Ok(value)
    }

    pub async fn registered_host(
        &self,
        host: HostId,
    ) -> Result<Option<RegisteredHost>, RegistryError> {
        let row = sqlx::query(&format!("{HOST_SELECT} WHERE h.id=$2"))
            .bind(HEARTBEAT_FRESHNESS_SECONDS as f64)
            .bind(host.uuid())
            .fetch_optional(self.pool())
            .await?;
        row.as_ref().map(host_from_row).transpose()
    }

    /// Every registered host, optionally in one pool, ordered by identity.
    pub async fn registered_hosts(
        &self,
        pool: Option<Uuid>,
    ) -> Result<Vec<RegisteredHost>, RegistryError> {
        let rows = sqlx::query(&format!(
            "{HOST_SELECT} WHERE ($2::uuid IS NULL OR h.host_pool_id=$2) ORDER BY h.id LIMIT $3"
        ))
        .bind(HEARTBEAT_FRESHNESS_SECONDS as f64)
        .bind(pool)
        .bind(MAX_REGISTRY_HOSTS + 1)
        .fetch_all(self.pool())
        .await?;
        if rows.len() as i64 > MAX_REGISTRY_HOSTS {
            return Err(RegistryError::Invalid);
        }
        rows.iter().map(host_from_row).collect()
    }
}
