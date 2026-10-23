//! Offline operator maintenance. Drain serializes with placement; never frees capacity.

use sandbox_protocol::{HostId, Id};
use serde::{Deserialize, Serialize};
use sqlx::Row;

use crate::Store;

#[derive(Debug, Serialize, PartialEq, Eq)]
pub struct HostMaintenance {
    pub host_id: String,
    pub supervisor_epoch: i64,
    pub status: String,
    pub outstanding_allocations: i64,
    pub reserved_vcpu: i64,
    pub reserved_memory_mib: i64,
    pub reserved_disk_mib: i64,
    pub unfinished_operations: i64,
    pub unknown_operations: i64,
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("host missing, epoch changed or maintenance state incompatible")]
    Conflict,
    #[error("host still has reservations or lacks a fresh authenticated observation")]
    Outstanding,
    #[error("host maintenance storage query failed")]
    Query(#[from] sqlx::Error),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HostCapacity {
    pub vcpu: u32,
    pub memory_mib: u64,
    pub disk_mib: u64,
}

/// Retained operator input. Database access is the authorization boundary.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HostProvision {
    pub version: u32,
    pub capacity: HostCapacity,
    pub capabilities: sandbox_protocol::host_capabilities::Capabilities,
}
impl HostProvision {
    pub fn from_json(bytes: &[u8]) -> Result<Self, Error> {
        if bytes.is_empty() || bytes.len() > 65536 {
            return Err(Error::Conflict);
        }
        let value: Self = serde_json::from_slice(bytes).map_err(|_| Error::Conflict)?;
        value.encoded()?;
        Ok(value)
    }
    fn encoded(&self) -> Result<(i32, i64, i64, serde_json::Value), Error> {
        if self.version != 1
            || self.capacity.vcpu == 0
            || self.capacity.memory_mib == 0
            || self.capacity.disk_mib == 0
            || self.capabilities.validate().is_err()
            || self.capabilities.limits.vcpu > self.capacity.vcpu
            || self.capabilities.limits.memory_mib > self.capacity.memory_mib
            || self.capabilities.limits.disk_mib > self.capacity.disk_mib
        {
            return Err(Error::Conflict);
        }
        let cpu = i32::try_from(self.capacity.vcpu).map_err(|_| Error::Conflict)?;
        let memory = i64::try_from(self.capacity.memory_mib).map_err(|_| Error::Conflict)?;
        let disk = i64::try_from(self.capacity.disk_mib).map_err(|_| Error::Conflict)?;
        let policy =
            serde_json::from_slice(&self.capabilities.to_json().map_err(|_| Error::Conflict)?)
                .map_err(|_| Error::Conflict)?;
        Ok((cpu, memory, disk, policy))
    }
}

async fn report(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    host: HostId,
    expected_epoch: i64,
) -> Result<HostMaintenance, Error> {
    let row = sqlx::query(
        "SELECT status,supervisor_epoch FROM hosts WHERE id=$1 AND supervisor_epoch=$2 FOR UPDATE",
    )
    .bind(host.uuid())
    .bind(expected_epoch)
    .fetch_optional(&mut **tx)
    .await?
    .ok_or(Error::Conflict)?;
    let reservations: (i64,i64,i64,i64) = sqlx::query_as("SELECT count(*),COALESCE(sum(vcpu),0)::bigint,COALESCE(sum(memory_mib),0)::bigint,COALESCE(sum(disk_mib),0)::bigint FROM allocations WHERE host_id=$1 AND released_at IS NULL")
        .bind(host.uuid()).fetch_one(&mut **tx).await?;
    let work: (i64,i64) = sqlx::query_as("SELECT count(*),count(*) FILTER (WHERE o.status='unknown') FROM operations o JOIN allocations a ON a.sandbox_id=o.sandbox_id WHERE a.host_id=$1 AND a.released_at IS NULL AND o.status IN ('queued','running','unknown')")
        .bind(host.uuid()).fetch_one(&mut **tx).await?;
    Ok(HostMaintenance {
        host_id: host.to_string(),
        supervisor_epoch: row.try_get("supervisor_epoch")?,
        status: row.try_get("status")?,
        outstanding_allocations: reservations.0,
        reserved_vcpu: reservations.1,
        reserved_memory_mib: reservations.2,
        reserved_disk_mib: reservations.3,
        unfinished_operations: work.0,
        unknown_operations: work.1,
    })
}

impl Store {
    /// Provision once in draining state, without an authenticated observation.
    /// Exact retries report the current state; they never reset health, reopen
    /// admission, change an epoch or overwrite an existing host's configuration.
    pub async fn provision_host(
        &self,
        host: HostId,
        expected_epoch: i64,
        request: &HostProvision,
    ) -> Result<HostMaintenance, Error> {
        if expected_epoch <= 0 {
            return Err(Error::Conflict);
        }
        let (cpu, memory, disk, policy) = request.encoded()?;
        let mut tx = self.pool().begin().await?;
        sqlx::query("SET LOCAL statement_timeout='2s'")
            .execute(&mut *tx)
            .await?;
        sqlx::query("SET LOCAL lock_timeout='1s'")
            .execute(&mut *tx)
            .await?;
        sqlx::query("INSERT INTO hosts(id,status,cpu_capacity,memory_capacity_mib,disk_capacity_mib,compatibility,supervisor_epoch,last_seen_at) VALUES($1,'draining',$2,$3,$4,$5,$6,NULL) ON CONFLICT(id) DO NOTHING")
            .bind(host.uuid()).bind(cpu).bind(memory).bind(disk).bind(&policy).bind(expected_epoch)
            .execute(&mut *tx).await?;
        let matches: bool = sqlx::query_scalar("SELECT supervisor_epoch=$2 AND cpu_capacity=$3 AND memory_capacity_mib=$4 AND disk_capacity_mib=$5 AND compatibility=$6 FROM hosts WHERE id=$1 FOR UPDATE")
            .bind(host.uuid()).bind(expected_epoch).bind(cpu).bind(memory).bind(disk).bind(&policy)
            .fetch_one(&mut *tx).await?;
        if !matches {
            return Err(Error::Conflict);
        }
        let value = report(&mut tx, host, expected_epoch).await?;
        tx.commit().await?;
        Ok(value)
    }

    /// Explicit operator approval; only a drained, empty host can change policy.
    /// Invalidate freshness until matching authenticated health arrives. Never
    /// downgrade to legacy metadata or modify ownership/capacity/epoch.
    pub async fn set_host_capability_policy(
        &self,
        host: HostId,
        expected_epoch: i64,
        capabilities: &sandbox_protocol::host_capabilities::Capabilities,
    ) -> Result<HostMaintenance, Error> {
        if expected_epoch <= 0 || capabilities.validate().is_err() {
            return Err(Error::Conflict);
        }
        let bytes = capabilities.to_json().map_err(|_| Error::Conflict)?;
        let policy: serde_json::Value =
            serde_json::from_slice(&bytes).map_err(|_| Error::Conflict)?;
        let mut tx = self.pool().begin().await?;
        let value = report(&mut tx, host, expected_epoch).await?;
        if value.status != "draining" {
            return Err(Error::Conflict);
        }
        if value.outstanding_allocations != 0 {
            return Err(Error::Outstanding);
        }
        sqlx::query(
            "UPDATE hosts SET compatibility=$3,last_seen_at=NULL,
            updated_at=clock_timestamp() WHERE id=$1 AND supervisor_epoch=$2",
        )
        .bind(host.uuid())
        .bind(expected_epoch)
        .bind(policy)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(value)
    }

    /// Count remaining reservations without interpreting absence as cleanup proof.
    pub async fn host_maintenance(
        &self,
        host: HostId,
        expected_epoch: i64,
    ) -> Result<HostMaintenance, Error> {
        if expected_epoch <= 0 {
            return Err(Error::Conflict);
        }
        let mut tx = self.pool().begin().await?;
        let value = report(&mut tx, host, expected_epoch).await?;
        tx.commit().await?;
        Ok(value)
    }

    /// Idempotently block new placements at exactly the provisioned host epoch.
    /// Existing allocations, permits, leases, request keys and histories are preserved.
    pub async fn drain_host(
        &self,
        host: HostId,
        expected_epoch: i64,
    ) -> Result<HostMaintenance, Error> {
        if expected_epoch <= 0 {
            return Err(Error::Conflict);
        }
        let mut tx = self.pool().begin().await?;
        let value = report(&mut tx, host, expected_epoch).await?;
        if !matches!(value.status.as_str(), "ready" | "draining") {
            return Err(Error::Conflict);
        }
        sqlx::query("UPDATE hosts SET status='draining',updated_at=clock_timestamp() WHERE id=$1 AND supervisor_epoch=$2")
            .bind(host.uuid()).bind(expected_epoch).execute(&mut *tx).await?;
        let value = HostMaintenance {
            status: "draining".into(),
            ..value
        };
        tx.commit().await?;
        Ok(value)
    }

    /// Explicitly reopen only a fully drained host with a fresh service observation.
    /// This does not issue epochs, alter capacity or infer resumable guest memory.
    pub async fn finish_host_drain(
        &self,
        host: HostId,
        expected_epoch: i64,
    ) -> Result<HostMaintenance, Error> {
        if expected_epoch <= 0 {
            return Err(Error::Conflict);
        }
        let mut tx = self.pool().begin().await?;
        let value = report(&mut tx, host, expected_epoch).await?;
        if value.status != "draining" {
            return Err(Error::Conflict);
        }
        let fresh: bool = sqlx::query_scalar("SELECT COALESCE(last_seen_at>clock_timestamp()-interval '30 seconds',false) FROM hosts WHERE id=$1")
            .bind(host.uuid()).fetch_one(&mut *tx).await?;
        if value.outstanding_allocations != 0 || !fresh {
            return Err(Error::Outstanding);
        }
        sqlx::query("UPDATE hosts SET status='ready',updated_at=clock_timestamp() WHERE id=$1 AND supervisor_epoch=$2")
            .bind(host.uuid()).bind(expected_epoch).execute(&mut *tx).await?;
        tx.commit().await?;
        Ok(HostMaintenance {
            status: "ready".into(),
            ..value
        })
    }
}
