//! Offline operator diagnostics. No endpoint, credential, provider reference or
//! customer payload is selected. This module grants no placement/release authority.
use crate::Store;
use sandbox_protocol::resources::{ARTIFACT_DISK_OVERHEAD_MIB, VMM_MEMORY_OVERHEAD_MIB};
use serde_json::Value;

impl Store {
    /// One database snapshot, bounded by the registry limit and a statement timeout.
    /// Database access is operator authority; never return this from a project route.
    pub async fn fleet_status(&self) -> Result<Value, sqlx::Error> {
        let mut tx = self.pool().begin().await?;
        sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            .execute(&mut *tx)
            .await?;
        sqlx::query("SET LOCAL statement_timeout='2s'")
            .execute(&mut *tx)
            .await?;
        let count: i64 = sqlx::query_scalar("SELECT count(*) FROM hosts")
            .fetch_one(&mut *tx)
            .await?;
        if count > crate::host_registry::MAX_REGISTRY_HOSTS {
            return Err(sqlx::Error::Protocol(
                "fleet exceeds diagnostic host bound".into(),
            ));
        }
        let hosts: Vec<Value> = sqlx::query_scalar(
            "SELECT jsonb_build_object(
                'host_id',h.id,'epoch',h.supervisor_epoch,'status',h.status,
                'retired',h.capacity_retired_at IS NOT NULL,
                'heartbeat_age_seconds',greatest(0,extract(epoch FROM (now()-h.last_seen_at))),
                'fresh',COALESCE(h.last_seen_at>now()-interval '30 seconds',false),
                'ready',h.capacity_retired_at IS NULL AND h.status='ready' AND h.endpoint IS NOT NULL
                    AND COALESCE(h.last_seen_at>now()-interval '30 seconds',false),
                'active_allocations',a.active,'previous_epoch_allocations',a.previous,
                'pending_cleanup',a.cleanup,'unknown_allocations',a.unknown,
                'confirmed_fenced_allocations',a.fenced,
                'renewal_pending',a.renewals,'renewal_lag_seconds',a.renewal_lag,
                'reserved',jsonb_build_object('vcpu',a.cpu,'memory_mib',a.memory,'disk_mib',a.disk),
                'available',jsonb_build_object(
                    'vcpu',CASE WHEN h.capacity_retired_at IS NULL THEN greatest(0,h.cpu_capacity-a.cpu) ELSE 0 END,
                    'memory_mib',CASE WHEN h.capacity_retired_at IS NULL THEN greatest(0,h.memory_capacity_mib-a.memory) ELSE 0 END,
                    'disk_mib',CASE WHEN h.capacity_retired_at IS NULL THEN greatest(0,h.disk_capacity_mib-a.disk) ELSE 0 END))
             FROM hosts h CROSS JOIN LATERAL (
                SELECT count(*) FILTER (WHERE released_at IS NULL) active,
                    count(*) FILTER (WHERE released_at IS NULL AND a.supervisor_epoch<>h.supervisor_epoch) previous,
                    count(*) FILTER (WHERE released_at IS NULL AND a.status='releasing') cleanup,
                    count(*) FILTER (WHERE released_at IS NULL AND s.observed_state='unknown') unknown,
                    count(*) FILTER (WHERE released_at IS NOT NULL AND release_evidence->>'fenced_absent'='true') fenced,
                    count(*) FILTER (WHERE released_at IS NULL AND renewal_pending) renewals,
                    COALESCE(max(greatest(0,extract(epoch FROM (now()-a.updated_at)))) FILTER (WHERE released_at IS NULL AND renewal_pending),0) renewal_lag,
                    COALESCE(sum(a.vcpu) FILTER (WHERE released_at IS NULL),0) cpu,
                    COALESCE(sum(a.memory_mib+$1) FILTER (WHERE released_at IS NULL),0) memory,
                    COALESCE(sum(a.disk_mib+$2) FILTER (WHERE released_at IS NULL),0) disk
                FROM allocations a JOIN sandboxes s ON s.id=a.sandbox_id WHERE a.host_id=h.id
             ) a ORDER BY h.id")
            .bind(VMM_MEMORY_OVERHEAD_MIB).bind(ARTIFACT_DISK_OVERHEAD_MIB)
            .fetch_all(&mut *tx).await?;
        let queue: Value = sqlx::query_scalar(
            "SELECT jsonb_build_object('queued',count(*),
             'oldest_age_seconds',COALESCE(max(greatest(0,extract(epoch FROM (now()-created_at)))),0),
             'insufficient_capacity',count(*) FILTER (WHERE error->>'code'='insufficient_capacity'),
             'project_quota_exceeded',count(*) FILTER (WHERE error->>'code'='project_quota_exceeded'),
             'no_ready_host',count(*) FILTER (WHERE error->>'code'='no_ready_host'),
             'no_compatible_host',count(*) FILTER (WHERE error->>'code'='no_compatible_host'))
             FROM operations WHERE status='queued'")
            .fetch_one(&mut *tx).await?;
        tx.commit().await?;
        Ok(serde_json::json!({"version":1,"hosts":hosts,"queue":queue}))
    }
}

/// Prometheus gauges with fixed label vocabularies. Internal identities belong
/// in operator JSON/logs, never in labels. A snapshot is not a cumulative counter.
#[must_use]
pub fn prometheus(status: &Value) -> String {
    let mut out = String::new();
    for key in [
        "queued",
        "oldest_age_seconds",
        "insufficient_capacity",
        "project_quota_exceeded",
        "no_ready_host",
        "no_compatible_host",
    ] {
        out.push_str(&format!(
            "hudsonbox_fleet_queue_{key} {}\n",
            status["queue"][key]
        ));
    }
    if let Some(hosts) = status["hosts"].as_array() {
        for key in [
            "active_allocations",
            "previous_epoch_allocations",
            "pending_cleanup",
            "unknown_allocations",
            "confirmed_fenced_allocations",
            "renewal_pending",
        ] {
            let total: i64 = hosts.iter().filter_map(|h| h[key].as_i64()).sum();
            out.push_str(&format!("hudsonbox_fleet_{key} {total}\n"));
        }
        let lag = hosts
            .iter()
            .filter_map(|h| h["renewal_lag_seconds"].as_f64())
            .fold(0.0_f64, f64::max);
        out.push_str(&format!("hudsonbox_fleet_renewal_lag_seconds {lag}\n"));
        for state in ["ready", "unavailable", "retired", "draining"] {
            let count = hosts
                .iter()
                .filter(|h| match state {
                    "retired" => h["retired"] == true,
                    "ready" => h["ready"] == true,
                    "draining" => h["retired"] == false && h["status"] == "draining",
                    _ => h["retired"] == false && h["ready"] == false,
                })
                .count();
            out.push_str(&format!(
                "hudsonbox_fleet_hosts{{state=\"{state}\"}} {count}\n"
            ));
        }
    }
    out
}
