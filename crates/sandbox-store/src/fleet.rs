//! Offline operator diagnostics. No endpoint, credential, provider reference or
//! customer payload is selected. This module grants no placement/release authority.
use crate::Store;
use sandbox_protocol::resources::{ARTIFACT_DISK_OVERHEAD_MIB, VMM_MEMORY_OVERHEAD_MIB};
use sandbox_protocol::{HostId, Id, rpc_telemetry::RpcFailure};
use serde_json::Value;

impl Store {
    /// Observed RPC status only. Epoch fencing rejects late replies; retired
    /// identities cannot accumulate new samples. This never changes health or leases.
    pub async fn record_host_rpc_failure(
        &self,
        host: HostId,
        epoch: i64,
        failure: RpcFailure,
    ) -> Result<(), sqlx::Error> {
        tokio::time::timeout(
            std::time::Duration::from_millis(100),
            sqlx::query(
                "UPDATE hosts SET rpc_failures=jsonb_set(rpc_failures, ARRAY[$3],
                to_jsonb(least((rpc_failures->>$3)::numeric+1,9223372036854775807)::bigint)),
                rpc_last_failure_at=clock_timestamp()
             WHERE id=$1 AND supervisor_epoch=$2 AND capacity_retired_at IS NULL",
            )
            .bind(host.uuid())
            .bind(epoch)
            .bind(failure.label())
            .execute(self.pool()),
        )
        .await
        .map_err(|_| sqlx::Error::PoolTimedOut)??;
        Ok(())
    }

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
        let count: i64 =
            sqlx::query_scalar("SELECT count(*) FROM hosts WHERE capacity_retired_at IS NULL")
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
                'retired',false,
                'rpc_failures',h.rpc_failures,'rpc_last_failure_at',h.rpc_last_failure_at,
                'capacity',jsonb_build_object('vcpu',h.cpu_capacity,'memory_mib',h.memory_capacity_mib,'disk_mib',h.disk_capacity_mib),
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
             ) a WHERE h.capacity_retired_at IS NULL ORDER BY h.id")
            .bind(VMM_MEMORY_OVERHEAD_MIB).bind(ARTIFACT_DISK_OVERHEAD_MIB)
            .fetch_all(&mut *tx).await?;
        let queue: Value = sqlx::query_scalar(
            "SELECT jsonb_build_object('queued',count(*),
             'oldest_age_seconds',COALESCE(max(greatest(0,extract(epoch FROM (now()-created_at)))),0),
             'insufficient_capacity',count(*) FILTER (WHERE error->>'code'='insufficient_capacity'),
             'project_quota_exceeded',count(*) FILTER (WHERE error->>'code'='project_quota_exceeded'),
             'no_ready_host',count(*) FILTER (WHERE error->>'code'='no_ready_host'),
             'no_compatible_host',count(*) FILTER (WHERE error->>'code'='no_compatible_host'))
             FROM operations o WHERE kind='create' AND attempt_count=0
                AND attempt_receipts='[]'::jsonb
                AND (status='queued' OR (status='running' AND phase='awaiting_placement'
                    AND lease_expires_at IS NULL))
                AND NOT EXISTS (SELECT 1 FROM allocations a WHERE a.sandbox_id=o.sandbox_id)")
            .fetch_one(&mut *tx).await?;
        let rejections: Value = sqlx::query_scalar(
            "SELECT jsonb_build_object(
                'insufficient_capacity',count(*) FILTER (WHERE error->>'code'='insufficient_capacity'),
                'project_quota_exceeded',count(*) FILTER (WHERE error->>'code'='project_quota_exceeded'),
                'no_ready_host',count(*) FILTER (WHERE error->>'code'='no_ready_host'),
                'no_compatible_host',count(*) FILTER (WHERE error->>'code'='no_compatible_host'),
                'host_pool_access_denied',count(*) FILTER (WHERE error->>'code'='host_pool_access_denied'))
             FROM operations WHERE kind='create' AND status='failed'")
            .fetch_one(&mut *tx).await?;
        tx.commit().await?;
        Ok(serde_json::json!({"version":1,"hosts":hosts,"queue":queue,"rejections":rejections}))
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
    for reason in [
        "insufficient_capacity",
        "project_quota_exceeded",
        "no_ready_host",
        "no_compatible_host",
        "host_pool_access_denied",
    ] {
        let count = status["rejections"][reason].as_i64().unwrap_or(0);
        out.push_str(&format!(
            "hudsonbox_fleet_rejections{{reason=\"{reason}\"}} {count}\n"
        ));
    }
    if let Some(hosts) = status["hosts"].as_array() {
        for reason in ["unavailable", "deadline_exceeded", "unknown"] {
            let total: u128 = hosts
                .iter()
                .filter_map(|h| h["rpc_failures"][reason].as_u64())
                .map(u128::from)
                .sum();
            out.push_str(&format!(
                "hudsonbox_fleet_rpc_failures{{reason=\"{reason}\"}} {total}\n"
            ));
        }
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
        for category in ["capacity", "reserved", "available"] {
            for resource in ["vcpu", "memory_mib", "disk_mib"] {
                let total: i64 = hosts
                    .iter()
                    .filter(|h| category == "reserved" || h["retired"] == false)
                    .filter_map(|h| h[category][resource].as_i64())
                    .sum();
                out.push_str(&format!(
                    "hudsonbox_fleet_{category}{{resource=\"{resource}\"}} {total}\n"
                ));
            }
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
                    _ => h["retired"] == false && h["ready"] == false && h["status"] != "draining",
                })
                .count();
            out.push_str(&format!(
                "hudsonbox_fleet_hosts{{state=\"{state}\"}} {count}\n"
            ));
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::prometheus;
    use serde_json::json;

    #[test]
    fn fixed_labels_partition_hosts_and_exclude_tombstone_capacity() {
        let status = json!({
            "queue":{"queued":0,"oldest_age_seconds":0,"insufficient_capacity":0,
                "project_quota_exceeded":0,"no_ready_host":0,"no_compatible_host":0},
            "rejections":{"host_pool_access_denied":3,"private-user-value":99},
            "hosts":[
                {"host_id":"private-host","status":"draining","ready":false,"retired":false,
                 "capacity":{"vcpu":4},"reserved":{"vcpu":2},"available":{"vcpu":2}},
                {"host_id":"private-tombstone","status":"draining","ready":false,"retired":true,
                 "capacity":{"vcpu":8},"reserved":{"vcpu":0},"available":{"vcpu":0}},
                {"status":"ready","ready":true,"retired":false,"capacity":{"vcpu":4}},
                {"status":"unhealthy","ready":false,"retired":false,"capacity":{"vcpu":4}}
            ]
        });
        let text = prometheus(&status);
        for state in ["ready", "unavailable", "retired", "draining"] {
            assert!(text.contains(&format!("hudsonbox_fleet_hosts{{state=\"{state}\"}} 1\n")));
        }
        assert!(text.contains("hudsonbox_fleet_capacity{resource=\"vcpu\"} 12\n"));
        assert!(text.contains("hudsonbox_fleet_reserved{resource=\"vcpu\"} 2\n"));
        assert!(
            text.contains("hudsonbox_fleet_rejections{reason=\"host_pool_access_denied\"} 3\n")
        );
        assert!(!text.contains("private-"));
        assert!(!text.contains("null"));
    }
}
