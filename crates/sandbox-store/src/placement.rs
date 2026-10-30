//! Multi-host reservations. No method here starts a VM or releases capacity.
//!
//! Lock order is operation -> project -> sandbox -> candidate hosts in
//! ascending host ID. Every placement writer takes the same project lock and
//! the same host-row locks before reading aggregate usage, so concurrent
//! controller workers cannot overbook a host or a project, and two placements
//! that share hosts always acquire them in the same order (no deadlock).
//! Existing reservations always require reconciliation, even when a lease expired.

use sandbox_protocol::{
    AllocationId, HostId, Id,
    resources::{ARTIFACT_DISK_OVERHEAD_MIB, VMM_MEMORY_OVERHEAD_MIB},
};
use serde_json::{Value, json};
use sqlx::Row;

use crate::{Store, claims::Claim, dispatch::CreateRejection};

/// Bound on waiting for placement, measured from operation admission by the
/// database clock. After it, the create fails with the last waiting reason.
pub const PLACEMENT_WAIT_SECONDS: i64 = 300;
/// Retry delays grow with time spent waiting and stay within these bounds.
pub const PLACEMENT_RETRY_MIN_SECONDS: i64 = 2;
pub const PLACEMENT_RETRY_MAX_SECONDS: i64 = 30;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Allocation {
    pub id: AllocationId,
    pub host_id: HostId,
    pub generation: i64,
    pub supervisor_epoch: i64,
}

#[derive(Debug, PartialEq, Eq)]
pub enum Reservation {
    /// Capacity is reserved; boot has not been dispatched or confirmed.
    Reserved(Allocation),
    /// Reconcile this incarnation instead of allocating or blindly starting another.
    Existing(Allocation),
}

/// Which hosts a placement may choose from. Both scopes are further limited to
/// the project's granted pools; neither falls back outside them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HostScope {
    /// Every ready registered host with a transport identity
    /// ([`crate::host_registry::RegisteredHost::admits_new_work`]).
    Fleet,
    /// Exactly the host a single-host controller is configured to reach, at
    /// its configured epoch. A legacy host without a registered transport
    /// identity is eligible only through this scope.
    Configured { host: HostId, epoch: i64 },
}

#[derive(Debug, thiserror::Error)]
pub enum PlacementError {
    #[error("operation claim expired or changed")]
    LostClaim,
    #[error("current project credential does not permit execution")]
    Unauthorized,
    #[error("project has no access to an eligible host pool")]
    NoPoolAccess,
    #[error("sandbox requires reconciliation before placement")]
    Reconcile,
    #[error("no allowed host is ready, fresh, or at the expected epoch")]
    HostUnavailable,
    #[error("no allowed host's capabilities permit the requested image or resources")]
    HostCompatibility,
    #[error("insufficient host capacity")]
    Capacity,
    #[error("project allocation quota exceeded")]
    Quota,
    #[error("invalid resource or quota metadata")]
    InvalidResources,
    #[error("placement storage query failed: {0}")]
    Query(#[from] sqlx::Error),
}

impl PlacementError {
    /// Conditions that can clear without operator or caller action. The
    /// operation waits (bounded by [`PLACEMENT_WAIT_SECONDS`]) instead of failing.
    #[must_use]
    pub fn wait_reason(&self) -> Option<PlacementWait> {
        match self {
            Self::Capacity => Some(PlacementWait::InsufficientCapacity),
            Self::Quota => Some(PlacementWait::ProjectQuota),
            Self::HostUnavailable => Some(PlacementWait::NoReadyHost),
            Self::HostCompatibility => Some(PlacementWait::NoCompatibleHost),
            _ => None,
        }
    }
}

/// Why a create is still queued. Recorded on the operation while it waits and
/// used as the failure code once the wait bound passes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlacementWait {
    InsufficientCapacity,
    ProjectQuota,
    NoReadyHost,
    NoCompatibleHost,
}

impl PlacementWait {
    #[must_use]
    pub fn code(self) -> &'static str {
        match self {
            Self::InsufficientCapacity => "insufficient_capacity",
            Self::ProjectQuota => "project_quota_exceeded",
            Self::NoReadyHost => "no_ready_host",
            Self::NoCompatibleHost => "no_compatible_host",
        }
    }

    #[must_use]
    pub fn rejection(self) -> CreateRejection {
        match self {
            Self::InsufficientCapacity => CreateRejection::InsufficientCapacity,
            Self::ProjectQuota => CreateRejection::ProjectQuota,
            Self::NoReadyHost => CreateRejection::NoReadyHost,
            Self::NoCompatibleHost => CreateRejection::NoCompatibleHost,
        }
    }

    fn message(self) -> &'static str {
        match self {
            Self::InsufficientCapacity => {
                "No allowed host has enough free capacity; placement is retried."
            }
            Self::ProjectQuota => "The project's allocation quota is in use; placement is retried.",
            Self::NoReadyHost => "No allowed host is ready and fresh; placement is retried.",
            Self::NoCompatibleHost => {
                "No allowed host supports this image and size; placement is retried."
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WaitOutcome {
    /// Claim released with a bounded retry time; the operation stays queued.
    Waiting,
    /// The wait bound passed. Fail the create with [`PlacementWait::rejection`].
    Expired,
}

fn positive(value: &Value, key: &str) -> Result<i64, PlacementError> {
    value
        .get(key)
        .and_then(Value::as_i64)
        .filter(|v| *v > 0)
        .ok_or(PlacementError::InvalidResources)
}

fn limit(value: &Value, key: &str, default: i64) -> Result<i64, PlacementError> {
    match value.get(key) {
        None => Ok(default),
        Some(v) => v
            .as_i64()
            .filter(|v| *v >= 0)
            .ok_or(PlacementError::InvalidResources),
    }
}

/// Host resources one allocation consumes: the request plus runtime overhead.
#[derive(Debug, Clone, Copy)]
struct Footprint {
    vcpu: i64,
    memory_mib: i64,
    disk_mib: i64,
}

impl Footprint {
    fn of(vcpu: i64, memory_mib: i64, disk_mib: i64) -> Self {
        Self {
            vcpu,
            memory_mib: memory_mib.saturating_add(VMM_MEMORY_OVERHEAD_MIB),
            disk_mib: disk_mib.saturating_add(ARTIFACT_DISK_OVERHEAD_MIB),
        }
    }
}

/// Remaining host capacity after a placement, used to rank candidates.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct Headroom {
    memory_mib: i64,
    vcpu: i64,
    disk_mib: i64,
}

/// Capacity held on each locked host. Unreleased allocations of every status
/// count, including uncertain and expired-lease ones. Seam: unassigned
/// warm-pool member reservations (#188) must be added here, under the same
/// host locks, before warm pools hold capacity.
async fn held(
    tx: &mut sqlx::PgConnection,
    hosts: &[uuid::Uuid],
) -> Result<std::collections::HashMap<uuid::Uuid, Footprint>, PlacementError> {
    let rows: Vec<(uuid::Uuid, i64, i64, i64)> = sqlx::query_as(
        "SELECT host_id,COALESCE(sum(vcpu),0)::bigint,
         COALESCE(sum(memory_mib+$2),0)::bigint,COALESCE(sum(disk_mib+$3),0)::bigint
         FROM allocations WHERE host_id=ANY($1) AND released_at IS NULL GROUP BY host_id",
    )
    .bind(hosts)
    .bind(VMM_MEMORY_OVERHEAD_MIB)
    .bind(ARTIFACT_DISK_OVERHEAD_MIB)
    .fetch_all(&mut *tx)
    .await?;
    Ok(rows
        .into_iter()
        .map(|(id, vcpu, memory_mib, disk_mib)| {
            (
                id,
                Footprint {
                    vcpu,
                    memory_mib,
                    disk_mib,
                },
            )
        })
        .collect())
}

impl Store {
    /// Reserve on the one host a single-host controller is configured to
    /// reach. Equivalent to [`Store::place_create`] with [`HostScope::Configured`].
    pub async fn reserve_create(
        &self,
        claim: &Claim,
        host_id: HostId,
        expected_epoch: i64,
    ) -> Result<Reservation, PlacementError> {
        self.place_create(
            claim,
            HostScope::Configured {
                host: host_id,
                epoch: expected_epoch,
            },
        )
        .await
    }

    /// Select a ready, fresh, compatible host with room for the request plus
    /// runtime overhead inside the project's granted pools, and persist the
    /// allocation, permit serial and `reserved` receipt before any dispatch.
    /// This does not authenticate hosts or inspect image bytes; dispatch
    /// rechecks host and authority under the host lock before the first RPC.
    pub async fn place_create(
        &self,
        claim: &Claim,
        scope: HostScope,
    ) -> Result<Reservation, PlacementError> {
        let mut tx = self.pool().begin().await?;
        let op = sqlx::query(
            "SELECT * FROM operations WHERE id=$1 AND claim_revision=$2
             AND lease_expires_at > clock_timestamp() AND status IN ('running','unknown')
             FOR UPDATE",
        )
        .bind(claim.operation_id.uuid())
        .bind(claim.revision)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or(PlacementError::LostClaim)?;
        let project_id: uuid::Uuid = op.try_get("project_id")?;
        let sandbox_id: uuid::Uuid = op.try_get("sandbox_id")?;
        let project = sqlx::query("SELECT * FROM projects WHERE id=$1 FOR UPDATE")
            .bind(project_id)
            .fetch_one(&mut *tx)
            .await?;
        let sandbox = sqlx::query("SELECT * FROM sandboxes WHERE id=$1 FOR UPDATE")
            .bind(sandbox_id)
            .fetch_one(&mut *tx)
            .await?;
        if op.try_get::<String, _>("kind")? != "create"
            || sandbox.try_get::<Option<uuid::Uuid>, _>("active_transition_operation_id")?
                != Some(claim.operation_id.uuid())
        {
            return Err(PlacementError::Reconcile);
        }

        // Resolve an existing incarnation before checking today's authority or
        // capacity. A revoked caller still needs service-owned reconciliation,
        // and an existing allocation stays pinned to its owner host and epoch.
        if let Some(id) = sandbox.try_get::<Option<uuid::Uuid>, _>("current_allocation_id")? {
            let row = sqlx::query("SELECT * FROM allocations WHERE id=$1 AND released_at IS NULL")
                .bind(id)
                .fetch_optional(&mut *tx)
                .await?
                .ok_or(PlacementError::Reconcile)?;
            let allocation = Allocation {
                id: AllocationId::from_uuid(id),
                host_id: HostId::from_uuid(row.try_get("host_id")?),
                generation: row.try_get("generation")?,
                supervisor_epoch: row.try_get("supervisor_epoch")?,
            };
            tx.commit().await?;
            return Ok(Reservation::Existing(allocation));
        }
        if op.try_get::<String, _>("status")? == "unknown"
            || op.try_get::<i32, _>("attempt_count")? != 0
            || sandbox.try_get::<i64, _>("generation")? != 0
            || sandbox.try_get::<String, _>("observed_state")? != "creating"
            || sandbox.try_get::<String, _>("desired_state")? != "running"
        {
            return Err(PlacementError::Reconcile);
        }
        let resources: Value = sandbox.try_get("resources")?;
        let cpu = positive(&resources, "vcpu")?;
        let memory = positive(&resources, "memory_mib")?;
        let disk = positive(&resources, "disk_mib")?;
        if !sandbox_protocol::resources::supported(cpu.into(), memory.into(), disk.into()) {
            return Err(PlacementError::InvalidResources);
        }
        let limits: Value = project.try_get("limits")?;
        let project_usage: (i64, i64, i64, i64) = sqlx::query_as(
            "SELECT count(*), COALESCE(sum(vcpu),0)::bigint,
             COALESCE(sum(memory_mib),0)::bigint, COALESCE(sum(disk_mib),0)::bigint
             FROM allocations WHERE project_id=$1 AND released_at IS NULL",
        )
        .bind(project_id)
        .fetch_one(&mut *tx)
        .await?;
        let over_quota = project_usage.0 >= limit(&limits, "sandboxes", 25)?
            || cpu > limit(&limits, "vcpu", 100)?.saturating_sub(project_usage.1)
            || memory > limit(&limits, "memory_mib", 204800)?.saturating_sub(project_usage.2)
            || disk > limit(&limits, "disk_mib", 1638400)?.saturating_sub(project_usage.3);

        // Grants only grow (there is no revoke), so reading them before host
        // locks cannot admit a project into a pool it was never granted.
        let pools: Vec<uuid::Uuid> = sqlx::query_scalar(
            "SELECT host_pool_id FROM project_host_pool_access WHERE project_id=$1",
        )
        .bind(project_id)
        .fetch_all(&mut *tx)
        .await?;
        let (only, epoch) = match scope {
            HostScope::Fleet => (None, None),
            HostScope::Configured { host, epoch } => (Some(host.uuid()), Some(epoch)),
        };
        // Static eligibility is evaluated again on each row after its lock is
        // granted, so a drain or epoch change that commits first excludes the
        // host. Locks are taken in ascending host ID by every placement.
        let hosts = sqlx::query(
            "SELECT * FROM hosts WHERE host_pool_id=ANY($1) AND status='ready' AND supervisor_epoch>0
             AND ($2::uuid IS NULL OR (id=$2 AND supervisor_epoch=$3))
             AND ($2::uuid IS NOT NULL OR endpoint IS NOT NULL)
             ORDER BY id FOR UPDATE",
        )
        .bind(&pools)
        .bind(only)
        .bind(epoch)
        .fetch_all(&mut *tx)
        .await?;
        // Time predicates run after acquiring every row lock. A host observation
        // or credential may expire while another transaction holds the host.
        let ids: Vec<uuid::Uuid> = hosts
            .iter()
            .map(|h| h.try_get("id"))
            .collect::<Result<_, _>>()?;
        let fresh: Vec<uuid::Uuid> = sqlx::query_scalar(
            "SELECT id FROM hosts WHERE id=ANY($1)
             AND COALESCE(last_seen_at > clock_timestamp()-interval '30 seconds',false)",
        )
        .bind(&ids)
        .fetch_all(&mut *tx)
        .await?;
        // Use persisted initiator identity, never the claim object's project fields.
        let authorized: (bool,) = sqlx::query_as(
            "SELECT p.status='active' AND o.initiator_kind='project'
             AND (o.deadline IS NULL OR o.deadline > clock_timestamp())
             AND (s.expires_at IS NULL OR s.expires_at > clock_timestamp())
             AND EXISTS (SELECT 1 FROM jsonb_array_elements(p.api_tokens) t
                WHERE t->>'key_id'=o.initiator_key_id
                AND (t->>'revoked_at' IS NULL OR (t->>'revoked_at')::timestamptz > clock_timestamp())
                AND (t->>'expires_at' IS NULL OR (t->>'expires_at')::timestamptz > clock_timestamp()))
             FROM projects p JOIN operations o ON o.project_id=p.id
             JOIN sandboxes s ON s.id=o.sandbox_id WHERE o.id=$1",
        ).bind(claim.operation_id.uuid()).fetch_one(&mut *tx).await?;
        if !authorized.0 {
            return Err(PlacementError::Unauthorized);
        }
        let granted = match only {
            None => !pools.is_empty(),
            Some(host) => {
                let pool: Option<uuid::Uuid> =
                    sqlx::query_scalar("SELECT host_pool_id FROM hosts WHERE id=$1")
                        .bind(host)
                        .fetch_optional(&mut *tx)
                        .await?;
                pool.is_some_and(|pool| pools.contains(&pool))
            }
        };
        if !granted {
            return Err(PlacementError::NoPoolAccess);
        }
        if over_quota {
            return Err(PlacementError::Quota);
        }
        let image: String = sandbox.try_get("image_digest")?;
        let mut ready = 0;
        let mut compatible = Vec::new();
        for host in &hosts {
            let id: uuid::Uuid = host.try_get("id")?;
            if !fresh.contains(&id) {
                continue;
            }
            ready += 1;
            // The legacy compatibility exception belongs only to an explicitly
            // configured host. Fleet selection must establish image/architecture
            // support rather than treating a registered address as evidence.
            if matches!(scope, HostScope::Fleet)
                && host
                    .try_get::<Option<Value>, _>("runtime_capabilities")?
                    .is_none()
            {
                continue;
            }
            if crate::host_capabilities::compatible(
                host,
                &image,
                cpu as u64,
                memory as u64,
                disk as u64,
            )
            .is_ok()
            {
                compatible.push(host);
            }
        }
        if ready == 0 {
            return Err(PlacementError::HostUnavailable);
        }
        if compatible.is_empty() {
            return Err(PlacementError::HostCompatibility);
        }

        let want = Footprint::of(cpu, memory, disk);
        let candidate_ids: Vec<uuid::Uuid> = compatible
            .iter()
            .map(|h| h.try_get("id"))
            .collect::<Result<_, _>>()?;
        let held = held(&mut tx, &candidate_ids).await?;
        // Spread: prefer the host left with the most free memory, then CPU,
        // then disk; ties go to the lowest host ID so selection is deterministic.
        let mut best: Option<(Headroom, uuid::Uuid, i64)> = None;
        for host in compatible {
            let id: uuid::Uuid = host.try_get("id")?;
            let used = held.get(&id).copied().unwrap_or(Footprint {
                vcpu: 0,
                memory_mib: 0,
                disk_mib: 0,
            });
            let room = Headroom {
                vcpu: i64::from(host.try_get::<i32, _>("cpu_capacity")?)
                    .saturating_sub(used.vcpu)
                    .saturating_sub(want.vcpu),
                memory_mib: host
                    .try_get::<i64, _>("memory_capacity_mib")?
                    .saturating_sub(used.memory_mib)
                    .saturating_sub(want.memory_mib),
                disk_mib: host
                    .try_get::<i64, _>("disk_capacity_mib")?
                    .saturating_sub(used.disk_mib)
                    .saturating_sub(want.disk_mib),
            };
            if room.vcpu < 0 || room.memory_mib < 0 || room.disk_mib < 0 {
                continue;
            }
            if best.as_ref().is_none_or(|(b, _, _)| room > *b) {
                best = Some((room, id, host.try_get("supervisor_epoch")?));
            }
        }
        let (_, host_uuid, host_epoch) = best.ok_or(PlacementError::Capacity)?;
        let host_id = HostId::from_uuid(host_uuid);
        let allocation = Allocation {
            id: AllocationId::generate(),
            host_id,
            generation: 1,
            supervisor_epoch: host_epoch,
        };
        sqlx::query(
            "INSERT INTO allocations(id,project_id,sandbox_id,host_id,generation,supervisor_epoch,
             vcpu,memory_mib,disk_mib,status) VALUES($1,$2,$3,$4,1,$5,$6,$7,$8,'reserved')",
        )
        .bind(allocation.id.uuid())
        .bind(project_id)
        .bind(sandbox_id)
        .bind(host_id.uuid())
        .bind(host_epoch)
        .bind(cpu as i32)
        .bind(memory)
        .bind(disk)
        .execute(&mut *tx)
        .await?;
        // The host row is already locked. Issuance and the allocation either
        // commit together or both roll back; do not replace this with a sequence.
        let serial: i64 = sqlx::query_scalar(
            "UPDATE hosts SET last_allocation_serial=last_allocation_serial+1
             WHERE id=$1 AND last_allocation_serial<9223372036854775807
             RETURNING last_allocation_serial",
        )
        .bind(host_id.uuid())
        .fetch_optional(&mut *tx)
        .await?
        .ok_or(PlacementError::Capacity)?;
        sqlx::query(
            "INSERT INTO allocation_permits(allocation_id,host_id,serial,project_id,
             sandbox_id,create_operation_id,generation,original_epoch)
             VALUES($1,$2,$3,$4,$5,$6,1,$7)",
        )
        .bind(allocation.id.uuid())
        .bind(host_id.uuid())
        .bind(serial)
        .bind(project_id)
        .bind(sandbox_id)
        .bind(claim.operation_id.uuid())
        .bind(host_epoch)
        .execute(&mut *tx)
        .await?;
        sqlx::query("UPDATE sandboxes SET current_allocation_id=$1,generation=1,state_revision=state_revision+1,updated_at=clock_timestamp() WHERE id=$2")
            .bind(allocation.id.uuid()).bind(sandbox_id).execute(&mut *tx).await?;
        // Check the lease again after potentially waiting for quota/host locks.
        // If it expired during placement, the entire reservation rolls back.
        // A waiting reason recorded earlier is cleared by the reservation.
        let changed = sqlx::query(
            "UPDATE operations SET phase='reserved',error=NULL,updated_at=clock_timestamp(),
             attempt_receipts=attempt_receipts || jsonb_build_array($3::jsonb)
             WHERE id=$1 AND claim_revision=$2 AND lease_expires_at > clock_timestamp()",
        )
        .bind(claim.operation_id.uuid())
        .bind(claim.revision)
        .bind(
            json!({"phase":"reserved","allocation_id":allocation.id.to_string(),
                "host_id":host_id.to_string(),"generation":1,"supervisor_epoch":host_epoch,
                "claim_revision":claim.revision}),
        )
        .execute(&mut *tx)
        .await?
        .rows_affected();
        if changed != 1 {
            return Err(PlacementError::LostClaim);
        }
        tx.commit().await?;
        Ok(Reservation::Reserved(allocation))
    }

    /// Keep an unplaced create queued with a visible reason and a bounded
    /// retry delay, releasing the claim. Once [`PLACEMENT_WAIT_SECONDS`] have
    /// passed since admission this changes nothing and returns
    /// [`WaitOutcome::Expired`]; the caller then fails the create through
    /// service-owned rejection. Only creates with no dispatch attempt qualify.
    pub async fn await_placement(
        &self,
        claim: &Claim,
        reason: PlacementWait,
    ) -> Result<WaitOutcome, PlacementError> {
        let mut tx = self.pool().begin().await?;
        let row: Option<(bool,)> = sqlx::query_as(
            "SELECT clock_timestamp() >= created_at + make_interval(secs => $3) FROM operations
             WHERE id=$1 AND claim_revision=$2 AND lease_expires_at > clock_timestamp()
             AND status='running' AND kind='create' AND attempt_count=0
             AND (phase IS NULL OR phase IN ('queued','awaiting_placement')) FOR UPDATE",
        )
        .bind(claim.operation_id.uuid())
        .bind(claim.revision)
        .bind(PLACEMENT_WAIT_SECONDS as f64)
        .fetch_optional(&mut *tx)
        .await?;
        let expired = row.ok_or(PlacementError::LostClaim)?.0;
        if expired {
            return Ok(WaitOutcome::Expired);
        }
        sqlx::query(
            "UPDATE operations SET lease_expires_at=NULL,phase='awaiting_placement',error=$3,
             next_retry_at=clock_timestamp()+make_interval(secs => LEAST($5::float8,GREATEST($4::float8,
                extract(epoch FROM clock_timestamp()-created_at)/10))),
             updated_at=clock_timestamp() WHERE id=$1 AND claim_revision=$2",
        )
        .bind(claim.operation_id.uuid())
        .bind(claim.revision)
        .bind(json!({"code":reason.code(),"message":reason.message(),"retryable":true}))
        .bind(PLACEMENT_RETRY_MIN_SECONDS as f64)
        .bind(PLACEMENT_RETRY_MAX_SECONDS as f64)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(WaitOutcome::Waiting)
    }
}
