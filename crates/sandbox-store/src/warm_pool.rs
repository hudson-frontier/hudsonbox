//! Warm pools (#188): immutable templates, pool policies, prepared-member
//! reservations, atomic once-only claims and durable warm capacity work.
//!
//! Warm supervisor capacity (a ready host) is not a warm VM. A member is one
//! prepared, unused microVM reservation on one host at one supervisor epoch.
//! This module never starts, adopts or destroys a VM. It persists intent,
//! verifies supervisor receipts against that intent and moves reservations.
//! The controller routes creates here before cold placement; only hosts that
//! implement warm adoption ever produce ready members (see docs/warm-pools.md).
//!
//! Lock order: create operation -> project -> sandbox -> policies (ascending)
//! -> hosts (ascending) -> members. Receipt recording takes the capacity
//! intent, then the create operation (if claimed), then the member. Placement
//! takes operation -> project -> sandbox -> hosts, a prefix of the same order.
//!
//! Capacity accounting is [`crate::placement::WARM_HELD`]: preparing, ready and
//! retiring members hold their reservation; a claiming member's reservation is
//! held by its allocation, created in the same transaction with the same size.

use sandbox_protocol::{
    AllocationId, HostId, Id, ProjectId, SandboxId,
    warm_pool::{
        CleanupReceipt, HandoffIntent, HandoffOutcome, HandoffReceipt, PrepareIntent,
        PrepareReceipt, VERSION,
    },
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sqlx::Row;
use std::collections::BTreeSet;
use uuid::Uuid;

use crate::{
    Store,
    claims::Claim,
    placement::{Allocation, Footprint, PlacementError, held},
};

/// Claims per warm capacity intent; an exhausted intent stays unfinished until
/// abandoned to retirement (prepare) or completed by cleanup evidence (retire).
pub const MAX_ATTEMPTS: i64 = 8;
/// Bound on queued/running warm intents per policy.
pub const MAX_PENDING: i64 = 64;
/// Bound on members created or retired by one reconcile pass.
pub const MAX_BUDGET: u32 = 16;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("invalid warm-pool input or bound")]
    Invalid,
    #[error("warm-pool request conflicts with retained state")]
    Conflict,
    #[error("snapshot-backed templates require durable published snapshots (#178)")]
    SnapshotUnavailable,
    #[error("project is not authorized for this template or pool")]
    Unauthorized,
    #[error("project has no access to the warm pool's host pool")]
    NoPoolAccess,
    #[error("project allocation quota exceeded")]
    Quota,
    #[error("sandbox image is not on the controller's allowlist")]
    ImageDenied,
    #[error("sandbox requires reconciliation before a warm claim")]
    Reconcile,
    #[error("claim expired or was replaced")]
    LostClaim,
    #[error("receipt epoch is no longer the host's current epoch")]
    StaleEpoch,
    #[error("supervisor receipt rejected: {0}")]
    Receipt(#[from] sandbox_protocol::warm_pool::Error),
    #[error("warm-pool storage query failed")]
    Query(#[from] sqlx::Error),
}

impl From<PlacementError> for Error {
    fn from(value: PlacementError) -> Self {
        match value {
            PlacementError::Query(e) => Self::Query(e),
            _ => Self::Conflict,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Publication {
    /// Usable only by the owning project.
    Private,
    /// Explicitly published for any project with pool access.
    Shared,
}

impl Publication {
    fn as_str(self) -> &'static str {
        match self {
            Self::Private => "private",
            Self::Shared => "shared",
        }
    }
}

/// One immutable template version. Never updated; a change is a new `id`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TemplateSpec {
    pub id: Uuid,
    pub name: String,
    pub version: u32,
    pub owner_project_id: Option<ProjectId>,
    pub publication: Publication,
    pub image_digest: String,
    pub vcpu: u32,
    pub memory_mib: u64,
    pub disk_mib: u64,
    pub boot_config: Value,
    pub compatibility: Value,
    /// Must be absent until #178 provides durable, published snapshots.
    pub snapshot_id: Option<Uuid>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PolicySpec {
    pub id: Uuid,
    pub template_id: Uuid,
    pub host_pool_id: Uuid,
    /// Dedicated policy owner; `None` is a shared policy.
    pub owner_project_id: Option<ProjectId>,
    pub min_ready: u32,
    pub max_total: u32,
    pub max_per_host: u32,
    pub idle_ttl_seconds: u32,
    pub enabled: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WarmWork {
    Prepare(PrepareIntent),
    Retire {
        member_id: Uuid,
        vm_ref: Option<String>,
        host_id: HostId,
        supervisor_epoch: i64,
    },
}

/// A fenced claim of one warm capacity intent.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WarmClaim {
    pub id: Uuid,
    pub policy: Uuid,
    pub revision: i64,
    pub work: WarmWork,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Reconciled {
    pub prepare: Vec<Uuid>,
    pub retire: Vec<Uuid>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WarmHandoff {
    pub allocation: Allocation,
    pub intent: HandoffIntent,
}

/// How the controller may contact the supervisor for a claimed member.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdoptMode {
    /// First dispatch under a still-authorized create: send AdoptWarmMember.
    Adopt,
    /// An adopt may already have been sent, or the create is no longer
    /// authorized: only InspectWarmHandoff, which never adopts and fences an
    /// unadopted member. A lost acknowledgement is never replayed as Adopt.
    Inspect,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdoptDispatch {
    pub handoff: WarmHandoff,
    pub mode: AdoptMode,
    pub allocation_expires_unix_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Claimed {
    /// A ready member was claimed; persist and inspect its handoff.
    Claimed(WarmHandoff),
    /// This create already claimed a member; resume its durable intent.
    Existing(WarmHandoff),
    /// No ready, compatible, authorized member. Use ordinary cold placement.
    None,
}

fn image_ok(digest: &str) -> bool {
    digest.len() == 71
        && digest.starts_with("sha256:")
        && digest[7..]
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

fn bounded_policy(spec: &PolicySpec) -> bool {
    spec.max_total >= spec.min_ready
        && spec.max_total <= 256
        && (1..=64).contains(&spec.max_per_host)
        && (60..=86400).contains(&spec.idle_ttl_seconds)
}

async fn timeouts(tx: &mut sqlx::PgConnection) -> Result<(), sqlx::Error> {
    sqlx::query("SET LOCAL statement_timeout='5s'")
        .execute(&mut *tx)
        .await?;
    sqlx::query("SET LOCAL lock_timeout='2s'")
        .execute(&mut *tx)
        .await?;
    Ok(())
}

/// Enqueue one warm intent for a member. Keys are per member and kind, so a
/// retried reconcile cannot duplicate work.
async fn enqueue(
    tx: &mut sqlx::PgConnection,
    host_pool: Uuid,
    policy: Uuid,
    kind: &str,
    member: Uuid,
) -> Result<Uuid, Error> {
    let id = Uuid::now_v7();
    let key = format!("{kind}:{member}");
    let row: Uuid = sqlx::query_scalar(
        "INSERT INTO capacity_operations(id,host_pool_id,warm_pool_id,kind,target_ref,idempotency_key,request)
         VALUES($1,$2,$3,$4,$5,$6,$7)
         ON CONFLICT (host_pool_id,idempotency_key) DO UPDATE SET updated_at=capacity_operations.updated_at
         RETURNING id",
    )
    .bind(id)
    .bind(host_pool)
    .bind(policy)
    .bind(kind)
    .bind(member.to_string())
    .bind(&key)
    .bind(json!({"version":1,"member_id":member}))
    .fetch_one(&mut *tx)
    .await?;
    Ok(row)
}

/// Retire one unassigned member and enqueue its fenced cleanup.
async fn begin_retire(
    tx: &mut sqlx::PgConnection,
    host_pool: Uuid,
    policy: Uuid,
    member: Uuid,
    reason: &str,
) -> Result<Uuid, Error> {
    sqlx::query(
        "UPDATE warm_pool_members SET state='retiring',retire_reason=COALESCE(retire_reason,$2),
         lease_expires_at=NULL,updated_at=clock_timestamp() WHERE id=$1",
    )
    .bind(member)
    .bind(reason)
    .execute(&mut *tx)
    .await?;
    enqueue(tx, host_pool, policy, "warm_retire", member).await
}

fn handoff_from(row: &sqlx::postgres::PgRow) -> Result<WarmHandoff, Error> {
    let intent: HandoffIntent =
        serde_json::from_value(row.try_get("handoff_intent")?).map_err(|_| Error::Conflict)?;
    Ok(WarmHandoff {
        allocation: Allocation {
            id: intent.allocation_id,
            host_id: intent.host_id,
            generation: 1,
            supervisor_epoch: intent.supervisor_epoch,
        },
        intent,
    })
}

impl Store {
    /// Insert one immutable template version. An exact retry is accepted;
    /// any change to an existing `id` is a conflict.
    pub async fn put_sandbox_template(&self, spec: &TemplateSpec) -> Result<(), Error> {
        if spec.snapshot_id.is_some() {
            return Err(Error::SnapshotUnavailable);
        }
        let name_ok = !spec.name.is_empty()
            && spec.name.len() <= 63
            && spec
                .name
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
            && !spec.name.starts_with('-')
            && !spec.name.ends_with('-');
        if !name_ok
            || spec.version == 0
            || !image_ok(&spec.image_digest)
            || !sandbox_protocol::resources::supported(
                spec.vcpu.into(),
                spec.memory_mib.into(),
                spec.disk_mib.into(),
            )
            || !spec.boot_config.is_object()
            || !spec.compatibility.is_object()
            || (spec.owner_project_id.is_none() && spec.publication != Publication::Shared)
            || serde_json::to_vec(&spec.boot_config).map_or(true, |v| v.len() > 16384)
            || serde_json::to_vec(&spec.compatibility).map_or(true, |v| v.len() > 16384)
        {
            return Err(Error::Invalid);
        }
        let version = i32::try_from(spec.version).map_err(|_| Error::Invalid)?;
        let memory = i64::try_from(spec.memory_mib).map_err(|_| Error::Invalid)?;
        let disk = i64::try_from(spec.disk_mib).map_err(|_| Error::Invalid)?;
        let cpu = i32::try_from(spec.vcpu).map_err(|_| Error::Invalid)?;
        let mut tx = self.pool().begin().await?;
        timeouts(&mut tx).await?;
        let owner = spec.owner_project_id.map(|p| p.uuid());
        sqlx::query(
            "INSERT INTO sandbox_templates(id,name,version,owner_project_id,publication,image_digest,
             vcpu,memory_mib,disk_mib,boot_config,compatibility)
             VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11) ON CONFLICT DO NOTHING",
        )
        .bind(spec.id)
        .bind(&spec.name)
        .bind(version)
        .bind(owner)
        .bind(spec.publication.as_str())
        .bind(&spec.image_digest)
        .bind(cpu)
        .bind(memory)
        .bind(disk)
        .bind(&spec.boot_config)
        .bind(&spec.compatibility)
        .execute(&mut *tx)
        .await
        .map_err(|e| match e {
            sqlx::Error::Database(d) if d.is_foreign_key_violation() => Error::Conflict,
            other => Error::Query(other),
        })?;
        let same: Option<bool> = sqlx::query_scalar(
            "SELECT name=$2 AND version=$3 AND owner_project_id IS NOT DISTINCT FROM $4
             AND publication=$5 AND image_digest=$6 AND vcpu=$7 AND memory_mib=$8 AND disk_mib=$9
             AND boot_config=$10 AND compatibility=$11 AND snapshot_id IS NULL
             FROM sandbox_templates WHERE id=$1",
        )
        .bind(spec.id)
        .bind(&spec.name)
        .bind(version)
        .bind(owner)
        .bind(spec.publication.as_str())
        .bind(&spec.image_digest)
        .bind(cpu)
        .bind(memory)
        .bind(disk)
        .bind(&spec.boot_config)
        .bind(&spec.compatibility)
        .fetch_optional(&mut *tx)
        .await?;
        if same != Some(true) {
            // Another id already holds this name/version, or the id changed.
            return Err(Error::Conflict);
        }
        tx.commit().await?;
        Ok(())
    }

    /// Create or adjust a warm-pool policy. Template, host pool and owner are
    /// fixed at creation. A dedicated host pool needs a dedicated policy owned
    /// by its granted project; a shared policy needs a shared host pool and an
    /// explicitly shared template. Lowering bounds never deletes members: the
    /// next reconcile retires excess ready members through cleanup.
    pub async fn put_warm_pool_policy(&self, spec: &PolicySpec) -> Result<(), Error> {
        if !bounded_policy(spec) {
            return Err(Error::Invalid);
        }
        let owner = spec.owner_project_id.map(|p| p.uuid());
        let mut tx = self.pool().begin().await?;
        timeouts(&mut tx).await?;
        let pool_policy: Value =
            sqlx::query_scalar("SELECT placement_policy FROM host_pools WHERE id=$1 FOR SHARE")
                .bind(spec.host_pool_id)
                .fetch_optional(&mut *tx)
                .await?
                .ok_or(Error::Conflict)?;
        let dedicated = pool_policy.get("tenancy").and_then(Value::as_str) == Some("dedicated");
        let (template_owner, publication): (Option<Uuid>, String) = sqlx::query_as(
            "SELECT owner_project_id,publication FROM sandbox_templates WHERE id=$1",
        )
        .bind(spec.template_id)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or(Error::Conflict)?;
        match owner {
            None => {
                if dedicated || publication != "shared" {
                    return Err(Error::Unauthorized);
                }
            }
            Some(project) => {
                if template_owner != Some(project) && publication != "shared" {
                    return Err(Error::Unauthorized);
                }
                let granted: bool = sqlx::query_scalar(
                    "SELECT EXISTS(SELECT 1 FROM project_host_pool_access WHERE project_id=$1 AND host_pool_id=$2)",
                )
                .bind(project)
                .bind(spec.host_pool_id)
                .fetch_one(&mut *tx)
                .await?;
                if !granted {
                    return Err(Error::NoPoolAccess);
                }
            }
        }
        let inserted = sqlx::query(
            "INSERT INTO warm_pool_policies(id,template_id,host_pool_id,owner_project_id,min_ready,
             max_total,max_per_host,idle_ttl_seconds,enabled) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)
             ON CONFLICT (id) DO NOTHING",
        )
        .bind(spec.id)
        .bind(spec.template_id)
        .bind(spec.host_pool_id)
        .bind(owner)
        .bind(spec.min_ready as i32)
        .bind(spec.max_total as i32)
        .bind(spec.max_per_host as i32)
        .bind(spec.idle_ttl_seconds as i32)
        .bind(spec.enabled)
        .execute(&mut *tx)
        .await
        .map_err(|e| match e {
            sqlx::Error::Database(d) if d.is_unique_violation() => Error::Conflict,
            other => Error::Query(other),
        })?
        .rows_affected();
        if inserted == 0 {
            let changed = sqlx::query(
                "UPDATE warm_pool_policies SET min_ready=$5,max_total=$6,max_per_host=$7,
                 idle_ttl_seconds=$8,enabled=$9,updated_at=clock_timestamp()
                 WHERE id=$1 AND template_id=$2 AND host_pool_id=$3
                 AND owner_project_id IS NOT DISTINCT FROM $4",
            )
            .bind(spec.id)
            .bind(spec.template_id)
            .bind(spec.host_pool_id)
            .bind(owner)
            .bind(spec.min_ready as i32)
            .bind(spec.max_total as i32)
            .bind(spec.max_per_host as i32)
            .bind(spec.idle_ttl_seconds as i32)
            .bind(spec.enabled)
            .execute(&mut *tx)
            .await?
            .rows_affected();
            if changed != 1 {
                return Err(Error::Conflict);
            }
        }
        tx.commit().await?;
        Ok(())
    }

    /// One bounded, durable reconcile pass for a policy. Retires (TTL-expired,
    /// drained/stale-epoch/unhealthy-host, disabled-policy or excess) ready
    /// members and claiming members whose allocation was released, then
    /// reserves new `preparing` members toward `min_ready` within `max_total`,
    /// `max_per_host`, host capacity and the pending-intent bound. Each member
    /// change and its capacity intent commit together.
    pub async fn reconcile_warm_pool(
        &self,
        policy: Uuid,
        budget: u32,
    ) -> Result<Reconciled, Error> {
        if budget == 0 || budget > MAX_BUDGET {
            return Err(Error::Invalid);
        }
        let mut tx = self.pool().begin().await?;
        timeouts(&mut tx).await?;
        let p = sqlx::query(
            "SELECT w.*,t.image_digest,t.vcpu t_vcpu,t.memory_mib t_memory,t.disk_mib t_disk
             FROM warm_pool_policies w JOIN sandbox_templates t ON t.id=w.template_id
             WHERE w.id=$1 FOR UPDATE OF w",
        )
        .bind(policy)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or(Error::Conflict)?;
        let host_pool: Uuid = p.try_get("host_pool_id")?;
        let enabled: bool = p.try_get("enabled")?;
        let min_ready = i64::from(p.try_get::<i32, _>("min_ready")?);
        let max_total = i64::from(p.try_get::<i32, _>("max_total")?);
        let per_host = i64::from(p.try_get::<i32, _>("max_per_host")?);
        let hosts = sqlx::query("SELECT * FROM hosts WHERE host_pool_id=$1 ORDER BY id FOR UPDATE")
            .bind(host_pool)
            .fetch_all(&mut *tx)
            .await?;
        let mut out = Reconciled::default();
        let mut left = budget as usize;

        // Unusable unassigned members. Evaluated after host locks so a drain or
        // epoch advance that committed first is observed.
        let stale: Vec<(Uuid, String)> = sqlx::query_as(
            "SELECT m.id, CASE
                WHEN m.state='claiming' THEN 'claim_released'
                WHEN NOT $2 THEN 'policy_disabled'
                WHEN h.supervisor_epoch<>m.supervisor_epoch THEN 'stale_epoch'
                WHEN h.status<>'ready' OR h.capacity_retired_at IS NOT NULL THEN 'host_unavailable'
                ELSE 'idle_ttl' END
             FROM warm_pool_members m JOIN hosts h ON h.id=m.host_id
             LEFT JOIN allocations a ON a.id=m.claimed_allocation_id
             WHERE m.pool_id=$1 AND (
                (m.state='claiming' AND a.released_at IS NOT NULL)
                OR (m.state='ready' AND (NOT $2 OR h.supervisor_epoch<>m.supervisor_epoch
                    OR h.status<>'ready' OR h.capacity_retired_at IS NOT NULL
                    OR m.lease_expires_at<=clock_timestamp())))
             ORDER BY m.created_at,m.id LIMIT $3 FOR UPDATE OF m",
        )
        .bind(policy)
        .bind(enabled)
        .bind(left as i64)
        .fetch_all(&mut *tx)
        .await?;
        for (member, reason) in stale {
            out.retire
                .push(begin_retire(&mut tx, host_pool, policy, member, &reason).await?);
            left -= 1;
        }
        // Preparation whose final claim was lost (worker crash or lease expiry
        // on the last attempt) can never be claimed or abandoned again. Whether
        // a VM booted is unknown: retire it durably with the reservation held.
        // SKIP LOCKED: a live claim holder keeps its row; no lock waits here.
        let exhausted: Vec<(Uuid, Uuid)> = sqlx::query_as(
            "SELECT o.id,m.id FROM capacity_operations o
             JOIN warm_pool_members m ON m.id::text=o.target_ref AND m.pool_id=o.warm_pool_id
             WHERE o.warm_pool_id=$1 AND o.kind='warm_prepare' AND o.status IN ('queued','running')
             AND o.claim_revision>=$2 AND (o.lease_expires_at IS NULL OR o.lease_expires_at<=clock_timestamp())
             AND m.state='preparing'
             ORDER BY o.created_at,o.id LIMIT $3 FOR UPDATE OF o,m SKIP LOCKED",
        )
        .bind(policy)
        .bind(MAX_ATTEMPTS)
        .bind(left as i64)
        .fetch_all(&mut *tx)
        .await?;
        for (operation, member) in exhausted {
            let retire =
                begin_retire(&mut tx, host_pool, policy, member, "prepare_exhausted").await?;
            sqlx::query(
                "UPDATE capacity_operations SET status='succeeded',lease_expires_at=NULL,error=NULL,
                 provider_receipt=$2,updated_at=clock_timestamp() WHERE id=$1",
            )
            .bind(operation)
            .bind(json!({"version":1,"outcome":"exhausted_to_retirement","retire_operation":retire}))
            .execute(&mut *tx)
            .await?;
            out.retire.push(retire);
            left -= 1;
        }
        let (ready, total): (i64, i64) = sqlx::query_as(
            "SELECT count(*) FILTER (WHERE state='ready'),
             count(*) FILTER (WHERE state IN ('preparing','ready','claiming','retiring'))
             FROM warm_pool_members WHERE pool_id=$1",
        )
        .bind(policy)
        .fetch_one(&mut *tx)
        .await?;
        // Stock beyond a lowered max_total: retire newest ready members first.
        let excess = (total - max_total).min(ready).min(left as i64);
        if excess > 0 {
            let ids: Vec<Uuid> = sqlx::query_scalar(
                "SELECT id FROM warm_pool_members WHERE pool_id=$1 AND state='ready'
                 ORDER BY created_at DESC,id DESC LIMIT $2 FOR UPDATE",
            )
            .bind(policy)
            .bind(excess)
            .fetch_all(&mut *tx)
            .await?;
            for member in ids {
                out.retire
                    .push(begin_retire(&mut tx, host_pool, policy, member, "excess").await?);
                left -= 1;
            }
        }
        if !enabled {
            tx.commit().await?;
            return Ok(out);
        }
        let (ready, preparing, total): (i64, i64, i64) = sqlx::query_as(
            "SELECT count(*) FILTER (WHERE state='ready'),count(*) FILTER (WHERE state='preparing'),
             count(*) FILTER (WHERE state IN ('preparing','ready','claiming','retiring'))
             FROM warm_pool_members WHERE pool_id=$1",
        )
        .bind(policy)
        .fetch_one(&mut *tx)
        .await?;
        let pending: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM capacity_operations WHERE warm_pool_id=$1 AND status IN ('queued','running')",
        )
        .bind(policy)
        .fetch_one(&mut *tx)
        .await?;
        let want = (min_ready - ready - preparing)
            .min(max_total - total)
            .min(MAX_PENDING - pending)
            .min(left as i64);
        if want > 0 {
            let image: String = p.try_get("image_digest")?;
            let cpu = i64::from(p.try_get::<i32, _>("t_vcpu")?);
            let memory: i64 = p.try_get("t_memory")?;
            let disk: i64 = p.try_get("t_disk")?;
            let template: Uuid = p.try_get("template_id")?;
            let fresh: Vec<Uuid> = sqlx::query_scalar(
                "SELECT id FROM hosts WHERE host_pool_id=$1 AND status='ready' AND supervisor_epoch>0
                 AND capacity_retired_at IS NULL AND runtime_capabilities IS NOT NULL
                 AND COALESCE(last_seen_at>clock_timestamp()-interval '30 seconds',false)",
            )
            .bind(host_pool)
            .fetch_all(&mut *tx)
            .await?;
            let ids: Vec<Uuid> = hosts
                .iter()
                .map(|h| h.try_get("id"))
                .collect::<Result<_, _>>()?;
            let mut used = held(&mut tx, &ids).await?;
            let per: Vec<(Uuid, i64)> = sqlx::query_as(
                "SELECT host_id,count(*) FROM warm_pool_members WHERE pool_id=$1
                 AND state IN ('preparing','ready','claiming','retiring') GROUP BY host_id",
            )
            .bind(policy)
            .fetch_all(&mut *tx)
            .await?;
            let mut per: std::collections::HashMap<Uuid, i64> = per.into_iter().collect();
            let need = Footprint::of(cpu, memory, disk);
            for _ in 0..want {
                // Spread: most free memory after the reservation, lowest ID on ties.
                let mut best: Option<(i64, Uuid, i64)> = None;
                for host in &hosts {
                    let id: Uuid = host.try_get("id")?;
                    if !fresh.contains(&id)
                        || per.get(&id).copied().unwrap_or(0) >= per_host
                        || crate::host_capabilities::compatible(
                            host,
                            &image,
                            cpu as u64,
                            memory as u64,
                            disk as u64,
                        )
                        .is_err()
                    {
                        continue;
                    }
                    let u = used.get(&id).copied().unwrap_or(Footprint {
                        vcpu: 0,
                        memory_mib: 0,
                        disk_mib: 0,
                    });
                    let cpu_left =
                        i64::from(host.try_get::<i32, _>("cpu_capacity")?) - u.vcpu - need.vcpu;
                    let memory_left = host.try_get::<i64, _>("memory_capacity_mib")?
                        - u.memory_mib
                        - need.memory_mib;
                    let disk_left =
                        host.try_get::<i64, _>("disk_capacity_mib")? - u.disk_mib - need.disk_mib;
                    if cpu_left < 0 || memory_left < 0 || disk_left < 0 {
                        continue;
                    }
                    if best.as_ref().is_none_or(|(m, _, _)| memory_left > *m) {
                        best = Some((memory_left, id, host.try_get("supervisor_epoch")?));
                    }
                }
                let Some((_, host, epoch)) = best else { break };
                let member = Uuid::now_v7();
                sqlx::query(
                    "INSERT INTO warm_pool_members(id,pool_id,host_id,supervisor_epoch,template_id,
                     vcpu,memory_mib,disk_mib,state) VALUES($1,$2,$3,$4,$5,$6,$7,$8,'preparing')",
                )
                .bind(member)
                .bind(policy)
                .bind(host)
                .bind(epoch)
                .bind(template)
                .bind(cpu as i32)
                .bind(memory)
                .bind(disk)
                .execute(&mut *tx)
                .await?;
                out.prepare
                    .push(enqueue(&mut tx, host_pool, policy, "warm_prepare", member).await?);
                let entry = used.entry(host).or_insert(Footprint {
                    vcpu: 0,
                    memory_mib: 0,
                    disk_mib: 0,
                });
                entry.vcpu += need.vcpu;
                entry.memory_mib += need.memory_mib;
                entry.disk_mib += need.disk_mib;
                *per.entry(host).or_default() += 1;
            }
        }
        tx.commit().await?;
        Ok(out)
    }

    /// Claim the next warm intent of a policy, fairly and at most
    /// [`MAX_ATTEMPTS`] times. The returned work is rebuilt from durable rows.
    pub async fn claim_warm_operation(
        &self,
        policy: Uuid,
        seconds: u32,
    ) -> Result<Option<WarmClaim>, Error> {
        if !(1..=300).contains(&seconds) {
            return Err(Error::Invalid);
        }
        let row = sqlx::query(
            "UPDATE capacity_operations SET status='running',claim_revision=claim_revision+1,
             lease_expires_at=clock_timestamp()+make_interval(secs=>$2),
             available_at=clock_timestamp()+make_interval(secs=>$2),updated_at=clock_timestamp()
             WHERE id=(SELECT id FROM capacity_operations WHERE warm_pool_id=$1 AND status IN ('queued','running')
                AND claim_revision<$3 AND available_at<=clock_timestamp()
                AND (lease_expires_at IS NULL OR lease_expires_at<=clock_timestamp())
                ORDER BY available_at,created_at,id FOR UPDATE SKIP LOCKED LIMIT 1)
             RETURNING id,claim_revision,kind,target_ref",
        )
        .bind(policy)
        .bind(f64::from(seconds))
        .bind(MAX_ATTEMPTS)
        .fetch_optional(self.pool())
        .await?;
        let Some(row) = row else { return Ok(None) };
        let id: Uuid = row.try_get("id")?;
        let revision: i64 = row.try_get("claim_revision")?;
        let kind: String = row.try_get("kind")?;
        let member: Uuid = row
            .try_get::<String, _>("target_ref")?
            .parse()
            .map_err(|_| Error::Conflict)?;
        let m = sqlx::query(
            "SELECT m.*,t.image_digest FROM warm_pool_members m
             JOIN sandbox_templates t ON t.id=m.template_id WHERE m.id=$1",
        )
        .bind(member)
        .fetch_one(self.pool())
        .await?;
        let host_id = HostId::from_uuid(m.try_get("host_id")?);
        let supervisor_epoch: i64 = m.try_get("supervisor_epoch")?;
        let work = if kind == "warm_prepare" {
            WarmWork::Prepare(PrepareIntent {
                version: VERSION,
                member_id: member,
                policy_id: policy,
                template_id: m.try_get("template_id")?,
                host_id,
                supervisor_epoch,
                image_digest: m.try_get("image_digest")?,
                vcpu: u32::try_from(m.try_get::<i32, _>("vcpu")?).map_err(|_| Error::Conflict)?,
                memory_mib: u64::try_from(m.try_get::<i64, _>("memory_mib")?)
                    .map_err(|_| Error::Conflict)?,
                disk_mib: u64::try_from(m.try_get::<i64, _>("disk_mib")?)
                    .map_err(|_| Error::Conflict)?,
                operation_id: id,
                operation_revision: revision,
            })
        } else {
            WarmWork::Retire {
                member_id: member,
                vm_ref: m.try_get("vm_ref")?,
                host_id,
                supervisor_epoch,
            }
        };
        Ok(Some(WarmClaim {
            id,
            policy,
            revision,
            work,
        }))
    }

    /// Requeue a live claim behind waiting work. Preserves intent identity.
    pub async fn retry_warm_operation(&self, claim: &WarmClaim) -> Result<(), Error> {
        let n = sqlx::query(
            "UPDATE capacity_operations SET status='queued',lease_expires_at=NULL,available_at=clock_timestamp(),
             error='retry',updated_at=clock_timestamp() WHERE id=$1 AND warm_pool_id=$2 AND claim_revision=$3
             AND status='running' AND lease_expires_at>clock_timestamp()",
        )
        .bind(claim.id)
        .bind(claim.policy)
        .bind(claim.revision)
        .execute(self.pool())
        .await?
        .rows_affected();
        if n != 1 {
            return Err(Error::LostClaim);
        }
        Ok(())
    }

    /// Lock a fenced, live warm intent row.
    async fn fenced(
        tx: &mut sqlx::PgConnection,
        claim: &WarmClaim,
        kind: &str,
    ) -> Result<Uuid, Error> {
        sqlx::query_scalar(
            "SELECT host_pool_id FROM capacity_operations WHERE id=$1 AND warm_pool_id=$2 AND claim_revision=$3
             AND kind=$4 AND status='running' AND lease_expires_at>clock_timestamp() FOR UPDATE",
        )
        .bind(claim.id)
        .bind(claim.policy)
        .bind(claim.revision)
        .bind(kind)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or(Error::LostClaim)
    }

    async fn succeed(
        tx: &mut sqlx::PgConnection,
        claim: &WarmClaim,
        receipt: &Value,
    ) -> Result<(), Error> {
        let n = sqlx::query(
            "UPDATE capacity_operations SET status='succeeded',lease_expires_at=NULL,provider_receipt=$4,
             error=NULL,updated_at=clock_timestamp() WHERE id=$1 AND warm_pool_id=$2 AND claim_revision=$3
             AND lease_expires_at>clock_timestamp()",
        )
        .bind(claim.id)
        .bind(claim.policy)
        .bind(claim.revision)
        .bind(receipt)
        .execute(&mut *tx)
        .await?
        .rows_affected();
        if n != 1 {
            return Err(Error::LostClaim);
        }
        Ok(())
    }

    /// Persist exact preparation evidence. A member becomes `ready` only if its
    /// host is still at the member's epoch and admits work; otherwise the
    /// reported VM is retained under `retiring` and queued for cleanup.
    pub async fn record_warm_prepared(
        &self,
        claim: &WarmClaim,
        receipt: &PrepareReceipt,
    ) -> Result<(), Error> {
        let WarmWork::Prepare(intent) = &claim.work else {
            return Err(Error::Invalid);
        };
        receipt.verify(intent)?;
        let mut tx = self.pool().begin().await?;
        timeouts(&mut tx).await?;
        let host_pool = Self::fenced(&mut tx, claim, "warm_prepare").await?;
        let (state, ttl, epoch_ok): (String, i32, bool) = sqlx::query_as(
            "SELECT m.state,w.idle_ttl_seconds,h.supervisor_epoch=m.supervisor_epoch AND h.status='ready'
             AND h.capacity_retired_at IS NULL AND w.enabled
             FROM warm_pool_members m JOIN warm_pool_policies w ON w.id=m.pool_id
             JOIN hosts h ON h.id=m.host_id WHERE m.id=$1 AND m.pool_id=$2 FOR UPDATE OF m",
        )
        .bind(intent.member_id)
        .bind(claim.policy)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or(Error::Conflict)?;
        if state != "preparing" {
            return Err(Error::Conflict);
        }
        let evidence = serde_json::to_value(receipt).map_err(|_| Error::Invalid)?;
        sqlx::query(
            "UPDATE warm_pool_members SET vm_ref=$2,prepare_receipt=$3,state='ready',
             lease_expires_at=clock_timestamp()+make_interval(secs=>$4),updated_at=clock_timestamp()
             WHERE id=$1",
        )
        .bind(intent.member_id)
        .bind(&receipt.vm_ref)
        .bind(&evidence)
        .bind(f64::from(ttl))
        .execute(&mut *tx)
        .await?;
        if !epoch_ok {
            begin_retire(
                &mut tx,
                host_pool,
                claim.policy,
                intent.member_id,
                "stale_epoch",
            )
            .await?;
        }
        Self::succeed(&mut tx, claim, &json!({"version":1,"prepared":evidence})).await?;
        tx.commit().await?;
        Ok(())
    }

    /// Give up on an exhausted or permanently failed preparation. Whether a VM
    /// booted is unknown, so the member is never offered: it is retained as
    /// `retiring` (reservation held) until cleanup evidence arrives.
    pub async fn abandon_warm_prepare(&self, claim: &WarmClaim) -> Result<Uuid, Error> {
        let WarmWork::Prepare(intent) = &claim.work else {
            return Err(Error::Invalid);
        };
        let mut tx = self.pool().begin().await?;
        timeouts(&mut tx).await?;
        let host_pool = Self::fenced(&mut tx, claim, "warm_prepare").await?;
        let state: String = sqlx::query_scalar(
            "SELECT state FROM warm_pool_members WHERE id=$1 AND pool_id=$2 FOR UPDATE",
        )
        .bind(intent.member_id)
        .bind(claim.policy)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or(Error::Conflict)?;
        if state != "preparing" {
            return Err(Error::Conflict);
        }
        let retire = begin_retire(
            &mut tx,
            host_pool,
            claim.policy,
            intent.member_id,
            "prepare_abandoned",
        )
        .await?;
        Self::succeed(
            &mut tx,
            claim,
            &json!({"version":1,"outcome":"abandoned_to_retirement","retire_operation":retire}),
        )
        .await?;
        tx.commit().await?;
        Ok(retire)
    }

    /// Persist cleanup evidence for a retiring member, releasing its
    /// reservation. If the member was customer-claimed and its handoff was
    /// rejected, the waiting create may now cold-boot its allocation.
    pub async fn record_warm_cleanup(
        &self,
        claim: &WarmClaim,
        receipt: &CleanupReceipt,
    ) -> Result<(), Error> {
        self.record_warm_cleanup_inner(claim, receipt, None).await
    }

    /// Accept previous-epoch evidence only for the exact live retire claim and
    /// the still-current reporting incarnation. Host routing/authentication is
    /// performed by the controller; a stale heartbeat is never release proof.
    pub async fn record_previous_warm_cleanup(
        &self,
        claim: &WarmClaim,
        receipt: &sandbox_protocol::warm_pool::PreviousCleanupReceipt,
    ) -> Result<(), Error> {
        use sandbox_protocol::warm_pool::{PreviousRetireIntent, RetireIntent};
        let WarmWork::Retire {
            member_id,
            vm_ref,
            host_id,
            supervisor_epoch,
        } = &claim.work
        else {
            return Err(Error::Invalid);
        };
        let expected = PreviousRetireIntent {
            version: VERSION,
            original: RetireIntent {
                version: VERSION,
                member_id: *member_id,
                vm_ref: vm_ref.clone(),
                host_id: *host_id,
                supervisor_epoch: *supervisor_epoch,
                operation_id: claim.id,
                operation_revision: claim.revision,
            },
            reporting_epoch: receipt.request.reporting_epoch,
        };
        receipt.verify(&expected)?;
        self.record_warm_cleanup_inner(claim, &receipt.cleanup, Some(receipt))
            .await
    }

    async fn record_warm_cleanup_inner(
        &self,
        claim: &WarmClaim,
        receipt: &CleanupReceipt,
        previous: Option<&sandbox_protocol::warm_pool::PreviousCleanupReceipt>,
    ) -> Result<(), Error> {
        let WarmWork::Retire {
            member_id,
            vm_ref,
            host_id,
            supervisor_epoch,
        } = &claim.work
        else {
            return Err(Error::Invalid);
        };
        receipt.verify(*member_id, vm_ref.as_deref(), *host_id, *supervisor_epoch)?;
        let mut tx = self.pool().begin().await?;
        timeouts(&mut tx).await?;
        if let Some(previous) = previous {
            let current: i64 =
                sqlx::query_scalar("SELECT supervisor_epoch FROM hosts WHERE id=$1 FOR UPDATE")
                    .bind(host_id.uuid())
                    .fetch_one(&mut *tx)
                    .await?;
            if current != previous.request.reporting_epoch {
                return Err(Error::Conflict);
            }
        }
        Self::fenced(&mut tx, claim, "warm_retire").await?;
        // Unlocked read to find the create operation, then lock it before the member.
        let allocation: Option<Uuid> =
            sqlx::query_scalar("SELECT claimed_allocation_id FROM warm_pool_members WHERE id=$1")
                .bind(member_id)
                .fetch_one(&mut *tx)
                .await?;
        let operation: Option<Uuid> = match allocation {
            Some(a) => sqlx::query_scalar(
                "SELECT o.id FROM allocation_permits p JOIN operations o ON o.id=p.create_operation_id
                 WHERE p.allocation_id=$1 FOR UPDATE OF o",
            )
            .bind(a)
            .fetch_optional(&mut *tx)
            .await?,
            None => None,
        };
        let row = sqlx::query(
            "SELECT state,vm_ref,host_id,supervisor_epoch FROM warm_pool_members
             WHERE id=$1 AND pool_id=$2 FOR UPDATE",
        )
        .bind(member_id)
        .bind(claim.policy)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or(Error::Conflict)?;
        if row.try_get::<String, _>("state")? != "retiring"
            || row.try_get::<Option<String>, _>("vm_ref")? != *vm_ref
            || row.try_get::<Uuid, _>("host_id")? != host_id.uuid()
            || row.try_get::<i64, _>("supervisor_epoch")? != *supervisor_epoch
        {
            return Err(Error::Conflict);
        }
        let evidence = match previous {
            Some(previous) => serde_json::to_value(previous),
            None => serde_json::to_value(receipt),
        }
        .map_err(|_| Error::Invalid)?;
        // An unnamed retire records the VM name the supervisor resolved.
        sqlx::query(
            "UPDATE warm_pool_members SET state='retired',cleanup_evidence=$2,
             vm_ref=COALESCE(vm_ref,$3),updated_at=clock_timestamp() WHERE id=$1",
        )
        .bind(member_id)
        .bind(&evidence)
        .bind(&receipt.vm_ref)
        .execute(&mut *tx)
        .await?;
        if let (None, Some(op), Some(allocation)) = (previous, operation, allocation) {
            let returned = sqlx::query(
                "UPDATE operations SET phase='reserved',next_retry_at=clock_timestamp(),
                 attempt_receipts=attempt_receipts||jsonb_build_array($2::jsonb),updated_at=clock_timestamp()
                 WHERE id=$1 AND phase='warm_handoff_rejected' AND status='running' AND attempt_count=0",
            )
            .bind(op)
            .bind(json!({"phase":"warm_member_cleaned","member_id":member_id}))
            .execute(&mut *tx)
            .await?
            .rows_affected();
            // The supervisor's Rejected receipt plus this destruction evidence
            // prove no host holds the allocation: it is undispatched again, so
            // the cold path may boot it or reject (and release) it before
            // dispatch exactly as for a fresh reservation.
            if returned == 1 {
                sqlx::query(
                    "UPDATE allocations SET lease_expires_at=NULL,updated_at=clock_timestamp()
                     WHERE id=$1 AND status='reserved' AND released_at IS NULL",
                )
                .bind(allocation)
                .execute(&mut *tx)
                .await?;
            }
        }
        Self::succeed(&mut tx, claim, &json!({"version":1,"cleanup":evidence})).await?;
        tx.commit().await?;
        Ok(())
    }

    /// Atomically claim one ready, compatible, authorized member for a create
    /// operation, or report that none exists. In one transaction: check the
    /// fenced operation, current project credential, quota and host-pool grant;
    /// claim the member once (revision++); create the allocation and permit on
    /// the member's host at the member's epoch with the member's exact size;
    /// persist the handoff intent; and park the create in `warm_handoff` so the
    /// ordinary cold dispatcher cannot boot a second VM for the allocation.
    pub async fn claim_warm_member(
        &self,
        claim: &Claim,
        images: &BTreeSet<String>,
    ) -> Result<Claimed, Error> {
        let mut tx = self.pool().begin().await?;
        timeouts(&mut tx).await?;
        let op = sqlx::query(
            "SELECT * FROM operations WHERE id=$1 AND claim_revision=$2
             AND lease_expires_at > clock_timestamp() AND status IN ('running','unknown') FOR UPDATE",
        )
        .bind(claim.operation_id.uuid())
        .bind(claim.revision)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or(Error::LostClaim)?;
        let project_id: Uuid = op.try_get("project_id")?;
        let sandbox_id: Uuid = op.try_get("sandbox_id")?;
        let project = sqlx::query("SELECT * FROM projects WHERE id=$1 FOR UPDATE")
            .bind(project_id)
            .fetch_one(&mut *tx)
            .await?;
        let sandbox = sqlx::query("SELECT * FROM sandboxes WHERE id=$1 FOR UPDATE")
            .bind(sandbox_id)
            .fetch_one(&mut *tx)
            .await?;
        if op.try_get::<String, _>("kind")? != "create"
            || sandbox.try_get::<Option<Uuid>, _>("active_transition_operation_id")?
                != Some(claim.operation_id.uuid())
        {
            return Err(Error::Reconcile);
        }
        if let Some(current) = sandbox.try_get::<Option<Uuid>, _>("current_allocation_id")? {
            let row = sqlx::query(
                "SELECT handoff_intent FROM warm_pool_members WHERE claimed_allocation_id=$1",
            )
            .bind(current)
            .fetch_optional(&mut *tx)
            .await?
            .ok_or(Error::Reconcile)?;
            let existing = handoff_from(&row)?;
            tx.commit().await?;
            return Ok(Claimed::Existing(existing));
        }
        if op.try_get::<String, _>("status")? == "unknown"
            || op.try_get::<i32, _>("attempt_count")? != 0
            || sandbox.try_get::<i64, _>("generation")? != 0
            || sandbox.try_get::<String, _>("observed_state")? != "creating"
            || sandbox.try_get::<String, _>("desired_state")? != "running"
        {
            return Err(Error::Reconcile);
        }
        let resources: Value = sandbox.try_get("resources")?;
        let size = |key: &str| {
            resources
                .get(key)
                .and_then(Value::as_i64)
                .filter(|v| *v > 0)
                .ok_or(Error::Invalid)
        };
        let (cpu, memory, disk) = (size("vcpu")?, size("memory_mib")?, size("disk_mib")?);
        let image: String = sandbox.try_get("image_digest")?;
        // The controller's image allowlist applies to a prepared VM exactly as
        // to a cold boot; the adopted create never reaches the cold check.
        if !images.contains(&image) {
            return Err(Error::ImageDenied);
        }
        let authorized: bool = sqlx::query_scalar(
            "SELECT p.status='active' AND o.initiator_kind='project'
             AND (o.deadline IS NULL OR o.deadline > clock_timestamp())
             AND (s.expires_at IS NULL OR s.expires_at > clock_timestamp())
             AND EXISTS (SELECT 1 FROM jsonb_array_elements(p.api_tokens) t
                WHERE t->>'key_id'=o.initiator_key_id
                AND (t->>'revoked_at' IS NULL OR (t->>'revoked_at')::timestamptz > clock_timestamp())
                AND (t->>'expires_at' IS NULL OR (t->>'expires_at')::timestamptz > clock_timestamp()))
             FROM projects p JOIN operations o ON o.project_id=p.id
             JOIN sandboxes s ON s.id=o.sandbox_id WHERE o.id=$1",
        )
        .bind(claim.operation_id.uuid())
        .fetch_one(&mut *tx)
        .await?;
        if !authorized {
            return Err(Error::Unauthorized);
        }
        // Quota applies to an already-prepared VM exactly as to a cold boot.
        let limits: Value = project.try_get("limits")?;
        let limit = |key: &str, default: i64| match limits.get(key) {
            None => Ok(default),
            Some(v) => v.as_i64().filter(|v| *v >= 0).ok_or(Error::Invalid),
        };
        let usage: (i64, i64, i64, i64) = sqlx::query_as(
            "SELECT count(*),COALESCE(sum(vcpu),0)::bigint,COALESCE(sum(memory_mib),0)::bigint,
             COALESCE(sum(disk_mib),0)::bigint FROM allocations WHERE project_id=$1 AND released_at IS NULL",
        )
        .bind(project_id)
        .fetch_one(&mut *tx)
        .await?;
        if usage.0 >= limit("sandboxes", 25)?
            || cpu > limit("vcpu", 100)?.saturating_sub(usage.1)
            || memory > limit("memory_mib", 204800)?.saturating_sub(usage.2)
            || disk > limit("disk_mib", 1638400)?.saturating_sub(usage.3)
        {
            return Err(Error::Quota);
        }
        // Candidate policies: granted host pool, dedicated-to-this-project or
        // shared, template authorized and exactly matching image and size.
        let policies: Vec<Uuid> = sqlx::query_scalar(
            "SELECT w.id FROM warm_pool_policies w JOIN sandbox_templates t ON t.id=w.template_id
             JOIN project_host_pool_access g ON g.host_pool_id=w.host_pool_id AND g.project_id=$1
             WHERE w.enabled AND (w.owner_project_id IS NULL OR w.owner_project_id=$1)
             AND (t.owner_project_id=$1 OR t.publication='shared') AND t.snapshot_id IS NULL
             AND t.image_digest=$2 AND t.vcpu=$3 AND t.memory_mib=$4 AND t.disk_mib=$5
             ORDER BY w.id FOR UPDATE OF w",
        )
        .bind(project_id)
        .bind(&image)
        .bind(cpu as i32)
        .bind(memory)
        .bind(disk)
        .fetch_all(&mut *tx)
        .await?;
        if policies.is_empty() {
            tx.commit().await?;
            return Ok(Claimed::None);
        }
        let hosts = sqlx::query(
            "SELECT * FROM hosts WHERE id IN (SELECT host_id FROM warm_pool_members
             WHERE pool_id=ANY($1) AND state='ready') ORDER BY id FOR UPDATE",
        )
        .bind(&policies)
        .fetch_all(&mut *tx)
        .await?;
        let mut eligible = Vec::new();
        for host in &hosts {
            if host
                .try_get::<Option<Value>, _>("runtime_capabilities")?
                .is_some()
                && crate::host_capabilities::compatible(
                    host,
                    &image,
                    cpu as u64,
                    memory as u64,
                    disk as u64,
                )
                .is_ok()
            {
                eligible.push(host.try_get::<Uuid, _>("id")?);
            }
        }
        // Time and epoch predicates after every lock. SKIP LOCKED is not used:
        // policy locks already serialize claimers of the same members.
        let member = sqlx::query(
            "SELECT m.id,m.host_id,m.supervisor_epoch,m.vm_ref,m.claim_revision,m.template_id FROM warm_pool_members m
             JOIN hosts h ON h.id=m.host_id
             WHERE m.pool_id=ANY($1) AND m.state='ready' AND m.lease_expires_at>clock_timestamp()
             AND m.host_id=ANY($2) AND h.status='ready' AND h.supervisor_epoch=m.supervisor_epoch
             AND h.capacity_retired_at IS NULL
             AND COALESCE(h.last_seen_at>clock_timestamp()-interval '30 seconds',false)
             AND m.vcpu=$3 AND m.memory_mib=$4 AND m.disk_mib=$5
             ORDER BY m.created_at,m.id LIMIT 1 FOR UPDATE OF m",
        )
        .bind(&policies)
        .bind(&eligible)
        .bind(cpu as i32)
        .bind(memory)
        .bind(disk)
        .fetch_optional(&mut *tx)
        .await?;
        let Some(member) = member else {
            tx.commit().await?;
            return Ok(Claimed::None);
        };
        let member_id: Uuid = member.try_get("id")?;
        let host_id = HostId::from_uuid(member.try_get("host_id")?);
        let epoch: i64 = member.try_get("supervisor_epoch")?;
        let vm_ref: String = member
            .try_get::<Option<String>, _>("vm_ref")?
            .ok_or(Error::Conflict)?;
        let revision = member.try_get::<i64, _>("claim_revision")? + 1;
        let allocation = Allocation {
            id: AllocationId::generate(),
            host_id,
            generation: 1,
            supervisor_epoch: epoch,
        };
        sqlx::query(
            "INSERT INTO allocations(id,project_id,sandbox_id,host_id,generation,supervisor_epoch,
             vcpu,memory_mib,disk_mib,status) VALUES($1,$2,$3,$4,1,$5,$6,$7,$8,'reserved')",
        )
        .bind(allocation.id.uuid())
        .bind(project_id)
        .bind(sandbox_id)
        .bind(host_id.uuid())
        .bind(epoch)
        .bind(cpu as i32)
        .bind(memory)
        .bind(disk)
        .execute(&mut *tx)
        .await?;
        // Same issuance as cold placement: the host row is locked above. The
        // permit names the allocation; it is not adoption authority.
        let serial: i64 = sqlx::query_scalar(
            "UPDATE hosts SET last_allocation_serial=last_allocation_serial+1
             WHERE id=$1 AND last_allocation_serial<9223372036854775807 RETURNING last_allocation_serial",
        )
        .bind(host_id.uuid())
        .fetch_optional(&mut *tx)
        .await?
        .ok_or(Error::Conflict)?;
        sqlx::query(
            "INSERT INTO allocation_permits(allocation_id,host_id,serial,project_id,
             sandbox_id,create_operation_id,generation,original_epoch) VALUES($1,$2,$3,$4,$5,$6,1,$7)",
        )
        .bind(allocation.id.uuid())
        .bind(host_id.uuid())
        .bind(serial)
        .bind(project_id)
        .bind(sandbox_id)
        .bind(claim.operation_id.uuid())
        .bind(epoch)
        .execute(&mut *tx)
        .await?;
        let intent = HandoffIntent {
            version: VERSION,
            member_id,
            vm_ref,
            template_id: member.try_get("template_id")?,
            allocation_id: allocation.id,
            project_id: ProjectId::from_uuid(project_id),
            sandbox_id: SandboxId::from_uuid(sandbox_id),
            create_operation_id: claim.operation_id,
            generation: 1,
            permit_serial: u64::try_from(serial).map_err(|_| Error::Conflict)?,
            host_id,
            supervisor_epoch: epoch,
            claim_revision: revision,
        };
        intent.validate()?;
        let intent_json = serde_json::to_value(&intent).map_err(|_| Error::Invalid)?;
        // Ready -> claiming and allocation insert commit together: the member's
        // reservation moves to the allocation with no gap and no double count.
        sqlx::query(
            "UPDATE warm_pool_members SET state='claiming',claim_revision=$2,claimed_allocation_id=$3,
             handoff_intent=$4,lease_expires_at=NULL,updated_at=clock_timestamp()
             WHERE id=$1 AND state='ready' AND claim_revision=$2-1",
        )
        .bind(member_id)
        .bind(revision)
        .bind(allocation.id.uuid())
        .bind(&intent_json)
        .execute(&mut *tx)
        .await?;
        sqlx::query("UPDATE sandboxes SET current_allocation_id=$1,generation=1,state_revision=state_revision+1,updated_at=clock_timestamp() WHERE id=$2")
            .bind(allocation.id.uuid()).bind(sandbox_id).execute(&mut *tx).await?;
        let changed = sqlx::query(
            "UPDATE operations SET phase='warm_handoff',error=NULL,updated_at=clock_timestamp(),
             attempt_receipts=attempt_receipts || jsonb_build_array($3::jsonb)
             WHERE id=$1 AND claim_revision=$2 AND lease_expires_at > clock_timestamp()",
        )
        .bind(claim.operation_id.uuid())
        .bind(claim.revision)
        .bind(json!({"phase":"warm_claimed","intent":intent_json}))
        .execute(&mut *tx)
        .await?
        .rows_affected();
        if changed != 1 {
            return Err(Error::LostClaim);
        }
        tx.commit().await?;
        Ok(Claimed::Claimed(WarmHandoff { allocation, intent }))
    }

    /// Persist a supervisor handoff receipt for the claimed member. The receipt
    /// must echo the durable intent exactly and the host must still be at the
    /// intent's epoch; otherwise nothing changes and the claim stays uncertain
    /// (reservation held by the allocation, member never offered again).
    ///
    /// Adopted: the member is `assigned` and the create proceeds as a
    /// dispatched create, so readiness is still established by inspecting the
    /// allocation. Rejected: the member retires; the create waits in
    /// `warm_handoff_rejected` until cleanup evidence lets it cold-boot.
    /// Exact retries of an already recorded receipt succeed without change.
    pub async fn record_warm_handoff(
        &self,
        claim: &Claim,
        receipt: &HandoffReceipt,
    ) -> Result<(), Error> {
        let mut tx = self.pool().begin().await?;
        timeouts(&mut tx).await?;
        let fenced: Option<Uuid> = sqlx::query_scalar(
            "SELECT id FROM operations WHERE id=$1 AND claim_revision=$2 AND kind='create'
             AND lease_expires_at>clock_timestamp() AND status IN ('running','unknown') FOR UPDATE",
        )
        .bind(claim.operation_id.uuid())
        .bind(claim.revision)
        .fetch_optional(&mut *tx)
        .await?;
        fenced.ok_or(Error::LostClaim)?;
        // Host before member, matching claim and reconcile lock order.
        let current_epoch: Option<i64> = sqlx::query_scalar(
            "SELECT h.supervisor_epoch FROM hosts h JOIN warm_pool_members m ON m.host_id=h.id
             WHERE m.id=$1 FOR SHARE OF h",
        )
        .bind(receipt.member_id)
        .fetch_optional(&mut *tx)
        .await?;
        let current_epoch = current_epoch.ok_or(Error::Conflict)?;
        let row = sqlx::query(
            "SELECT m.*,w.host_pool_id FROM warm_pool_members m JOIN warm_pool_policies w ON w.id=m.pool_id
             JOIN allocation_permits p ON p.allocation_id=m.claimed_allocation_id
             WHERE m.id=$1 AND p.create_operation_id=$2 FOR UPDATE OF m",
        )
        .bind(receipt.member_id)
        .bind(claim.operation_id.uuid())
        .fetch_optional(&mut *tx)
        .await?
        .ok_or(Error::Conflict)?;
        let intent: HandoffIntent =
            serde_json::from_value(row.try_get("handoff_intent")?).map_err(|_| Error::Conflict)?;
        receipt.verify(&intent)?;
        let evidence = serde_json::to_value(receipt).map_err(|_| Error::Invalid)?;
        let state: String = row.try_get("state")?;
        if state != "claiming" {
            let recorded: Option<Value> = row.try_get("handoff_receipt")?;
            return if recorded.as_ref() == Some(&evidence) {
                Ok(())
            } else {
                Err(Error::Conflict)
            };
        }
        if current_epoch != intent.supervisor_epoch {
            return Err(Error::StaleEpoch);
        }
        match &receipt.outcome {
            HandoffOutcome::Adopted { .. } => {
                sqlx::query(
                    "UPDATE warm_pool_members SET state='assigned',handoff_receipt=$2,updated_at=clock_timestamp()
                     WHERE id=$1",
                )
                .bind(receipt.member_id)
                .bind(&evidence)
                .execute(&mut *tx)
                .await?;
                sqlx::query(
                    "UPDATE allocations SET lease_expires_at=least(clock_timestamp()+interval '60 seconds',
                     (SELECT expires_at FROM sandboxes WHERE id=allocations.sandbox_id)),updated_at=clock_timestamp()
                     WHERE id=$1",
                )
                .bind(intent.allocation_id.uuid())
                .execute(&mut *tx)
                .await?;
                // Equivalent to a dispatched create: later passes inspect the
                // allocation; no cold Create can be issued for it.
                sqlx::query(
                    "UPDATE operations SET phase='create_dispatched',attempt_count=attempt_count+1,
                     attempt_receipts=attempt_receipts||jsonb_build_array($2::jsonb),updated_at=clock_timestamp()
                     WHERE id=$1",
                )
                .bind(claim.operation_id.uuid())
                .bind(json!({"phase":"warm_adopted","receipt":evidence}))
                .execute(&mut *tx)
                .await?;
            }
            HandoffOutcome::Rejected { .. } => {
                sqlx::query(
                    "UPDATE warm_pool_members SET handoff_receipt=$2,updated_at=clock_timestamp() WHERE id=$1",
                )
                .bind(receipt.member_id)
                .bind(&evidence)
                .execute(&mut *tx)
                .await?;
                begin_retire(
                    &mut tx,
                    row.try_get("host_pool_id")?,
                    row.try_get("pool_id")?,
                    receipt.member_id,
                    "handoff_rejected",
                )
                .await?;
                sqlx::query(
                    "UPDATE operations SET phase='warm_handoff_rejected',
                     attempt_receipts=attempt_receipts||jsonb_build_array($2::jsonb),updated_at=clock_timestamp()
                     WHERE id=$1",
                )
                .bind(claim.operation_id.uuid())
                .bind(json!({"phase":"warm_rejected","receipt":evidence}))
                .execute(&mut *tx)
                .await?;
            }
        }
        tx.commit().await?;
        Ok(())
    }

    /// Durably mark the handoff dispatched before the first AdoptWarmMember and
    /// set the allocation's initial lease. Returns [`AdoptMode::Inspect`] when
    /// an adopt was already dispatched (lost acknowledgement or restart) or the
    /// create's credential, deadline or sandbox expiry no longer authorizes
    /// binding project identity: those paths may only inspect and fence.
    pub async fn begin_warm_adopt(
        &self,
        claim: &Claim,
        images: &BTreeSet<String>,
    ) -> Result<AdoptDispatch, Error> {
        let mut tx = self.pool().begin().await?;
        timeouts(&mut tx).await?;
        let op = sqlx::query(
            "SELECT phase,sandbox_id FROM operations WHERE id=$1 AND claim_revision=$2 AND kind='create'
             AND lease_expires_at>clock_timestamp() AND status IN ('running','unknown') FOR UPDATE",
        )
        .bind(claim.operation_id.uuid())
        .bind(claim.revision)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or(Error::LostClaim)?;
        let phase: Option<String> = op.try_get("phase")?;
        let sandbox: Uuid = op.try_get("sandbox_id")?;
        let row = sqlx::query(
            "SELECT m.handoff_intent,m.state FROM warm_pool_members m JOIN sandboxes s
             ON s.current_allocation_id=m.claimed_allocation_id
             JOIN allocation_permits p ON p.allocation_id=m.claimed_allocation_id
             WHERE s.id=$1 AND p.create_operation_id=$2",
        )
        .bind(sandbox)
        .bind(claim.operation_id.uuid())
        .fetch_optional(&mut *tx)
        .await?
        .ok_or(Error::Reconcile)?;
        if row.try_get::<String, _>("state")? != "claiming" {
            return Err(Error::Conflict);
        }
        let handoff = handoff_from(&row)?;
        let (authorized, image): (bool, String) = sqlx::query_as(
            "SELECT p.status='active' AND o.initiator_kind='project'
             AND (o.deadline IS NULL OR o.deadline > clock_timestamp())
             AND (s.expires_at IS NULL OR s.expires_at > clock_timestamp())
             AND EXISTS (SELECT 1 FROM jsonb_array_elements(p.api_tokens) t
                WHERE t->>'key_id'=o.initiator_key_id
                AND (t->>'revoked_at' IS NULL OR (t->>'revoked_at')::timestamptz > clock_timestamp())
                AND (t->>'expires_at' IS NULL OR (t->>'expires_at')::timestamptz > clock_timestamp())),
             s.image_digest
             FROM projects p JOIN operations o ON o.project_id=p.id
             JOIN sandboxes s ON s.id=o.sandbox_id WHERE o.id=$1",
        )
        .bind(claim.operation_id.uuid())
        .fetch_one(&mut *tx)
        .await?;
        // An image removed from the allowlist after the claim may still be
        // fenced by inspection, never adopted.
        let authorized = authorized && images.contains(&image);
        let mode = match phase.as_deref() {
            Some("warm_handoff") if authorized => AdoptMode::Adopt,
            Some("warm_handoff" | "warm_adopt_dispatched") => AdoptMode::Inspect,
            _ => return Err(Error::Reconcile),
        };
        let lease: time::OffsetDateTime = sqlx::query_scalar(
            "UPDATE allocations SET lease_expires_at=least(clock_timestamp()+interval '60 seconds',
             (SELECT expires_at FROM sandboxes WHERE id=allocations.sandbox_id)),updated_at=clock_timestamp()
             WHERE id=$1 AND released_at IS NULL RETURNING lease_expires_at",
        )
        .bind(handoff.allocation.id.uuid())
        .fetch_optional(&mut *tx)
        .await?
        .ok_or(Error::Conflict)?;
        if mode == AdoptMode::Adopt {
            sqlx::query(
                "UPDATE operations SET phase='warm_adopt_dispatched',updated_at=clock_timestamp(),
                 attempt_receipts=attempt_receipts||jsonb_build_array($2::jsonb) WHERE id=$1",
            )
            .bind(claim.operation_id.uuid())
            .bind(json!({"phase":"warm_adopt_dispatch_intent","intent_sha256":handoff.intent.digest()?}))
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;
        let millis =
            i64::try_from(lease.unix_timestamp_nanos() / 1_000_000).map_err(|_| Error::Conflict)?;
        Ok(AdoptDispatch {
            handoff,
            mode,
            allocation_expires_unix_ms: millis,
        })
    }

    /// Policies the upkeep worker visits, in ID order, at most `limit`.
    pub async fn warm_pool_policy_ids(&self, limit: u32) -> Result<Vec<Uuid>, Error> {
        if !(1..=256).contains(&limit) {
            return Err(Error::Invalid);
        }
        Ok(
            sqlx::query_scalar("SELECT id FROM warm_pool_policies ORDER BY id LIMIT $1")
                .bind(i64::from(limit))
                .fetch_all(self.pool())
                .await?,
        )
    }

    /// Durable handoff intent for a create, for restart/lost-ack reconciliation.
    pub async fn warm_handoff(&self, sandbox: SandboxId) -> Result<Option<WarmHandoff>, Error> {
        let row = sqlx::query(
            "SELECT m.handoff_intent FROM warm_pool_members m JOIN sandboxes s
             ON s.current_allocation_id=m.claimed_allocation_id WHERE s.id=$1",
        )
        .bind(sandbox.uuid())
        .fetch_optional(self.pool())
        .await?;
        row.as_ref().map(handoff_from).transpose()
    }

    /// Members of a policy as JSON for operators and tests.
    pub async fn warm_pool_members(&self, policy: Uuid) -> Result<Vec<Value>, Error> {
        Ok(sqlx::query_scalar(
            "SELECT to_jsonb(m) FROM warm_pool_members m WHERE pool_id=$1 ORDER BY created_at,id",
        )
        .bind(policy)
        .fetch_all(self.pool())
        .await?)
    }
}
