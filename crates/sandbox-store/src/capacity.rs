//! Durable infrastructure intent. Static effects and receipts commit together;
//! claims authorize no sandbox mutation or external provisioning.
use crate::{
    Store,
    host_maintenance::{self, HostProvision},
};
use sandbox_protocol::{HostId, Id};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::Row;
use uuid::Uuid;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("invalid capacity input or exceeded capacity bound")]
    Invalid,
    #[error("capacity request conflicts with retained intent or host state")]
    Conflict,
    #[error("capacity claim expired or was replaced")]
    LostClaim,
    #[error("capacity storage query failed")]
    Query(#[from] sqlx::Error),
    #[error(transparent)]
    Maintenance(#[from] host_maintenance::Error),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum Action {
    /// Enroll an existing operator-supplied machine; never create infrastructure.
    Provision {
        config: HostProvision,
    },
    Status,
    Drain,
    Retire,
}
impl Action {
    pub fn kind(&self) -> &'static str {
        match self {
            Self::Provision { .. } => "provision",
            Self::Status => "status",
            Self::Drain => "drain",
            Self::Retire => "retire",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Request {
    pub host: HostId,
    pub epoch: i64,
    pub action: Action,
}
#[derive(Debug, Clone)]
pub struct Claim {
    pub id: Uuid,
    pub pool: Uuid,
    pub revision: i64,
    pub request: Request,
}

pub const MAX_PENDING: i64 = 1000;
impl Store {
    pub async fn capacity_operation(&self, id: Uuid) -> Result<Option<Value>, Error> {
        Ok(
            sqlx::query_scalar("SELECT to_jsonb(c) FROM capacity_operations c WHERE id=$1")
                .bind(id)
                .fetch_optional(self.pool())
                .await?,
        )
    }

    /// Operator-only enqueue. Exact retries return the original ID, including
    /// after completion; changed inputs never replace an earlier intent.
    pub async fn enqueue_capacity(
        &self,
        pool: Uuid,
        key: &str,
        request: &Request,
    ) -> Result<Uuid, Error> {
        if request.epoch <= 0
            || key.is_empty()
            || key.len() > 128
            || !key.bytes().all(|b| b.is_ascii_graphic())
        {
            return Err(Error::Invalid);
        }
        if let Action::Provision { config } = &request.action {
            if config.host_pool_id != Some(pool) {
                return Err(Error::Invalid);
            }
            HostProvision::from_json(&serde_json::to_vec(config).map_err(|_| Error::Invalid)?)?;
        }
        let value = serde_json::to_value(request).map_err(|_| Error::Invalid)?;
        let mut tx = self.pool().begin().await?;
        let _kind: String =
            sqlx::query_scalar("SELECT type FROM host_pools WHERE id=$1 FOR UPDATE")
                .bind(pool)
                .fetch_optional(&mut *tx)
                .await?
                .ok_or(Error::Conflict)?;
        if let Some(row) = sqlx::query("SELECT id,request FROM capacity_operations WHERE host_pool_id=$1 AND idempotency_key=$2")
            .bind(pool).bind(key).fetch_optional(&mut *tx).await? {
            if row.try_get::<Value,_>("request")? != value { return Err(Error::Conflict); }
            let id = row.try_get("id")?; tx.commit().await?; return Ok(id);
        }
        let count: i64 = sqlx::query_scalar("SELECT count(*) FROM capacity_operations WHERE host_pool_id=$1 AND status<>'succeeded'")
            .bind(pool).fetch_one(&mut *tx).await?;
        if count >= MAX_PENDING {
            return Err(Error::Invalid);
        }
        let id = Uuid::now_v7();
        sqlx::query("INSERT INTO capacity_operations(id,host_pool_id,kind,target_ref,idempotency_key,request) VALUES($1,$2,$3,$4,$5,$6)")
            .bind(id).bind(pool).bind(request.action.kind()).bind(request.host.to_string()).bind(key).bind(value).execute(&mut *tx).await?;
        tx.commit().await?;
        Ok(id)
    }

    pub async fn claim_capacity(&self, pool: Uuid, seconds: u32) -> Result<Option<Claim>, Error> {
        if !(1..=300).contains(&seconds) {
            return Err(Error::Invalid);
        }
        let row = sqlx::query("UPDATE capacity_operations SET status='running',claim_revision=claim_revision+1,lease_expires_at=clock_timestamp()+make_interval(secs=>$2),updated_at=clock_timestamp() WHERE id=(SELECT id FROM capacity_operations WHERE host_pool_id=$1 AND status<>'succeeded' AND (lease_expires_at IS NULL OR lease_expires_at<=clock_timestamp()) ORDER BY created_at,id FOR UPDATE SKIP LOCKED LIMIT 1) RETURNING id,claim_revision,request")
            .bind(pool).bind(f64::from(seconds)).fetch_optional(self.pool()).await?;
        row.map(|r| {
            Ok(Claim {
                id: r.try_get("id")?,
                pool,
                revision: r.try_get("claim_revision")?,
                request: serde_json::from_value(r.try_get("request")?)
                    .map_err(|_| Error::Invalid)?,
            })
        })
        .transpose()
    }

    /// A bounded retry preserves intent and receipt identity. No automatic loop.
    pub async fn retry_capacity(&self, claim: &Claim) -> Result<(), Error> {
        let n=sqlx::query("UPDATE capacity_operations SET status='queued',lease_expires_at=NULL,error='operator_retry',updated_at=clock_timestamp() WHERE id=$1 AND host_pool_id=$2 AND claim_revision=$3 AND status='running' AND lease_expires_at>clock_timestamp()")
            .bind(claim.id).bind(claim.pool).bind(claim.revision).execute(self.pool()).await?.rows_affected();
        if n != 1 {
            return Err(Error::LostClaim);
        }
        Ok(())
    }

    /// Re-read durable input; callers cannot alter a claimed target/action.
    /// No external effects occur between checking the fence and its receipt.
    pub async fn apply_static_capacity(&self, claim: &Claim) -> Result<Value, Error> {
        let mut tx = self.pool().begin().await?;
        sqlx::query("SET LOCAL statement_timeout='2s'")
            .execute(&mut *tx)
            .await?;
        sqlx::query("SET LOCAL lock_timeout='1s'")
            .execute(&mut *tx)
            .await?;
        // Same pool-before-host order as enrollment and pool reconfiguration.
        let kind: String = sqlx::query_scalar("SELECT type FROM host_pools WHERE id=$1 FOR UPDATE")
            .bind(claim.pool)
            .fetch_optional(&mut *tx)
            .await?
            .ok_or(Error::Conflict)?;
        if kind != "standalone" {
            return Err(Error::Conflict);
        }
        let value: Value=sqlx::query_scalar("SELECT request FROM capacity_operations WHERE id=$1 AND host_pool_id=$2 AND claim_revision=$3 AND status='running' AND lease_expires_at>clock_timestamp() FOR UPDATE")
            .bind(claim.id).bind(claim.pool).bind(claim.revision).fetch_optional(&mut *tx).await?.ok_or(Error::LostClaim)?;
        let request: Request = serde_json::from_value(value).map_err(|_| Error::Invalid)?;
        let retired: Option<bool>=sqlx::query_scalar("SELECT capacity_retired_at IS NOT NULL FROM hosts WHERE id=$1 AND host_pool_id=$2 FOR UPDATE")
            .bind(request.host.uuid()).bind(claim.pool).fetch_optional(&mut *tx).await?;
        // A retired host is a retained tombstone. Only status and exact retire
        // intents may acknowledge it; health, retries or new provision cannot reopen.
        if retired == Some(true) {
            if !matches!(request.action, Action::Status | Action::Retire) {
                return Err(Error::Conflict);
            }
            let epoch: i64 = sqlx::query_scalar("SELECT supervisor_epoch FROM hosts WHERE id=$1")
                .bind(request.host.uuid())
                .fetch_one(&mut *tx)
                .await?;
            if epoch != request.epoch {
                return Err(Error::Conflict);
            }
        } else if let Action::Provision { config } = &request.action {
            host_maintenance::provision_in(&mut tx, request.host, request.epoch, config).await?;
        } else if retired.is_none() {
            return Err(Error::Conflict);
        }
        let receipt = if retired == Some(true) {
            serde_json::json!({"version":1,"host":request.host,"epoch":request.epoch,"retired":true})
        } else {
            let mut report = host_maintenance::report(&mut tx, request.host, request.epoch).await?;
            let retiring = matches!(request.action, Action::Retire);
            match request.action {
                Action::Drain => {
                    if !matches!(report.status.as_str(), "ready" | "draining") {
                        return Err(Error::Conflict);
                    }
                    sqlx::query("UPDATE hosts SET status='draining',updated_at=clock_timestamp() WHERE id=$1").bind(request.host.uuid()).execute(&mut *tx).await?;
                    report.status = "draining".into();
                }
                Action::Retire => {
                    let fresh: bool=sqlx::query_scalar("SELECT COALESCE(last_seen_at>clock_timestamp()-interval '30 seconds',false) FROM hosts WHERE id=$1")
                        .bind(request.host.uuid()).fetch_one(&mut *tx).await?;
                    if report.status != "draining" || report.outstanding_allocations != 0 || !fresh
                    {
                        return Err(Error::Conflict);
                    }
                    sqlx::query("UPDATE hosts SET capacity_retired_at=clock_timestamp(),updated_at=clock_timestamp() WHERE id=$1").bind(request.host.uuid()).execute(&mut *tx).await?;
                }
                _ => {}
            }
            serde_json::json!({"version":1,"host":request.host,"epoch":request.epoch,"action":request.action.kind(),"retired":retiring,"report":report})
        };
        let n=sqlx::query("UPDATE capacity_operations SET status='succeeded',lease_expires_at=NULL,provider_receipt=$4,error=NULL,updated_at=clock_timestamp() WHERE id=$1 AND host_pool_id=$2 AND claim_revision=$3 AND lease_expires_at>clock_timestamp()")
            .bind(claim.id).bind(claim.pool).bind(claim.revision).bind(&receipt).execute(&mut *tx).await?.rows_affected();
        if n != 1 {
            return Err(Error::LostClaim);
        }
        tx.commit().await?;
        Ok(receipt)
    }
}
