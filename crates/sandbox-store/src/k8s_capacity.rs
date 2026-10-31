//! Frozen worker envelopes and external-effect fences for optional Kubernetes capacity.
use crate::{
    Store,
    capacity::{Action, Claim, Error, Request},
    host_maintenance::{self, HostProvision},
};
use sandbox_protocol::Id;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sqlx::Row;
use uuid::Uuid;

pub const TOMBSTONE: &str = "hudsonbox.io/capacity-tombstone";
pub const WORKER_LABEL: &str = "hudsonbox.io/worker";
/// Immutable, per-worker Secret must contain ca.pem, tls.crt, tls.key.
/// Its host.json pins this host/epoch and the admission budget; never reuse it.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkerConfig {
    pub image: String,
    pub runtime: RuntimeFiles,
    pub identity_secret: String,
    pub controller_certificate_sha256: String,
    pub server_certificate_sha256: String,
    #[serde(default)]
    pub output_reader_certificate_sha256: Vec<String>,
    #[serde(default)]
    pub file_reader_certificate_sha256: Vec<String>,
    /// Immutable Secret with private config.json for the existing S3 output adapter.
    #[serde(default)]
    pub artifacts_secret: Option<String>,
    pub overhead_vcpu: u32,
    pub overhead_memory_mib: u64,
    pub overhead_disk_mib: u64,
}
/// Image-local artifacts only; no platform credentials in the worker definition.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Artifact {
    pub path: String,
    pub sha256: String,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Image {
    pub kernel: Artifact,
    pub rootfs: Artifact,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimeFiles {
    pub guardian_binary: String,
    pub firecracker: Artifact,
    pub jailer: Artifact,
    pub images: std::collections::BTreeMap<String, Image>,
    pub jail_uid: u32,
    pub jail_gid: u32,
}
fn label(s: &str) -> bool {
    !s.is_empty()
        && s.len() <= 63
        && s.bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
        && !s.starts_with('-')
        && !s.ends_with('-')
}
impl WorkerConfig {
    pub fn pod(
        &self,
        pool: Uuid,
        request: &Request,
        config: &HostProvision,
        namespace: &str,
    ) -> Result<Value, Error> {
        let (image, digest) = self.image.split_once("@sha256:").ok_or(Error::Invalid)?;
        if image.is_empty()
            || image.len() > 256
            || image.bytes().any(|b| !b.is_ascii_graphic())
            || digest.len() != 64
            || !digest.bytes().all(|b| b.is_ascii_hexdigit())
            || !label(&self.identity_secret)
            || !label(namespace)
            || self.server_certificate_sha256.len() != 64
            || !self
                .server_certificate_sha256
                .bytes()
                .all(|b| b.is_ascii_hexdigit())
            || self.controller_certificate_sha256.len() != 64
            || !self
                .controller_certificate_sha256
                .bytes()
                .all(|b| b.is_ascii_hexdigit())
            || self.overhead_vcpu == 0
            || self.overhead_memory_mib < 256
            || self.overhead_disk_mib < 512
        {
            return Err(Error::Invalid);
        }
        let cpu = config
            .capacity
            .vcpu
            .checked_add(self.overhead_vcpu)
            .filter(|n| *n <= 1024)
            .ok_or(Error::Invalid)?;
        let memory = config
            .capacity
            .memory_mib
            .checked_add(self.overhead_memory_mib)
            .filter(|n| *n <= 16_777_216)
            .ok_or(Error::Invalid)?;
        let disk = config
            .capacity
            .disk_mib
            .checked_add(self.overhead_disk_mib)
            .filter(|n| *n <= 1_073_741_824)
            .ok_or(Error::Invalid)?;
        let architecture = match config.capabilities.architecture.as_str() {
            "x86_64" => "amd64",
            "aarch64" => "arm64",
            _ => return Err(Error::Invalid),
        };
        if self.output_reader_certificate_sha256.len() > 8
            || self.file_reader_certificate_sha256.len() > 8
            || self
                .output_reader_certificate_sha256
                .iter()
                .chain(&self.file_reader_certificate_sha256)
                .any(|pin| {
                    pin.len() != 64
                        || !pin.bytes().all(|b| b.is_ascii_hexdigit())
                        || pin.eq_ignore_ascii_case(&self.controller_certificate_sha256)
                })
            || self
                .artifacts_secret
                .as_deref()
                .is_some_and(|name| !label(name))
        {
            return Err(Error::Invalid);
        }
        let resources = json!({"cpu":cpu.to_string(),"memory":format!("{memory}Mi"),"ephemeral-storage":format!("{disk}Mi")});
        if self.runtime.images.is_empty()
            || self.runtime.images.len() > 32
            || self.runtime.jail_uid == 0
            || self.runtime.jail_gid == 0
        {
            return Err(Error::Invalid);
        }
        let host_config = json!({"host":request.host,"epoch":request.epoch,"capacity":config.capacity,
            "capabilities":config.capabilities,"launch_permits_required":true,
            "state_root":"/var/lib/hudsonbox/state","cgroup_parent":"/sys/fs/cgroup/hudsonbox",
            "guardian_binary":self.runtime.guardian_binary,"firecracker":self.runtime.firecracker,
            "jailer":self.runtime.jailer,"images":self.runtime.images,"jail_uid":self.runtime.jail_uid,"jail_gid":self.runtime.jail_gid});
        let host_json = serde_json::to_string(&host_config).map_err(|_| Error::Invalid)?;
        if host_json.len() > 32768 {
            return Err(Error::Invalid);
        }
        let mut pod = json!({"apiVersion":"v1","kind":"Pod","metadata":{
            "name":format!("hb-{}",request.host.uuid()),"namespace":namespace,
            "labels":{WORKER_LABEL:"true","hudsonbox.io/pool":pool.to_string()},
            "annotations":{"hudsonbox.io/host":request.host.to_string(),"hudsonbox.io/epoch":request.epoch.to_string(),"hudsonbox.io/certificate-sha256":self.server_certificate_sha256},
            "finalizers":[TOMBSTONE]},"spec":{
            "hostname":format!("hb-{}",request.host.uuid()),"subdomain":"hudsonbox-workers",
            "restartPolicy":"Never","automountServiceAccountToken":false,"terminationGracePeriodSeconds":120,
            "nodeSelector":{"hudsonbox.io/kvm":"true","kubernetes.io/os":"linux","kubernetes.io/arch":architecture},
            "tolerations":[{"key":"hudsonbox.io/kvm","operator":"Equal","value":"true","effect":"NoSchedule"}],
            "affinity":{"podAntiAffinity":{"requiredDuringSchedulingIgnoredDuringExecution":[{
                "labelSelector":{"matchLabels":{WORKER_LABEL:"true"}},"namespaceSelector":{},"topologyKey":"kubernetes.io/hostname"}]}},
            "containers":[{"name":"supervisor","image":self.image,"imagePullPolicy":"IfNotPresent",
                "command":["/bin/sh","-ec","umask 077; printf '%s' \"$HUDSONBOX_HOST_CONFIG\" > /var/lib/hudsonbox/host.json; exec /usr/local/bin/sandbox-host \"$@\"","worker"],
                "env":[{"name":"HUDSONBOX_HOST_CONFIG","value":host_json}],"args":["--config","/var/lib/hudsonbox/host.json","--listen","0.0.0.0:7443",
                    "--ca-cert","/identity/ca.pem","--server-cert","/identity/tls.crt","--server-key","/identity/tls.key",
                    "--controller-cert-sha256",self.controller_certificate_sha256],
                "securityContext":{"privileged":true,"runAsUser":0,"readOnlyRootFilesystem":true},
                "resources":{"requests":resources,"limits":resources},
                "ports":[{"name":"grpc","containerPort":7443}],
                "volumeMounts":[{"name":"identity","mountPath":"/identity","readOnly":true},
                    {"name":"state","mountPath":"/var/lib/hudsonbox"},{"name":"kvm","mountPath":"/dev/kvm"},
                    {"name":"cgroup","mountPath":"/sys/fs/cgroup"},{"name":"tmp","mountPath":"/tmp"}]}],
            "volumes":[{"name":"identity","secret":{"secretName":self.identity_secret,"defaultMode":256}},
                {"name":"state","emptyDir":{"sizeLimit":format!("{}Mi",config.capacity.disk_mib)}},
                {"name":"tmp","emptyDir":{"sizeLimit":format!("{}Mi",self.overhead_disk_mib)}},
                {"name":"kvm","hostPath":{"path":"/dev/kvm","type":"CharDevice"}},
                {"name":"cgroup","hostPath":{"path":"/sys/fs/cgroup","type":"Directory"}}]}});
        let args = pod["spec"]["containers"][0]["args"]
            .as_array_mut()
            .ok_or(Error::Invalid)?;
        for (flag, pins) in [
            (
                "--output-reader-cert-sha256",
                &self.output_reader_certificate_sha256,
            ),
            (
                "--file-reader-cert-sha256",
                &self.file_reader_certificate_sha256,
            ),
        ] {
            for pin in pins {
                args.extend([json!(flag), json!(pin)]);
            }
        }
        if let Some(secret) = &self.artifacts_secret {
            args.extend([json!("--output-config"), json!("/artifacts/config.json")]);
            pod["spec"]["containers"][0]["volumeMounts"]
                .as_array_mut()
                .ok_or(Error::Invalid)?
                .push(json!({"name":"artifacts","mountPath":"/artifacts/config.json","subPath":"config.json","readOnly":true}));
            pod["spec"]["volumes"]
                .as_array_mut()
                .ok_or(Error::Invalid)?
                .push(json!({"name":"artifacts","secret":{"secretName":secret,"defaultMode":256}}));
        }
        Ok(pod)
    }
}

pub(crate) async fn enroll(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    id: Uuid,
    pool: Uuid,
    request: &Request,
    config: &HostProvision,
    worker: &WorkerConfig,
) -> Result<(), Error> {
    let row = sqlx::query(
        "SELECT provider_config,credential_secret_ref,max_hosts FROM host_pools WHERE id=$1",
    )
    .bind(pool)
    .fetch_one(&mut **tx)
    .await?;
    let provider: Value = row.try_get("provider_config")?;
    let namespace = provider["namespace"].as_str().ok_or(Error::Invalid)?;
    if provider["worker_label_selector"] != format!("{WORKER_LABEL}=true") {
        return Err(Error::Invalid);
    }
    // Account for existing hosts, including standalone inventory if a DB was misconfigured.
    let active: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM (SELECT id FROM hosts WHERE host_pool_id=$1 AND capacity_retired_at IS NULL UNION SELECT host_id FROM k8s_workers WHERE host_pool_id=$1 AND NOT retired) inventory",
    )
    .bind(pool)
    .fetch_one(&mut **tx)
    .await?;
    let retained: i64 =
        sqlx::query_scalar("SELECT count(*) FROM k8s_workers WHERE host_pool_id=$1")
            .bind(pool)
            .fetch_one(&mut **tx)
            .await?;
    if active >= i64::from(row.try_get::<i32, _>("max_hosts")?) || retained >= 1000 {
        return Err(Error::Invalid);
    }
    let exists: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM hosts WHERE id=$1)")
        .bind(request.host.uuid())
        .fetch_one(&mut **tx)
        .await?;
    if exists || request.epoch != 1 {
        return Err(Error::Conflict);
    }
    let pod = worker.pod(pool, request, config, namespace)?;
    let credential: Option<String> = row.try_get("credential_secret_ref")?;
    let credential = credential.ok_or(Error::Invalid)?;
    host_maintenance::provision_in(tx, request.host, request.epoch, config).await?;
    // Secret references are immutable/versioned operator objects, never credential bytes.
    let frozen =
        json!({"version":1,"namespace":namespace,"credential_secret_ref":credential,"pod":pod});
    sqlx::query("INSERT INTO k8s_workers(host_id,host_pool_id,provision_operation_id,frozen) VALUES($1,$2,$3,$4)")
        .bind(request.host.uuid()).bind(pool).bind(id).bind(frozen).execute(&mut **tx).await?;
    Ok(())
}

#[derive(Debug)]
pub struct Effect {
    pub request: Request,
    pub frozen: Value,
    pub pod_uid: Option<String>,
    pub retired: bool,
}
impl Store {
    /// Include irreversibly closed hosts whose external retirement is uncertain.
    pub async fn k8s_capacity_inventory(&self, pool: Uuid) -> Result<u32, Error> {
        let count:i64=sqlx::query_scalar("SELECT count(*) FROM (SELECT id FROM hosts WHERE host_pool_id=$1 AND capacity_retired_at IS NULL UNION SELECT host_id FROM k8s_workers WHERE host_pool_id=$1 AND NOT retired) inventory")
            .bind(pool).fetch_one(self.pool()).await?;
        u32::try_from(count).map_err(|_| Error::Invalid)
    }
    /// Re-read authoritative input, freeze irreversible retirement before dispatch.
    /// No provider result, timeout or missing Pod releases sandbox reservations.
    pub async fn prepare_k8s_capacity(&self, claim: &Claim) -> Result<Effect, Error> {
        let mut tx = self.pool().begin().await?;
        sqlx::query("SET LOCAL lock_timeout='1s'")
            .execute(&mut *tx)
            .await?;
        let kind: String = sqlx::query_scalar("SELECT type FROM host_pools WHERE id=$1 FOR UPDATE")
            .bind(claim.pool)
            .fetch_one(&mut *tx)
            .await?;
        if kind != "k8s" {
            return Err(Error::Conflict);
        }
        let value:Value=sqlx::query_scalar("SELECT request FROM capacity_operations WHERE id=$1 AND host_pool_id=$2 AND claim_revision=$3 AND status='running' AND lease_expires_at>clock_timestamp() FOR UPDATE")
            .bind(claim.id).bind(claim.pool).bind(claim.revision).fetch_optional(&mut *tx).await?.ok_or(Error::LostClaim)?;
        let request: Request = serde_json::from_value(value).map_err(|_| Error::Invalid)?;
        let row=sqlx::query("SELECT frozen,pod_uid,retired,provision_operation_id FROM k8s_workers WHERE host_id=$1 AND host_pool_id=$2 FOR UPDATE")
            .bind(request.host.uuid()).bind(claim.pool).fetch_optional(&mut *tx).await?.ok_or(Error::Conflict)?;
        let provision: Uuid = row.try_get("provision_operation_id")?;
        let retired: bool = row.try_get("retired")?;
        let epoch: i64 =
            sqlx::query_scalar("SELECT supervisor_epoch FROM hosts WHERE id=$1 FOR UPDATE")
                .bind(request.host.uuid())
                .fetch_one(&mut *tx)
                .await?;
        if epoch != request.epoch {
            return Err(Error::Conflict);
        }
        if matches!(request.action, Action::K8sProvision { .. }) {
            if provision != claim.id || retired {
                return Err(Error::Conflict);
            }
        } else {
            let completed: bool = sqlx::query_scalar(
                "SELECT status='succeeded' FROM capacity_operations WHERE id=$1",
            )
            .bind(provision)
            .fetch_one(&mut *tx)
            .await?;
            if !completed {
                return Err(Error::Conflict);
            }
            match request.action {
                Action::Drain if !retired => {
                    let report =
                        host_maintenance::report(&mut tx, request.host, request.epoch).await?;
                    if !matches!(report.status.as_str(), "ready" | "draining") {
                        return Err(Error::Conflict);
                    }
                    sqlx::query("UPDATE hosts SET status='draining',updated_at=clock_timestamp() WHERE id=$1").bind(request.host.uuid()).execute(&mut *tx).await?;
                }
                Action::Retire if !retired => {
                    let closed: bool = sqlx::query_scalar(
                        "SELECT capacity_retired_at IS NOT NULL FROM hosts WHERE id=$1",
                    )
                    .bind(request.host.uuid())
                    .fetch_one(&mut *tx)
                    .await?;
                    if !closed {
                        let report =
                            host_maintenance::report(&mut tx, request.host, request.epoch).await?;
                        let fresh:bool=sqlx::query_scalar("SELECT COALESCE(last_seen_at>clock_timestamp()-interval '30 seconds',false) FROM hosts WHERE id=$1").bind(request.host.uuid()).fetch_one(&mut *tx).await?;
                        if report.status != "draining"
                            || report.outstanding_allocations != 0
                            || report.unfinished_operations != 0
                            || !fresh
                        {
                            return Err(Error::Conflict);
                        }
                        // Irreversible admission closure: a lost DELETE reply cannot reopen this worker.
                        sqlx::query("UPDATE hosts SET capacity_retired_at=clock_timestamp(),updated_at=clock_timestamp() WHERE id=$1").bind(request.host.uuid()).execute(&mut *tx).await?;
                    }
                }
                Action::Status | Action::Retire => {}
                _ => return Err(Error::Conflict),
            }
        }
        let effect = Effect {
            request,
            frozen: row.try_get("frozen")?,
            pod_uid: row.try_get("pod_uid")?,
            retired,
        };
        tx.commit().await?;
        Ok(effect)
    }
    /// Receipt writes are fenced by the live claim. External operations themselves
    /// are create-only or UID-conditional deletion of a permanently named tombstone.
    pub async fn complete_k8s_capacity(
        &self,
        claim: &Claim,
        uid: &str,
        receipt: &Value,
    ) -> Result<Value, Error> {
        if uid.is_empty() || uid.len() > 128 {
            return Err(Error::Invalid);
        }
        let mut tx = self.pool().begin().await?;
        let value:Value=sqlx::query_scalar("SELECT request FROM capacity_operations WHERE id=$1 AND host_pool_id=$2 AND claim_revision=$3 AND status='running' AND lease_expires_at>clock_timestamp() FOR UPDATE")
            .bind(claim.id).bind(claim.pool).bind(claim.revision).fetch_optional(&mut *tx).await?.ok_or(Error::LostClaim)?;
        let request: Request = serde_json::from_value(value).map_err(|_| Error::Invalid)?;
        let n=sqlx::query("UPDATE k8s_workers SET pod_uid=$3,retired=retired OR $4 WHERE host_id=$1 AND host_pool_id=$2 AND (pod_uid IS NULL OR pod_uid=$3)")
            .bind(request.host.uuid()).bind(claim.pool).bind(uid).bind(matches!(request.action,Action::Retire)).execute(&mut *tx).await?.rows_affected();
        if n != 1 {
            return Err(Error::Conflict);
        }
        let n=sqlx::query("UPDATE capacity_operations SET status='succeeded',lease_expires_at=NULL,provider_receipt=$4,error=NULL,updated_at=clock_timestamp() WHERE id=$1 AND host_pool_id=$2 AND claim_revision=$3 AND lease_expires_at>clock_timestamp()")
            .bind(claim.id).bind(claim.pool).bind(claim.revision).bind(receipt).execute(&mut *tx).await?.rows_affected();
        if n != 1 {
            return Err(Error::LostClaim);
        }
        tx.commit().await?;
        Ok(receipt.clone())
    }
}
