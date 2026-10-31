//! Optional Kubernetes capacity only: no sandbox CRDs, lifecycle or recovery.
use crate::{
    capacity::{CapacityFuture, CapacityProvider},
    host_discovery::{
        DiscoveredWorker, DiscoveryError, DiscoveryFuture, HostDiscovery, validate_workers,
    },
};
use sandbox_store::{
    Store,
    capacity::{Action, Claim, Error},
    host_registry::{HostPool, PoolType, ProviderConfig},
    k8s_capacity::{TOMBSTONE, WORKER_LABEL},
};
use serde_json::{Value, json};
use std::{future::Future, pin::Pin, sync::Arc};

pub type ApiFuture<'a> = Pin<Box<dyn Future<Output = Result<Option<Value>, Error>> + Send + 'a>>;
/// POST is create-only (409 => GET); DELETE must carry a UID precondition.
/// Implementations must bound deadlines/body sizes and never log credentials.
pub trait KubernetesApi: Send + Sync + std::fmt::Debug {
    fn credential_reference(&self) -> &str;
    fn call<'a>(
        &'a self,
        method: &'a str,
        namespace: &'a str,
        name: Option<&'a str>,
        body: Option<Value>,
        selector: Option<String>,
    ) -> ApiFuture<'a>;
}
/// Explicit cluster identity and versioned credential reference. No ambient
/// kubeconfig, shell, cloud credentials, redirects or environment proxies.
pub struct HttpKubernetesApi {
    client: reqwest::Client,
    server: String,
    token: String,
    credential_reference: String,
}
impl std::fmt::Debug for HttpKubernetesApi {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HttpKubernetesApi").finish_non_exhaustive()
    }
}
impl HttpKubernetesApi {
    pub fn new(
        server: &str,
        ca_pem: &[u8],
        token: String,
        credential_reference: String,
    ) -> Result<Self, Error> {
        let url = reqwest::Url::parse(server).map_err(|_| Error::Invalid)?;
        if url.scheme() != "https"
            || url.host_str().is_none()
            || !url.username().is_empty()
            || url.password().is_some()
            || url.query().is_some()
            || url.fragment().is_some()
            || url.path() != "/"
            || token.is_empty()
            || token.len() > 16384
            || !token.bytes().all(|b| b.is_ascii_graphic())
            || credential_reference.is_empty()
        {
            return Err(Error::Invalid);
        }
        let client = reqwest::Client::builder()
            .https_only(true)
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(std::time::Duration::from_secs(10))
            .tls_certs_only([reqwest::Certificate::from_pem(ca_pem).map_err(|_| Error::Invalid)?])
            .build()
            .map_err(|_| Error::Invalid)?;
        Ok(Self {
            client,
            server: server.trim_end_matches('/').into(),
            token,
            credential_reference,
        })
    }
}
impl KubernetesApi for HttpKubernetesApi {
    fn credential_reference(&self) -> &str {
        &self.credential_reference
    }
    fn call<'a>(
        &'a self,
        method: &'a str,
        namespace: &'a str,
        name: Option<&'a str>,
        body: Option<Value>,
        selector: Option<String>,
    ) -> ApiFuture<'a> {
        Box::pin(async move {
            if !matches!(method, "GET" | "POST" | "DELETE")
                || namespace.is_empty()
                || namespace.len() > 63
                || !namespace
                    .bytes()
                    .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
                || name.is_some_and(|n| {
                    n.is_empty()
                        || n.len() > 63
                        || !n
                            .bytes()
                            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
                })
            {
                return Err(Error::Invalid);
            }
            let mut url = reqwest::Url::parse(&format!(
                "{}/api/v1/namespaces/{namespace}/pods{}",
                self.server,
                name.map(|n| format!("/{n}")).unwrap_or_default()
            ))
            .map_err(|_| Error::Invalid)?;
            if let Some(selector) = selector {
                url.query_pairs_mut()
                    .append_pair("labelSelector", &selector)
                    .append_pair("limit", "1000");
            }
            let mut req = self
                .client
                .request(
                    reqwest::Method::from_bytes(method.as_bytes()).map_err(|_| Error::Invalid)?,
                    url,
                )
                .bearer_auth(&self.token);
            if let Some(body) = body {
                req = req
                    .header("Content-Type", "application/json")
                    .body(serde_json::to_vec(&body).map_err(|_| Error::Invalid)?);
            }
            let mut response = req.send().await.map_err(|_| Error::Conflict)?;
            if response.status() == reqwest::StatusCode::NOT_FOUND {
                return Ok(None);
            }
            // A conflict is reconciled by the caller's GET, never update/apply.
            if method == "POST" && response.status() == reqwest::StatusCode::CONFLICT {
                return Ok(None);
            }
            if !response.status().is_success() {
                return Err(Error::Conflict);
            }
            let mut bytes = Vec::new();
            while let Some(chunk) = response.chunk().await.map_err(|_| Error::Conflict)? {
                if bytes.len() + chunk.len() > 4 * 1024 * 1024 {
                    return Err(Error::Invalid);
                }
                bytes.extend_from_slice(&chunk);
            }
            Ok(Some(
                serde_json::from_slice(&bytes).map_err(|_| Error::Invalid)?,
            ))
        })
    }
}
#[derive(Debug)]
pub struct KubernetesCapacityProvider {
    api: Arc<dyn KubernetesApi>,
}
impl KubernetesCapacityProvider {
    pub fn new(api: Arc<dyn KubernetesApi>) -> Self {
        Self { api }
    }
}
fn verify(pod: &Value, desired: &Value, expected_uid: Option<&str>) -> Result<String, Error> {
    let uid = pod["metadata"]["uid"]
        .as_str()
        .filter(|s| !s.is_empty() && s.len() <= 128)
        .ok_or(Error::Conflict)?;
    if expected_uid.is_some_and(|expected| expected != uid)
        || pod["metadata"]["name"] != desired["metadata"]["name"]
        || pod["metadata"]["namespace"] != desired["metadata"]["namespace"]
        || pod["metadata"]["annotations"] != desired["metadata"]["annotations"]
        || !pod["metadata"]["finalizers"]
            .as_array()
            .is_some_and(|f| f.iter().any(|v| v == TOMBSTONE))
        || pod["metadata"]["labels"][WORKER_LABEL] != "true"
        || pod["metadata"]["labels"]["hudsonbox.io/pool"]
            != desired["metadata"]["labels"]["hudsonbox.io/pool"]
    {
        return Err(Error::Conflict);
    }
    // Admission may default fields, but may not change the frozen resource envelope.
    if pod["spec"]["containers"][0]["image"] != desired["spec"]["containers"][0]["image"]
        || pod["spec"]["containers"][0]["resources"]
            != desired["spec"]["containers"][0]["resources"]
        || pod["spec"]["restartPolicy"] != "Never"
        || pod["spec"]["affinity"] != desired["spec"]["affinity"]
    {
        return Err(Error::Conflict);
    }
    Ok(uid.into())
}
impl CapacityProvider for KubernetesCapacityProvider {
    fn pool_type(&self) -> PoolType {
        PoolType::K8s
    }
    fn apply<'a>(&'a self, store: &'a Store, claim: &'a Claim) -> CapacityFuture<'a> {
        Box::pin(async move {
            let effect = store.prepare_k8s_capacity(claim).await?;
            if effect.frozen["credential_secret_ref"].as_str()
                != Some(self.api.credential_reference())
            {
                return Err(Error::Conflict);
            }
            let desired = &effect.frozen["pod"];
            let namespace = effect.frozen["namespace"].as_str().ok_or(Error::Invalid)?;
            let name = desired["metadata"]["name"].as_str().ok_or(Error::Invalid)?;
            let mut pod = self
                .api
                .call("GET", namespace, Some(name), None, None)
                .await?;
            if matches!(effect.request.action, Action::K8sProvision { .. }) && pod.is_none() {
                store.validate_capacity_claim(claim).await?;
                self.api
                    .call("POST", namespace, None, Some(desired.clone()), None)
                    .await?;
                pod = self
                    .api
                    .call("GET", namespace, Some(name), None, None)
                    .await?;
            }
            let mut pod = pod.ok_or(Error::Conflict)?;
            let uid = verify(&pod, desired, effect.pod_uid.as_deref())?;
            if matches!(effect.request.action, Action::K8sProvision { .. })
                && !pod["metadata"]["deletionTimestamp"].is_null()
            {
                return Err(Error::Conflict);
            }
            if matches!(effect.request.action, Action::Retire)
                && pod["metadata"]["deletionTimestamp"].is_null()
            {
                store.validate_capacity_claim(claim).await?;
                self.api.call("DELETE",namespace,Some(name),Some(json!({"apiVersion":"v1","kind":"DeleteOptions","gracePeriodSeconds":120,"preconditions":{"uid":uid}})),None).await?;
                pod = self
                    .api
                    .call("GET", namespace, Some(name), None, None)
                    .await?
                    .ok_or(Error::Conflict)?;
                verify(&pod, desired, Some(&uid))?;
            }
            if matches!(effect.request.action, Action::Retire)
                && pod["metadata"]["deletionTimestamp"].as_str().is_none()
            {
                return Err(Error::Conflict);
            }
            let receipt = json!({"version":1,"host":effect.request.host,"epoch":effect.request.epoch,"pod_uid":uid,"name":name,"namespace":namespace,
                "action":effect.request.action.kind(),"deletion_requested":pod["metadata"]["deletionTimestamp"].is_string(),"physical_termination_confirmed":false});
            store.complete_k8s_capacity(claim, &uid, &receipt).await
        })
    }
}
impl HostDiscovery for KubernetesCapacityProvider {
    fn pool_type(&self) -> PoolType {
        PoolType::K8s
    }
    fn discover<'a>(&'a self, pool: &'a HostPool) -> DiscoveryFuture<'a> {
        Box::pin(async move {
            let ProviderConfig::K8s(config) = &pool.provider_config else {
                return Err(DiscoveryError::WrongPoolType);
            };
            if pool.credential_secret_ref.as_deref() != Some(self.api.credential_reference()) {
                return Err(DiscoveryError::InvalidConfig);
            }
            let list = self
                .api
                .call(
                    "GET",
                    &config.namespace,
                    None,
                    None,
                    Some(format!("{WORKER_LABEL}=true,hudsonbox.io/pool={}", pool.id)),
                )
                .await
                .map_err(|_| DiscoveryError::Unavailable)?
                .ok_or(DiscoveryError::Unavailable)?;
            if list["metadata"]["continue"]
                .as_str()
                .is_some_and(|s| !s.is_empty())
            {
                return Err(DiscoveryError::InvalidResult);
            }
            let pods = list["items"]
                .as_array()
                .ok_or(DiscoveryError::InvalidResult)?;
            if pods.len() > 1000 {
                return Err(DiscoveryError::InvalidResult);
            }
            let mut workers = Vec::new();
            for pod in pods {
                if !pod["metadata"]["deletionTimestamp"].is_null()
                    || pod["status"]["phase"] != "Running"
                {
                    continue;
                }
                let meta = &pod["metadata"];
                let annotations = &meta["annotations"];
                let name = meta["name"].as_str().ok_or(DiscoveryError::InvalidResult)?;
                let uid = meta["uid"].as_str().ok_or(DiscoveryError::InvalidResult)?;
                let host = annotations["hudsonbox.io/host"]
                    .as_str()
                    .ok_or(DiscoveryError::InvalidResult)?
                    .parse()
                    .map_err(|_| DiscoveryError::InvalidResult)?;
                if annotations["hudsonbox.io/epoch"] != "1"
                    || meta["namespace"] != config.namespace
                    || meta["labels"]["hudsonbox.io/pool"] != pool.id.to_string()
                    || meta["labels"][WORKER_LABEL] != "true"
                {
                    return Err(DiscoveryError::InvalidResult);
                }
                workers.push(DiscoveredWorker {
                    host_id: host,
                    endpoint: format!(
                        "https://{name}.hudsonbox-workers.{}.svc:7443",
                        config.namespace
                    ),
                    certificate_sha256: annotations["hudsonbox.io/certificate-sha256"]
                        .as_str()
                        .ok_or(DiscoveryError::InvalidResult)?
                        .into(),
                    provider_ref: Some(format!("k8s/{}/{name}/{uid}", config.namespace)),
                });
            }
            validate_workers(&workers)
        })
    }
}
