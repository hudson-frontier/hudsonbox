//! Owner routing for API reads. The host comes from the authorized sandbox's
//! persisted allocation, never from the caller; the endpoint and certificate pin
//! come from the operator registry, or from operator configuration for the one
//! legacy host whose registry transport is NULL.
use crate::problem::Problem;
use sandbox_protocol::HostId;
use sandbox_store::Store;
use sandbox_supervisor::routing::{ChannelPool, ClientTls, HostAddress, Lease, PoolLimits};
use std::fmt;

pub struct OwnerRoutes {
    store: Option<Store>,
    legacy: Option<(HostId, String)>,
    pool: ChannelPool,
}

impl fmt::Debug for OwnerRoutes {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OwnerRoutes")
            .field("registry", &self.store.is_some())
            .field("legacy", &self.legacy.as_ref().map(|(host, _)| host))
            .finish_non_exhaustive()
    }
}

/// One request slot on one owner host. `epoch` is the registered incarnation,
/// unknown only for a legacy host configured without registry routing.
#[derive(Debug)]
pub struct OwnerRoute {
    pub lease: Lease,
    pub epoch: Option<i64>,
}

fn endpoint_ok(endpoint: &str) -> bool {
    endpoint.parse::<http::Uri>().is_ok_and(|uri| {
        uri.scheme_str() == Some("https")
            && uri.host().is_some()
            && uri.authority().is_some_and(|a| !a.as_str().contains('@'))
            && uri.path() == "/"
            && uri.query().is_none()
    })
}

impl OwnerRoutes {
    /// `store` enables registry routing; `legacy` keeps one configured host.
    pub fn new(
        store: Option<Store>,
        legacy: Option<(HostId, String)>,
        ca: &[u8],
        cert: &[u8],
        key: &[u8],
    ) -> anyhow::Result<Self> {
        anyhow::ensure!(
            store.is_some() || legacy.is_some(),
            "configure a host or registry routing"
        );
        anyhow::ensure!(
            legacy.as_ref().is_none_or(|(_, e)| endpoint_ok(e)),
            "invalid host endpoint"
        );
        anyhow::ensure!(
            [ca, cert, key]
                .iter()
                .all(|v| !v.is_empty() && v.len() <= 65536),
            "invalid host TLS material"
        );
        let tls = ClientTls::from_pem(ca, cert, key)
            .map_err(|_| anyhow::anyhow!("invalid host TLS material"))?;
        Ok(Self {
            store,
            legacy,
            pool: ChannelPool::new(tls, PoolLimits::default())
                .map_err(|_| anyhow::anyhow!("invalid host pool limits"))?,
        })
    }

    async fn address(&self, host: HostId) -> Result<(HostAddress, Option<i64>), Problem> {
        let legacy = self.legacy.as_ref().filter(|(h, _)| *h == host);
        let Some(store) = &self.store else {
            let (_, endpoint) = legacy.ok_or(Problem::Unavailable)?;
            // Without the registry the incarnation is checked by the supervisor
            // against the request scope only.
            let address =
                HostAddress::legacy(host, 1, endpoint).map_err(|_| Problem::Unavailable)?;
            return Ok((address, None));
        };
        let registered = store
            .registered_host(host)
            .await
            .map_err(|_| Problem::Unavailable)?
            .ok_or(Problem::Unavailable)?;
        let epoch = registered.supervisor_epoch;
        let address = match (&registered.transport, legacy) {
            (Some(t), _) => HostAddress::registered(
                host,
                epoch,
                &t.endpoint,
                &t.server_name,
                &t.certificate_sha256,
            ),
            (None, Some((_, endpoint))) => HostAddress::legacy(host, epoch, endpoint),
            (None, None) => return Err(Problem::Unavailable),
        }
        .map_err(|_| Problem::Unavailable)?;
        Ok((address, Some(epoch)))
    }

    /// Reserve one request slot on exactly `host`. Saturated, backing-off and
    /// unregistered hosts fail fast without affecting other hosts.
    pub async fn owner_route(&self, host: HostId) -> Result<OwnerRoute, Problem> {
        let (address, epoch) = self.address(host).await?;
        let lease = self
            .pool
            .acquire(&address, false)
            .map_err(|_| Problem::Unavailable)?;
        Ok(OwnerRoute { lease, epoch })
    }

    /// Transport-level failures back the host off; application statuses do not.
    pub fn observe<T>(&self, route: &OwnerRoute, result: &Result<T, tonic::Status>) {
        match result {
            Err(status)
                if matches!(
                    status.code(),
                    tonic::Code::Unavailable | tonic::Code::DeadlineExceeded | tonic::Code::Unknown
                ) =>
            {
                self.pool.report_failure(&route.lease.address)
            }
            _ => self.pool.report_success(&route.lease.address),
        }
    }
}
