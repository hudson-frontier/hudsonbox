//! Trusted read-only host adapter. Endpoints and credentials are operator inputs.
use crate::problem::Problem;
use sandbox_protocol::{
    HostId,
    supervisor::{LiveOutputObservation, LiveOutputRequest},
};
use std::{fmt, future::Future, pin::Pin};

pub trait LiveReader: fmt::Debug + Send + Sync {
    fn read<'a>(
        &'a self,
        host: HostId,
        request: LiveOutputRequest,
    ) -> Pin<Box<dyn Future<Output = Result<LiveOutputObservation, Problem>> + Send + 'a>>;
}
pub struct LiveClient {
    routes: crate::owners::OwnerRoutes,
    allow_simulated: bool,
}
impl fmt::Debug for LiveClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LiveClient")
            .field("routes", &self.routes)
            .finish_non_exhaustive()
    }
}
impl LiveClient {
    /// Legacy single-host reader.
    pub fn new(
        host: HostId,
        endpoint: String,
        ca: Vec<u8>,
        cert: Vec<u8>,
        key: Vec<u8>,
        allow_simulated: bool,
    ) -> anyhow::Result<Self> {
        Self::routed(
            None,
            Some((host, endpoint)),
            &ca,
            &cert,
            &key,
            allow_simulated,
        )
    }
    /// Registry routing to each allocation owner, optionally keeping one legacy host.
    pub fn routed(
        store: Option<sandbox_store::Store>,
        legacy: Option<(HostId, String)>,
        ca: &[u8],
        cert: &[u8],
        key: &[u8],
        allow_simulated: bool,
    ) -> anyhow::Result<Self> {
        Ok(Self {
            routes: crate::owners::OwnerRoutes::new(store, legacy, ca, cert, key)
                .map_err(|e| anyhow::anyhow!("live reader: {e}"))?,
            allow_simulated,
        })
    }
}
impl LiveReader for LiveClient {
    fn read<'a>(
        &'a self,
        host: HostId,
        request: LiveOutputRequest,
    ) -> Pin<Box<dyn Future<Output = Result<LiveOutputObservation, Problem>> + Send + 'a>> {
        Box::pin(async move {
            let route = self.routes.owner_route(host).await?;
            let mut client =
                sandbox_protocol::supervisor::live_output_client::LiveOutputClient::new(
                    route.lease.channel.clone(),
                )
                .max_decoding_message_size(sandbox_supervisor::transport::MAX_MESSAGE_BYTES)
                .max_encoding_message_size(sandbox_supervisor::transport::MAX_MESSAGE_BYTES);
            let response = client.read(request).await;
            self.routes.observe(&route, &response).await;
            let reply = response
                .map_err(|e| match e.code() {
                    tonic::Code::NotFound | tonic::Code::FailedPrecondition => {
                        Problem::OutputMissing
                    }
                    tonic::Code::OutOfRange => Problem::OutputRange,
                    _ => Problem::Unavailable,
                })?
                .into_inner();
            // A reply from any other host or incarnation is never accepted.
            if reply.simulated && !self.allow_simulated
                || reply.host_id != host.to_string()
                || route
                    .epoch
                    .is_some_and(|epoch| reply.supervisor_epoch != epoch)
            {
                return Err(Problem::Unavailable);
            }
            Ok(reply)
        })
    }
}
