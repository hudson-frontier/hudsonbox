//! Opt-in history worker, independent of execution and lifecycle maintenance.
use crate::{Controller, ControllerConfig};
use sandbox_protocol::history::Domain;
use sandbox_store::{
    Store,
    history::{Claim, Preparation},
};
use std::time::Duration;
#[derive(Debug)]
pub struct Retirer {
    store: Store,
    config: ControllerConfig,
    hosts: Controller,
    next: u8,
    host_cursor: usize,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HistoryTick {
    Idle,
    Completed,
}
#[derive(Debug, thiserror::Error)]
pub enum HistoryError {
    #[error("history retirement storage failed: {0}")]
    Store(#[from] sandbox_store::history::Error),
    #[error("history RPC failed; retained prefix requires reconciliation")]
    Rpc,
}
/// Healthy host incarnations in rotating order, so one host cannot starve others.
fn rotation(hosts: &Controller, cursor: &mut usize) -> Vec<(sandbox_protocol::HostId, i64)> {
    let mut healthy = hosts.hosts.healthy();
    if !healthy.is_empty() {
        let first = *cursor % healthy.len();
        healthy.rotate_left(first);
    }
    *cursor = cursor.wrapping_add(1);
    healthy
}
impl Controller {
    pub fn history_retirer(&self) -> Retirer {
        Retirer {
            store: self.store.clone(),
            config: self.config.clone(),
            hosts: self.worker(0),
            next: 0,
            host_cursor: 0,
        }
    }
}
impl Retirer {
    pub async fn tick(&mut self) -> Result<HistoryTick, HistoryError> {
        let turn = self.next;
        self.next = (self.next + 1) % 4;
        let domain = if turn.is_multiple_of(2) {
            Domain::Commands
        } else {
            Domain::Files
        };
        for (host, epoch) in rotation(&self.hosts, &mut self.host_cursor) {
            let tick = if turn >= 2 {
                self.released_tick(host, epoch, domain).await?
            } else {
                self.claimed_tick(host, epoch, domain).await?
            };
            if tick != HistoryTick::Idle {
                return Ok(tick);
            }
        }
        Ok(HistoryTick::Idle)
    }
    async fn claimed_tick(
        &mut self,
        host: sandbox_protocol::HostId,
        epoch: i64,
        domain: Domain,
    ) -> Result<HistoryTick, HistoryError> {
        let Some(preparation) = self
            .store
            .claim_history(host, epoch, domain, 120, self.config.allow_simulated)
            .await?
        else {
            return Ok(HistoryTick::Idle);
        };
        let claim = match &preparation {
            Preparation::Binding { claim, .. } | Preparation::Retire { claim, .. } => claim.clone(),
        };
        let result = self.process(host, epoch, preparation, &claim).await;
        if result.is_err() {
            let _ = self.store.defer_history(&claim).await;
        }
        result
    }
    async fn released_tick(
        &mut self,
        host: sandbox_protocol::HostId,
        epoch: i64,
        domain: Domain,
    ) -> Result<HistoryTick, HistoryError> {
        let Some(prepared) = self
            .store
            .claim_released_history(host, epoch, domain, 120, self.config.allow_simulated)
            .await?
        else {
            return Ok(HistoryTick::Idle);
        };
        let result = async {
            let mut route = self
                .hosts
                .route(host, epoch, true)
                .await
                .map_err(|_| HistoryError::Rpc)?;
            let observed = tokio::time::timeout(
                Duration::from_secs(30),
                self.hosts.rpc(
                    host,
                    epoch,
                    route.client.retire_released_history(prepared.request),
                ),
            )
            .await
            .map_err(|_| HistoryError::Rpc)?
            .map_err(|_| HistoryError::Rpc)?
            .into_inner();
            self.store
                .complete_released_history(&prepared.claim, &observed, self.config.allow_simulated)
                .await?;
            Ok(HistoryTick::Completed)
        }
        .await;
        if result.is_err() {
            let _ = self.store.defer_released_history(&prepared.claim).await;
        }
        result
    }
    async fn process(
        &mut self,
        host: sandbox_protocol::HostId,
        epoch: i64,
        preparation: Preparation,
        claim: &Claim,
    ) -> Result<HistoryTick, HistoryError> {
        let mut route = self
            .hosts
            .route(host, epoch, true)
            .await
            .map_err(|_| HistoryError::Rpc)?;
        let request = match preparation {
            Preparation::Binding { request, .. } => {
                let observed = tokio::time::timeout(
                    Duration::from_secs(30),
                    self.hosts
                        .rpc(host, epoch, route.client.history_binding(request)),
                )
                .await
                .map_err(|_| HistoryError::Rpc)?
                .map_err(|_| HistoryError::Rpc)?
                .into_inner();
                self.store
                    .bind_history(claim, &observed, self.config.allow_simulated)
                    .await?
            }
            Preparation::Retire { request, .. } => request,
        };
        let observed = tokio::time::timeout(
            Duration::from_secs(30),
            self.hosts
                .rpc(host, epoch, route.client.retire_history(request)),
        )
        .await
        .map_err(|_| HistoryError::Rpc)?
        .map_err(|_| HistoryError::Rpc)?
        .into_inner();
        self.store
            .complete_history(claim, &observed, self.config.allow_simulated)
            .await?;
        Ok(HistoryTick::Completed)
    }
}
