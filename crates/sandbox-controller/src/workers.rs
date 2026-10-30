//! Bounded independent reconciliation loops sharing one notification listener.
use crate::{Controller, ControllerError, Tick, wakeup};
use std::{future::Future, time::Duration};
use tokio::{
    sync::watch,
    task::{JoinError, JoinSet},
    time::timeout,
};

pub const DEFAULT_WORKERS: u32 = 4;
pub const MAX_WORKERS: u32 = 16;

/// Cooperative stop checked between ticks, never in place of durable recovery.
#[derive(Debug, Clone)]
pub struct WorkerShutdown(watch::Receiver<bool>);

impl WorkerShutdown {
    pub fn is_stopping(&self) -> bool {
        *self.0.borrow()
    }

    pub async fn wait(&mut self) {
        let _ = self.0.wait_for(|stopping| *stopping).await;
    }
}

/// Each worker owns its scheduling cursor and at most one cached upload source.
/// Database claims, rather than this process, arbitrate operation ownership.
/// Dropping the pool aborts workers and its single notification listener.
#[derive(Debug)]
pub struct WorkerPool {
    tasks: JoinSet<()>,
    stop: watch::Sender<bool>,
    _wakeup: wakeup::WorkWakeup,
}

impl WorkerPool {
    pub fn start(controller: &Controller, count: u32) -> Result<Self, ControllerError> {
        if !(1..=MAX_WORKERS).contains(&count) {
            return Err(ControllerError::InvalidWorkerCount);
        }
        let listener = wakeup::WorkWakeup::start(controller.store.clone());
        let mut tasks = JoinSet::new();
        let (stop, stopping) = watch::channel(false);
        for index in 0..count {
            let mut worker = controller.worker(index as usize);
            let mut signal = listener.subscribe();
            let mut stopping = WorkerShutdown(stopping.clone());
            tasks.spawn(async move {
                while !stopping.is_stopping() {
                    match worker.tick().await {
                        Ok(Tick::Idle) => {}
                        Ok(tick) => eprintln!("sandbox controller worker {index}: {tick:?}"),
                        Err(error) => eprintln!("sandbox controller worker {index}: {error}"),
                    }
                    tokio::select! {
                        biased;
                        _ = stopping.wait() => break,
                        _ = wakeup::wait(&mut signal) => {}
                    }
                }
            });
        }
        Ok(Self {
            tasks,
            stop,
            _wakeup: listener,
        })
    }

    /// A loop exiting or panicking must be reported to the process supervisor.
    pub async fn join_next(&mut self) -> Option<Result<(), JoinError>> {
        self.tasks.join_next().await
    }

    /// Track optional service workers under the same supervision and drain budget.
    /// Workers must check the signal between ticks and while waiting for a tick.
    pub fn spawn_background<F, Fut>(&mut self, build: F)
    where
        F: FnOnce(WorkerShutdown) -> Fut,
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.tasks
            .spawn(build(WorkerShutdown(self.stop.subscribe())));
    }

    /// Stop scheduling ticks and give all current ticks one shared grace period.
    /// Returns false after interrupting remaining work; claims still expire normally.
    /// Task failures are returned only after all sibling tasks have been joined.
    pub async fn drain(&mut self, grace: Duration) -> Result<bool, JoinError> {
        self.stop.send_replace(true);
        let mut failed = None;
        let drained = timeout(grace, async {
            while let Some(result) = self.tasks.join_next().await {
                if let Err(error) = result {
                    failed = Some(error);
                }
            }
        })
        .await
        .is_ok();
        if !drained {
            self.tasks.shutdown().await;
        }
        match failed {
            Some(error) => Err(error),
            None => Ok(drained),
        }
    }

    /// Cancel in-flight work and wait for its futures to be dropped. Persisted
    /// claims expire normally; cancellation cannot authorize another dispatch.
    pub async fn shutdown(&mut self) {
        self.stop.send_replace(true);
        self.tasks.shutdown().await;
    }
}
