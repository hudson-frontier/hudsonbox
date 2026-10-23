//! Cooperative tick boundaries, forced drain recovery, and real process signals.
#![allow(clippy::unwrap_used, clippy::expect_used)]
#[allow(dead_code)]
mod common;

use common::Fixture;
use sandbox_controller::workers::WorkerPool;
use sqlx::PgPool;
use std::{sync::Arc, time::Duration};
use tokio::sync::{Notify, oneshot};
use tokio::time::timeout;

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn drain_finishes_current_work_and_wakes_idle_workers(pool: PgPool) {
    let f = Fixture::new(&pool).await;
    let mut workers = WorkerPool::start(&f.controller().await, 2).unwrap();
    let (started, running) = oneshot::channel();
    let release = Arc::new(Notify::new());
    let (finished, completed) = oneshot::channel();
    let (draining, drain_started) = oneshot::channel();
    workers.spawn_background({
        let release = release.clone();
        |mut stopping| async move {
            started.send(()).unwrap();
            stopping.wait().await;
            draining.send(()).unwrap();
            release.notified().await;
            // A stop arrives during this tick; it must finish before returning.
            assert!(stopping.is_stopping());
            stopping.wait().await;
            finished.send(()).unwrap();
        }
    });
    running.await.unwrap();
    let task = tokio::spawn(async move { workers.drain(Duration::from_secs(5)).await });
    timeout(Duration::from_secs(2), drain_started)
        .await
        .unwrap()
        .unwrap();
    assert!(!task.is_finished());
    release.notify_one();
    assert!(
        timeout(Duration::from_secs(2), task)
            .await
            .unwrap()
            .unwrap()
            .unwrap()
    );
    completed.await.unwrap();
}

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn drain_has_one_shared_deadline_and_joins_cancelled_work(pool: PgPool) {
    let f = Fixture::new(&pool).await;
    let mut workers = WorkerPool::start(&f.controller().await, 1).unwrap();
    let mut cancellations = Vec::new();
    for _ in 0..3 {
        let (started, running) = oneshot::channel();
        let (cancelled, dropped) = oneshot::channel::<()>();
        workers.spawn_background(|_| async move {
            // This sender is dropped only when the cancelled task is joined.
            let _cancelled = cancelled;
            started.send(()).unwrap();
            std::future::pending::<()>().await;
        });
        running.await.unwrap();
        cancellations.push(dropped);
    }
    assert!(
        !timeout(
            Duration::from_secs(2),
            workers.drain(Duration::from_millis(50))
        )
        .await
        .unwrap()
        .unwrap()
    );
    for cancelled in cancellations {
        assert!(cancelled.await.is_err());
    }
    assert!(workers.join_next().await.is_none());
}

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn background_failure_is_supervised_and_reported_after_drain(pool: PgPool) {
    let f = Fixture::new(&pool).await;
    let mut workers = WorkerPool::start(&f.controller().await, 1).unwrap();
    workers.spawn_background(|_| async { panic!("injected worker failure") });
    assert!(
        timeout(Duration::from_secs(2), workers.join_next())
            .await
            .unwrap()
            .unwrap()
            .is_err()
    );
    workers.spawn_background(|_| async { panic!("injected drain failure") });
    assert!(
        timeout(
            Duration::from_secs(2),
            workers.drain(Duration::from_secs(1))
        )
        .await
        .unwrap()
        .is_err()
    );
    assert!(workers.join_next().await.is_none());
}

#[cfg(unix)]
#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn service_binary_drains_on_sigterm_and_sigint(pool: PgPool) {
    use std::process::Stdio;
    use tokio::{
        io::{AsyncBufReadExt, BufReader},
        process::Command,
    };

    let f = Fixture::new(&pool).await;
    let directory = tempfile::tempdir().unwrap();
    let ca = directory.path().join("ca.pem");
    let cert = directory.path().join("client.pem");
    let key = directory.path().join("client.key");
    std::fs::write(&ca, &f.ca).unwrap();
    std::fs::write(&cert, &f.cert).unwrap();
    std::fs::write(&key, &f.key).unwrap();
    // Reuse this test's isolated database, not the repository's shared database.
    use sqlx::ConnectOptions;
    let database_url = pool.connect_options().to_url_lossy().to_string();
    for signal in ["-TERM", "-INT"] {
        let mut child = Command::new(env!("CARGO_BIN_EXE_sandbox-controller"))
            .env("DATABASE_URL", &database_url)
            .arg("--endpoint")
            .arg(&f.config.endpoint)
            .arg("--host-id")
            .arg(f.config.host.to_string())
            .args(["--host-epoch", "1", "--workers", "2"])
            .arg("--ca-cert")
            .arg(&ca)
            .arg("--client-cert")
            .arg(&cert)
            .arg("--client-key")
            .arg(&key)
            .arg("--image-digest")
            .arg(f.config.allowed_images.first().unwrap())
            .args(["--allow-simulated", "--retire-history", "--archive-output"])
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .unwrap();
        let mut lines = BufReader::new(child.stderr.take().unwrap()).lines();
        timeout(Duration::from_secs(10), async {
            loop {
                let line = lines
                    .next_line()
                    .await
                    .unwrap()
                    .expect("controller exited before ready");
                if line == "sandbox controller: workers ready" {
                    break;
                }
            }
        })
        .await
        .unwrap();
        assert!(
            Command::new("kill")
                .arg(signal)
                .arg(child.id().unwrap().to_string())
                .status()
                .await
                .unwrap()
                .success()
        );
        assert!(
            timeout(Duration::from_secs(5), child.wait())
                .await
                .unwrap()
                .unwrap()
                .success()
        );
        let mut drained = false;
        while let Some(line) = lines.next_line().await.unwrap() {
            drained |= line == "sandbox controller: stopping new ticks and draining workers";
        }
        assert!(drained, "{signal} bypassed the controller drain");
    }
}
