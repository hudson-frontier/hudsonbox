//! Operator snapshots over PostgreSQL and two loopback mTLS fake supervisors.
//! No VM, physical fencing or production health evidence is implied.
#![allow(clippy::unwrap_used, clippy::expect_used)]
#[allow(dead_code)]
mod common;
use common::Fixture;
use sandbox_controller::Tick;
use sandbox_protocol::{HostId, Id, supervisor::supervisor_server::Supervisor};
use sandbox_store::{dispatch::DispatchError, fleet::prometheus};
use serde_json::{Value, json};
use sqlx::PgPool;

async fn host(f: &Fixture, id: HostId) -> Value {
    let mut value = f.store.fleet_status().await.unwrap()["hosts"]
        .as_array()
        .unwrap()
        .iter()
        .find(|h| h["host_id"] == id.uuid().to_string())
        .unwrap()
        .clone();
    value
        .as_object_mut()
        .unwrap()
        .remove("heartbeat_age_seconds");
    value
}
fn redacted(value: &Value) {
    match value {
        Value::Object(fields) => {
            for (key, value) in fields {
                assert!(
                    ![
                        "host_id",
                        "host_pool_id",
                        "supervisor_epoch",
                        "endpoint",
                        "transport_server_name",
                        "transport_certificate_sha256",
                        "provider_ref",
                        "credential_secret_ref",
                        "rpc_failures",
                        "rpc_last_failure_at"
                    ]
                    .contains(&key.as_str()),
                    "private field {key}"
                );
                redacted(value);
            }
        }
        Value::Array(values) => values.iter().for_each(redacted),
        _ => {}
    }
}

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn drain_lost_cleanup_reply_and_confirmed_absence_are_distinct(pool: PgPool) {
    let a = Fixture::new(&pool).await;
    let b = Fixture::new(&pool).await;
    let (_, other) = b.admit().await;
    assert_eq!(b.controller().await.tick().await.unwrap(), Tick::Confirmed);
    let untouched = host(&b, b.config.host).await;
    let (_, sandbox) = a.admit().await;
    let (claim, delayed_create) = a.prepared().await;
    a.store.record_create_unknown(&claim).await.unwrap();
    a.store.drain_host(a.config.host, 1).await.unwrap();
    let drained = host(&a, a.config.host).await;
    assert_eq!(drained["status"], "draining");
    assert_eq!(drained["active_allocations"], 1);
    assert_eq!(drained["confirmed_fenced_allocations"], 0);
    assert!(a.store.finish_host_drain(a.config.host, 1).await.is_err());

    let (status, destroy) = a
        .send(
            "POST",
            &format!("/v1/sandboxes/{sandbox}/destroy"),
            json!({}),
        )
        .await;
    assert_eq!(status, 202);
    a.fake.lose_next_stop_reply().await;
    let mut controller = a.controller().await;
    assert_eq!(controller.tick().await.unwrap(), Tick::Unknown);
    let pending = host(&a, a.config.host).await;
    assert_eq!(pending["pending_cleanup"], 1);
    assert_eq!(pending["active_allocations"], 1);
    assert_eq!(pending["reserved"], drained["reserved"]);
    assert_eq!(pending["confirmed_fenced_allocations"], 0);
    // An actual gRPC lost reply counts once; health success does not erase it.
    assert_eq!(pending["rpc_failures"]["unavailable"], 1);
    assert_eq!(host(&b, b.config.host).await, untouched);
    a.reclaim_now().await;
    assert_eq!(controller.tick().await.unwrap(), Tick::Confirmed);
    let released = host(&a, a.config.host).await;
    assert_eq!(released["pending_cleanup"], 0);
    assert_eq!(released["active_allocations"], 0);
    assert_eq!(released["confirmed_fenced_allocations"], 1);
    assert_eq!(released["rpc_failures"]["unavailable"], 1);
    assert_eq!(released["reserved"]["vcpu"], 0);
    assert_eq!(released["status"], "draining");
    assert!(
        a.fake
            .create(tonic::Request::new(delayed_create))
            .await
            .is_err()
    );
    assert_eq!(a.fake.total_starts().await, 0);
    assert_eq!(b.fake.total_starts().await, 1);
    a.store.finish_host_drain(a.config.host, 1).await.unwrap();
    assert_eq!(host(&a, a.config.host).await["status"], "ready");
    let metrics = prometheus(&a.store.fleet_status().await.unwrap());
    assert!(metrics.contains("hudsonbox_fleet_rpc_failures{reason=\"unavailable\"} 1\n"));
    for secret in [
        a.config.host.to_string(),
        b.config.host.to_string(),
        a.config.endpoint.clone(),
    ] {
        assert!(!metrics.contains(&secret));
    }
    let path = format!(
        "/v1/operations/{}",
        destroy["operation_id"].as_str().unwrap()
    );
    let (status, body) = a.send("GET", &path, Value::Null).await;
    assert_eq!(status, 200);
    redacted(&body);
    let (status, _) = b.send("GET", &path, Value::Null).await;
    assert_eq!(status, 404);
    let (status, body) = b
        .send("GET", &format!("/v1/sandboxes/{other}"), Value::Null)
        .await;
    assert_eq!(status, 200);
    redacted(&body);
}

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn unavailable_return_and_epoch_change_never_invent_fencing(pool: PgPool) {
    let a = Fixture::new(&pool).await;
    a.admit().await;
    assert_eq!(a.controller().await.tick().await.unwrap(), Tick::Confirmed);
    let b = Fixture::new(&pool).await;
    b.admit().await;
    assert_eq!(b.controller().await.tick().await.unwrap(), Tick::Confirmed);
    let initial = host(&a, a.config.host).await;
    let other = host(&b, b.config.host).await;
    sqlx::query(
        "UPDATE hosts SET last_seen_at=clock_timestamp()-interval '31 seconds' WHERE id=$1",
    )
    .bind(a.config.host.uuid())
    .execute(&pool)
    .await
    .unwrap();
    assert_eq!(a.store.reconcile_unavailable_hosts().await.unwrap(), 1);
    let stale = host(&a, a.config.host).await;
    assert_eq!(stale["fresh"], false);
    assert_eq!(stale["unknown_allocations"], 1);
    assert_eq!(stale["confirmed_fenced_allocations"], 0);
    assert_eq!(stale["reserved"], initial["reserved"]);
    assert_eq!(
        stale["rpc_failures"], initial["rpc_failures"],
        "staleness is not an RPC outcome"
    );
    assert_eq!(host(&b, b.config.host).await, other);
    a.store
        .observe_configured_host(a.config.host, 1)
        .await
        .unwrap();
    let returned = host(&a, a.config.host).await;
    assert_eq!(returned["fresh"], true);
    assert_eq!(
        returned["unknown_allocations"], 1,
        "health is not a runtime observation"
    );
    assert_eq!(returned["confirmed_fenced_allocations"], 0);
    sqlx::query("UPDATE hosts SET supervisor_epoch=2 WHERE id=$1")
        .bind(a.config.host.uuid())
        .execute(&pool)
        .await
        .unwrap();
    let advanced = host(&a, a.config.host).await;
    assert_eq!(advanced["previous_epoch_allocations"], 1);
    assert_eq!(advanced["reserved"], initial["reserved"]);
    assert_eq!(advanced["confirmed_fenced_allocations"], 0);
    assert!(matches!(
        a.store.observe_configured_host(a.config.host, 1).await,
        Err(DispatchError::HostUnavailable)
    ));
    assert_eq!(a.fake.total_starts().await, 1);
    assert_eq!(b.fake.total_starts().await, 1);
}

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn actual_failed_health_counts_without_fencing_or_cross_host_changes(pool: PgPool) {
    let a = Fixture::new(&pool).await;
    let mut controller = a.controller().await;
    let b = Fixture::new(&pool).await;
    b.controller().await;
    let other = host(&b, b.config.host).await;
    a.fake.fail_next_health_check().await;
    assert!(controller.tick().await.is_err());
    let failed = host(&a, a.config.host).await;
    assert_eq!(failed["fresh"], false);
    assert_eq!(failed["rpc_failures"]["unavailable"], 1);
    assert_eq!(failed["confirmed_fenced_allocations"], 0);
    assert_eq!(host(&b, b.config.host).await, other);
    // A new process authenticates the original host, never clearing the history.
    a.controller().await;
    let returned = host(&a, a.config.host).await;
    assert_eq!(returned["fresh"], true);
    assert_eq!(returned["rpc_failures"], failed["rpc_failures"]);
    for route in ["/v1/hosts", "/v1/fleet-status", "/v1/metrics"] {
        let (status, _) = a.send("GET", route, Value::Null).await;
        assert_eq!(status, 404);
    }
}
