//! Portable PostgreSQL/simulated-host qualification, never Linux/KVM evidence.
#![allow(clippy::unwrap_used, clippy::expect_used)]
#[allow(dead_code)]
mod common;
use common::Fixture;
use sandbox_controller::Tick;
use sandbox_protocol::{Id, supervisor::supervisor_server::Supervisor};
use sandbox_store::{claims::OperationKind, dispatch::DispatchError};
use serde_json::Value;
use sqlx::PgPool;
use tonic::Request;

async fn stale(f: &Fixture) {
    sqlx::query(
        "UPDATE hosts SET last_seen_at=clock_timestamp()-interval '31 seconds' WHERE id=$1",
    )
    .bind(f.config.host.uuid())
    .execute(f.store.pool())
    .await
    .unwrap();
}
async fn allocations(pool: &PgPool) -> Value {
    sqlx::query_scalar("SELECT jsonb_agg(to_jsonb(a) ORDER BY id) FROM allocations a")
        .fetch_one(pool)
        .await
        .unwrap()
}
async fn operations(pool: &PgPool) -> Value {
    sqlx::query_scalar("SELECT jsonb_agg(to_jsonb(o) ORDER BY id) FROM operations o")
        .fetch_one(pool)
        .await
        .unwrap()
}

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn partitioned_host_keeps_reservations_while_other_host_stays_running(pool: PgPool) {
    let lost = Fixture::new(&pool).await;
    let (_, sandbox) = lost.admit().await;
    assert_eq!(
        lost.controller().await.tick().await.unwrap(),
        Tick::Confirmed
    );
    let healthy = Fixture::new(&pool).await;
    healthy.admit().await;
    assert_eq!(
        healthy.controller().await.tick().await.unwrap(),
        Tick::Confirmed
    );
    stale(&lost).await;
    let before = allocations(&pool).await;
    let ops = operations(&pool).await;
    // A process restart loses no durable ownership. Concurrent replicas may
    // divide the batch or skip the same locked row; only one changes it.
    let restarted = sandbox_store::Store::from_pool(pool.clone());
    let (a, b) = tokio::join!(
        lost.store.reconcile_unavailable_hosts(),
        restarted.reconcile_unavailable_hosts()
    );
    assert_eq!(a.unwrap() + b.unwrap(), 1);
    assert_eq!(allocations(&pool).await, before);
    assert_eq!(operations(&pool).await, ops);
    let states: Vec<(uuid::Uuid, String)> =
        sqlx::query_as("SELECT id,observed_state FROM sandboxes")
            .fetch_all(&pool)
            .await
            .unwrap();
    for (id, state) in states {
        assert_eq!(
            state,
            if id == sandbox.uuid() {
                "unknown"
            } else {
                "running"
            }
        );
    }
    // Heartbeat freshness alone cannot recover an allocation's runtime state.
    lost.store
        .observe_configured_host(lost.config.host, 1)
        .await
        .unwrap();
    assert_eq!(restarted.reconcile_unavailable_hosts().await.unwrap(), 0);
    let state: String = sqlx::query_scalar("SELECT observed_state FROM sandboxes WHERE id=$1")
        .bind(sandbox.uuid())
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(state, "unknown");
    assert_eq!(lost.fake.total_starts().await, 1);
}

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn returning_host_reconciles_original_create_and_rejects_late_claim_reply(pool: PgPool) {
    let f = Fixture::new(&pool).await;
    f.admit().await;
    let (old, request) = f.prepared().await;
    let reply = f
        .fake
        .create(Request::new(request))
        .await
        .unwrap()
        .into_inner();
    // Start happened but the controller lost its reply and then restarted.
    f.store.record_owner_unavailable(&old).await.unwrap();
    stale(&f).await;
    assert_eq!(f.store.reconcile_unavailable_hosts().await.unwrap(), 1);
    let before = allocations(&pool).await;
    f.reclaim_now().await;
    let new = f
        .store
        .claim_next(OperationKind::Create, 30)
        .await
        .unwrap()
        .unwrap();
    assert!(new.revision > old.revision);
    assert_eq!(new.previous_status, "unknown");
    assert!(matches!(
        f.store.record_create_observation(&old, &reply, true).await,
        Err(DispatchError::LostClaim)
    ));
    assert!(matches!(
        f.store.record_owner_unavailable(&old).await,
        Err(DispatchError::LostClaim)
    ));
    assert_eq!(allocations(&pool).await, before);
    f.reclaim_now().await;
    assert_eq!(f.controller().await.tick().await.unwrap(), Tick::Confirmed);
    assert_eq!(f.fake.total_starts().await, 1);
    let (generation, count, held): (i64, i64, i64) = sqlx::query_as(
        "SELECT max(generation),count(*),count(*) FILTER (WHERE released_at IS NULL) FROM allocations")
        .fetch_one(&pool).await.unwrap();
    assert_eq!((generation, count, held), (1, 1, 1));
}

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn epoch_change_is_unknown_without_fencing_or_checkpoint_recovery(pool: PgPool) {
    let f = Fixture::new(&pool).await;
    f.admit().await;
    let (claim, request) = f.prepared().await;
    let reply = f
        .fake
        .create(Request::new(request))
        .await
        .unwrap()
        .into_inner();
    let before = allocations(&pool).await;
    // An externally advanced epoch with even a fresh heartbeat is not a stop.
    sqlx::query("UPDATE hosts SET supervisor_epoch=2,last_seen_at=clock_timestamp()")
        .execute(&pool)
        .await
        .unwrap();
    assert_eq!(f.store.reconcile_unavailable_hosts().await.unwrap(), 1);
    assert!(matches!(
        f.store
            .record_create_observation(&claim, &reply, true)
            .await,
        Err(DispatchError::Conflict)
    ));
    assert_eq!(allocations(&pool).await, before);
    assert_eq!(f.fake.total_starts().await, 1);
    let (state, generation): (String, i64) =
        sqlx::query_as("SELECT observed_state,generation FROM sandboxes")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!((state.as_str(), generation), ("unknown", 1));
}

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn unavailable_owner_preserves_dispatch_boundary_and_does_not_invent_attempts(pool: PgPool) {
    let f = Fixture::new(&pool).await;
    f.admit().await;
    let claim = f
        .store
        .claim_next(OperationKind::Create, 30)
        .await
        .unwrap()
        .unwrap();
    f.store.record_owner_unavailable(&claim).await.unwrap();
    let (status, attempts): (String, i32) =
        sqlx::query_as("SELECT status,attempt_count FROM operations")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!((status.as_str(), attempts), ("running", 0));
    f.store
        .observe_configured_host(f.config.host, 1)
        .await
        .unwrap();
    f.store
        .reserve_create(&claim, f.config.host, 1)
        .await
        .unwrap();
    f.store
        .prepare_create_dispatch(&claim, &f.config.allowed_images)
        .await
        .unwrap();
    let (phase, receipts): (String, Value) =
        sqlx::query_as("SELECT phase,attempt_receipts FROM operations")
            .fetch_one(&pool)
            .await
            .unwrap();
    f.store.record_owner_unavailable(&claim).await.unwrap();
    let (status, after_phase, after_receipts, attempts): (String, String, Value, i32) =
        sqlx::query_as("SELECT status,phase,attempt_receipts,attempt_count FROM operations")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(status, "unknown");
    assert_eq!(after_phase, phase);
    assert_eq!(after_receipts, receipts);
    assert_eq!(attempts, 1);
    assert_eq!(f.fake.total_starts().await, 0);
}

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn stale_scan_skips_busy_sandboxes_and_revisits_without_overwriting_release(pool: PgPool) {
    let f = Fixture::new(&pool).await;
    let (_, sandbox) = f.admit().await;
    assert_eq!(f.controller().await.tick().await.unwrap(), Tick::Confirmed);
    stale(&f).await;
    let mut tx = pool.begin().await.unwrap();
    sqlx::query("SELECT id FROM sandboxes WHERE id=$1 FOR UPDATE")
        .bind(sandbox.uuid())
        .execute(&mut *tx)
        .await
        .unwrap();
    assert_eq!(f.store.reconcile_unavailable_hosts().await.unwrap(), 0);
    tx.rollback().await.unwrap();
    assert_eq!(f.store.reconcile_unavailable_hosts().await.unwrap(), 1);
    assert_eq!(f.store.reconcile_unavailable_hosts().await.unwrap(), 0);
    let held: i64 =
        sqlx::query_scalar("SELECT count(*) FROM allocations WHERE released_at IS NULL")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(held, 1);
}

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn uncertain_external_command_is_inspected_after_restart_never_replayed(pool: PgPool) {
    use sandbox_protocol::supervisor::{CommandInspection, CommandRequest};
    use sandbox_store::execute::ExecuteAction;
    let f = Fixture::new(&pool).await;
    let (_, sandbox) = f.admit().await;
    assert_eq!(f.controller().await.tick().await.unwrap(), Tick::Confirmed);
    let (status, _) = f
        .send(
            "POST",
            &format!("/v1/sandboxes/{sandbox}/execute"),
            serde_json::json!({"argv":["/bin/busybox","echo","external-effect"],
            "deadline_unix_ms":sandbox_fake_host::unix_ms().unwrap()+60_000}),
        )
        .await;
    assert_eq!(status, http::StatusCode::ACCEPTED);
    let old = f
        .store
        .claim_next(OperationKind::Execute, 30)
        .await
        .unwrap()
        .unwrap();
    let ExecuteAction::Dispatch { owner, command } = f
        .store
        .prepare_execute(&old, f.config.host, 1)
        .await
        .unwrap()
    else {
        panic!("initial dispatch");
    };
    let reply = f
        .fake
        .execute_command(Request::new(CommandRequest {
            ownership: Some(owner),
            command: Some((&command).into()),
        }))
        .await
        .unwrap()
        .into_inner();
    f.store.record_owner_unavailable(&old).await.unwrap();
    stale(&f).await;
    f.store.reconcile_unavailable_hosts().await.unwrap();
    let before = allocations(&pool).await;
    f.reclaim_now().await;
    let new = f
        .store
        .claim_next(OperationKind::Execute, 30)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(new.previous_status, "unknown");
    assert!(matches!(
        f.store.record_execute_observation(&old, &reply, true).await,
        Err(DispatchError::LostClaim)
    ));
    // Inspection remains permitted even while placement freshness is absent.
    let ExecuteAction::Inspect { owner, digest } = f
        .store
        .prepare_execute(&new, f.config.host, 1)
        .await
        .unwrap()
    else {
        panic!("uncertain effects must never dispatch again");
    };
    let inspected = f
        .fake
        .inspect_command(Request::new(CommandInspection {
            ownership: Some(owner),
            command_digest: digest.to_vec(),
        }))
        .await
        .unwrap()
        .into_inner();
    f.store
        .record_execute_observation(&new, &inspected, true)
        .await
        .unwrap();
    let (state, attempts): (String, i32) =
        sqlx::query_as("SELECT status,attempt_count FROM operations WHERE kind='execute'")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!((state.as_str(), attempts), ("succeeded", 1));
    assert_eq!(f.fake.total_commands().await, 1);
    assert_eq!(allocations(&pool).await, before);
}

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn failed_health_immediately_closes_placement_and_keeps_create_reservation(pool: PgPool) {
    use sandbox_store::placement::{PlacementError, Reservation};
    let f = Fixture::new(&pool).await;
    f.admit().await;
    let (claim, _) = f.prepared().await;
    let before = allocations(&pool).await;
    f.store
        .mark_host_runtime_unknown(f.config.host, 1)
        .await
        .unwrap();
    let fresh: bool = sqlx::query_scalar("SELECT last_seen_at IS NOT NULL FROM hosts")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert!(!fresh);
    let Reservation::Existing(original) = f
        .store
        .reserve_create(&claim, f.config.host, 1)
        .await
        .unwrap()
    else {
        panic!("retained owner");
    };
    assert_eq!(original.generation, 1);
    assert_eq!(allocations(&pool).await, before);
    // A second admission cannot spend the apparently idle host's capacity.
    f.admit().await;
    let next = f
        .store
        .claim_next(OperationKind::Create, 30)
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        f.store.reserve_create(&next, f.config.host, 1).await,
        Err(PlacementError::HostUnavailable)
    ));
    assert_eq!(allocations(&pool).await, before);
}
