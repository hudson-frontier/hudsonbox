//! Create lifecycle through PostgreSQL, the HTTP router, and gRPC/mTLS.
#![allow(clippy::unwrap_used, clippy::expect_used)]
mod common;
use common::Fixture;
use http::StatusCode;
use sandbox_controller::{Controller, ControllerError, Tick};
use sandbox_protocol::{
    HostId, Id, OperationId, ProjectId, SandboxId,
    supervisor::{StopRequest, supervisor_server::Supervisor},
};
use sandbox_store::{
    claims::OperationKind,
    dispatch::{CreateAction, CreateRejection, DispatchError},
};
use serde_json::Value;
use sqlx::PgPool;
use std::collections::BTreeSet;

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn admitted_http_create_reaches_confirmed_simulated_state(pool: PgPool) {
    let f = Fixture::new(&pool).await;
    let (operation, sandbox) = f.admit().await;
    let mut controller = f.controller().await;
    assert_eq!(controller.tick().await.unwrap(), Tick::Confirmed);
    let (status, body) = f
        .send("GET", &format!("/v1/operations/{operation}"), Value::Null)
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["status"], "succeeded");
    assert_eq!(body["result"]["simulated"], true);
    assert!(body["completed_at"].is_string());
    let (_, body) = f
        .send("GET", &format!("/v1/sandboxes/{sandbox}"), Value::Null)
        .await;
    assert_eq!(body["observed_state"], "running");
    assert_eq!(body["observation_simulated"], true);
    assert!(body["observed_at"].is_string());
    assert!(body.get("active_operation_id").is_none());
    assert_eq!(controller.tick().await.unwrap(), Tick::Idle);
    assert_eq!(f.fake.total_starts().await, 1);
}

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn lost_rpc_reply_is_reconciled_from_the_same_single_start(pool: PgPool) {
    let f = Fixture::new(&pool).await;
    let (operation, _) = f.admit().await;
    let mut controller = f.controller().await;
    f.fake.lose_next_create_reply().await;
    assert_eq!(controller.tick().await.unwrap(), Tick::Unknown);
    let (_, body) = f
        .send("GET", &format!("/v1/operations/{operation}"), Value::Null)
        .await;
    assert_eq!(body["status"], "unknown");
    assert!(body.get("completed_at").is_none());
    let (count,): (i64,) =
        sqlx::query_as("SELECT count(*) FROM allocations WHERE released_at IS NULL")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(count, 1);
    // Revocation prevents new dispatch, but cannot erase already-applied work.
    sqlx::query("UPDATE projects SET api_tokens='[]'")
        .execute(&pool)
        .await
        .unwrap();
    f.reclaim_now().await;
    assert_eq!(f.controller().await.tick().await.unwrap(), Tick::Confirmed);
    assert_eq!(f.fake.total_starts().await, 1);
    let (attempts, status, error): (i32, String, Option<Value>) =
        sqlx::query_as("SELECT attempt_count,status,error FROM operations")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(attempts, 1);
    assert_eq!(status, "succeeded");
    assert!(error.is_none());
}

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn initial_create_lease_allows_sixty_seconds_for_guest_boot(pool: PgPool) {
    let f = Fixture::new(&pool).await;
    f.admit().await;
    f.store
        .observe_configured_host(f.config.host, 1)
        .await
        .unwrap();
    let claim = f
        .store
        .claim_next(OperationKind::Create, 30)
        .await
        .unwrap()
        .unwrap();
    f.store
        .reserve_create(&claim, f.config.host, 1)
        .await
        .unwrap();
    let CreateAction::Start(request) = f
        .store
        .prepare_create_dispatch(&claim, &f.config.allowed_images)
        .await
        .unwrap()
    else {
        panic!("start")
    };
    let now_ms: i64 =
        sqlx::query_scalar("SELECT floor(extract(epoch FROM clock_timestamp())*1000)::bigint")
            .fetch_one(&pool)
            .await
            .unwrap();
    let remaining_ms = request.allocation_expires_unix_ms - now_ms;
    assert!(
        (55_000..=60_000).contains(&remaining_ms),
        "initial allocation lease should be close to 60 seconds, got {remaining_ms}ms"
    );
}

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn crash_after_intent_before_send_remains_unknown_without_replay(pool: PgPool) {
    let f = Fixture::new(&pool).await;
    f.admit().await;
    f.prepared().await;
    f.reclaim_now().await;
    let mut controller = f.controller().await;
    for _ in 0..2 {
        assert_eq!(controller.tick().await.unwrap(), Tick::Unknown);
        f.reclaim_now().await;
    }
    assert_eq!(f.fake.total_starts().await, 0);
    let (attempts, receipts): (i32, Value) =
        sqlx::query_as("SELECT attempt_count,attempt_receipts FROM operations")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(attempts, 1);
    assert_eq!(receipts.as_array().unwrap().len(), 2);
    assert_eq!(receipts[1]["phase"], "create_dispatch_intent");
}

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn crash_before_intent_can_continue_from_proven_undispatched_reservation(pool: PgPool) {
    let f = Fixture::new(&pool).await;
    f.admit().await;
    let mut controller = f.controller().await;
    let old = f
        .store
        .claim_next(OperationKind::Create, 30)
        .await
        .unwrap()
        .unwrap();
    f.store
        .reserve_create(&old, f.config.host, 1)
        .await
        .unwrap();
    f.reclaim_now().await;
    assert_eq!(controller.tick().await.unwrap(), Tick::Confirmed);
    assert_eq!(f.fake.total_starts().await, 1);
}

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn multiple_controllers_cannot_dispatch_the_same_create(pool: PgPool) {
    let f = Fixture::new(&pool).await;
    f.admit().await;
    let mut tasks = tokio::task::JoinSet::new();
    for _ in 0..8 {
        let mut controller = f.controller().await;
        tasks.spawn(async move { controller.tick().await.unwrap() });
    }
    let mut confirmed = 0;
    while let Some(result) = tasks.join_next().await {
        if result.unwrap() == Tick::Confirmed {
            confirmed += 1;
        }
    }
    assert_eq!(confirmed, 1);
    assert_eq!(f.fake.total_starts().await, 1);
}

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn simulated_host_requires_explicit_opt_in_before_claiming(pool: PgPool) {
    let mut f = Fixture::new(&pool).await;
    f.admit().await;
    f.config.allow_simulated = false;
    assert!(matches!(
        Controller::connect(
            f.store.clone(),
            f.config.clone(),
            f.ca.as_bytes(),
            f.cert.as_bytes(),
            f.key.as_bytes()
        )
        .await,
        Err(ControllerError::HostIdentity)
    ));
    let (status,): (String,) = sqlx::query_as("SELECT status FROM operations")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(status, "queued");
    assert_eq!(f.fake.total_starts().await, 0);
}

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn denied_images_release_only_undispatched_reservations(pool: PgPool) {
    let mut f = Fixture::new(&pool).await;
    f.admit().await;
    f.config.allowed_images = BTreeSet::from([format!("sha256:{}", "b".repeat(64))]);
    assert_eq!(f.controller().await.tick().await.unwrap(), Tick::Rejected);
    assert_eq!(f.fake.total_starts().await, 0);
    let (status, attempts): (String, i32) =
        sqlx::query_as("SELECT status,attempt_count FROM operations")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(status, "failed");
    assert_eq!(attempts, 0);
    let (proof,): (Value,) =
        sqlx::query_as("SELECT release_evidence FROM allocations WHERE released_at IS NOT NULL")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(proof["dispatch_intent_absent"], true);
}

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn revoked_project_cannot_dispatch_or_leave_an_undispatched_allocation(pool: PgPool) {
    let f = Fixture::new(&pool).await;
    f.admit().await;
    sqlx::query("UPDATE projects SET api_tokens='[]'")
        .execute(&pool)
        .await
        .unwrap();
    assert_eq!(f.controller().await.tick().await.unwrap(), Tick::Rejected);
    assert_eq!(f.fake.total_starts().await, 0);
    let (status,): (String,) = sqlx::query_as("SELECT status FROM operations")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(status, "failed");
    let (count,): (i64,) = sqlx::query_as("SELECT count(*) FROM allocations")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(count, 0);
}

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn mismatched_or_stale_evidence_cannot_commit_success(pool: PgPool) {
    let f = Fixture::new(&pool).await;
    f.admit().await;
    let (claim, request) = f.prepared().await;
    let observation = f
        .fake
        .create(tonic::Request::new(request))
        .await
        .unwrap()
        .into_inner();
    for field in [
        "project",
        "sandbox",
        "host",
        "allocation",
        "operation",
        "epoch",
        "generation",
        "revision",
        "deadline",
        "create_operation",
    ] {
        let mut wrong = observation.clone();
        let owner = wrong.ownership.as_mut().unwrap();
        match field {
            "project" => owner.project_id = ProjectId::generate().to_string(),
            "sandbox" => owner.sandbox_id = SandboxId::generate().to_string(),
            "host" => owner.host_id = HostId::generate().to_string(),
            "allocation" => owner.allocation_id = "wrong".into(),
            "operation" => owner.operation_id = OperationId::generate().to_string(),
            "epoch" => owner.supervisor_epoch += 1,
            "generation" => owner.generation += 1,
            "revision" => owner.claim_revision += 1,
            "deadline" => owner.claim_expires_unix_ms += 1,
            _ => wrong.create_operation_id = OperationId::generate().to_string(),
        }
        assert!(
            matches!(
                f.store
                    .record_create_observation(&claim, &wrong, true)
                    .await,
                Err(DispatchError::BadEvidence)
            ),
            "{field}"
        );
    }
    assert!(matches!(
        f.store
            .record_create_observation(&claim, &observation, false)
            .await,
        Err(DispatchError::SimulationDenied)
    ));
    f.reclaim_now().await;
    f.store
        .claim_next(OperationKind::Create, 30)
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        f.store
            .record_create_observation(&claim, &observation, true)
            .await,
        Err(DispatchError::LostClaim)
    ));
    assert!(
        f.store
            .reject_undispatched_create(&claim, CreateRejection::Unauthorized)
            .await
            .is_err()
    );
    let (status,): (String,) = sqlx::query_as("SELECT status FROM operations")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(status, "running");
}

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn expired_readiness_is_not_release_but_matching_stop_evidence_is(pool: PgPool) {
    let f = Fixture::new(&pool).await;
    f.admit().await;
    let (claim, request) = f.prepared().await;
    let ownership = request.ownership.clone();
    let observation = f
        .fake
        .create(tonic::Request::new(request))
        .await
        .unwrap()
        .into_inner();
    sqlx::query("UPDATE allocations SET lease_expires_at=clock_timestamp()-interval '1 second'")
        .execute(&pool)
        .await
        .unwrap();
    assert!(matches!(
        f.store
            .record_create_observation(&claim, &observation, true)
            .await,
        Err(DispatchError::BadEvidence)
    ));
    assert!(
        f.store
            .reject_undispatched_create(&claim, CreateRejection::Unauthorized)
            .await
            .is_err()
    );
    let stopped = f
        .fake
        .stop(tonic::Request::new(StopRequest { ownership }))
        .await
        .unwrap()
        .into_inner();
    f.store
        .record_create_observation(&claim, &stopped, true)
        .await
        .unwrap();
    let (status,): (String,) = sqlx::query_as("SELECT status FROM operations")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(status, "failed");
    let (count,): (i64,) =
        sqlx::query_as("SELECT count(*) FROM allocations WHERE released_at IS NULL")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(count, 0);
}

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn authority_is_rechecked_after_reservation_and_before_rpc(pool: PgPool) {
    let f = Fixture::new(&pool).await;
    f.admit().await;
    let mut controller = f.controller().await;
    let claim = f
        .store
        .claim_next(OperationKind::Create, 30)
        .await
        .unwrap()
        .unwrap();
    f.store
        .reserve_create(&claim, f.config.host, 1)
        .await
        .unwrap();
    sqlx::query("UPDATE projects SET status='suspended'")
        .execute(&pool)
        .await
        .unwrap();
    f.reclaim_now().await;
    assert_eq!(controller.tick().await.unwrap(), Tick::Rejected);
    assert_eq!(f.fake.total_starts().await, 0);
    let (count,): (i64,) =
        sqlx::query_as("SELECT count(*) FROM allocations WHERE released_at IS NULL")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(count, 0);
}

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn claim_expiry_during_completion_lock_wait_rolls_back_every_write(pool: PgPool) {
    let f = Fixture::new(&pool).await;
    f.admit().await;
    f.store
        .observe_configured_host(f.config.host, 1)
        .await
        .unwrap();
    let claim = f
        .store
        .claim_next(OperationKind::Create, 1)
        .await
        .unwrap()
        .unwrap();
    f.store
        .reserve_create(&claim, f.config.host, 1)
        .await
        .unwrap();
    let CreateAction::Start(request) = f
        .store
        .prepare_create_dispatch(&claim, &f.config.allowed_images)
        .await
        .unwrap()
    else {
        panic!("start")
    };
    let observation = f
        .fake
        .create(tonic::Request::new(request))
        .await
        .unwrap()
        .into_inner();
    let mut lock = pool.begin().await.unwrap();
    sqlx::query("SELECT id FROM projects FOR UPDATE")
        .execute(&mut *lock)
        .await
        .unwrap();
    let store = f.store.clone();
    let task = tokio::spawn(async move {
        store
            .record_create_observation(&claim, &observation, true)
            .await
    });
    tokio::time::sleep(std::time::Duration::from_millis(1200)).await;
    lock.commit().await.unwrap();
    assert!(matches!(task.await.unwrap(), Err(DispatchError::LostClaim)));
    let (state, source): (String, Option<bool>) =
        sqlx::query_as("SELECT observed_state,observation_simulated FROM sandboxes")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(state, "creating");
    assert_eq!(source, None);
    let (status,): (String,) = sqlx::query_as("SELECT status FROM operations")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(status, "running");
}

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn stale_or_future_observation_time_cannot_confirm_readiness(pool: PgPool) {
    let f = Fixture::new(&pool).await;
    f.admit().await;
    let (claim, request) = f.prepared().await;
    let original = f
        .fake
        .create(tonic::Request::new(request))
        .await
        .unwrap()
        .into_inner();
    for at in [1, i64::MAX] {
        let mut observation = original.clone();
        observation.observed_unix_ms = at;
        assert!(matches!(
            f.store
                .record_create_observation(&claim, &observation, true)
                .await,
            Err(DispatchError::BadEvidence)
        ));
    }
    f.store
        .record_create_observation(&claim, &original, true)
        .await
        .unwrap();
}

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn host_health_cannot_change_epoch_or_promote_a_draining_host(pool: PgPool) {
    let mut f = Fixture::new(&pool).await;
    f.admit().await;
    f.config.epoch = 2;
    assert!(matches!(
        Controller::connect(
            f.store.clone(),
            f.config.clone(),
            f.ca.as_bytes(),
            f.cert.as_bytes(),
            f.key.as_bytes()
        )
        .await,
        Err(ControllerError::HostIdentity)
    ));
    f.config.epoch = 1;
    sqlx::query("UPDATE hosts SET status='draining'")
        .execute(&pool)
        .await
        .unwrap();
    assert_eq!(f.controller().await.tick().await.unwrap(), Tick::Deferred);
    let (status, epoch): (String, i64) =
        sqlx::query_as("SELECT status,supervisor_epoch FROM hosts")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(status, "draining");
    assert_eq!(epoch, 1);
    assert_eq!(f.fake.total_starts().await, 0);
}

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn legacy_reserved_size_is_rejected_before_first_dispatch(pool: PgPool) {
    let f = Fixture::new(&pool).await;
    f.admit().await;
    let claim = f
        .store
        .claim_next(OperationKind::Create, 30)
        .await
        .unwrap()
        .unwrap();
    f.store
        .observe_configured_host(f.config.host, 1)
        .await
        .unwrap();
    f.store
        .reserve_create(&claim, f.config.host, 1)
        .await
        .unwrap();
    // This reserved row predates the shared host minimum; no intent was committed.
    sqlx::query("UPDATE allocations SET memory_mib=127")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("UPDATE sandboxes SET resources=jsonb_set(resources,'{memory_mib}','127')")
        .execute(&pool)
        .await
        .unwrap();
    f.reclaim_now().await;
    assert_eq!(f.controller().await.tick().await.unwrap(), Tick::Rejected);
    assert_eq!(f.fake.total_starts().await, 0);
    let (status, attempts, code): (String, i32, String) =
        sqlx::query_as("SELECT status,attempt_count,error->>'code' FROM operations")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(
        (status.as_str(), attempts, code.as_str()),
        ("failed", 0, "invalid_resources")
    );
    let (released,):(bool,)=sqlx::query_as("SELECT released_at IS NOT NULL AND release_evidence->>'dispatch_intent_absent'='true' FROM allocations").fetch_one(&pool).await.unwrap();
    assert!(released);
}

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn sizing_changes_never_replay_or_reject_a_dispatched_incarnation(pool: PgPool) {
    let f = Fixture::new(&pool).await;
    f.admit().await;
    let (_, request) = f.prepared().await;
    f.fake.create(tonic::Request::new(request)).await.unwrap();
    // Retained sizing now falls below current policy, but its recorded dispatch
    // still requires inspection of the original incarnation, not a new admission.
    sqlx::query("UPDATE allocations SET memory_mib=127")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("UPDATE sandboxes SET resources=jsonb_set(resources,'{memory_mib}','127')")
        .execute(&pool)
        .await
        .unwrap();
    f.reclaim_now().await;
    assert_eq!(f.controller().await.tick().await.unwrap(), Tick::Confirmed);
    assert_eq!(f.fake.total_starts().await, 1);
    let (attempts, status): (i32, String) =
        sqlx::query_as("SELECT attempt_count,status FROM operations")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!((attempts, status.as_str()), (1, "succeeded"));
}

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn project_without_pool_grant_fails_before_any_reservation(pool: PgPool) {
    let f = Fixture::new(&pool).await;
    let (operation, _) = f.admit().await;
    sqlx::query("DELETE FROM project_host_pool_access")
        .execute(&pool)
        .await
        .unwrap();
    assert_eq!(f.controller().await.tick().await.unwrap(), Tick::Rejected);
    assert_eq!(f.fake.total_starts().await, 0);
    let (_, body) = f
        .send("GET", &format!("/v1/operations/{operation}"), Value::Null)
        .await;
    assert_eq!(body["status"], "failed");
    assert_eq!(body["error"]["code"], "host_pool_access_denied");
    let (count,): (i64,) = sqlx::query_as("SELECT count(*) FROM allocations")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(count, 0);
}

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn full_host_keeps_create_queued_with_reason_then_fails_after_bound(pool: PgPool) {
    let f = Fixture::new(&pool).await;
    let (operation, _) = f.admit().await;
    // Room for the request but not its 128 MiB VMM overhead.
    sqlx::query("UPDATE hosts SET memory_capacity_mib=(SELECT (resources->>'memory_mib')::bigint FROM sandboxes)")
        .execute(&pool)
        .await
        .unwrap();
    let mut controller = f.controller().await;
    assert_eq!(controller.tick().await.unwrap(), Tick::Deferred);
    let (_, body) = f
        .send("GET", &format!("/v1/operations/{operation}"), Value::Null)
        .await;
    assert_eq!(body["status"], "running");
    assert_eq!(body["phase"], "awaiting_placement");
    assert_eq!(body["error"]["code"], "insufficient_capacity");
    sqlx::query("UPDATE operations SET next_retry_at=NULL,created_at=clock_timestamp()-interval '301 seconds'")
        .execute(&pool)
        .await
        .unwrap();
    assert_eq!(controller.tick().await.unwrap(), Tick::Rejected);
    let (_, body) = f
        .send("GET", &format!("/v1/operations/{operation}"), Value::Null)
        .await;
    assert_eq!(body["status"], "failed");
    assert_eq!(body["error"]["code"], "insufficient_capacity");
    assert_eq!(f.fake.total_starts().await, 0);
}
