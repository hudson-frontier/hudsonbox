//! Synthetic authenticated observations exercise DB fencing, not real isolation.
use super::*;
use sandbox_protocol::{
    guest_model::Context,
    history::Domain,
    supervisor::{HistoryBindingObservation, HistoryObservation},
};
use sandbox_store::history::{Claim, Error, Preparation};

pub(super) async fn finish(f: &Fixture, id: OperationId) {
    sqlx::query("UPDATE operations SET status='failed',phase='rejected_before_dispatch',completed_at=clock_timestamp(),error='{}',lease_expires_at=NULL,next_retry_at=NULL WHERE id=$1")
        .bind(id.uuid()).execute(f.store.pool()).await.unwrap();
}
async fn prepare(f: &Fixture) -> (Claim, sandbox_protocol::supervisor::HistoryRequest) {
    let Preparation::Binding { claim, request } = f
        .store
        .claim_history(f.host, 1, Domain::Commands, 30, false)
        .await
        .unwrap()
        .unwrap()
    else {
        panic!("expected binding")
    };
    let context = Context {
        allocation_id: f.allocation,
        generation: 1,
        boot_id: "original-boot".into(),
    };
    let request = f
        .store
        .bind_history(
            &claim,
            &HistoryBindingObservation {
                request: Some(request),
                context: Some((&context).into()),
                simulated: false,
                observed_unix_ms: now_ms(),
            },
            false,
        )
        .await
        .unwrap();
    (claim, request)
}
fn observation(request: sandbox_protocol::supervisor::HistoryRequest) -> HistoryObservation {
    HistoryObservation {
        completed: request.barrier.clone(),
        request: Some(request),
        simulated: false,
        observed_unix_ms: now_ms(),
    }
}
#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn history_requires_exact_ack_and_keeps_original_handles(pool: PgPool) {
    let f = Fixture::new(&pool).await;
    let id = f.admit().await;
    finish(&f, id).await;
    let (claim, request) = prepare(&f).await;
    let mut bad = observation(request.clone());
    bad.completed.as_mut().unwrap().through = OperationId::generate().to_string();
    assert!(matches!(
        f.store.complete_history(&claim, &bad, false).await,
        Err(Error::Evidence)
    ));
    let done: Option<uuid::Uuid> = sqlx::query_scalar(
        "SELECT completed_through FROM allocation_history WHERE allocation_id=$1",
    )
    .bind(f.allocation.uuid())
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(done, None);
    f.store
        .complete_history(&claim, &observation(request), false)
        .await
        .unwrap();
    assert_eq!(
        f.store.admit_execute(&f.request).await.unwrap(),
        ExecuteAdmission::Accepted {
            operation_id: id,
            status: "failed".into()
        }
    );
    let done: uuid::Uuid = sqlx::query_scalar(
        "SELECT completed_through FROM allocation_history WHERE allocation_id=$1",
    )
    .bind(f.allocation.uuid())
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(done, id.uuid());
}
#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn history_lost_reply_reclaims_same_prefix_and_rejects_old_claim(pool: PgPool) {
    let mut f = Fixture::new(&pool).await;
    let id = f.admit().await;
    finish(&f, id).await;
    let (old, request) = prepare(&f).await;
    sqlx::query("UPDATE allocation_history SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE allocation_id=$1").bind(f.allocation.uuid()).execute(&pool).await.unwrap();
    f.request.idempotency_key = key();
    let next = f.admit().await;
    finish(&f, next).await;
    let Preparation::Retire {
        claim,
        request: new,
    } = f
        .store
        .claim_history(f.host, 1, Domain::Commands, 30, false)
        .await
        .unwrap()
        .unwrap()
    else {
        panic!("expected retained context")
    };
    assert!(claim.revision > old.revision);
    assert_eq!(new.barrier, request.barrier);
    assert!(matches!(
        f.store
            .complete_history(&old, &observation(request), false)
            .await,
        Err(Error::LostClaim)
    ));
    f.store
        .complete_history(&claim, &observation(new), false)
        .await
        .unwrap();
    let Preparation::Retire { claim, request } = f
        .store
        .claim_history(f.host, 1, Domain::Commands, 30, false)
        .await
        .unwrap()
        .unwrap()
    else {
        panic!("next prefix")
    };
    assert_eq!(request.barrier.as_ref().unwrap().through, next.to_string());
    let earlier: uuid::Uuid = sqlx::query_scalar(
        "SELECT completed_through FROM completed_allocation_history WHERE allocation_id=$1",
    )
    .bind(f.allocation.uuid())
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(earlier, id.uuid());
    f.store
        .complete_history(&claim, &observation(request), false)
        .await
        .unwrap();
    let advanced: uuid::Uuid = sqlx::query_scalar(
        "SELECT completed_through FROM completed_allocation_history WHERE allocation_id=$1",
    )
    .bind(f.allocation.uuid())
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(advanced, next.uuid());
}
#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn history_unknown_predecessor_blocks_prefix(pool: PgPool) {
    let f = Fixture::new(&pool).await;
    let id = f.admit().await;
    assert!(
        f.store
            .claim_history(f.host, 1, Domain::Commands, 30, false)
            .await
            .unwrap()
            .is_none()
    );
    let c = f.claim().await;
    f.store.prepare_execute(&c, f.host, 1).await.unwrap();
    f.store.record_execute_unknown(&c).await.unwrap();
    assert!(
        f.store
            .claim_history(f.host, 1, Domain::Commands, 30, false)
            .await
            .unwrap()
            .is_none()
    );
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM allocation_history")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(count, 0);
    let status: String = sqlx::query_scalar("SELECT status FROM operations WHERE id=$1")
        .bind(id.uuid())
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(status, "unknown");
}
#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn history_reserved_floor_and_repeated_clock_rollback_never_reuse_ids(pool: PgPool) {
    let mut f = Fixture::new(&pool).await;
    let future: uuid::Uuid = "ffffffff-ff00-7000-8000-000000000000".parse().unwrap();
    sqlx::query("INSERT INTO allocation_history(allocation_id,domain,reserved_through) VALUES($1,'files',$2)").bind(f.allocation.uuid()).bind(future).execute(&pool).await.unwrap();
    let first = f.admit().await;
    assert!(first.uuid() > future);
    finish(&f, first).await;
    f.request.idempotency_key = key();
    let second = f.admit().await;
    assert!(second > first);
    let retained: uuid::Uuid =
        sqlx::query_scalar("SELECT last_admitted_operation_id FROM allocations WHERE id=$1")
            .bind(f.allocation.uuid())
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(retained, second.uuid());
}
#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn history_boot_binding_cannot_change_or_accept_simulation(pool: PgPool) {
    let f = Fixture::new(&pool).await;
    let id = f.admit().await;
    finish(&f, id).await;
    let Preparation::Binding { claim, request } = f
        .store
        .claim_history(f.host, 1, Domain::Commands, 30, false)
        .await
        .unwrap()
        .unwrap()
    else {
        panic!("expected binding")
    };
    let context = Context {
        allocation_id: f.allocation,
        generation: 1,
        boot_id: "original".into(),
    };
    let mut reply = HistoryBindingObservation {
        request: Some(request),
        context: Some((&context).into()),
        simulated: true,
        observed_unix_ms: now_ms(),
    };
    assert!(matches!(
        f.store.bind_history(&claim, &reply, false).await,
        Err(Error::Evidence)
    ));
    reply.simulated = false;
    f.store.bind_history(&claim, &reply, false).await.unwrap();
    reply.context.as_mut().unwrap().boot_id = "replacement".into();
    assert!(matches!(
        f.store.bind_history(&claim, &reply, false).await,
        Err(Error::Evidence)
    ));
}

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn history_completion_rechecks_outcome_and_current_epoch(pool: PgPool) {
    let f = Fixture::new(&pool).await;
    let id = f.admit().await;
    finish(&f, id).await;
    let (claim, request) = prepare(&f).await;
    sqlx::query("UPDATE operations SET phase='exited' WHERE id=$1")
        .bind(id.uuid())
        .execute(&pool)
        .await
        .unwrap();
    assert!(matches!(
        f.store
            .complete_history(&claim, &observation(request.clone()), false)
            .await,
        Err(Error::Evidence)
    ));
    sqlx::query("UPDATE operations SET phase='rejected_before_dispatch' WHERE id=$1")
        .bind(id.uuid())
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("UPDATE hosts SET supervisor_epoch=2 WHERE id=$1")
        .bind(f.host.uuid())
        .execute(&pool)
        .await
        .unwrap();
    assert!(matches!(
        f.store
            .complete_history(&claim, &observation(request), false)
            .await,
        Err(Error::LostClaim)
    ));
    let done: Option<uuid::Uuid> = sqlx::query_scalar(
        "SELECT completed_through FROM allocation_history WHERE allocation_id=$1",
    )
    .bind(f.allocation.uuid())
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(done, None);
}
#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn history_corrupt_candidate_is_deferred_without_reserving_prefix(pool: PgPool) {
    let f = Fixture::new(&pool).await;
    let id = f.admit().await;
    finish(&f, id).await;
    sqlx::query("UPDATE operations SET phase='exited' WHERE id=$1")
        .bind(id.uuid())
        .execute(&pool)
        .await
        .unwrap();
    assert!(
        f.store
            .claim_history(f.host, 1, Domain::Commands, 30, false)
            .await
            .unwrap()
            .is_none()
    );
    let scanned: bool = sqlx::query_scalar(
        "SELECT history_commands_scan_at IS NOT NULL FROM allocations WHERE id=$1",
    )
    .bind(f.allocation.uuid())
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(scanned);
    let reserved: i64 =
        sqlx::query_scalar("SELECT count(*) FROM allocation_history WHERE allocation_id=$1")
            .bind(f.allocation.uuid())
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(reserved, 0);
    sqlx::query("UPDATE operations SET phase='rejected_before_dispatch' WHERE id=$1")
        .bind(id.uuid())
        .execute(&pool)
        .await
        .unwrap();
    assert!(
        f.store
            .claim_history(f.host, 1, Domain::Commands, 30, false)
            .await
            .unwrap()
            .is_some()
    );
}

#[sqlx::test(migrations = false)]
async fn history_migration_backfills_admission_order_without_retiring_rows(pool: PgPool) {
    let old = sqlx::migrate::Migrator {
        migrations: std::borrow::Cow::Owned(
            sandbox_store::MIGRATOR.iter().take(14).cloned().collect(),
        ),
        ..sqlx::migrate::Migrator::DEFAULT
    };
    old.run(&pool).await.unwrap();
    let mut f = Fixture::new(&pool).await;
    let first = legacy_execute::seed(&pool, &f.request, f.allocation).await;
    finish(&f, first).await;
    f.request.idempotency_key = key();
    let second = legacy_execute::seed(&pool, &f.request, f.allocation).await;
    finish(&f, second).await;
    let before: Vec<serde_json::Value> =
        sqlx::query_scalar("SELECT to_jsonb(o) FROM operations o ORDER BY id")
            .fetch_all(&pool)
            .await
            .unwrap();
    sandbox_store::MIGRATOR.run(&pool).await.unwrap();
    let pool = reconnect_after_upgrade(&pool).await;
    f.store = Store::from_pool(pool.clone());
    let after: Vec<serde_json::Value> =
        sqlx::query_scalar("SELECT to_jsonb(o) FROM operations o ORDER BY id")
            .fetch_all(&pool)
            .await
            .unwrap();
    assert_eq!(before, after);
    let last: uuid::Uuid =
        sqlx::query_scalar("SELECT last_admitted_operation_id FROM allocations WHERE id=$1")
            .bind(f.allocation.uuid())
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(last, first.uuid().max(second.uuid()));
    let retired: i64 = sqlx::query_scalar("SELECT count(*) FROM allocation_history")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(retired, 0);
    f.request.idempotency_key = key();
    let new = f.admit().await;
    assert!(new.uuid() > last);
}

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn history_refunds_slots_and_bytes_only_after_verified_ack(pool: PgPool) {
    for (count, limit) in [(32, 1), (8, 8 * 1024 * 1024)] {
        let mut f = Fixture::new(&pool).await;
        f.request.command.output_limit = limit;
        let mut ids = Vec::new();
        for _ in 0..count {
            f.request.idempotency_key = key();
            let id = f.admit().await;
            finish(&f, id).await;
            ids.push(id);
        }
        let old = f.request.clone();
        f.request.idempotency_key = key();
        assert_eq!(
            f.store.admit_execute(&f.request).await.unwrap(),
            ExecuteAdmission::CapacityExhausted
        );
        let (claim, request) = prepare(&f).await;
        assert_eq!(
            f.store.admit_execute(&f.request).await.unwrap(),
            ExecuteAdmission::CapacityExhausted
        );
        f.store
            .complete_history(&claim, &observation(request), false)
            .await
            .unwrap();
        let new = f.admit().await;
        assert!(new > *ids.last().unwrap());
        assert_eq!(
            f.store.admit_execute(&old).await.unwrap(),
            ExecuteAdmission::Accepted {
                operation_id: *ids.last().unwrap(),
                status: "failed".into()
            }
        );
        let c = f.claim().await;
        assert!(matches!(
            f.store.prepare_execute(&c, f.host, 1).await.unwrap(),
            ExecuteAction::Dispatch { .. }
        ));
        let retained: i64 =
            sqlx::query_scalar("SELECT count(*) FROM operations WHERE execution_allocation_id=$1")
                .bind(f.allocation.uuid())
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(retained, count + 1);
    }
}
#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn history_malformed_completion_remains_charged(pool: PgPool) {
    let mut f = Fixture::new(&pool).await;
    f.request.command.output_limit = 8 * 1024 * 1024;
    for _ in 0..8 {
        f.request.idempotency_key = key();
        let id = f.admit().await;
        finish(&f, id).await;
    }
    let (claim, request) = prepare(&f).await;
    f.store
        .complete_history(&claim, &observation(request), false)
        .await
        .unwrap();
    let saved: serde_json::Value =
        sqlx::query_scalar("SELECT completion FROM allocation_history WHERE allocation_id=$1")
            .bind(f.allocation.uuid())
            .fetch_one(&pool)
            .await
            .unwrap();
    for path in [
        "{completed,context,boot_id}",
        "{request,ownership,host_id}",
        "{request,ownership,project_id}",
        "{request,barrier,through}",
    ] {
        sqlx::query("UPDATE allocation_history SET completion=jsonb_set($2,$3::text[],'\"wrong\"') WHERE allocation_id=$1").bind(f.allocation.uuid()).bind(&saved).bind(path).execute(&pool).await.unwrap();
        f.request.idempotency_key = key();
        assert_eq!(
            f.store.admit_execute(&f.request).await.unwrap(),
            ExecuteAdmission::CapacityExhausted
        );
    }
    sqlx::query("UPDATE allocation_history SET completion=$2 WHERE allocation_id=$1")
        .bind(f.allocation.uuid())
        .bind(saved)
        .execute(&pool)
        .await
        .unwrap();
    let _ = f.admit().await;
}

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn history_scan_rotates_past_more_than_one_batch_of_empty_allocations(pool: PgPool) {
    let first = Fixture::new(&pool).await;
    let mut last = None;
    for _ in 0..33 {
        let mut f = Fixture::new(&pool).await;
        sqlx::query("UPDATE allocations SET host_id=$2 WHERE id=$1")
            .bind(f.allocation.uuid())
            .bind(first.host.uuid())
            .execute(&pool)
            .await
            .unwrap();
        f.host = first.host;
        last = Some(f);
    }
    let mut f = last.unwrap();
    // Bulk setup can outlive the simulated heartbeat and leases on a slow DB.
    // This case exercises scan order, so refresh the running fixtures before admission.
    sqlx::query("UPDATE hosts SET last_seen_at=clock_timestamp() WHERE id=$1")
        .bind(first.host.uuid())
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("UPDATE allocations SET lease_expires_at=clock_timestamp()+interval '60 seconds' WHERE host_id=$1")
        .bind(first.host.uuid()).execute(&pool).await.unwrap();
    f.request.command.deadline_unix_ms = now_ms() + 60_000;
    // Put the history-bearing allocation after the first batch explicitly.
    // Generated identity order is not the scan priority this test exercises.
    // The other 33 allocations are unscanned (NULLS FIRST); this old timestamp
    // still sorts before the current timestamps written by the first scan.
    sqlx::query(
        "UPDATE allocations SET history_commands_scan_at='2000-01-01T00:00:00Z' WHERE id=$1",
    )
    .bind(f.allocation.uuid())
    .execute(&pool)
    .await
    .unwrap();
    let id = f.admit().await;
    finish(&f, id).await;
    assert!(
        first
            .store
            .claim_history(first.host, 1, Domain::Commands, 30, false)
            .await
            .unwrap()
            .is_none()
    );
    let claim = first
        .store
        .claim_history(first.host, 1, Domain::Commands, 30, false)
        .await
        .unwrap()
        .unwrap();
    let Preparation::Binding { claim, .. } = claim else {
        panic!("binding")
    };
    assert_eq!(claim.allocation_id, f.allocation);
}
#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn history_expired_claim_after_allocation_lock_wait_cannot_complete(pool: PgPool) {
    let f = Fixture::new(&pool).await;
    let id = f.admit().await;
    finish(&f, id).await;
    let (mut claim, request) = prepare(&f).await;
    let expires:time::OffsetDateTime=sqlx::query_scalar("UPDATE allocation_history SET lease_expires_at=clock_timestamp()+interval '200 milliseconds' WHERE allocation_id=$1 RETURNING lease_expires_at").bind(f.allocation.uuid()).fetch_one(&pool).await.unwrap();
    claim.expires_at = expires;
    let mut tx = pool.begin().await.unwrap();
    sqlx::query("SELECT id FROM allocations WHERE id=$1 FOR UPDATE")
        .bind(f.allocation.uuid())
        .execute(&mut *tx)
        .await
        .unwrap();
    let store = f.store.clone();
    let c = claim.clone();
    let task = tokio::spawn(async move {
        store
            .complete_history(&c, &observation(request), false)
            .await
    });
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    tx.commit().await.unwrap();
    assert!(matches!(task.await.unwrap(), Err(Error::LostClaim)));
    let completed: Option<uuid::Uuid> = sqlx::query_scalar(
        "SELECT completed_through FROM allocation_history WHERE allocation_id=$1",
    )
    .bind(f.allocation.uuid())
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(completed, None);
}

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn history_concurrent_admission_never_enters_reserved_prefix(pool: PgPool) {
    let mut f = Fixture::new(&pool).await;
    let old = f.admit().await;
    finish(&f, old).await;
    f.request.idempotency_key = key();
    let (admission, prepared) = tokio::join!(
        f.store.admit_execute(&f.request),
        f.store
            .claim_history(f.host, 1, Domain::Commands, 30, false)
    );
    let ExecuteAdmission::Accepted {
        operation_id: new, ..
    } = admission.unwrap()
    else {
        panic!("admit")
    };
    let prepared = match prepared.unwrap() {
        Some(p) => p,
        None => f
            .store
            .claim_history(f.host, 1, Domain::Commands, 30, false)
            .await
            .unwrap()
            .unwrap(),
    };
    let Preparation::Binding { claim, .. } = prepared else {
        panic!("binding")
    };
    let floor: uuid::Uuid = sqlx::query_scalar(
        "SELECT reserved_through FROM allocation_history WHERE allocation_id=$1",
    )
    .bind(claim.allocation_id.uuid())
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(floor, old.uuid());
    assert!(new.uuid() > floor);
    assert!(
        f.store
            .claim_history(f.host, 1, Domain::Commands, 30, false)
            .await
            .unwrap()
            .is_none()
    );
}

#[path = "released_history.rs"]
mod released;
