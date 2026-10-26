use super::*;
use sandbox_cleanup::{Cleaner, CleanupError, CleanupTick, Retirement};
use sandbox_protocol::output::{OutputOwner, OutputRetirement};
use sandbox_store::output_cleanup::CleanupCompletion;

#[path = "compaction.rs"]
mod compaction_tests;

#[path = "storage_audit.rs"]
mod audit_tests;

fn completion(m: &CleanupManifest) -> CleanupCompletion {
    let Some(plans) = &m.plans else {
        return CleanupCompletion::NoUploadsAuthorized;
    };
    let receipt = |p: &OutputPlan, previous: Option<OutputRef>| OutputRetirement {
        version: 1,
        plan_sha256: p.metadata_digest().unwrap(),
        marker_version: previous
            .as_ref()
            .and_then(|p| p.object_version.as_ref())
            .map(|_| "retired-version".into()),
        previous,
        marker_etag: "retired-etag".into(),
    };
    CleanupCompletion::Retired {
        stdout: Box::new(receipt(
            &plans.stdout,
            m.references.as_ref().map(|r| r.stdout.clone()),
        )),
        stderr: Box::new(receipt(
            &plans.stderr,
            m.references.as_ref().map(|r| r.stderr.clone()),
        )),
    }
}
async fn prepared(pool: &PgPool) -> (Fixture, CleanupClaim, CleanupCompletion) {
    let (f, _) = published(pool).await;
    age(&f, false).await;
    f.store.enqueue_expired_output(100).await.unwrap();
    let c = next(&f).await;
    let receipt = completion(&ready(&f, &c).await);
    (f, c, receipt)
}
async fn stored(f: &Fixture) -> Option<CleanupCompletion> {
    sqlx::query_scalar::<_, Option<Value>>(
        "SELECT receipt FROM output_cleanup WHERE operation_id=$1",
    )
    .bind(f.operation.uuid())
    .fetch_one(f.store.pool())
    .await
    .unwrap()
    .map(|r| serde_json::from_value(r).unwrap())
}
async fn retry_now(f: &Fixture) {
    sqlx::query("UPDATE output_cleanup SET next_retry_at=clock_timestamp()-interval '1 second' WHERE operation_id=$1")
        .bind(f.operation.uuid()).execute(f.store.pool()).await.unwrap();
}

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn cleanup_completion_is_idempotent_and_never_reopens_execution(pool: PgPool) {
    let (f, c, receipt) = prepared(&pool).await;
    let before = f.snapshot().await;
    f.store
        .complete_output_cleanup(&c, &receipt, true)
        .await
        .unwrap();
    // Discarding the first result models losing the commit acknowledgement.
    f.store
        .complete_output_cleanup(&c, &receipt, true)
        .await
        .unwrap();
    assert_eq!(stored(&f).await, Some(receipt.clone()));
    assert!(f.store.claim_output_cleanup(30).await.unwrap().is_none());
    assert_eq!(f.store.enqueue_expired_output(100).await.unwrap(), 0);
    assert!(f.store.claim_output(30).await.unwrap().is_none());
    assert!(matches!(
        f.store.defer_output_cleanup(&c, 1).await,
        Err(OutputError::LostClaim)
    ));
    assert_eq!(f.snapshot().await, before);
    let mut other = receipt;
    let CleanupCompletion::Retired { stdout, .. } = &mut other else {
        panic!("retired")
    };
    stdout.marker_etag = "different-ack".into();
    assert!(matches!(
        f.store.complete_output_cleanup(&c, &other, true).await,
        Err(OutputError::BadEvidence)
    ));
}

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn cleanup_completion_rejects_wrong_or_partial_receipts(pool: PgPool) {
    let (f, c, receipt) = prepared(&pool).await;
    assert!(matches!(
        f.store.complete_output_cleanup(&c, &receipt, false).await,
        Err(OutputError::SimulationDenied)
    ));
    assert!(matches!(
        f.store
            .complete_output_cleanup(&c, &CleanupCompletion::NoUploadsAuthorized, true)
            .await,
        Err(OutputError::BadEvidence)
    ));
    for field in 0..7 {
        let mut bad = receipt.clone();
        let CleanupCompletion::Retired { stdout, stderr } = &mut bad else {
            panic!("retired")
        };
        match field {
            0 => stdout.version = 2,
            1 => stdout.plan_sha256 = "0".repeat(64),
            2 => stdout.previous = None,
            3 => stdout.previous.as_mut().unwrap().etag = "other-object".into(),
            4 => stdout.marker_version = stdout.previous.as_ref().unwrap().object_version.clone(),
            5 => stdout.marker_version = None,
            _ => std::mem::swap(stdout, stderr),
        }
        assert!(
            matches!(
                f.store.complete_output_cleanup(&c, &bad, true).await,
                Err(OutputError::BadEvidence)
            ),
            "field {field}"
        );
        assert!(stored(&f).await.is_none());
    }
    f.store
        .complete_output_cleanup(&c, &receipt, true)
        .await
        .unwrap();
}

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn cleanup_completion_requires_preparation_and_unchanged_evidence(pool: PgPool) {
    let (f, _) = published(&pool).await;
    age(&f, false).await;
    f.store.enqueue_expired_output(100).await.unwrap();
    let c = next(&f).await;
    assert!(matches!(
        f.store
            .complete_output_cleanup(&c, &CleanupCompletion::NoUploadsAuthorized, true)
            .await,
        Err(OutputError::BadEvidence)
    ));
    let receipt = completion(&ready(&f, &c).await);
    sqlx::query("UPDATE operations SET output_refs=jsonb_set(output_refs,'{0,etag}','\"tampered\"') WHERE id=$1")
        .bind(f.operation.uuid()).execute(&pool).await.unwrap();
    assert!(matches!(
        f.store.complete_output_cleanup(&c, &receipt, true).await,
        Err(OutputError::Corrupt)
    ));
    assert!(stored(&f).await.is_none());
}

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn cleanup_completion_fences_replaced_workers(pool: PgPool) {
    let (f, old, receipt) = prepared(&pool).await;
    sqlx::query("UPDATE output_cleanup SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE operation_id=$1")
        .bind(f.operation.uuid()).execute(&pool).await.unwrap();
    let new = next(&f).await;
    assert!(new.revision > old.revision);
    ready(&f, &new).await;
    assert!(matches!(
        f.store.complete_output_cleanup(&old, &receipt, true).await,
        Err(OutputError::LostClaim)
    ));
    f.store
        .complete_output_cleanup(&new, &receipt, true)
        .await
        .unwrap();
    assert!(matches!(
        f.store.complete_output_cleanup(&old, &receipt, true).await,
        Err(OutputError::LostClaim)
    ));
}

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn cleanup_completion_rechecks_lease_after_both_lock_waits(pool: PgPool) {
    for table in ["operations", "output_cleanup"] {
        let (f, c, receipt) = prepared(&pool).await;
        sqlx::query("UPDATE output_cleanup SET lease_expires_at=clock_timestamp()+interval '1 second' WHERE operation_id=$1")
            .bind(f.operation.uuid()).execute(&pool).await.unwrap();
        let mut lock = pool.begin().await.unwrap();
        let sql = if table == "operations" {
            "SELECT id FROM operations WHERE id=$1 FOR UPDATE"
        } else {
            "SELECT operation_id FROM output_cleanup WHERE operation_id=$1 FOR UPDATE"
        };
        sqlx::query(sql)
            .bind(f.operation.uuid())
            .fetch_one(&mut *lock)
            .await
            .unwrap();
        let store = f.store.clone();
        let task =
            tokio::spawn(async move { store.complete_output_cleanup(&c, &receipt, true).await });
        tokio::time::sleep(std::time::Duration::from_millis(1200)).await;
        lock.commit().await.unwrap();
        assert!(matches!(task.await.unwrap(), Err(OutputError::LostClaim)));
        assert!(stored(&f).await.is_none());
        // Complete this fixture so the next iteration does not claim it.
        // Failed completion drops its transaction. SQLx queues the rollback,
        // so SKIP LOCKED can temporarily find no replacement claim until that
        // connection finishes releasing its row locks.
        let next = tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                if let Some(claim) = f.store.claim_output_cleanup(30).await.unwrap() {
                    break claim;
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("rollback must release the cleanup claim");
        let receipt = completion(&ready(&f, &next).await);
        f.store
            .complete_output_cleanup(&next, &receipt, true)
            .await
            .unwrap();
    }
}

struct NeverRetire;
impl Retirement for NeverRetire {
    async fn retire(
        &self,
        _: &OutputPlan,
        _: &OutputOwner,
        _: Option<&OutputRef>,
        _: i64,
    ) -> Result<OutputRetirement, sandbox_artifacts::Error> {
        panic!("this fixture never authorizes a storage call")
    }
}

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn cleanup_worker_requires_simulation_opt_in_and_handles_no_upload_authority(pool: PgPool) {
    let f = Fixture::new(&pool).await;
    f.finish().await;
    f.work().await;
    age(&f, false).await;
    let before = f.snapshot().await;
    let denied = Cleaner::new(f.store.clone(), NeverRetire, false);
    let Err(CleanupError::Attempt {
        operation_id,
        source,
    }) = denied.tick().await
    else {
        panic!("denied")
    };
    assert_eq!(operation_id, f.operation);
    assert!(matches!(
        *source,
        CleanupError::Store(OutputError::SimulationDenied)
    ));
    assert!(stored(&f).await.is_none());
    let deferred:bool=sqlx::query_scalar("SELECT lease_expires_at IS NULL AND next_retry_at>clock_timestamp() FROM output_cleanup WHERE operation_id=$1")
        .bind(f.operation.uuid()).fetch_one(&pool).await.unwrap();
    assert!(deferred);
    retry_now(&f).await;
    let worker = Cleaner::new(f.store.clone(), NeverRetire, true);
    assert_eq!(
        worker.tick().await.unwrap(),
        CleanupTick::Completed(f.operation)
    );
    assert_eq!(
        stored(&f).await,
        Some(CleanupCompletion::NoUploadsAuthorized)
    );
    assert_eq!(worker.tick().await.unwrap(), CleanupTick::Idle);
    assert_eq!(f.snapshot().await, before);
}

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn cleanup_worker_waits_for_grace_without_storage_calls(pool: PgPool) {
    let (f, _) = published(&pool).await;
    age(&f, true).await;
    let before = f.snapshot().await;
    let worker = Cleaner::new(f.store.clone(), NeverRetire, true);
    assert_eq!(
        worker.tick().await.unwrap(),
        CleanupTick::Waiting(f.operation)
    );
    assert_eq!(worker.tick().await.unwrap(), CleanupTick::Idle);
    assert!(stored(&f).await.is_none());
    assert_eq!(f.snapshot().await, before);
}

#[sqlx::test(migrations = false)]
async fn cleanup_completion_upgrade_preserves_pending_inventory(pool: PgPool) {
    let old = sqlx::migrate::Migrator {
        migrations: std::borrow::Cow::Owned(
            sandbox_store::MIGRATOR.iter().take(7).cloned().collect(),
        ),
        ..sqlx::migrate::Migrator::DEFAULT
    };
    old.run(&pool).await.unwrap();
    let (f, _) = published_with_schema(&pool, true).await;
    age(&f, false).await;
    f.store.enqueue_expired_output(100).await.unwrap();
    let before: Value =
        sqlx::query_scalar("SELECT to_jsonb(c) FROM output_cleanup c WHERE operation_id=$1")
            .bind(f.operation.uuid())
            .fetch_one(&pool)
            .await
            .unwrap();
    sandbox_store::MIGRATOR.run(&pool).await.unwrap();
    sandbox_store::MIGRATOR.run(&pool).await.unwrap();
    let after:Value=sqlx::query_scalar("SELECT to_jsonb(c)-ARRAY['completed_at','receipt'] FROM output_cleanup c WHERE operation_id=$1")
        .bind(f.operation.uuid()).fetch_one(&pool).await.unwrap();
    assert_eq!(before, after);
    assert!(stored(&f).await.is_none());
    let c = next(&f).await;
    let receipt = completion(&ready(&f, &c).await);
    f.store
        .complete_output_cleanup(&c, &receipt, true)
        .await
        .unwrap();
}

fn s3_config() -> sandbox_artifacts::S3Config {
    let env = |key| std::env::var(key).expect("required private versioned MinIO fixture");
    sandbox_artifacts::S3Config {
        endpoint: env("HUDSON_TEST_S3_ENDPOINT"),
        region: "us-east-1".into(),
        bucket: env("HUDSON_TEST_S3_VERSIONED_BUCKET"),
        access_key: env("HUDSON_TEST_S3_ACCESS_KEY"),
        secret_key: env("HUDSON_TEST_S3_SECRET_KEY"),
        session_token: None,
        allow_loopback_http: true,
    }
}

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn cleanup_completion_schema_rejects_unpaired_or_premature_completion(pool: PgPool) {
    let (f, c, _) = prepared(&pool).await;
    let error=sqlx::query("UPDATE output_cleanup SET completed_at=clock_timestamp(),lease_expires_at=NULL WHERE operation_id=$1")
        .bind(f.operation.uuid()).execute(&pool).await.unwrap_err();
    assert_eq!(
        error.as_database_error().unwrap().code().as_deref(),
        Some("23514")
    );
    assert!(stored(&f).await.is_none());
    f.store.defer_output_cleanup(&c, 60).await.unwrap();
    let (g, _) = published(&pool).await;
    age(&g, true).await;
    g.store.enqueue_expired_output(100).await.unwrap();
    let c = next(&g).await;
    assert!(matches!(
        g.store.prepare_output_cleanup(&c).await.unwrap(),
        CleanupPreparation::Waiting { .. }
    ));
    let error=sqlx::query("UPDATE output_cleanup SET completed_at=clock_timestamp(),receipt='{}',lease_expires_at=NULL WHERE operation_id=$1")
        .bind(g.operation.uuid()).execute(&pool).await.unwrap_err();
    assert_eq!(
        error.as_database_error().unwrap().code().as_deref(),
        Some("23514")
    );
    assert!(stored(&g).await.is_none());
}

async fn archive_fixture(
    pool: &PgPool,
    publish: bool,
) -> (Fixture, OutputPlans, Option<OutputRefs>) {
    use sha2::{Digest, Sha256};
    let f = Fixture::new(pool).await;
    f.finish().await;
    let claim = f.store.claim_output(30).await.unwrap().unwrap();
    let work = f.store.prepare_output(&claim, 5, 0, true).await.unwrap();
    let mut p = plans(&work);
    p.stdout.sha256 = hex::encode(Sha256::digest(b"a\x00\xffz"));
    p.stderr.sha256 = hex::encode(Sha256::digest(b"err"));
    f.store.save_output_plans(&claim, &p, true).await.unwrap();
    let store = s3_config().build().unwrap();
    let stdout = store
        .upload(&p.stdout, &work.ticket.owner, now(), b"a\x00\xffz")
        .await
        .unwrap();
    let refs = if publish {
        let stderr = store
            .upload(&p.stderr, &work.ticket.owner, now(), b"err")
            .await
            .unwrap();
        let r = OutputRefs { stdout, stderr };
        f.store.publish_output(&claim, &r, true).await.unwrap();
        Some(r)
    } else {
        None
    };
    // Let real retention elapse: rewriting metadata after upload would break
    // the immutable digest binding this integration is intended to verify.
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            let expired: bool = sqlx::query_scalar(
                "SELECT output_expires_at<=clock_timestamp() FROM operations WHERE id=$1",
            )
            .bind(f.operation.uuid())
            .fetch_one(pool)
            .await
            .unwrap();
            if expired && now() >= work.ticket.delete_after_unix_ms {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
    })
    .await
    .unwrap();
    (f, p, refs)
}

struct FailStderr(sandbox_artifacts::ArtifactRetirer);
impl Retirement for FailStderr {
    async fn retire(
        &self,
        plan: &OutputPlan,
        owner: &OutputOwner,
        selected: Option<&OutputRef>,
        now: i64,
    ) -> Result<OutputRetirement, sandbox_artifacts::Error> {
        if plan.name == OutputName::Stderr {
            return Err(sandbox_artifacts::Error::Unavailable);
        }
        self.0.retire(plan, owner, selected, now).await
    }
}

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
#[ignore = "requires PostgreSQL and private versioned MinIO fixture"]
async fn output_minio_cleanup_worker_recovers_partial_retirement_and_records_both_receipts(
    pool: PgPool,
) {
    let (f, p, refs) = archive_fixture(&pool, true).await;
    let refs = refs.unwrap();
    let before = f.snapshot().await;
    let failing = Cleaner::new(
        f.store.clone(),
        FailStderr(s3_config().build_retirer().unwrap()),
        true,
    );
    let Err(CleanupError::Attempt {
        operation_id,
        source,
    }) = failing.tick().await
    else {
        panic!("partial failure")
    };
    assert_eq!(operation_id, f.operation);
    assert!(matches!(
        *source,
        CleanupError::Storage(sandbox_artifacts::Error::Unavailable)
    ));
    assert!(stored(&f).await.is_none());
    let objects = s3_config().build().unwrap();
    // Trusted historical time here inspects storage state independently of
    // API retention: stdout's old version is gone while stderr is still intact.
    assert!(matches!(
        objects
            .read(
                &refs.stdout,
                &p.stdout.owner,
                p.stdout.created_unix_ms + 1,
                0,
                4
            )
            .await,
        Err(sandbox_artifacts::Error::Missing)
    ));
    assert_eq!(
        objects
            .read(
                &refs.stderr,
                &p.stderr.owner,
                p.stderr.created_unix_ms + 1,
                0,
                3
            )
            .await
            .unwrap()
            .bytes,
        b"err"
    );
    assert_eq!(f.snapshot().await, before);
    retry_now(&f).await;
    let recovered = Cleaner::new(f.store.clone(), s3_config().build_retirer().unwrap(), true);
    assert_eq!(
        recovered.tick().await.unwrap(),
        CleanupTick::Completed(f.operation)
    );
    let CleanupCompletion::Retired { stdout, stderr } = stored(&f).await.unwrap() else {
        panic!("two receipts")
    };
    assert_eq!(stdout.previous, Some(refs.stdout.clone()));
    assert_eq!(stderr.previous, Some(refs.stderr.clone()));
    for (plan, reference, bytes) in [
        (&p.stdout, &refs.stdout, &b"a\x00\xffz"[..]),
        (&p.stderr, &refs.stderr, &b"err"[..]),
    ] {
        assert!(matches!(
            objects
                .read(reference, &plan.owner, plan.created_unix_ms + 1, 0, 1)
                .await,
            Err(sandbox_artifacts::Error::Missing)
        ));
        assert_eq!(
            objects
                .upload(plan, &plan.owner, plan.created_unix_ms + 1, bytes)
                .await,
            Err(sandbox_artifacts::Error::Conflict)
        );
    }
    assert_eq!(recovered.tick().await.unwrap(), CleanupTick::Idle);
    assert_eq!(f.snapshot().await, before);
    let view = f
        .store
        .output_for_project(f.project, f.operation)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(view.status, "expired");
    assert!(view.references.is_none());
}

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
#[ignore = "requires PostgreSQL and private versioned MinIO fixture"]
async fn output_minio_cleanup_worker_retires_orphans_after_owner_lifecycle_changes(pool: PgPool) {
    let (f, p, _) = archive_fixture(&pool, false).await;
    let before = f.snapshot().await;
    sqlx::query("UPDATE sandboxes SET current_allocation_id=NULL WHERE id=$1")
        .bind(f.sandbox.uuid())
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("UPDATE hosts SET supervisor_epoch=supervisor_epoch+1 WHERE id=$1")
        .bind(f.host.uuid())
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("UPDATE projects SET status='deleting' WHERE id=$1")
        .bind(f.project.uuid())
        .execute(&pool)
        .await
        .unwrap();
    let worker = Cleaner::new(f.store.clone(), s3_config().build_retirer().unwrap(), true);
    assert_eq!(
        worker.tick().await.unwrap(),
        CleanupTick::Completed(f.operation)
    );
    let CleanupCompletion::Retired { stdout, stderr } = stored(&f).await.unwrap() else {
        panic!("retired")
    };
    assert!(stdout.previous.is_some());
    assert!(stderr.previous.is_none());
    assert_eq!(stdout.plan_sha256, p.stdout.metadata_digest().unwrap());
    assert_eq!(stderr.plan_sha256, p.stderr.metadata_digest().unwrap());
    assert_eq!(f.snapshot().await, before);
    assert_eq!(worker.tick().await.unwrap(), CleanupTick::Idle);
}
