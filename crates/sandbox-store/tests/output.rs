//! Real PostgreSQL transactions with synthetic guest receipts, not VM evidence.
#![allow(clippy::unwrap_used, clippy::expect_used)]
use sandbox_protocol::{
    AllocationId, HostId, Id, IdempotencyKey, OperationId, ProjectId, ProjectToken, SandboxId,
    guest_model::{self as guest, Receipt, State},
    output::{OutputName, OutputPlan, OutputPlans, OutputRef, OutputRefs},
    supervisor::CommandObservation,
};
use sandbox_store::{
    Store,
    claims::{Claim, OperationKind},
    execute::{ExecuteAction, ExecuteAdmission, ExecuteCommand},
    output::{OutputClaim, OutputError, OutputWork},
};
use serde_json::{Value, json};
use sqlx::{PgPool, Row};
use time::OffsetDateTime;
#[path = "support/legacy_execute.rs"]
mod legacy_execute;

#[path = "support/output_cleanup.rs"]
mod cleanup_tests;

fn now() -> i64 {
    (OffsetDateTime::now_utc().unix_timestamp_nanos() / 1_000_000) as i64
}
struct Fixture {
    store: Store,
    project: ProjectId,
    sandbox: SandboxId,
    allocation: AllocationId,
    host: HostId,
    operation: OperationId,
    execution_claim: Claim,
    observation: CommandObservation,
}
impl Fixture {
    async fn new(pool: &PgPool) -> Self {
        Self::with_schema(pool, false).await
    }
    async fn with_schema(pool: &PgPool, legacy: bool) -> Self {
        let project = ProjectId::generate();
        let sandbox = SandboxId::generate();
        let allocation = AllocationId::generate();
        let host = HostId::generate();
        let token = ProjectToken::generate().unwrap();
        sqlx::query("INSERT INTO projects(id,name,status,limits,api_tokens) VALUES($1,'output-test','active','{}',$2)")
            .bind(project.uuid()).bind(json!([{"key_id":token.key_id().as_str(),"hash":hex::encode(token.hash().as_bytes())}])).execute(pool).await.unwrap();
        sqlx::query("INSERT INTO hosts(id,status,cpu_capacity,memory_capacity_mib,disk_capacity_mib,supervisor_epoch,last_seen_at) VALUES($1,'ready',4,8192,65536,1,clock_timestamp())")
            .bind(host.uuid()).execute(pool).await.unwrap();
        sqlx::query("INSERT INTO sandboxes(id,project_id,image_digest,resources,desired_state,observed_state,generation) VALUES($1,$2,'sha256:fixture','{}','running','running',1)")
            .bind(sandbox.uuid()).bind(project.uuid()).execute(pool).await.unwrap();
        sqlx::query("INSERT INTO allocations(id,project_id,sandbox_id,host_id,generation,supervisor_epoch,vcpu,memory_mib,disk_mib,status,lease_expires_at) VALUES($1,$2,$3,$4,1,1,1,128,64,'running',clock_timestamp()+interval '5 minutes')")
            .bind(allocation.uuid()).bind(project.uuid()).bind(sandbox.uuid()).bind(host.uuid()).execute(pool).await.unwrap();
        sqlx::query("UPDATE sandboxes SET current_allocation_id=$2 WHERE id=$1")
            .bind(sandbox.uuid())
            .bind(allocation.uuid())
            .execute(pool)
            .await
            .unwrap();
        let store = Store::from_pool(pool.clone());
        let request=ExecuteCommand { project_id:project,sandbox_id:sandbox,key_id:token.key_id().clone(),
            idempotency_key: IdempotencyKey::parse(&OperationId::generate().to_string()).unwrap(),
            command: serde_json::from_value(json!({"argv":["private-command"],"deadline_unix_ms":now()+60000,"output_limit":100})).unwrap() };
        let operation = if legacy {
            legacy_execute::seed(pool, &request, allocation).await
        } else {
            let ExecuteAdmission::Accepted { operation_id, .. } =
                store.admit_execute(&request).await.unwrap()
            else {
                panic!("admit")
            };
            operation_id
        };
        let execution_claim = store
            .claim_next(OperationKind::Execute, 30)
            .await
            .unwrap()
            .unwrap();
        let (owner, command) = if legacy {
            let owner = sandbox_protocol::supervisor::Ownership {
                host_id: host.to_string(),
                project_id: project.to_string(),
                sandbox_id: sandbox.to_string(),
                allocation_id: allocation.to_string(),
                operation_id: operation.to_string(),
                generation: 1,
                supervisor_epoch: 1,
                claim_revision: execution_claim.revision,
                claim_expires_unix_ms: (execution_claim.lease_expires_at.unix_timestamp_nanos()
                    / 1_000_000) as i64,
            };
            let command = request.command.for_operation(operation);
            let evidence = json!({"phase":"execute_dispatch_intent","host_id":owner.host_id,"project_id":owner.project_id,"sandbox_id":owner.sandbox_id,"operation_id":owner.operation_id,"allocation_id":owner.allocation_id,"generation":1,"supervisor_epoch":1,"claim_revision":owner.claim_revision,"simulated":null,"command_digest":hex::encode(command.digest().unwrap())});
            sqlx::query("UPDATE operations SET phase='execute_dispatched',attempt_count=1,attempt_receipts=jsonb_build_array($2::jsonb) WHERE id=$1").bind(operation.uuid()).bind(evidence).execute(pool).await.unwrap();
            (owner, command)
        } else {
            let ExecuteAction::Dispatch { owner, command } = store
                .prepare_execute(&execution_claim, host, 1)
                .await
                .unwrap()
            else {
                panic!("dispatch")
            };
            (owner, command)
        };
        let digest = command.digest().unwrap();
        let receipt = Receipt {
            version: 1,
            context: guest::Context {
                allocation_id: allocation,
                generation: 1,
                boot_id: "pinned-guest-boot".into(),
            },
            operation_id: operation,
            digest,
            state: State::Exited,
            deadline_unix_ms: command.deadline_unix_ms,
            output_limit: command.output_limit,
            cancel_requested: false,
            cleanup_confirmed: true,
            exit_code: Some(0),
            signal: None,
            stdout: guest::Output {
                seen: 10,
                stored: 4,
                truncated: true,
            },
            stderr: guest::Output {
                seen: 3,
                stored: 3,
                truncated: false,
            },
            reason: None,
        };
        let observation = CommandObservation {
            ownership: Some(owner),
            simulated: true,
            observed_unix_ms: now(),
            command_digest: digest.to_vec(),
            receipt: Some((&receipt).into()),
            not_started: false,
        };
        Self {
            store,
            project,
            sandbox,
            allocation,
            host,
            operation,
            execution_claim,
            observation,
        }
    }
    async fn finish(&self) {
        self.store
            .record_execute_observation(&self.execution_claim, &self.observation, true)
            .await
            .unwrap();
    }
    async fn work(&self) -> (OutputClaim, OutputWork) {
        let claim = self.store.claim_output(30).await.unwrap().unwrap();
        let work = self
            .store
            .prepare_output(&claim, 3600, 600, true)
            .await
            .unwrap();
        (claim, work)
    }
    async fn snapshot(&self) -> Value {
        sqlx::query_scalar("SELECT jsonb_build_array(status,phase,result,error,attempt_count,attempt_receipts,claim_revision,
            lease_expires_at,next_retry_at,completed_at,execution_allocation_id) FROM operations WHERE id=$1")
            .bind(self.operation.uuid()).fetch_one(self.store.pool()).await.unwrap()
    }
    async fn expire_claim(&self) {
        sqlx::query("UPDATE operations SET output_lease_expires_at=clock_timestamp()-interval '1 second' WHERE id=$1")
            .bind(self.operation.uuid()).execute(self.store.pool()).await.unwrap();
    }
}
fn plans(work: &OutputWork) -> OutputPlans {
    let plan = |name, stats: &guest::Output| OutputPlan {
        version: 1,
        owner: work.ticket.owner.clone(),
        upload_attempt: work.ticket.upload_attempt,
        name,
        size: stats.stored,
        sha256: "a".repeat(64),
        seen: stats.seen,
        truncated: stats.truncated,
        created_unix_ms: work.ticket.created_unix_ms,
        expires_unix_ms: work.ticket.expires_unix_ms,
        delete_after_unix_ms: work.ticket.delete_after_unix_ms,
    };
    OutputPlans {
        stdout: plan(OutputName::Stdout, &work.receipt.stdout),
        stderr: plan(OutputName::Stderr, &work.receipt.stderr),
    }
}
fn refs(plans: &OutputPlans) -> OutputRefs {
    let reference = |plan: &OutputPlan| OutputRef {
        plan: plan.clone(),
        etag: "test-etag".into(),
        object_version: Some("test-version".into()),
    };
    OutputRefs {
        stdout: reference(&plans.stdout),
        stderr: reference(&plans.stderr),
    }
}

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn publication_is_separate_from_success_and_recovers_lost_ack(pool: PgPool) {
    let f = Fixture::new(&pool).await;
    assert!(f.store.claim_output(30).await.unwrap().is_none());
    f.finish().await;
    let before = f.snapshot().await;
    let view = f
        .store
        .operation_for_project(f.project, f.operation)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(view.status, "succeeded");
    assert_eq!(view.output_status.as_deref(), Some("pending"));
    let (claim, work) = f.work().await;
    let p = plans(&work);
    let r = refs(&p);
    assert!(matches!(
        f.store.publish_output(&claim, &r, true).await,
        Err(OutputError::BadEvidence)
    ));
    f.store.save_output_plans(&claim, &p, true).await.unwrap();
    f.store.save_output_plans(&claim, &p, true).await.unwrap();
    f.store.publish_output(&claim, &r, true).await.unwrap();
    // Lost acknowledgement is resolved by inspecting the selected references,
    // never by re-claiming or dispatching the completed command.
    let selected = f
        .store
        .output_for_project(f.project, f.operation)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(selected.status, "published");
    assert_eq!(selected.references, Some(r.clone()));
    assert_eq!(selected.owner, Some(work.ticket.owner));
    assert!(matches!(
        f.store.publish_output(&claim, &r, true).await,
        Err(OutputError::LostClaim)
    ));
    assert!(f.store.claim_output(30).await.unwrap().is_none());
    assert!(
        f.store
            .claim_next(OperationKind::Execute, 30)
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(before, f.snapshot().await);
}

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn concurrent_claims_choose_one_and_replacement_reuses_exact_plan(pool: PgPool) {
    let f = Fixture::new(&pool).await;
    f.finish().await;
    let mut tasks = tokio::task::JoinSet::new();
    for _ in 0..8 {
        let store = f.store.clone();
        tasks.spawn(async move { store.claim_output(30).await.unwrap() });
    }
    let mut claims = vec![];
    while let Some(result) = tasks.join_next().await {
        if let Some(c) = result.unwrap() {
            claims.push(c);
        }
    }
    assert_eq!(claims.len(), 1);
    let first = claims.remove(0);
    let work = f
        .store
        .prepare_output(&first, 3600, 600, true)
        .await
        .unwrap();
    let p = plans(&work);
    f.store.save_output_plans(&first, &p, true).await.unwrap();
    f.expire_claim().await;
    assert!(matches!(
        f.store.renew_output_claim(&first, 30).await,
        Err(OutputError::LostClaim)
    ));
    let next = f.store.claim_output(30).await.unwrap().unwrap();
    assert_eq!(next.revision, first.revision + 1);
    let recovered = f
        .store
        .prepare_output(&next, 86400, 3600, true)
        .await
        .unwrap();
    assert_eq!(recovered.ticket, work.ticket);
    assert_eq!(recovered.plans, Some(p.clone()));
    assert!(matches!(
        f.store.save_output_plans(&first, &p, true).await,
        Err(OutputError::LostClaim)
    ));
    assert!(matches!(
        f.store.publish_output(&first, &refs(&p), true).await,
        Err(OutputError::LostClaim)
    ));
    f.store
        .publish_output(&next, &refs(&p), true)
        .await
        .unwrap();
}

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn plans_must_match_every_pinned_field_statistics_and_retention(pool: PgPool) {
    let f = Fixture::new(&pool).await;
    f.finish().await;
    let (claim, work) = f.work().await;
    let p = plans(&work);
    for field in 0..15 {
        let mut bad = p.clone();
        match field {
            0 => bad.stdout.owner.project_id = ProjectId::generate(),
            1 => bad.stdout.owner.sandbox_id = SandboxId::generate(),
            2 => bad.stdout.owner.operation_id = OperationId::generate(),
            3 => bad.stdout.owner.allocation_id = AllocationId::generate(),
            4 => bad.stdout.owner.host_id = HostId::generate(),
            5 => bad.stdout.owner.host_epoch += 1,
            6 => bad.stdout.owner.generation += 1,
            7 => bad.stdout.owner.boot_id = "another-boot".into(),
            8 => {
                bad.stdout.upload_attempt = OperationId::generate();
                bad.stderr.upload_attempt = bad.stdout.upload_attempt;
            }
            9 => {
                bad.stdout.expires_unix_ms += 1;
                bad.stderr.expires_unix_ms += 1;
            }
            10 => bad.stdout.size += 1,
            11 => bad.stderr.seen += 1,
            12 => bad.stdout.name = OutputName::Stderr,
            13 => bad.stdout.sha256 = "not-a-digest".into(),
            _ => {
                bad.stdout.size = 11;
                bad.stdout.seen = 11;
                bad.stdout.truncated = false;
            }
        }
        assert!(
            matches!(
                f.store.save_output_plans(&claim, &bad, true).await,
                Err(OutputError::BadEvidence)
            ),
            "field {field}"
        );
    }
    f.store.save_output_plans(&claim, &p, true).await.unwrap();
    let mut bad = p.clone();
    bad.stdout.sha256 = "b".repeat(64);
    assert!(matches!(
        f.store.save_output_plans(&claim, &bad, true).await,
        Err(OutputError::BadEvidence)
    ));
    assert!(matches!(
        f.store.publish_output(&claim, &refs(&bad), true).await,
        Err(OutputError::BadEvidence)
    ));
    let mut r = refs(&p);
    r.stderr.etag = "\nheader".into();
    assert!(matches!(
        f.store.publish_output(&claim, &r, true).await,
        Err(OutputError::BadEvidence)
    ));
    assert!(
        f.store
            .output_for_project(f.project, f.operation)
            .await
            .unwrap()
            .unwrap()
            .references
            .is_none()
    );
}

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn original_ownership_survives_destroy_host_restart_and_credential_revocation(pool: PgPool) {
    let f = Fixture::new(&pool).await;
    f.finish().await;
    let before = f.snapshot().await;
    sqlx::query("UPDATE projects SET api_tokens='[]' WHERE id=$1")
        .bind(f.project.uuid())
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("UPDATE sandboxes SET desired_state='destroyed',observed_state='destroyed',current_allocation_id=NULL,generation=generation+1 WHERE id=$1")
        .bind(f.sandbox.uuid()).execute(&pool).await.unwrap();
    sqlx::query(
        "UPDATE allocations SET status='released',released_at=clock_timestamp(),release_evidence='{}' WHERE id=$1",
    )
    .bind(f.allocation.uuid())
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query("UPDATE hosts SET supervisor_epoch=2 WHERE id=$1")
        .bind(f.host.uuid())
        .execute(&pool)
        .await
        .unwrap();
    let (claim, work) = f.work().await;
    assert_eq!(work.ticket.owner.host_epoch, 1);
    assert_eq!(work.ticket.owner.generation, 1);
    let p = plans(&work);
    f.store.save_output_plans(&claim, &p, true).await.unwrap();
    f.store
        .publish_output(&claim, &refs(&p), true)
        .await
        .unwrap();
    assert_eq!(before, f.snapshot().await);
    let (reserved,): (i64,) =
        sqlx::query_as("SELECT count(*) FROM allocations WHERE released_at IS NULL")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(reserved, 0);
}

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn tenant_visibility_simulation_policy_and_project_deletion(pool: PgPool) {
    let f = Fixture::new(&pool).await;
    f.finish().await;
    assert!(
        f.store
            .output_for_project(ProjectId::generate(), f.operation)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        f.store
            .output_for_project(f.project, OperationId::generate())
            .await
            .unwrap()
            .is_none()
    );
    let claim = f.store.claim_output(30).await.unwrap().unwrap();
    assert!(matches!(
        f.store.prepare_output(&claim, 3600, 600, false).await,
        Err(OutputError::SimulationDenied)
    ));
    let work = f
        .store
        .prepare_output(&claim, 3600, 600, true)
        .await
        .unwrap();
    let p = plans(&work);
    assert!(matches!(
        f.store.save_output_plans(&claim, &p, false).await,
        Err(OutputError::SimulationDenied)
    ));
    f.store.save_output_plans(&claim, &p, true).await.unwrap();
    assert!(matches!(
        f.store.publish_output(&claim, &refs(&p), false).await,
        Err(OutputError::SimulationDenied)
    ));
    sqlx::query("UPDATE projects SET status='suspended' WHERE id=$1")
        .bind(f.project.uuid())
        .execute(&pool)
        .await
        .unwrap();
    assert!(
        f.store
            .output_for_project(f.project, f.operation)
            .await
            .unwrap()
            .is_none()
    );
    // Archival of existing work may proceed during suspension, but deletion
    // cannot create or select new retained customer objects.
    f.store
        .prepare_output(&claim, 3600, 600, true)
        .await
        .unwrap();
    sqlx::query("UPDATE projects SET status='deleting' WHERE id=$1")
        .bind(f.project.uuid())
        .execute(&pool)
        .await
        .unwrap();
    assert!(matches!(
        f.store.publish_output(&claim, &refs(&p), true).await,
        Err(OutputError::ProjectDeleting)
    ));
}

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn publication_expires_without_rewriting_execution_or_restarting_retention(pool: PgPool) {
    let f = Fixture::new(&pool).await;
    f.finish().await;
    // Set authoritative completion into the past, before the first archive
    // claim. Retention must not start over when that claim finally arrives.
    sqlx::query(
        "UPDATE operations SET completed_at=clock_timestamp()-interval '1 hour' WHERE id=$1",
    )
    .bind(f.operation.uuid())
    .execute(&pool)
    .await
    .unwrap();
    let before = f.snapshot().await;
    let claim = f.store.claim_output(30).await.unwrap().unwrap();
    assert!(matches!(
        f.store.prepare_output(&claim, 10, 600, true).await,
        Err(OutputError::Expired)
    ));
    let view = f
        .store
        .output_for_project(f.project, f.operation)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(view.status, "expired");
    assert!(view.references.is_none());
    assert_eq!(before, f.snapshot().await);
    assert!(f.store.claim_output(30).await.unwrap().is_none());
}

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn expiry_during_upload_blocks_publication_and_retained_reads(pool: PgPool) {
    let f = Fixture::new(&pool).await;
    f.finish().await;
    let (claim, work) = f.work().await;
    let p = plans(&work);
    f.store.save_output_plans(&claim, &p, true).await.unwrap();
    sqlx::query("UPDATE operations SET response_expires_at=clock_timestamp()-interval '1 second' WHERE id=$1")
        .bind(f.operation.uuid()).execute(&pool).await.unwrap();
    assert!(matches!(
        f.store.publish_output(&claim, &refs(&p), true).await,
        Err(OutputError::Expired)
    ));
    assert_eq!(
        f.store
            .output_for_project(f.project, f.operation)
            .await
            .unwrap()
            .unwrap()
            .status,
        "expired"
    );
    assert!(matches!(
        f.store.prepare_output(&claim, 3600, 600, true).await,
        Err(OutputError::Expired)
    ));
}

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn expired_claim_while_waiting_for_project_lock_cannot_persist_ticket(pool: PgPool) {
    let f = Fixture::new(&pool).await;
    f.finish().await;
    let mut lock = pool.begin().await.unwrap();
    sqlx::query("SELECT id FROM projects WHERE id=$1 FOR UPDATE")
        .bind(f.project.uuid())
        .fetch_one(&mut *lock)
        .await
        .unwrap();
    // Finish setup before starting the lease. A one-second lease can expire
    // before the worker reaches the intended project lock on a loaded machine.
    let claim = f.store.claim_output(10).await.unwrap().unwrap();
    let expires_at = claim.lease_expires_at;
    let store = f.store.clone();
    let waiting = tokio::spawn(async move { store.prepare_output(&claim, 3600, 600, true).await });
    // Wait for a real blocked query, then for the DB lease to expire. The lock
    // is held by this test, so releasing it controls the resumed transaction.
    let mut blocked = false;
    for _ in 0..500 {
        let (n,):(i64,)=sqlx::query_as("SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock'")
            .fetch_one(&pool).await.unwrap();
        if n > 0 {
            blocked = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    assert!(blocked);
    sqlx::query("SELECT pg_sleep(GREATEST(0.0,EXTRACT(EPOCH FROM ($1::timestamptz-clock_timestamp())))::double precision+0.05)")
        .bind(expires_at)
        .execute(&pool)
        .await
        .unwrap();
    lock.commit().await.unwrap();
    assert!(matches!(
        waiting.await.unwrap(),
        Err(OutputError::LostClaim)
    ));
    let row = sqlx::query("SELECT output_ticket,output_status FROM operations WHERE id=$1")
        .bind(f.operation.uuid())
        .fetch_one(&pool)
        .await
        .unwrap();
    assert!(row.get::<Option<Value>, _>("output_ticket").is_none());
    assert_eq!(row.get::<String, _>("output_status"), "pending");
}

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn corrupt_execution_or_published_metadata_never_selects_output(pool: PgPool) {
    let f = Fixture::new(&pool).await;
    f.finish().await;
    let (claim, work) = f.work().await;
    let p = plans(&work);
    f.store.save_output_plans(&claim, &p, true).await.unwrap();
    let original: Value = sqlx::query_scalar("SELECT attempt_receipts FROM operations WHERE id=$1")
        .bind(f.operation.uuid())
        .fetch_one(&pool)
        .await
        .unwrap();
    for path in ["boot", "digest", "owner", "stats", "state"] {
        let mut changed = original.clone();
        match path {
            "boot" => changed[1]["guest_receipt"]["context"]["boot_id"] = json!("different"),
            "digest" => changed[0]["command_digest"] = json!("f".repeat(64)),
            "owner" => changed[1]["host_id"] = json!(HostId::generate().to_string()),
            "stats" => changed[1]["guest_receipt"]["stdout"]["stored"] = json!(5),
            _ => changed[1]["guest_receipt"]["state"] = json!("unknown"),
        }
        sqlx::query("UPDATE operations SET attempt_receipts=$2 WHERE id=$1")
            .bind(f.operation.uuid())
            .bind(changed)
            .execute(&pool)
            .await
            .unwrap();
        assert!(
            matches!(
                f.store.publish_output(&claim, &refs(&p), true).await,
                Err(OutputError::Corrupt)
            ),
            "{path}"
        );
    }
    sqlx::query("UPDATE operations SET attempt_receipts=$2 WHERE id=$1")
        .bind(f.operation.uuid())
        .bind(original)
        .execute(&pool)
        .await
        .unwrap();
    f.store
        .publish_output(&claim, &refs(&p), true)
        .await
        .unwrap();
    sqlx::query("UPDATE operations SET output_refs=jsonb_set(output_refs,'{0,plan,sha256}',to_jsonb(repeat('b',64))) WHERE id=$1")
        .bind(f.operation.uuid()).execute(&pool).await.unwrap();
    assert!(matches!(
        f.store.output_for_project(f.project, f.operation).await,
        Err(OutputError::Corrupt)
    ));
}

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn deferred_claims_and_database_constraints_keep_work_bounded(pool: PgPool) {
    let f = Fixture::new(&pool).await;
    f.finish().await;
    assert!(matches!(
        f.store.claim_output(0).await,
        Err(OutputError::InvalidPolicy)
    ));
    let claim = f.store.claim_output(30).await.unwrap().unwrap();
    assert!(matches!(
        f.store.prepare_output(&claim, 0, 600, true).await,
        Err(OutputError::InvalidPolicy)
    ));
    f.store.renew_output_claim(&claim, 60).await.unwrap();
    f.store.defer_output(&claim, 30).await.unwrap();
    assert!(f.store.claim_output(30).await.unwrap().is_none());
    assert!(matches!(
        f.store.defer_output(&claim, 1).await,
        Err(OutputError::LostClaim)
    ));
    for assignment in [
        "output_status='published'",
        "output_claim_revision=-1",
        "output_plan='{}'",
        "output_status='expired'",
        "output_status='unsupported'",
    ] {
        let sql = format!("UPDATE operations SET {assignment} WHERE id=$1");
        let error = sqlx::query(&sql)
            .bind(f.operation.uuid())
            .execute(&pool)
            .await
            .unwrap_err();
        assert_eq!(error.as_database_error().unwrap().code().unwrap(), "23514");
    }
}

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn simultaneous_publications_select_one_reference_pair(pool: PgPool) {
    let f = Fixture::new(&pool).await;
    f.finish().await;
    let (claim, work) = f.work().await;
    let p = plans(&work);
    f.store.save_output_plans(&claim, &p, true).await.unwrap();
    let a = refs(&p);
    let mut b = a.clone();
    b.stdout.etag = "another-etag".into();
    let (x, y) = tokio::join!(
        f.store.publish_output(&claim, &a, true),
        f.store.publish_output(&claim, &b, true)
    );
    assert_eq!(usize::from(x.is_ok()) + usize::from(y.is_ok()), 1);
    let chosen = if x.is_ok() {
        assert!(matches!(y, Err(OutputError::LostClaim)));
        a
    } else {
        assert!(matches!(x, Err(OutputError::LostClaim)));
        b
    };
    assert_eq!(
        f.store
            .output_for_project(f.project, f.operation)
            .await
            .unwrap()
            .unwrap()
            .references,
        Some(chosen)
    );
}

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn public_metadata_and_references_expire_even_without_a_cleanup_worker(pool: PgPool) {
    let f = Fixture::new(&pool).await;
    f.finish().await;
    let (claim, work) = f.work().await;
    let p = plans(&work);
    f.store.save_output_plans(&claim, &p, true).await.unwrap();
    f.store
        .publish_output(&claim, &refs(&p), true)
        .await
        .unwrap();
    sqlx::query("UPDATE operations SET response_expires_at=clock_timestamp()-interval '1 second' WHERE id=$1")
        .bind(f.operation.uuid()).execute(&pool).await.unwrap();
    let view = f
        .store
        .output_for_project(f.project, f.operation)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(view.status, "expired");
    assert!(view.references.is_none());
    assert_eq!(
        f.store
            .operation_for_project(f.project, f.operation)
            .await
            .unwrap()
            .unwrap()
            .output_status
            .as_deref(),
        Some("expired")
    );
    let (persisted, count): (String, i32) = sqlx::query_as(
        "SELECT output_status,jsonb_array_length(output_refs) FROM operations WHERE id=$1",
    )
    .bind(f.operation.uuid())
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!((persisted, count), ("published".into(), 2)); // Keep cleanup evidence.
}

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn only_confirmed_final_receipts_queue_output(pool: PgPool) {
    for state in [
        State::Exited,
        State::TimedOut,
        State::Cancelled,
        State::Unknown,
        State::LaunchIntent,
    ] {
        let mut f = Fixture::new(&pool).await;
        let mut receipt: Receipt = f.observation.receipt.clone().unwrap().try_into().unwrap();
        receipt.state = state;
        receipt.exit_code = if state == State::Exited {
            Some(7)
        } else {
            None
        };
        receipt.cleanup_confirmed = state != State::LaunchIntent;
        receipt.cancel_requested = state == State::Cancelled;
        f.observation.receipt = Some((&receipt).into());
        f.finish().await;
        let expected = if matches!(state, State::Exited | State::TimedOut | State::Cancelled) {
            "pending"
        } else {
            "none"
        };
        let (status,): (String,) =
            sqlx::query_as("SELECT output_status FROM operations WHERE id=$1")
                .bind(f.operation.uuid())
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(status, expected);
        // Leave no live execution for Fixture::new's global test claim query.
        if expected == "none" {
            sqlx::query("UPDATE operations SET next_retry_at=clock_timestamp()+interval '1 hour' WHERE id=$1")
                .bind(f.operation.uuid()).execute(&pool).await.unwrap();
        }
    }
}

#[sqlx::test(migrations = false)]
async fn publication_upgrade_preserves_old_outcomes_and_only_queues_receipt_candidates(
    pool: PgPool,
) {
    use sqlx::migrate::Migrator;
    use std::borrow::Cow;
    let old = Migrator {
        migrations: Cow::Owned(sandbox_store::MIGRATOR.iter().take(5).cloned().collect()),
        ..Migrator::DEFAULT
    };
    old.run(&pool).await.unwrap();
    let f = Fixture::with_schema(&pool, true).await;
    let mut history: Value =
        sqlx::query_scalar("SELECT attempt_receipts FROM operations WHERE id=$1")
            .bind(f.operation.uuid())
            .fetch_one(&pool)
            .await
            .unwrap();
    let mut observed = history[0].clone();
    observed["phase"] = json!("command_observed");
    observed["simulated"] = json!(true);
    observed["guest_receipt"] =
        serde_json::to_value(Receipt::try_from(f.observation.receipt.clone().unwrap()).unwrap())
            .unwrap();
    history.as_array_mut().unwrap().push(observed);
    sqlx::query("UPDATE operations SET status='succeeded',phase='exited',completed_at=clock_timestamp(),lease_expires_at=NULL,attempt_receipts=$2 WHERE id=$1")
        .bind(f.operation.uuid()).bind(history).execute(&pool).await.unwrap();
    let legacy = OperationId::generate();
    sqlx::query("INSERT INTO operations(id,project_id,sandbox_id,kind,initiator_kind,idempotency_key,request_digest,digest_version,payload,status)
        VALUES($1,$2,$3,'execute','service',$4,$5,1,'{}','unknown')")
        .bind(legacy.uuid()).bind(f.project.uuid()).bind(f.sandbox.uuid()).bind(legacy.to_string()).bind(vec![0u8;32]).execute(&pool).await.unwrap();
    let before: Value =
        sqlx::query_scalar("SELECT jsonb_agg(to_jsonb(o) ORDER BY id) FROM operations o")
            .fetch_one(&pool)
            .await
            .unwrap();
    sandbox_store::MIGRATOR.run(&pool).await.unwrap();
    sandbox_store::MIGRATOR.run(&pool).await.unwrap();
    let mut after:Value=sqlx::query_scalar("SELECT jsonb_agg(to_jsonb(o)-ARRAY['output_status','output_claim_revision','output_lease_expires_at','output_next_retry_at','output_ticket','output_plan','output_expires_at'] ORDER BY id) FROM operations o")
        .fetch_one(&pool).await.unwrap();
    for row in after.as_array_mut().unwrap() {
        for key in [
            "payload_compacted_at",
            "command_summary",
            "payload_compaction_next_at",
            "file_allocation_id",
        ] {
            assert_eq!(row.as_object_mut().unwrap().remove(key), Some(Value::Null));
        }
    }
    assert_eq!(before, after);
    let (status,): (String,) = sqlx::query_as("SELECT output_status FROM operations WHERE id=$1")
        .bind(legacy.uuid())
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(status, "none");
    let (claim, work) = f.work().await;
    assert_eq!(claim.operation_id, f.operation);
    assert_eq!(work.ticket.owner.allocation_id, f.allocation);
}

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn expired_claim_after_lock_wait_cannot_save_plans_or_publish(pool: PgPool) {
    for publish in [false, true] {
        let f = Fixture::new(&pool).await;
        f.finish().await;
        let (claim, work) = f.work().await;
        let p = plans(&work);
        if publish {
            f.store.save_output_plans(&claim, &p, true).await.unwrap();
        }
        f.store.renew_output_claim(&claim, 1).await.unwrap();
        let mut lock = pool.begin().await.unwrap();
        sqlx::query("SELECT id FROM projects WHERE id=$1 FOR UPDATE")
            .bind(f.project.uuid())
            .fetch_one(&mut *lock)
            .await
            .unwrap();
        let store = f.store.clone();
        let waiting = tokio::spawn(async move {
            if publish {
                store.publish_output(&claim, &refs(&p), true).await
            } else {
                store.save_output_plans(&claim, &p, true).await
            }
        });
        let mut blocked = false;
        for _ in 0..100 {
            let (n,):(i64,)=sqlx::query_as("SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock'")
                .fetch_one(&pool).await.unwrap();
            if n > 0 {
                blocked = true;
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        assert!(blocked);
        sqlx::query("SELECT pg_sleep(1.1)")
            .execute(&pool)
            .await
            .unwrap();
        lock.commit().await.unwrap();
        assert!(matches!(
            waiting.await.unwrap(),
            Err(OutputError::LostClaim)
        ));
        let row = sqlx::query("SELECT output_plan,output_refs FROM operations WHERE id=$1")
            .bind(f.operation.uuid())
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(
            row.get::<Option<Value>, _>("output_plan").is_some(),
            publish
        );
        assert_eq!(row.get::<Value, _>("output_refs"), json!([]));
        // Prevent the next fixture's generic claim query selecting this work.
        let replacement = f.store.claim_output(30).await.unwrap().unwrap();
        f.store.defer_output(&replacement, 3600).await.unwrap();
    }
}

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn stream_scope_uses_retained_execution_boot_without_claiming_or_current_pointer(
    pool: PgPool,
) {
    use sandbox_store::stream::StreamSource;
    let mut f = Fixture::new(&pool).await;
    assert!(matches!(
        f.store
            .stream_for_project(f.project, f.operation)
            .await
            .unwrap()
            .unwrap()
            .source,
        StreamSource::Pending
    ));
    let wire = f.observation.receipt.as_mut().unwrap();
    wire.state = sandbox_protocol::guest::State::LaunchIntent as i32;
    wire.exit_code = None;
    wire.cleanup_confirmed = false;
    f.finish().await;
    let before = f.snapshot().await;
    sqlx::query("UPDATE sandboxes SET current_allocation_id=NULL WHERE id=$1")
        .bind(f.sandbox.uuid())
        .execute(&pool)
        .await
        .unwrap();
    let StreamSource::Live { scope, simulated } = f
        .store
        .stream_for_project(f.project, f.operation)
        .await
        .unwrap()
        .unwrap()
        .source
    else {
        panic!("live scope")
    };
    assert!(simulated);
    assert_eq!(scope.owner.allocation_id, f.allocation);
    assert_eq!(scope.owner.boot_id, "pinned-guest-boot");
    assert_eq!(scope.owner.project_id, f.project);
    assert_eq!(scope.owner.operation_id, f.operation);
    assert_eq!(before, f.snapshot().await);
    assert!(
        f.store
            .stream_for_project(ProjectId::generate(), f.operation)
            .await
            .unwrap()
            .is_none()
    );
    sqlx::query("UPDATE projects SET status='suspended' WHERE id=$1")
        .bind(f.project.uuid())
        .execute(&pool)
        .await
        .unwrap();
    assert!(
        f.store
            .stream_for_project(f.project, f.operation)
            .await
            .unwrap()
            .is_none()
    );
}

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn stream_rejects_corrupt_intent_or_receipt_and_expired_or_old_epoch_history(pool: PgPool) {
    use sandbox_store::stream::StreamSource;
    let f = Fixture::new(&pool).await;
    f.finish().await;
    let original: Value = sqlx::query_scalar("SELECT payload FROM operations WHERE id=$1")
        .bind(f.operation.uuid())
        .fetch_one(&pool)
        .await
        .unwrap();
    let mut changed = original.clone();
    changed["argv"] = json!(["different"]);
    sqlx::query("UPDATE operations SET payload=$2 WHERE id=$1")
        .bind(f.operation.uuid())
        .bind(changed)
        .execute(&pool)
        .await
        .unwrap();
    assert!(matches!(
        f.store.stream_for_project(f.project, f.operation).await,
        Err(OutputError::Corrupt)
    ));
    sqlx::query("UPDATE operations SET payload=$2 WHERE id=$1")
        .bind(f.operation.uuid())
        .bind(original)
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
            .stream_for_project(f.project, f.operation)
            .await
            .unwrap()
            .unwrap()
            .source,
        StreamSource::Missing
    ));
    sqlx::query("UPDATE operations SET response_expires_at=clock_timestamp()-interval '1 second' WHERE id=$1").bind(f.operation.uuid()).execute(&pool).await.unwrap();
    assert!(matches!(
        f.store
            .stream_for_project(f.project, f.operation)
            .await
            .unwrap()
            .unwrap()
            .source,
        StreamSource::Expired
    ));
}

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn stream_prefers_verified_publication_after_guest_is_gone(pool: PgPool) {
    use sandbox_store::stream::StreamSource;
    let f = Fixture::new(&pool).await;
    f.finish().await;
    let (claim, work) = f.work().await;
    let p = plans(&work);
    let r = refs(&p);
    f.store.save_output_plans(&claim, &p, true).await.unwrap();
    f.store.publish_output(&claim, &r, true).await.unwrap();
    sqlx::query("UPDATE hosts SET supervisor_epoch=2 WHERE id=$1")
        .bind(f.host.uuid())
        .execute(&pool)
        .await
        .unwrap();
    let before = f.snapshot().await;
    let StreamSource::Archived(v) = f
        .store
        .stream_for_project(f.project, f.operation)
        .await
        .unwrap()
        .unwrap()
        .source
    else {
        panic!("archived")
    };
    assert_eq!(v.references, Some(r));
    assert_eq!(before, f.snapshot().await);
}
