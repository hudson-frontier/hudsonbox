use super::*;
use sandbox_store::storage_audit::{Entry, Error};

async fn upload_fixture(pool: &PgPool) -> (Fixture, sandbox_protocol::file_sources::SourcePlan) {
    use sandbox_store::uploads::{UploadAdmission, UploadCommand, UploadInput};
    let f = Fixture::new(pool).await;
    let token = ProjectToken::generate().unwrap();
    sqlx::query("UPDATE projects SET api_tokens=$2 WHERE id=$1")
        .bind(f.project.uuid())
        .bind(
            json!([{"key_id":token.key_id().as_str(),"hash":hex::encode(token.hash().as_bytes())}]),
        )
        .execute(pool)
        .await
        .unwrap();
    let request = UploadCommand {
        project_id: f.project,
        sandbox_id: f.sandbox,
        key_id: token.key_id().clone(),
        key: IdempotencyKey::parse(&OperationId::generate().to_string()).unwrap(),
        input: UploadInput {
            path: "private-file".into(),
            size: 3,
            sha256: [1; 32],
            mode: 0o644,
        },
    };
    let admission = f.store.admit_upload(&request, &token.hash()).await.unwrap();
    let UploadAdmission::Accepted { plan, .. } = admission else {
        panic!("upload admission: {admission:?}")
    };
    (f, *plan)
}

async fn ledger(pool: &PgPool) -> Value {
    sqlx::query_scalar(
        "SELECT jsonb_build_array(
        (SELECT jsonb_agg(to_jsonb(o) ORDER BY id) FROM operations o),
        (SELECT jsonb_agg(to_jsonb(c) ORDER BY operation_id) FROM output_cleanup c),
        (SELECT jsonb_agg(to_jsonb(f) ORDER BY operation_id) FROM file_uploads f),
        (SELECT jsonb_agg(to_jsonb(a) ORDER BY id) FROM allocations a))",
    )
    .fetch_one(pool)
    .await
    .unwrap()
}

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn storage_audit_paginates_one_snapshot_and_preserves_the_ledger(pool: PgPool) {
    let mut fixtures = Vec::new();
    for _ in 0..18 {
        fixtures.push(published(&pool).await.0);
    }
    let before = ledger(&pool).await;
    let store = Store::from_pool(pool.clone());
    let mut audit = store.storage_audit(true).await.unwrap();
    let first = audit.next_page().await.unwrap();
    assert_eq!(first.len(), 32);
    // A snapshot must not mix a newly published operation or a concurrently
    // altered owner into later pages. A fresh audit must reject the corruption.
    let added = published(&pool).await.0;
    let last = fixtures.last().unwrap();
    sqlx::query("UPDATE operations SET output_ticket=jsonb_set(output_ticket,'{owner,boot_id}','\"changed-after-snapshot\"') WHERE id=$1")
        .bind(last.operation.uuid()).execute(&pool).await.unwrap();
    let after_mutation = ledger(&pool).await;
    let second = audit.next_page().await.unwrap();
    assert_eq!(second.len(), 4);
    for entry in first.into_iter().chain(second) {
        let Entry::Output(reference) = entry else {
            panic!("payload reference")
        };
        assert_ne!(reference.plan.owner.operation_id, added.operation);
        assert_ne!(reference.plan.owner.boot_id, "changed-after-snapshot");
    }
    assert!(audit.next_page().await.unwrap().is_empty());
    assert_eq!(ledger(&pool).await, after_mutation);
    assert_ne!(before, after_mutation);
    let mut fresh = store.storage_audit(true).await.unwrap();
    assert_eq!(fresh.next_page().await.unwrap().len(), 32);
    assert!(matches!(fresh.next_page().await, Err(Error::Evidence)));
    assert!(matches!(fresh.next_page().await, Err(Error::Failed)));
}

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn storage_audit_requires_explicit_simulation_and_preserves_unresolved_uploads(pool: PgPool) {
    let f = Fixture::new(&pool).await;
    f.finish().await;
    let (claim, work) = f.work().await;
    f.store
        .save_output_plans(&claim, &plans(&work), true)
        .await
        .unwrap();
    let before = ledger(&pool).await;
    let mut audit = f.store.storage_audit(false).await.unwrap();
    assert!(matches!(
        audit.next_page().await,
        Err(Error::SimulationDenied)
    ));
    let mut audit = f.store.storage_audit(true).await.unwrap();
    assert!(matches!(audit.next_page().await, Err(Error::Unresolved)));
    assert!(matches!(audit.next_page().await, Err(Error::Failed)));
    assert_eq!(ledger(&pool).await, before);
}

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn storage_audit_retains_exact_cleanup_receipts_after_payload_compaction(pool: PgPool) {
    let (f, claim, receipt) = prepared(&pool).await;
    f.store
        .complete_output_cleanup(&claim, &receipt, true)
        .await
        .unwrap();
    sqlx::query("UPDATE operations SET response_expires_at=clock_timestamp()-interval '1 second' WHERE id=$1")
        .bind(f.operation.uuid()).execute(&pool).await.unwrap();
    assert!(
        matches!(f.store.compact_expired_response().await.unwrap(), sandbox_store::compaction::Compaction::Completed(id) if id == f.operation)
    );
    let before = ledger(&pool).await;
    let mut audit = f.store.storage_audit(true).await.unwrap();
    let entries = audit.next_page().await.unwrap();
    let CleanupCompletion::Retired { stdout, stderr } = &receipt else {
        panic!("markers")
    };
    assert_eq!(entries.len(), 2);
    for (entry, expected) in entries.into_iter().zip([stdout, stderr]) {
        let Entry::RetiredOutput { plan, receipt } = entry else {
            panic!("retired output")
        };
        assert_eq!(&receipt, expected.as_ref());
        assert_eq!(plan.owner.operation_id, f.operation);
    }
    assert!(audit.next_page().await.unwrap().is_empty());
    assert_eq!(ledger(&pool).await, before);
    sqlx::query("UPDATE output_cleanup SET receipt=jsonb_set(receipt,'{stdout,plan_sha256}',to_jsonb(repeat('0',64)))")
        .execute(&pool).await.unwrap();
    let mut fresh = f.store.storage_audit(true).await.unwrap();
    assert!(matches!(fresh.next_page().await, Err(Error::Evidence)));
}

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn storage_audit_does_not_confuse_unissued_output_with_missing_storage_evidence(
    pool: PgPool,
) {
    for _ in 0..18 {
        let f = Fixture::new(&pool).await;
        f.finish().await;
    }
    let (f, _) = published(&pool).await;
    let before = ledger(&pool).await;
    let mut audit = f.store.storage_audit(true).await.unwrap();
    assert_eq!(audit.next_page().await.unwrap().len(), 2);
    assert!(audit.next_page().await.unwrap().is_empty());
    assert_eq!(ledger(&pool).await, before);
}

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn storage_audit_checks_upload_owners_and_retired_source_receipts(pool: PgPool) {
    use sandbox_protocol::file_sources::{SourceRef, SourceRetirement};
    let (f, mut plan) = upload_fixture(&pool).await;
    let mut audit = f.store.storage_audit(false).await.unwrap();
    assert!(matches!(audit.next_page().await, Err(Error::Unresolved)));
    drop(audit);
    let mut reference = SourceRef {
        plan: plan.clone(),
        etag: "source-etag".into(),
        object_version: Some("source-version".into()),
    };
    let claim = f
        .store
        .claim_next(OperationKind::FileWrite, 30)
        .await
        .unwrap()
        .unwrap();
    f.store
        .accept_upload_source(&claim, &reference)
        .await
        .unwrap();
    let before = ledger(&pool).await;
    let mut audit = f.store.storage_audit(false).await.unwrap();
    let entries = audit.next_page().await.unwrap();
    assert!(matches!(entries.as_slice(), [Entry::Source(r)] if r == &reference));
    assert!(audit.next_page().await.unwrap().is_empty());
    assert_eq!(ledger(&pool).await, before);
    drop(audit);
    // Shift the complete fixture policy together to make cleanup eligible.
    plan.created_unix_ms = now() - 6000;
    plan.write_expires_unix_ms = now() - 5000;
    plan.expires_unix_ms = now() - 4000;
    plan.delete_after_unix_ms = now() - 3000;
    reference.plan = plan.clone();
    sqlx::query("UPDATE file_uploads SET plan=$2,source_ref=$3 WHERE operation_id=$1")
        .bind(plan.owner.operation_id.uuid())
        .bind(json!(plan))
        .bind(json!(reference))
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("UPDATE operations SET deadline=clock_timestamp()-interval '6 minutes',lease_expires_at=NULL WHERE id=$1")
        .bind(plan.owner.operation_id.uuid()).execute(&pool).await.unwrap();
    let cleanup = f
        .store
        .claim_file_source_cleanup(30)
        .await
        .unwrap()
        .unwrap();
    let receipt = SourceRetirement {
        version: 1,
        plan_sha256: plan.metadata_digest().unwrap(),
        previous: Some(reference),
        marker_etag: "retired-etag".into(),
        marker_version: Some("retired-version".into()),
    };
    f.store
        .complete_file_source_cleanup(&cleanup, &receipt)
        .await
        .unwrap();
    let before = ledger(&pool).await;
    let mut audit = f.store.storage_audit(false).await.unwrap();
    let entries = audit.next_page().await.unwrap();
    assert!(
        matches!(entries.as_slice(), [Entry::RetiredSource { plan:p, receipt:r }] if p == &plan && r == &receipt)
    );
    assert!(audit.next_page().await.unwrap().is_empty());
    assert_eq!(ledger(&pool).await, before);
    drop(audit);
    sqlx::query("UPDATE file_uploads SET plan=jsonb_set(plan,'{owner,scope,host_epoch}','99') WHERE operation_id=$1")
        .bind(plan.owner.operation_id.uuid()).execute(&pool).await.unwrap();
    let mut corrupt = f.store.storage_audit(false).await.unwrap();
    assert!(matches!(corrupt.next_page().await, Err(Error::Evidence)));
}

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn storage_audit_refuses_changed_or_incomplete_migration_authority(pool: PgPool) {
    let store = Store::from_pool(pool.clone());
    let mut audit = store.storage_audit(false).await.unwrap();
    assert!(audit.next_page().await.unwrap().is_empty());
    drop(audit);
    sqlx::query("UPDATE _sqlx_migrations SET checksum=$1 WHERE version=(SELECT min(version) FROM _sqlx_migrations)")
        .bind(vec![0u8;48]).execute(&pool).await.unwrap();
    assert!(matches!(
        store.storage_audit(false).await,
        Err(Error::Schema)
    ));
}
