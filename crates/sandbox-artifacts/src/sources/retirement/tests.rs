#![allow(clippy::unwrap_used, clippy::expect_used)]
use super::*;
use crate::sources::tests::{memory, owner, plan};
use crate::tests::TESTS;
use object_store::ObjectStoreExt;

#[tokio::test]
async fn recovery_verifies_pinned_bytes_without_extending_customer_read_authority() {
    let _test = TESTS.lock().await;
    let store = memory();
    let p = plan(b"private-recovery-bytes");
    let reference = store
        .upload(&p, &p.owner, 1000, b"private-recovery-bytes")
        .await
        .unwrap();
    store.verify_reference(&reference, &p.owner).await.unwrap();
    assert!(matches!(
        store.read(&reference, &p.owner, 3000).await,
        Err(Error::Expired)
    ));
    assert_eq!(
        store.verify_reference(&reference, &owner()).await,
        Err(Error::OwnerMismatch)
    );
    let mut wrong = reference.clone();
    wrong.etag = "wrong-etag".into();
    assert!(store.verify_reference(&wrong, &p.owner).await.is_err());
    let permit = TRANSFERS.acquire_many(4).await.unwrap();
    assert_eq!(
        store.verify_reference(&reference, &p.owner).await,
        Err(Error::Busy)
    );
    drop(permit);
    let path = Path::from(p.object_key().unwrap());
    store
        .store
        .inner
        .put(&path, b"restored-corrupt-bytes".to_vec().into())
        .await
        .unwrap();
    assert!(store.verify_reference(&reference, &p.owner).await.is_err());
    assert_eq!(
        store
            .store
            .inner
            .get(&path)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap(),
        b"restored-corrupt-bytes"[..]
    );
}

#[tokio::test]
async fn recovery_requires_exact_marker_without_repairing_or_reopening_retired_keys() {
    let _test = TESTS.lock().await;
    for uploaded in [false, true] {
        let store = memory();
        let p = plan(b"retired");
        let reference = if uploaded {
            Some(store.upload(&p, &p.owner, 1000, b"retired").await.unwrap())
        } else {
            None
        };
        let receipt = retirer(&store)
            .retire(&p, &p.owner, reference.as_ref(), 3000)
            .await
            .unwrap();
        store
            .verify_retirement(&p, &p.owner, &receipt)
            .await
            .unwrap();
        assert_eq!(
            store.verify_retirement(&p, &owner(), &receipt).await,
            Err(Error::OwnerMismatch)
        );
        let mut wrong = receipt.clone();
        wrong.marker_etag = "different-marker".into();
        assert_eq!(
            store.verify_retirement(&p, &p.owner, &wrong).await,
            Err(Error::Conflict)
        );
        wrong = receipt.clone();
        wrong.plan_sha256 = "0".repeat(64);
        assert_eq!(
            store.verify_retirement(&p, &p.owner, &wrong).await,
            Err(Error::InvalidMetadata)
        );
        let path = Path::from(p.object_key().unwrap());
        let before = store.store.inner.head(&path).await.unwrap();
        let permit = TRANSFERS.acquire_many(4).await.unwrap();
        assert_eq!(
            store.verify_retirement(&p, &p.owner, &receipt).await,
            Err(Error::Busy)
        );
        drop(permit);
        store
            .verify_retirement(&p, &p.owner, &receipt)
            .await
            .unwrap();
        assert_eq!(store.store.inner.head(&path).await.unwrap(), before);
        store.store.inner.delete(&path).await.unwrap();
        assert_eq!(
            store.verify_retirement(&p, &p.owner, &receipt).await,
            Err(Error::Missing)
        );
        assert!(matches!(
            store.store.inner.head(&path).await,
            Err(object_store::Error::NotFound { .. })
        ));
        // Restoring payload bytes over a forgotten fence must be rejected, not
        // "fixed" by installing another marker with a new storage identity.
        store
            .store
            .inner
            .put_opts(
                &path,
                b"retired".to_vec().into(),
                PutOptions {
                    mode: PutMode::Create,
                    attributes: Attributes::from_iter([(
                        crate::sources::metadata_key(),
                        p.metadata_digest().unwrap(),
                    )]),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert!(matches!(
            store.verify_retirement(&p, &p.owner, &receipt).await,
            Err(Error::Conflict | Error::Corrupt)
        ));
        assert_eq!(
            store
                .store
                .inner
                .get(&path)
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap(),
            b"retired"[..]
        );
    }
}

#[tokio::test]
#[ignore = "requires private versioned MinIO bucket"]
async fn output_minio_file_source_recovery_rejects_recreated_marker_identity() {
    let _test = TESTS.lock().await;
    let store = config(true).build_sources().unwrap();
    let cleaner = config(true).build_source_retirer().unwrap();
    let p = plan(b"retained-version");
    let reference = store
        .upload(&p, &p.owner, 1000, b"retained-version")
        .await
        .unwrap();
    store.verify_reference(&reference, &p.owner).await.unwrap();
    let receipt = cleaner
        .retire(&p, &p.owner, Some(&reference), 3000)
        .await
        .unwrap();
    store
        .verify_retirement(&p, &p.owner, &receipt)
        .await
        .unwrap();
    let path = Path::from(p.object_key().unwrap());
    let current = store.store.inner.get(&path).await.unwrap();
    let attributes = current.attributes.clone();
    let bytes = current.bytes().await.unwrap();
    let replacement = store
        .store
        .inner
        .put_opts(
            &path,
            bytes.into(),
            PutOptions {
                attributes,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_ne!(replacement.version, receipt.marker_version);
    assert_eq!(
        store.verify_retirement(&p, &p.owner, &receipt).await,
        Err(Error::Conflict)
    );
    assert_eq!(
        store.store.inner.head(&path).await.unwrap().version,
        replacement.version
    );
    for version in [
        receipt.marker_version.unwrap(),
        replacement.version.unwrap(),
    ] {
        cleaner.delete.delete(&p, &version).await.unwrap();
    }
}

struct NoDelete;
impl SourceVersionDelete for NoDelete {
    fn delete<'a>(&'a self, _: &'a SourcePlan, _: &'a str) -> BoxFuture<'a, Result<(), Error>> {
        Box::pin(async { panic!("unversioned retirement must never delete a key") })
    }
}
fn retirer(store: &SourceStore) -> SourceRetirer {
    SourceRetirer {
        store: store.clone(),
        delete: Arc::new(NoDelete),
    }
}

#[tokio::test]
async fn binary_and_empty_retirement_keep_retry_barriers() {
    let _test = TESTS.lock().await;
    for bytes in [&b"\x00\xffsecret-output\n"[..], &b""[..]] {
        let store = memory();
        let p = plan(bytes);
        let r = store.upload(&p, &p.owner, 1000, bytes).await.unwrap();
        let cleaner = retirer(&store);
        let receipt = cleaner.retire(&p, &p.owner, Some(&r), 3000).await.unwrap();
        assert_eq!(receipt.previous, Some(r.clone()));
        assert!(receipt.validate(&p).is_ok());
        assert_eq!(
            cleaner.retire(&p, &p.owner, Some(&r), 4000).await.unwrap(),
            receipt
        );
        // Simulates a create-only upload authorized before expiry arriving late.
        assert_eq!(
            store.upload(&p, &p.owner, 1000, bytes).await,
            Err(Error::Conflict)
        );
        assert!(matches!(
            store.read(&r, &p.owner, 1500).await,
            Err(Error::Corrupt)
        ));
        let marker = store
            .store
            .inner
            .get(&Path::from(p.object_key().unwrap()))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert!(!String::from_utf8_lossy(&marker).contains("secret-output"));
        assert!(marker.len() < MAX_MARKER as usize);
    }
}

#[tokio::test]
async fn retirement_checks_owner_selection_retention_and_capacity_before_mutation() {
    let _test = TESTS.lock().await;
    let store = memory();
    let p = plan(b"abc");
    let r = store.upload(&p, &p.owner, 1000, b"abc").await.unwrap();
    let cleaner = retirer(&store);
    assert_eq!(
        cleaner.retire(&p, &owner(), Some(&r), 3000).await,
        Err(Error::OwnerMismatch)
    );
    for now in [0, 1999, 2000, 2999] {
        assert_eq!(
            cleaner.retire(&p, &p.owner, Some(&r), now).await,
            Err(Error::InvalidMetadata)
        );
    }
    let mut wrong = r.clone();
    wrong.etag = "wrong-etag".into();
    assert_eq!(
        cleaner.retire(&p, &p.owner, Some(&wrong), 3000).await,
        Err(Error::Corrupt)
    );
    wrong.plan.owner = owner();
    assert_eq!(
        cleaner.retire(&p, &p.owner, Some(&wrong), 3000).await,
        Err(Error::InvalidMetadata)
    );
    let permit = TRANSFERS.acquire_many(4).await.unwrap();
    assert_eq!(
        cleaner.retire(&p, &p.owner, Some(&r), 3000).await,
        Err(Error::Busy)
    );
    drop(permit);
    assert_eq!(store.read(&r, &p.owner, 1000).await.unwrap().bytes, b"abc");
    assert_eq!(format!("{cleaner:?}"), "SourceRetirer { .. }");
}

#[tokio::test]
async fn missing_or_orphan_output_is_sealed_without_inventing_an_empty_stream() {
    let _test = TESTS.lock().await;
    for uploaded in [false, true] {
        let store = memory();
        let p = plan(b"orphan");
        if uploaded {
            store.upload(&p, &p.owner, 1000, b"orphan").await.unwrap();
        }
        let receipt = retirer(&store)
            .retire(&p, &p.owner, None, 3000)
            .await
            .unwrap();
        assert_eq!(receipt.previous.is_some(), uploaded);
        assert_eq!(
            store.upload(&p, &p.owner, 1000, b"orphan").await,
            Err(Error::Conflict)
        );
    }
}

#[tokio::test]
async fn concurrent_retirement_and_late_upload_converge_to_one_marker() {
    let _test = TESTS.lock().await;
    for _ in 0..8 {
        let store = memory();
        let p = plan(b"late");
        let cleaner = retirer(&store);
        let (a, b, upload) = tokio::join!(
            cleaner.retire(&p, &p.owner, None, 3000),
            cleaner.retire(&p, &p.owner, None, 3000),
            store.upload(&p, &p.owner, 1000, b"late"),
        );
        assert!(a.is_ok() || matches!(a, Err(Error::Conflict | Error::Unavailable)));
        assert!(b.is_ok() || matches!(b, Err(Error::Conflict | Error::Unavailable)));
        assert!(upload.is_ok() || matches!(upload, Err(Error::Conflict | Error::Unavailable)));
        let final_receipt = cleaner.retire(&p, &p.owner, None, 3000).await.unwrap();
        for successful in [a.ok(), b.ok()].into_iter().flatten() {
            assert_eq!(successful, final_receipt);
        }
        assert_eq!(
            store.upload(&p, &p.owner, 1000, b"late").await,
            Err(Error::Conflict)
        );
    }
}

#[tokio::test]
async fn corrupt_data_or_markers_fail_without_deletion() {
    let _test = TESTS.lock().await;
    let store = memory();
    let p = plan(b"abc");
    let r = store.upload(&p, &p.owner, 1000, b"abc").await.unwrap();
    let path = Path::from(p.object_key().unwrap());
    store
        .store
        .inner
        .put(&path, b"other-data".to_vec().into())
        .await
        .unwrap();
    assert_eq!(
        retirer(&store).retire(&p, &p.owner, Some(&r), 3000).await,
        Err(Error::Corrupt)
    );
    assert_eq!(
        store
            .store
            .inner
            .get(&path)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap(),
        b"other-data"[..]
    );
    let missing = plan(b"xyz");
    let path = Path::from(missing.object_key().unwrap());
    let cleaner = retirer(&store);
    let receipt = cleaner
        .retire(&missing, &missing.owner, None, 3000)
        .await
        .unwrap();
    for bytes in [b"garbage".to_vec(), vec![0; MAX_MARKER as usize + 1]] {
        store
            .store
            .inner
            .put_opts(
                &path,
                bytes.clone().into(),
                PutOptions {
                    attributes: Attributes::from_iter([(
                        marker_key(),
                        hex::encode(Sha256::digest(&bytes)),
                    )]),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(
            cleaner.retire(&missing, &missing.owner, None, 3000).await,
            Err(Error::Corrupt)
        );
    }
    let mut bad = receipt.clone();
    bad.version = 2;
    assert!(bad.validate(&missing).is_err());
    bad = receipt.clone();
    bad.marker_etag = "\r\n".into();
    assert!(bad.validate(&missing).is_err());
    bad = receipt;
    bad.previous = Some(r);
    assert!(bad.validate(&missing).is_err());
}

// The delete transport can lose an acknowledgement after a real deletion. Its
// replacement must discover the persisted marker and the now-absent old version.
struct LoseDeleteAck {
    inner: Arc<dyn SourceVersionDelete>,
}
impl SourceVersionDelete for LoseDeleteAck {
    fn delete<'a>(
        &'a self,
        plan: &'a SourcePlan,
        version: &'a str,
    ) -> BoxFuture<'a, Result<(), Error>> {
        Box::pin(async move {
            self.inner.delete(plan, version).await?;
            Err(Error::Unavailable)
        })
    }
}
struct RefuseDelete;
impl SourceVersionDelete for RefuseDelete {
    fn delete<'a>(&'a self, _: &'a SourcePlan, _: &'a str) -> BoxFuture<'a, Result<(), Error>> {
        Box::pin(async { Err(Error::Unavailable) })
    }
}
struct IgnoreDelete;
impl SourceVersionDelete for IgnoreDelete {
    fn delete<'a>(&'a self, _: &'a SourcePlan, _: &'a str) -> BoxFuture<'a, Result<(), Error>> {
        Box::pin(async { Ok(()) })
    }
}
fn config(versioned: bool) -> crate::S3Config {
    let env = |key| std::env::var(key).expect("required MinIO test configuration");
    crate::S3Config {
        endpoint: env("HUDSON_TEST_S3_ENDPOINT"),
        region: "us-east-1".into(),
        bucket: env(if versioned {
            "HUDSON_TEST_S3_VERSIONED_BUCKET"
        } else {
            "HUDSON_TEST_S3_BUCKET"
        }),
        access_key: env("HUDSON_TEST_S3_ACCESS_KEY"),
        secret_key: env("HUDSON_TEST_S3_SECRET_KEY"),
        session_token: None,
        allow_loopback_http: true,
    }
}

#[tokio::test]
#[ignore = "requires private unversioned and versioned MinIO buckets"]
async fn output_minio_file_source_retirement_reclaims_exact_versions_and_recovers_interruptions() {
    let _test = TESTS.lock().await;
    for versioned in [false, true] {
        let store = config(versioned).build_sources().unwrap();
        let cleaner = config(versioned).build_source_retirer().unwrap();
        for bytes in [&b"\x00\xffprivate-versioned-output"[..], &b""[..]] {
            let p = plan(bytes);
            let r = store.upload(&p, &p.owner, 1000, bytes).await.unwrap();
            assert_eq!(r.object_version.is_some(), versioned);
            if versioned {
                let interrupted = SourceRetirer {
                    store: cleaner.store.clone(),
                    delete: Arc::new(RefuseDelete),
                };
                assert_eq!(
                    interrupted.retire(&p, &p.owner, Some(&r), 3000).await,
                    Err(Error::Unavailable)
                );
                // Marker is durable, but old bytes remain: no success receipt.
                assert!(matches!(
                    cleaner
                        .store
                        .current_retirement(&p, Some(&r))
                        .await
                        .unwrap(),
                    Current::Retired(_)
                ));
                assert_eq!(store.fetch(&p, Some(&r)).await.unwrap().1, bytes);
                let Current::Retired(pending) = cleaner
                    .store
                    .current_retirement(&p, Some(&r))
                    .await
                    .unwrap()
                else {
                    panic!("marker missing")
                };
                assert_eq!(
                    store.verify_retirement(&p, &p.owner, &pending).await,
                    Err(Error::Conflict)
                );
                let false_ack = SourceRetirer {
                    store: cleaner.store.clone(),
                    delete: Arc::new(IgnoreDelete),
                };
                assert_eq!(
                    false_ack.retire(&p, &p.owner, Some(&r), 3000).await,
                    Err(Error::Unavailable)
                );
                assert_eq!(store.fetch(&p, Some(&r)).await.unwrap().1, bytes);
                assert_eq!(
                    store.upload(&p, &p.owner, 1000, bytes).await,
                    Err(Error::Conflict)
                );
                let lost_ack = SourceRetirer {
                    store: cleaner.store.clone(),
                    delete: Arc::new(LoseDeleteAck {
                        inner: cleaner.delete.clone(),
                    }),
                };
                assert_eq!(
                    lost_ack.retire(&p, &p.owner, Some(&r), 3000).await,
                    Err(Error::Unavailable)
                );
                assert!(matches!(
                    store.fetch(&p, Some(&r)).await,
                    Err(Error::Missing)
                ));
            }
            let receipt = cleaner.retire(&p, &p.owner, Some(&r), 3000).await.unwrap();
            store
                .verify_retirement(&p, &p.owner, &receipt)
                .await
                .unwrap();
            assert_eq!(
                cleaner.retire(&p, &p.owner, Some(&r), 4000).await.unwrap(),
                receipt
            );
            assert_eq!(
                store.upload(&p, &p.owner, 1000, bytes).await,
                Err(Error::Conflict)
            );
            if let Some(version) = &receipt.marker_version {
                // Only this test's random marker is removed by the raw private
                // transport for fixture teardown; production retire never does.
                cleaner.delete.delete(&p, version).await.unwrap();
            } else {
                store
                    .store
                    .inner
                    .delete(&Path::from(p.object_key().unwrap()))
                    .await
                    .unwrap();
            }
        }
    }
}

#[tokio::test]
#[ignore = "requires private versioned MinIO bucket"]
async fn output_minio_file_source_retirement_checks_old_version_bytes_before_deletion() {
    let _test = TESTS.lock().await;
    let store = config(true).build_sources().unwrap();
    let cleaner = config(true).build_source_retirer().unwrap();
    let p = plan(b"abc");
    let path = Path::from(p.object_key().unwrap());
    // A valid-looking marker must not authorize deletion of a corrupt old
    // version, even when it supplies the correct ETag and length for it.
    let result = store
        .store
        .inner
        .put_opts(
            &path,
            b"xyz".to_vec().into(),
            PutOptions {
                mode: PutMode::Create,
                attributes: Attributes::from_iter([(
                    crate::sources::metadata_key(),
                    p.metadata_digest().unwrap(),
                )]),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let previous = SourceRef {
        plan: p.clone(),
        etag: result.e_tag.unwrap(),
        object_version: result.version,
    };
    let marker = Marker {
        version: 1,
        plan_sha256: p.metadata_digest().unwrap(),
        previous: Some(previous.clone()),
    };
    let bytes = serde_json::to_vec(&marker).unwrap();
    let mark = store
        .store
        .inner
        .put_opts(
            &path,
            bytes.clone().into(),
            PutOptions {
                attributes: Attributes::from_iter([(
                    marker_key(),
                    hex::encode(Sha256::digest(&bytes)),
                )]),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(
        cleaner.retire(&p, &p.owner, None, 3000).await,
        Err(Error::Corrupt)
    );
    let body = store
        .store
        .inner
        .get_opts(
            &path,
            GetOptions {
                version: previous.object_version.clone(),
                ..Default::default()
            },
        )
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    assert_eq!(body, b"xyz"[..]);
    for version in [previous.object_version.unwrap(), mark.version.unwrap()] {
        cleaner.delete.delete(&p, &version).await.unwrap();
    }
}

struct PausedDelete {
    started: Arc<tokio::sync::Notify>,
}
impl SourceVersionDelete for PausedDelete {
    fn delete<'a>(&'a self, _: &'a SourcePlan, _: &'a str) -> BoxFuture<'a, Result<(), Error>> {
        Box::pin(async move {
            self.started.notify_one();
            std::future::pending().await
        })
    }
}

#[tokio::test]
#[ignore = "requires private versioned MinIO bucket"]
async fn output_minio_file_source_retirement_survives_cancelled_worker_and_concurrent_recovery() {
    let _test = TESTS.lock().await;
    let store = config(true).build_sources().unwrap();
    let cleaner = config(true).build_source_retirer().unwrap();
    let p = plan(b"cancelled-cleanup");
    let r = store
        .upload(&p, &p.owner, 1000, b"cancelled-cleanup")
        .await
        .unwrap();
    let started = Arc::new(tokio::sync::Notify::new());
    let paused = SourceRetirer {
        store: store.clone(),
        delete: Arc::new(PausedDelete {
            started: started.clone(),
        }),
    };
    let task_plan = p.clone();
    let task_ref = r.clone();
    let task = tokio::spawn(async move {
        paused
            .retire(&task_plan, &task_plan.owner, Some(&task_ref), 3000)
            .await
    });
    tokio::time::timeout(std::time::Duration::from_secs(5), started.notified())
        .await
        .unwrap();
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    assert_eq!(
        store.fetch(&p, Some(&r)).await.unwrap().1,
        b"cancelled-cleanup"
    );
    let (a, b) = tokio::join!(
        cleaner.retire(&p, &p.owner, Some(&r), 3000),
        cleaner.retire(&p, &p.owner, Some(&r), 3000)
    );
    let a = a.unwrap();
    assert_eq!(a, b.unwrap());
    assert!(matches!(
        store.fetch(&p, Some(&r)).await,
        Err(Error::Missing)
    ));
    assert_eq!(
        store.upload(&p, &p.owner, 1000, b"cancelled-cleanup").await,
        Err(Error::Conflict)
    );
    assert!(TRANSFERS.try_acquire_many(4).is_ok());
    cleaner
        .delete
        .delete(&p, a.marker_version.as_deref().unwrap())
        .await
        .unwrap();
}

#[tokio::test]
async fn maximum_valid_path_and_version_metadata_fit_retirement_markers() {
    let _test = TESTS.lock().await;
    let store = memory();
    let mut p = plan(b"x");
    p.upload.path = vec!["\"".repeat(255); 16].join("/");
    p.validate().unwrap();
    let reference = store.upload(&p, &p.owner, 1000, b"x").await.unwrap();
    let mut largest = reference.clone();
    largest.etag = "\"".repeat(1024);
    largest.object_version = Some("\"".repeat(1024));
    largest.validate().unwrap();
    let marker = Marker {
        version: 1,
        plan_sha256: p.metadata_digest().unwrap(),
        previous: Some(largest),
    };
    assert!(serde_json::to_vec(&marker).unwrap().len() <= MAX_MARKER as usize);
    retirer(&store)
        .retire(&p, &p.owner, Some(&reference), 3000)
        .await
        .unwrap();
}
