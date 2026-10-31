//! Warm-pool adoption through PostgreSQL, the HTTP router and gRPC/mTLS to the
//! simulated fake host (#188). No VM is prepared or adopted: the fake records
//! simulated members. None of this is Linux/KVM evidence.
#![allow(clippy::unwrap_used, clippy::expect_used)]
#[allow(dead_code)]
mod common;
use common::Fixture;
use sandbox_controller::{Controller, Tick, warm_pool::Step};
use sandbox_protocol::{
    Id, ProjectId,
    host_capabilities::{Capabilities, Image, Limits},
    supervisor::{Ownership, WarmAdoptRequest, supervisor_server::Supervisor},
    warm_pool::{self, HandoffIntent, HandoffOutcome, WarmReceipt},
};
use sandbox_store::{
    claims::OperationKind,
    warm_pool::{Claimed, PolicySpec, Publication, TemplateSpec},
};
use serde_json::{Value, json};
use sqlx::PgPool;
use uuid::Uuid;

const LEGACY_POOL: &str = "00000000-0000-4000-8000-000000000001";

fn capabilities() -> Capabilities {
    Capabilities {
        version: 1,
        simulated: true,
        architecture: "x86_64".into(),
        host_kernel_release: "6.8.0-fixture".into(),
        kvm_api_version: 12,
        cgroup_version: 2,
        supervisor_version: "0.0.0".into(),
        firecracker_version: "1.17.0".into(),
        firecracker_digest: format!("sha256:{}", "f".repeat(64)),
        jailer_digest: format!("sha256:{}", "e".repeat(64)),
        limits: Limits {
            vcpu: 4,
            memory_mib: 8192,
            disk_mib: 65536,
        },
        images: vec![Image {
            rootfs_digest: format!("sha256:{}", "a".repeat(64)),
            kernel_digest: format!("sha256:{}", "b".repeat(64)),
            minimum_disk_mib: 256,
            guest_version: "0.0.0".into(),
            bootstrap_version: 1,
            guest_protocol_version: 1,
        }],
    }
}

fn images() -> std::collections::BTreeSet<String> {
    [format!("sha256:{}", "a".repeat(64))].into()
}

async fn connect(f: &Fixture) -> Controller {
    f.controller().await
}

/// Enroll the fixture host's simulated capabilities and add a shared template
/// and policy matching `Fixture::admit` (2 vCPU, 2048 MiB, 8192 MiB disk).
async fn warm(f: &Fixture, min_ready: u32) -> Uuid {
    let value = capabilities();
    f.store.drain_host(f.config.host, 1).await.unwrap();
    f.store
        .set_host_capability_policy(f.config.host, 1, &value)
        .await
        .unwrap();
    f.fake.set_capabilities(value).await.unwrap();
    connect(f).await;
    f.store.finish_host_drain(f.config.host, 1).await.unwrap();
    let template = TemplateSpec {
        id: Uuid::now_v7(),
        name: "node".into(),
        version: 1,
        owner_project_id: None,
        publication: Publication::Shared,
        image_digest: format!("sha256:{}", "a".repeat(64)),
        vcpu: 2,
        memory_mib: 2048,
        disk_mib: 8192,
        boot_config: json!({"version":1,"runtime":"node"}),
        compatibility: json!({"version":1,"architecture":"x86_64"}),
        snapshot_id: None,
    };
    f.store.put_sandbox_template(&template).await.unwrap();
    let policy = PolicySpec {
        id: Uuid::now_v7(),
        template_id: template.id,
        host_pool_id: LEGACY_POOL.parse().unwrap(),
        owner_project_id: None,
        min_ready,
        max_total: min_ready,
        max_per_host: 4,
        idle_ttl_seconds: 600,
        enabled: true,
    };
    f.store.put_warm_pool_policy(&policy).await.unwrap();
    policy.id
}

async fn member_states(pool: &PgPool) -> Vec<String> {
    sqlx::query_scalar("SELECT state FROM warm_pool_members ORDER BY created_at,id")
        .fetch_all(pool)
        .await
        .unwrap()
}

async fn phase(pool: &PgPool) -> (String, String) {
    sqlx::query_as("SELECT status,phase FROM operations ORDER BY created_at DESC LIMIT 1")
        .fetch_one(pool)
        .await
        .unwrap()
}

async fn intent(pool: &PgPool) -> HandoffIntent {
    let value: Value = sqlx::query_scalar(
        "SELECT handoff_intent FROM warm_pool_members WHERE handoff_intent IS NOT NULL LIMIT 1",
    )
    .fetch_one(pool)
    .await
    .unwrap();
    serde_json::from_value(value).unwrap()
}

fn request(intent: &HandoffIntent, revision: i64) -> WarmAdoptRequest {
    let now = time::OffsetDateTime::now_utc().unix_timestamp() * 1000;
    WarmAdoptRequest {
        ownership: Some(Ownership {
            host_id: intent.host_id.to_string(),
            project_id: intent.project_id.to_string(),
            sandbox_id: intent.sandbox_id.to_string(),
            allocation_id: intent.allocation_id.to_string(),
            operation_id: intent.create_operation_id.to_string(),
            generation: intent.generation,
            supervisor_epoch: intent.supervisor_epoch,
            claim_revision: revision,
            claim_expires_unix_ms: now + 30_000,
        }),
        intent_json: warm_pool::encode(intent).unwrap(),
        launch_permit_json: warm_pool::encode(&intent.permit()).unwrap(),
        allocation_expires_unix_ms: now + 60_000,
    }
}

fn outcome(
    reply: tonic::Response<sandbox_protocol::supervisor::WarmObservation>,
) -> HandoffOutcome {
    match warm_pool::decode(&reply.into_inner().receipt_json).unwrap() {
        WarmReceipt::Handoff { receipt } => receipt.outcome,
        other => panic!("{other:?}"),
    }
}

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn creates_adopt_prepared_members_with_distinct_identities_and_no_cold_start(pool: PgPool) {
    let f = Fixture::new(&pool).await;
    warm(&f, 2).await;
    let controller = connect(&f).await;
    let pass = controller.warm_upkeep(4).await.unwrap();
    assert_eq!(pass.reserved, 2);
    assert_eq!(pass.steps, vec![Step::Prepared, Step::Prepared]);
    assert_eq!(f.fake.warm_ready().await, 2);
    // A prepared member carries template evidence only: no project identity.
    let prepared: Vec<Value> = sqlx::query_scalar("SELECT prepare_receipt FROM warm_pool_members")
        .fetch_all(&pool)
        .await
        .unwrap();
    for receipt in &prepared {
        assert!(receipt.get("project_id").is_none(), "{receipt}");
    }
    let mut controller = connect(&f).await;
    let (first, _) = f.admit().await;
    assert_eq!(controller.tick().await.unwrap(), Tick::Confirmed);
    let (second, _) = f.admit().await;
    assert_eq!(controller.tick().await.unwrap(), Tick::Confirmed);
    assert_eq!(f.fake.total_starts().await, 0);
    assert_eq!(f.fake.warm_adoptions().await, 2);
    for op in [first, second] {
        let (status,): (String,) = sqlx::query_as("SELECT status FROM operations WHERE id=$1")
            .bind(op.uuid())
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(status, "succeeded");
    }
    // Each adoption bound its own per-allocation identity after its claim.
    let identities: Vec<String> = sqlx::query_scalar(
        "SELECT handoff_receipt->'outcome'->>'identity_sha256' FROM warm_pool_members
         WHERE state='assigned'",
    )
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(identities.len(), 2);
    assert_ne!(identities[0], identities[1]);
    // Customer-used members are terminal; they never return to ready.
    assert_eq!(member_states(&pool).await, vec!["assigned", "assigned"]);
    let pass = controller.warm_upkeep(4).await.unwrap();
    assert!(pass.steps.is_empty(), "host is full: {pass:?}");
    assert_eq!(f.fake.warm_ready().await, 0);
}

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn lost_adopt_ack_is_inspected_after_restart_and_never_replayed(pool: PgPool) {
    let f = Fixture::new(&pool).await;
    warm(&f, 1).await;
    connect(&f).await.warm_upkeep(4).await.unwrap();
    f.admit().await;
    f.fake.lose_next_adopt_reply().await;
    assert_eq!(connect(&f).await.tick().await.unwrap(), Tick::Deferred);
    assert_eq!(phase(&pool).await.1, "warm_adopt_dispatched");
    assert_eq!(f.fake.warm_adoptions().await, 1);
    // A restarted controller only inspects the durable scope.
    f.reclaim_now().await;
    assert_eq!(connect(&f).await.tick().await.unwrap(), Tick::Confirmed);
    assert_eq!(phase(&pool).await.0, "succeeded");
    assert_eq!(f.fake.warm_adoptions().await, 1);
    assert_eq!(f.fake.total_starts().await, 0);
    assert_eq!(member_states(&pool).await, vec!["assigned"]);
}

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn uncertain_unadopted_handoff_is_fenced_retired_and_cold_booted(pool: PgPool) {
    let f = Fixture::new(&pool).await;
    let policy = warm(&f, 1).await;
    connect(&f).await.warm_upkeep(4).await.unwrap();
    f.admit().await;
    // A worker claims, marks the adopt dispatched and crashes before sending.
    let claim = f
        .store
        .claim_next(OperationKind::Create, 30)
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        f.store.claim_warm_member(&claim, &images()).await.unwrap(),
        Claimed::Claimed(_)
    ));
    f.store.begin_warm_adopt(&claim, &images()).await.unwrap();
    let scope = intent(&pool).await;
    f.reclaim_now().await;
    // Recovery inspects: the member was never adopted, so it is fenced.
    assert_eq!(connect(&f).await.tick().await.unwrap(), Tick::Deferred);
    assert_eq!(phase(&pool).await.1, "warm_handoff_rejected");
    assert_eq!(member_states(&pool).await, vec!["retiring"]);
    // The adopt that was "in flight" under the crashed claim arrives late:
    // its operation claim is stale. Even under the current claim revision the
    // fenced member cannot be adopted.
    let late = f
        .fake
        .adopt_warm_member(tonic::Request::new(request(&scope, claim.revision)))
        .await
        .unwrap_err();
    assert_eq!(late.code(), tonic::Code::FailedPrecondition);
    assert!(matches!(
        outcome(
            f.fake
                .adopt_warm_member(tonic::Request::new(request(&scope, claim.revision + 1)))
                .await
                .unwrap()
        ),
        HandoffOutcome::Rejected { reason } if reason == "fenced_by_inspect"
    ));
    assert_eq!(f.fake.warm_adoptions().await, 0);
    // Cleanup evidence releases the member; the create then cold-boots the
    // same allocation once. Replenishment does not reuse the claimed member.
    let controller = connect(&f).await;
    let pass = controller.warm_upkeep(1).await.unwrap();
    assert!(pass.steps.contains(&Step::Cleaned), "{pass:?}");
    let states = member_states(&pool).await;
    assert_eq!(states[0], "retired");
    f.reclaim_now().await;
    assert_eq!(connect(&f).await.tick().await.unwrap(), Tick::Confirmed);
    assert_eq!(f.fake.total_starts().await, 1);
    assert_eq!(phase(&pool).await.0, "succeeded");
    let (count,): (i64,) = sqlx::query_as("SELECT count(*) FROM allocations")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(count, 1);
    assert!(
        member_states(&pool).await.len() <= 2,
        "replenishment for {policy} creates new members, never reuses a claimed one"
    );
}

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn adoption_refuses_other_tenants_permits_and_stale_epochs(pool: PgPool) {
    let f = Fixture::new(&pool).await;
    warm(&f, 1).await;
    connect(&f).await.warm_upkeep(4).await.unwrap();
    f.admit().await;
    let claim = f
        .store
        .claim_next(OperationKind::Create, 30)
        .await
        .unwrap()
        .unwrap();
    f.store.claim_warm_member(&claim, &images()).await.unwrap();
    let scope = intent(&pool).await;
    let adopt = |r: WarmAdoptRequest| f.fake.adopt_warm_member(tonic::Request::new(r));
    // Another project cannot present this scope under its own ownership.
    let mut other = request(&scope, 1);
    other.ownership.as_mut().unwrap().project_id = ProjectId::generate().to_string();
    assert_eq!(
        adopt(other).await.unwrap_err().code(),
        tonic::Code::PermissionDenied
    );
    // A permit for another project (or serial) is not adoption authority.
    let mut forged = scope.permit();
    forged.project = ProjectId::generate();
    let mut wrong = request(&scope, 1);
    wrong.launch_permit_json = warm_pool::encode(&forged).unwrap();
    assert_eq!(
        adopt(wrong).await.unwrap_err().code(),
        tonic::Code::PermissionDenied
    );
    // An intent rewritten for another tenant no longer matches the
    // ownership and registered permit presented with it.
    let mut stolen = scope.clone();
    stolen.project_id = ProjectId::generate();
    let mut rewritten = request(&scope, 1);
    rewritten.intent_json = warm_pool::encode(&stolen).unwrap();
    assert_eq!(
        adopt(rewritten).await.unwrap_err().code(),
        tonic::Code::PermissionDenied
    );
    // A scope at another supervisor epoch is refused before any state change.
    let mut stale = scope.clone();
    stale.supervisor_epoch = 2;
    assert_eq!(
        adopt(request(&stale, 1)).await.unwrap_err().code(),
        tonic::Code::FailedPrecondition
    );
    assert_eq!(f.fake.warm_adoptions().await, 0);
    assert_eq!(f.fake.warm_ready().await, 1);
    // The exact scope adopts once; a different scope for the member is refused.
    assert!(matches!(
        outcome(adopt(request(&scope, 1)).await.unwrap()),
        HandoffOutcome::Adopted { .. }
    ));
    let mut second = scope.clone();
    second.claim_revision += 1;
    assert_eq!(
        adopt(request(&second, 2)).await.unwrap_err().code(),
        tonic::Code::AlreadyExists
    );
    assert_eq!(f.fake.warm_adoptions().await, 1);
    // Retiring an adopted member would destroy customer state: refused.
    let retire = sandbox_protocol::warm_pool::RetireIntent {
        version: 1,
        member_id: scope.member_id,
        vm_ref: Some(scope.vm_ref.clone()),
        host_id: scope.host_id,
        supervisor_epoch: 1,
        operation_id: Uuid::now_v7(),
        operation_revision: 1,
    };
    let refused = f
        .fake
        .retire_warm_member(tonic::Request::new(
            sandbox_protocol::supervisor::WarmRetireRequest {
                intent_json: warm_pool::encode(&retire).unwrap(),
            },
        ))
        .await
        .unwrap_err();
    assert_eq!(refused.code(), tonic::Code::FailedPrecondition);
}

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn creates_stay_cold_without_ready_members(pool: PgPool) {
    let f = Fixture::new(&pool).await;
    warm(&f, 1).await;
    // No upkeep ran: the policy has no ready member.
    f.admit().await;
    assert_eq!(connect(&f).await.tick().await.unwrap(), Tick::Confirmed);
    assert_eq!(f.fake.total_starts().await, 1);
    assert_eq!(f.fake.warm_adoptions().await, 0);
    assert!(member_states(&pool).await.is_empty());
}

async fn controller_allowing(f: &Fixture, image: &str) -> Controller {
    let mut config = f.config.clone();
    config.allowed_images = [image.to_owned()].into();
    Controller::connect(
        f.store.clone(),
        config,
        f.ca.as_bytes(),
        f.cert.as_bytes(),
        f.key.as_bytes(),
    )
    .await
    .unwrap()
}

async fn rejection(pool: &PgPool) -> (String, Option<String>) {
    sqlx::query_as(
        "SELECT status,error->>'code' FROM operations WHERE kind='create'
         ORDER BY created_at DESC LIMIT 1",
    )
    .fetch_one(pool)
    .await
    .unwrap()
}

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn adoption_requires_the_controller_image_allowlist(pool: PgPool) {
    let f = Fixture::new(&pool).await;
    warm(&f, 1).await;
    connect(&f).await.warm_upkeep(4).await.unwrap();
    assert_eq!(f.fake.warm_ready().await, 1);
    f.admit().await;
    // The image was admitted and prepared, then removed from the allowlist.
    let mut denied = controller_allowing(&f, &format!("sha256:{}", "9".repeat(64))).await;
    denied.tick().await.unwrap();
    // Denied before any claim: no member is bound, no VM booted, and the
    // create reports the same rejection as a cold boot.
    assert_eq!(f.fake.warm_adoptions().await, 0);
    assert_eq!(f.fake.total_starts().await, 0);
    assert_eq!(member_states(&pool).await, ["ready"]);
    assert_eq!(
        rejection(&pool).await,
        ("failed".into(), Some("image_not_allowed".into()))
    );
}

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn allowlist_removed_after_claim_fences_instead_of_adopting(pool: PgPool) {
    let f = Fixture::new(&pool).await;
    warm(&f, 1).await;
    connect(&f).await.warm_upkeep(4).await.unwrap();
    f.admit().await;
    // A worker under the old allowlist claims the member, then crashes.
    let claim = f
        .store
        .claim_next(OperationKind::Create, 30)
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        f.store.claim_warm_member(&claim, &images()).await.unwrap(),
        Claimed::Claimed(_)
    ));
    f.reclaim_now().await;
    let mut denied = controller_allowing(&f, &format!("sha256:{}", "9".repeat(64))).await;
    // First dispatch under a denied image is inspect-only: the member is
    // fenced, never adopted.
    assert_eq!(denied.tick().await.unwrap(), Tick::Deferred);
    assert_eq!(phase(&pool).await.1, "warm_handoff_rejected");
    assert_eq!(f.fake.warm_adoptions().await, 0);
    let pass = denied.warm_upkeep(1).await.unwrap();
    assert!(pass.steps.contains(&Step::Cleaned), "{pass:?}");
    f.reclaim_now().await;
    denied.tick().await.unwrap();
    assert_eq!(f.fake.warm_adoptions().await, 0);
    assert_eq!(f.fake.total_starts().await, 0);
    assert_eq!(
        rejection(&pool).await,
        ("failed".into(), Some("image_not_allowed".into()))
    );
    let (live,): (i64,) =
        sqlx::query_as("SELECT count(*) FROM allocations WHERE released_at IS NULL")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(live, 0);
}

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn lost_prepare_acks_through_exhaustion_clean_up_by_resolved_name(pool: PgPool) {
    let f = Fixture::new(&pool).await;
    warm(&f, 1).await;
    let controller = connect(&f).await;
    // Every acknowledgement is lost: the fake booted the member, the database
    // never learned its name.
    f.fake
        .lose_prepare_replies(sandbox_store::warm_pool::MAX_ATTEMPTS as u32)
        .await;
    let pass = controller.warm_upkeep(8).await.unwrap();
    assert_eq!(pass.steps.last(), Some(&Step::Abandoned), "{pass:?}");
    let (state, vm): (String, Option<String>) =
        sqlx::query_as("SELECT state,vm_ref FROM warm_pool_members")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!((state.as_str(), vm), ("retiring", None));
    // Both sides still hold the reservation: unknown is never refunded.
    assert_eq!(f.fake.warm_holding().await, 1);
    let pass = controller.warm_upkeep(1).await.unwrap();
    assert_eq!(pass.steps, vec![Step::Cleaned]);
    let (state, vm): (String, Option<String>) =
        sqlx::query_as("SELECT state,vm_ref FROM warm_pool_members ORDER BY created_at LIMIT 1")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(state, "retired");
    assert!(vm.is_some_and(|v| v.starts_with("warm-")));
    assert_eq!(f.fake.warm_holding().await, 0);
    // Capacity freed only by that evidence is reused by a replacement.
    let pass = controller.warm_upkeep(4).await.unwrap();
    assert_eq!(pass.steps, vec![Step::Prepared]);
    assert_eq!(f.fake.warm_ready().await, 1);
}

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn permit_hosts_adopt_only_registered_permits(pool: PgPool) {
    let f = Fixture::new(&pool).await;
    f.fake.require_launch_permits().await;
    warm(&f, 1).await;
    connect(&f).await.warm_upkeep(4).await.unwrap();
    f.admit().await;
    let claim = f
        .store
        .claim_next(OperationKind::Create, 30)
        .await
        .unwrap()
        .unwrap();
    f.store.claim_warm_member(&claim, &images()).await.unwrap();
    let scope = intent(&pool).await;
    // A permit that agrees with every caller field is still not proof of
    // issuance: before registration it binds and fences nothing.
    for inspect in [false, true] {
        let request = tonic::Request::new(request(&scope, claim.revision));
        let refused = if inspect {
            f.fake.inspect_warm_handoff(request).await
        } else {
            f.fake.adopt_warm_member(request).await
        };
        assert_eq!(refused.unwrap_err().code(), tonic::Code::PermissionDenied);
    }
    assert_eq!(f.fake.warm_ready().await, 1);
    // The controller registers the database-issued permit, then adopts once.
    f.reclaim_now().await;
    assert_eq!(connect(&f).await.tick().await.unwrap(), Tick::Confirmed);
    assert_eq!(f.fake.warm_adoptions().await, 1);
    assert!(f.fake.registered_permits().await >= scope.permit_serial);
    // A forged permit at an unregistered serial, otherwise consistent, is
    // denied even for an exact replay of the adopted scope.
    let mut forged = scope.permit();
    forged.serial += 1;
    let mut replay = request(&scope, claim.revision + 1);
    replay.launch_permit_json = warm_pool::encode(&forged).unwrap();
    assert_eq!(
        f.fake
            .adopt_warm_member(tonic::Request::new(replay))
            .await
            .unwrap_err()
            .code(),
        tonic::Code::PermissionDenied
    );
    // Cold creates on the same host also go through registration.
    f.admit().await;
    assert_eq!(connect(&f).await.tick().await.unwrap(), Tick::Confirmed);
    assert_eq!(f.fake.total_starts().await, 1);
    assert_eq!(f.fake.warm_adoptions().await, 1);
}

fn next_fake(f: &Fixture, epoch: i64) -> sandbox_fake_host::FakeHost {
    sandbox_fake_host::FakeHost::new(sandbox_fake_host::FakeConfig {
        host: f.config.host,
        epoch,
        images: images(),
        capacity: sandbox_protocol::supervisor::Resources {
            vcpu: 4,
            memory_mib: 8192,
            disk_mib: 65536,
        },
    })
    .unwrap()
}

async fn previous_cleanup(
    fake: &sandbox_fake_host::FakeHost,
    request: &warm_pool::PreviousRetireIntent,
) -> Result<warm_pool::PreviousCleanupReceipt, tonic::Status> {
    let reply = fake
        .retire_warm_member(tonic::Request::new(
            sandbox_protocol::supervisor::WarmRetireRequest {
                intent_json: warm_pool::encode(request).unwrap(),
            },
        ))
        .await?
        .into_inner();
    assert!(reply.simulated);
    match warm_pool::decode(&reply.receipt_json).unwrap() {
        WarmReceipt::PreviousCleanup { receipt } => Ok(receipt),
        other => panic!("{other:?}"),
    }
}

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn previous_cleanup_requires_retained_exact_ownership_and_current_claim(pool: PgPool) {
    use sandbox_store::warm_pool::{Error, WarmWork};
    let f = Fixture::new(&pool).await;
    let policy = warm(&f, 1).await;
    connect(&f).await.warm_upkeep(4).await.unwrap();
    f.store.drain_host(f.config.host, 1).await.unwrap();
    assert_eq!(
        f.store
            .advance_host_epoch(f.config.host, 1)
            .await
            .unwrap()
            .supervisor_epoch,
        2
    );
    f.store.reconcile_warm_pool(policy, 4).await.unwrap();
    let claim = f
        .store
        .claim_warm_operation(policy, 30)
        .await
        .unwrap()
        .unwrap();
    let WarmWork::Retire {
        member_id,
        vm_ref,
        host_id,
        supervisor_epoch,
    } = &claim.work
    else {
        panic!()
    };
    let request = warm_pool::PreviousRetireIntent {
        version: warm_pool::VERSION,
        original: warm_pool::RetireIntent {
            version: warm_pool::VERSION,
            member_id: *member_id,
            vm_ref: vm_ref.clone(),
            host_id: *host_id,
            supervisor_epoch: *supervisor_epoch,
            operation_id: claim.id,
            operation_revision: claim.revision,
        },
        reporting_epoch: 2,
    };
    // A fresh incarnation has no proof, even if every caller field agrees.
    let restarted = next_fake(&f, 2);
    assert!(previous_cleanup(&restarted, &request).await.is_err());
    assert_eq!(member_states(&pool).await, ["retiring"]);
    assert_eq!(
        f.store
            .host_maintenance(f.config.host, 2)
            .await
            .unwrap()
            .unretired_warm_members,
        1
    );
    restarted.retain_warm_members_from(&f.fake).await.unwrap();
    // The old handle is now fenced, so a delayed old retire cannot apply.
    assert!(
        f.fake
            .retire_warm_member(tonic::Request::new(
                sandbox_protocol::supervisor::WarmRetireRequest {
                    intent_json: warm_pool::encode(&request.original).unwrap(),
                }
            ))
            .await
            .is_err()
    );
    for change in 0..4 {
        let mut wrong = request.clone();
        match change {
            0 => wrong.reporting_epoch = 3,
            1 => wrong.original.member_id = Uuid::now_v7(),
            2 => wrong.original.vm_ref = Some("other-vm".into()),
            _ => wrong.original.supervisor_epoch = 0,
        }
        assert!(previous_cleanup(&restarted, &wrong).await.is_err());
    }
    assert_eq!(restarted.warm_holding().await, 1);
    // An unnamed request resolves the exact retained member and reports its
    // actual name. The store still requires the exact named durable intent.
    let mut unnamed = request.clone();
    unnamed.original.vm_ref = None;
    let resolved = previous_cleanup(&restarted, &unnamed).await.unwrap();
    assert_eq!(resolved.cleanup.vm_ref, request.original.vm_ref);
    assert!(matches!(
        f.store
            .record_previous_warm_cleanup(&claim, &resolved)
            .await,
        Err(Error::Receipt(_))
    ));
    let receipt = previous_cleanup(&restarted, &request).await.unwrap();
    receipt.verify(&request).unwrap();
    assert!(matches!(
        f.store.record_warm_cleanup(&claim, &receipt.cleanup).await,
        Err(Error::Conflict)
    ));
    let mut forged = receipt.clone();
    forged.request.original.operation_revision += 1;
    assert!(matches!(
        f.store.record_previous_warm_cleanup(&claim, &forged).await,
        Err(Error::Receipt(_))
    ));
    forged = receipt.clone();
    forged.request.reporting_epoch = 3;
    assert!(matches!(
        f.store.record_previous_warm_cleanup(&claim, &forged).await,
        Err(Error::Conflict)
    ));
    assert_eq!(member_states(&pool).await, ["retiring"]);
    // A lost response does not grant a stale worker database authority.
    sqlx::query("UPDATE capacity_operations SET lease_expires_at=clock_timestamp()-interval '1 second',available_at=clock_timestamp()-interval '1 second' WHERE id=$1")
        .bind(claim.id).execute(&pool).await.unwrap();
    assert!(matches!(
        f.store.record_previous_warm_cleanup(&claim, &receipt).await,
        Err(Error::LostClaim)
    ));
    let retry = f
        .store
        .claim_warm_operation(policy, 30)
        .await
        .unwrap()
        .unwrap();
    let mut retried = request.clone();
    retried.original.operation_revision = retry.revision;
    let evidence = previous_cleanup(&restarted, &retried).await.unwrap();
    f.store
        .record_previous_warm_cleanup(&retry, &evidence)
        .await
        .unwrap();
    assert_eq!(member_states(&pool).await, ["retired"]);
    assert_eq!(
        f.store
            .host_maintenance(f.config.host, 2)
            .await
            .unwrap()
            .unretired_warm_members,
        0
    );
    assert_eq!(restarted.warm_holding().await, 0);
}

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn previous_cleanup_never_destroys_an_adopted_member(pool: PgPool) {
    let f = Fixture::new(&pool).await;
    let policy = warm(&f, 1).await;
    let mut controller = connect(&f).await;
    controller.warm_upkeep(4).await.unwrap();
    f.admit().await;
    for _ in 0..6 {
        controller.tick().await.unwrap();
        if member_states(&pool).await == ["assigned"] {
            break;
        }
    }
    assert_eq!(member_states(&pool).await, ["assigned"]);
    let scope = intent(&pool).await;
    let restarted = next_fake(&f, 2);
    restarted.retain_warm_members_from(&f.fake).await.unwrap();
    let request = warm_pool::PreviousRetireIntent {
        version: warm_pool::VERSION,
        original: warm_pool::RetireIntent {
            version: warm_pool::VERSION,
            member_id: scope.member_id,
            vm_ref: Some(scope.vm_ref),
            host_id: scope.host_id,
            supervisor_epoch: scope.supervisor_epoch,
            operation_id: Uuid::now_v7(),
            operation_revision: 1,
        },
        reporting_epoch: 2,
    };
    assert!(previous_cleanup(&restarted, &request).await.is_err());
    assert_eq!(member_states(&pool).await, ["assigned"]);
    assert_eq!(f.fake.warm_adoptions().await, 1);
    // This helper models retained member evidence only. Ordinary allocation
    // recovery remains separate and cannot be bypassed through warm cleanup.
    assert!(
        next_fake(&f, 3)
            .retain_warm_members_from(&f.fake)
            .await
            .is_err()
    );
    assert!(f.store.warm_pool_members(policy).await.unwrap()[0]["cleanup_evidence"].is_null());
}

#[path = "warm_adoption/cleanup_transport.rs"]
mod cleanup_transport;
