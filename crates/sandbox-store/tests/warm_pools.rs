//! Warm pools (#188) against isolated PostgreSQL databases. Hosts are
//! simulated database identities and supervisor receipts are constructed by
//! the test (a fake supervisor); no VM is prepared, adopted or destroyed.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use sandbox_protocol::{
    HostId, Id, IdempotencyKey, ProjectId, ProjectToken, RequestDigest,
    host_capabilities::{Capabilities, Image, Limits},
    warm_pool::{CleanupReceipt, HandoffOutcome, HandoffReceipt, PrepareReceipt, VERSION},
};
use sandbox_store::{
    Store,
    admission::{CreateSandbox, Resources},
    capacity,
    claims::{Claim, OperationKind},
    host_registry::{HostPoolSpec, PlacementPolicy, PoolType, Tenancy},
    placement::{HostScope, PlacementError, Reservation},
    warm_pool::{
        AdoptMode, Claimed, Error, PolicySpec, Publication, TemplateSpec, WarmClaim, WarmHandoff,
        WarmWork,
    },
};
use serde_json::json;
use sha2::Digest;
use sqlx::PgPool;
use uuid::Uuid;

fn images() -> std::collections::BTreeSet<String> {
    [image()].into()
}

fn image() -> String {
    format!("sha256:{}", "a".repeat(64))
}

/// 1 vCPU, 512 MiB, 1024 MiB disk plus 128 MiB VMM and 401 MiB artifact overhead.
const SLOT: (i32, i64, i64) = (1, 640, 1425);

async fn project(pool: &PgPool, limits: serde_json::Value) -> (ProjectId, ProjectToken) {
    let id = ProjectId::generate();
    let token = ProjectToken::generate().unwrap();
    sqlx::query(
        "INSERT INTO projects(id,name,status,limits,api_tokens) VALUES($1,'warm','active',$2,$3)",
    )
    .bind(id.uuid())
    .bind(limits)
    .bind(json!([{"key_id":token.key_id().as_str(),"hash":hex::encode(token.hash().as_bytes())}]))
    .execute(pool)
    .await
    .unwrap();
    (id, token)
}

async fn host_pool(store: &Store, name: &str, tenancy: Tenancy) -> Uuid {
    let id = Uuid::now_v7();
    store
        .put_host_pool(&HostPoolSpec {
            version: 1,
            id,
            name: name.into(),
            pool_type: PoolType::Standalone,
            min_ready_hosts: 0,
            max_hosts: 16,
            placement_policy: PlacementPolicy {
                version: 1,
                tenancy,
            },
            provider_config: json!({"version":1}),
            credential_secret_ref: None,
        })
        .await
        .unwrap();
    id
}

fn capabilities() -> Capabilities {
    Capabilities {
        version: 1,
        simulated: true,
        architecture: "x86_64".into(),
        host_kernel_release: "6.8.0-fixture".into(),
        kvm_api_version: 12,
        cgroup_version: 2,
        supervisor_version: env!("CARGO_PKG_VERSION").into(),
        firecracker_version: "1.17.0".into(),
        firecracker_digest: format!("sha256:{}", "c".repeat(64)),
        jailer_digest: format!("sha256:{}", "d".repeat(64)),
        limits: Limits {
            vcpu: 4,
            memory_mib: 8192,
            disk_mib: 65536,
        },
        images: vec![Image {
            rootfs_digest: image(),
            kernel_digest: format!("sha256:{}", "b".repeat(64)),
            minimum_disk_mib: 256,
            guest_version: env!("CARGO_PKG_VERSION").into(),
            bootstrap_version: 1,
            guest_protocol_version: 1,
        }],
    }
}

async fn host(pool: &PgPool, host_pool: Uuid, slots: i64) -> HostId {
    let id = HostId::generate();
    let n = sqlx::query_scalar::<_, i64>("SELECT count(*) FROM hosts")
        .fetch_one(pool)
        .await
        .unwrap();
    let caps = serde_json::to_value(capabilities()).unwrap();
    sqlx::query("INSERT INTO hosts(id,status,cpu_capacity,memory_capacity_mib,disk_capacity_mib,supervisor_epoch,
        last_seen_at,host_pool_id,endpoint,transport_server_name,transport_certificate_sha256,
        compatibility,runtime_capabilities,runtime_capabilities_epoch)
        VALUES($1,'ready',$2,$3,$4,1,clock_timestamp(),$5,$6,$7,$8,$9,$9,1)")
        .bind(id.uuid()).bind(SLOT.0 * slots as i32).bind(SLOT.1 * slots).bind(SLOT.2 * slots)
        .bind(host_pool).bind(format!("https://10.1.0.{}:7443", n + 1))
        .bind(format!("host-{}.sandbox.internal", id.uuid()))
        .bind(hex::encode(sha2::Sha256::digest(id.uuid().as_bytes())))
        .bind(caps)
        .execute(pool).await.unwrap();
    id
}

fn template(owner: Option<ProjectId>, publication: Publication, name: &str) -> TemplateSpec {
    TemplateSpec {
        id: Uuid::now_v7(),
        name: name.into(),
        version: 1,
        owner_project_id: owner,
        publication,
        image_digest: image(),
        vcpu: 1,
        memory_mib: 512,
        disk_mib: 1024,
        boot_config: json!({"version":1,"runtime":"node"}),
        compatibility: json!({"version":1,"architecture":"x86_64"}),
        snapshot_id: None,
    }
}

fn policy(template: Uuid, host_pool: Uuid, owner: Option<ProjectId>, min_ready: u32) -> PolicySpec {
    PolicySpec {
        id: Uuid::now_v7(),
        template_id: template,
        host_pool_id: host_pool,
        owner_project_id: owner,
        min_ready,
        max_total: min_ready.max(1) + 1,
        max_per_host: 8,
        idle_ttl_seconds: 600,
        enabled: true,
    }
}

async fn create(store: &Store, project: ProjectId, token: &ProjectToken) -> Claim {
    let payload = json!({"image_digest":image()});
    store
        .admit_create_sandbox(
            &CreateSandbox {
                project_id: project,
                key_id: token.key_id().clone(),
                idempotency_key: IdempotencyKey::parse(&Uuid::now_v7().to_string()).unwrap(),
                request_digest: RequestDigest::compute("POST", "/v1/sandboxes", &payload).unwrap(),
                image_digest: image(),
                name: None,
                resources: Resources {
                    vcpu: 1,
                    memory_mib: 512,
                    disk_mib: 1024,
                },
                payload,
            },
            &sandbox_protocol::images::ImageAllowlist::new([image()]).unwrap(),
        )
        .await
        .unwrap();
    store
        .claim_next(OperationKind::Create, 30)
        .await
        .unwrap()
        .unwrap()
}

/// Fake supervisor: echo the intent exactly.
fn prepared(claim: &WarmClaim, vm: &str) -> PrepareReceipt {
    let WarmWork::Prepare(i) = &claim.work else {
        panic!("{claim:?}")
    };
    PrepareReceipt {
        version: VERSION,
        member_id: i.member_id,
        template_id: i.template_id,
        host_id: i.host_id,
        supervisor_epoch: i.supervisor_epoch,
        image_digest: i.image_digest.clone(),
        vcpu: i.vcpu,
        memory_mib: i.memory_mib,
        disk_mib: i.disk_mib,
        vm_ref: vm.into(),
    }
}

fn handoff(h: &WarmHandoff, outcome: HandoffOutcome) -> HandoffReceipt {
    let i = &h.intent;
    HandoffReceipt {
        version: VERSION,
        member_id: i.member_id,
        vm_ref: i.vm_ref.clone(),
        allocation_id: i.allocation_id,
        project_id: i.project_id,
        sandbox_id: i.sandbox_id,
        host_id: i.host_id,
        supervisor_epoch: i.supervisor_epoch,
        claim_revision: i.claim_revision,
        intent_sha256: i.digest().unwrap(),
        outcome,
    }
}

fn adopted() -> HandoffOutcome {
    HandoffOutcome::Adopted {
        identity_sha256: "e".repeat(64),
    }
}

fn cleaned(claim: &WarmClaim) -> CleanupReceipt {
    let WarmWork::Retire {
        member_id,
        vm_ref,
        host_id,
        supervisor_epoch,
    } = &claim.work
    else {
        panic!("{claim:?}")
    };
    CleanupReceipt {
        version: VERSION,
        member_id: *member_id,
        vm_ref: vm_ref.clone(),
        host_id: *host_id,
        supervisor_epoch: *supervisor_epoch,
        destroyed: true,
    }
}

/// Reconcile and prepare `n` ready members through the fake supervisor.
async fn stock(store: &Store, policy: Uuid, n: usize) {
    let planned = store.reconcile_warm_pool(policy, 16).await.unwrap();
    assert_eq!(planned.prepare.len(), n, "{planned:?}");
    for i in 0..n {
        let claim = store
            .claim_warm_operation(policy, 30)
            .await
            .unwrap()
            .unwrap();
        store
            .record_warm_prepared(&claim, &prepared(&claim, &format!("warm-{i}")))
            .await
            .unwrap();
    }
}

async fn states(pool: &PgPool, policy: Uuid) -> Vec<String> {
    sqlx::query_scalar(
        "SELECT state FROM warm_pool_members WHERE pool_id=$1 ORDER BY created_at,id",
    )
    .bind(policy)
    .fetch_all(pool)
    .await
    .unwrap()
}

/// Host use (allocations + unassigned warm reservations) never exceeds capacity
/// and a claiming member is never counted alongside its live allocation.
async fn assert_accounted(pool: &PgPool) {
    let over: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM hosts h JOIN LATERAL (SELECT COALESCE(sum(vcpu),0) cpu,
         COALESCE(sum(memory_mib+128),0) mem,COALESCE(sum(disk_mib+401),0) disk FROM (
           SELECT vcpu,memory_mib,disk_mib FROM allocations WHERE host_id=h.id AND released_at IS NULL
           UNION ALL SELECT m.vcpu,m.memory_mib,m.disk_mib FROM warm_pool_members m
             LEFT JOIN allocations a ON a.id=m.claimed_allocation_id WHERE m.host_id=h.id
             AND (m.state IN ('preparing','ready','retiring') OR (m.state='claiming' AND a.released_at IS NOT NULL))
         ) r) u ON true WHERE u.cpu>h.cpu_capacity OR u.mem>h.memory_capacity_mib OR u.disk>h.disk_capacity_mib",
    )
    .fetch_one(pool)
    .await
    .unwrap();
    assert_eq!(over, 0, "host overbooked");
}

struct Fixture {
    store: Store,
    shared: Uuid,
    host: HostId,
    policy: Uuid,
    project: ProjectId,
    token: ProjectToken,
}

/// One shared host pool with one host of `slots`, a shared template/policy and
/// a granted project.
async fn fixture(pool: &PgPool, slots: i64, min_ready: u32) -> Fixture {
    let store = Store::from_pool(pool.clone());
    let shared = host_pool(&store, "shared", Tenancy::Shared).await;
    let host = host(pool, shared, slots).await;
    let (project, token) = self::project(pool, json!({})).await;
    store.grant_host_pool_access(project, shared).await.unwrap();
    let t = template(None, Publication::Shared, "node");
    store.put_sandbox_template(&t).await.unwrap();
    let p = self::policy(t.id, shared, None, min_ready);
    store.put_warm_pool_policy(&p).await.unwrap();
    Fixture {
        store,
        shared,
        host,
        policy: p.id,
        project,
        token,
    }
}

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn templates_are_immutable_and_snapshot_backed_templates_are_refused(pool: PgPool) {
    let store = Store::from_pool(pool.clone());
    let t = template(None, Publication::Shared, "node");
    store.put_sandbox_template(&t).await.unwrap();
    store.put_sandbox_template(&t).await.unwrap();
    let mut changed = t.clone();
    changed.memory_mib = 1024;
    assert!(matches!(
        store.put_sandbox_template(&changed).await,
        Err(Error::Conflict)
    ));
    let mut snap = template(None, Publication::Shared, "snap");
    snap.snapshot_id = Some(Uuid::now_v7());
    assert!(matches!(
        store.put_sandbox_template(&snap).await,
        Err(Error::SnapshotUnavailable)
    ));
    // A NULL owner is never implicit publication.
    assert!(matches!(
        store
            .put_sandbox_template(&template(None, Publication::Private, "private"))
            .await,
        Err(Error::Invalid)
    ));
    assert!(
        sqlx::query("UPDATE sandbox_templates SET memory_mib=1024 WHERE id=$1")
            .bind(t.id)
            .execute(&pool)
            .await
            .is_err()
    );
}

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn replenishment_is_bounded_and_counts_reservations_against_hosts(pool: PgPool) {
    // Two slots on the only host; min_ready 3 can reserve at most two.
    let f = fixture(&pool, 2, 3).await;
    let planned = f.store.reconcile_warm_pool(f.policy, 16).await.unwrap();
    assert_eq!(planned.prepare.len(), 2);
    // Idempotent: a second pass adds nothing while reservations hold capacity.
    assert!(
        f.store
            .reconcile_warm_pool(f.policy, 16)
            .await
            .unwrap()
            .prepare
            .is_empty()
    );
    assert_accounted(&pool).await;
    // Warm reservations consume capacity before customer placement.
    let claim = create(&f.store, f.project, &f.token).await;
    assert!(matches!(
        f.store.place_create(&claim, HostScope::Fleet).await,
        Err(PlacementError::Capacity)
    ));
    // Generic standalone claimers never see warm intents.
    assert!(
        f.store
            .claim_capacity(f.shared, 30)
            .await
            .unwrap()
            .is_none()
    );
    let bad = sqlx::query_scalar::<_, i64>(
        "SELECT count(*) FROM capacity_operations WHERE warm_pool_id IS NOT NULL AND kind NOT LIKE 'warm_%'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(bad, 0);
}

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn concurrent_claims_take_each_member_once(pool: PgPool) {
    let f = fixture(&pool, 8, 2).await;
    stock(&f.store, f.policy, 2).await;
    let mut tasks = tokio::task::JoinSet::new();
    for _ in 0..6 {
        let claim = create(&f.store, f.project, &f.token).await;
        let store = f.store.clone();
        tasks.spawn(async move {
            let first = store.claim_warm_member(&claim, &images()).await.unwrap();
            // A duplicated worker finds the same durable handoff.
            let again = store.claim_warm_member(&claim, &images()).await.unwrap();
            (first, again)
        });
    }
    let mut claimed = Vec::new();
    let mut none = 0;
    while let Some(r) = tasks.join_next().await {
        match r.unwrap() {
            (Claimed::Claimed(a), Claimed::Existing(b)) => {
                assert_eq!(a, b);
                claimed.push(a.intent.member_id);
            }
            (Claimed::None, Claimed::None) => none += 1,
            other => panic!("{other:?}"),
        }
    }
    claimed.sort();
    claimed.dedup();
    assert_eq!((claimed.len(), none), (2, 4));
    assert_eq!(states(&pool, f.policy).await, ["claiming", "claiming"]);
    let linked: i64 = sqlx::query_scalar(
        "SELECT count(DISTINCT claimed_allocation_id) FROM warm_pool_members WHERE pool_id=$1",
    )
    .bind(f.policy)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(linked, 2);
    // Parked creates are not cold-dispatchable.
    let parked: i64 =
        sqlx::query_scalar("SELECT count(*) FROM operations WHERE phase='warm_handoff'")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(parked, 2);
    assert_accounted(&pool).await;
}

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn claim_transfers_reservation_exactly_once_on_a_full_host(pool: PgPool) {
    // One slot: the warm member fills the host. A claim must not need a second slot.
    let f = fixture(&pool, 1, 1).await;
    stock(&f.store, f.policy, 1).await;
    let claim = create(&f.store, f.project, &f.token).await;
    let Claimed::Claimed(h) = f.store.claim_warm_member(&claim, &images()).await.unwrap() else {
        panic!()
    };
    assert_eq!(h.allocation.host_id, f.host);
    assert_accounted(&pool).await;
    // Still full: neither a new member nor a cold create fits.
    assert!(
        f.store
            .reconcile_warm_pool(f.policy, 16)
            .await
            .unwrap()
            .prepare
            .is_empty()
    );
    let other = create(&f.store, f.project, &f.token).await;
    assert!(matches!(
        f.store.place_create(&other, HostScope::Fleet).await,
        Err(PlacementError::Capacity)
    ));
    // Adoption: member assigned, create proceeds as dispatched (inspection
    // establishes readiness); retries of the same receipt are idempotent.
    let receipt = handoff(&h, adopted());
    f.store.record_warm_handoff(&claim, &receipt).await.unwrap();
    f.store.record_warm_handoff(&claim, &receipt).await.unwrap();
    assert_eq!(states(&pool, f.policy).await, ["assigned"]);
    let (phase, attempts, observed): (String, i32, String) = sqlx::query_as(
        "SELECT o.phase,o.attempt_count,s.observed_state FROM operations o JOIN sandboxes s ON s.id=o.sandbox_id WHERE o.id=$1",
    )
    .bind(claim.operation_id.uuid())
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(
        (phase.as_str(), attempts, observed.as_str()),
        ("create_dispatched", 1, "creating")
    );
    // A conflicting receipt for the same member is refused.
    let mut forged = handoff(
        &h,
        HandoffOutcome::Rejected {
            reason: "late".into(),
        },
    );
    forged.claim_revision = h.intent.claim_revision;
    assert!(matches!(
        f.store.record_warm_handoff(&claim, &forged).await,
        Err(Error::Conflict)
    ));
    assert_accounted(&pool).await;
}

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn tenant_template_and_quota_denials(pool: PgPool) {
    let store = Store::from_pool(pool.clone());
    let shared = host_pool(&store, "shared", Tenancy::Shared).await;
    host(&pool, shared, 8).await;
    let (owner, owner_token) = project(&pool, json!({})).await;
    let (other, other_token) = project(&pool, json!({})).await;
    let (ungranted, ungranted_token) = project(&pool, json!({})).await;
    let (tight, tight_token) = project(&pool, json!({"sandboxes":0})).await;
    for p in [owner, other, tight] {
        store.grant_host_pool_access(p, shared).await.unwrap();
    }
    // Private template owned by `owner`, dedicated policy for `owner`.
    let private = template(Some(owner), Publication::Private, "private");
    store.put_sandbox_template(&private).await.unwrap();
    // Another project cannot own a policy over someone's private template, and
    // a shared policy needs a shared template.
    assert!(matches!(
        store
            .put_warm_pool_policy(&policy(private.id, shared, Some(other), 1))
            .await,
        Err(Error::Unauthorized)
    ));
    assert!(matches!(
        store
            .put_warm_pool_policy(&policy(private.id, shared, None, 1))
            .await,
        Err(Error::Unauthorized)
    ));
    assert!(matches!(
        store
            .put_warm_pool_policy(&policy(private.id, shared, Some(ungranted), 1))
            .await,
        Err(Error::Unauthorized)
    ));
    let dedicated = policy(private.id, shared, Some(owner), 1);
    store.put_warm_pool_policy(&dedicated).await.unwrap();
    stock(&store, dedicated.id, 1).await;
    // Other tenants, ungranted and over-quota projects never get the member.
    let c = create(&store, other, &other_token).await;
    assert_eq!(
        store.claim_warm_member(&c, &images()).await.unwrap(),
        Claimed::None
    );
    let c = create(&store, ungranted, &ungranted_token).await;
    assert_eq!(
        store.claim_warm_member(&c, &images()).await.unwrap(),
        Claimed::None
    );
    let shared_template = template(None, Publication::Shared, "public");
    store.put_sandbox_template(&shared_template).await.unwrap();
    let public = policy(shared_template.id, shared, None, 1);
    store.put_warm_pool_policy(&public).await.unwrap();
    stock(&store, public.id, 1).await;
    let c = create(&store, tight, &tight_token).await;
    assert!(matches!(
        store.claim_warm_member(&c, &images()).await,
        Err(Error::Quota)
    ));
    assert_eq!(states(&pool, dedicated.id).await, ["ready"]);
    assert_eq!(states(&pool, public.id).await, ["ready"]);
    // The owner can claim (dedicated or shared, both authorized).
    let c = create(&store, owner, &owner_token).await;
    assert!(matches!(
        store.claim_warm_member(&c, &images()).await.unwrap(),
        Claimed::Claimed(_)
    ));
    // Dedicated host pool refuses a shared policy.
    let dedicated_pool = host_pool(&store, "dedicated", Tenancy::Dedicated).await;
    store
        .grant_host_pool_access(owner, dedicated_pool)
        .await
        .unwrap();
    assert!(matches!(
        store
            .put_warm_pool_policy(&policy(shared_template.id, dedicated_pool, None, 1))
            .await,
        Err(Error::Unauthorized)
    ));
}

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn lost_ack_and_restart_resume_the_same_durable_intent(pool: PgPool) {
    let f = fixture(&pool, 4, 1).await;
    stock(&f.store, f.policy, 1).await;
    let claim = create(&f.store, f.project, &f.token).await;
    let Claimed::Claimed(h) = f.store.claim_warm_member(&claim, &images()).await.unwrap() else {
        panic!()
    };
    // Controller "restarts": the lease expires, another worker reclaims the
    // create and finds the same intent instead of claiming another member.
    sqlx::query("UPDATE operations SET lease_expires_at=clock_timestamp()-interval '1 second',next_retry_at=NULL WHERE id=$1")
        .bind(claim.operation_id.uuid()).execute(&pool).await.unwrap();
    assert!(matches!(
        f.store
            .record_warm_handoff(&claim, &handoff(&h, adopted()))
            .await,
        Err(Error::LostClaim)
    ));
    let again = f
        .store
        .claim_next(OperationKind::Create, 30)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(again.operation_id, claim.operation_id);
    assert_eq!(
        f.store.claim_warm_member(&again, &images()).await.unwrap(),
        Claimed::Existing(h.clone())
    );
    assert_eq!(
        f.store.warm_handoff(claim.sandbox_id).await.unwrap(),
        Some(h.clone())
    );
    // Uncertain claim: the member never becomes ready again, even after TTL.
    sqlx::query(
        "UPDATE warm_pool_members SET lease_expires_at=clock_timestamp()-interval '1 second'",
    )
    .execute(&pool)
    .await
    .unwrap();
    f.store.reconcile_warm_pool(f.policy, 16).await.unwrap();
    assert!(
        states(&pool, f.policy)
            .await
            .contains(&"claiming".to_string())
    );
    // The inspected receipt (same intent) completes the handoff.
    f.store
        .record_warm_handoff(&again, &handoff(&h, adopted()))
        .await
        .unwrap();
    assert_accounted(&pool).await;
}

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn adopt_is_dispatched_once_then_only_inspected(pool: PgPool) {
    let f = fixture(&pool, 4, 2).await;
    stock(&f.store, f.policy, 2).await;
    let claim = create(&f.store, f.project, &f.token).await;
    let Claimed::Claimed(h) = f.store.claim_warm_member(&claim, &images()).await.unwrap() else {
        panic!()
    };
    // The scope names the exact registered permit and create operation.
    let (serial, op): (i64, Uuid) = sqlx::query_as(
        "SELECT serial,create_operation_id FROM allocation_permits WHERE allocation_id=$1",
    )
    .bind(h.intent.allocation_id.uuid())
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(h.intent.permit_serial, serial as u64);
    assert_eq!(op, claim.operation_id.uuid());
    assert_eq!(h.intent.permit().allocation, h.intent.allocation_id);
    let first = f.store.begin_warm_adopt(&claim, &images()).await.unwrap();
    assert_eq!(first.mode, AdoptMode::Adopt);
    assert_eq!(first.handoff, h);
    // Lost acknowledgement: every later attempt may only inspect.
    let again = f.store.begin_warm_adopt(&claim, &images()).await.unwrap();
    assert_eq!(again.mode, AdoptMode::Inspect);
    let phase: String = sqlx::query_scalar("SELECT phase FROM operations WHERE id=$1")
        .bind(claim.operation_id.uuid())
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(phase, "warm_adopt_dispatched");

    // A second create whose credential is revoked after its claim never
    // dispatches adoption of project identity: inspect-and-fence only.
    let other = create(&f.store, f.project, &f.token).await;
    let Claimed::Claimed(_) = f.store.claim_warm_member(&other, &images()).await.unwrap() else {
        panic!()
    };
    sqlx::query("UPDATE projects SET api_tokens='[]' WHERE id=$1")
        .bind(f.project.uuid())
        .execute(&pool)
        .await
        .unwrap();
    let revoked = f.store.begin_warm_adopt(&other, &images()).await.unwrap();
    assert_eq!(revoked.mode, AdoptMode::Inspect);
    let phase: String = sqlx::query_scalar("SELECT phase FROM operations WHERE id=$1")
        .bind(other.operation_id.uuid())
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(phase, "warm_handoff");
    assert_accounted(&pool).await;
}

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn stale_epoch_receipts_are_refused_and_stale_members_retire(pool: PgPool) {
    let f = fixture(&pool, 4, 2).await;
    stock(&f.store, f.policy, 2).await;
    let claim = create(&f.store, f.project, &f.token).await;
    let Claimed::Claimed(h) = f.store.claim_warm_member(&claim, &images()).await.unwrap() else {
        panic!()
    };
    f.store.drain_host(f.host, 1).await.unwrap();
    f.store.advance_host_epoch(f.host, 1).await.unwrap();
    assert!(matches!(
        f.store
            .record_warm_handoff(&claim, &handoff(&h, adopted()))
            .await,
        Err(Error::StaleEpoch)
    ));
    // The other (ready, epoch 1) member retires; the claim stays uncertain.
    let r = f.store.reconcile_warm_pool(f.policy, 16).await.unwrap();
    assert_eq!((r.retire.len(), r.prepare.len()), (1, 0));
    let mut s = states(&pool, f.policy).await;
    s.sort();
    assert_eq!(s, ["claiming", "retiring"]);
    let reason: String =
        sqlx::query_scalar("SELECT retire_reason FROM warm_pool_members WHERE state='retiring'")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(reason, "stale_epoch");
    // Cleanup must be bound to the member's own epoch.
    let c = f
        .store
        .claim_warm_operation(f.policy, 30)
        .await
        .unwrap()
        .unwrap();
    let mut wrong = cleaned(&c);
    wrong.supervisor_epoch = 2;
    assert!(matches!(
        f.store.record_warm_cleanup(&c, &wrong).await,
        Err(Error::Receipt(_))
    ));
    assert_accounted(&pool).await;
}

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn drain_retires_ready_members_and_blocks_host_retirement_until_cleanup(pool: PgPool) {
    let f = fixture(&pool, 4, 1).await;
    stock(&f.store, f.policy, 1).await;
    f.store.drain_host(f.host, 1).await.unwrap();
    // A draining host's member is never claimed.
    let claim = create(&f.store, f.project, &f.token).await;
    assert_eq!(
        f.store.claim_warm_member(&claim, &images()).await.unwrap(),
        Claimed::None
    );
    let report = f.store.host_maintenance(f.host, 1).await.unwrap();
    assert_eq!(report.unretired_warm_members, 1);
    let r = f.store.reconcile_warm_pool(f.policy, 16).await.unwrap();
    assert_eq!((r.retire.len(), r.prepare.len()), (1, 0));
    // Static retirement refuses while the warm reservation is unreleased.
    let retire = capacity::Request {
        host: f.host,
        epoch: 1,
        action: capacity::Action::Retire,
    };
    f.store
        .enqueue_capacity(f.shared, "retire", &retire)
        .await
        .unwrap();
    let c = f.store.claim_capacity(f.shared, 30).await.unwrap().unwrap();
    assert!(matches!(
        f.store.apply_static_capacity(&c).await,
        Err(capacity::Error::Conflict)
    ));
    // Failed cleanup (no receipt) retains the reservation; evidence releases it.
    let w = f
        .store
        .claim_warm_operation(f.policy, 30)
        .await
        .unwrap()
        .unwrap();
    f.store.retry_warm_operation(&w).await.unwrap();
    assert_eq!(states(&pool, f.policy).await, ["retiring"]);
    let w = f
        .store
        .claim_warm_operation(f.policy, 30)
        .await
        .unwrap()
        .unwrap();
    f.store.record_warm_cleanup(&w, &cleaned(&w)).await.unwrap();
    assert_eq!(states(&pool, f.policy).await, ["retired"]);
    assert_eq!(
        f.store
            .host_maintenance(f.host, 1)
            .await
            .unwrap()
            .unretired_warm_members,
        0
    );
}

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn rejected_handoff_retires_member_then_falls_back_to_cold_boot(pool: PgPool) {
    let f = fixture(&pool, 2, 1).await;
    stock(&f.store, f.policy, 1).await;
    let claim = create(&f.store, f.project, &f.token).await;
    let Claimed::Claimed(h) = f.store.claim_warm_member(&claim, &images()).await.unwrap() else {
        panic!()
    };
    f.store
        .record_warm_handoff(
            &claim,
            &handoff(
                &h,
                HandoffOutcome::Rejected {
                    reason: "vm_missing".into(),
                },
            ),
        )
        .await
        .unwrap();
    assert_eq!(states(&pool, f.policy).await, ["retiring"]);
    // Both the allocation and the possibly-live member VM are counted.
    assert_accounted(&pool).await;
    let phase: String = sqlx::query_scalar("SELECT phase FROM operations WHERE id=$1")
        .bind(claim.operation_id.uuid())
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(phase, "warm_handoff_rejected");
    // Max_total includes retiring: no replacement beyond the bound (2).
    let r = f.store.reconcile_warm_pool(f.policy, 16).await.unwrap();
    assert!(r.prepare.len() <= 1);
    let w = loop {
        let w = f
            .store
            .claim_warm_operation(f.policy, 30)
            .await
            .unwrap()
            .unwrap();
        if matches!(w.work, WarmWork::Retire { .. }) {
            break w;
        }
        f.store.retry_warm_operation(&w).await.unwrap();
    };
    f.store.record_warm_cleanup(&w, &cleaned(&w)).await.unwrap();
    let phase: String = sqlx::query_scalar("SELECT phase FROM operations WHERE id=$1")
        .bind(claim.operation_id.uuid())
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(phase, "reserved");
    // The customer-used member is never returned to the pool.
    let ready_again: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM warm_pool_members WHERE claimed_allocation_id IS NOT NULL AND state='ready'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(ready_again, 0);
}

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn ordinary_cold_boot_is_unchanged_without_warm_members(pool: PgPool) {
    let f = fixture(&pool, 2, 0).await;
    let claim = create(&f.store, f.project, &f.token).await;
    assert_eq!(
        f.store.claim_warm_member(&claim, &images()).await.unwrap(),
        Claimed::None
    );
    assert!(matches!(
        f.store.place_create(&claim, HostScope::Fleet).await,
        Ok(Reservation::Reserved(_))
    ));
}

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn expired_prepare_lease_cannot_record_and_exhaustion_abandons_to_retirement(pool: PgPool) {
    let f = fixture(&pool, 2, 1).await;
    f.store.reconcile_warm_pool(f.policy, 16).await.unwrap();
    let first = f
        .store
        .claim_warm_operation(f.policy, 30)
        .await
        .unwrap()
        .unwrap();
    sqlx::query("UPDATE capacity_operations SET lease_expires_at=clock_timestamp()-interval '1 second',available_at=clock_timestamp() WHERE id=$1")
        .bind(first.id).execute(&pool).await.unwrap();
    let second = f
        .store
        .claim_warm_operation(f.policy, 30)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(second.id, first.id);
    assert!(matches!(
        f.store
            .record_warm_prepared(&first, &prepared(&first, "warm-x"))
            .await,
        Err(Error::LostClaim)
    ));
    let mut last = second;
    while last.revision < sandbox_store::warm_pool::MAX_ATTEMPTS {
        f.store.retry_warm_operation(&last).await.unwrap();
        last = f
            .store
            .claim_warm_operation(f.policy, 30)
            .await
            .unwrap()
            .unwrap();
    }
    f.store.abandon_warm_prepare(&last).await.unwrap();
    // Unknown whether a VM booted: reservation still held as retiring.
    assert_eq!(states(&pool, f.policy).await, ["retiring"]);
    assert_accounted(&pool).await;
}

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn crash_on_final_prepare_attempt_is_retired_by_upkeep_with_reservation_held(pool: PgPool) {
    let f = fixture(&pool, 2, 1).await;
    f.store.reconcile_warm_pool(f.policy, 16).await.unwrap();
    // Every attempt's worker crashes; the eighth claim also expires unanswered.
    let mut last = f
        .store
        .claim_warm_operation(f.policy, 30)
        .await
        .unwrap()
        .unwrap();
    while last.revision < sandbox_store::warm_pool::MAX_ATTEMPTS {
        f.store.retry_warm_operation(&last).await.unwrap();
        last = f
            .store
            .claim_warm_operation(f.policy, 30)
            .await
            .unwrap()
            .unwrap();
    }
    // A live final claim is never swept out from under its holder.
    let pass = f.store.reconcile_warm_pool(f.policy, 16).await.unwrap();
    assert!(pass.retire.is_empty(), "{pass:?}");
    assert_eq!(states(&pool, f.policy).await, ["preparing"]);
    sqlx::query("UPDATE capacity_operations SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE id=$1")
        .bind(last.id).execute(&pool).await.unwrap();
    // Exhausted: no claimer will ever take it again, and the dead holder
    // cannot abandon it.
    assert!(
        f.store
            .claim_warm_operation(f.policy, 30)
            .await
            .unwrap()
            .is_none()
    );
    assert!(matches!(
        f.store.abandon_warm_prepare(&last).await,
        Err(Error::LostClaim)
    ));
    // Upkeep durably retires it with its reservation held; a replacement is
    // reserved only within the remaining capacity.
    let pass = f.store.reconcile_warm_pool(f.policy, 16).await.unwrap();
    assert_eq!(pass.retire.len(), 1, "{pass:?}");
    let (status, outcome): (String, Option<String>) = sqlx::query_as(
        "SELECT status,provider_receipt->>'outcome' FROM capacity_operations WHERE id=$1",
    )
    .bind(last.id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(status, "succeeded");
    assert_eq!(outcome.as_deref(), Some("exhausted_to_retirement"));
    assert_eq!(states(&pool, f.policy).await[0], "retiring");
    assert_accounted(&pool).await;
    // Idempotent: a second pass does not enqueue another retirement.
    let again = f.store.reconcile_warm_pool(f.policy, 16).await.unwrap();
    assert!(again.retire.is_empty(), "{again:?}");
    // Only exact cleanup evidence for the unnamed member releases it; the
    // supervisor names the VM it found.
    let retire = loop {
        let claim = f
            .store
            .claim_warm_operation(f.policy, 30)
            .await
            .unwrap()
            .unwrap();
        if matches!(claim.work, WarmWork::Retire { .. }) {
            break claim;
        }
        f.store.retry_warm_operation(&claim).await.unwrap();
    };
    let WarmWork::Retire { vm_ref, .. } = &retire.work else {
        unreachable!()
    };
    assert!(vm_ref.is_none());
    let mut evidence = cleaned(&retire);
    evidence.vm_ref = Some("warm-found".into());
    f.store
        .record_warm_cleanup(&retire, &evidence)
        .await
        .unwrap();
    let (state, vm): (String, Option<String>) =
        sqlx::query_as("SELECT state,vm_ref FROM warm_pool_members ORDER BY created_at,id LIMIT 1")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(
        (state.as_str(), vm.as_deref()),
        ("retired", Some("warm-found"))
    );
    assert_accounted(&pool).await;
}

async fn fleet_host(store: &Store, host: HostId) -> serde_json::Value {
    store.fleet_status().await.unwrap()["hosts"]
        .as_array()
        .unwrap()
        .iter()
        .find(|h| h["host_id"] == host.uuid().to_string())
        .unwrap()
        .clone()
}

/// Fleet diagnostics charge unadopted warm reservations exactly as placement
/// does, and a claim moves the charge to the allocation without double counting.
#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn fleet_available_charges_warm_reservations_like_placement(pool: PgPool) {
    let f = fixture(&pool, 2, 1).await;
    let empty = fleet_host(&f.store, f.host).await;
    assert_eq!(
        empty["warm_held"],
        json!({"members":0,"vcpu":0,"memory_mib":0,"disk_mib":0})
    );
    assert_eq!(empty["available"], empty["capacity"]);
    stock(&f.store, f.policy, 1).await;
    assert_eq!(states(&pool, f.policy).await, ["ready"]);
    let slot = json!({"vcpu":SLOT.0,"memory_mib":SLOT.1,"disk_mib":SLOT.2});
    let warm = fleet_host(&f.store, f.host).await;
    assert_eq!(warm["warm_held"]["members"], 1);
    assert_eq!(warm["reserved"], slot);
    assert_eq!(
        warm["available"], slot,
        "one of two slots is held by the ready member"
    );
    assert_eq!(warm["active_allocations"], 0);
    // Placement agrees: exactly the one advertised slot fits, then none.
    let first = create(&f.store, f.project, &f.token).await;
    assert!(matches!(
        f.store.place_create(&first, HostScope::Fleet).await,
        Ok(Reservation::Reserved(_))
    ));
    let full = fleet_host(&f.store, f.host).await;
    assert_eq!(
        full["available"],
        json!({"vcpu":0,"memory_mib":0,"disk_mib":0})
    );
    assert_eq!(full["reserved"], full["capacity"]);
    let second = create(&f.store, f.project, &f.token).await;
    assert!(matches!(
        f.store.place_create(&second, HostScope::Fleet).await,
        Err(PlacementError::Capacity)
    ));
    // Claiming transfers the reservation to the allocation exactly once.
    let Claimed::Claimed(_) = f.store.claim_warm_member(&second, &images()).await.unwrap() else {
        panic!("ready member must be claimable")
    };
    let claimed = fleet_host(&f.store, f.host).await;
    assert_eq!(claimed["warm_held"]["members"], 0);
    assert_eq!(claimed["active_allocations"], 2);
    assert_eq!(claimed["reserved"], full["reserved"]);
    assert_eq!(claimed["available"], full["available"]);
    let metrics = sandbox_store::fleet::prometheus(&f.store.fleet_status().await.unwrap());
    assert!(metrics.contains("hudsonbox_fleet_warm_held_members 0\n"));
    assert!(!metrics.contains(&f.host.uuid().to_string()));
    assert_accounted(&pool).await;
}
