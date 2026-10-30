//! Multi-host placement (#194) against isolated PostgreSQL databases.
//! Hosts are simulated database identities; no supervisor or VM is started.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use sandbox_protocol::{
    HostId, Id, IdempotencyKey, ProjectId, ProjectToken, RequestDigest,
    host_capabilities::{Capabilities, Image, Limits},
};
use sandbox_store::{
    Store,
    admission::{CreateSandbox, Resources},
    claims::{Claim, OperationKind},
    host_registry::{HostPoolSpec, PlacementPolicy, PoolType, RegistryError, Tenancy},
    placement::{
        HostScope, PLACEMENT_RETRY_MAX_SECONDS, PLACEMENT_RETRY_MIN_SECONDS, PlacementError,
        PlacementWait, Reservation, WaitOutcome,
    },
};
use serde_json::{Value, json};
use sha2::Digest;
use sqlx::PgPool;
use std::collections::HashMap;
use uuid::Uuid;

fn image() -> String {
    format!("sha256:{}", "a".repeat(64))
}

/// One sandbox: 1 vCPU, 512 MiB RAM, 1024 MiB disk. With the 128 MiB VMM and
/// 401 MiB artifact overhead it occupies 1 vCPU, 640 MiB and 1425 MiB of host.
const SLOT: (i32, i64, i64) = (1, 640, 1425);

async fn project(pool: &PgPool) -> (ProjectId, ProjectToken) {
    let id = ProjectId::generate();
    let token = ProjectToken::generate().unwrap();
    sqlx::query("INSERT INTO projects(id,name,status,limits,api_tokens) VALUES($1,'fleet','active','{}',$2)")
        .bind(id.uuid()).bind(json!([{"key_id":token.key_id().as_str(),"hash":hex::encode(token.hash().as_bytes())}]))
        .execute(pool).await.unwrap();
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

/// A ready, fresh, registered host with a transport identity and room for
/// `slots` sandboxes including runtime overhead.
async fn host(pool: &PgPool, host_pool: Uuid, slots: i64) -> HostId {
    let id = HostId::generate();
    let n = sqlx::query_scalar::<_, i64>("SELECT count(*) FROM hosts")
        .fetch_one(pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO hosts(id,status,cpu_capacity,memory_capacity_mib,disk_capacity_mib,supervisor_epoch,
        last_seen_at,host_pool_id,endpoint,transport_server_name,transport_certificate_sha256)
        VALUES($1,'ready',$2,$3,$4,1,clock_timestamp(),$5,$6,$7,$8)")
        .bind(id.uuid()).bind(SLOT.0 * slots as i32).bind(SLOT.1 * slots).bind(SLOT.2 * slots)
        .bind(host_pool).bind(format!("https://10.0.0.{}:7443", n + 1))
        .bind(format!("host-{}.sandbox.internal", id.uuid()))
        .bind(hex::encode(sha2::Sha256::digest(id.uuid().as_bytes())))
        .execute(pool).await.unwrap();
    enroll(pool, id, &capabilities()).await;
    id
}

async fn admit(store: &Store, project: ProjectId, token: &ProjectToken) {
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
}

async fn next(store: &Store) -> Claim {
    store
        .claim_next(OperationKind::Create, 30)
        .await
        .unwrap()
        .unwrap()
}

async fn create(store: &Store, project: ProjectId, token: &ProjectToken) -> Claim {
    admit(store, project, token).await;
    next(store).await
}

fn placed(result: Result<Reservation, PlacementError>) -> HostId {
    match result {
        Ok(Reservation::Reserved(a)) => a.host_id,
        other => panic!("{other:?}"),
    }
}

/// Unreleased use per host including overhead, and the capacity it must fit.
async fn assert_within_capacity(pool: &PgPool) {
    let rows: Vec<(Uuid, i64, i64, i64, i32, i64, i64)> = sqlx::query_as(
        "SELECT h.id,COALESCE(sum(a.vcpu),0)::bigint,COALESCE(sum(a.memory_mib+128),0)::bigint,
         COALESCE(sum(a.disk_mib+401),0)::bigint,h.cpu_capacity,h.memory_capacity_mib,h.disk_capacity_mib
         FROM hosts h LEFT JOIN allocations a ON a.host_id=h.id AND a.released_at IS NULL GROUP BY h.id",
    )
    .fetch_all(pool)
    .await
    .unwrap();
    for (id, cpu, memory, disk, cpu_cap, memory_cap, disk_cap) in rows {
        assert!(
            cpu <= i64::from(cpu_cap) && memory <= memory_cap && disk <= disk_cap,
            "host {id} overbooked: {cpu}/{cpu_cap} {memory}/{memory_cap} {disk}/{disk_cap}"
        );
    }
    let duplicates: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM (SELECT sandbox_id FROM allocations WHERE released_at IS NULL
         GROUP BY sandbox_id HAVING count(*)>1) d",
    )
    .fetch_one(pool)
    .await
    .unwrap();
    assert_eq!(duplicates, 0, "a sandbox has more than one owner");
}

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn fleet_requires_capability_evidence_but_configured_legacy_still_works(pool: PgPool) {
    let store = Store::from_pool(pool.clone());
    let shared = host_pool(&store, "shared", Tenancy::Shared).await;
    let legacy = host(&pool, shared, 1).await;
    sqlx::query("UPDATE hosts SET compatibility='{}',runtime_capabilities=NULL,runtime_capabilities_epoch=NULL WHERE id=$1")
        .bind(legacy.uuid()).execute(&pool).await.unwrap();
    let (id, token) = project(&pool).await;
    store.grant_host_pool_access(id, shared).await.unwrap();
    let claim = create(&store, id, &token).await;
    assert!(matches!(
        store.place_create(&claim, HostScope::Fleet).await,
        Err(PlacementError::HostCompatibility)
    ));
    let (allocations, permits): (i64, i64) = sqlx::query_as(
        "SELECT (SELECT count(*) FROM allocations),(SELECT count(*) FROM allocation_permits)",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!((allocations, permits), (0, 0));
    assert_eq!(
        placed(store.reserve_create(&claim, legacy, 1).await),
        legacy
    );
    // Already-reserved work still returns its original owner in fleet mode;
    // qualification cannot strand reconciliation or create a replacement.
    assert!(matches!(
        store.place_create(&claim, HostScope::Fleet).await.unwrap(),
        Reservation::Existing(a) if a.host_id == legacy
    ));
}

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn concurrent_fleet_placement_never_overbooks_or_duplicates(pool: PgPool) {
    let store = Store::from_pool(pool.clone());
    let shared = host_pool(&store, "shared", Tenancy::Shared).await;
    let hosts = [
        host(&pool, shared, 2).await,
        host(&pool, shared, 2).await,
        host(&pool, shared, 2).await,
    ];
    for _ in 0..4 {
        let (id, token) = project(&pool).await;
        store.grant_host_pool_access(id, shared).await.unwrap();
        for _ in 0..4 {
            admit(&store, id, &token).await;
        }
    }
    // Sixteen workers race claiming and placing sixteen creates onto six slots.
    let mut tasks = tokio::task::JoinSet::new();
    for _ in 0..16 {
        let store = store.clone();
        tasks.spawn(async move {
            let claim = next(&store).await;
            let first = store.place_create(&claim, HostScope::Fleet).await;
            // A duplicated worker retrying the same claim finds the same owner.
            let again = store.place_create(&claim, HostScope::Fleet).await;
            (first, again)
        });
    }
    let mut per_host: HashMap<HostId, usize> = HashMap::new();
    let mut full = 0;
    while let Some(result) = tasks.join_next().await {
        match result.unwrap() {
            (Ok(Reservation::Reserved(a)), Ok(Reservation::Existing(b))) => {
                assert_eq!(a, b);
                *per_host.entry(a.host_id).or_default() += 1;
            }
            (Err(PlacementError::Capacity), Err(PlacementError::Capacity)) => full += 1,
            other => panic!("{other:?}"),
        }
    }
    assert_eq!(full, 10);
    for host in hosts {
        assert_eq!(per_host.get(&host), Some(&2), "{per_host:?}");
    }
    assert_within_capacity(&pool).await;
    // Permit serials stay contiguous per host under concurrency.
    let serials: Vec<(Uuid, Vec<i64>)> = sqlx::query_as(
        "SELECT host_id,array_agg(serial ORDER BY serial) FROM allocation_permits GROUP BY host_id",
    )
    .fetch_all(&pool)
    .await
    .unwrap();
    assert!(serials.iter().all(|(_, s)| s == &vec![1, 2]));
}

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn concurrent_duplicate_claim_owners_create_one_allocation(pool: PgPool) {
    let store = Store::from_pool(pool.clone());
    let shared = host_pool(&store, "shared", Tenancy::Shared).await;
    host(&pool, shared, 4).await;
    host(&pool, shared, 4).await;
    let (id, token) = project(&pool).await;
    store.grant_host_pool_access(id, shared).await.unwrap();
    let claim = create(&store, id, &token).await;
    let (a, b) = tokio::join!(
        store.place_create(&claim, HostScope::Fleet),
        store.place_create(&claim, HostScope::Fleet)
    );
    match (a.unwrap(), b.unwrap()) {
        (Reservation::Reserved(x), Reservation::Existing(y))
        | (Reservation::Existing(y), Reservation::Reserved(x)) => assert_eq!(x, y),
        other => panic!("{other:?}"),
    }
    let allocations: i64 = sqlx::query_scalar("SELECT count(*) FROM allocations")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(allocations, 1);
    // A superseded claim revision cannot place again.
    sqlx::query("UPDATE operations SET claim_revision=claim_revision+1")
        .execute(&pool)
        .await
        .unwrap();
    assert!(matches!(
        store.place_create(&claim, HostScope::Fleet).await,
        Err(PlacementError::LostClaim)
    ));
}

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn spread_selection_and_existing_allocations_stay_pinned(pool: PgPool) {
    let store = Store::from_pool(pool.clone());
    let shared = host_pool(&store, "shared", Tenancy::Shared).await;
    let small = host(&pool, shared, 1).await;
    let large = host(&pool, shared, 3).await;
    let (id, token) = project(&pool).await;
    store.grant_host_pool_access(id, shared).await.unwrap();
    let first = create(&store, id, &token).await;
    // The host left with the most free memory wins.
    assert_eq!(
        placed(store.place_create(&first, HostScope::Fleet).await),
        large
    );
    // Draining the owner does not move an existing allocation elsewhere.
    store.drain_host(large, 1).await.unwrap();
    match store.place_create(&first, HostScope::Fleet).await.unwrap() {
        Reservation::Existing(a) => assert_eq!((a.host_id, a.supervisor_epoch), (large, 1)),
        other => panic!("{other:?}"),
    }
    let second = create(&store, id, &token).await;
    assert_eq!(
        placed(store.place_create(&second, HostScope::Fleet).await),
        small
    );
    let receipt: Value =
        sqlx::query_scalar("SELECT attempt_receipts->0 FROM operations WHERE id=$1")
            .bind(second.operation_id.uuid())
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(receipt["host_id"], small.to_string());
}

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn uncertain_and_previous_epoch_allocations_keep_holding_capacity(pool: PgPool) {
    let store = Store::from_pool(pool.clone());
    let shared = host_pool(&store, "shared", Tenancy::Shared).await;
    let a = host(&pool, shared, 1).await;
    let b = host(&pool, shared, 1).await;
    let (id, token) = project(&pool).await;
    store.grant_host_pool_access(id, shared).await.unwrap();
    let first = create(&store, id, &token).await;
    let owner = placed(store.place_create(&first, HostScope::Fleet).await);
    // The outcome is unknown, the release is unconfirmed and its lease expired,
    // and the owner restarted at a new epoch. Nothing proves the VM is gone.
    // A far retry time keeps the next claims on the new creates.
    sqlx::query(
        "UPDATE operations SET status='unknown',lease_expires_at=NULL,
        next_retry_at=clock_timestamp()+interval '1 hour' WHERE id=$1",
    )
    .bind(first.operation_id.uuid())
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query("UPDATE allocations SET status='releasing',lease_expires_at=clock_timestamp()-interval '1 minute'")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("UPDATE hosts SET supervisor_epoch=2 WHERE id=$1")
        .bind(owner.uuid())
        .execute(&pool)
        .await
        .unwrap();
    let second = create(&store, id, &token).await;
    let other = if owner == a { b } else { a };
    assert_eq!(
        placed(store.place_create(&second, HostScope::Fleet).await),
        other
    );
    let third = create(&store, id, &token).await;
    assert!(matches!(
        store.place_create(&third, HostScope::Fleet).await,
        Err(PlacementError::Capacity)
    ));
    assert_within_capacity(&pool).await;
}

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn stale_draining_unregistered_and_incompatible_hosts_are_excluded(pool: PgPool) {
    let store = Store::from_pool(pool.clone());
    let shared = host_pool(&store, "shared", Tenancy::Shared).await;
    let stale = host(&pool, shared, 4).await;
    let draining = host(&pool, shared, 4).await;
    let legacy = host(&pool, shared, 4).await;
    let wrong_image = host(&pool, shared, 4).await;
    sqlx::query(
        "UPDATE hosts SET last_seen_at=clock_timestamp()-interval '31 seconds' WHERE id=$1",
    )
    .bind(stale.uuid())
    .execute(&pool)
    .await
    .unwrap();
    store.drain_host(draining, 1).await.unwrap();
    // A host without a registered transport identity is reachable only through
    // a single-host controller's configuration, never fleet placement.
    sqlx::query("UPDATE hosts SET endpoint=NULL,transport_server_name=NULL,transport_certificate_sha256=NULL WHERE id=$1")
        .bind(legacy.uuid()).execute(&pool).await.unwrap();
    let mut caps = capabilities();
    caps.images[0].rootfs_digest = format!("sha256:{}", "e".repeat(64));
    enroll(&pool, wrong_image, &caps).await;
    let (id, token) = project(&pool).await;
    store.grant_host_pool_access(id, shared).await.unwrap();
    let claim = create(&store, id, &token).await;
    assert!(matches!(
        store.place_create(&claim, HostScope::Fleet).await,
        Err(PlacementError::HostCompatibility)
    ));
    sqlx::query("UPDATE hosts SET last_seen_at=NULL WHERE id=$1")
        .bind(wrong_image.uuid())
        .execute(&pool)
        .await
        .unwrap();
    assert!(matches!(
        store.place_create(&claim, HostScope::Fleet).await,
        Err(PlacementError::HostUnavailable)
    ));
    // An architecture mismatch between approved policy and runtime also excludes.
    let compatible = host(&pool, shared, 4).await;
    enroll(&pool, compatible, &capabilities()).await;
    let mut other_arch = capabilities();
    other_arch.architecture = "aarch64".into();
    sqlx::query("UPDATE hosts SET runtime_capabilities=$2 WHERE id=$1")
        .bind(compatible.uuid())
        .bind(serde_json::to_value(&other_arch).unwrap())
        .execute(&pool)
        .await
        .unwrap();
    assert!(matches!(
        store.place_create(&claim, HostScope::Fleet).await,
        Err(PlacementError::HostCompatibility)
    ));
    enroll(&pool, compatible, &capabilities()).await;
    assert_eq!(
        placed(store.place_create(&claim, HostScope::Fleet).await),
        compatible
    );
    // The legacy host remains usable through explicit configured scope.
    let second = create(&store, id, &token).await;
    assert_eq!(
        placed(
            store
                .place_create(
                    &second,
                    HostScope::Configured {
                        host: legacy,
                        epoch: 1
                    }
                )
                .await
        ),
        legacy
    );
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

/// Install matching approved policy and current-epoch runtime observation.
async fn enroll(pool: &PgPool, host: HostId, caps: &Capabilities) {
    let value = serde_json::to_value(caps).unwrap();
    sqlx::query("UPDATE hosts SET compatibility=$2,runtime_capabilities=$2,runtime_capabilities_epoch=supervisor_epoch,
        last_seen_at=clock_timestamp() WHERE id=$1")
        .bind(host.uuid()).bind(value).execute(pool).await.unwrap();
}

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn drain_racing_placement_never_reserves_unreported_capacity(pool: PgPool) {
    let store = Store::from_pool(pool.clone());
    let shared = host_pool(&store, "shared", Tenancy::Shared).await;
    let (id, token) = project(&pool).await;
    store.grant_host_pool_access(id, shared).await.unwrap();
    for _ in 0..12 {
        let target = host(&pool, shared, 1).await;
        let claim = create(&store, id, &token).await;
        let (drained, placement) = tokio::join!(
            store.drain_host(target, 1),
            store.place_create(&claim, HostScope::Fleet)
        );
        let drained = drained.unwrap();
        match placement {
            // Placement committed first: drain observed and reported it.
            Ok(Reservation::Reserved(a)) => {
                assert_eq!(a.host_id, target);
                assert_eq!(drained.outstanding_allocations, 1);
            }
            // Drain committed first: the host was re-evaluated and excluded.
            Err(PlacementError::HostUnavailable) => {
                assert_eq!(drained.outstanding_allocations, 0);
                sqlx::query("UPDATE operations SET status='cancelled',lease_expires_at=NULL,completed_at=clock_timestamp() WHERE id=$1")
                    .bind(claim.operation_id.uuid())
                    .execute(&pool)
                    .await
                    .unwrap();
            }
            other => panic!("{other:?}"),
        }
    }
    assert_within_capacity(&pool).await;

    // Deterministic order: an uncommitted drain holds the host row; placement
    // waits, re-evaluates the drained row and chooses the other host even
    // though spread would otherwise prefer the larger, drained one.
    let low = host(&pool, shared, 1).await;
    let high = host(&pool, shared, 2).await;
    assert!(low.uuid() < high.uuid());
    let claim = create(&store, id, &token).await;
    let mut tx = pool.begin().await.unwrap();
    sqlx::query("UPDATE hosts SET status='draining' WHERE id=$1")
        .bind(high.uuid())
        .execute(&mut *tx)
        .await
        .unwrap();
    let task = {
        let store = store.clone();
        let claim = claim.clone();
        tokio::spawn(async move { store.place_create(&claim, HostScope::Fleet).await })
    };
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    tx.commit().await.unwrap();
    assert_eq!(placed(task.await.unwrap()), low);
}

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn tenant_pools_deny_access_without_fallback(pool: PgPool) {
    let store = Store::from_pool(pool.clone());
    let shared = host_pool(&store, "shared", Tenancy::Shared).await;
    let dedicated = host_pool(&store, "tenant-a", Tenancy::Dedicated).await;
    let shared_host = host(&pool, shared, 1).await;
    let dedicated_host = host(&pool, dedicated, 4).await;
    let (tenant, tenant_token) = project(&pool).await;
    let (other, other_token) = project(&pool).await;
    let (ungranted, ungranted_token) = project(&pool).await;
    store
        .grant_host_pool_access(tenant, dedicated)
        .await
        .unwrap();
    store.grant_host_pool_access(other, shared).await.unwrap();
    // A dedicated pool accepts exactly one project.
    assert!(matches!(
        store.grant_host_pool_access(other, dedicated).await,
        Err(RegistryError::Conflict)
    ));

    let first = create(&store, other, &other_token).await;
    assert_eq!(
        placed(store.place_create(&first, HostScope::Fleet).await),
        shared_host
    );
    // The shared pool is full; the idle dedicated pool is never a fallback.
    let second = create(&store, other, &other_token).await;
    assert!(matches!(
        store.place_create(&second, HostScope::Fleet).await,
        Err(PlacementError::Capacity)
    ));
    // Naming the dedicated host directly is denied too.
    assert!(matches!(
        store
            .place_create(
                &second,
                HostScope::Configured {
                    host: dedicated_host,
                    epoch: 1
                }
            )
            .await,
        Err(PlacementError::NoPoolAccess)
    ));
    store.defer_claim(&second, 3600).await.unwrap();

    let owned = create(&store, tenant, &tenant_token).await;
    assert_eq!(
        placed(store.place_create(&owned, HostScope::Fleet).await),
        dedicated_host
    );
    // The tenant is not granted the shared pool either.
    assert!(matches!(
        store
            .place_create(
                &create(&store, tenant, &tenant_token).await,
                HostScope::Configured {
                    host: shared_host,
                    epoch: 1
                }
            )
            .await,
        Err(PlacementError::NoPoolAccess)
    ));
    // No grant at all is denial, even with free capacity everywhere.
    let denied = create(&store, ungranted, &ungranted_token).await;
    assert!(matches!(
        store.place_create(&denied, HostScope::Fleet).await,
        Err(PlacementError::NoPoolAccess)
    ));
    let allocations: Vec<(Uuid, Uuid)> =
        sqlx::query_as("SELECT project_id,host_id FROM allocations ORDER BY created_at")
            .fetch_all(&pool)
            .await
            .unwrap();
    assert_eq!(
        allocations,
        vec![
            (other.uuid(), shared_host.uuid()),
            (tenant.uuid(), dedicated_host.uuid())
        ]
    );
}

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn project_quota_holds_across_hosts_under_concurrency(pool: PgPool) {
    let store = Store::from_pool(pool.clone());
    let shared = host_pool(&store, "shared", Tenancy::Shared).await;
    for _ in 0..4 {
        host(&pool, shared, 2).await;
    }
    let (id, token) = project(&pool).await;
    store.grant_host_pool_access(id, shared).await.unwrap();
    sqlx::query("UPDATE projects SET limits='{\"sandboxes\":2,\"memory_mib\":2048}' WHERE id=$1")
        .bind(id.uuid())
        .execute(&pool)
        .await
        .unwrap();
    for _ in 0..6 {
        admit(&store, id, &token).await;
    }
    let mut tasks = tokio::task::JoinSet::new();
    for _ in 0..6 {
        let store = store.clone();
        tasks.spawn(async move {
            let claim = next(&store).await;
            store.place_create(&claim, HostScope::Fleet).await
        });
    }
    let (mut reserved, mut quota) = (0, 0);
    while let Some(result) = tasks.join_next().await {
        match result.unwrap() {
            Ok(Reservation::Reserved(_)) => reserved += 1,
            Err(PlacementError::Quota) => quota += 1,
            other => panic!("{other:?}"),
        }
    }
    assert_eq!((reserved, quota), (2, 4));
    // Quota counts requested resources; runtime overhead is host-side only.
    let memory: i64 =
        sqlx::query_scalar("SELECT sum(memory_mib)::bigint FROM allocations WHERE project_id=$1")
            .bind(id.uuid())
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(memory, 1024);
}

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn insufficient_capacity_waits_with_reason_then_fails_after_bound(pool: PgPool) {
    let store = Store::from_pool(pool.clone());
    let shared = host_pool(&store, "shared", Tenancy::Shared).await;
    let only = host(&pool, shared, 1).await;
    let (id, token) = project(&pool).await;
    store.grant_host_pool_access(id, shared).await.unwrap();
    let holder = create(&store, id, &token).await;
    assert_eq!(
        placed(store.place_create(&holder, HostScope::Fleet).await),
        only
    );
    let waiting = create(&store, id, &token).await;
    let error = store
        .place_create(&waiting, HostScope::Fleet)
        .await
        .unwrap_err();
    assert_eq!(
        error.wait_reason(),
        Some(PlacementWait::InsufficientCapacity)
    );
    assert_eq!(
        store
            .await_placement(&waiting, PlacementWait::InsufficientCapacity)
            .await
            .unwrap(),
        WaitOutcome::Waiting
    );
    let (status, phase, error, delay, leased): (String, String, Value, f64, bool) = sqlx::query_as(
        "SELECT status,phase,error,extract(epoch FROM next_retry_at-clock_timestamp())::float8,
         lease_expires_at IS NOT NULL FROM operations WHERE id=$1",
    )
    .bind(waiting.operation_id.uuid())
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(
        (status.as_str(), phase.as_str()),
        ("running", "awaiting_placement")
    );
    assert_eq!(error["code"], "insufficient_capacity");
    assert_eq!(error["retryable"], true);
    assert!(!leased);
    assert!(delay > 0.0 && delay <= PLACEMENT_RETRY_MIN_SECONDS as f64 + 0.5);
    // The released claim cannot be used again.
    assert!(matches!(
        store
            .await_placement(&waiting, PlacementWait::InsufficientCapacity)
            .await,
        Err(PlacementError::LostClaim)
    ));

    // Backoff grows with time waited and stays bounded.
    sqlx::query("UPDATE operations SET next_retry_at=NULL,created_at=clock_timestamp()-interval '290 seconds' WHERE id=$1")
        .bind(waiting.operation_id.uuid()).execute(&pool).await.unwrap();
    let retry = next(&store).await;
    assert_eq!(retry.operation_id, waiting.operation_id);
    assert_eq!(
        store
            .await_placement(&retry, PlacementWait::InsufficientCapacity)
            .await
            .unwrap(),
        WaitOutcome::Waiting
    );
    let delay: f64 = sqlx::query_scalar(
        "SELECT extract(epoch FROM next_retry_at-clock_timestamp())::float8 FROM operations WHERE id=$1",
    )
    .bind(waiting.operation_id.uuid())
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(delay > 20.0 && delay <= PLACEMENT_RETRY_MAX_SECONDS as f64 + 0.5);

    // Past the bound the create fails with the waiting reason, freeing nothing
    // it never held and leaving the holder's allocation untouched.
    sqlx::query("UPDATE operations SET next_retry_at=NULL,created_at=clock_timestamp()-interval '301 seconds' WHERE id=$1")
        .bind(waiting.operation_id.uuid()).execute(&pool).await.unwrap();
    let last = next(&store).await;
    assert!(store.place_create(&last, HostScope::Fleet).await.is_err());
    assert_eq!(
        store
            .await_placement(&last, PlacementWait::InsufficientCapacity)
            .await
            .unwrap(),
        WaitOutcome::Expired
    );
    store
        .reject_undispatched_create(&last, PlacementWait::InsufficientCapacity.rejection())
        .await
        .unwrap();
    let (status, phase, error): (String, String, Value) =
        sqlx::query_as("SELECT status,phase,error FROM operations WHERE id=$1")
            .bind(waiting.operation_id.uuid())
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(
        (status.as_str(), phase.as_str()),
        ("failed", "rejected_before_dispatch")
    );
    assert_eq!(error["code"], "insufficient_capacity");
    let unreleased: i64 =
        sqlx::query_scalar("SELECT count(*) FROM allocations WHERE released_at IS NULL")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(unreleased, 1);
}

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn waiting_create_clears_reason_once_capacity_appears(pool: PgPool) {
    let store = Store::from_pool(pool.clone());
    let shared = host_pool(&store, "shared", Tenancy::Shared).await;
    let (id, token) = project(&pool).await;
    store.grant_host_pool_access(id, shared).await.unwrap();
    let claim = create(&store, id, &token).await;
    let error = store
        .place_create(&claim, HostScope::Fleet)
        .await
        .unwrap_err();
    assert_eq!(error.wait_reason(), Some(PlacementWait::NoReadyHost));
    store
        .await_placement(&claim, PlacementWait::NoReadyHost)
        .await
        .unwrap();
    let added = host(&pool, shared, 1).await;
    sqlx::query("UPDATE operations SET next_retry_at=NULL")
        .execute(&pool)
        .await
        .unwrap();
    let retry = next(&store).await;
    assert_eq!(
        placed(store.place_create(&retry, HostScope::Fleet).await),
        added
    );
    let (phase, error): (String, Option<Value>) =
        sqlx::query_as("SELECT phase,error FROM operations WHERE id=$1")
            .bind(claim.operation_id.uuid())
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(phase, "reserved");
    assert!(error.is_none());
}

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn create_claims_take_turns_across_projects(pool: PgPool) {
    let store = Store::from_pool(pool.clone());
    let (busy, busy_token) = project(&pool).await;
    let (quiet, quiet_token) = project(&pool).await;
    for _ in 0..3 {
        admit(&store, busy, &busy_token).await;
    }
    admit(&store, quiet, &quiet_token).await;
    // Claimed creates keep their turn, so the quiet project's only create
    // goes second even though the busy project's backlog is older.
    let mut order = Vec::new();
    while let Some(claim) = store.claim_next(OperationKind::Create, 30).await.unwrap() {
        order.push(claim.project_id);
    }
    assert_eq!(order, vec![busy, quiet, busy, busy]);
}
