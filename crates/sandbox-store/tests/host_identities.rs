//! Two simulated host identities in one isolated PostgreSQL database. Drain,
//! capability approval, epoch fencing and reservations stay with their host.
#![allow(clippy::unwrap_used, clippy::expect_used)]
use sandbox_protocol::{
    HostId, Id, IdempotencyKey, ProjectId, ProjectToken, RequestDigest,
    host_capabilities::{Capabilities, Image, Limits},
};
use sandbox_store::{
    Store,
    admission::{CreateSandbox, Resources},
    claims::{Claim, OperationKind},
    dispatch::DispatchError,
    host_maintenance::Error,
    placement::{PlacementError, Reservation},
    recovery::PreviousAction,
};
use serde_json::{Value, json};
use sqlx::PgPool;

fn image(c: char) -> Image {
    Image {
        rootfs_digest: format!("sha256:{}", c.to_string().repeat(64)),
        kernel_digest: format!("sha256:{}", "b".repeat(64)),
        minimum_disk_mib: 256,
        guest_version: "0.0.0".into(),
        bootstrap_version: 1,
        guest_protocol_version: 1,
    }
}

fn capabilities(images: Vec<Image>) -> Capabilities {
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
            vcpu: 2,
            memory_mib: 2048,
            disk_mib: 8192,
        },
        images,
    }
}

/// Operator-provisioned, capability-enrolled, fresh and ready at epoch 1.
async fn enrolled(store: &Store, value: &Capabilities) -> HostId {
    let id = HostId::generate();
    sqlx::query("INSERT INTO hosts(id,status,cpu_capacity,memory_capacity_mib,disk_capacity_mib,supervisor_epoch,last_seen_at)
        VALUES($1,'ready',4,8192,65536,1,clock_timestamp())")
        .bind(id.uuid()).execute(store.pool()).await.unwrap();
    store.drain_host(id, 1).await.unwrap();
    store
        .set_host_capability_policy(id, 1, value)
        .await
        .unwrap();
    observe(store, id, 1, value).await.unwrap();
    store.finish_host_drain(id, 1).await.unwrap();
    id
}

async fn observe(
    store: &Store,
    id: HostId,
    epoch: i64,
    value: &Capabilities,
) -> Result<(), DispatchError> {
    store
        .observe_configured_host_with_capabilities(id, epoch, &value.to_json().unwrap(), true)
        .await
}

async fn claim(store: &Store, digest: &str) -> Claim {
    let project = ProjectId::generate();
    let token = ProjectToken::generate().unwrap();
    sqlx::query("INSERT INTO projects(id,name,status,limits,api_tokens) VALUES($1,'identities','active','{}',$2)")
        .bind(project.uuid())
        .bind(json!([{"key_id":token.key_id().as_str(),"hash":hex::encode(token.hash().as_bytes())}]))
        .execute(store.pool()).await.unwrap();
    let payload =
        json!({"image_digest":digest,"resources":{"vcpu":1,"memory_mib":128,"disk_mib":256}});
    store
        .admit_create_sandbox(
            &CreateSandbox {
                project_id: project,
                key_id: token.key_id().clone(),
                idempotency_key: IdempotencyKey::parse(&uuid::Uuid::now_v7().to_string()).unwrap(),
                request_digest: RequestDigest::compute("POST", "/v1/sandboxes", &payload).unwrap(),
                image_digest: digest.into(),
                name: None,
                resources: Resources {
                    vcpu: 1,
                    memory_mib: 128,
                    disk_mib: 256,
                },
                payload,
            },
            &sandbox_protocol::images::ImageAllowlist::new([digest.to_owned()]).unwrap(),
        )
        .await
        .unwrap();
    store
        .claim_next(OperationKind::Create, 30)
        .await
        .unwrap()
        .unwrap()
}

async fn reserve(store: &Store, claim: &Claim, host: HostId, epoch: i64) -> uuid::Uuid {
    match store.reserve_create(claim, host, epoch).await.unwrap() {
        Reservation::Reserved(a) => {
            assert_eq!(a.host_id, host);
            a.id.uuid()
        }
        Reservation::Existing(_) => panic!("fresh claim must reserve"),
    }
}

async fn host_row(pool: &PgPool, id: HostId) -> Value {
    sqlx::query_scalar("SELECT to_jsonb(hosts) FROM hosts WHERE id=$1")
        .bind(id.uuid())
        .fetch_one(pool)
        .await
        .unwrap()
}

async fn host_allocations(pool: &PgPool, id: HostId) -> Value {
    sqlx::query_scalar("SELECT COALESCE(jsonb_agg(to_jsonb(a) ORDER BY a.id),'[]') FROM allocations a WHERE host_id=$1")
        .bind(id.uuid()).fetch_one(pool).await.unwrap()
}

async fn observed_state(pool: &PgPool, sandbox: uuid::Uuid) -> String {
    sqlx::query_scalar("SELECT observed_state FROM sandboxes WHERE id=$1")
        .bind(sandbox)
        .fetch_one(pool)
        .await
        .unwrap()
}

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn draining_and_advancing_one_host_never_changes_another_hosts_admission(pool: PgPool) {
    let store = Store::from_pool(pool.clone());
    let caps = capabilities(vec![image('a')]);
    let digest = caps.images[0].rootfs_digest.clone();
    let (a, b) = (enrolled(&store, &caps).await, enrolled(&store, &caps).await);
    let on_a = claim(&store, &digest).await;
    let on_b = claim(&store, &digest).await;
    reserve(&store, &on_a, a, 1).await;
    reserve(&store, &on_b, b, 1).await;
    let b_row = host_row(&pool, b).await;
    let b_allocations = host_allocations(&pool, b).await;
    let b_report = store.host_maintenance(b, 1).await.unwrap();

    let drained = store.drain_host(a, 1).await.unwrap();
    assert_eq!(
        (drained.status.as_str(), drained.outstanding_allocations),
        ("draining", 1)
    );
    assert_eq!(drained.reserved_disk_mib, 256);
    let advanced = store.advance_host_epoch(a, 1).await.unwrap();
    assert_eq!(
        (
            advanced.supervisor_epoch,
            advanced.previous_epoch_allocations,
            advanced.unfinished_operations
        ),
        (2, 1, 1)
    );
    observe(&store, a, 2, &caps).await.unwrap();
    // Host B shares epoch numbering with neither A's old nor new incarnation.
    assert_eq!(host_row(&pool, b).await, b_row);
    assert_eq!(host_allocations(&pool, b).await, b_allocations);
    assert_eq!(store.host_maintenance(b, 1).await.unwrap(), b_report);
    assert_eq!(b_report.previous_epoch_allocations, 0);

    // A's draining successor and stale epoch both refuse placement; B still admits.
    let next = claim(&store, &digest).await;
    for epoch in [1, 2] {
        assert!(matches!(
            store.reserve_create(&next, a, epoch).await,
            Err(PlacementError::HostUnavailable)
        ));
    }
    assert!(matches!(
        store.reserve_create(&next, b, 2).await,
        Err(PlacementError::HostUnavailable)
    ));
    reserve(&store, &next, b, 1).await;
    assert_eq!(
        store
            .host_maintenance(b, 1)
            .await
            .unwrap()
            .outstanding_allocations,
        2
    );
    assert_eq!(
        store
            .host_maintenance(a, 2)
            .await
            .unwrap()
            .outstanding_allocations,
        1
    );

    // A's new epoch number grants no authority over B, and B's over A's past.
    let b_row = host_row(&pool, b).await;
    for result in [
        store.drain_host(b, 2).await,
        store.host_maintenance(b, 2).await,
        store.finish_host_drain(b, 2).await,
        store.advance_host_epoch(b, 2).await,
        store.set_host_capability_policy(b, 2, &caps).await,
        store.host_maintenance(a, 1).await,
        store.drain_host(a, 1).await,
    ] {
        assert!(matches!(result, Err(Error::Conflict)), "{result:?}");
    }
    assert!(matches!(
        observe(&store, b, 2, &caps).await,
        Err(DispatchError::HostUnavailable)
    ));
    assert!(matches!(
        store.invalidate_host_capability_observation(b, 2).await,
        Err(DispatchError::HostUnavailable)
    ));
    assert_eq!(host_row(&pool, b).await, b_row);

    // A remains closed until its own reservation is released and it is reopened.
    assert!(matches!(
        store.finish_host_drain(a, 2).await,
        Err(Error::Outstanding)
    ));
    assert_eq!(host_row(&pool, b).await["status"], "ready");
}

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn capability_approval_on_one_empty_host_never_changes_another_hosts_contract(pool: PgPool) {
    let store = Store::from_pool(pool.clone());
    let old = capabilities(vec![image('a')]);
    let new = capabilities(vec![image('c')]);
    let (a, b) = (enrolled(&store, &old).await, enrolled(&store, &old).await);
    let busy = claim(&store, &old.images[0].rootfs_digest).await;
    reserve(&store, &busy, b, 1).await;
    // B is ready and busy; it cannot receive A's approval as a side effect.
    assert!(matches!(
        store.set_host_capability_policy(b, 1, &new).await,
        Err(Error::Conflict)
    ));
    let b_row = host_row(&pool, b).await;

    store.drain_host(a, 1).await.unwrap();
    store.set_host_capability_policy(a, 1, &new).await.unwrap();
    assert!(matches!(
        observe(&store, a, 1, &old).await,
        Err(DispatchError::HostCompatibility)
    ));
    observe(&store, a, 1, &new).await.unwrap();
    store.finish_host_drain(a, 1).await.unwrap();
    assert_eq!(host_row(&pool, b).await, b_row);

    // Each host admits exactly the images approved for its own identity.
    let old_image = claim(&store, &old.images[0].rootfs_digest).await;
    assert!(matches!(
        store.reserve_create(&old_image, a, 1).await,
        Err(PlacementError::HostCompatibility)
    ));
    reserve(&store, &old_image, b, 1).await;
    let new_image = claim(&store, &new.images[0].rootfs_digest).await;
    assert!(matches!(
        store.reserve_create(&new_image, b, 1).await,
        Err(PlacementError::HostCompatibility)
    ));
    reserve(&store, &new_image, a, 1).await;
    // B's health cannot be satisfied with A's approved contract either.
    assert!(matches!(
        observe(&store, b, 1, &new).await,
        Err(DispatchError::HostCompatibility)
    ));
}

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn an_epoch_for_one_host_cannot_claim_mark_or_reconcile_another_hosts_allocations(
    pool: PgPool,
) {
    let store = Store::from_pool(pool.clone());
    let caps = capabilities(vec![image('a')]);
    let digest = caps.images[0].rootfs_digest.clone();
    let (a, b) = (enrolled(&store, &caps).await, enrolled(&store, &caps).await);
    let on_a = claim(&store, &digest).await;
    let on_b = claim(&store, &digest).await;
    let alloc_a = reserve(&store, &on_a, a, 1).await;
    let alloc_b = reserve(&store, &on_b, b, 1).await;
    store.drain_host(b, 1).await.unwrap();
    store.advance_host_epoch(b, 1).await.unwrap();
    observe(&store, b, 2, &caps).await.unwrap();

    // Previous-epoch recovery resolves only allocations owned by the named host.
    assert!(
        store
            .prepare_previous_allocation(&on_a, b, 2)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        store
            .prepare_previous_allocation(&on_b, a, 2)
            .await
            .unwrap()
            .is_none()
    );
    assert!(matches!(
        store
            .prepare_previous_allocation(&on_b, b, 2)
            .await
            .unwrap(),
        Some(PreviousAction::RejectUndispatched)
    ));

    sqlx::query("UPDATE allocations SET status='running',lease_expires_at=clock_timestamp()+interval '1 minute'")
        .execute(&pool).await.unwrap();
    sqlx::query("UPDATE sandboxes SET observed_state='running',active_transition_operation_id=NULL,observed_at=clock_timestamp(),observation_simulated=true")
        .execute(&pool).await.unwrap();
    // Neither host's current epoch number lets the other claim maintenance.
    assert!(store.claim_allocation(a, 2, 30).await.unwrap().is_none());
    assert!(store.claim_allocation(b, 1, 30).await.unwrap().is_none());
    let claimed = store.claim_allocation(a, 1, 30).await.unwrap().unwrap();
    assert_eq!(claimed.allocation_id.uuid(), alloc_a);
    let claimed = store.claim_allocation(b, 2, 30).await.unwrap().unwrap();
    assert_eq!(claimed.allocation_id.uuid(), alloc_b);

    // B's successor health marks only B's prior incarnation uncertain.
    observe(&store, b, 2, &caps).await.unwrap();
    assert_eq!(
        observed_state(&pool, on_b.sandbox_id.uuid()).await,
        "unknown"
    );
    assert_eq!(
        observed_state(&pool, on_a.sandbox_id.uuid()).await,
        "running"
    );
    // A failed health check at B's old epoch number cannot touch A either.
    store.mark_host_runtime_unknown(b, 1).await.unwrap();
    store.mark_host_runtime_unknown(b, 2).await.unwrap();
    assert_eq!(
        observed_state(&pool, on_a.sandbox_id.uuid()).await,
        "running"
    );
    store.mark_host_runtime_unknown(a, 1).await.unwrap();
    assert_eq!(
        observed_state(&pool, on_a.sandbox_id.uuid()).await,
        "unknown"
    );

    let (ra, rb) = (
        store.host_maintenance(a, 1).await.unwrap(),
        store.host_maintenance(b, 2).await.unwrap(),
    );
    assert_eq!(
        (
            ra.outstanding_allocations,
            ra.previous_epoch_allocations,
            ra.unknown_state_allocations
        ),
        (1, 0, 1)
    );
    assert_eq!(
        (
            rb.outstanding_allocations,
            rb.previous_epoch_allocations,
            rb.unknown_state_allocations
        ),
        (1, 1, 1)
    );
}

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn drain_report_keeps_pinned_uncertain_work_with_its_original_host(pool: PgPool) {
    let store = Store::from_pool(pool.clone());
    let caps = capabilities(vec![image('a')]);
    let (a, b) = (enrolled(&store, &caps).await, enrolled(&store, &caps).await);
    let create = claim(&store, &caps.images[0].rootfs_digest).await;
    let first = reserve(&store, &create, a, 1).await;
    let sandbox = create.sandbox_id.uuid();
    sqlx::query("UPDATE operations SET status='succeeded',completed_at=clock_timestamp(),lease_expires_at=NULL WHERE id=$1")
        .bind(create.operation_id.uuid()).execute(&pool).await.unwrap();
    sqlx::query("UPDATE allocations SET status='running' WHERE id=$1")
        .bind(first)
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("UPDATE sandboxes SET observed_state='running',active_transition_operation_id=NULL WHERE id=$1")
        .bind(sandbox).execute(&pool).await.unwrap();
    // A command whose outcome is unknown remains pinned to A's incarnation.
    sqlx::query("INSERT INTO operations(id,project_id,sandbox_id,kind,initiator_kind,idempotency_key,request_digest,digest_version,payload,status,execution_allocation_id)
        SELECT $1,project_id,id,'execute','project',$2,'\\x00',1,'{}','unknown',$3 FROM sandboxes WHERE id=$4")
        .bind(uuid::Uuid::now_v7()).bind("pinned-command-0001").bind(first).bind(sandbox)
        .execute(&pool).await.unwrap();
    let report = store.host_maintenance(a, 1).await.unwrap();
    assert_eq!(
        (report.unfinished_operations, report.unknown_operations),
        (1, 1)
    );

    // Fixture only: a verified release of A's incarnation and a successor on B.
    // Production replacement placement is not implemented by this store.
    sqlx::query("UPDATE allocations SET status='released',released_at=clock_timestamp(),release_evidence='{\"fixture\":true}' WHERE id=$1")
        .bind(first).execute(&pool).await.unwrap();
    let second = uuid::Uuid::now_v7();
    sqlx::query("INSERT INTO allocations(id,project_id,sandbox_id,host_id,generation,supervisor_epoch,vcpu,memory_mib,disk_mib,status)
        SELECT $1,project_id,id,$2,2,1,1,128,256,'running' FROM sandboxes WHERE id=$3")
        .bind(second).bind(b.uuid()).bind(sandbox).execute(&pool).await.unwrap();
    sqlx::query("UPDATE sandboxes SET current_allocation_id=$1,generation=2 WHERE id=$2")
        .bind(second)
        .bind(sandbox)
        .execute(&pool)
        .await
        .unwrap();
    // Unpinned lifecycle work follows the sandbox's live owner.
    sqlx::query("INSERT INTO operations(id,project_id,sandbox_id,kind,initiator_kind,idempotency_key,request_digest,digest_version,payload,status)
        SELECT $1,project_id,id,'destroy','project',$2,'\\x00',1,'{}','queued' FROM sandboxes WHERE id=$3")
        .bind(uuid::Uuid::now_v7()).bind("lifecycle-destroy-0001").bind(sandbox)
        .execute(&pool).await.unwrap();

    let ra = store.host_maintenance(a, 1).await.unwrap();
    assert_eq!(
        (
            ra.outstanding_allocations,
            ra.unfinished_operations,
            ra.unknown_operations
        ),
        (0, 0, 0)
    );
    let rb = store.host_maintenance(b, 1).await.unwrap();
    assert_eq!(
        (
            rb.outstanding_allocations,
            rb.unfinished_operations,
            rb.unknown_operations
        ),
        (1, 1, 0),
        "B must not inherit A's pinned unknown command"
    );
}
