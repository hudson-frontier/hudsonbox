use super::*;
use sandbox_protocol::{ProjectId, ProjectToken, supervisor::PreviousAllocationRequest};
use sandbox_store::{Store, claims::OperationKind, dispatch::CreateAction};
use serde_json::{Value, json};
use sqlx::{PgPool, Row};

async fn connect(f: &Fixture, store: &Store, image: &str) -> sandbox_controller::Controller {
    sandbox_controller::Controller::connect(
        store.clone(),
        sandbox_controller::ControllerConfig {
            endpoint: f.url.clone(),
            host: f.config.host,
            epoch: f.config.epoch,
            allowed_images: std::collections::BTreeSet::from([image.into()]),
            allow_simulated: false,
        },
        f.tls.ca.pem().as_bytes(),
        f.tls.host.cert.pem().as_bytes(),
        f.tls.host.key.serialize_pem().as_bytes(),
    )
    .await
    .unwrap()
}
async fn snapshot(pool: &PgPool, id: OperationId) -> Value {
    sqlx::query_scalar("SELECT to_jsonb(o) FROM operations o WHERE id=$1")
        .bind(id.uuid())
        .fetch_one(pool)
        .await
        .unwrap()
}
async fn admitted(app: &axum::Router, token: &str, image: &str) -> (OperationId, SandboxId) {
    let (status, a) = http(
        app,
        token,
        "POST",
        "/v1/sandboxes",
        &OperationId::generate().to_string(),
        json!({"image_digest":image,"resources":{"vcpu":1,"memory_mib":128,"disk_mib":64}}),
    )
    .await;
    assert_eq!(status, http::StatusCode::ACCEPTED, "{a}");
    (
        a["operation_id"].as_str().unwrap().parse().unwrap(),
        a["sandbox_id"].as_str().unwrap().parse().unwrap(),
    )
}
async fn complete(
    controller: &mut sandbox_controller::Controller,
    pool: &PgPool,
    id: OperationId,
    status: &str,
) {
    let until = Instant::now() + Duration::from_secs(30);
    loop {
        controller.tick().await.unwrap();
        let row = snapshot(pool, id).await;
        if row["status"] == status {
            break;
        }
        assert!(Instant::now() < until, "operation did not converge: {row}");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
#[ignore = "requires dedicated Linux/KVM development host"]
async fn real_previous_epoch_reclaims_running_vm_without_inventing_command_outcome(pool: PgPool) {
    recover(pool, false).await;
}
#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
#[ignore = "requires dedicated Linux/KVM development host"]
async fn real_previous_epoch_reconciles_lost_create_without_relaunch(pool: PgPool) {
    recover(pool, true).await;
}

#[tokio::test]
#[ignore = "requires dedicated Linux/KVM development host"]
async fn real_previous_epoch_absence_requires_retained_fence_and_no_contradictory_files() {
    let mut f = Fixture::new().await;
    let mut client = f.client().await;
    let owner = Ownership {
        host_id: f.config.host.to_string(),
        project_id: ProjectId::generate().to_string(),
        sandbox_id: SandboxId::generate().to_string(),
        allocation_id: AllocationId::generate().to_string(),
        operation_id: OperationId::generate().to_string(),
        generation: 1,
        supervisor_epoch: 1,
        claim_revision: 1,
        claim_expires_unix_ms: guardian::wall_ms() + 120000,
    };
    let absent = client
        .inspect(InspectRequest {
            ownership: Some(owner.clone()),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(absent.state, AllocationState::Absent as i32);
    f.restart().await;
    let mut client = f.client().await;
    let request = PreviousAllocationRequest {
        ownership: Some(owner.clone()),
        reporting_epoch: 2,
    };
    // The journal fence alone is insufficient when an unowned runtime directory exists.
    let contradictory = f.config.state_root.join("a").join(&owner.allocation_id);
    fs::create_dir_all(&contradictory).unwrap();
    assert!(
        client
            .reconcile_previous_allocation(request.clone())
            .await
            .is_err()
    );
    fs::remove_dir(&contradictory).unwrap();
    let response = client
        .reconcile_previous_allocation(request.clone())
        .await
        .unwrap()
        .into_inner();
    let release = response.release.unwrap();
    assert_eq!(response.request, Some(request));
    assert_eq!(release.state, AllocationState::FencedAbsent as i32);
    assert!(release.create_operation_id.is_empty());
    assert_eq!(release.start_count, 0);
    assert!(!release.simulated);
}
async fn recover(pool: PgPool, lost_create: bool) {
    let mut f = Fixture::new().await;
    let project = ProjectId::generate();
    let token = ProjectToken::generate().unwrap();
    sqlx::query("INSERT INTO projects(id,name,status,limits,api_tokens) VALUES($1,'epoch-recovery','active','{}',$2)")
        .bind(project.uuid()).bind(json!([{"key_id":token.key_id().as_str(),"hash":hex::encode(token.hash().as_bytes())}])).execute(&pool).await.unwrap();
    // One 1 vCPU/128 MiB/64 MiB sandbox plus 128 MiB VMM and 401 MiB artifact overhead.
    sqlx::query("INSERT INTO hosts(id,status,cpu_capacity,memory_capacity_mib,disk_capacity_mib,supervisor_epoch) VALUES($1,'ready',1,256,465,1)")
        .bind(f.config.host.uuid()).execute(&pool).await.unwrap();
    // Placement requires an explicit grant for the configured host's legacy pool.
    sqlx::query("INSERT INTO project_host_pool_access(id,project_id,host_pool_id) SELECT gen_random_uuid(),$1,host_pool_id FROM hosts WHERE id=$2")
        .bind(project.uuid()).bind(f.config.host.uuid()).execute(&pool).await.unwrap();
    let token = token.render_once();
    let image = f.config.images.keys().next().unwrap().clone();
    let store = Store::from_pool(pool.clone());
    let app = sandbox_api::router(sandbox_api::AppState {
        store: store.clone(),
        images: sandbox_protocol::images::ImageAllowlist::new([image.clone()]).unwrap(),
    });
    let mut controller = connect(&f, &store, &image).await;
    let (create, sandbox) = admitted(&app, &token, &image).await;
    if lost_create {
        let claim = store
            .claim_next(OperationKind::Create, 30)
            .await
            .unwrap()
            .unwrap();
        store
            .reserve_create(&claim, f.config.host, 1)
            .await
            .unwrap();
        let CreateAction::Start(request) = store
            .prepare_create_dispatch(&claim, &std::collections::BTreeSet::from([image.clone()]))
            .await
            .unwrap()
        else {
            panic!("start")
        };
        let mut client = f.client().await;
        // Boot may outlive the RPC deadline. Both a reply and a timeout are
        // discarded here; inspect the original identity without another create.
        let _ = client.create(request.clone()).await;
        f.ready(&mut client, request.ownership.as_ref().unwrap())
            .await;
    } else {
        complete(&mut controller, &pool, create, "succeeded").await;
    }
    let allocation: String =
        sqlx::query_scalar("SELECT current_allocation_id::text FROM sandboxes WHERE id=$1")
            .bind(sandbox.uuid())
            .fetch_one(&pool)
            .await
            .unwrap();
    let allocation = AllocationId::from_uuid(allocation.parse().unwrap());
    let owner = Ownership {
        host_id: f.config.host.to_string(),
        project_id: project.to_string(),
        sandbox_id: sandbox.to_string(),
        allocation_id: allocation.to_string(),
        operation_id: create.to_string(),
        generation: 1,
        supervisor_epoch: 1,
        claim_revision: 1,
        claim_expires_unix_ms: guardian::wall_ms() + 120000,
    };
    let manifest = f.manifest(&owner);
    let mut command = None;
    if !lost_create {
        let (status,a)=http(&app,&token,"POST",&format!("/v1/sandboxes/{sandbox}/execute"),&OperationId::generate().to_string(),
            json!({"argv":["/bin/busybox","sh","-c","echo started; /bin/busybox sleep 30"],"deadline_unix_ms":guardian::wall_ms()+45000})).await;
        assert_eq!(status, http::StatusCode::ACCEPTED, "{a}");
        let id: OperationId = a["operation_id"].as_str().unwrap().parse().unwrap();
        let guest = manifest.guest_client().unwrap();
        let until = Instant::now() + Duration::from_secs(10);
        loop {
            controller.tick().await.unwrap();
            if let Ok(receipt) = guest.inspect(id).await
                && receipt.stdout.stored == 8
            {
                break;
            }
            assert!(Instant::now() < until, "command never started");
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        command = Some(id);
    }
    let create_before = snapshot(&pool, create).await;
    f.restart().await;
    // Health/current epoch alone is not database release evidence.
    assert!(controller.tick().await.is_err());
    let held: bool = sqlx::query_scalar("SELECT released_at IS NULL FROM allocations WHERE id=$1")
        .bind(allocation.uuid())
        .fetch_one(&pool)
        .await
        .unwrap();
    assert!(held);
    let mut client = f.client().await;
    assert_eq!(
        client
            .inspect(InspectRequest {
                ownership: Some(owner.clone())
            })
            .await
            .unwrap_err()
            .code(),
        Code::FailedPrecondition
    );
    let request = PreviousAllocationRequest {
        ownership: Some(owner.clone()),
        reporting_epoch: 2,
    };
    for field in [
        "current_epoch",
        "same_epoch",
        "project",
        "generation",
        "missing",
        "expired",
    ] {
        let mut bad = request.clone();
        match field {
            "current_epoch" => bad.reporting_epoch = 3,
            "same_epoch" => bad.ownership.as_mut().unwrap().supervisor_epoch = 2,
            "project" => {
                bad.ownership.as_mut().unwrap().project_id = ProjectId::generate().to_string()
            }
            "generation" => bad.ownership.as_mut().unwrap().generation += 1,
            "missing" => {
                bad.ownership.as_mut().unwrap().allocation_id = AllocationId::generate().to_string()
            }
            _ => bad.ownership.as_mut().unwrap().claim_expires_unix_ms = 1,
        }
        assert!(
            client.reconcile_previous_allocation(bad).await.is_err(),
            "{field}"
        );
    }
    // A read-only API identity cannot invoke the controller's cleanup method.
    let mut reader = transport::connect(
        &f.url,
        f.config.host,
        f.tls.ca.pem().as_bytes(),
        f.reader.cert.pem().as_bytes(),
        f.reader.key.serialize_pem().as_bytes(),
    )
    .await
    .unwrap();
    assert_eq!(
        reader
            .reconcile_previous_allocation(request.clone())
            .await
            .unwrap_err()
            .code(),
        Code::PermissionDenied
    );
    let until = Instant::now() + Duration::from_secs(15);
    loop {
        if let Ok(reply) = client.reconcile_previous_allocation(request.clone()).await {
            let value = reply.into_inner();
            assert_eq!(value.request.as_ref(), Some(&request));
            let released = value.release.unwrap();
            assert_eq!(released.state, AllocationState::Released as i32);
            assert!(!released.simulated);
            break; // Discard this release acknowledgement; the database still retains capacity.
        }
        assert!(Instant::now() < until, "old host cleanup not confirmed");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(!manifest.group().exists());
    assert!(!manifest.directory().join("run").exists());
    assert!(manifest.receipt().unwrap().cleanup_confirmed);
    assert!(
        sqlx::query_scalar::<_, bool>("SELECT released_at IS NULL FROM allocations WHERE id=$1")
            .bind(allocation.uuid())
            .fetch_one(&pool)
            .await
            .unwrap()
    );
    sqlx::query("UPDATE hosts SET supervisor_epoch=2")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("UPDATE allocations SET maintenance_next_at=NULL,maintenance_lease_until=NULL")
        .execute(&pool)
        .await
        .unwrap();
    // Emulate the lost controller claim expiring; recovery must claim a new revision.
    sqlx::query("UPDATE operations SET lease_expires_at=clock_timestamp()-interval '1 second',next_retry_at=NULL WHERE completed_at IS NULL").execute(&pool).await.unwrap();
    let mut current = connect(&f, &store, &image).await;
    let until = Instant::now() + Duration::from_secs(20);
    loop {
        current.tick().await.unwrap();
        let done:bool=sqlx::query_scalar("SELECT destroyed_at IS NOT NULL AND current_allocation_id IS NULL FROM sandboxes WHERE id=$1").bind(sandbox.uuid()).fetch_one(&pool).await.unwrap();
        let command_unknown = if let Some(id) = command {
            snapshot(&pool, id).await["status"] == "unknown"
        } else {
            true
        };
        if done && command_unknown {
            break;
        }
        assert!(Instant::now() < until, "database recovery did not converge");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let after = snapshot(&pool, create).await;
    if lost_create {
        assert_eq!(after["status"], "failed");
        assert_eq!(after["error"]["create_outcome_unknown"], true);
    } else {
        assert_eq!(after, create_before);
    }
    assert_eq!(after["attempt_count"], 1);
    if let Some(id) = command {
        let row = snapshot(&pool, id).await;
        assert_eq!(row["attempt_count"], 1);
        assert!(row["completed_at"].is_null());
        assert!(row["result"].is_null());
    }
    let row = sqlx::query("SELECT supervisor_epoch,release_evidence FROM allocations WHERE id=$1")
        .bind(allocation.uuid())
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(row.get::<i64, _>("supervisor_epoch"), 1);
    let proof: Value = row.get("release_evidence");
    assert_eq!(proof["reporting_epoch"], 2);
    assert_eq!(proof["simulated"], false);
    // The same host can now admit and run a fresh sandbox under epoch two.
    let (replacement, new_sandbox) = admitted(&app, &token, &image).await;
    complete(&mut current, &pool, replacement, "succeeded").await;
    let (status, d) = http(
        &app,
        &token,
        "POST",
        &format!("/v1/sandboxes/{new_sandbox}/destroy"),
        &OperationId::generate().to_string(),
        json!({}),
    )
    .await;
    assert_eq!(status, http::StatusCode::ACCEPTED);
    complete(
        &mut current,
        &pool,
        d["operation_id"].as_str().unwrap().parse().unwrap(),
        "succeeded",
    )
    .await;
    let reserved: i64 =
        sqlx::query_scalar("SELECT count(*) FROM allocations WHERE released_at IS NULL")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(reserved, 0);
    eprintln!(
        "previous_epoch_recovery_observation={}",
        json!({"simulated":false,"lost_create":lost_create,"original_epoch_retained":true,
        "reporting_epoch":2,"physical_cleanup_verified":true,"database_capacity_reclaimed":true,"replacement_ran":true,
        "unknown_command_not_replayed":command.is_some(),"stale_mutations_rejected":true,"missing_journal_rejected":true,"reader_denied":true})
    );
}
