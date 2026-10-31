//! #197 portable Kubernetes API/PostgreSQL failure checks, not cluster/KVM evidence.
#![allow(clippy::unwrap_used, clippy::expect_used)]
use sandbox_controller::{
    capacity::ProviderRouter,
    host_discovery::{DiscoveryError, HostDiscovery, reconcile},
    k8s_capacity::{ApiFuture, KubernetesApi, KubernetesCapacityProvider},
};
use sandbox_protocol::{
    HostId, Id,
    host_capabilities::{Capabilities, Image, Limits},
};
use sandbox_store::{
    Store,
    capacity::{Action, Claim, Error, Request},
    host_maintenance::{HostCapacity, HostProvision},
    host_registry::{HostPoolSpec, PlacementPolicy, PoolType, Tenancy},
    k8s_capacity::{Artifact, Image as RuntimeImage, RuntimeFiles, WORKER_LABEL, WorkerConfig},
};
use serde_json::{Value, json};
use sqlx::PgPool;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};
use uuid::Uuid;
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
            rootfs_digest: format!("sha256:{}", "a".repeat(64)),
            kernel_digest: format!("sha256:{}", "b".repeat(64)),
            minimum_disk_mib: 256,
            guest_version: env!("CARGO_PKG_VERSION").into(),
            bootstrap_version: 1,
            guest_protocol_version: 1,
        }],
    }
}

fn provision(pool: Option<Uuid>) -> HostProvision {
    HostProvision {
        version: 1,
        capacity: HostCapacity {
            vcpu: 4,
            memory_mib: 8192,
            disk_mib: 65536,
        },
        capabilities: capabilities(),
        host_pool_id: pool,
    }
}

fn worker() -> WorkerConfig {
    let artifact = || Artifact {
        path: "/opt/artifact".into(),
        sha256: "a".repeat(64),
    };
    WorkerConfig {
        image: format!("registry.invalid/worker@sha256:{}", "b".repeat(64)),
        identity_secret: format!("identity-{}", Uuid::now_v7()),
        controller_certificate_sha256: "c".repeat(64),
        server_certificate_sha256: "d".repeat(64),
        output_reader_certificate_sha256: vec!["e".repeat(64)],
        file_reader_certificate_sha256: vec!["f".repeat(64)],
        artifacts_secret: Some("artifacts-v1".into()),
        runtime: RuntimeFiles {
            guardian_binary: "/usr/local/bin/sandbox-supervisor".into(),
            firecracker: artifact(),
            jailer: artifact(),
            images: std::collections::BTreeMap::from([(
                format!("sha256:{}", "a".repeat(64)),
                RuntimeImage {
                    kernel: artifact(),
                    rootfs: artifact(),
                },
            )]),
            jail_uid: 1234,
            jail_gid: 1234,
        },
        overhead_vcpu: 1,
        overhead_memory_mib: 256,
        overhead_disk_mib: 512,
    }
}
async fn setup(pg: PgPool) -> (Store, Uuid, Request, Arc<FakeKubernetes>, ProviderRouter) {
    let store = Store::from_pool(pg);
    let pool = Uuid::now_v7();
    store.put_host_pool(&HostPoolSpec{version:1,id:pool,name:"k8s".into(),pool_type:PoolType::K8s,min_ready_hosts:0,max_hosts:2,
        placement_policy:PlacementPolicy{version:1,tenancy:Tenancy::Shared},provider_config:json!({"version":1,"namespace":"workers","worker_label_selector":"hudsonbox.io/worker=true"}),credential_secret_ref:Some("secrets/cluster-v1".into())}).await.unwrap();
    let request = Request {
        host: HostId::generate(),
        epoch: 1,
        action: Action::K8sProvision {
            config: Box::new(provision(Some(pool))),
            worker: Box::new(worker()),
        },
    };
    let api = Arc::new(FakeKubernetes::default());
    let router =
        ProviderRouter::default().with(Arc::new(KubernetesCapacityProvider::new(api.clone())));
    (store, pool, request, api, router)
}
#[derive(Debug, Default)]
struct FakeKubernetes {
    pod: Mutex<Option<Value>>,
    extra: Mutex<Vec<Value>>,
    calls: Mutex<Vec<String>>,
    lose_create: AtomicBool,
    lose_delete: AtomicBool,
    pause_post: AtomicBool,
    post_started: tokio::sync::Notify,
    post_release: tokio::sync::Notify,
}
impl KubernetesApi for FakeKubernetes {
    fn credential_reference(&self) -> &str {
        "secrets/cluster-v1"
    }
    fn call<'a>(
        &'a self,
        method: &'a str,
        _namespace: &'a str,
        name: Option<&'a str>,
        body: Option<Value>,
        selector: Option<String>,
    ) -> ApiFuture<'a> {
        Box::pin(async move {
            if method == "POST" && self.pause_post.swap(false, Ordering::SeqCst) {
                self.post_started.notify_one();
                self.post_release.notified().await;
            }
            self.calls.lock().unwrap().push(method.into());
            let mut pod = self.pod.lock().unwrap();
            match method {
                "GET" if name.is_none() => {
                    assert!(selector.unwrap().contains("hudsonbox.io/pool="));
                    Ok(Some(
                        json!({"metadata":{},"items":pod.iter().cloned().chain(self.extra.lock().unwrap().iter().cloned()).collect::<Vec<_>>()}),
                    ))
                }
                "GET" => Ok(pod.clone()),
                "POST" => {
                    if pod.is_some() {
                        return Ok(None);
                    }
                    let mut created = body.unwrap();
                    created["metadata"]["uid"] = json!(Uuid::now_v7().to_string());
                    created["status"] = json!({"phase":"Running"});
                    *pod = Some(created.clone());
                    if self.lose_create.swap(false, Ordering::SeqCst) {
                        return Err(Error::Conflict);
                    }
                    Ok(Some(created))
                }
                "DELETE" => {
                    let current = pod.as_mut().ok_or(Error::Conflict)?;
                    assert_eq!(
                        body.unwrap()["preconditions"]["uid"],
                        current["metadata"]["uid"]
                    );
                    current["metadata"]["deletionTimestamp"] = json!("2026-10-30T12:00:00Z");
                    // Finalizer preserves the name/UID even after physical cleanup.
                    if self.lose_delete.swap(false, Ordering::SeqCst) {
                        return Err(Error::Conflict);
                    }
                    Ok(Some(current.clone()))
                }
                _ => panic!("unexpected effect"),
            }
        })
    }
}
async fn enqueue_claim(store: &Store, pool: Uuid, key: &str, request: &Request) -> Claim {
    store.enqueue_capacity(pool, key, request).await.unwrap();
    store.claim_capacity(pool, 30).await.unwrap().unwrap()
}
async fn fresh(store: &Store, request: &Request) {
    store
        .observe_configured_host_with_capabilities(
            request.host,
            1,
            &capabilities().to_json().unwrap(),
            true,
        )
        .await
        .unwrap();
}
#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn lost_create_ack_reconciles_frozen_config_and_fences_old_receipts(pg: PgPool) {
    let (store, pool, request, api, router) = setup(pg.clone()).await;
    let claim = enqueue_claim(&store, pool, "new", &request).await;
    api.lose_create.store(true, Ordering::SeqCst);
    assert!(router.apply(&store, &claim).await.is_err());
    assert!(store.cancel_capacity(pool, claim.id).await.is_err());
    assert_eq!(
        store.enqueue_capacity(pool, "new", &request).await.unwrap(),
        claim.id
    );
    let mut changed = request.clone();
    changed.epoch = 2;
    assert!(store.enqueue_capacity(pool, "new", &changed).await.is_err());
    let old_uid = api.pod.lock().unwrap().as_ref().unwrap()["metadata"]["uid"].clone();
    // A new provider configuration must not redirect the already dispatched intent.
    sqlx::query("UPDATE host_pools SET provider_config=jsonb_set(provider_config,'{namespace}','\"elsewhere\"'),credential_secret_ref='secrets/cluster-v2' WHERE id=$1").bind(pool).execute(&pg).await.unwrap();
    sqlx::query("UPDATE capacity_operations SET lease_expires_at=clock_timestamp()-interval '1 second',available_at=clock_timestamp()-interval '1 second' WHERE id=$1").bind(claim.id).execute(&pg).await.unwrap();
    let latest = store.claim_capacity(pool, 30).await.unwrap().unwrap();
    assert!(router.apply(&store, &claim).await.is_err());
    let receipt = router.apply(&store, &latest).await.unwrap();
    assert_eq!(receipt["pod_uid"], old_uid);
    assert_eq!(receipt["namespace"], "workers");
    assert_eq!(
        api.calls
            .lock()
            .unwrap()
            .iter()
            .filter(|s| *s == "POST")
            .count(),
        1
    );
    assert!(
        store
            .complete_k8s_capacity(&claim, "wrong", &json!({}))
            .await
            .is_err()
    );
    assert!(store.apply_static_capacity(&latest).await.is_err());
}
#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn lost_delete_ack_retains_tombstone_and_admission_closure(pg: PgPool) {
    let (store, pool, request, api, router) = setup(pg.clone()).await;
    let provision = enqueue_claim(&store, pool, "new", &request).await;
    router.apply(&store, &provision).await.unwrap();
    let retire = Request {
        action: Action::Retire,
        ..request.clone()
    };
    let claim = enqueue_claim(&store, pool, "remove", &retire).await;
    // No fresh authenticated health => no DELETE.
    assert!(router.apply(&store, &claim).await.is_err());
    fresh(&store, &request).await;
    api.lose_delete.store(true, Ordering::SeqCst);
    assert!(router.apply(&store, &claim).await.is_err());
    assert!(store.finish_host_drain(request.host, 1).await.is_err());
    let receipt = router.apply(&store, &claim).await.unwrap();
    assert_eq!(receipt["deletion_requested"], true);
    assert_eq!(receipt["physical_termination_confirmed"], false);
    assert_eq!(
        api.calls
            .lock()
            .unwrap()
            .iter()
            .filter(|s| *s == "DELETE")
            .count(),
        1
    );
    // A delayed old create sees the retained name, never another live incarnation.
    let frozen = store.prepare_k8s_capacity(&provision).await;
    assert!(frozen.is_err());
    assert!(
        api.call("POST", "workers", None, Some(json!({})), None)
            .await
            .unwrap()
            .is_none()
    );
    let same_host = Request {
        action: Action::K8sProvision {
            config: Box::new(provision_config(pool)),
            worker: Box::new(worker()),
        },
        ..request.clone()
    };
    assert!(
        store
            .enqueue_capacity(pool, "reuse", &same_host)
            .await
            .is_err()
    );
    let replacement = Request {
        host: HostId::generate(),
        ..same_host
    };
    store
        .enqueue_capacity(pool, "replacement", &replacement)
        .await
        .unwrap();
    let discovery = KubernetesCapacityProvider::new(api);
    assert!(
        discovery
            .discover(&store.host_pool(pool).await.unwrap().unwrap())
            .await
            .unwrap()
            .is_empty()
    );
}
fn provision_config(pool: Uuid) -> HostProvision {
    provision(Some(pool))
}
#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn unretired_warm_members_block_pod_retirement_until_cleanup(pg: PgPool) {
    let (store, pool, request, api, router) = setup(pg.clone()).await;
    let provision = enqueue_claim(&store, pool, "new", &request).await;
    router.apply(&store, &provision).await.unwrap();
    fresh(&store, &request).await;
    let claim = enqueue_claim(
        &store,
        pool,
        "retire",
        &Request {
            action: Action::Retire,
            ..request.clone()
        },
    )
    .await;
    let template = Uuid::now_v7();
    let policy = Uuid::now_v7();
    let member = Uuid::now_v7();
    sqlx::query("INSERT INTO sandbox_templates(id,name,version,publication,image_digest,vcpu,memory_mib,disk_mib,boot_config,compatibility) VALUES($1,'k8s-warm',1,'shared',$2,1,512,1024,'{}','{}')")
        .bind(template).bind(format!("sha256:{}", "a".repeat(64))).execute(&pg).await.unwrap();
    sqlx::query("INSERT INTO warm_pool_policies(id,template_id,host_pool_id,min_ready,max_total,max_per_host,idle_ttl_seconds,enabled) VALUES($1,$2,$3,0,1,1,600,true)")
        .bind(policy).bind(template).bind(pool).execute(&pg).await.unwrap();
    // An expired lease and absent prepare receipt represent an unknown prepare
    // outcome. No allocation or customer operation accounts for this reservation.
    sqlx::query("INSERT INTO warm_pool_members(id,pool_id,host_id,supervisor_epoch,template_id,vcpu,memory_mib,disk_mib,state,lease_expires_at) VALUES($1,$2,$3,1,$4,1,512,1024,'preparing',clock_timestamp()-interval '1 minute')")
        .bind(member).bind(policy).bind(request.host.uuid()).bind(template).execute(&pg).await.unwrap();
    let initial_calls = api.calls.lock().unwrap().clone();
    for state in ["preparing", "ready", "retiring"] {
        sqlx::query("UPDATE hosts SET last_seen_at=clock_timestamp() WHERE id=$1")
            .bind(request.host.uuid())
            .execute(&pg)
            .await
            .unwrap();
        sqlx::query("UPDATE warm_pool_members SET state=$2,vm_ref=CASE WHEN $2='preparing' THEN NULL ELSE 'warm-vm' END,prepare_receipt=CASE WHEN $2='preparing' THEN NULL ELSE '{}'::jsonb END,lease_expires_at=CASE WHEN $2='preparing' THEN clock_timestamp()-interval '1 minute' ELSE clock_timestamp()+interval '1 minute' END WHERE id=$1")
            .bind(member).bind(state).execute(&pg).await.unwrap();
        let report = store.host_maintenance(request.host, 1).await.unwrap();
        assert_eq!(report.status, "draining");
        assert_eq!(report.outstanding_allocations, 0);
        assert_eq!(report.unfinished_operations, 0);
        assert_eq!(report.unretired_warm_members, 1);
        assert!(router.apply(&store, &claim).await.is_err(), "{state}");
        assert_eq!(*api.calls.lock().unwrap(), initial_calls);
        let closed: bool =
            sqlx::query_scalar("SELECT capacity_retired_at IS NOT NULL FROM hosts WHERE id=$1")
                .bind(request.host.uuid())
                .fetch_one(&pg)
                .await
                .unwrap();
        assert!(!closed);
    }
    sqlx::query("UPDATE warm_pool_members SET state='retired',cleanup_evidence='{}' WHERE id=$1")
        .bind(member)
        .execute(&pg)
        .await
        .unwrap();
    assert_eq!(
        store
            .host_maintenance(request.host, 1)
            .await
            .unwrap()
            .unretired_warm_members,
        0
    );
    assert_eq!(
        router.apply(&store, &claim).await.unwrap()["deletion_requested"],
        true
    );
    assert_eq!(
        api.calls
            .lock()
            .unwrap()
            .iter()
            .filter(|s| *s == "DELETE")
            .count(),
        1
    );
}
#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn occupied_unknown_worker_refuses_removal_and_drain_preserves_allocations(pg: PgPool) {
    let (store, pool, request, api, router) = setup(pg.clone()).await;
    let claim = enqueue_claim(&store, pool, "new", &request).await;
    router.apply(&store, &claim).await.unwrap();
    fresh(&store, &request).await;
    store.finish_host_drain(request.host, 1).await.unwrap();
    let project = Uuid::now_v7();
    let sandbox = Uuid::now_v7();
    let allocation = Uuid::now_v7();
    sqlx::query("INSERT INTO projects(id,name,status,limits,api_tokens) VALUES($1,'k8s','active','{}','[]')").bind(project).execute(&pg).await.unwrap();
    sqlx::query("INSERT INTO sandboxes(id,project_id,image_digest,resources,desired_state,observed_state) VALUES($1,$2,'fixture','{}','running','unknown')").bind(sandbox).bind(project).execute(&pg).await.unwrap();
    sqlx::query("INSERT INTO allocations(id,project_id,sandbox_id,host_id,generation,supervisor_epoch,vcpu,memory_mib,disk_mib,status) VALUES($1,$2,$3,$4,1,1,1,512,1024,'running')").bind(allocation).bind(project).bind(sandbox).bind(request.host.uuid()).execute(&pg).await.unwrap();
    let drain = Request {
        action: Action::Drain,
        ..request.clone()
    };
    let claim = enqueue_claim(&store, pool, "drain", &drain).await;
    router.apply(&store, &claim).await.unwrap();
    let retire = Request {
        action: Action::Retire,
        ..request.clone()
    };
    let claim = enqueue_claim(&store, pool, "retire", &retire).await;
    assert!(router.apply(&store, &claim).await.is_err());
    let report = store.host_maintenance(request.host, 1).await.unwrap();
    assert_eq!(report.outstanding_allocations, 1);
    assert_eq!(report.unknown_state_allocations, 1);
    assert_eq!(report.status, "draining");
    assert!(!api.calls.lock().unwrap().iter().any(|s| s == "DELETE"));
    let discovery = KubernetesCapacityProvider::new(api.clone());
    let discovered = discovery
        .discover(&store.host_pool(pool).await.unwrap().unwrap())
        .await
        .unwrap();
    assert_eq!(discovered.len(), 1);
    assert_eq!(discovered[0].host_id, request.host);
    assert!(
        discovered[0]
            .endpoint
            .contains(".hudsonbox-workers.workers.svc:7443")
    );
    api.pod.lock().unwrap().as_mut().unwrap()["metadata"]["uid"] = json!("different-incarnation");
    let claim = enqueue_claim(
        &store,
        pool,
        "status",
        &Request {
            action: Action::Status,
            ..request
        },
    )
    .await;
    assert!(router.apply(&store, &claim).await.is_err());
}
#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn pending_workers_bound_concurrent_demand_and_failed_inputs_rollback(pg: PgPool) {
    let (store, pool, request, _api, _router) = setup(pg.clone()).await;
    let other = Request {
        host: HostId::generate(),
        ..request.clone()
    };
    let (a, b) = tokio::join!(
        store.enqueue_capacity(pool, "a", &request),
        store.enqueue_capacity(pool, "b", &other)
    );
    a.unwrap();
    b.unwrap();
    let third = Request {
        host: HostId::generate(),
        ..request.clone()
    };
    assert!(store.enqueue_capacity(pool, "c", &third).await.is_err());
    let host_pool = store.host_pool(pool).await.unwrap().unwrap();
    let plan = sandbox_controller::capacity::plan(
        &store,
        &host_pool,
        sandbox_controller::capacity::Demand {
            waiting_sandboxes: 100,
            sandboxes_per_host: 2,
            max_additional_hosts: 100,
        },
    )
    .await
    .unwrap();
    assert_eq!(plan.suggested_additional_hosts, 0);
    let count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM capacity_operations WHERE host_pool_id=$1")
            .bind(pool)
            .fetch_one(&pg)
            .await
            .unwrap();
    assert_eq!(count, 2);
}
#[test]
fn pod_envelope_has_explicit_overhead_and_global_node_exclusion() {
    let pool = Uuid::now_v7();
    let request = Request {
        host: HostId::generate(),
        epoch: 1,
        action: Action::Status,
    };
    let config = provision(Some(pool));
    let mut worker = worker();
    let pod = worker.pod(pool, &request, &config, "workers").unwrap();
    let artifact_mount = pod["spec"]["containers"][0]["volumeMounts"]
        .as_array()
        .unwrap()
        .iter()
        .find(|mount| mount["name"] == "artifacts")
        .unwrap();
    assert_eq!(artifact_mount["mountPath"], "/artifacts/config.json");
    assert_eq!(artifact_mount["subPath"], "config.json");
    assert_eq!(artifact_mount["readOnly"], true);
    assert_eq!(pod["metadata"]["labels"][WORKER_LABEL], "true");
    assert_eq!(
        pod["spec"]["containers"][0]["resources"]["requests"],
        json!({"cpu":"5","memory":"8448Mi","ephemeral-storage":"66048Mi"})
    );
    assert_eq!(
        pod["spec"]["containers"][0]["resources"]["requests"],
        pod["spec"]["containers"][0]["resources"]["limits"]
    );
    assert_eq!(
        pod["spec"]["affinity"]["podAntiAffinity"]["requiredDuringSchedulingIgnoredDuringExecution"]
            [0]["namespaceSelector"],
        json!({})
    );
    let runtime: Value = serde_json::from_str(
        pod["spec"]["containers"][0]["env"][0]["value"]
            .as_str()
            .unwrap(),
    )
    .unwrap();
    assert_eq!(
        runtime["capacity"]["memory_mib"],
        config.capacity.memory_mib
    );
    assert_eq!(runtime["host"], request.host.to_string());
    assert_eq!(runtime["launch_permits_required"], true);
    #[cfg(target_os = "linux")]
    let _: sandbox_supervisor::host::Config = serde_json::from_value(runtime).unwrap();
    worker.overhead_memory_mib = 0;
    assert!(worker.pod(pool, &request, &config, "workers").is_err());
    worker.overhead_memory_mib = u64::MAX;
    assert!(worker.pod(pool, &request, &config, "workers").is_err());
}

#[cfg(unix)]
#[test]
fn projected_artifact_secret_requires_a_regular_file_mount() {
    use std::os::unix::{fs::PermissionsExt, fs::symlink};
    let root = tempfile::tempdir().unwrap();
    let version = root.path().join("..version");
    std::fs::create_dir(&version).unwrap();
    let file = version.join("config.json");
    std::fs::write(&file, br#"{"endpoint":"https://objects.example.test","region":"us-east-1","bucket":"output-test","access_key":"test-access","secret_key":"test-secret"}"#).unwrap();
    std::fs::set_permissions(&file, std::fs::Permissions::from_mode(0o400)).unwrap();
    symlink("..version", root.path().join("..data")).unwrap();
    symlink("..data/config.json", root.path().join("config.json")).unwrap();
    assert!(sandbox_artifacts::S3Config::read_private(&root.path().join("config.json")).is_err());
    // Model the regular inode exposed by a single-file subPath bind mount.
    let mounted = root.path().join("mounted.json");
    std::fs::hard_link(&file, &mounted).unwrap();
    assert!(sandbox_artifacts::S3Config::read_private(&mounted).is_ok());
}

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn delayed_expired_create_cannot_resurrect_a_retired_worker(pg: PgPool) {
    let (store, pool, request, api, router) = setup(pg.clone()).await;
    let router = Arc::new(router);
    let first = enqueue_claim(&store, pool, "worker", &request).await;
    api.pause_post.store(true, Ordering::SeqCst);
    let task = {
        let store = Store::from_pool(pg.clone());
        let router = router.clone();
        let first = first.clone();
        tokio::spawn(async move { router.apply(&store, &first).await })
    };
    tokio::time::timeout(
        std::time::Duration::from_secs(5),
        api.post_started.notified(),
    )
    .await
    .unwrap();
    sqlx::query("UPDATE capacity_operations SET lease_expires_at=clock_timestamp()-interval '1 second',available_at=clock_timestamp()-interval '1 second' WHERE id=$1").bind(first.id).execute(&pg).await.unwrap();
    let successor = store.claim_capacity(pool, 30).await.unwrap().unwrap();
    router.apply(&store, &successor).await.unwrap();
    fresh(&store, &request).await;
    let retire = enqueue_claim(
        &store,
        pool,
        "retire",
        &Request {
            action: Action::Retire,
            ..request.clone()
        },
    )
    .await;
    router.apply(&store, &retire).await.unwrap();
    let before = api.pod.lock().unwrap().clone();
    api.post_release.notify_one();
    assert!(
        tokio::time::timeout(std::time::Duration::from_secs(5), task)
            .await
            .unwrap()
            .unwrap()
            .is_err()
    );
    assert_eq!(api.pod.lock().unwrap().clone(), before);
    assert!(api.pod.lock().unwrap().as_ref().unwrap()["metadata"]["deletionTimestamp"].is_string());
    assert!(store.finish_host_drain(request.host, 1).await.is_err());
}
#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn admission_mutation_missing_object_and_finalizer_loss_fail_closed(pg: PgPool) {
    let (store, pool, request, api, router) = setup(pg.clone()).await;
    let claim = enqueue_claim(&store, pool, "worker", &request).await;
    api.lose_create.store(true, Ordering::SeqCst);
    assert!(router.apply(&store, &claim).await.is_err());
    fresh(&store, &request).await;
    assert!(store.finish_host_drain(request.host, 1).await.is_err());
    let original = api.pod.lock().unwrap().clone().unwrap();
    // Default fields/canonical quantities are harmless; narrowing global affinity is not.
    api.pod.lock().unwrap().as_mut().unwrap()["spec"]["affinity"]["podAntiAffinity"]["requiredDuringSchedulingIgnoredDuringExecution"]
        [0]["namespaceSelector"] = json!({"matchLabels":{"pool":"one"}});
    assert!(router.apply(&store, &claim).await.is_err());
    *api.pod.lock().unwrap() = Some(original.clone());
    api.pod.lock().unwrap().as_mut().unwrap()["spec"]["containers"][0]["args"] = json!(["changed"]);
    assert!(router.apply(&store, &claim).await.is_err());
    *api.pod.lock().unwrap() = Some(original.clone());
    api.pod.lock().unwrap().as_mut().unwrap()["metadata"]["finalizers"] = json!([]);
    assert!(router.apply(&store, &claim).await.is_err());
    *api.pod.lock().unwrap() = Some(original);
    {
        let mut pod = api.pod.lock().unwrap();
        let pod = pod.as_mut().unwrap();
        pod["spec"]["dnsPolicy"] = json!("ClusterFirst");
        pod["metadata"]["annotations"]["cni.example/pod-ip"] = json!("192.0.2.10");
        pod["spec"]["volumes"][1]["emptyDir"]["sizeLimit"] = json!("64Gi");
        pod["spec"]["tolerations"].as_array_mut().unwrap().push(json!({"key":"node.kubernetes.io/not-ready","operator":"Exists","effect":"NoExecute","tolerationSeconds":300}));
        pod["spec"]["containers"][0]["resources"]["requests"]["cpu"] = json!("5000m");
    }
    router.apply(&store, &claim).await.unwrap();
    *api.pod.lock().unwrap() = None;
    let status = enqueue_claim(
        &store,
        pool,
        "status",
        &Request {
            action: Action::Status,
            ..request
        },
    )
    .await;
    assert!(router.apply(&store, &status).await.is_err());
    assert_eq!(
        api.calls
            .lock()
            .unwrap()
            .iter()
            .filter(|s| *s == "POST")
            .count(),
        1
    );
}

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn second_worker_on_same_node_is_not_double_counted(pg: PgPool) {
    let (store, pool, request, api, router) = setup(pg).await;
    let claim = enqueue_claim(&store, pool, "worker", &request).await;
    router.apply(&store, &claim).await.unwrap();
    let mut second = api.pod.lock().unwrap().clone().unwrap();
    let other = HostId::generate();
    second["metadata"]["name"] = json!(format!("hb-{}", other.uuid()));
    second["metadata"]["uid"] = json!(Uuid::now_v7().to_string());
    second["metadata"]["annotations"]["hudsonbox.io/host"] = json!(other.to_string());
    second["metadata"]["annotations"]["hudsonbox.io/certificate-sha256"] = json!("9".repeat(64));
    api.pod.lock().unwrap().as_mut().unwrap()["spec"]["nodeName"] = json!("node-a");
    second["spec"]["nodeName"] = json!("node-a");
    api.extra.lock().unwrap().push(second);
    let discovery = KubernetesCapacityProvider::new(api.clone());
    let spec = store.host_pool(pool).await.unwrap().unwrap();
    // Same node: refused, never two capacity contributions for one node.
    assert!(matches!(
        discovery.discover(&spec).await,
        Err(DiscoveryError::InvalidResult)
    ));
    // Distinct nodes: both counted, proving the node is the reason for refusal.
    api.extra.lock().unwrap()[0]["spec"]["nodeName"] = json!("node-b");
    assert_eq!(discovery.discover(&spec).await.unwrap().len(), 2);
    // A terminating predecessor on the node is not an active worker.
    api.extra.lock().unwrap()[0]["spec"]["nodeName"] = json!("node-a");
    api.extra.lock().unwrap()[0]["metadata"]["deletionTimestamp"] = json!("2026-10-30T12:00:00Z");
    let workers = discovery.discover(&spec).await.unwrap();
    assert_eq!(workers.len(), 1);
    assert_eq!(workers[0].host_id, request.host);
}
#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn recreated_pod_with_same_name_gets_new_identity_without_inherited_ownership(pg: PgPool) {
    let (store, pool, request, api, router) = setup(pg.clone()).await;
    let provision = enqueue_claim(&store, pool, "new", &request).await;
    let receipt = router.apply(&store, &provision).await.unwrap();
    let old_uid = receipt["pod_uid"].as_str().unwrap().to_owned();
    let name = receipt["name"].as_str().unwrap().to_owned();
    let retire = Request {
        action: Action::Retire,
        ..request.clone()
    };
    let retire = enqueue_claim(&store, pool, "remove", &retire).await;
    fresh(&store, &request).await;
    router.apply(&store, &retire).await.unwrap();
    // Named delete completes physically, then a Pod with the same name reappears.
    let mut recreated = api.pod.lock().unwrap().take().unwrap();
    let new_uid = Uuid::now_v7().to_string();
    recreated["metadata"]["uid"] = json!(new_uid);
    recreated["metadata"]
        .as_object_mut()
        .unwrap()
        .remove("deletionTimestamp");
    *api.pod.lock().unwrap() = Some(recreated);
    let discovery = KubernetesCapacityProvider::new(api.clone());
    let workers = discovery
        .discover(&store.host_pool(pool).await.unwrap().unwrap())
        .await
        .unwrap();
    assert_eq!(workers.len(), 1);
    let provider_ref = workers[0].provider_ref.clone().unwrap();
    assert!(provider_ref.ends_with(&format!("/{name}/{new_uid}")));
    assert!(!provider_ref.contains(&old_uid));
    // The registry never treats the new incarnation as the retired host.
    let registered = store.registered_hosts(Some(pool)).await.unwrap();
    assert!(
        !reconcile(&workers, &registered)
            .matched
            .contains(&request.host)
    );
    // The old identity stays fenced: its stale receipts and re-provision fail.
    assert!(router.apply(&store, &retire).await.is_err());
    assert!(router.apply(&store, &provision).await.is_err());
    assert!(store.finish_host_drain(request.host, 1).await.is_err());
    assert!(
        store
            .enqueue_capacity(
                pool,
                "reuse",
                &Request {
                    epoch: 2,
                    ..request.clone()
                }
            )
            .await
            .is_err()
    );
    // Nothing owned by the old incarnation transfers to the new one.
    for table in ["allocations", "warm_pool_members"] {
        let n: i64 = sqlx::query_scalar(&format!("SELECT count(*) FROM {table} WHERE host_id=$1"))
            .bind(request.host.uuid())
            .fetch_one(&pg)
            .await
            .unwrap();
        assert_eq!(n, 0, "{table}");
    }
    assert_eq!(
        api.calls
            .lock()
            .unwrap()
            .iter()
            .filter(|s| *s == "POST")
            .count(),
        1
    );
}
