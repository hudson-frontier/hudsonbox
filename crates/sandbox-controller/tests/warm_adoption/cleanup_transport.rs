//! Test-only reply faults after the fake has applied a real authenticated RPC.
use sandbox_fake_host::FakeHost;
use sandbox_protocol::supervisor::{self as rpc, supervisor_server::Supervisor};
use std::{collections::VecDeque, sync::Arc};
use tokio::sync::{Mutex, Notify};
use tonic::{Request, Response, Status};

#[derive(Debug)]
pub(super) struct ReplyGate {
    applied: Notify,
    release: Notify,
    drop_reply: bool,
}

impl ReplyGate {
    pub(super) fn new(drop_reply: bool) -> Arc<Self> {
        Arc::new(Self {
            applied: Notify::new(),
            release: Notify::new(),
            drop_reply,
        })
    }

    pub(super) async fn applied(&self) {
        tokio::time::timeout(std::time::Duration::from_secs(10), self.applied.notified())
            .await
            .expect("cleanup must reach the authenticated endpoint and apply");
    }

    pub(super) fn release(&self) {
        self.release.notify_one();
    }
}

#[derive(Clone, Debug)]
pub(super) struct CleanupTransport {
    fake: FakeHost,
    gates: Arc<Mutex<VecDeque<Arc<ReplyGate>>>>,
    pub(super) requests: Arc<Mutex<Vec<Vec<u8>>>>,
}

impl CleanupTransport {
    pub(super) fn new(fake: &FakeHost, gates: Vec<Arc<ReplyGate>>) -> Self {
        Self {
            fake: fake.clone(),
            gates: Arc::new(Mutex::new(gates.into())),
            requests: Arc::default(),
        }
    }
}

// Delegate the unrelated RPCs without inventing a second fake implementation.
macro_rules! supervisor {
    ($($method:ident($request:ident) -> $reply:ident),* $(,)?) => {
        #[tonic::async_trait]
        impl Supervisor for CleanupTransport {
            $(async fn $method(&self, request: Request<rpc::$request>)
                -> Result<Response<rpc::$reply>, Status> {
                self.fake.$method(request).await
            })*

            async fn retire_warm_member(&self, request: Request<rpc::WarmRetireRequest>)
                -> Result<Response<rpc::WarmObservation>, Status> {
                self.requests.lock().await.push(request.get_ref().intent_json.clone());
                let reply = self.fake.retire_warm_member(request).await?;
                let gate = self.gates.lock().await.pop_front();
                if let Some(gate) = gate {
                    // Notify only AFTER destruction and receipt construction.
                    gate.applied.notify_one();
                    gate.release.notified().await;
                    if gate.drop_reply {
                        return Err(Status::unavailable("test: cleanup reply lost after apply"));
                    }
                }
                Ok(reply)
            }
        }
    };
}

supervisor! {
    forget_allocation(AllocationForgetRequest) -> AllocationForgetObservation,
    retire_allocation_metadata(AllocationMetadataRequest) -> AllocationMetadataObservation,
    fence_allocation(AllocationFenceRequest) -> AllocationFenceObservation,
    allocation_authority(AllocationAuthorityRequest) -> AllocationAuthorityObservation,
    retire_released_history(ReleasedHistoryRequest) -> ReleasedHistoryObservation,
    history_binding(LeaseInspection) -> HistoryBindingObservation,
    retire_history(HistoryRequest) -> HistoryObservation,
    begin_file(FileRequest) -> FileObservation,
    write_file(FileWriteRequest) -> FileObservation,
    inspect_file(FileRequest) -> FileObservation,
    commit_file(FileRequest) -> FileObservation,
    abort_file(FileRequest) -> FileObservation,
    prepare_output(OutputRequest) -> OutputObservation,
    archive_output(OutputRequest) -> OutputObservation,
    execute_command(CommandRequest) -> CommandObservation,
    inspect_command(CommandInspection) -> CommandObservation,
    cancel_command(CommandInspection) -> CommandObservation,
    health(HealthRequest) -> HostInfo,
    create(CreateRequest) -> Observation,
    inspect(InspectRequest) -> Observation,
    stop(StopRequest) -> Observation,
    renew_lease(LeaseRequest) -> LeaseObservation,
    inspect_lease(LeaseInspection) -> LeaseObservation,
    reconcile_previous_allocation(PreviousAllocationRequest) -> PreviousAllocationObservation,
    prepare_warm_member(WarmPrepareRequest) -> WarmObservation,
    adopt_warm_member(WarmAdoptRequest) -> WarmObservation,
    inspect_warm_handoff(WarmAdoptRequest) -> WarmObservation,
}

struct ServerTask(tokio::task::JoinHandle<()>);
impl Drop for ServerTask {
    fn drop(&mut self) {
        self.0.abort();
    }
}

struct EpochCleanup {
    fixture: super::Fixture,
    current: FakeHost,
    old_transport: CleanupTransport,
    current_transport: CleanupTransport,
    _servers: Vec<ServerTask>,
}

impl EpochCleanup {
    async fn new(pool: &sqlx::PgPool, gates: Vec<Arc<ReplyGate>>, rejected_create: bool) -> Self {
        use super::*;
        let f = Fixture::new(pool).await;
        let old_transport = CleanupTransport::new(&f.fake, vec![]);
        let (endpoint, pin, old_task) = f.serve_supervisor(old_transport.clone()).await;
        register(&f, &endpoint, &pin).await;
        warm(&f, 1).await;
        assert_eq!(
            connect(&f).await.warm_upkeep(1).await.unwrap().steps,
            [Step::Prepared]
        );
        if rejected_create {
            // A lost pre-dispatch worker is inspected and rejected. Previous-
            // epoch cleanup must not re-enable this old allocation's cold path.
            f.admit().await;
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
            f.reclaim_now().await;
            assert_eq!(connect(&f).await.tick().await.unwrap(), Tick::Deferred);
            assert_eq!(phase(pool).await.1, "warm_handoff_rejected");
        }
        f.store.drain_host(f.config.host, 1).await.unwrap();
        assert_eq!(
            f.store
                .advance_host_epoch(f.config.host, 1)
                .await
                .unwrap()
                .supervisor_epoch,
            2
        );
        let current = next_fake(&f, 2);
        current.retain_warm_members_from(&f.fake).await.unwrap();
        let current_transport = CleanupTransport::new(&current, gates);
        let (endpoint, pin, current_task) = f.serve_supervisor(current_transport.clone()).await;
        register(&f, &endpoint, &pin).await;
        Self {
            fixture: f,
            current,
            old_transport,
            current_transport,
            _servers: vec![ServerTask(old_task), ServerTask(current_task)],
        }
    }

    async fn assert_held(&self, epoch: i64) {
        let f = &self.fixture;
        let reservation: (String, bool, i32, i64, i64) = sqlx::query_as(
            "SELECT state,cleanup_evidence IS NULL,vcpu,memory_mib,disk_mib FROM warm_pool_members",
        )
        .fetch_one(f.store.pool())
        .await
        .unwrap();
        assert_eq!(reservation, ("retiring".into(), true, 2, 2048, 8192));
        assert_eq!(
            f.store
                .host_maintenance(f.config.host, epoch)
                .await
                .unwrap()
                .unretired_warm_members,
            1
        );
    }

    async fn assert_no_old_cleanup_or_cold_create(&self) {
        assert!(self.old_transport.requests.lock().await.is_empty());
        assert_eq!(self.fixture.fake.total_starts().await, 0);
        assert_eq!(self.current.total_starts().await, 0);
        assert_eq!(self.current.warm_adoptions().await, 0);
    }
}

async fn register(f: &super::Fixture, endpoint: &str, pin: &str) {
    use sandbox_protocol::Id;
    sqlx::query("UPDATE hosts SET endpoint=$2,transport_server_name=$3,transport_certificate_sha256=$4 WHERE id=$1")
        .bind(f.config.host.uuid()).bind(endpoint)
        .bind(sandbox_supervisor::transport::host_server_name(f.config.host)).bind(pin)
        .execute(f.store.pool()).await.unwrap();
}

async fn retire_claim(pool: &sqlx::PgPool) -> (uuid::Uuid, i64, String) {
    sqlx::query_as(
        "SELECT id,claim_revision,status FROM capacity_operations WHERE kind='warm_retire'",
    )
    .fetch_one(pool)
    .await
    .unwrap()
}

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn controller_routes_previous_cleanup_after_lost_reply_and_restart(pool: sqlx::PgPool) {
    use super::*;
    let lost = ReplyGate::new(true);
    let retry = ReplyGate::new(false);
    let scenario = EpochCleanup::new(&pool, vec![lost.clone(), retry.clone()], true).await;
    let f = &scenario.fixture;
    let controller = connect(f).await;
    let first = tokio::spawn(async move { controller.warm_upkeep(1).await });
    lost.applied().await;
    // The VM is gone, but destruction alone cannot refund the DB reservation.
    assert_eq!(scenario.current.warm_holding().await, 0);
    scenario.assert_held(2).await;
    let first_claim = retire_claim(&pool).await;
    assert_eq!(first_claim.2, "running");
    lost.release();
    assert_eq!(first.await.unwrap().unwrap().steps, [Step::Retried]);
    scenario.assert_held(2).await;

    // A new controller authenticates epoch 2 and reclaims the same durable
    // operation. Hold its exact receipt too, so capacity cannot release early.
    let controller = connect(f).await;
    let second = tokio::spawn(async move { controller.warm_upkeep(1).await });
    retry.applied().await;
    scenario.assert_held(2).await;
    let second_claim = retire_claim(&pool).await;
    assert_eq!(second_claim.0, first_claim.0);
    assert_eq!(second_claim.1, first_claim.1 + 1);
    assert_eq!(second_claim.2, "running");
    let requests = scenario.current_transport.requests.lock().await.clone();
    assert_eq!(requests.len(), 2);
    let first_request: warm_pool::PreviousRetireIntent = warm_pool::decode(&requests[0]).unwrap();
    let second_request: warm_pool::PreviousRetireIntent = warm_pool::decode(&requests[1]).unwrap();
    assert_eq!(first_request.reporting_epoch, 2);
    assert_eq!(first_request.original.supervisor_epoch, 1);
    let (member, vm_ref): (Uuid, Option<String>) =
        sqlx::query_as("SELECT id,vm_ref FROM warm_pool_members")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(first_request.original.member_id, member);
    assert_eq!(first_request.original.vm_ref, vm_ref);
    assert_eq!(first_request.original.host_id, f.config.host);
    assert_eq!(first_request.original.operation_id, first_claim.0);
    assert_eq!(first_request.original.operation_revision, first_claim.1);
    let mut expected = first_request;
    expected.original.operation_revision = second_claim.1;
    assert_eq!(second_request, expected);
    retry.release();
    assert_eq!(second.await.unwrap().unwrap().steps, [Step::Cleaned]);
    assert_eq!(member_states(&pool).await, ["retired"]);
    let evidence: Value = sqlx::query_scalar("SELECT cleanup_evidence FROM warm_pool_members")
        .fetch_one(&pool)
        .await
        .unwrap();
    let receipt: warm_pool::PreviousCleanupReceipt = serde_json::from_value(evidence).unwrap();
    receipt.verify(&second_request).unwrap();
    assert_eq!(
        retire_claim(&pool).await,
        (second_claim.0, second_claim.1, "succeeded".into())
    );
    assert_eq!(
        f.store
            .host_maintenance(f.config.host, 2)
            .await
            .unwrap()
            .unretired_warm_members,
        0
    );
    // Ordinary same-epoch cleanup would reset this to reserved and enable a
    // cold boot. Previous-epoch cleanup leaves owner-loss recovery in charge.
    assert_eq!(phase(&pool).await.1, "warm_handoff_rejected");
    f.reclaim_now().await;
    connect(f).await.tick().await.unwrap();
    scenario.assert_no_old_cleanup_or_cold_create().await;
}

#[sqlx::test(migrator = "sandbox_store::MIGRATOR")]
async fn controller_refuses_epoch_two_cleanup_when_registry_advances_before_commit(
    pool: sqlx::PgPool,
) {
    use super::*;
    let delayed = ReplyGate::new(false);
    let scenario = EpochCleanup::new(&pool, vec![delayed.clone()], false).await;
    let f = &scenario.fixture;
    let controller = connect(f).await;
    let upkeep = tokio::spawn(async move { controller.warm_upkeep(1).await });
    delayed.applied().await;
    assert_eq!(scenario.current.warm_holding().await, 0);
    scenario.assert_held(2).await;
    // Destruction is real in the fake, but epoch 2 loses reporting authority
    // while its valid reply is in flight to the controller.
    assert_eq!(
        f.store
            .advance_host_epoch(f.config.host, 2)
            .await
            .unwrap()
            .supervisor_epoch,
        3
    );
    delayed.release();
    assert!(matches!(
        upkeep.await.unwrap(),
        Err(sandbox_store::warm_pool::Error::Conflict)
    ));
    scenario.assert_held(3).await;
    assert_eq!(retire_claim(&pool).await.2, "running");
    assert_eq!(scenario.current_transport.requests.lock().await.len(), 1);
    scenario.assert_no_old_cleanup_or_cold_create().await;
}
