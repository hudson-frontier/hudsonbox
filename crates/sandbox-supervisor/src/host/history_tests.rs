#![allow(clippy::unwrap_used)]
use super::*;
use sandbox_protocol::{
    command::CommandRecord, guest_model::Context, supervisor_files::FileRecord,
};
fn fixture() -> (Record, Barrier) {
    let context = Context {
        allocation_id: AllocationId::generate(),
        generation: 1,
        boot_id: "boot".into(),
    };
    let owner = Ownership {
        host_id: HostId::generate().to_string(),
        project_id: ProjectId::generate().to_string(),
        sandbox_id: SandboxId::generate().to_string(),
        allocation_id: context.allocation_id.to_string(),
        operation_id: OperationId::generate().to_string(),
        generation: 1,
        supervisor_epoch: 1,
        claim_revision: 1,
        claim_expires_unix_ms: 1,
    };
    let record = Record {
        retirement: None,
        metadata_retirement: None,
        owner,
        revisions: BTreeMap::new(),
        create: None,
        manifest: None,
        dispatched: false,
        stopped: false,
        released: false,
        commands: BTreeMap::new(),
        files: BTreeMap::new(),
        archives: BTreeMap::new(),
        command_history: None,
        released_commands: None,
        released_files: None,
        file_history: None,
        lease_revision: 0,
        lease_request: None,
        gate: Arc::new(Mutex::new(())),
        file_io: journal::file_io(),
        readers: Arc::default(),
    };
    (
        record,
        Barrier {
            version: 1,
            context,
            domain: Domain::Commands,
            through: id(100),
        },
    )
}
fn id(n: u64) -> OperationId {
    format!("op_019a9fad-3000-7000-8000-{n:012x}")
        .parse()
        .unwrap()
}
#[test]
fn installed_floor_retains_capacity_until_completion_and_survives_roundtrip() {
    let (mut record, barrier) = fixture();
    for n in 1..=32 {
        let key = id(n).to_string();
        record
            .commands
            .insert(key.clone(), CommandRecord::fenced([n as u8; 32]));
        record.revisions.insert(key, 1);
    }
    let lifecycle = record.owner.operation_id.clone();
    record.revisions.insert(lifecycle.clone(), 1);
    let first = prepare(&mut record, barrier.clone(), 1).unwrap();
    assert_eq!(record.commands.len(), 32);
    assert!(check(&record, Domain::Commands, id(1)).is_err());
    assert!(check(&record, Domain::Commands, id(101)).is_ok());
    let mut recovered: Record =
        serde_json::from_slice(&serde_json::to_vec(&record).unwrap()).unwrap();
    let retry = prepare(&mut recovered, barrier.clone(), 2).unwrap();
    assert_eq!(recovered.commands.len(), 32);
    assert!(complete(&mut recovered, &first).is_err());
    complete(&mut recovered, &retry).unwrap();
    assert!(recovered.commands.is_empty());
    assert_eq!(recovered.revisions.len(), 1);
    assert!(recovered.revisions.contains_key(&lifecycle));
    assert!(prepare(&mut recovered, barrier.clone(), 1).is_err());
    let mut lower = barrier.clone();
    lower.through = id(99);
    assert!(prepare(&mut recovered, lower.clone(), 2).is_err());
    let current = prepare(&mut recovered, lower.clone(), 3).unwrap();
    assert!(prepare(&mut recovered, lower, 3).unwrap().completed);
    assert!(prepare(&mut recovered, barrier.clone(), 3).is_err());
    assert!(current.completed);
    assert_eq!(current.barrier, barrier);
    assert!(check(&recovered, Domain::Commands, id(100)).is_err());
}
#[test]
fn incomplete_prefix_blocks_advancement_unknown_outcomes_and_other_boots() {
    let (mut record, barrier) = fixture();
    let pending = CommandRecord {
        digest: [0; 32],
        context: Some(barrier.context.clone()),
        deadline_unix_ms: 10,
        output_limit: 1,
        not_started: false,
        receipt: None,
    };
    record.commands.insert(id(1).to_string(), pending);
    assert!(prepare(&mut record, barrier.clone(), 1).is_err());
    record.commands.clear();
    let first = prepare(&mut record, barrier.clone(), 1).unwrap();
    let mut next = barrier.clone();
    next.through = id(101);
    assert!(prepare(&mut record, next.clone(), 2).is_err());
    complete(&mut record, &first).unwrap();
    next.context.boot_id = "other".into();
    assert!(prepare(&mut record, next.clone(), 2).is_err());
    next.context = barrier.context.clone();
    assert!(!prepare(&mut record, next, 2).unwrap().completed);
}
#[test]
fn file_history_releases_only_terminal_domain_records_and_preserves_newer_work() {
    let (mut record, mut barrier) = fixture();
    barrier.domain = Domain::Files;
    for n in 1..=16 {
        let key = id(n).to_string();
        record
            .files
            .insert(key.clone(), FileRecord::fenced([0; 32]));
        record.revisions.insert(key, 1);
    }
    let pending = FileRecord {
        digest: [0; 32],
        context: Some(barrier.context.clone()),
        size: 1,
        not_started: false,
        commit_requested: false,
        abort_requested: false,
        state: 1,
    };
    record.files.insert(id(101).to_string(), pending.clone());
    record.revisions.insert(id(101).to_string(), 1);
    record
        .commands
        .insert(id(50).to_string(), CommandRecord::fenced([0; 32]));
    record.revisions.insert(id(50).to_string(), 1);
    for state in [0, 1, 2, 4] {
        let mut unresolved = pending.clone();
        unresolved.state = state;
        unresolved.commit_requested = true;
        record.files.insert(id(1).to_string(), unresolved);
        assert!(prepare(&mut record, barrier.clone(), 1).is_err());
    }
    record
        .files
        .insert(id(1).to_string(), FileRecord::fenced([0; 32]));
    let r = prepare(&mut record, barrier, 1).unwrap();
    complete(&mut record, &r).unwrap();
    assert_eq!(record.files.len(), 1);
    assert_eq!(record.commands.len(), 1);
    assert_eq!(record.revisions.len(), 2);
    assert!(check(&record, Domain::Files, id(1)).is_err());
    assert!(check(&record, Domain::Commands, id(50)).is_ok());
}

#[test]
fn failed_durable_intent_write_poison_fences_without_discarding_receipts() {
    let (mut record, barrier) = fixture();
    let key = id(1).to_string();
    record
        .commands
        .insert(key.clone(), CommandRecord::fenced([0; 32]));
    record.revisions.insert(key.clone(), 1);
    let temp = tempfile::tempdir().unwrap();
    let artifact = Artifact {
        path: temp.path().join("unused"),
        sha256: "0".repeat(64),
    };
    let host: HostId = record.owner.host_id.parse().unwrap();
    let config = Config {
        capabilities: None,
        host,
        epoch: 1,
        state_root: temp.path().into(),
        cgroup_parent: temp.path().join("unused"),
        launch_permits_required: false,
        guardian_binary: temp.path().join("unused"),
        firecracker: artifact.clone(),
        jailer: artifact,
        images: BTreeMap::new(),
        jail_uid: 65534,
        jail_gid: 65534,
        capacity: Capacity {
            vcpu: 1,
            memory_mib: 128,
            disk_mib: 64,
        },
    };
    let allocation = record.owner.allocation_id.clone();
    let mut journal = Journal {
        launch_authority: None,
        version: 1,
        host,
        epoch: 1,
        records: BTreeMap::from([(allocation.clone(), record.clone())]),
        warm: BTreeMap::new(),
        warm_refusals: Default::default(),
        poisoned: false,
    };
    journal::save(&config, &mut journal).unwrap();
    let original = fs::read(temp.path().join("host.json")).unwrap();
    fs::rename(
        temp.path().join("host.json"),
        temp.path().join("before.json"),
    )
    .unwrap();
    fs::create_dir(temp.path().join("host.json")).unwrap();
    prepare(&mut record, barrier, 1).unwrap();
    journal.records.insert(allocation.clone(), record);
    assert!(journal::save(&config, &mut journal).is_err());
    assert!(journal.poisoned);
    assert!(journal.records[&allocation].commands.contains_key(&key));
    assert!(
        !journal.records[&allocation]
            .command_history
            .as_ref()
            .unwrap()
            .completed
    );
    assert!(journal::save(&config, &mut journal).is_err());
    assert_eq!(fs::read(temp.path().join("before.json")).unwrap(), original);
    let before: Journal = serde_json::from_slice(&original).unwrap();
    assert!(before.records[&allocation].command_history.is_none());
    assert!(before.records[&allocation].commands.contains_key(&key));
}
