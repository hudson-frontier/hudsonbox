use super::*;
use sandbox_protocol::{
    files as fm,
    guest::FileState,
    supervisor::{CommandRequest, FileRequest, FileWriteRequest},
};
use sha2::{Digest, Sha256};

pub(super) fn upload(owner: &Ownership, path: &str, bytes: &[u8]) -> FileRequest {
    let id = OperationId::generate();
    let mut owner = owner.clone();
    owner.operation_id = id.to_string();
    let descriptor = fm::Upload {
        operation_id: id,
        path: path.into(),
        size: bytes.len() as u64,
        sha256: Sha256::digest(bytes).into(),
        mode: 0o644,
    };
    FileRequest {
        ownership: Some(owner),
        upload: Some((&descriptor).into()),
    }
}

#[tokio::test]
#[ignore = "requires root, HUDSON_GUARDIAN_TEST_VM=1 and native KVM artifacts"]
async fn real_supervisor_file_fencing_publication_capacity_and_restart() {
    let mut f = Fixture::new().await;
    let mut c = f.client().await;
    let create = f.request();
    let owner = create.ownership.as_ref().unwrap().clone();
    let _ = c.create(create).await;
    f.ready(&mut c, &owner).await;
    let guest = f.manifest(&owner).guest_client().unwrap();
    let bytes: Vec<u8> = (0..65539).map(|n| (n % 251) as u8).collect();
    let req = upload(&owner, "private-transfer-path.bin", &bytes);
    // A CA-valid read-only service cannot call any mutation or ownership-fencing RPC.
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
        reader.begin_file(req.clone()).await.unwrap_err().code(),
        Code::PermissionDenied
    );
    assert_eq!(
        reader.inspect_file(req.clone()).await.unwrap_err().code(),
        Code::PermissionDenied
    );
    assert_eq!(
        reader.commit_file(req.clone()).await.unwrap_err().code(),
        Code::PermissionDenied
    );
    assert_eq!(
        reader.abort_file(req.clone()).await.unwrap_err().code(),
        Code::PermissionDenied
    );
    assert_eq!(
        reader
            .write_file(FileWriteRequest {
                request: Some(req.clone()),
                offset: 0,
                data: vec![0]
            })
            .await
            .unwrap_err()
            .code(),
        Code::PermissionDenied
    );
    let begin = c.begin_file(req.clone()).await.unwrap().into_inner();
    assert!(!begin.simulated && !begin.not_started);
    assert_eq!(begin.state, FileState::Staging as i32);
    assert_eq!(
        begin.context.as_ref().unwrap().boot_id,
        guest.context().boot_id
    );
    for (i, chunk) in bytes.chunks(fm::MAX_CHUNK_BYTES).enumerate() {
        let write = FileWriteRequest {
            request: Some(req.clone()),
            offset: (i * fm::MAX_CHUNK_BYTES) as u64,
            data: chunk.to_vec(),
        };
        let stored = c
            .write_file(write.clone())
            .await
            .unwrap()
            .into_inner()
            .stored
            .unwrap();
        assert_eq!(stored, write.offset + chunk.len() as u64);
        assert_eq!(
            c.write_file(write).await.unwrap().into_inner().stored,
            Some(stored)
        );
    }
    let conflict = c
        .write_file(FileWriteRequest {
            request: Some(req.clone()),
            offset: 0,
            data: vec![255],
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(conflict.state, 0);
    assert!(conflict.stored.is_none());
    assert_eq!(
        c.inspect_file(req.clone())
            .await
            .unwrap()
            .into_inner()
            .state,
        FileState::Staging as i32
    );
    // Drop the commit response, reconnect, and reconcile the original operation.
    c.commit_file(req.clone()).await.unwrap();
    drop(c);
    let mut c = f.client().await;
    assert_eq!(
        c.commit_file(req.clone()).await.unwrap().into_inner().state,
        FileState::Committed as i32
    );
    let captured = guest
        .capture_file("private-transfer-path.bin")
        .await
        .unwrap();
    assert_eq!(captured.sha256, <[u8; 32]>::from(Sha256::digest(&bytes)));
    let mut downloaded = Vec::new();
    while downloaded.len() < bytes.len() {
        downloaded.extend(
            guest
                .read_file(
                    &captured,
                    downloaded.len() as u64,
                    fm::MAX_CHUNK_BYTES as u32,
                )
                .await
                .unwrap()
                .data,
        );
    }
    assert_eq!(downloaded, bytes);
    guest.release_file(&captured).await.unwrap();
    let command = sandbox_protocol::guest_model::Execute {
        operation_id: OperationId::generate(),
        argv: vec![
            "/bin/busybox".into(),
            "sh".into(),
            "-c".into(),
            "printf changed > /workspace/private-transfer-path.bin".into(),
        ],
        env: BTreeMap::new(),
        cwd: "/".into(),
        deadline_unix_ms: guardian::wall_ms() + 10000,
        output_limit: 1024,
    };
    guest.execute(&command).await.unwrap();
    let until = Instant::now() + Duration::from_secs(10);
    loop {
        let receipt = guest.inspect(command.operation_id).await.unwrap();
        if receipt.cleanup_confirmed {
            assert_eq!(receipt.exit_code, Some(0));
            break;
        }
        assert!(Instant::now() < until);
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    c.begin_file(req.clone()).await.unwrap();
    c.commit_file(req.clone()).await.unwrap();
    let captured = guest
        .capture_file("private-transfer-path.bin")
        .await
        .unwrap();
    assert_eq!(
        guest.read_file(&captured, 0, 32).await.unwrap().data,
        b"changed"
    );
    guest.release_file(&captured).await.unwrap();
    let mut changed = req.clone();
    changed.upload.as_mut().unwrap().mode = 0o755;
    assert_eq!(
        c.begin_file(changed).await.unwrap_err().code(),
        Code::AlreadyExists
    );
    let mut revised = req.clone();
    revised.ownership.as_mut().unwrap().claim_revision = 2;
    c.inspect_file(revised).await.unwrap();
    assert_eq!(
        c.inspect_file(req.clone()).await.unwrap_err().code(),
        Code::FailedPrecondition
    );
    let mut wrong = req.clone();
    wrong.ownership.as_mut().unwrap().project_id =
        sandbox_protocol::ProjectId::generate().to_string();
    assert_eq!(
        c.inspect_file(wrong).await.unwrap_err().code(),
        Code::FailedPrecondition
    );
    let mut command_owner = owner.clone();
    command_owner.operation_id = req.ownership.as_ref().unwrap().operation_id.clone();
    let mut reused_command = command.clone();
    reused_command.operation_id = command_owner.operation_id.parse().unwrap();
    assert_eq!(
        c.execute_command(CommandRequest {
            ownership: Some(command_owner),
            command: Some((&reused_command).into())
        })
        .await
        .unwrap_err()
        .code(),
        Code::AlreadyExists
    );
    let aborted = upload(&owner, "aborted.bin", b"x");
    c.begin_file(aborted.clone()).await.unwrap();
    assert_eq!(
        c.abort_file(aborted.clone())
            .await
            .unwrap()
            .into_inner()
            .state,
        FileState::Aborted as i32
    );
    assert_eq!(
        c.begin_file(aborted).await.unwrap().into_inner().state,
        FileState::Aborted as i32
    );
    let staging = upload(&owner, "unfinished.bin", b"x");
    c.begin_file(staging.clone()).await.unwrap();
    let fenced = upload(&owner, "never-started.bin", b"x");
    assert!(
        c.inspect_file(fenced.clone())
            .await
            .unwrap()
            .into_inner()
            .not_started
    );
    let mut late = fenced;
    late.ownership.as_mut().unwrap().claim_revision = 2;
    assert!(c.begin_file(late).await.unwrap().into_inner().not_started);
    for i in 4..16 {
        assert!(
            c.inspect_file(upload(&owner, &format!("fence-{i}"), b""))
                .await
                .unwrap()
                .into_inner()
                .not_started
        );
    }
    assert_eq!(
        c.begin_file(upload(&owner, "overflow", b""))
            .await
            .unwrap_err()
            .code(),
        Code::ResourceExhausted
    );
    let retained: serde_json::Value =
        guardian::read_json(&f.config.state_root.join("host.json")).unwrap();
    let files = &retained["records"][&owner.allocation_id]["files"];
    assert_eq!(files.as_object().unwrap().len(), 16);
    let encoded = serde_json::to_string(files).unwrap();
    assert!(!encoded.contains("private-transfer-path.bin"));
    assert_eq!(
        files[&req.ownership.as_ref().unwrap().operation_id]["commit_requested"],
        true
    );
    let mut destroy = owner.clone();
    destroy.operation_id = OperationId::generate().to_string();
    let _ = c
        .stop(StopRequest {
            ownership: Some(destroy),
        })
        .await;
    f.released(&mut c, &owner).await;
    let after_stop = c.inspect_file(staging.clone()).await.unwrap().into_inner();
    assert_eq!(after_stop.state, 0);
    assert!(!after_stop.not_started);
    f.restart().await;
    let mut c = f.client().await;
    assert_eq!(
        c.begin_file(staging.clone()).await.unwrap_err().code(),
        Code::FailedPrecondition
    );
    let mut changed_epoch = staging;
    changed_epoch.ownership.as_mut().unwrap().supervisor_epoch = f.config.epoch;
    assert_eq!(
        c.begin_file(changed_epoch).await.unwrap_err().code(),
        Code::FailedPrecondition
    );
    let recovered: serde_json::Value =
        guardian::read_json(&f.config.state_root.join("host.json")).unwrap();
    assert_eq!(&recovered["records"][&owner.allocation_id]["files"], files);
    println!(
        "real_supervisor_file_observation={}",
        serde_json::json!({"simulated":false,"bytes":bytes.len(),"controller_only":true,"retry_preserved_later_changes":true,"full_file_capacity_destroy":true,"old_epoch_rejected":true,"retained_files_preserved":true,"cleanup_confirmed":true})
    );
}

#[tokio::test]
#[ignore = "requires root and HUDSON_GUARDIAN_TEST_VM=1 fixture artifacts"]
async fn real_file_journal_upgrade_and_corruption_fail_closed() {
    let mut f = Fixture::with_agent(false).await;
    let mut c = f.client().await;
    let owner = f.request().ownership.unwrap();
    // A lifecycle-only retained allocation has the old JSON shape, without files.
    c.inspect(InspectRequest {
        ownership: Some(owner.clone()),
    })
    .await
    .unwrap();
    let old: serde_json::Value =
        guardian::read_json(&f.config.state_root.join("host.json")).unwrap();
    assert!(old["records"][&owner.allocation_id].get("files").is_none());
    f.restart().await;
    let mut c = f.client().await;
    let owner = f.request().ownership.unwrap();
    let req = upload(&owner, "unstarted", b"");
    assert!(
        c.inspect_file(req.clone())
            .await
            .unwrap()
            .into_inner()
            .not_started
    );
    f.child.kill().unwrap();
    f.child.wait().unwrap();
    let path = f.config.state_root.join("host.json");
    let mut retained: serde_json::Value = guardian::read_json(&path).unwrap();
    retained["records"][&owner.allocation_id]["files"][&req.ownership.unwrap().operation_id]["commit_requested"] =
        true.into();
    fs::write(&path, serde_json::to_vec(&retained).unwrap()).unwrap();
    f.config.epoch += 1;
    assert!(sandbox_supervisor::host::Host::open(f.config.clone()).is_err());
}

#[tokio::test]
#[ignore = "requires root and HUDSON_GUARDIAN_TEST_VM=1 fixture artifacts"]
async fn real_concurrent_file_admission_cannot_exceed_host_history_limit() {
    use sandbox_protocol::supervisor::supervisor_server::Supervisor;
    let mut f = Fixture::with_agent(false).await;
    let mut c = f.client().await;
    let owner = f.request().ownership.unwrap();
    let req = upload(&owner, "fence", b"");
    c.inspect_file(req).await.unwrap();
    f.child.kill().unwrap();
    f.child.wait().unwrap();
    let path = f.config.state_root.join("host.json");
    let mut journal: serde_json::Value = guardian::read_json(&path).unwrap();
    let template = journal["records"][&owner.allocation_id].clone();
    let file = template["files"]
        .as_object()
        .unwrap()
        .values()
        .next()
        .unwrap()
        .clone();
    let mut records = serde_json::Map::new();
    for n in 0..64 {
        let allocation = AllocationId::generate().to_string();
        let mut record = template.clone();
        record["owner"]["allocation_id"] = allocation.clone().into();
        let mut files = serde_json::Map::new();
        let mut revisions = serde_json::Map::new();
        for _ in 0..if n == 63 { 15 } else { 16 } {
            let operation = OperationId::generate().to_string();
            revisions.insert(operation.clone(), 1.into());
            files.insert(operation, file.clone());
        }
        record["files"] = files.into();
        record["revisions"] = revisions.into();
        records.insert(allocation, record);
    }
    journal["records"] = records.into();
    fs::write(&path, serde_json::to_vec(&journal).unwrap()).unwrap();
    f.config.epoch += 1;
    let host = sandbox_supervisor::host::Host::open(f.config.clone()).unwrap();
    let a = upload(&f.request().ownership.unwrap(), "last-a", b"");
    let b = upload(&f.request().ownership.unwrap(), "last-b", b"");
    let (a, b) = tokio::join!(
        host.inspect_file(tonic::Request::new(a)),
        host.inspect_file(tonic::Request::new(b))
    );
    assert_eq!(usize::from(a.is_ok()) + usize::from(b.is_ok()), 1);
    assert_eq!(
        a.err().or_else(|| b.err()).unwrap().code(),
        Code::ResourceExhausted
    );
    let journal: serde_json::Value = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
    assert_eq!(
        journal["records"]
            .as_object()
            .unwrap()
            .values()
            .map(|r| r["files"].as_object().map_or(0, |f| f.len()))
            .sum::<usize>(),
        1024
    );
    drop(host);
}
