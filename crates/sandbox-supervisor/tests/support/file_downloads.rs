use super::*;
use sandbox_protocol::{
    file_downloads::{self as model, ReadScope},
    supervisor::{FileCaptureRequest, FileDownloadRequest, FileReleaseRequest, FileWriteRequest},
};
use sha2::{Digest, Sha256};

#[tokio::test]
#[ignore = "requires root, HUDSON_GUARDIAN_TEST_VM=1 and native KVM artifacts"]
async fn real_file_reader_auth_capture_reconnect_replacement_destroy_and_restart() {
    let mut f = Fixture::new().await;
    let mut controller = f.client().await;
    let create = f.request();
    let owner = create.ownership.as_ref().unwrap().clone();
    let _ = controller.create(create).await;
    f.ready(&mut controller, &owner).await;
    let bytes: Vec<u8> = (0..65539).map(|n| (n % 251) as u8).collect();
    let upload = super::supervisor_files::upload(&owner, "download.bin", &bytes);
    controller.begin_file(upload.clone()).await.unwrap();
    for (i, chunk) in bytes.chunks(32768).enumerate() {
        controller
            .write_file(FileWriteRequest {
                request: Some(upload.clone()),
                offset: (i * 32768) as u64,
                data: chunk.to_vec(),
            })
            .await
            .unwrap();
    }
    controller.commit_file(upload).await.unwrap();
    let scope = ReadScope {
        version: 1,
        host_id: f.config.host,
        host_epoch: f.config.epoch,
        project_id: owner.project_id.parse().unwrap(),
        sandbox_id: owner.sandbox_id.parse().unwrap(),
        allocation_id: owner.allocation_id.parse().unwrap(),
        generation: owner.generation,
    };
    let capture_request = FileCaptureRequest {
        scope_json: serde_json::to_vec(&scope).unwrap(),
        path: "download.bin".into(),
        expires_unix_ms: guardian::wall_ms() + 30000,
    };
    for leaf in [&f.tls.host, &f.reader] {
        let mut denied = transport::connect_file_reader(
            &f.url,
            f.config.host,
            f.tls.ca.pem().as_bytes(),
            leaf.cert.pem().as_bytes(),
            leaf.key.serialize_pem().as_bytes(),
        )
        .await
        .unwrap();
        assert_eq!(
            denied
                .capture(capture_request.clone())
                .await
                .unwrap_err()
                .code(),
            Code::PermissionDenied
        );
    }
    let mut denied = transport::connect(
        &f.url,
        f.config.host,
        f.tls.ca.pem().as_bytes(),
        f.file_reader.cert.pem().as_bytes(),
        f.file_reader.key.serialize_pem().as_bytes(),
    )
    .await
    .unwrap();
    assert_eq!(
        denied.health(HealthRequest {}).await.unwrap_err().code(),
        Code::PermissionDenied
    );
    let mut reader = transport::connect_file_reader(
        &f.url,
        f.config.host,
        f.tls.ca.pem().as_bytes(),
        f.file_reader.cert.pem().as_bytes(),
        f.file_reader.key.serialize_pem().as_bytes(),
    )
    .await
    .unwrap();
    let journal_before = fs::read(f.config.state_root.join("host.json")).unwrap();
    let response = reader
        .capture(capture_request.clone())
        .await
        .unwrap()
        .into_inner();
    let handle = model::captured(&capture_request, &response, false, guardian::wall_ms()).unwrap();
    assert_eq!(
        handle.capture.as_ref().unwrap().sha256,
        Sha256::digest(&bytes).to_vec()
    );
    let replacement = super::supervisor_files::upload(&owner, "download.bin", b"changed");
    controller.begin_file(replacement.clone()).await.unwrap();
    controller
        .write_file(FileWriteRequest {
            request: Some(replacement.clone()),
            offset: 0,
            data: b"changed".to_vec(),
        })
        .await
        .unwrap();
    controller.commit_file(replacement).await.unwrap();
    let journal_after_publication = fs::read(f.config.state_root.join("host.json")).unwrap();
    assert_ne!(journal_before, journal_after_publication);
    drop(reader);
    let mut reader = transport::connect_file_reader(
        &f.url,
        f.config.host,
        f.tls.ca.pem().as_bytes(),
        f.file_reader.cert.pem().as_bytes(),
        f.file_reader.key.serialize_pem().as_bytes(),
    )
    .await
    .unwrap();
    let mut read = FileDownloadRequest {
        scope_json: capture_request.scope_json.clone(),
        handle: Some(handle.clone()),
        offset: 0,
        limit: 32768,
        expires_unix_ms: guardian::wall_ms() + 30000,
    };
    let mut retrieved = Vec::new();
    while retrieved.len() < bytes.len() {
        read.offset = retrieved.len() as u64;
        let reply = reader.read(read.clone()).await.unwrap().into_inner();
        retrieved.extend(
            model::chunk(&read, &reply, false, guardian::wall_ms())
                .unwrap()
                .data,
        );
    }
    assert_eq!(retrieved, bytes);
    assert_eq!(
        Sha256::digest(&retrieved).to_vec(),
        handle.capture.as_ref().unwrap().sha256
    );
    let mut forged = read.clone();
    forged
        .handle
        .as_mut()
        .unwrap()
        .capture
        .as_mut()
        .unwrap()
        .path = "different".into();
    assert_eq!(
        reader.read(forged).await.unwrap_err().code(),
        Code::FailedPrecondition
    );
    let mut foreign = scope.clone();
    foreign.project_id = sandbox_protocol::ProjectId::generate();
    let mut forged = read.clone();
    forged.scope_json = serde_json::to_vec(&foreign).unwrap();
    assert!(reader.read(forged).await.is_err());
    let release = FileReleaseRequest {
        scope_json: read.scope_json.clone(),
        handle: Some(handle.clone()),
        expires_unix_ms: guardian::wall_ms() + 30000,
    };
    let reply = reader.release(release.clone()).await.unwrap().into_inner();
    model::released(&release, &reply, false, guardian::wall_ms()).unwrap();
    reader.release(release).await.unwrap();
    assert_eq!(
        reader.read(read.clone()).await.unwrap_err().code(),
        Code::NotFound
    );
    let fresh = reader
        .capture(capture_request.clone())
        .await
        .unwrap()
        .into_inner();
    let fresh = model::captured(&capture_request, &fresh, false, guardian::wall_ms()).unwrap();
    assert_ne!(fresh.id, handle.id);
    let mut new_read = read.clone();
    new_read.handle = Some(fresh);
    new_read.offset = 0;
    let reply = reader.read(new_read.clone()).await.unwrap().into_inner();
    assert_eq!(
        model::chunk(&new_read, &reply, false, guardian::wall_ms())
            .unwrap()
            .data,
        b"changed"
    );
    assert_eq!(
        fs::read(f.config.state_root.join("host.json")).unwrap(),
        journal_after_publication
    );
    let _ = controller
        .stop(StopRequest {
            ownership: Some(owner.clone()),
        })
        .await;
    f.released(&mut controller, &owner).await;
    assert!(reader.read(new_read.clone()).await.is_err());
    assert!(reader.capture(capture_request).await.is_err());
    f.restart().await;
    let mut reader = transport::connect_file_reader(
        &f.url,
        f.config.host,
        f.tls.ca.pem().as_bytes(),
        f.file_reader.cert.pem().as_bytes(),
        f.file_reader.key.serialize_pem().as_bytes(),
    )
    .await
    .unwrap();
    assert!(reader.read(new_read).await.is_err());
    println!(
        "real_file_download_observation={}",
        serde_json::json!({"simulated":false,"bytes":bytes.len(),"full_sha256_verified":true,"separate_reader_authority":true,"captured_bytes_survive_replacement":true,"reconnect_uses_same_capture":true,"released_handle_not_recreated":true,"journal_unchanged_by_reads":true,"destroy_and_restart_rejected":true,"cleanup_confirmed":true})
    );
}
