//! Actual development certificate generation, filesystem bounds and authenticated RPC.
#![cfg(unix)]
#![allow(clippy::unwrap_used, clippy::expect_used)]
use sandbox_fake_host::{FakeConfig, FakeHost};
use sandbox_protocol::{
    HostId, Id,
    supervisor::{HealthRequest, supervisor_server::SupervisorServer},
};
use sandbox_supervisor::transport::{self, ControllerIdentity};
use serde_json::Value;
use std::{collections::BTreeSet, fs, os::unix::fs::PermissionsExt, path::Path, process::Command};
use tokio_stream::wrappers::TcpListenerStream;

fn generate(directory: &Path, host: Option<HostId>) -> std::process::Output {
    let mut command = Command::new(env!("CARGO_BIN_EXE_sandbox-local-pki"));
    command.arg("--directory").arg(directory);
    if let Some(host) = host {
        command.arg("--host-id").arg(host.to_string());
    }
    command.output().unwrap()
}
fn private_parent() -> tempfile::TempDir {
    let parent = tempfile::tempdir_in(std::env::temp_dir().canonicalize().unwrap()).unwrap();
    fs::set_permissions(parent.path(), fs::Permissions::from_mode(0o700)).unwrap();
    parent
}
fn data(report: &Value, role: &str, field: &str) -> Vec<u8> {
    fs::read(report[role][field].as_str().unwrap()).unwrap()
}

#[test]
fn private_bundle_has_distinct_identities_and_prints_no_secret_material() {
    let parent = private_parent();
    let directory = parent.path().join("pki");
    let host = HostId::generate();
    let output = generate(&directory, Some(host));
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let report: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(report["version"], 1);
    assert_eq!(report["host_id"], host.to_string());
    assert_eq!(
        fs::metadata(&directory).unwrap().permissions().mode() & 0o777,
        0o700
    );
    let manifest: Value =
        serde_json::from_slice(&fs::read(directory.join("manifest.json")).unwrap()).unwrap();
    assert_eq!(manifest, report);
    let mut pins = BTreeSet::new();
    let mut keys = BTreeSet::new();
    for role in [
        "api_server",
        "host_server",
        "controller",
        "output_reader",
        "file_reader",
    ] {
        assert!(pins.insert(report[role]["sha256_der"].as_str().unwrap()));
        for field in ["certificate", "private_key"] {
            let path = Path::new(report[role][field].as_str().unwrap());
            assert_eq!(
                fs::metadata(path).unwrap().permissions().mode() & 0o777,
                0o600
            );
        }
        let key = data(&report, role, "private_key");
        assert!(keys.insert(key.clone()), "service keys must be distinct");
        assert!(!output.stdout.windows(key.len()).any(|bytes| bytes == key));
    }
    assert!(
        keys.insert(data(&report, "ca", "private_key")),
        "CA key must be distinct"
    );
    for entry in fs::read_dir(&directory).unwrap() {
        assert_eq!(
            entry.unwrap().metadata().unwrap().permissions().mode() & 0o777,
            0o600
        );
    }
    assert!(!String::from_utf8_lossy(&output.stdout).contains("PRIVATE KEY"));
    let remaining = report["expires_unix_seconds"].as_i64().unwrap()
        - time::OffsetDateTime::now_utc().unix_timestamp();
    assert!((6 * 86400..=7 * 86400).contains(&remaining));
    let original = fs::read(directory.join("ca.key")).unwrap();
    assert!(!generate(&directory, Some(host)).status.success());
    assert_eq!(fs::read(directory.join("ca.key")).unwrap(), original);
}

#[test]
fn unsafe_parent_relative_existing_and_symlink_destinations_are_refused() {
    let parent = private_parent();
    let public = parent.path().join("public");
    fs::create_dir(&public).unwrap();
    fs::set_permissions(&public, fs::Permissions::from_mode(0o755)).unwrap();
    assert!(!generate(&public.join("child"), None).status.success());
    assert!(!public.join("child").exists());
    let linked = parent.path().join("linked");
    std::os::unix::fs::symlink(parent.path(), &linked).unwrap();
    assert!(!generate(&linked.join("child"), None).status.success());
    let existing = parent.path().join("keep");
    fs::write(&existing, b"unrelated").unwrap();
    assert!(!generate(&existing, None).status.success());
    assert_eq!(fs::read(existing).unwrap(), b"unrelated");
    assert!(!generate(Path::new("relative-pki"), None).status.success());
}

#[tokio::test]
async fn generated_controller_connects_and_readers_cannot_mutate_the_host() {
    let parent = private_parent();
    let directory = parent.path().join("pki");
    let output = generate(&directory, None);
    assert!(output.status.success());
    let report: Value = serde_json::from_slice(&output.stdout).unwrap();
    let host: HostId = report["host_id"].as_str().unwrap().parse().unwrap();
    let ca = data(&report, "ca", "certificate");
    let mut pin = [0; 32];
    hex::decode_to_slice(
        report["controller"]["sha256_der"].as_str().unwrap(),
        &mut pin,
    )
    .unwrap();
    let authorized = ControllerIdentity::new(vec![pin]).unwrap();
    let fake = FakeHost::new(FakeConfig {
        host,
        epoch: 1,
        images: BTreeSet::from([format!("sha256:{}", "a".repeat(64))]),
        capacity: sandbox_protocol::supervisor::Resources {
            vcpu: 4,
            memory_mib: 8192,
            disk_mib: 65536,
        },
    })
    .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("https://{}", listener.local_addr().unwrap());
    let service = tonic::service::interceptor::InterceptedService::new(
        SupervisorServer::new(fake),
        authorized,
    );
    let tls = transport::server_tls(
        &ca,
        &data(&report, "host_server", "certificate"),
        &data(&report, "host_server", "private_key"),
    );
    let server = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .tls_config(tls)
            .unwrap()
            .add_service(service)
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .unwrap();
    });
    let mut controller = transport::connect(
        &endpoint,
        host,
        &ca,
        &data(&report, "controller", "certificate"),
        &data(&report, "controller", "private_key"),
    )
    .await
    .unwrap();
    assert_eq!(
        controller
            .health(HealthRequest {})
            .await
            .unwrap()
            .into_inner()
            .host_id,
        host.to_string()
    );
    for role in ["output_reader", "file_reader"] {
        let mut reader = transport::connect(
            &endpoint,
            host,
            &ca,
            &data(&report, role, "certificate"),
            &data(&report, role, "private_key"),
        )
        .await
        .unwrap();
        assert_eq!(
            reader.health(HealthRequest {}).await.unwrap_err().code(),
            tonic::Code::PermissionDenied
        );
        assert_eq!(
            reader
                .create(sandbox_protocol::supervisor::CreateRequest::default())
                .await
                .unwrap_err()
                .code(),
            tonic::Code::PermissionDenied
        );
    }
    let other_host = HostId::generate();
    assert!(
        transport::connect(
            &endpoint,
            other_host,
            &ca,
            &data(&report, "controller", "certificate"),
            &data(&report, "controller", "private_key")
        )
        .await
        .is_err()
    );
    server.abort();
    let _ = server.await;
}
