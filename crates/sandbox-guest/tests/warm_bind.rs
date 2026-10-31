//! Portable real-TLS guest-boundary checks; no VM/KVM evidence.
#![allow(clippy::unwrap_used, clippy::expect_used)]
#[path = "../../sandbox-protocol/tests/support/guest_tls.rs"]
mod guest_tls;
use guest_tls::{Fixture, Leaf};
use sandbox_protocol::{
    HostId, Id, OperationId, ProjectId, SandboxId,
    bootstrap::Bootstrap,
    guest_wire::{self, ClientTls},
    warm_bind::*,
    warm_pool::HandoffIntent,
};
use tempfile::TempDir;
use uuid::Uuid;
const BOOT: &str = "boot-1";
fn now() -> i64 {
    1_700_000_000_000
}
struct Member {
    fixture: Fixture,
    bootstrap: MemberBootstrap,
    directory: TempDir,
    guest: MemberGuest,
}
fn member() -> Member {
    let fixture = Fixture::new();
    let id = Uuid::now_v7();
    let leaf = Leaf::new(&fixture.ca, guest_wire::member_name(id), false);
    let bootstrap = MemberBootstrap {
        version: MEMBER_VERSION,
        member: id,
        slot: fixture.allocation,
        valid_until_unix_ms: i64::MAX,
        ca_pem: fixture.ca.pem(),
        guest_cert_pem: leaf.cert.pem(),
        guest_key_pem: leaf.key.serialize_pem(),
        host_pin: fixture.host.pin(),
    };
    let directory = tempfile::tempdir().unwrap();
    let guest = MemberGuest::open(
        bootstrap.clone(),
        BOOT.into(),
        &directory.path().join("state"),
    )
    .unwrap();
    Member {
        fixture,
        bootstrap,
        directory,
        guest,
    }
}
fn client(m: &Member) -> ClientTls {
    use sha2::{Digest, Sha256};
    use tokio_rustls::rustls::pki_types::{CertificateDer, pem::PemObject};
    let cert = CertificateDer::pem_slice_iter(m.bootstrap.guest_cert_pem.as_bytes())
        .next()
        .unwrap()
        .unwrap();
    ClientTls::member(
        m.bootstrap.ca_pem.as_bytes(),
        m.fixture.host.cert.pem().as_bytes(),
        m.fixture.host.key.serialize_pem().as_bytes(),
        Sha256::digest(cert).into(),
        m.bootstrap.member,
    )
    .unwrap()
}
fn request(m: &Member) -> (Fixture, BindRequest) {
    let allocation = Fixture::new();
    // Certificate server name must match slot, although the TLS fixture has a distinct ID.
    let leaf = Leaf::new(
        &allocation.ca,
        guest_wire::guest_name(m.bootstrap.slot),
        false,
    );
    let bootstrap = Bootstrap {
        version: 1,
        allocation: m.bootstrap.slot,
        generation: 1,
        valid_until_unix_ms: i64::MAX,
        ca_pem: allocation.ca.pem(),
        guest_cert_pem: leaf.cert.pem(),
        guest_key_pem: leaf.key.serialize_pem(),
        host_pin: allocation.host.pin(),
    };
    let scope = HandoffIntent {
        version: 1,
        member_id: m.bootstrap.member,
        vm_ref: "warm-member".into(),
        template_id: Uuid::now_v7(),
        allocation_id: m.bootstrap.slot,
        project_id: ProjectId::generate(),
        sandbox_id: SandboxId::generate(),
        create_operation_id: OperationId::generate(),
        generation: 1,
        permit_serial: 1,
        host_id: HostId::generate(),
        supervisor_epoch: 1,
        claim_revision: 1,
    };
    let req = BindRequest {
        version: BIND_VERSION,
        request_id: Uuid::now_v7(),
        member: m.bootstrap.member,
        slot: m.bootstrap.slot,
        boot_id: BOOT.into(),
        scope_sha256: scope.digest().unwrap(),
        scope,
        allocation: bootstrap,
    };
    (allocation, req)
}
async fn call(m: &Member, tls: &ClientTls, req: MemberRequest) -> anyhow::Result<MemberResponse> {
    let server = m.bootstrap.server_tls(now())?;
    let (a, b) = tokio::io::duplex(1 << 17);
    let (served, response) = tokio::join!(
        serve_member_connection(&m.guest, &server, a, now),
        member_call(tls, b, &req)
    );
    let response = response?;
    served?;
    Ok(response)
}
#[tokio::test]
async fn authenticated_binding_persists_and_exact_retry_survives_restart() {
    let mut m = member();
    let (_, req) = request(&m);
    assert!(m.guest.bound().is_none());
    call(&m, &client(&m), MemberRequest::Bind(req.clone()))
        .await
        .unwrap()
        .verify_bound(&req)
        .unwrap();
    // Separate ordinary allocation channel accepts only its new host credential.
    assert_eq!(m.guest.bound().unwrap(), req.allocation);
    let path = m.directory.path().join("state");
    assert!(MemberGuest::open(m.bootstrap.clone(), BOOT.into(), &path).is_err());
    // Drop closes the cross-process incarnation lock, as process death would.
    drop(m.guest);
    m.guest = MemberGuest::open(m.bootstrap.clone(), BOOT.into(), &path).unwrap();
    call(&m, &client(&m), MemberRequest::Bind(req.clone()))
        .await
        .unwrap()
        .verify_bound(&req)
        .unwrap();
    let mut changed = req.clone();
    changed.scope.project_id = ProjectId::generate();
    changed.scope_sha256 = changed.scope.digest().unwrap();
    assert!(matches!(
        call(&m, &client(&m), MemberRequest::Bind(changed))
            .await
            .unwrap(),
        MemberResponse::Rejected { .. }
    ));
    drop(m.guest);
    assert!(MemberGuest::open(m.bootstrap.clone(), "new-boot".into(), &path).is_err());
}
#[tokio::test]
async fn wrong_tls_identity_and_allocation_protocol_cannot_bind() {
    let m = member();
    let (_, req) = request(&m);
    let other = member();
    assert!(
        call(&m, &client(&other), MemberRequest::Bind(req.clone()))
            .await
            .is_err()
    );
    assert!(
        call(&m, &m.fixture.client(), MemberRequest::Bind(req))
            .await
            .is_err()
    );
    assert!(m.guest.bound().is_none());
}
#[test]
fn scope_boot_version_and_credentials_are_checked_before_installation() {
    let m = member();
    let (_, req) = request(&m);
    for field in 0..9 {
        let mut bad = req.clone();
        match field {
            0 => bad.version += 1,
            1 => bad.boot_id = "old-boot".into(),
            2 => bad.member = Uuid::now_v7(),
            3 => bad.scope.supervisor_epoch = 0,
            4 => bad.scope.claim_revision = 0,
            5 => bad.scope.permit_serial = 0,
            6 => bad.scope.project_id = ProjectId::generate(), // digest no longer matches
            7 => bad.allocation.host_pin = m.bootstrap.host_pin,
            _ => bad.allocation.guest_key_pem = m.bootstrap.guest_key_pem.clone(),
        }
        assert!(
            matches!(
                m.guest.handle(MemberRequest::Bind(bad), now()),
                MemberResponse::Rejected { .. }
            ),
            "field {field}"
        );
        assert!(m.guest.bound().is_none());
    }
    m.guest
        .handle(MemberRequest::Bind(req.clone()), now())
        .verify_bound(&req)
        .unwrap();
    let mut other = req.clone();
    other.allocation = request(&m).1.allocation;
    assert!(matches!(
        m.guest.handle(MemberRequest::Bind(other), now()),
        MemberResponse::Rejected { .. }
    ));
    assert!(!format!("{req:?}").contains("PRIVATE KEY"));
}
#[test]
fn partial_persistence_and_changed_bootstrap_never_reset_to_unused() {
    let m = member();
    let path = m.directory.path().join("state");
    std::fs::write(path.join("binding.pending"), b"partial").unwrap();
    let (_, req) = request(&m);
    assert!(matches!(
        m.guest.handle(MemberRequest::Bind(req), now()),
        MemberResponse::Rejected { .. }
    ));
    drop(m.guest);
    assert!(MemberGuest::open(m.bootstrap.clone(), BOOT.into(), &path).is_err());
    std::fs::remove_file(path.join("binding.pending")).unwrap();
    let mut changed = m.bootstrap.clone();
    changed.slot = sandbox_protocol::AllocationId::generate();
    assert!(MemberGuest::open(changed, BOOT.into(), &path).is_err());
}
#[test]
fn v2_is_strict_and_cold_v1_remains_readable() {
    let m = member();
    let (_, req) = request(&m);
    let bytes = m.bootstrap.encode_device().unwrap();
    assert!(Bootstrap::read_device(bytes.as_slice()).is_err());
    assert!(matches!(
        read_device(bytes.as_slice()).unwrap(),
        Device::Member(_)
    ));
    let cold = req.allocation.encode_device().unwrap();
    assert!(matches!(
        read_device(cold.as_slice()).unwrap(),
        Device::Allocation(_)
    ));
    assert!(read_device(&bytes[..20]).is_err());
    let mut bad = bytes;
    bad[8..12].copy_from_slice(&u32::MAX.to_be_bytes());
    assert!(read_device(bad.as_slice()).is_err());
}
