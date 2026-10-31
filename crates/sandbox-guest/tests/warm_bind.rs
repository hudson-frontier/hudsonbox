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
    call(&m, &client(&m), MemberRequest::Bind(Box::new(req.clone())))
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
    call(&m, &client(&m), MemberRequest::Bind(Box::new(req.clone())))
        .await
        .unwrap()
        .verify_bound(&req)
        .unwrap();
    let mut changed = req.clone();
    changed.scope.project_id = ProjectId::generate();
    changed.scope_sha256 = changed.scope.digest().unwrap();
    assert!(matches!(
        call(&m, &client(&m), MemberRequest::Bind(Box::new(changed)))
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
        call(
            &m,
            &client(&other),
            MemberRequest::Bind(Box::new(req.clone()))
        )
        .await
        .is_err()
    );
    assert!(
        call(&m, &m.fixture.client(), MemberRequest::Bind(Box::new(req)))
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
                m.guest.handle(MemberRequest::Bind(Box::new(bad)), now()),
                MemberResponse::Rejected { .. }
            ),
            "field {field}"
        );
        assert!(m.guest.bound().is_none());
    }
    m.guest
        .handle(MemberRequest::Bind(Box::new(req.clone())), now())
        .verify_bound(&req)
        .unwrap();
    let mut other = req.clone();
    other.allocation = request(&m).1.allocation;
    assert!(matches!(
        m.guest.handle(MemberRequest::Bind(Box::new(other)), now()),
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
        m.guest.handle(MemberRequest::Bind(Box::new(req)), now()),
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

#[tokio::test]
async fn allocation_identity_works_only_on_its_separate_channel() {
    use sha2::{Digest, Sha256};
    use tokio_rustls::rustls::pki_types::{CertificateDer, pem::PemObject};
    let m = member();
    let (allocation, req) = request(&m);
    m.guest
        .handle(MemberRequest::Bind(Box::new(req.clone())), now())
        .verify_bound(&req)
        .unwrap();
    let cert = CertificateDer::pem_slice_iter(req.allocation.guest_cert_pem.as_bytes())
        .next()
        .unwrap()
        .unwrap();
    let host = ClientTls::new(
        req.allocation.ca_pem.as_bytes(),
        allocation.host.cert.pem().as_bytes(),
        allocation.host.key.serialize_pem().as_bytes(),
        Sha256::digest(cert).into(),
        m.bootstrap.slot,
    )
    .unwrap();
    let tls = m.guest.bound().unwrap().server_tls(now()).unwrap();
    let (a, b) = tokio::io::duplex(1 << 17);
    let (server, client) = tokio::join!(tls.accept(a), host.connect(b));
    assert!(server.is_ok());
    assert!(client.is_ok());
    // Member credential/ALPN cannot access the installed allocation service.
    let old = client_for_member(&m, &m.fixture.host);
    let (a, b) = tokio::io::duplex(1 << 17);
    let (server, client) = tokio::join!(tls.accept(a), old.connect(b));
    assert!(server.is_err() || client.is_err());
}
fn client_for_member(m: &Member, host: &Leaf) -> ClientTls {
    use sha2::{Digest, Sha256};
    use tokio_rustls::rustls::pki_types::{CertificateDer, pem::PemObject};
    let cert = CertificateDer::pem_slice_iter(m.bootstrap.guest_cert_pem.as_bytes())
        .next()
        .unwrap()
        .unwrap();
    ClientTls::member(
        m.bootstrap.ca_pem.as_bytes(),
        host.cert.pem().as_bytes(),
        host.key.serialize_pem().as_bytes(),
        Sha256::digest(cert).into(),
        m.bootstrap.member,
    )
    .unwrap()
}
#[tokio::test]
async fn same_ca_unpinned_host_and_wrong_alpn_are_rejected() {
    let m = member();
    let (_, req) = request(&m);
    let unpinned = Leaf::new(&m.fixture.ca, "other-host.internal".into(), true);
    assert!(
        call(
            &m,
            &client_for_member(&m, &unpinned),
            MemberRequest::Bind(Box::new(req))
        )
        .await
        .is_err()
    );
    // Same certificates, CA, peer pins and SNI: only the ALPN differs.
    let wrong_protocol = sandbox_protocol::guest_wire::ServerTls::new(
        m.bootstrap.ca_pem.as_bytes(),
        m.bootstrap.guest_cert_pem.as_bytes(),
        m.bootstrap.guest_key_pem.as_bytes(),
        m.bootstrap.host_pin,
    )
    .unwrap();
    let host = client(&m);
    let (a, b) = tokio::io::duplex(1 << 17);
    let (server, client) = tokio::join!(wrong_protocol.accept(a), host.connect(b));
    assert!(server.is_err() || client.is_err());
    assert!(m.guest.bound().is_none());
}
#[test]
fn concurrent_conflicting_binds_install_one_identity() {
    let m = member();
    let (_, first) = request(&m);
    let (_, second) = request(&m);
    let answers = std::thread::scope(|scope| {
        let a = scope.spawn(|| {
            m.guest
                .handle(MemberRequest::Bind(Box::new(first.clone())), now())
        });
        let b = scope.spawn(|| {
            m.guest
                .handle(MemberRequest::Bind(Box::new(second.clone())), now())
        });
        [a.join().unwrap(), b.join().unwrap()]
    });
    assert_eq!(
        answers
            .iter()
            .filter(|r| matches!(r, MemberResponse::Bound { .. }))
            .count(),
        1
    );
    assert_eq!(
        answers
            .iter()
            .filter(|r| matches!(r, MemberResponse::Rejected { .. }))
            .count(),
        1
    );
    let installed = m.guest.bound().unwrap();
    assert!(installed == first.allocation || installed == second.allocation);
}
#[test]
fn missing_or_corrupt_committed_binding_is_never_unused() {
    for corrupt in [true, false] {
        let m = member();
        let (_, req) = request(&m);
        m.guest
            .handle(MemberRequest::Bind(Box::new(req.clone())), now())
            .verify_bound(&req)
            .unwrap();
        let path = m.directory.path().join("state");
        drop(m.guest);
        if corrupt {
            std::fs::write(path.join("binding.json"), b"corrupt").unwrap();
        } else {
            std::fs::remove_file(path.join("binding.json")).unwrap();
        }
        assert!(MemberGuest::open(m.bootstrap.clone(), BOOT.into(), &path).is_err());
    }
}
#[tokio::test]
async fn authenticated_oversize_and_unknown_actions_do_not_bind() {
    use tokio::io::AsyncWriteExt;
    for payload in [
        None,
        Some(br#"{"kind":"execute","version":1,"argv":["touch","/tmp/work"]}"#.as_slice()),
    ] {
        let m = member();
        let server = m.bootstrap.server_tls(now()).unwrap();
        let host = client(&m);
        let (a, b) = tokio::io::duplex(1 << 17);
        let (served, sent) =
            tokio::join!(serve_member_connection(&m.guest, &server, a, now), async {
                let mut stream = host.connect(b).await?;
                match payload {
                    None => stream.write_u32(u32::MAX).await?,
                    Some(bytes) => {
                        stream.write_u32(bytes.len() as u32).await?;
                        stream.write_all(bytes).await?;
                    }
                }
                stream.flush().await?;
                Ok::<_, anyhow::Error>(())
            });
        sent.unwrap();
        assert!(served.is_err());
        assert!(m.guest.bound().is_none());
    }
}

#[test]
fn pem_reencoding_cannot_disguise_a_reused_member_key() {
    use rcgen::{CertificateParams, ExtendedKeyUsagePurpose, KeyPair, KeyUsagePurpose};
    let m = member();
    let (allocation, mut req) = request(&m);
    let reused = KeyPair::from_pem(&m.bootstrap.guest_key_pem).unwrap();
    let mut params =
        CertificateParams::new(vec![guest_wire::guest_name(m.bootstrap.slot)]).unwrap();
    params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
    params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
    req.allocation.guest_cert_pem = params.signed_by(&reused, &allocation.ca).unwrap().pem();
    req.allocation.guest_key_pem = m.bootstrap.guest_key_pem.replace('\n', "\r\n");
    // This is a valid server key/certificate pair. Only decoded key reuse differs
    // from a legitimate fresh allocation identity.
    assert!(req.allocation.server_tls(now()).is_ok());
    assert!(matches!(
        m.guest.handle(MemberRequest::Bind(Box::new(req)), now()),
        MemberResponse::Rejected { .. }
    ));
    assert!(m.guest.bound().is_none());
}

#[test]
fn retained_state_rejects_symlink_fifo_oversize_and_public_modes_promptly() {
    use std::os::unix::fs::{PermissionsExt, symlink};
    for target in ["incarnation.json", "binding.json"] {
        for attack in 0..5 {
            let m = member();
            let (_, req) = request(&m);
            m.guest
                .handle(MemberRequest::Bind(Box::new(req.clone())), now())
                .verify_bound(&req)
                .unwrap();
            let path = m.directory.path().join("state");
            drop(m.guest);
            let file = path.join(target);
            match attack {
                0 => {
                    std::fs::remove_file(&file).unwrap();
                    symlink("/dev/zero", &file).unwrap();
                }
                1 => {
                    std::fs::remove_file(&file).unwrap();
                    rustix::fs::mknodat(
                        rustix::fs::CWD,
                        &file,
                        rustix::fs::FileType::Fifo,
                        rustix::fs::Mode::from_raw_mode(0o600),
                        0,
                    )
                    .unwrap();
                }
                2 => std::fs::write(&file, vec![b'x'; 128 * 1024]).unwrap(),
                3 => {
                    std::fs::set_permissions(&file, std::fs::Permissions::from_mode(0o644)).unwrap()
                }
                _ => {
                    std::fs::remove_file(&file).unwrap();
                    std::fs::create_dir(&file).unwrap();
                }
            }
            let start = std::time::Instant::now();
            assert!(MemberGuest::open(m.bootstrap.clone(), BOOT.into(), &path).is_err());
            assert!(start.elapsed() < std::time::Duration::from_secs(2));
        }
    }
}
#[test]
fn wrong_allocation_san_and_reencoded_member_identity_are_rejected() {
    let m = member();
    let (allocation, mut req) = request(&m);
    req.allocation.guest_cert_pem = allocation.guest.cert.pem(); // valid CA and key, wrong allocation SAN
    req.allocation.guest_key_pem = allocation.guest.key.serialize_pem();
    assert!(req.allocation.server_tls(now()).is_ok());
    assert!(matches!(
        m.guest.handle(MemberRequest::Bind(Box::new(req)), now()),
        MemberResponse::Rejected { .. }
    ));
    let (_, mut req) = request(&m);
    req.allocation.ca_pem = m.bootstrap.ca_pem.replace('\n', "\r\n");
    req.allocation.guest_cert_pem = m.bootstrap.guest_cert_pem.replace('\n', "\r\n");
    req.allocation.guest_key_pem = m.bootstrap.guest_key_pem.replace('\n', "\r\n");
    assert_ne!(req.allocation.host_pin, m.bootstrap.host_pin);
    assert!(req.allocation.server_tls(now()).is_ok());
    assert!(matches!(
        m.guest.handle(MemberRequest::Bind(Box::new(req)), now()),
        MemberResponse::Rejected { .. }
    ));
    assert!(m.guest.bound().is_none());
}
