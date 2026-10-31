#![allow(clippy::unwrap_used)]
use super::*;
use crate::launch_authority::AuthorityFile;
use sandbox_protocol::{allocation_authority::Permit, allocation_retirement::DomainClosure};
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt, symlink};

fn data() -> (tempfile::TempDir, Manifest, Intent) {
    let temp = tempfile::tempdir().unwrap();
    let host = HostId::generate();
    let root = temp.path().join("a");
    let p = Permit {
        host,
        project: ProjectId::generate(),
        sandbox: SandboxId::generate(),
        allocation: AllocationId::generate(),
        create_operation: OperationId::generate(),
        generation: 1,
        original_epoch: 1,
        serial: 1,
    };
    let artifact = Artifact {
        path: "/unused".into(),
        sha256: "ab".repeat(32),
    };
    let manifest = Manifest {
        launch_permit: Some(p.clone()),
        config: Config {
            state_root: root,
            cgroup_parent: "/sys/fs/cgroup/hudson-guardians-tests".into(),
            firecracker: artifact.clone(),
            jailer: artifact.clone(),
            kernel: artifact.clone(),
            rootfs: artifact,
            jail_uid: 65534,
            jail_gid: 65534,
        },
        start: Start {
            owner: Owner {
                host,
                project: p.project,
                sandbox: p.sandbox,
                allocation: p.allocation,
                create_operation: p.create_operation,
                generation: 1,
                epoch: 1,
            },
            vcpu: 1,
            memory_mib: 128,
            disk_mib: 64,
            expires_unix_ms: wall_ms() + 30000,
        },
        member: None,
    };
    let intent = Intent {
        version: 1,
        retirement: OperationId::generate(),
        permit: p,
        commands: DomainClosure::Empty {},
        files: DomainClosure::Empty {},
        release_evidence_sha256: "cd".repeat(32),
        simulated: false,
    };
    (temp, manifest, intent)
}
fn fixture() -> (tempfile::TempDir, AuthorityFile, Manifest, Intent) {
    assert_eq!(std::env::var("HUDSON_GUARDIAN_TEST_VM").as_deref(), Ok("1"));
    let (temp, manifest, intent) = data();
    let authority =
        AuthorityFile::initialize(manifest.config.state_root.clone(), intent.permit.host, 1)
            .unwrap();
    authority
        .register(1, std::slice::from_ref(&intent.permit))
        .unwrap();
    (temp, authority, manifest, intent)
}
fn open(m: &Manifest, i: &Intent, plan: Option<&Plan>) -> Result<Session> {
    Session::open(
        &m.config.state_root,
        &m.config.cgroup_parent,
        1,
        1,
        i,
        Some(m),
        plan,
    )
}
#[test]
#[ignore = "requires root in the dedicated HUDSON_GUARDIAN_TEST_VM"]
fn each_owned_unlink_can_resume_and_delayed_cleanup_cannot_recreate_metadata() {
    let (temp, authority, m, intent) = fixture();
    m.fence_unstarted().unwrap();
    authority
        .fence(1, &intent.permit, intent.retirement)
        .unwrap();
    let old_lock = File::open(m.directory().join("lifecycle.lock")).unwrap();
    let mut session = open(&m, &intent, None).unwrap();
    let plan_path = temp.path().join("retained-plan.json");
    write_json(&plan_path, &session.plan).unwrap();
    assert!(m.reconcile("delayed").is_err());
    assert!(m.fence_unstarted().is_err());
    for remaining in [2, 1, 0] {
        assert!(!session.remove_next().unwrap());
        drop(session);
        let names: Vec<_> = fs::read_dir(m.directory())
            .unwrap()
            .map(|e| e.unwrap().file_name())
            .collect();
        assert_eq!(names.len(), remaining);
        assert!(m.reconcile("delayed").is_err());
        assert!(m.fence_unstarted().is_err());
        assert_eq!(fs::read_dir(m.directory()).unwrap().count(), remaining);
        let plan: Plan = read_json(&plan_path).unwrap();
        session = open(&m, &intent, Some(&plan)).unwrap();
    }
    assert!(session.remove_next().unwrap());
    drop(session);
    assert!(!m.directory().exists());
    // A stale inode can become lockable, but never grants launch/metadata ownership.
    rustix::fs::flock(
        &old_lock,
        rustix::fs::FlockOperation::NonBlockingLockExclusive,
    )
    .unwrap();
    assert!(m.prepare().is_err());
    assert!(m.reconcile("delayed").is_err());
    assert!(m.fence_unstarted().is_err());
    assert!(!m.directory().exists());
    let plan: Plan = read_json(&plan_path).unwrap();
    let mut resumed = open(&m, &intent, Some(&plan)).unwrap();
    assert!(resumed.is_removed());
    assert!(resumed.remove_next().unwrap());
    drop(resumed);
    assert!(authority.checkpoint(1).is_ok());
    assert!(
        crate::launch_authority::authorize(&m.config.state_root, Some(&intent.permit)).is_err()
    );
}
#[test]
#[ignore = "requires root in the dedicated HUDSON_GUARDIAN_TEST_VM"]
fn deletion_rejects_live_locks_unowned_entries_missing_evidence_and_scope_changes() {
    let (_temp, authority, m, intent) = fixture();
    m.fence_unstarted().unwrap();
    assert!(open(&m, &intent, None).is_err()); // Active is not launch denial.
    authority
        .fence(1, &intent.permit, intent.retirement)
        .unwrap();
    fs::create_dir_all(&m.config.cgroup_parent).unwrap();
    fs::create_dir(m.group()).unwrap();
    assert!(open(&m, &intent, None).is_err());
    assert!(m.group().exists());
    fs::remove_dir(m.group()).unwrap();
    let lock = m.lifecycle_lock(false).unwrap();
    assert!(open(&m, &intent, None).is_err());
    drop(lock);
    fs::write(m.directory().join("unowned"), b"keep").unwrap();
    assert!(open(&m, &intent, None).is_err());
    assert_eq!(fs::read(m.directory().join("unowned")).unwrap(), b"keep");
    fs::remove_file(m.directory().join("unowned")).unwrap();
    let session = open(&m, &intent, None).unwrap();
    let plan = session.plan.clone();
    drop(session);
    let mut legacy_value = serde_json::to_value(&plan).unwrap();
    legacy_value["version"] = 1.into();
    legacy_value["metadata"]
        .as_object_mut()
        .unwrap()
        .remove("staged");
    let legacy: Plan = serde_json::from_value(legacy_value).unwrap();
    assert!(open(&m, &intent, Some(&legacy)).is_ok());
    let mut corrupt = plan.clone();
    if let Metadata::Stopped { receipt, .. } = &mut corrupt.metadata {
        receipt.reason = Some("changed_after_removal".into());
    }
    assert!(open(&m, &intent, Some(&corrupt)).is_err());
    let mut changed = intent.clone();
    changed.release_evidence_sha256 = "ef".repeat(32);
    assert!(open(&m, &changed, Some(&plan)).is_err());
    changed = intent.clone();
    changed.retirement = OperationId::generate();
    assert!(open(&m, &changed, Some(&plan)).is_err());
    changed = intent.clone();
    changed.simulated = true;
    assert!(open(&m, &changed, Some(&plan)).is_err());
    let receipt = m.record_path();
    let bytes = fs::read(&receipt).unwrap();
    fs::write(&receipt, b"{}").unwrap();
    assert!(open(&m, &intent, Some(&plan)).is_err());
    fs::write(&receipt, bytes).unwrap();
    let alias = m.directory().join("alias");
    fs::hard_link(&receipt, &alias).unwrap();
    assert!(open(&m, &intent, Some(&plan)).is_err());
    fs::remove_file(alias).unwrap();
    fs::remove_file(&receipt).unwrap();
    symlink("manifest.json", &receipt).unwrap();
    assert!(open(&m, &intent, Some(&plan)).is_err());
    fs::remove_file(receipt).unwrap();
    assert!(open(&m, &intent, None).is_err()); // Missing alone is never cleanup.
    assert!(open(&m, &intent, Some(&plan)).is_ok()); // Retained deletion intent permits partial removal.
}

#[test]
#[ignore = "requires root in the dedicated HUDSON_GUARDIAN_TEST_VM"]
fn retirement_reconciles_bounded_owned_atomic_write_stages() {
    let (temp, authority, m, intent) = fixture();
    m.fence_unstarted().unwrap();
    authority
        .fence(1, &intent.permit, intent.retirement)
        .unwrap();
    // Exercise the production atomic writer so the classifier stays aligned
    // with the exact staging-name format used by guardian metadata updates.
    let receipt_path = m.directory().join("receipt.json");
    let receipt: Receipt = read_json(&receipt_path).unwrap();
    write_json(&receipt_path, &receipt).unwrap();
    let leftover_stage = fs::read_dir(m.directory())
        .unwrap()
        .map(|entry| entry.unwrap().file_name().into_string().unwrap())
        .any(|name| is_staging_name(&name));
    assert!(
        !leftover_stage,
        "guardian atomic writer must retire its stage after successful rename"
    );
    let staged_names: Vec<_> = [b"partial receipt".as_slice(), b"partial manifest"]
        .into_iter()
        .map(|bytes| {
            let name = format!("{}.tmp", OperationId::generate());
            let mut file = OpenOptions::new()
                .write(true)
                .create_new(true)
                .mode(0o600)
                .open(m.directory().join(&name))
                .unwrap();
            file.write_all(bytes).unwrap();
            file.sync_all().unwrap();
            name
        })
        .collect();

    let mut session = open(&m, &intent, None).unwrap();
    let plan = session.plan.clone();
    assert_eq!(plan.version, 2);
    match &plan.metadata {
        Metadata::Stopped { staged, .. } => {
            assert_eq!(staged.len(), staged_names.len());
            assert!(staged_names.iter().all(|name| staged.contains_key(name)));
        }
        Metadata::Absent {} => panic!("stopped guardian must retain its staged inventory"),
    }
    let plan_path = temp.path().join("retained-plan.json");
    write_json(&plan_path, &plan).unwrap();
    assert!(!session.remove_next().unwrap());
    drop(session);

    let mut removed = false;
    for _ in 0..8 {
        let retained: Plan = read_json(&plan_path).unwrap();
        let mut resumed = open(&m, &intent, Some(&retained)).unwrap();
        if resumed.remove_next().unwrap() {
            removed = true;
            break;
        }
    }
    assert!(removed);
    assert!(!m.directory().exists());
    assert!(
        authority.retirement_guard(1, &intent).unwrap().phase
            != crate::launch_authority::RetirementPhase::Closed
    );
}

#[test]
#[ignore = "requires root in the dedicated HUDSON_GUARDIAN_TEST_VM"]
fn retirement_bounds_owned_atomic_write_stages_and_keeps_unknown_metadata() {
    let (_temp, authority, m, intent) = fixture();
    m.fence_unstarted().unwrap();
    authority
        .fence(1, &intent.permit, intent.retirement)
        .unwrap();
    for _ in 0..=MAX_STAGED_FILES {
        let path = m
            .directory()
            .join(format!("{}.tmp", OperationId::generate()));
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(path)
            .unwrap();
        file.write_all(b"stage").unwrap();
        file.sync_all().unwrap();
    }
    assert!(open(&m, &intent, None).is_err());
    assert_eq!(
        fs::read_dir(m.directory()).unwrap().count(),
        NAMES.len() + MAX_STAGED_FILES + 1
    );
    for item in fs::read_dir(m.directory()).unwrap() {
        let item = item.unwrap();
        let name = item.file_name().into_string().unwrap();
        if is_staging_name(&name) {
            fs::remove_file(item.path()).unwrap();
        }
    }
    let unexpected = m.directory().join("notes.tmp");
    fs::write(&unexpected, b"preserve").unwrap();
    fs::set_permissions(&unexpected, fs::Permissions::from_mode(0o600)).unwrap();
    let error = open(&m, &intent, None).err().unwrap();
    assert!(error.to_string().contains("notes.tmp"));
    assert_eq!(fs::read(&unexpected).unwrap(), b"preserve");
}
#[test]
#[ignore = "requires root in the dedicated HUDSON_GUARDIAN_TEST_VM"]
fn unused_permit_requires_absence_and_cannot_consume_a_guardian_plan() {
    let (_temp, authority, m, intent) = fixture();
    authority
        .fence(1, &intent.permit, intent.retirement)
        .unwrap();
    let mut session = Session::open(
        &m.config.state_root,
        &m.config.cgroup_parent,
        1,
        1,
        &intent,
        None,
        None,
    )
    .unwrap();
    let plan = session.plan.clone();
    assert!(session.remove_next().unwrap());
    drop(session);
    assert!(open(&m, &intent, Some(&plan)).is_err());
    fs::create_dir(m.directory()).unwrap();
    assert!(
        Session::open(
            &m.config.state_root,
            &m.config.cgroup_parent,
            1,
            1,
            &intent,
            None,
            Some(&plan)
        )
        .is_err()
    );
    assert!(m.fence_unstarted().is_err());
    assert_eq!(fs::read_dir(m.directory()).unwrap().count(), 0);
}

#[test]
#[ignore = "requires root in the dedicated HUDSON_GUARDIAN_TEST_VM; no KVM needed"]
fn retained_member_absence_requires_free_lifecycle_lock_and_no_runtime_paths() {
    let (_temp, _authority, mut m, _) = fixture();
    let slot = m.start.owner.allocation;
    let member = uuid::Uuid::now_v7();
    m.launch_permit = None;
    m.start.owner = Owner::inert(m.start.owner.host, member, slot, 1);
    m.member = Some(Member {
        member,
        slot,
        valid_until_unix_ms: wall_ms() + 60_000,
        bound_from: None,
    });
    assert!(m.prove_member_absent().is_err());
    m.fence_unstarted().unwrap();
    assert!(m.prove_member_absent().unwrap().cleanup_confirmed);
    let held = m.lifecycle_lock(false).unwrap();
    assert!(m.prove_member_absent().is_err());
    drop(held);
    let run = m.directory().join("run");
    fs::create_dir(&run).unwrap();
    assert!(m.prove_member_absent().is_err());
    fs::remove_dir(&run).unwrap();
    symlink("/nonexistent", &run).unwrap();
    assert!(m.prove_member_absent().is_err());
    fs::remove_file(&run).unwrap();
    assert!(m.prove_member_absent().is_ok());
    fs::remove_file(m.record_path()).unwrap();
    assert!(m.prove_member_absent().is_err());
}

// A stopped guardian has no live process or runtime resources. Keep its actual
// serialized inert manifest and permit-bearing adoption receipt, as production does.
fn adopted(cold: &Manifest) -> (Manifest, Manifest, Receipt) {
    let mut original = cold.clone();
    let member = uuid::Uuid::now_v7();
    let slot = AllocationId::generate();
    original.launch_permit = None;
    original.start.owner = Owner::inert(cold.start.owner.host, member, slot, 1);
    original.member = Some(Member {
        member,
        slot,
        valid_until_unix_ms: wall_ms() + 60_000,
        bound_from: None,
    });
    let binding = Binding {
        owner: cold.start.owner.clone(),
        permit: cold.launch_permit.clone(),
        intent_sha256: "12".repeat(32),
        identity_digest: "34".repeat(32),
        guest_identity_sha256: "56".repeat(32),
        identity_expires_unix_ms: wall_ms() + 30_000,
        confirmed: true,
    };
    let view = original.bound(&binding).unwrap();
    let receipt = Receipt {
        version: 1,
        owner: original.start.owner.clone(),
        digest: original.digest().unwrap(),
        host_boot_id: "dead-test-boot".into(),
        state: State::Stopped,
        cleanup_confirmed: true,
        renewal_revision: 1,
        expires_unix_ms: original.start.expires_unix_ms,
        guardian_host_pid: None,
        guardian_start_ticks: None,
        firecracker_namespace_pid: None,
        cgroup_inode: None,
        reason: Some("stopped_adopted".into()),
        identity_digest: Some(binding.identity_digest.clone()),
        identity_expires_unix_ms: Some(binding.identity_expires_unix_ms),
        guest_boot_id: Some("guest-boot".into()),
        binding: Some(binding),
    };
    (original, view, receipt)
}

#[test]
fn adopted_retirement_mapping_rejects_forged_slot_owner_permit_and_manifest() {
    let (_temp, cold, _) = data();
    let (original, view, receipt) = adopted(&cold);
    assert_ne!(view.key(), view.start.owner.allocation);
    assert_eq!(
        retirement_manifest(&view, &receipt)
            .unwrap()
            .digest()
            .unwrap(),
        original.digest().unwrap()
    );
    let mut forged = view.clone();
    forged.member.as_mut().unwrap().slot = AllocationId::generate();
    assert!(retirement_manifest(&forged, &receipt).is_err());
    forged = view.clone();
    forged.config.rootfs.sha256 = "ab".repeat(31) + "cd";
    assert!(retirement_manifest(&forged, &receipt).is_err());
    let mut wrong = receipt.clone();
    wrong
        .binding
        .as_mut()
        .unwrap()
        .permit
        .as_mut()
        .unwrap()
        .serial += 1;
    assert!(retirement_manifest(&view, &wrong).is_err());
    wrong = receipt.clone();
    wrong.binding.as_mut().unwrap().owner.project = ProjectId::generate();
    assert!(retirement_manifest(&view, &wrong).is_err());
    wrong = receipt.clone();
    wrong.binding.as_mut().unwrap().confirmed = false;
    assert!(retirement_manifest(&view, &wrong).is_err());
}

#[test]
#[ignore = "requires root in the dedicated HUDSON_GUARDIAN_TEST_VM; no KVM needed"]
fn adopted_slot_metadata_retirement_resumes_exact_serialized_evidence() {
    let (temp, authority, cold, intent) = fixture();
    let (original, view, receipt) = adopted(&cold);
    private_dir(&original.directory()).unwrap();
    drop(original.lifecycle_lock(true).unwrap());
    write_json(&original.directory().join("manifest.json"), &original).unwrap();
    write_json(&original.record_path(), &receipt).unwrap();
    // A different stopped VM at the allocation-keyed path must survive.
    cold.fence_unstarted().unwrap();
    let other_manifest = fs::read(cold.directory().join("manifest.json")).unwrap();
    let other_receipt = fs::read(cold.record_path()).unwrap();
    authority
        .fence(1, &intent.permit, intent.retirement)
        .unwrap();
    let mut forged = view.clone();
    forged.member.as_mut().unwrap().slot = cold.key();
    assert!(open(&forged, &intent, None).is_err());
    let receipt_path = original.record_path();
    let mut wrong = receipt.clone();
    wrong
        .binding
        .as_mut()
        .unwrap()
        .permit
        .as_mut()
        .unwrap()
        .serial += 1;
    write_json(&receipt_path, &wrong).unwrap();
    assert!(open(&view, &intent, None).is_err());
    write_json(&receipt_path, &receipt).unwrap();
    let mut session = open(&view, &intent, None).unwrap();
    assert_eq!(session.plan.version, 3);
    let plan_path = temp.path().join("adopted-plan.json");
    write_json(&plan_path, &session.plan).unwrap();
    let plan: Plan = read_json(&plan_path).unwrap();
    match &plan.metadata {
        Metadata::Stopped {
            original_manifest,
            files,
            receipt: saved,
            ..
        } => {
            assert_eq!(
                original_manifest.as_ref().unwrap().digest().unwrap(),
                original.digest().unwrap()
            );
            assert_eq!(files["manifest.json"].sha256, original.digest().unwrap());
            assert_eq!(
                serde_json::to_vec(saved).unwrap(),
                serde_json::to_vec(&receipt).unwrap()
            );
        }
        _ => panic!("missing stopped metadata"),
    }
    assert!(
        plan.verify_removed(
            &view.config.state_root,
            &view.config.cgroup_parent,
            &intent,
            Some(&view)
        )
        .is_err()
    );
    // Retry after each unlink, including after both JSON files disappear.
    for _ in 0..3 {
        assert!(!session.remove_next().unwrap());
        drop(session);
        assert!(open(&view, &intent, None).is_err());
        let retained: Plan = read_json(&plan_path).unwrap();
        session = open(&view, &intent, Some(&retained)).unwrap();
    }
    assert!(session.remove_next().unwrap());
    drop(session);
    // Recover at a new authority epoch with the same original immutable scope.
    authority.advance_epoch(1, 2).unwrap();
    let retained: Plan = read_json(&plan_path).unwrap();
    assert!(open(&view, &intent, Some(&retained)).is_err()); // stale reporting epoch
    assert!(
        Session::open(
            &view.config.state_root,
            &view.config.cgroup_parent,
            2,
            2,
            &intent,
            Some(&view),
            Some(&retained)
        )
        .is_err()
    ); // rolled-back frontier
    let mut resumed = Session::open(
        &view.config.state_root,
        &view.config.cgroup_parent,
        2,
        1,
        &intent,
        Some(&view),
        Some(&retained),
    )
    .unwrap();
    assert!(resumed.is_removed());
    assert!(resumed.remove_next().unwrap());
    drop(resumed);
    retained
        .verify_removed(
            &view.config.state_root,
            &view.config.cgroup_parent,
            &intent,
            Some(&view),
        )
        .unwrap();
    let mut changed = intent.clone();
    changed.release_evidence_sha256 = "ef".repeat(32);
    assert!(retained.validate(&changed, Some(&view)).is_err());
    let mut corrupt = retained.clone();
    if let Metadata::Stopped {
        original_manifest, ..
    } = &mut corrupt.metadata
    {
        original_manifest.as_mut().unwrap().start.memory_mib += 128;
    }
    assert!(corrupt.validate(&intent, Some(&view)).is_err());
    assert_eq!(
        fs::read(cold.directory().join("manifest.json")).unwrap(),
        other_manifest
    );
    assert_eq!(fs::read(cold.record_path()).unwrap(), other_receipt);
    assert!(!original.directory().exists());
}
