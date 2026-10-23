#![allow(clippy::unwrap_used)]
use super::*;
use crate::Id;
use serde_json::json;

fn policy() -> Policy {
    Policy::parse(json!({"version":1,"protected":["8.8.8.8/32"],"bandwidth_bps":1000000,
        "egress":[{"destination":{"kind":"name","name":"Packages.Example.","networks":["8.8.0.0/16"]},"ports":[443]}]
    }).to_string().as_bytes()).unwrap()
}
fn admission(now: Instant) -> Admission {
    Admission::new(
        Scope {
            host_id: HostId::generate(),
            allocation_id: AllocationId::generate(),
            host_epoch: 1,
            generation: 1,
            boot_id: "boot-1".into(),
        },
        1,
        policy(),
        now + Duration::from_secs(1000),
        now,
    )
    .unwrap()
}
fn ip(value: &str) -> IpAddr {
    value.parse().unwrap()
}

#[test]
fn cidrs_are_canonical_and_cover_exact_boundaries() {
    for invalid in [
        "8.8.8.1/24",
        "8.8.8.0/024",
        "8.8.8.0/+24",
        "8.8.8.8/33",
        "::/0",
        "8.8.8.8",
    ] {
        assert!(invalid.parse::<Cidr>().is_err(), "{invalid}");
    }
    let all: Cidr = "0.0.0.0/0".parse().unwrap();
    assert!(all.contains(Ipv4Addr::BROADCAST));
    let subnet: Cidr = "8.8.8.0/24".parse().unwrap();
    assert!(subnet.contains("8.8.8.255".parse().unwrap()));
    assert!(!subnet.contains("8.8.9.0".parse().unwrap()));
    assert_eq!(serde_json::to_string(&subnet).unwrap(), "\"8.8.8.0/24\"");
}

#[test]
fn malformed_duplicate_unknown_and_unbounded_policy_fails() {
    let original = serde_json::to_value(policy()).unwrap();
    for change in ["version", "protected", "bandwidth_bps", "egress"] {
        let mut value = original.clone();
        value[change] = match change {
            "version" => json!(2),
            "protected" => json!(["not-cidr"]),
            "bandwidth_bps" => json!(0),
            _ => json!(vec![value["egress"][0].clone(); 65]),
        };
        assert!(Policy::parse(value.to_string().as_bytes()).is_err());
        assert!(serde_json::from_value::<Policy>(value).is_err());
    }
    assert!(
        Policy::parse(
            br#"{"version":1,"version":1,"protected":[],"egress":[],"bandwidth_bps":64000}"#
        )
        .is_err()
    );
    let mut unknown = original.clone();
    unknown["unknown"] = json!(true);
    assert!(Policy::parse(unknown.to_string().as_bytes()).is_err());
    assert!(Policy::parse(&vec![b' '; 65537]).is_err());
    for ports in [
        json!([]),
        json!([0]),
        json!([53]),
        json!([853]),
        json!([443, 443]),
        json!([65536]),
    ] {
        let mut value = original.clone();
        value["egress"][0]["ports"] = ports;
        assert!(Policy::parse(value.to_string().as_bytes()).is_err());
    }
}

#[test]
fn exact_canonical_questions_are_required_before_upstream_work() {
    let now = Instant::now();
    let mut gate = admission(now);
    assert_eq!(
        gate.question("PACKAGES.EXAMPLE.", now).unwrap().name(),
        "packages.example"
    );
    for name in [
        "secret.packages.example",
        "packages.example.attacker.test",
        "packages.example..",
        "*.packages.example",
        "8.8.4.4",
        "páckages.example",
        "packages.example\0",
    ] {
        assert_eq!(gate.question(name, now).unwrap_err(), Error::QuestionDenied);
    }
    assert_eq!(gate.pending.len(), 1);
}

#[test]
fn dns_grants_require_the_name_ceiling_port_and_unexpired_ttl() {
    let now = Instant::now();
    let mut gate = admission(now);
    let target = ip("8.8.4.4");
    assert!(!gate.allows_tcp(target, 443, now));
    let question = gate.question("packages.example", now).unwrap();
    gate.answer(question, &[target], 2, now).unwrap();
    assert!(gate.allows_tcp(target, 443, now));
    assert!(!gate.allows_tcp(target, 80, now));
    assert!(!gate.allows_tcp(ip("8.8.4.5"), 443, now));
    assert!(!gate.allows_tcp(target, 443, now + Duration::from_secs(2)));
}

#[test]
fn mixed_protected_ipv6_and_out_of_ceiling_answers_revoke_prior_pins() {
    let now = Instant::now();
    let mut gate = admission(now);
    let target = ip("8.8.4.4");
    for bad in [
        ip("8.8.8.8"),
        ip("169.254.169.254"),
        ip("10.0.0.1"),
        ip("::ffff:8.8.4.4"),
        ip("1.1.1.1"),
    ] {
        let question = gate.question("packages.example", now).unwrap();
        gate.answer(question, &[target], 60, now).unwrap();
        let question = gate.question("packages.example", now).unwrap();
        assert_eq!(
            gate.answer(question, &[target, bad], 60, now),
            Err(Error::AnswerDenied)
        );
        assert!(!gate.allows_tcp(target, 443, now));
    }
}

#[test]
fn stale_queries_cannot_replace_newer_dns_or_policy_authority() {
    let now = Instant::now();
    let mut gate = admission(now);
    let old = gate.question("packages.example", now).unwrap();
    let new = gate.question("packages.example", now).unwrap();
    gate.answer(new, &[ip("8.8.4.4")], 60, now).unwrap();
    assert_eq!(
        gate.answer(old, &[ip("8.8.4.5")], 60, now),
        Err(Error::Stale)
    );
    assert!(gate.allows_tcp(ip("8.8.4.4"), 443, now));
    let old = gate.question("packages.example", now).unwrap();
    gate.replace(policy(), 2, now).unwrap();
    assert_eq!(
        gate.answer(old, &[ip("8.8.4.4")], 60, now),
        Err(Error::Stale)
    );
    assert!(!gate.allows_tcp(ip("8.8.4.4"), 443, now));
    assert_eq!(gate.replace(policy(), 2, now), Err(Error::Stale));
}

#[test]
fn unanswered_expired_or_zero_ttl_queries_cannot_grant_access() {
    let now = Instant::now();
    let mut gate = admission(now);
    for (addresses, ttl, at) in [
        (vec![], 30, now),
        (vec![ip("8.8.4.4")], 0, now),
        (vec![ip("8.8.4.4")], 30, now + QUERY_TIMEOUT),
    ] {
        let query = gate.question("packages.example", now).unwrap();
        assert_eq!(
            gate.answer(query, &addresses, ttl, at),
            Err(Error::AnswerDenied)
        );
    }
    let query = gate.question("packages.example", now).unwrap();
    assert_eq!(
        gate.answer(query, &[ip("8.8.4.4"); 17], 30, now),
        Err(Error::AnswerDenied)
    );
}

#[test]
fn ttl_cap_deadline_and_close_cannot_be_bypassed() {
    let now = Instant::now();
    let mut gate = admission(now);
    let target = ip("8.8.4.4");
    let query = gate.question("packages.example", now).unwrap();
    gate.answer(query, &[target], u32::MAX, now).unwrap();
    assert!(!gate.allows_tcp(target, 443, now + Duration::from_secs(300)));
    let query = gate.question("packages.example", now).unwrap();
    gate.close();
    assert_eq!(gate.answer(query, &[target], 30, now), Err(Error::Inactive));
    assert_eq!(gate.replace(policy(), 2, now), Err(Error::Inactive));
    assert!(!gate.allows_tcp(target, 443, now));
}

#[test]
fn broad_literal_grants_never_override_platform_denials() {
    let now = Instant::now();
    let mut gate = admission(now);
    let literal=Policy::parse(br#"{"version":1,"protected":["8.8.8.8/32"],"bandwidth_bps":64000,"egress":[{"destination":{"kind":"cidr","cidr":"0.0.0.0/0"},"ports":[443]}]}"#).unwrap();
    gate.replace(literal, 2, now).unwrap();
    assert!(gate.allows_tcp(ip("8.8.4.4"), 443, now));
    for address in [
        "0.1.2.3",
        "10.0.0.1",
        "100.64.0.1",
        "127.0.0.1",
        "169.254.169.254",
        "172.16.0.1",
        "192.0.0.1",
        "192.0.2.1",
        "192.88.99.1",
        "192.168.0.1",
        "198.18.0.1",
        "198.51.100.1",
        "203.0.113.1",
        "224.0.0.1",
        "255.255.255.255",
        "8.8.8.8",
        "::1",
    ] {
        assert!(!gate.allows_tcp(ip(address), 443, now), "{address}");
    }
    assert!(!gate.allows_tcp(ip("8.8.4.4"), 443, now + Duration::from_secs(1000)));
}

#[test]
fn explicit_literal_permission_survives_an_overlapping_dns_ttl() {
    let now = Instant::now();
    let mut gate = admission(now);
    let mut value = serde_json::to_value(policy()).unwrap();
    value["egress"].as_array_mut().unwrap().push(json!({
        "destination":{"kind":"cidr","cidr":"8.8.4.0/24"},"ports":[443]
    }));
    gate.replace(Policy::parse(value.to_string().as_bytes()).unwrap(), 2, now)
        .unwrap();
    let question = gate.question("packages.example", now).unwrap();
    gate.answer(question, &[ip("8.8.4.4")], 1, now).unwrap();
    let later = now + Duration::from_secs(2);
    assert!(gate.allows_tcp(ip("8.8.4.4"), 443, later));
    assert!(!gate.allows_tcp(ip("8.8.5.5"), 443, later));
    assert!(!gate.allows_tcp(ip("8.8.4.4"), 80, later));
}

#[test]
fn every_ownership_field_fences_an_answer_from_another_engine() {
    let now = Instant::now();
    for field in 0..5 {
        let mut old = admission(now);
        let old_question = old.question("packages.example", now).unwrap();
        let mut replacement = old.scope.clone();
        match field {
            0 => replacement.host_id = HostId::generate(),
            1 => replacement.allocation_id = AllocationId::generate(),
            2 => replacement.host_epoch += 1,
            3 => replacement.generation += 1,
            _ => replacement.boot_id = "replacement-boot".into(),
        }
        let mut current =
            Admission::new(replacement, 1, policy(), now + Duration::from_secs(60), now).unwrap();
        let current_question = current.question("packages.example", now).unwrap();
        assert_eq!(
            current.answer(old_question, &[ip("8.8.4.5")], 30, now),
            Err(Error::Stale)
        );
        current
            .answer(current_question, &[ip("8.8.4.4")], 30, now)
            .unwrap();
        assert!(current.allows_tcp(ip("8.8.4.4"), 443, now));
        assert!(!current.allows_tcp(ip("8.8.4.5"), 443, now));
        assert!(!current.allows_tcp(ip("8.8.4.4"), 443, now + Duration::from_secs(60)));
    }
}
