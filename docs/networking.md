# Sandbox networking

Status: versioned policy parsing and a deterministic DNS admission model are implemented. Host namespace, firewall, resolver transport and bandwidth enforcement remain unimplemented. This document owns egress policy, name resolution, ingress, and bandwidth limits for sandboxes. [Architecture](architecture.md#isolation-and-data-protection) owns where enforcement sits in the system; [threat model](threat-model.md) owns what these rules are defending against.

Networking was previously described only as "deny-by-default egress" spread across other documents, with no grammar, no resolver design, and no position on inbound connections. This document is that position.

## Enforcement point

All of it is enforced on the **host**, by the supervisor, outside the sandbox. Nothing inside the guest participates in its own policy. A sandbox has one virtual interface on a per-sandbox network namespace; nftables rules attached to that namespace decide what leaves it.

Guest root does not change this. [Decision 0003](decisions/0003-guest-root-with-our-kernel.md) gives customers root inside their own VM, which reaches nothing in this document.

## Egress

**Deny by default. Literal-IP grants and DNS grants are explicit, separate permissions.**

```text
allow  151.101.0.0/16      tcp/443     # Fastly — PyPI, crates.io
allow  140.82.112.0/20     tcp/443     # GitHub
allow  151.101.0.0/16      tcp/80      # Debian mirrors
deny   169.254.0.0/16                  # cloud metadata — never allowlistable
deny   10.0.0.0/8, 172.16.0.0/12, 192.168.0.0/16
deny   0.0.0.0/0                       # everything else
```

An explicit literal-IP grant permits its CIDR and TCP ports independently of DNS. A DNS grant names one exact approved hostname, its TCP ports and an IPv4 CIDR ceiling. It permits only addresses returned by the host resolver inside that ceiling, for the admitted TTL. A ceiling alone grants no packet access. The two grant types form a union; an overlapping literal-IP grant intentionally permits its range after a DNS binding expires. Use only a DNS grant when TTL-limited access is required. [Decision 0005](decisions/0005-explicit-ip-and-dns-egress-grants.md) resolves the earlier conflict between static CIDR permission and resolver-created pins.

Four destinations are permanently denied and cannot be added to any allowlist: the cloud metadata address, the platform's database and object storage, the supervisor's control channel, and every other sandbox's address range. The operator's allowlist is applied after those denials, never over them.

Ship a default allowlist covering the common package registries, built from the IP ranges those services publish. Operators extend it. Those ranges move, so the refresh mechanism is a real piece of work, not a config file written once — see the open decisions.

IPv6 is disabled on the sandbox interface in the first release. A half-enforced second address family is a bypass, and enabling it later is additive.

## Name resolution

**The guest gets one resolver: ours, on the host. It answers only for approved names and refuses everything else.**

The guest's `resolv.conf` points at the host resolver, and outbound port 53 to anywhere else is denied. Guest changes to this file cannot widen packet permission or make the host forward an unapproved question.

The resolver accepts only exact approved names, canonicalized to lowercase without a final dot. Wildcards, arbitrary subdomains, IP literals and extra labels do not match. It creates a fresh canonical upstream A question; guest-supplied DNS options or arbitrary query bytes are never forwarded. A query for anything else is refused before upstream work. Only validated answer addresses inside that name's ceiling can become pins.

Refusing rather than forwarding is the point. A forwarding resolver is an exfiltration channel: a guest asks for `<stolen-secret>.attacker.example`, the resolver dutifully walks the delegation chain, and the attacker's nameserver receives the secret without a single outbound connection appearing in the firewall log. Our resolver never forwards a name it does not already allow, so the query dies on the host.

Pinning lets approved addresses rotate inside their operator-selected ceiling. Any protected, IPv6, out-of-ceiling, empty or oversized answer refuses the complete response and removes that name's prior pins. The TTL is the minimum across the validated alias/address chain, capped at five minutes and the allocation's host-provided deadline. Zero TTL does not grant access. A later question supersedes an earlier pending question for the same name. Late answers, older policy revisions and different host/allocation/epoch/generation/boot identities cannot restore permission.

The host adapter must atomically install the current policy/pins before returning a successful DNS response and must fail closed if installation fails. Policy replacement clears old pins and pending questions. Closing allocation authority is irreversible for that admission instance. A restart begins without cached DNS grants. Established flows must remain subject to current destination permission; connection tracking cannot preserve access after a DNS grant expires or policy is revoked.

External TCP/UDP port 53 and TCP port 853 are reserved from egress grants. The host resolver is a separate narrowly controlled endpoint. Encrypted application traffic to an explicitly allowed HTTPS endpoint can carry a tunnel, including DNS-over-HTTPS; this policy does not inspect TLS or identify an application protocol inside allowed traffic.

## Version 1 policy model

The [shared policy implementation](../crates/sandbox-protocol/src/network_policy.rs) validates this operator document and models DNS admissions. Supervisor configuration and host enforcement remain the next integration step. This example uses a documentation address for the protected platform endpoint; an operator must supply the actual installation's protected addresses, including public infrastructure endpoints.

```json
{
  "version": 1,
  "protected": ["203.0.113.9/32"],
  "bandwidth_bps": 1000000,
  "egress": [
    {
      "destination": {
        "kind": "name",
        "name": "github.com",
        "networks": ["140.82.112.0/20"]
      },
      "ports": [443]
    }
  ]
}
```

CIDRs must be canonical IPv4 network addresses. Unknown or duplicate object fields, unsupported versions, repeated ports/names/ceiling entries, invalid names and invalid bounds fail. Input is bounded to 64 KiB, 64 egress rules, 64 additional protected ranges, 32 TCP ports per rule and 16 ceilings per DNS name. Each DNS answer has at most 16 address records; a question has a five-second completion window. Pending questions and pins are bounded by the approved names. IPv6 and the built-in private, loopback, link-local, metadata, documentation, benchmarking, multicast and reserved ranges remain denied even under `0.0.0.0/0`. Additional protected ranges take the same precedence.

## Ingress

**Nothing reaches a port inside a sandbox in the first release.**

Sandboxes make outbound connections. They receive none. There is no proxy, no per-port URL, and no published hostname.

This is a real limitation and worth stating plainly: a customer can run a web server inside a sandbox, and nobody — including them — can open it. Long-running services are supported in the sense that a process may outlive the request that started it, not in the sense that it is reachable.

Ingress is deferred rather than rejected. It needs its own authentication model, its own threat analysis for guest-controlled content on a routable hostname, and a decision about whether that content shares any origin with the management UI. Revisit when a user has a concrete need; the answer is likely a host proxy issuing one authenticated URL per exposed port, never a bare public hostname.

## Bandwidth

Each sandbox gets a configured bandwidth cap, enforced by the host on its interface. Without one, a single sandbox can saturate the host uplink and degrade every other tenant on that machine, which is a cross-tenant effect even though no data crosses between them.

The version 1 model accepts 64,000 through 1,000,000,000 bits per second. Interface enforcement remains part of the host integration; parsing the value does not apply a cap.

The cap is part of the sandbox's resource reservation alongside CPU, memory, and disk. Accounting for aggregate host bandwidth during placement is deferred.

## What this does not prevent

Stated so the threat model stays honest.

A sandbox can send data to any destination the operator allowed. If GitHub is reachable, a customer's code can push a repository containing whatever it likes. Egress policy controls *where* traffic goes, never what it carries. Preventing a customer from exfiltrating their own data is not a goal — [threat model](threat-model.md#what-we-do-not-promise) says so directly.

Traffic to allowed destinations is also not inspected. There is no TLS interception, and none is planned.

## Acceptance checks

The [unprivileged policy tests](../crates/sandbox-protocol/src/network_policy/tests.rs) cover canonical CIDRs, strict bounded metadata, exact DNS questions, port/ceiling checks, mixed protected answers, TTL expiry, stale replies, policy replacement and irreversible closure. They model admission decisions; they do not create a resolver, forward packets or certify host isolation. Remaining host acceptance requires tests for:

1. A sandbox cannot reach the metadata address, the platform database, object storage, the supervisor channel, or another sandbox — by address, by hostname, and after the guest edits its own `resolv.conf` or `/etc/hosts`.
2. A guest cannot reach an alternate DNS endpoint through a destination/port permission absent from its policy; plaintext DNS can reach only the controlled host resolver. Preserve the encrypted-traffic limitation stated above.
3. A query for an unapproved name is refused and never leaves the host, including queries whose labels encode data.
4. Under a DNS-only grant, an approved name resolves inside its ceiling, gets pinned, and permits the selected port until expiry; an unpinned address is denied. An explicit overlapping literal grant retains its separately authorized access.
5. Egress denial holds over IPv6, over redirects, and over alternate protocols, including while the sandbox is restoring from a snapshot.
6. The bandwidth cap holds under a deliberate saturation attempt, and other sandboxes on that host remain usable.
7. Policy is reapplied on resume before any customer process is released, per [lifecycle](lifecycle.md#resume).

## Open decisions

How the default allowlist is refreshed as published CDN ranges change, and whether that refresh is automatic. Installation defaults within the selected bandwidth range. Whether projects can hold their own allowlist entries or only the installation can. Whether a future ingress design belongs in this document or its own.
