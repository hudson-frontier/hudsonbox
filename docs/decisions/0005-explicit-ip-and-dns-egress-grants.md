# 0005: explicit literal-IP and DNS egress grants

Status: Accepted
Date: 2026-10-23

## Context

The networking design granted static CIDR/port access while also requiring a
resolver-created address pin to expire with DNS TTL. If the static range already
authorizes the address, removing a DNS pin cannot revoke that permission.
[Issue #109](https://github.com/hudson-frontier/hudsonbox/issues/109) requires this
interaction to be settled before connecting the host network engine.

## Decision

Use two explicit grant types. A literal-IP grant authorizes its CIDR and TCP
ports independently. A DNS grant contains an exact approved name, TCP ports and
CIDR ceilings; only a validated host-resolver answer inside those ceilings can
create a temporary grant. A ceiling alone permits no traffic. Grants form a
union, with permanent and installation-protected destinations always denied.

DNS pins expire at the minimum validated answer-chain TTL, the configured cap
and the host-provided allocation deadline. Scope, policy revision and question
ordering fence late answers. Policy replacement and allocation closure revoke
pins. The [networking contract](../networking.md) owns bounds, host-adapter
requirements and the remaining integration/acceptance work.

## Consequences

Operators can choose stable literal endpoints or TTL-limited names deliberately.
Adding an overlapping literal grant also permits traffic after the name's pin
expires. The host must install changes before disclosing a successful DNS answer
and must enforce current permission on established flows.

IP-based permission does not authenticate the application served at an address.
Allowed HTTPS destinations can carry encrypted tunnels. Exact canonical DNS
questions prevent arbitrary name forwarding; they do not inspect allowed TLS
traffic. Host networking remains necessary to enforce these decisions against
guest root.

## Alternatives considered

Static CIDR grants with mandatory DNS pins leave literal-address workloads and
expiry behavior ambiguous. Treating every resolved address as an unconditional
grant would let name resolution widen the operator's intended address scope.
TLS interception would introduce a separate trust model and remains outside
the selected runtime contract.
