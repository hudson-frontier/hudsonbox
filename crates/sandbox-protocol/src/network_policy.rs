//! Bounded operator policy and DNS admission model. Host enforcement is separate.
use crate::{AllocationId, HostId};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    net::{IpAddr, Ipv4Addr},
    str::FromStr,
    time::{Duration, Instant},
};

const MAX_RULES: usize = 64;
const MAX_PORTS: usize = 32;
const MAX_NETWORKS: usize = 16;
const MAX_ANSWERS: usize = 16;
const QUERY_TIMEOUT: Duration = Duration::from_secs(5);
const MAX_TTL: u32 = 300;

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum Error {
    #[error("invalid network policy")]
    InvalidPolicy,
    #[error("invalid canonical IPv4 CIDR")]
    InvalidCidr,
    #[error("DNS question is not approved")]
    QuestionDenied,
    #[error("DNS answer is not approved")]
    AnswerDenied,
    #[error("stale network authority")]
    Stale,
    #[error("network authority expired or closed")]
    Inactive,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Cidr {
    address: Ipv4Addr,
    prefix: u8,
}
impl Cidr {
    pub fn contains(self, address: Ipv4Addr) -> bool {
        let mask = u32::MAX
            .checked_shl(32 - u32::from(self.prefix))
            .unwrap_or(0);
        u32::from(address) & mask == u32::from(self.address)
    }
}
impl FromStr for Cidr {
    type Err = Error;
    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let (address, prefix) = value.split_once('/').ok_or(Error::InvalidCidr)?;
        let parsed = Self {
            address: address.parse().map_err(|_| Error::InvalidCidr)?,
            prefix: prefix.parse().map_err(|_| Error::InvalidCidr)?,
        };
        if parsed.prefix > 32 || parsed.to_string() != value || !parsed.contains(parsed.address) {
            return Err(Error::InvalidCidr);
        }
        Ok(parsed)
    }
}
impl fmt::Display for Cidr {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}/{}", self.address, self.prefix)
    }
}
impl Serialize for Cidr {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(self)
    }
}
impl<'de> Deserialize<'de> for Cidr {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        String::deserialize(deserializer)?
            .parse()
            .map_err(serde::de::Error::custom)
    }
}

/// Platform-private, link-local, metadata, documentation, multicast and reserved ranges.
/// Operator-protected public infrastructure is added by the policy's protected list.
fn permanently_denied(address: Ipv4Addr) -> bool {
    const BLOCKED: &[(u32, u8)] = &[
        (0x0000_0000, 8),
        (0x0a00_0000, 8),
        (0x6440_0000, 10),
        (0x7f00_0000, 8),
        (0xa9fe_0000, 16),
        (0xac10_0000, 12),
        (0xc000_0000, 24),
        (0xc000_0200, 24),
        (0xc058_6300, 24),
        (0xc0a8_0000, 16),
        (0xc612_0000, 15),
        (0xc633_6400, 24),
        (0xcb00_7100, 24),
        (0xe000_0000, 4),
        (0xf000_0000, 4),
    ];
    BLOCKED.iter().any(|&(network, prefix)| {
        Cidr {
            address: Ipv4Addr::from(network),
            prefix,
        }
        .contains(address)
    })
}

fn canonical_name(value: &str) -> Result<String, Error> {
    let name = value.strip_suffix('.').unwrap_or(value);
    if name.is_empty()
        || name.len() > 253
        || !name.is_ascii()
        || name.parse::<IpAddr>().is_ok()
        || name.split('.').any(|label| {
            label.is_empty()
                || label.len() > 63
                || label.starts_with('-')
                || label.ends_with('-')
                || !label
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
        })
    {
        return Err(Error::QuestionDenied);
    }
    Ok(name.to_ascii_lowercase())
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum Destination {
    Cidr { cidr: Cidr },
    Name { name: String, networks: Vec<Cidr> },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Rule {
    pub destination: Destination,
    pub ports: Vec<u16>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Policy {
    version: u8,
    protected: Vec<Cidr>,
    egress: Vec<Rule>,
    bandwidth_bps: u64,
}
impl<'de> Deserialize<'de> for Policy {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Input {
            version: u8,
            protected: Vec<Cidr>,
            egress: Vec<Rule>,
            bandwidth_bps: u64,
        }
        let input = Input::deserialize(deserializer)?;
        Self::validate(Self {
            version: input.version,
            protected: input.protected,
            egress: input.egress,
            bandwidth_bps: input.bandwidth_bps,
        })
        .map_err(serde::de::Error::custom)
    }
}
impl Policy {
    pub fn parse(bytes: &[u8]) -> Result<Self, Error> {
        if bytes.len() > 65536 {
            return Err(Error::InvalidPolicy);
        }
        serde_json::from_slice(bytes).map_err(|_| Error::InvalidPolicy)
    }
    fn validate(mut policy: Self) -> Result<Self, Error> {
        if policy.version != 1
            || policy.protected.len() > 64
            || policy.egress.len() > MAX_RULES
            || !(64000..=1_000_000_000).contains(&policy.bandwidth_bps)
        {
            return Err(Error::InvalidPolicy);
        }
        let mut names = BTreeSet::new();
        for rule in &mut policy.egress {
            if rule.ports.is_empty()
                || rule.ports.len() > MAX_PORTS
                || rule.ports.iter().any(|port| matches!(port, 0 | 53 | 853))
            {
                return Err(Error::InvalidPolicy);
            }
            rule.ports.sort_unstable();
            if rule.ports.windows(2).any(|pair| pair[0] == pair[1]) {
                return Err(Error::InvalidPolicy);
            }
            if let Destination::Name { name, networks } = &mut rule.destination {
                *name = canonical_name(name).map_err(|_| Error::InvalidPolicy)?;
                if !names.insert(name.clone())
                    || networks.is_empty()
                    || networks.len() > MAX_NETWORKS
                {
                    return Err(Error::InvalidPolicy);
                }
                networks.sort_unstable();
                if networks.windows(2).any(|pair| pair[0] == pair[1]) {
                    return Err(Error::InvalidPolicy);
                }
            }
        }
        Ok(policy)
    }
    pub fn rules(&self) -> &[Rule] {
        &self.egress
    }
    pub fn protected(&self) -> &[Cidr] {
        &self.protected
    }
    pub fn bandwidth_bps(&self) -> u64 {
        self.bandwidth_bps
    }
    fn permitted_address(&self, address: Ipv4Addr) -> bool {
        !permanently_denied(address) && !self.protected.iter().any(|cidr| cidr.contains(address))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Scope {
    pub host_id: HostId,
    pub allocation_id: AllocationId,
    pub host_epoch: u64,
    pub generation: u64,
    pub boot_id: String,
}

/// A question approved before upstream resolution. Its fields cannot be forged by callers.
#[derive(Debug)]
pub struct Question {
    scope: Scope,
    revision: u64,
    serial: u64,
    name: String,
    expires_at: Instant,
}
impl Question {
    pub fn name(&self) -> &str {
        &self.name
    }
}

#[derive(Debug, Clone)]
struct Binding {
    addresses: BTreeSet<Ipv4Addr>,
    ports: Vec<u16>,
    expires_at: Instant,
}

/// Deterministic admission model for a future host resolver/firewall adapter.
/// An adapter must install changes atomically before publishing a DNS response.
#[derive(Debug)]
pub struct Admission {
    scope: Scope,
    revision: u64,
    serial: u64,
    policy: Policy,
    deadline: Instant,
    closed: bool,
    pending: BTreeMap<String, u64>,
    bindings: BTreeMap<String, Binding>,
}
impl Admission {
    pub fn new(
        scope: Scope,
        revision: u64,
        policy: Policy,
        deadline: Instant,
        now: Instant,
    ) -> Result<Self, Error> {
        if revision == 0
            || scope.host_epoch == 0
            || scope.generation == 0
            || scope.boot_id.is_empty()
            || scope.boot_id.len() > 128
            || scope.boot_id.chars().any(char::is_control)
        {
            return Err(Error::InvalidPolicy);
        }
        if deadline <= now {
            return Err(Error::Inactive);
        }
        Ok(Self {
            scope,
            revision,
            serial: 0,
            policy,
            deadline,
            closed: false,
            pending: BTreeMap::new(),
            bindings: BTreeMap::new(),
        })
    }
    fn active(&self, now: Instant) -> Result<(), Error> {
        if self.closed || now >= self.deadline {
            Err(Error::Inactive)
        } else {
            Ok(())
        }
    }
    pub fn question(&mut self, name: &str, now: Instant) -> Result<Question, Error> {
        self.active(now)?;
        let name = canonical_name(name)?;
        if !self.policy.egress.iter().any(|rule| {
            matches!(
                &rule.destination, Destination::Name { name: approved, .. } if approved == &name
            )
        }) {
            return Err(Error::QuestionDenied);
        }
        self.serial = self.serial.checked_add(1).ok_or(Error::Stale)?;
        self.pending.insert(name.clone(), self.serial);
        Ok(Question {
            scope: self.scope.clone(),
            revision: self.revision,
            serial: self.serial,
            name,
            expires_at: (now + QUERY_TIMEOUT).min(self.deadline),
        })
    }
    /// TTL is the minimum across the validated answer's entire alias/address chain.
    /// A denied current answer removes prior pins; stale answers cannot change newer pins.
    pub fn answer(
        &mut self,
        question: Question,
        addresses: &[IpAddr],
        ttl_seconds: u32,
        now: Instant,
    ) -> Result<(), Error> {
        self.active(now)?;
        if question.scope != self.scope
            || question.revision != self.revision
            || self.pending.get(&question.name) != Some(&question.serial)
        {
            return Err(Error::Stale);
        }
        self.pending.remove(&question.name);
        self.bindings.remove(&question.name);
        if now >= question.expires_at
            || ttl_seconds == 0
            || addresses.is_empty()
            || addresses.len() > MAX_ANSWERS
        {
            return Err(Error::AnswerDenied);
        }
        let rule = self
            .policy
            .egress
            .iter()
            .find(|rule| {
                matches!(
                    &rule.destination, Destination::Name { name, .. } if name == &question.name
                )
            })
            .ok_or(Error::Stale)?;
        let Destination::Name { networks, .. } = &rule.destination else {
            return Err(Error::Stale);
        };
        let mut approved = BTreeSet::new();
        for address in addresses {
            let IpAddr::V4(address) = address else {
                return Err(Error::AnswerDenied);
            };
            if !self.policy.permitted_address(*address)
                || !networks.iter().any(|cidr| cidr.contains(*address))
            {
                return Err(Error::AnswerDenied);
            }
            approved.insert(*address);
        }
        self.bindings.insert(
            question.name,
            Binding {
                addresses: approved,
                ports: rule.ports.clone(),
                expires_at: (now + Duration::from_secs(u64::from(ttl_seconds.min(MAX_TTL))))
                    .min(self.deadline),
            },
        );
        Ok(())
    }
    pub fn allows_tcp(&self, address: IpAddr, port: u16, now: Instant) -> bool {
        if self.active(now).is_err() || matches!(port, 0 | 53 | 853) {
            return false;
        }
        let IpAddr::V4(address) = address else {
            return false;
        };
        if !self.policy.permitted_address(address) {
            return false;
        }
        self.policy.egress.iter().any(|rule| {
            rule.ports.contains(&port)
                && matches!(
                    rule.destination, Destination::Cidr { cidr } if cidr.contains(address)
                )
        }) || self.bindings.values().any(|binding| {
            now < binding.expires_at
                && binding.ports.contains(&port)
                && binding.addresses.contains(&address)
        })
    }
    pub fn replace(&mut self, policy: Policy, revision: u64, now: Instant) -> Result<(), Error> {
        self.active(now)?;
        if revision <= self.revision {
            return Err(Error::Stale);
        }
        self.policy = policy;
        self.revision = revision;
        self.pending.clear();
        self.bindings.clear();
        Ok(())
    }
    pub fn close(&mut self) {
        self.closed = true;
        self.pending.clear();
        self.bindings.clear();
    }
}

#[cfg(test)]
mod tests;
