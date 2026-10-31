//! Authenticated supervisor transport, Linux guardian and durable lifecycle service.

pub mod archive;
pub mod routing;
pub mod transport;

#[cfg(unix)]
pub mod guest;

#[cfg(target_os = "linux")]
pub mod guardian;
pub mod lease;

#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
mod warm_cleanup;
pub mod warm_member;

pub mod identity;

#[cfg(target_os = "linux")]
pub mod host;

pub mod live_output;

pub mod file_downloads;

#[cfg(target_os = "linux")]
pub mod launch_authority;
