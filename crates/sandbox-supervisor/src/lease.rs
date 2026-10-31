//! An irrevocable allocation deadline. Zero fences all future lease extension.
use anyhow::{Result, ensure};
use std::sync::atomic::{AtomicU64, Ordering};
#[derive(Debug)]
pub struct Deadline(AtomicU64);
impl Deadline {
    pub fn new(expires_unix_ms: i64, wall_ms: i64, boot_ms: u64) -> Result<Self> {
        Ok(Self(AtomicU64::new(Self::convert(
            expires_unix_ms,
            wall_ms,
            boot_ms,
        )?)))
    }
    fn convert(expires: i64, wall: i64, boot: u64) -> Result<u64> {
        let remaining = expires
            .checked_sub(wall)
            .ok_or_else(|| anyhow::anyhow!("invalid lease deadline"))?;
        ensure!(
            (1..=300_000).contains(&remaining),
            "lease must expire within 300 seconds"
        );
        boot.checked_add(remaining as u64)
            .ok_or_else(|| anyhow::anyhow!("lease clock overflow"))
    }
    pub fn value(&self) -> u64 {
        self.0.load(Ordering::SeqCst)
    }
    pub fn stop(&self) {
        self.0.store(0, Ordering::SeqCst);
    }
    /// Called only after the renewal record is durable, using freshly read clocks.
    pub fn extend(&self, prior: u64, expires: i64, wall: u64, boot: u64) -> Result<()> {
        let wall = i64::try_from(wall)?;
        let next = Self::convert(expires, wall, boot)?;
        ensure!(prior > boot && next >= prior, "expired or shortening lease");
        self.0
            .compare_exchange(prior, next, Ordering::SeqCst, Ordering::SeqCst)
            .map_err(|_| anyhow::anyhow!("lease was fenced or changed"))?;
        Ok(())
    }
    /// Rebind a live deadline to a new absolute expiry, which may be shorter.
    /// Used once when a warm member's idle lease becomes its allocation lease,
    /// after that binding is durable. A fenced or expired deadline never revives.
    pub fn replace(&self, prior: u64, expires: i64, wall: u64, boot: u64) -> Result<()> {
        let wall = i64::try_from(wall)?;
        let next = Self::convert(expires, wall, boot)?;
        ensure!(prior > boot && next > boot, "expired lease");
        self.0
            .compare_exchange(prior, next, Ordering::SeqCst, Ordering::SeqCst)
            .map_err(|_| anyhow::anyhow!("lease was fenced or changed"))?;
        Ok(())
    }
    pub fn expire(&self, boot: u64) -> bool {
        let prior = self.value();
        prior != 0
            && (boot >= prior
                && self
                    .0
                    .compare_exchange(prior, 0, Ordering::SeqCst, Ordering::SeqCst)
                    .is_ok())
    }
}
#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;
    #[test]
    fn expired_stopped_or_stale_deadlines_never_revive() {
        let d = Deadline::new(2000, 1000, 100).unwrap();
        let prior = d.value();
        assert_eq!(prior, 1100);
        assert!(!d.expire(1099));
        d.extend(prior, 3000, 1500, 600).unwrap();
        assert_eq!(d.value(), 2100);
        assert!(d.extend(prior, 4000, 1500, 600).is_err());
        assert!(d.expire(2100));
        assert!(d.extend(2100, 5000, 2500, 2200).is_err());
        assert_eq!(d.value(), 0);
        let d = Deadline::new(2000, 1000, 100).unwrap();
        let prior = d.value();
        d.stop();
        assert!(d.extend(prior, 3000, 1000, 101).is_err());
    }
    #[test]
    fn lease_conversion_is_bounded_and_cannot_shorten() {
        assert!(Deadline::new(1000, 1000, 1).is_err());
        assert!(Deadline::new(301001, 1000, 1).is_err());
        assert!(Deadline::new(2000, 1000, u64::MAX).is_err());
        let d = Deadline::new(2000, 1000, 100).unwrap();
        assert!(d.extend(d.value(), 1500, 1000, 100).is_err());
    }
    #[test]
    fn replacement_may_shorten_but_never_revives_a_fence() {
        let d = Deadline::new(200_000, 0, 1_000).unwrap();
        let prior = d.value();
        d.replace(prior, 10_000, 0, 1_000).unwrap();
        assert_eq!(d.value(), 11_000);
        assert!(d.replace(prior, 20_000, 0, 1_000).is_err());
        d.stop();
        assert!(d.replace(0, 20_000, 0, 1_000).is_err());
        assert!(d.replace(11_000, 20_000, 0, 1_000).is_err());
        assert_eq!(d.value(), 0);
        let d = Deadline::new(5_000, 0, 1_000).unwrap();
        assert!(d.replace(d.value(), 400_000, 0, 1_000).is_err());
        assert!(d.replace(d.value(), 1_000, 0, 7_000).is_err());
    }
    #[test]
    fn concurrent_expiry_and_extension_cannot_overwrite_a_fence() {
        for _ in 0..100 {
            let d = std::sync::Arc::new(Deadline::new(2000, 1000, 100).unwrap());
            let old = d.value();
            let other = d.clone();
            let stop = std::thread::spawn(move || {
                other.stop();
            });
            let _ = d.extend(old, 3000, 1000, 101);
            stop.join().unwrap();
            assert_eq!(d.value(), 0);
        }
    }
}
