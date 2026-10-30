//! Supported guest sizes, shared by admission, placement and both host implementations.
//! Host overhead and aggregate capacity are separate from these per-guest bounds.
pub const MIN_VCPU: i32 = 1;
pub const MAX_VCPU: i32 = 4;
pub const MIN_MEMORY_MIB: i64 = 128;
pub const MAX_MEMORY_MIB: i64 = 8192;
pub const MIN_DISK_MIB: i64 = 64;
pub const MAX_DISK_MIB: i64 = 65536;
/// Per-allocation host memory beyond guest RAM: the VMM allowance the real
/// supervisor charges at admission. Database placement charges the same amount.
pub const VMM_MEMORY_OVERHEAD_MIB: i64 = 128;
/// Per-allocation host disk beyond the writable rootfs: the guardian's bounded
/// staged Firecracker, jailer, kernel and bootstrap copies.
pub const ARTIFACT_DISK_OVERHEAD_MIB: i64 = 401;

/// i128 represents both signed database/API inputs and unsigned wire inputs
/// without wrapping negative values or truncating oversized requests.
#[must_use]
pub fn supported(vcpu: i128, memory_mib: i128, disk_mib: i128) -> bool {
    (i128::from(MIN_VCPU)..=i128::from(MAX_VCPU)).contains(&vcpu)
        && (i128::from(MIN_MEMORY_MIB)..=i128::from(MAX_MEMORY_MIB)).contains(&memory_mib)
        && (i128::from(MIN_DISK_MIB)..=i128::from(MAX_DISK_MIB)).contains(&disk_mib)
}

#[cfg(test)]
mod tests {
    use super::supported;
    #[test]
    fn supported_guest_sizes_include_edges_and_reject_outside_each_dimension() {
        assert!(supported(1, 128, 64));
        assert!(supported(4, 8192, 65536));
        for (cpu, memory, disk) in [
            (0, 128, 64),
            (5, 128, 64),
            (1, 127, 64),
            (1, 8193, 64),
            (1, 128, 63),
            (1, 128, 65537),
            (-1, 128, 64),
            (1, -1, 64),
            (1, 128, -1),
            (1, i128::from(u64::MAX), 64),
            (1, 128, i128::from(u64::MAX)),
        ] {
            assert!(
                !supported(cpu, memory, disk),
                "accepted unsupported guest size {cpu}/{memory}/{disk}"
            );
        }
    }
}
