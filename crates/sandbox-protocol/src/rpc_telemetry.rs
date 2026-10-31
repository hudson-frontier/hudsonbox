//! Fixed transport-status vocabulary. Admission failures are not RPC outcomes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RpcFailure {
    Unavailable,
    DeadlineExceeded,
    Unknown,
}

impl RpcFailure {
    /// Application rejections and successful replies are not transport failures.
    pub fn from_result<T>(result: &Result<T, tonic::Status>) -> Option<Self> {
        match result.as_ref().err()?.code() {
            tonic::Code::Unavailable => Some(Self::Unavailable),
            tonic::Code::DeadlineExceeded => Some(Self::DeadlineExceeded),
            tonic::Code::Unknown => Some(Self::Unknown),
            _ => None,
        }
    }

    #[must_use]
    pub const fn label(self) -> &'static str {
        match self {
            Self::Unavailable => "unavailable",
            Self::DeadlineExceeded => "deadline_exceeded",
            Self::Unknown => "unknown",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn only_transport_statuses_count() {
        assert_eq!(RpcFailure::from_result(&Ok::<_, tonic::Status>(())), None);
        for code in [
            tonic::Code::NotFound,
            tonic::Code::PermissionDenied,
            tonic::Code::FailedPrecondition,
            tonic::Code::ResourceExhausted,
            tonic::Code::InvalidArgument,
            tonic::Code::Unauthenticated,
        ] {
            assert_eq!(
                RpcFailure::from_result(&Err::<(), _>(tonic::Status::new(code, "private detail"))),
                None
            );
        }
        for (code, expected) in [
            (tonic::Code::Unavailable, RpcFailure::Unavailable),
            (tonic::Code::DeadlineExceeded, RpcFailure::DeadlineExceeded),
            (tonic::Code::Unknown, RpcFailure::Unknown),
        ] {
            assert_eq!(
                RpcFailure::from_result(&Err::<(), _>(tonic::Status::new(code, "private detail"))),
                Some(expected)
            );
        }
    }
}
