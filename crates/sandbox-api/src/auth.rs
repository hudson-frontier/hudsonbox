//! Project bearer token authentication.
//!
//! Every request carries a token or is rejected. There is no unauthenticated
//! path, in any environment, per `docs/auth-design.md`.
//!
//! The checks run in a fixed order and every failure produces the same
//! response, so the reason is visible to us in logs and invisible to a caller
//! probing for valid key identifiers.

use axum::extract::{FromRef, FromRequestParts};
use http::header::AUTHORIZATION;
use http::request::Parts;
use sandbox_protocol::{ProjectId, ProjectToken, TokenKeyId};
use sandbox_store::Store;
use sandbox_store::projects::{ProjectStatus, TokenRecord};
use time::OffsetDateTime;

use crate::problem::Problem;

/// A caller whose token verified, and the project it grants access to.
///
/// Constructing one is the only way to reach a handler that needs
/// authentication, so a route cannot forget the check.
#[derive(Debug, Clone)]
pub struct Authenticated {
    /// The project this request acts within.
    pub project_id: ProjectId,
    /// Which credential was used. Recorded on every admitted operation.
    pub key_id: TokenKeyId,
    // Retain only the verified hash, never the bearer secret.
    hash: sandbox_protocol::TokenHash,
}

/// Why authentication failed. Never sent to the caller — only logged.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Rejection {
    MissingHeader,
    NotBearer,
    Malformed,
    UnknownKeyId,
    WrongSecret,
    Expired,
    Revoked,
    ProjectSuspended,
    ProjectDeleting,
}

/// Verify a presented token against stored metadata.
///
/// Split out from the extractor so it can be tested without an HTTP stack,
/// and so the ordering of the checks is readable in one place.
fn verify(
    token: &ProjectToken,
    record: &TokenRecord,
    now: OffsetDateTime,
) -> Result<(), Rejection> {
    // The secret is compared first and in constant time. Checking expiry or
    // status before the secret would let a caller learn which key identifiers
    // exist by timing or by response latency alone.
    verify_hash(&token.hash(), record, now)
}
fn verify_hash(
    hash: &sandbox_protocol::TokenHash,
    record: &TokenRecord,
    now: OffsetDateTime,
) -> Result<(), Rejection> {
    if !hash.verify(&record.hash) {
        return Err(Rejection::WrongSecret);
    }
    if record.revoked_at.is_some_and(|at| at <= now) {
        return Err(Rejection::Revoked);
    }
    if record.expires_at.is_some_and(|at| at <= now) {
        return Err(Rejection::Expired);
    }
    match record.project_status {
        ProjectStatus::Active => Ok(()),
        ProjectStatus::Suspended => Err(Rejection::ProjectSuspended),
        ProjectStatus::Deleting => Err(Rejection::ProjectDeleting),
    }
}

impl Authenticated {
    pub(crate) async fn admit_upload(
        &self,
        store: &Store,
        request: sandbox_store::uploads::UploadCommand,
    ) -> Result<sandbox_store::uploads::UploadAdmission, Problem> {
        store
            .admit_upload(&request, &self.hash)
            .await
            .map_err(|_| Problem::Unavailable)
    }

    pub(crate) async fn file_view(
        &self,
        store: &Store,
        sandbox: sandbox_protocol::SandboxId,
    ) -> Result<sandbox_store::files::FileView, Problem> {
        use sandbox_store::files::FileAccess;
        match store
            .authorized_file_view(self.project_id, sandbox, &self.key_id, &self.hash)
            .await
            .map_err(|_| Problem::Unavailable)?
        {
            FileAccess::Unauthorized => Err(Problem::Unauthenticated),
            FileAccess::NotFound => Err(Problem::NotFound),
            FileAccess::Ready(view) => Ok(view),
        }
    }

    /// Reauthorize after slow I/O. Key identifiers alone cannot validate a
    /// credential that was removed, revoked, rotated or moved to another project.
    pub async fn revalidate(&self, store: &Store) -> Result<(), Problem> {
        let record = store
            .token_by_key_id(&self.key_id)
            .await
            .map_err(|_| Problem::Unavailable)?
            .ok_or(Problem::Unauthenticated)?;
        if record.project_id != self.project_id || record.key_id != self.key_id {
            return Err(Problem::Unauthenticated);
        }
        verify_hash(&self.hash, &record, OffsetDateTime::now_utc())
            .map_err(|_| Problem::Unauthenticated)
    }
}

impl<S> FromRequestParts<S> for Authenticated
where
    Store: FromRef<S>,
    S: Send + Sync,
{
    type Rejection = Problem;

    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        let store = Store::from_ref(state);

        let (token, rejection) = match extract(parts) {
            Ok(token) => (Some(token), None),
            Err(rejection) => (None, Some(rejection)),
        };

        let outcome = match token {
            None => Err(rejection.unwrap_or(Rejection::Malformed)),
            Some(token) => match store.token_by_key_id(token.key_id()).await {
                Err(error) => {
                    tracing::error!(%error, "token lookup failed");
                    return Err(Problem::Unavailable);
                }
                Ok(None) => Err(Rejection::UnknownKeyId),
                Ok(Some(record)) => {
                    verify(&token, &record, OffsetDateTime::now_utc()).map(|()| Authenticated {
                        project_id: record.project_id,
                        key_id: record.key_id.clone(),
                        hash: token.hash(),
                    })
                }
            },
        };

        match outcome {
            Ok(authenticated) => Ok(authenticated),
            Err(rejection) => {
                // The reason lives in the log, never in the response.
                tracing::info!(?rejection, "rejected an unauthenticated request");
                Err(Problem::Unauthenticated)
            }
        }
    }
}

/// Pull a bearer token out of the request, without touching storage.
fn extract(parts: &Parts) -> Result<ProjectToken, Rejection> {
    let header = parts
        .headers
        .get(AUTHORIZATION)
        .ok_or(Rejection::MissingHeader)?;
    let value = header.to_str().map_err(|_| Rejection::Malformed)?;
    let (scheme, raw) = value.split_once(' ').ok_or(Rejection::NotBearer)?;
    if !scheme.eq_ignore_ascii_case("Bearer") {
        return Err(Rejection::NotBearer);
    }
    let raw = raw.trim();

    ProjectToken::parse(raw).map_err(|_| Rejection::Malformed)
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used, clippy::unwrap_used)]

    use super::*;
    use sandbox_protocol::Id;
    use time::Duration;

    fn record(token: &ProjectToken) -> TokenRecord {
        TokenRecord {
            project_id: ProjectId::generate(),
            project_status: ProjectStatus::Active,
            key_id: token.key_id().clone(),
            hash: token.hash(),
            expires_at: None,
            revoked_at: None,
        }
    }

    fn now() -> OffsetDateTime {
        OffsetDateTime::now_utc()
    }

    #[test]
    fn accepts_a_matching_secret() {
        let token = ProjectToken::generate().expect("generate");
        assert_eq!(verify(&token, &record(&token), now()), Ok(()));
    }

    #[test]
    fn rejects_a_different_secret() {
        let token = ProjectToken::generate().expect("generate");
        let other = ProjectToken::generate().expect("generate");
        let mut stored = record(&token);
        stored.hash = other.hash();

        assert_eq!(verify(&token, &stored, now()), Err(Rejection::WrongSecret));
    }

    #[test]
    fn rejects_an_expired_token() {
        let token = ProjectToken::generate().expect("generate");
        let mut stored = record(&token);
        stored.expires_at = Some(now() - Duration::seconds(1));

        assert_eq!(verify(&token, &stored, now()), Err(Rejection::Expired));
    }

    #[test]
    fn accepts_a_token_that_has_not_expired_yet() {
        let token = ProjectToken::generate().expect("generate");
        let mut stored = record(&token);
        stored.expires_at = Some(now() + Duration::hours(1));

        assert_eq!(verify(&token, &stored, now()), Ok(()));
    }

    #[test]
    fn rejects_a_revoked_token() {
        let token = ProjectToken::generate().expect("generate");
        let mut stored = record(&token);
        stored.revoked_at = Some(now() - Duration::seconds(1));

        assert_eq!(verify(&token, &stored, now()), Err(Rejection::Revoked));
    }

    #[test]
    fn revocation_is_checked_before_expiry() {
        // Both apply. Revocation is the more specific fact and the one an
        // operator will look for in the log.
        let token = ProjectToken::generate().expect("generate");
        let mut stored = record(&token);
        stored.revoked_at = Some(now() - Duration::hours(2));
        stored.expires_at = Some(now() - Duration::hours(1));

        assert_eq!(verify(&token, &stored, now()), Err(Rejection::Revoked));
    }

    #[test]
    fn rejects_a_suspended_project() {
        let token = ProjectToken::generate().expect("generate");
        let mut stored = record(&token);
        stored.project_status = ProjectStatus::Suspended;

        assert_eq!(
            verify(&token, &stored, now()),
            Err(Rejection::ProjectSuspended)
        );
    }

    #[test]
    fn rejects_a_project_being_deleted() {
        let token = ProjectToken::generate().expect("generate");
        let mut stored = record(&token);
        stored.project_status = ProjectStatus::Deleting;

        assert_eq!(
            verify(&token, &stored, now()),
            Err(Rejection::ProjectDeleting)
        );
    }

    #[test]
    fn a_wrong_secret_beats_every_other_check() {
        // A caller must not be able to distinguish "valid secret, suspended
        // project" from "invalid secret" by which failure they trigger.
        let token = ProjectToken::generate().expect("generate");
        let other = ProjectToken::generate().expect("generate");
        let mut stored = record(&token);
        stored.hash = other.hash();
        stored.project_status = ProjectStatus::Suspended;
        stored.revoked_at = Some(now() - Duration::hours(1));

        assert_eq!(verify(&token, &stored, now()), Err(Rejection::WrongSecret));
    }
}
