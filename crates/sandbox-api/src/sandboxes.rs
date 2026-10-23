//! Sandbox routes.

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::{Json, http::header};
use sandbox_protocol::RequestDigest;
pub use sandbox_protocol::api::{AdmittedResponse, CreateRequest, RequestedResources};
use sandbox_protocol::images::valid_image_digest;
use sandbox_store::admission::{Admission, CreateSandbox, Resources};

use crate::AppState;
use crate::auth::Authenticated;
use crate::headers::RequestKey;
use crate::problem::Problem;

use sandbox_protocol::resources::{MAX_DISK_MIB, MAX_MEMORY_MIB, MAX_VCPU};

/// Check what the service can answer for without touching the database.
fn validate(request: &CreateRequest) -> Result<Resources, Problem> {
    if request
        .name
        .as_ref()
        .is_some_and(|value| value.contains('\0'))
        || request
            .correlation_id
            .as_ref()
            .is_some_and(|value| value.contains('\0'))
    {
        return Err(Problem::BadRequest(
            "name and correlation_id must not contain NUL",
        ));
    }
    let digest = &request.image_digest;
    if !valid_image_digest(digest) {
        return Err(Problem::BadRequest(
            "image_digest must be sha256: followed by 64 lowercase hex characters",
        ));
    }

    let r = &request.resources;
    if r.vcpu < 1 || r.memory_mib < 1 || r.disk_mib < 1 {
        return Err(Problem::BadRequest("resources must be positive"));
    }
    if r.vcpu > MAX_VCPU || r.memory_mib > MAX_MEMORY_MIB || r.disk_mib > MAX_DISK_MIB {
        return Err(Problem::BadRequest(
            "resources exceed the largest supported sandbox",
        ));
    }
    if request.name.as_ref().is_some_and(|n| n.len() > 200) {
        return Err(Problem::BadRequest("name is too long"));
    }

    Ok(Resources {
        vcpu: r.vcpu,
        memory_mib: r.memory_mib,
        disk_mib: r.disk_mib,
    })
}

/// `POST /v1/sandboxes`.
///
/// Returns `202` with a durable operation handle. Acceptance is not
/// completion: the caller inspects the operation until the lifecycle evidence
/// exists.
pub async fn create(
    State(state): State<AppState>,
    caller: Authenticated,
    RequestKey(idempotency_key): RequestKey,
    request: Result<Json<CreateRequest>, axum::extract::rejection::JsonRejection>,
) -> Result<Response, Problem> {
    let Json(request) = request.map_err(Problem::from_json)?;
    let resources = validate(&request)?;
    let digest = RequestDigest::compute("POST", "/v1/sandboxes", &request).map_err(|error| {
        tracing::error!(%error, "could not digest a validated request");
        Problem::Internal
    })?;

    let payload = serde_json::to_value(&request).map_err(|error| {
        tracing::error!(%error, "could not store a validated request");
        Problem::Internal
    })?;

    let admission = state
        .store
        .admit_create_sandbox(
            &CreateSandbox {
                project_id: caller.project_id,
                key_id: caller.key_id,
                idempotency_key,
                request_digest: digest,
                image_digest: request.image_digest.clone(),
                name: request.name.clone(),
                resources,
                payload,
            },
            &state.images,
        )
        .await
        .map_err(|error| {
            tracing::error!(%error, "admission failed");
            Problem::Unavailable
        })?;

    Ok(match admission {
        Admission::ResponseExpired(id) => return Err(Problem::ResponseExpired(id)),
        Admission::ImageDenied => return Err(Problem::ImageDenied),
        Admission::InvalidResources => {
            return Err(Problem::BadRequest(
                "resources must be 1-4 vCPU, 128-8192 MiB memory and 64-65536 MiB disk",
            ));
        }
        Admission::Admitted {
            sandbox_id,
            operation_id,
        } => accepted(&sandbox_id.to_string(), &operation_id.to_string(), "queued"),
        // An identical retry returns the original handle and its status now,
        // never another dispatch — even if the sandbox has since moved on.
        Admission::Existing {
            sandbox_id,
            operation_id,
            status,
        } => accepted(&sandbox_id.to_string(), &operation_id.to_string(), &status),
        Admission::DigestConflict { .. } => {
            return Err(Problem::Conflict(
                "this idempotency key was used for a different request",
            ));
        }
    })
}

pub(crate) fn accepted(sandbox_id: &str, operation_id: &str, status: &str) -> Response {
    let body = Json(AdmittedResponse {
        sandbox_id: sandbox_id.to_owned(),
        operation_id: operation_id.to_owned(),
        status: status.to_owned(),
        status_url: format!("/v1/operations/{operation_id}"),
    });

    let mut response = (StatusCode::ACCEPTED, body).into_response();
    response.headers_mut().insert(
        header::CACHE_CONTROL,
        header::HeaderValue::from_static("no-store"),
    );
    response
}

/// Build the sandbox routes.
pub fn routes() -> axum::Router<AppState> {
    axum::Router::new().route("/v1/sandboxes", axum::routing::post(create))
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used, clippy::unwrap_used)]

    use super::*;

    fn request() -> CreateRequest {
        CreateRequest {
            image_digest: format!("sha256:{}", "a".repeat(64)),
            name: Some("ok".to_owned()),
            resources: RequestedResources {
                vcpu: 2,
                memory_mib: 2048,
                disk_mib: 8192,
            },
            correlation_id: None,
        }
    }

    #[test]
    fn accepts_a_well_formed_request() {
        assert!(validate(&request()).is_ok());
    }

    #[test]
    fn rejects_a_digest_that_is_not_sha256() {
        for digest in [
            "latest",
            "sha256:short",
            "sha512:aaaa",
            &format!("sha256:{}", "z".repeat(64)),
            &format!("sha256:{}", "a".repeat(63)),
        ] {
            let mut r = request();
            r.image_digest = digest.to_owned();
            assert!(validate(&r).is_err(), "accepted image digest {digest:?}");
        }
    }

    #[test]
    fn rejects_a_sandbox_larger_than_the_envelope() {
        let mut r = request();
        r.resources.vcpu = MAX_VCPU + 1;
        assert!(validate(&r).is_err(), "accepted more vCPUs than supported");

        let mut r = request();
        r.resources.memory_mib = MAX_MEMORY_MIB + 1;
        assert!(validate(&r).is_err(), "accepted more memory than supported");
    }

    #[test]
    fn accepts_exactly_the_ceiling() {
        let mut r = request();
        r.resources.vcpu = MAX_VCPU;
        r.resources.memory_mib = MAX_MEMORY_MIB;
        r.resources.disk_mib = MAX_DISK_MIB;
        assert!(validate(&r).is_ok(), "the documented maximum was rejected");
    }

    #[test]
    fn rejects_nonpositive_resources() {
        for (vcpu, memory, disk) in [
            (0, 2048, 8192),
            (2, 0, 8192),
            (2, 2048, 0),
            (-1, 2048, 8192),
        ] {
            let mut r = request();
            r.resources = RequestedResources {
                vcpu,
                memory_mib: memory,
                disk_mib: disk,
            };
            assert!(validate(&r).is_err(), "accepted {vcpu}/{memory}/{disk}");
        }
    }
}
