//! Project-scoped asynchronous destruction.
use crate::{
    AppState, auth::Authenticated, headers::RequestKey, problem::Problem, sandboxes::accepted,
};
use axum::{
    Json,
    extract::{Path, State},
    response::Response,
};
pub use sandbox_protocol::api::DestroyRequest;
use sandbox_protocol::{RequestDigest, SandboxId};
use sandbox_store::destroy::{DestroyAdmission, DestroySandbox};

pub async fn destroy(
    State(state): State<AppState>,
    caller: Authenticated,
    path: Result<Path<String>, axum::extract::rejection::PathRejection>,
    RequestKey(key): RequestKey,
    request: Result<Json<DestroyRequest>, axum::extract::rejection::JsonRejection>,
) -> Result<Response, Problem> {
    let Path(id) = path.map_err(|_| Problem::BadRequest("invalid sandbox id"))?;
    let Json(request) = request.map_err(Problem::from_json)?;
    if request
        .correlation_id
        .as_ref()
        .is_some_and(|value| value.contains('\0'))
    {
        return Err(Problem::BadRequest("correlation_id must not contain NUL"));
    }
    let sandbox: SandboxId = id
        .parse()
        .map_err(|_| Problem::BadRequest("that is not a valid sandbox id"))?;
    if request
        .correlation_id
        .as_ref()
        .is_some_and(|v| v.len() > 200)
    {
        return Err(Problem::BadRequest("correlation_id is too long"));
    }
    let path = format!("/v1/sandboxes/{sandbox}/destroy");
    let digest = RequestDigest::compute("POST", &path, &request).map_err(|_| Problem::Internal)?;
    let outcome = state
        .store
        .admit_destroy(&DestroySandbox {
            project_id: caller.project_id,
            sandbox_id: sandbox,
            key_id: caller.key_id,
            idempotency_key: key,
            request_digest: digest,
            correlation_id: request.correlation_id,
        })
        .await
        .map_err(|error| {
            tracing::error!(%error,"destroy admission failed");
            Problem::Unavailable
        })?;
    match outcome {
        DestroyAdmission::ResponseExpired(id) => Err(Problem::ResponseExpired(id)),
        DestroyAdmission::Accepted {
            operation_id,
            status,
        } => Ok(accepted(
            &sandbox.to_string(),
            &operation_id.to_string(),
            &status,
        )),
        DestroyAdmission::NotFound => Err(Problem::NotFound),
        DestroyAdmission::Unauthorized => Err(Problem::Unauthenticated),
        DestroyAdmission::DigestConflict => Err(Problem::Conflict(
            "this idempotency key was used for a different request",
        )),
        DestroyAdmission::Busy(operation) => Err(Problem::TransitionInProgress(operation)),
    }
}

pub fn routes() -> axum::Router<AppState> {
    axum::Router::new().route(
        "/v1/sandboxes/{sandbox_id}/destroy",
        axum::routing::post(destroy),
    )
}
