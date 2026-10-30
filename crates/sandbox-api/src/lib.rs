//! The HTTP API: authentication, admission, status, and the OpenAPI surface.
//!
//! Routes are added as the operations behind them exist. A route that returns
//! a plausible shape without doing the work is worse than no route, because a
//! client cannot tell the difference.

pub mod auth;
pub mod cancel;
pub mod destroy;
pub mod execute;
pub mod files;
pub mod headers;
pub mod health;
pub mod lists;
pub mod outputs;
pub mod owners;
pub mod problem;
pub mod provision;
pub mod reads;
pub mod sandboxes;
pub mod server;
pub mod streams;
pub mod uploads;

use axum::Router;
use axum::extract::FromRef;
use sandbox_store::Store;

/// Everything a handler needs.
#[derive(Debug, Clone)]
pub struct AppState {
    /// The durable store.
    pub store: Store,
    /// Operator-approved rootfs digests. Required even in development.
    pub images: sandbox_protocol::images::ImageAllowlist,
}

impl FromRef<AppState> for Store {
    fn from_ref(state: &AppState) -> Self {
        state.store.clone()
    }
}

/// Build the router.
pub fn router(state: AppState) -> Router {
    router_with_output(state, None)
}

/// Operator-selected private output reader. Never supplied by an HTTP request.
pub fn router_with_output(
    state: AppState,
    reader: Option<std::sync::Arc<dyn outputs::OutputReader>>,
) -> Router {
    router_with_streams(state, reader, None)
}

pub fn router_with_streams(
    state: AppState,
    reader: Option<std::sync::Arc<dyn outputs::OutputReader>>,
    live: Option<std::sync::Arc<dyn streams::live::LiveReader>>,
) -> Router {
    router_with_files(state, reader, live, None)
}

pub fn router_with_files(
    state: AppState,
    reader: Option<std::sync::Arc<dyn outputs::OutputReader>>,
    live: Option<std::sync::Arc<dyn streams::live::LiveReader>>,
    files: Option<std::sync::Arc<dyn files::client::FileReader>>,
) -> Router {
    router_with_uploads(state, reader, live, files, None)
}
pub fn router_with_uploads(
    state: AppState,
    reader: Option<std::sync::Arc<dyn outputs::OutputReader>>,
    live: Option<std::sync::Arc<dyn streams::live::LiveReader>>,
    files: Option<std::sync::Arc<dyn files::client::FileReader>>,
    sources: Option<std::sync::Arc<dyn sandbox_artifacts::sources::SourceBackend>>,
) -> Router {
    let uploads = uploads::routes(state.store.clone(), sources);
    let files = files::routes(state.store.clone(), files);
    let streams = streams::routes(state.store.clone(), live, reader.clone());
    let outputs = outputs::routes(state.store.clone(), reader);
    Router::new()
        .merge(sandboxes::routes())
        .merge(reads::routes())
        .merge(lists::routes())
        .merge(destroy::routes())
        .merge(execute::routes())
        .merge(cancel::routes())
        .with_state(state)
        .merge(outputs)
        .merge(streams)
        .merge(files)
        .merge(uploads)
}
