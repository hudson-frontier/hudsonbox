//! Project-scoped HTTPS client. Models and requests are generated from OpenAPI.
//! No automatic retries: retain each mutation key and payload across uncertainty.
mod config;
mod files;
pub mod models;
pub mod requests;
mod stream;
mod transport;
pub use files::{Download, read_upload};
use reqwest::header::HeaderValue;
use std::{fmt, path::Path, time::Duration};
pub use stream::{Event, EventStream};
pub use transport::RangeChunk;
use url::Url;

/// Errors deliberately omit URLs, credentials, request bodies and backend titles.
#[derive(thiserror::Error)]
pub enum Error {
    #[error("invalid private client configuration or credential file")]
    Config,
    #[error("invalid or oversized request")]
    Request,
    #[error("transport failed; mutation outcome may be unknown; retain the same key and payload")]
    Transport,
    #[error("HTTP request failed with status {status}")]
    Http {
        status: u16,
        code: Option<String>,
        operation_id: Option<String>,
    },
    #[error("invalid, oversized or inconsistent server response")]
    Protocol,
    #[error("wait deadline reached; the operation continues; poll the same operation")]
    WaitTimeout,
    #[error("local file operation failed; destination must not already exist")]
    File,
    #[error("download integrity verification failed")]
    Integrity,
}
impl fmt::Debug for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}
impl Error {
    /// Preserve an authorized original handle on expired/conflicting responses.
    /// Only canonical public operation IDs are safe to reflect in diagnostics.
    pub fn operation_id(&self) -> Option<&str> {
        let Self::Http {
            operation_id: Some(id),
            ..
        } = self
        else {
            return None;
        };
        let uuid = uuid::Uuid::try_parse(id.strip_prefix("op_")?).ok()?;
        (uuid.get_version_num() == 7
            && uuid.get_variant() == uuid::Variant::RFC4122
            && id == &format!("op_{uuid}"))
            .then_some(id.as_str())
    }
    /// Known static API code safe for diagnostic output; unknown additive values remain
    /// available in the HTTP variant but are never reflected into diagnostics.
    pub fn problem_code(&self) -> Option<&'static str> {
        match self {
            Self::Http {
                code: Some(code), ..
            } => Some(requests::known_problem_code(code).unwrap_or("unrecognized_problem_code")),
            _ => None,
        }
    }
    pub fn kind(&self) -> &'static str {
        match self {
            Self::Config => "configuration",
            Self::Request => "request",
            Self::Transport => "transport",
            Self::Http { .. } => "http",
            Self::Protocol => "protocol",
            Self::WaitTimeout => "wait_timeout",
            Self::File => "file",
            Self::Integrity => "integrity",
        }
    }
}

/// Validated local configuration identity. Contains no credential or TLS material.
/// `origin` is URL-normalized HTTPS (including its trailing slash); `project_id`
/// is the credential file's declared project, not a server authorization proof.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ClientIdentity<'a> {
    pub origin: &'a str,
    pub project_id: &'a str,
}

#[derive(Clone)]
pub struct Client {
    http: reqwest::Client,
    origin: Url,
    project_id: String,
    token: HeaderValue,
    request_timeout: Duration,
}
impl fmt::Debug for Client {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Client").finish_non_exhaustive()
    }
}
impl Client {
    /// Config and credentials are trusted local files, never workload arguments.
    pub fn from_config(path: &Path) -> Result<Self, Error> {
        config::load(path)
    }

    /// Returns the identity loaded by this client without rereading configuration.
    /// Callers own durable profile binding and must compare before recovery or sends.
    /// Credential rotation does not change this identity when origin/project match.
    pub fn identity(&self) -> ClientIdentity<'_> {
        ClientIdentity {
            origin: self.origin.as_str(),
            project_id: &self.project_id,
        }
    }

    /// Read one operation with a caller-selected JSON byte limit (1 byte to 2 MiB).
    /// The limit applies before decoding, including ignored fields and whitespace.
    /// Oversized success bodies are protocol errors; oversized problems retain
    /// their HTTP status without their body. This never retries or mutates.
    pub async fn get_operation_bounded(
        &self,
        args: requests::GetOperation<'_>,
        max_response_bytes: usize,
    ) -> Result<models::OperationBody, Error> {
        if !(1..=2 * 1024 * 1024).contains(&max_response_bytes) {
            return Err(Error::Request);
        }
        let request = self.request(
            reqwest::Method::GET,
            &["v1", "operations", args.operation_id],
        )?;
        self.json_bounded(request, 200, max_response_bytes).await
    }

    /// Polls only this operation. Timeout and disconnect never cancel or resubmit it.
    pub async fn wait(
        &self,
        operation_id: &str,
        deadline: Duration,
    ) -> Result<models::OperationBody, Error> {
        if deadline.is_zero() || deadline > Duration::from_secs(86400) {
            return Err(Error::Request);
        }
        tokio::time::timeout(deadline, async {
            loop {
                let op = self
                    .get_operation(requests::GetOperation { operation_id })
                    .await?;
                if op.operation_id != operation_id {
                    return Err(Error::Protocol);
                }
                match op.status.as_str() {
                    "queued" | "running" => tokio::time::sleep(Duration::from_millis(500)).await,
                    "succeeded" | "failed" | "cancelled" | "unknown" => return Ok(op),
                    _ => return Err(Error::Protocol),
                }
            }
        })
        .await
        .map_err(|_| Error::WaitTimeout)?
    }
}

/// Generate once per logical mutation. Save before sending; reuse after uncertainty.
pub fn new_idempotency_key() -> String {
    uuid::Uuid::new_v4().to_string()
}

pub fn valid_key(value: &str) -> bool {
    (16..=128).contains(&value.len())
        && value
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || b"._-".contains(&c))
}
