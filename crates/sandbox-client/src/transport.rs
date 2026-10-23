use crate::{Client, Error, EventStream, models::ProblemBody};
use reqwest::{
    Method, RequestBuilder, Response,
    header::{HeaderMap, HeaderValue},
};
use serde::{Serialize, de::DeserializeOwned};
use std::{fmt, time::Duration};

/// Verified framing metadata; bytes remain workload-controlled. Never print Debug as output.
#[derive(Serialize)]
pub struct RangeChunk {
    #[serde(skip)]
    pub bytes: Vec<u8>,
    pub offset: u64,
    pub next_offset: u64,
    pub size: u64,
    pub eof: bool,
    pub simulated: bool,
    pub seen: Option<u64>,
    pub truncated: Option<bool>,
    pub sha256: Option<String>,
    pub guest_reported: Option<bool>,
}
impl fmt::Debug for RangeChunk {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RangeChunk")
            .field("bytes", &self.bytes.len())
            .field("offset", &self.offset)
            .finish_non_exhaustive()
    }
}
impl Client {
    pub(super) fn request(
        &self,
        method: Method,
        segments: &[&str],
    ) -> Result<RequestBuilder, Error> {
        if segments.iter().any(|s| {
            s.is_empty()
                || s.len() > 256
                || matches!(*s, "." | "..")
                || s.chars().any(|c| c.is_control() || "/\\%?#".contains(c))
        }) {
            return Err(Error::Request);
        }
        let mut url = self.origin.clone();
        url.path_segments_mut()
            .map_err(|_| Error::Config)?
            .clear()
            .extend(segments);
        Ok(self
            .http
            .request(method, url)
            .header("Authorization", self.token.clone())
            .timeout(self.request_timeout))
    }
    pub(super) fn query(
        &self,
        request: RequestBuilder,
        name: &str,
        value: &str,
    ) -> Result<RequestBuilder, Error> {
        let max = if name == "path" { 4096 } else { 2048 };
        if value.len() > max || value.chars().any(char::is_control) {
            return Err(Error::Request);
        }
        Ok(request.query(&[(name, value)]))
    }
    pub(super) fn header(
        &self,
        request: RequestBuilder,
        name: &'static str,
        value: &str,
    ) -> Result<RequestBuilder, Error> {
        let max = match name {
            "X-File-Capture" => 16384,
            "Last-Event-ID" => 2048,
            _ => 128,
        };
        if value.is_empty()
            || value.len() > max
            || value.chars().any(char::is_control)
            || (name == "Idempotency-Key" && !crate::valid_key(value))
        {
            return Err(Error::Request);
        }
        let mut header = HeaderValue::from_str(value).map_err(|_| Error::Request)?;
        header.set_sensitive(true);
        Ok(request.header(name, header))
    }
    pub(super) fn json_body<T: Serialize>(
        &self,
        request: RequestBuilder,
        value: &T,
    ) -> Result<RequestBuilder, Error> {
        let body = serde_json::to_vec(value).map_err(|_| Error::Request)?;
        if body.len() > 65536 {
            return Err(Error::Request);
        }
        Ok(request
            .header("Content-Type", "application/json")
            .body(body))
    }
    pub(super) fn binary_body(
        &self,
        request: RequestBuilder,
        body: &[u8],
    ) -> Result<RequestBuilder, Error> {
        if body.len() > 8 * 1024 * 1024 {
            return Err(Error::Request);
        }
        Ok(request
            .header("Content-Type", "application/octet-stream")
            .body(body.to_vec()))
    }
    async fn send(&self, request: RequestBuilder, status: u16) -> Result<Response, Error> {
        self.send_bounded_problem(request, status, 65536).await
    }
    async fn send_bounded_problem(
        &self,
        request: RequestBuilder,
        status: u16,
        problem_limit: usize,
    ) -> Result<Response, Error> {
        let response = request.send().await.map_err(|_| Error::Transport)?;
        if response.status().as_u16() != status {
            let status = response.status().as_u16();
            let problem = bounded(response, problem_limit)
                .await
                .ok()
                .and_then(|b| serde_json::from_slice::<ProblemBody>(&b).ok())
                .filter(|p| p.status == status);
            let (code, operation_id) = problem
                .map(|p| (Some(p.code), p.operation_id))
                .unwrap_or_default();
            return Err(Error::Http {
                status,
                code,
                operation_id,
            });
        }
        if one(response.headers(), "cache-control")? != "no-store" {
            return Err(Error::Protocol);
        }
        Ok(response)
    }
    pub(super) async fn json<T: DeserializeOwned>(
        &self,
        request: RequestBuilder,
        status: u16,
    ) -> Result<T, Error> {
        self.json_bounded(request, status, 2 * 1024 * 1024).await
    }
    pub(super) async fn json_bounded<T: DeserializeOwned>(
        &self,
        request: RequestBuilder,
        status: u16,
        limit: usize,
    ) -> Result<T, Error> {
        let response = self
            .send_bounded_problem(request, status, limit.min(65536))
            .await?;
        content_type(&response, "application/json")?;
        let body = bounded(response, limit).await?;
        serde_json::from_slice(&body).map_err(|_| Error::Protocol)
    }
    pub(super) async fn empty(&self, request: RequestBuilder, status: u16) -> Result<(), Error> {
        let response = self.send(request, status).await?;
        if !bounded(response, 0).await?.is_empty() {
            return Err(Error::Protocol);
        }
        Ok(())
    }
    pub(super) async fn events(
        &self,
        request: RequestBuilder,
        status: u16,
    ) -> Result<EventStream, Error> {
        // API streams last at most 90 seconds. Idle timeout remains 20 seconds.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(100);
        let response = self
            .send(request.timeout(Duration::from_secs(100)), status)
            .await?;
        content_type(&response, "text/event-stream")?;
        Ok(EventStream::new(response, deadline))
    }
    pub(super) async fn binary(
        &self,
        request: RequestBuilder,
        status: u16,
        prefix: &str,
        offset: u64,
        limit: u64,
    ) -> Result<RangeChunk, Error> {
        if !(1..=32768).contains(&limit) {
            return Err(Error::Request);
        }
        let response = self.send(request, status).await?;
        content_type(&response, "application/octet-stream")?;
        let headers = response.headers();
        let read = |name: &str| one(headers, &format!("x-{prefix}-{name}"));
        let mut chunk = RangeChunk {
            bytes: Vec::new(),
            offset: parse(read("offset")?)?,
            next_offset: parse(read("next-offset")?)?,
            size: parse(read("size")?)?,
            eof: parse(read("eof")?)?,
            simulated: parse(read("simulated")?)?,
            seen: None,
            truncated: None,
            sha256: None,
            guest_reported: None,
        };
        if prefix == "output" {
            chunk.seen = Some(parse(read("seen")?)?);
            chunk.truncated = Some(parse(read("truncated")?)?);
            if chunk.size > 10485760
                || chunk.seen < Some(chunk.size)
                || chunk.truncated != Some(chunk.seen != Some(chunk.size))
            {
                return Err(Error::Protocol);
            }
        } else {
            chunk.sha256 = Some(read("sha256")?.to_owned());
            chunk.guest_reported = Some(parse(read("guest-reported")?)?);
            if chunk.size > 8388608
                || !valid_sha(read("sha256")?)
                || chunk.guest_reported != Some(true)
            {
                return Err(Error::Protocol);
            }
        }
        chunk.bytes = bounded(response, limit as usize).await?;
        if chunk.offset != offset
            || chunk.next_offset
                != chunk
                    .offset
                    .checked_add(chunk.bytes.len() as u64)
                    .ok_or(Error::Protocol)?
            || chunk.next_offset > chunk.size
            || chunk.eof != (chunk.next_offset == chunk.size)
            || (!chunk.eof && chunk.bytes.is_empty())
        {
            return Err(Error::Protocol);
        }
        Ok(chunk)
    }
}
fn one<'a>(headers: &'a HeaderMap, key: &str) -> Result<&'a str, Error> {
    let mut values = headers.get_all(key).iter();
    let value = values
        .next()
        .ok_or(Error::Protocol)?
        .to_str()
        .map_err(|_| Error::Protocol)?;
    if values.next().is_some() {
        return Err(Error::Protocol);
    }
    Ok(value)
}
fn content_type(response: &Response, expected: &str) -> Result<(), Error> {
    if one(response.headers(), "content-type")?
        .split(';')
        .next()
        .map(str::trim)
        != Some(expected)
        || response.headers().contains_key("content-encoding")
    {
        return Err(Error::Protocol);
    }
    Ok(())
}
fn parse<T: std::str::FromStr>(s: &str) -> Result<T, Error> {
    s.parse().map_err(|_| Error::Protocol)
}
pub(super) fn valid_sha(s: &str) -> bool {
    s.len() == 64
        && s.bytes()
            .all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c))
}
async fn bounded(mut response: Response, cap: usize) -> Result<Vec<u8>, Error> {
    if response.content_length().is_some_and(|v| v > cap as u64) {
        return Err(Error::Protocol);
    }
    let mut bytes = Vec::new();
    while let Some(chunk) = response.chunk().await.map_err(|_| Error::Transport)? {
        if chunk.len() > cap.saturating_sub(bytes.len()) {
            return Err(Error::Protocol);
        }
        bytes.extend_from_slice(&chunk);
    }
    Ok(bytes)
}
