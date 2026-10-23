use crate::{
    Error,
    models::{EndEvent, OutputEvent, StreamProblemEvent},
};
use base64::{Engine, engine::general_purpose::STANDARD};
use serde::Serialize;
use std::fmt;
use tokio::time::{Instant, timeout_at};

/// The cursor denotes bytes after this complete event. Persist after consuming it.
#[derive(Serialize)]
#[serde(tag = "event", rename_all = "snake_case")]
pub enum Event {
    Output { cursor: String, data: OutputEvent },
    End { cursor: String, data: EndEvent },
    Gap { data: StreamProblemEvent },
    Error { data: StreamProblemEvent },
}
impl Event {
    pub fn cursor(&self) -> Option<&str> {
        match self {
            Self::Output { cursor, .. } | Self::End { cursor, .. } => Some(cursor),
            _ => None,
        }
    }
    pub fn terminal(&self) -> bool {
        !matches!(self, Self::Output { .. })
    }
}
impl fmt::Debug for Event {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Event").finish_non_exhaustive()
    }
}

/// Bounded decoder for the API's UTF-8 SSE profile. Reconnect explicitly to the
/// same operation with the last consumed cursor. EOF is not command completion.
pub struct EventStream {
    response: reqwest::Response,
    deadline: Instant,
    pending: Vec<u8>,
    position: usize,
    parser: Parser,
    ended: bool,
}
impl fmt::Debug for EventStream {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EventStream").finish_non_exhaustive()
    }
}
impl EventStream {
    pub(super) fn new(response: reqwest::Response, deadline: Instant) -> Self {
        Self {
            response,
            deadline,
            pending: Vec::new(),
            position: 0,
            parser: Parser::default(),
            ended: false,
        }
    }
    pub async fn next(&mut self) -> Result<Option<Event>, Error> {
        if self.ended {
            return Ok(None);
        }
        // Buffered bytes must not bypass the request's total lifetime. The
        // transport starts this deadline before receiving response headers.
        let result = if Instant::now() >= self.deadline {
            Err(Error::Transport)
        } else {
            timeout_at(self.deadline, self.read_next())
                .await
                .unwrap_or(Err(Error::Transport))
        };
        if result.is_err()
            || result
                .as_ref()
                .is_ok_and(|v| v.as_ref().is_none_or(Event::terminal))
        {
            self.ended = true;
        }
        result
    }
    async fn read_next(&mut self) -> Result<Option<Event>, Error> {
        loop {
            while self.position < self.pending.len() {
                let byte = self.pending[self.position];
                self.position += 1;
                if let Some(event) = self.parser.byte(byte)? {
                    return Ok(Some(event));
                }
            }
            match self.response.chunk().await.map_err(|_| Error::Transport)? {
                Some(bytes) => {
                    // Bound even an unexpectedly large transport frame before copying it.
                    if bytes.len() > 262144 {
                        return Err(Error::Protocol);
                    }
                    self.pending = bytes.to_vec();
                    self.position = 0;
                }
                None => return Ok(None), // Discard an incomplete event; never advance its cursor.
            }
        }
    }
}
#[derive(Default)]
struct Parser {
    line: Vec<u8>,
    data: String,
    event: String,
    id: Option<String>,
    frame_bytes: usize,
    after_cr: bool,
    first_line_seen: bool,
}
impl Parser {
    fn byte(&mut self, byte: u8) -> Result<Option<Event>, Error> {
        if self.after_cr && byte == b'\n' {
            self.after_cr = false;
            return Ok(None);
        }
        self.after_cr = byte == b'\r';
        self.frame_bytes += 1;
        if self.frame_bytes > 65536 {
            return Err(Error::Protocol);
        }
        if byte != b'\n' && byte != b'\r' {
            self.line.push(byte);
            return Ok(None);
        }
        let bytes = std::mem::take(&mut self.line);
        let line = std::str::from_utf8(&bytes).map_err(|_| Error::Protocol)?;
        let line = if !self.first_line_seen {
            line.trim_start_matches('\u{feff}')
        } else {
            line
        };
        self.first_line_seen = true;
        if line.is_empty() {
            self.frame_bytes = 0;
            let data = std::mem::take(&mut self.data);
            let kind = std::mem::take(&mut self.event);
            let id = self.id.take();
            if data.is_empty() {
                return Ok(None);
            }
            return decode(&kind, id, data.trim_end_matches('\n')).map(Some);
        }
        if line.starts_with(':') {
            return Ok(None);
        }
        let (field, value) = line.split_once(':').unwrap_or((line, ""));
        let value = value.strip_prefix(' ').unwrap_or(value);
        match field {
            "event" => {
                self.event = value.to_owned();
            }
            "id" if !value.contains('\0') => {
                self.id = Some(value.to_owned());
            }
            "data" => {
                self.data.push_str(value);
                self.data.push('\n');
            }
            _ => (), // Ignore extension/retry fields; reconnect is caller controlled.
        }
        Ok(None)
    }
}
fn decode(kind: &str, id: Option<String>, data: &str) -> Result<Event, Error> {
    let cursor = || {
        id.as_ref()
            .filter(|s| !s.is_empty() && s.len() <= 2048 && !s.chars().any(char::is_control))
            .cloned()
            .ok_or(Error::Protocol)
    };
    match kind {
        "output" => {
            let data: OutputEvent = serde_json::from_str(data).map_err(|_| Error::Protocol)?;
            let bytes = STANDARD
                .decode(&data.data_base64)
                .map_err(|_| Error::Protocol)?;
            if !matches!(data.stream.as_str(), "stdout" | "stderr")
                || bytes.len() > 32768
                || data.offset.checked_add(bytes.len() as u64) != Some(data.next_offset)
                || data.next_offset > data.stored
                || data.stored > 10485760
                || data.stored > data.seen
                || data.at_end != (data.next_offset == data.stored)
                || !data.guest_reported
                || data.truncated != (data.seen > data.stored)
            {
                return Err(Error::Protocol);
            }
            Ok(Event::Output {
                cursor: cursor()?,
                data,
            })
        }
        "end" => {
            let data: EndEvent = serde_json::from_str(data).map_err(|_| Error::Protocol)?;
            if data.reason != "complete"
                || !data.guest_reported
                || [&data.stdout, &data.stderr].into_iter().any(|s| {
                    s.stored > s.seen || s.stored > 10485760 || s.truncated != (s.seen > s.stored)
                })
            {
                return Err(Error::Protocol);
            }
            Ok(Event::End {
                cursor: cursor()?,
                data,
            })
        }
        "gap" | "error" => {
            if id.is_some() {
                return Err(Error::Protocol);
            }
            let data: StreamProblemEvent =
                serde_json::from_str(data).map_err(|_| Error::Protocol)?;
            Ok(if kind == "gap" {
                Event::Gap { data }
            } else {
                Event::Error { data }
            })
        }
        _ => Err(Error::Protocol),
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test(start_paused = true)]
    async fn total_deadline_applies_to_already_buffered_events() -> Result<(), Error> {
        let output = serde_json::json!({
            "stream":"stdout", "offset":0, "next_offset":1, "data_base64":"eA==",
            "at_end":true, "complete":false, "seen":1, "stored":1,
            "truncated":false, "simulated":false, "guest_reported":true,
        });
        let frame = format!("event: output\nid: saved-cursor\ndata: {output}\n\n");
        let response = reqwest::Response::from(axum::http::Response::new(frame.repeat(2)));
        let mut stream = EventStream::new(
            response,
            Instant::now() + std::time::Duration::from_secs(100),
        );
        assert!(matches!(stream.next().await?, Some(Event::Output { .. })));
        tokio::time::advance(std::time::Duration::from_secs(101)).await;
        assert!(matches!(stream.next().await, Err(Error::Transport)));
        assert!(stream.next().await?.is_none());
        Ok(())
    }
    fn frames(bytes: &[u8]) -> Result<Vec<Event>, Error> {
        let mut parser = Parser::default();
        let mut events = Vec::new();
        for byte in bytes {
            if let Some(e) = parser.byte(*byte)? {
                events.push(e);
            }
        }
        Ok(events)
    }
    #[test]
    fn accepts_cr_lf_crlf_comments_bom_and_multiline_json() -> Result<(), Error> {
        for nl in ["\n", "\r", "\r\n"] {
            let text = format!(
                "\u{feff}: keepalive{nl}{nl}event: gap{nl}data: {{\"code\":{nl}data: \"output_expired\"}}{nl}{nl}"
            );
            assert!(matches!(
                frames(text.as_bytes())?.as_slice(),
                [Event::Gap { .. }]
            ));
        }
        Ok(())
    }
    #[test]
    fn incomplete_frame_never_dispatches_and_large_frames_fail() -> Result<(), Error> {
        assert!(frames(b"event: gap\nid: never-persist\ndata: {}").is_ok_and(|v| v.is_empty()));
        assert!(frames(&vec![b'x'; 65537]).is_err());
        assert!(frames(b"event: gap\ndata: {}\n\n").is_err());
        assert!(frames(b"event: gap\nid: forbidden\ndata: {\"code\":\"expired\"}\n\n").is_err());
        Ok(())
    }
}
