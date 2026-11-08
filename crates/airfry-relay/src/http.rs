//! Minimal HTTP/1.1 request parsing and response writing.
//!
//! The relay serves NIP-11 relay information documents and a `/health` endpoint on the same
//! TCP port as the WebSocket endpoint. We therefore have to look at the raw request before
//! handing the stream to the WebSocket upgrade machinery. This module implements just enough
//! of HTTP/1.1 for that: request line + headers, and plain responses.

use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

/// A parsed HTTP request head.
#[derive(Clone, Debug)]
pub struct HttpRequest {
    pub method: String,
    pub path: String,
    pub headers: Vec<(String, String)>,
}

impl HttpRequest {
    /// Case-insensitive header lookup.
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str())
    }

    /// True when the client asked for the NIP-11 relay information document.
    pub fn wants_nostr_json(&self) -> bool {
        self.header("accept")
            .map(|v| v.to_ascii_lowercase().contains("application/nostr+json"))
            .unwrap_or(false)
    }

    /// True when this is a WebSocket upgrade request.
    pub fn is_websocket_upgrade(&self) -> bool {
        self.header("upgrade")
            .map(|v| v.eq_ignore_ascii_case("websocket"))
            .unwrap_or(false)
    }
}

/// Errors produced while reading a request head.
#[derive(Debug)]
pub enum HttpError {
    /// The peer closed the connection before sending a full request.
    Eof,
    /// The request head exceeded the configured limit.
    TooLarge,
    /// The request head was not valid HTTP/1.x.
    Malformed(&'static str),
    /// Underlying IO error.
    Io(io::Error),
}

impl std::fmt::Display for HttpError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            HttpError::Eof => write!(f, "connection closed before request"),
            HttpError::TooLarge => write!(f, "request head too large"),
            HttpError::Malformed(m) => write!(f, "malformed request: {m}"),
            HttpError::Io(e) => write!(f, "io error: {e}"),
        }
    }
}

impl std::error::Error for HttpError {}

impl From<io::Error> for HttpError {
    fn from(e: io::Error) -> Self {
        HttpError::Io(e)
    }
}

/// A parsed HTTP request head plus the raw bytes consumed from the socket.
///
/// `raw` contains the complete request head followed by any bytes that were read past the end
/// of the head. For WebSocket upgrades the whole `raw` buffer must be replayed to the upgrade
/// machinery (see [`PrefixedStream`]) because tungstenite parses the request itself.
pub struct ParsedRequest {
    pub request: HttpRequest,
    /// Bytes read past the end of the request head.
    pub leftover: Vec<u8>,
    /// The full raw byte sequence consumed from the socket (head + leftover).
    pub raw: Vec<u8>,
}

/// Read a single HTTP request head from `stream`.
pub async fn read_request_head<S>(
    stream: &mut S,
    max_bytes: usize,
) -> Result<ParsedRequest, HttpError>
where
    S: AsyncRead + Unpin,
{
    use tokio::io::AsyncReadExt;

    let mut buf: Vec<u8> = Vec::with_capacity(1024);
    let mut chunk = [0u8; 1024];
    loop {
        if let Some(end) = find_head_end(&buf) {
            let head = std::str::from_utf8(&buf[..end])
                .map_err(|_| HttpError::Malformed("non-utf8 head"))?;
            let req = parse_head(head)?;
            let leftover = buf[end..].to_vec();
            return Ok(ParsedRequest {
                request: req,
                leftover,
                raw: buf,
            });
        }
        if buf.len() >= max_bytes {
            return Err(HttpError::TooLarge);
        }
        let n = stream.read(&mut chunk).await?;
        if n == 0 {
            if buf.is_empty() {
                return Err(HttpError::Eof);
            }
            return Err(HttpError::Malformed("truncated head"));
        }
        buf.extend_from_slice(&chunk[..n]);
    }
}

fn find_head_end(buf: &[u8]) -> Option<usize> {
    // Accept both CRLFCRLF and LFLF terminators.
    if let Some(pos) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
        return Some(pos + 4);
    }
    buf.windows(2).position(|w| w == b"\n\n").map(|p| p + 2)
}

fn parse_head(head: &str) -> Result<HttpRequest, HttpError> {
    let mut lines = head.split('\n').map(|l| l.trim_end_matches('\r'));
    let request_line = lines.next().ok_or(HttpError::Malformed("empty request"))?;
    let mut parts = request_line.split_whitespace();
    let method = parts
        .next()
        .ok_or(HttpError::Malformed("missing method"))?
        .to_string();
    let target = parts.next().ok_or(HttpError::Malformed("missing target"))?;
    let version = parts
        .next()
        .ok_or(HttpError::Malformed("missing version"))?;
    if !version.starts_with("HTTP/1.") {
        return Err(HttpError::Malformed("unsupported http version"));
    }
    // Strip query string; we only route on the path.
    let path = target.split('?').next().unwrap_or(target).to_string();
    let mut headers = Vec::new();
    for line in lines {
        if line.is_empty() {
            continue;
        }
        let (name, value) = line
            .split_once(':')
            .ok_or(HttpError::Malformed("header without colon"))?;
        headers.push((name.trim().to_string(), value.trim().to_string()));
    }
    Ok(HttpRequest {
        method,
        path,
        headers,
    })
}

/// A stream that first replays `prefix` bytes and then reads/writes the inner stream.
///
/// Needed because we consume the HTTP request head ourselves before handing the socket to
/// tungstenite; any bytes read past the head must be replayed.
pub struct PrefixedStream<S> {
    prefix: Vec<u8>,
    pos: usize,
    inner: S,
}

impl<S> PrefixedStream<S> {
    pub fn new(prefix: Vec<u8>, inner: S) -> Self {
        PrefixedStream {
            prefix,
            pos: 0,
            inner,
        }
    }
}

impl<S: AsyncRead + Unpin> AsyncRead for PrefixedStream<S> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if self.pos < self.prefix.len() {
            let remaining = self.prefix.len() - self.pos;
            let n = remaining.min(buf.remaining());
            let start = self.pos;
            buf.put_slice(&self.prefix[start..start + n]);
            self.pos += n;
            return Poll::Ready(Ok(()));
        }
        Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

impl<S: AsyncWrite + Unpin> AsyncWrite for PrefixedStream<S> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        data: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.inner).poll_write(cx, data)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

/// A plain HTTP response.
pub struct HttpResponse {
    pub status: u16,
    pub reason: &'static str,
    pub content_type: &'static str,
    pub body: Vec<u8>,
    pub extra_headers: Vec<(String, String)>,
}

impl HttpResponse {
    pub fn new(
        status: u16,
        reason: &'static str,
        content_type: &'static str,
        body: impl Into<Vec<u8>>,
    ) -> Self {
        HttpResponse {
            status,
            reason,
            content_type,
            body: body.into(),
            extra_headers: Vec::new(),
        }
    }

    pub fn json(status: u16, reason: &'static str, body: impl Into<Vec<u8>>) -> Self {
        HttpResponse::new(status, reason, "application/json", body)
    }

    /// NIP-11 relay information document response (`application/nostr+json`).
    pub fn nostr_json(status: u16, reason: &'static str, body: impl Into<Vec<u8>>) -> Self {
        HttpResponse::new(status, reason, "application/nostr+json", body)
    }

    pub fn text(status: u16, reason: &'static str, body: impl Into<Vec<u8>>) -> Self {
        HttpResponse::new(status, reason, "text/plain; charset=utf-8", body)
    }

    pub fn html(status: u16, reason: &'static str, body: impl Into<Vec<u8>>) -> Self {
        HttpResponse::new(status, reason, "text/html; charset=utf-8", body)
    }

    pub fn with_header(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.extra_headers.push((name.into(), value.into()));
        self
    }

    /// Serialize the response to raw bytes.
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(self.body.len() + 256);
        out.extend_from_slice(format!("HTTP/1.1 {} {}\r\n", self.status, self.reason).as_bytes());
        out.extend_from_slice(format!("Content-Type: {}\r\n", self.content_type).as_bytes());
        out.extend_from_slice(format!("Content-Length: {}\r\n", self.body.len()).as_bytes());
        out.extend_from_slice(b"Connection: close\r\n");
        out.extend_from_slice(b"Access-Control-Allow-Origin: *\r\n");
        for (k, v) in &self.extra_headers {
            out.extend_from_slice(format!("{}: {}\r\n", k, v).as_bytes());
        }
        out.extend_from_slice(b"\r\n");
        out.extend_from_slice(&self.body);
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_request() {
        let head = "GET /health HTTP/1.1\r\nHost: x\r\nAccept: application/nostr+json\r\n\r\n";
        let req = parse_head(head).unwrap();
        assert_eq!(req.method, "GET");
        assert_eq!(req.path, "/health");
        assert!(req.wants_nostr_json());
        assert!(!req.is_websocket_upgrade());
    }

    #[test]
    fn detects_upgrade() {
        let head = "GET / HTTP/1.1\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n\r\n";
        let req = parse_head(head).unwrap();
        assert!(req.is_websocket_upgrade());
    }

    #[test]
    fn response_bytes() {
        let r = HttpResponse::json(200, "OK", "{\"a\":1}");
        let b = r.to_bytes();
        let s = String::from_utf8(b).unwrap();
        assert!(s.starts_with("HTTP/1.1 200 OK\r\n"));
        assert!(s.contains("Content-Length: 7\r\n"));
        assert!(s.ends_with("{\"a\":1}"));
    }

    #[test]
    fn nostr_json_content_type() {
        let r = HttpResponse::nostr_json(200, "OK", "{}");
        let s = String::from_utf8(r.to_bytes()).unwrap();
        assert!(s.contains("Content-Type: application/nostr+json\r\n"));
    }

    #[tokio::test]
    async fn reads_head_and_leftover() {
        let raw = b"GET / HTTP/1.1\r\nHost: x\r\n\r\nEXTRA";
        let mut cursor = std::io::Cursor::new(raw.to_vec());
        let parsed = read_request_head(&mut cursor, 4096).await.unwrap();
        assert_eq!(parsed.request.path, "/");
        assert_eq!(parsed.leftover, b"EXTRA");
        assert_eq!(parsed.raw, raw);
    }
}
