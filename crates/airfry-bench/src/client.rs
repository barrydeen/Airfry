//! Minimal WebSocket client helpers shared by every benchmark subcommand.
//!
//! The relay speaks NIP-01 over text frames. Parsing relay frames with `serde_json` on the hot
//! path would dominate the client's CPU budget at 100k+ events/s, so `classify` recognises the
//! handful of frames we care about with cheap prefix scans. Anything it does not recognise is
//! reported as `Other` and ignored.

use futures_util::{SinkExt, StreamExt};
use std::time::Duration;
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

pub type Ws = WebSocketStream<MaybeTlsStream<TcpStream>>;

/// Connect to a relay websocket endpoint.
///
/// TCP_NODELAY is enabled on the client socket: without it, Nagle's algorithm combined with the
/// peer's delayed ACK adds a fixed ~40ms stall to every request/response round trip, which would
/// swamp the real relay latency we are trying to measure (and mask any difference between relays).
pub async fn connect(url: &str) -> Result<Ws, String> {
    let (ws, _resp) = tokio_tungstenite::connect_async(url)
        .await
        .map_err(|e| format!("connect {url}: {e}"))?;
    if let MaybeTlsStream::Plain(tcp) = ws.get_ref() {
        let _ = tcp.set_nodelay(true);
    }
    Ok(ws)
}

/// A relay frame classified by type. Borrowed slices point into the original frame.
#[derive(Debug, PartialEq, Eq)]
pub enum RelayFrame<'a> {
    /// `["OK","<id>",<bool>,"<message>"]`
    Ok { id: &'a str, ok: bool },
    /// `["EVENT","<sub>",{...}]`
    Event,
    /// `["EOSE","<sub>"]`
    Eose(&'a str),
    /// `["CLOSED","<sub>","<message>"]`
    Closed(&'a str),
    /// `["NOTICE","<message>"]`
    Notice,
    /// `["AUTH","<challenge>"]`
    Auth,
    /// `["COUNT","<sub>",{"count":N}]`
    Count(&'a str),
    /// Anything else (unknown frame type or malformed).
    Other,
}

/// Classify a relay frame without allocating.
pub fn classify(frame: &str) -> RelayFrame<'_> {
    if let Some(rest) = frame.strip_prefix("[\"OK\",\"") {
        if let Some(pos) = rest.find("\",") {
            let id = &rest[..pos];
            let after = &rest[pos + 2..];
            return RelayFrame::Ok {
                id,
                ok: after.starts_with("true"),
            };
        }
    }
    if frame.starts_with("[\"EVENT\",") {
        return RelayFrame::Event;
    }
    if let Some(rest) = frame.strip_prefix("[\"EOSE\",\"") {
        if let Some(end) = rest.find('"') {
            return RelayFrame::Eose(&rest[..end]);
        }
    }
    if let Some(rest) = frame.strip_prefix("[\"CLOSED\",\"") {
        if let Some(end) = rest.find('"') {
            return RelayFrame::Closed(&rest[..end]);
        }
    }
    if frame.starts_with("[\"NOTICE\",") {
        return RelayFrame::Notice;
    }
    if frame.starts_with("[\"AUTH\",") {
        return RelayFrame::Auth;
    }
    if let Some(rest) = frame.strip_prefix("[\"COUNT\",\"") {
        if let Some(end) = rest.find('"') {
            return RelayFrame::Count(&rest[..end]);
        }
    }
    RelayFrame::Other
}

/// Build a `["EVENT",{...}]` frame from an already serialized event object.
pub fn event_frame(event_json: &str) -> String {
    let mut out = String::with_capacity(event_json.len() + 16);
    out.push_str("[\"EVENT\",");
    out.push_str(event_json);
    out.push(']');
    out
}

/// Build a `["REQ","<sub>",<filter>...]` frame.
pub fn req_frame(sub_id: &str, filters_json: &[String]) -> String {
    let mut out =
        String::with_capacity(32 + filters_json.iter().map(|f| f.len() + 1).sum::<usize>());
    out.push_str("[\"REQ\",\"");
    out.push_str(sub_id);
    out.push('"');
    for f in filters_json {
        out.push(',');
        out.push_str(f);
    }
    out.push(']');
    out
}

/// Build a `["CLOSE","<sub>"]` frame.
pub fn close_frame(sub_id: &str) -> String {
    format!("[\"CLOSE\",\"{sub_id}\"]")
}

/// Send a text frame, mapping errors to a readable string.
pub async fn send_text<S>(sink: &mut S, frame: String) -> Result<(), String>
where
    S: SinkExt<Message> + Unpin,
    <S as futures_util::Sink<Message>>::Error: std::fmt::Display,
{
    sink.send(Message::Text(frame))
        .await
        .map_err(|e| format!("send: {e}"))
}

/// Extract the text payload of a websocket message, if it is text or utf-8 binary.
pub fn message_text(msg: &Message) -> Option<&str> {
    match msg {
        Message::Text(t) => Some(t.as_str()),
        Message::Binary(b) => std::str::from_utf8(b).ok(),
        _ => None,
    }
}

/// Drain and discard any frames that arrive before the first benchmark frame (the relay sends a
/// NIP-42 `AUTH` challenge immediately on connect).
pub async fn drain_initial<S>(stream: &mut S, budget: Duration) -> usize
where
    S: StreamExt<Item = Result<Message, tokio_tungstenite::tungstenite::Error>> + Unpin,
{
    let deadline = tokio::time::Instant::now() + budget;
    let mut n = 0;
    loop {
        match tokio::time::timeout_at(deadline, stream.next()).await {
            Ok(Some(Ok(_))) => n += 1,
            Ok(Some(Err(_))) | Ok(None) => break,
            Err(_) => break,
        }
    }
    n
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classify_ok_true() {
        let f = "[\"OK\",\"abc\",true,\"\"]";
        assert_eq!(
            classify(f),
            RelayFrame::Ok {
                id: "abc",
                ok: true
            }
        );
    }

    #[test]
    fn classify_ok_false_with_message() {
        let f = "[\"OK\",\"deadbeef\",false,\"invalid: bad signature\"]";
        assert_eq!(
            classify(f),
            RelayFrame::Ok {
                id: "deadbeef",
                ok: false
            }
        );
    }

    #[test]
    fn classify_eose_and_closed() {
        assert_eq!(classify("[\"EOSE\",\"sub-1\"]"), RelayFrame::Eose("sub-1"));
        assert_eq!(
            classify("[\"CLOSED\",\"sub-2\",\"invalid: nope\"]"),
            RelayFrame::Closed("sub-2")
        );
    }

    #[test]
    fn classify_event_notice_auth_count() {
        assert_eq!(
            classify("[\"EVENT\",\"s\",{\"id\":\"00\"}]"),
            RelayFrame::Event
        );
        assert_eq!(classify("[\"NOTICE\",\"hi\"]"), RelayFrame::Notice);
        assert_eq!(classify("[\"AUTH\",\"challenge\"]"), RelayFrame::Auth);
        assert_eq!(
            classify("[\"COUNT\",\"s\",{\"count\":3}]"),
            RelayFrame::Count("s")
        );
        assert_eq!(classify("[\"WEIRD\",1]"), RelayFrame::Other);
        assert_eq!(classify("not json"), RelayFrame::Other);
    }

    #[test]
    fn frame_builders() {
        assert_eq!(
            event_frame("{\"id\":\"00\"}"),
            "[\"EVENT\",{\"id\":\"00\"}]"
        );
        assert_eq!(
            req_frame("s1", &["{\"kinds\":[1]}".to_string()]),
            "[\"REQ\",\"s1\",{\"kinds\":[1]}]"
        );
        assert_eq!(close_frame("s1"), "[\"CLOSE\",\"s1\"]");
    }
}
