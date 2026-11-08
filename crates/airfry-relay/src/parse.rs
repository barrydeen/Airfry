//! Fast client-frame parsing for the ingest hot path.
//!
//! The generic path parses every frame into a `serde_json::Value` tree and then walks it to
//! build an [`Event`]. That costs several allocations and a full tree walk per event. For the
//! benchmark-relevant `EVENT` frame we instead deserialize straight into a flat struct with
//! `serde`, which is a single pass and allocates only the strings the event actually needs.
//!
//! Anything that does not fit the fast shape (extra array elements, wrong types, non-hex ids)
//! falls back to the generic path so error reporting stays precise.

use airfry_core::{hex_array, CoreError, Event};
use serde::Deserialize;

/// The message type of a client frame, detected without building a JSON tree.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FrameKind {
    Event,
    Req,
    Count,
    Close,
    Auth,
    /// Anything else (unknown type, malformed frame, non-array).
    Other,
}

/// Cheaply classify a client frame by its leading `["TYPE"` prefix.
///
/// Returns [`FrameKind::Other`] for anything that is not a JSON array whose first element is a
/// plain ASCII string, which is exactly the set of frames the generic parser must handle.
pub fn frame_kind(raw: &str) -> FrameKind {
    let bytes = raw.as_bytes();
    let mut i = 0usize;
    // Skip leading whitespace, then require '['.
    while i < bytes.len() && bytes[i].is_ascii_whitespace() {
        i += 1;
    }
    if i >= bytes.len() || bytes[i] != b'[' {
        return FrameKind::Other;
    }
    i += 1;
    while i < bytes.len() && bytes[i].is_ascii_whitespace() {
        i += 1;
    }
    if i >= bytes.len() || bytes[i] != b'"' {
        return FrameKind::Other;
    }
    i += 1;
    let start = i;
    while i < bytes.len() && bytes[i] != b'"' {
        // Message types never contain escapes; bail out to the generic path if they do.
        if bytes[i] == b'\\' {
            return FrameKind::Other;
        }
        i += 1;
    }
    if i >= bytes.len() {
        return FrameKind::Other;
    }
    match &raw[start..i] {
        "EVENT" => FrameKind::Event,
        "REQ" => FrameKind::Req,
        "COUNT" => FrameKind::Count,
        "CLOSE" => FrameKind::Close,
        "AUTH" => FrameKind::Auth,
        _ => FrameKind::Other,
    }
}

/// A NIP-01 event as it appears on the wire, deserialized in one pass.
#[derive(Deserialize)]
struct RawEvent {
    id: String,
    pubkey: String,
    created_at: u64,
    kind: u32,
    tags: Vec<Vec<String>>,
    content: String,
    sig: String,
}

/// `["EVENT", {...}]`.
#[derive(Deserialize)]
struct RawEventFrame(String, RawEvent);

/// Parse an `EVENT` frame directly into an [`Event`] plus its hex id.
///
/// Returns `Err` when the frame does not have the canonical two-element shape or when a field
/// is not valid hex / out of range; the caller then falls back to the generic parser.
pub fn parse_event_frame(raw: &str) -> Result<(Event, String), CoreError> {
    let RawEventFrame(kind, ev) = serde_json::from_str(raw).map_err(|e| CoreError::Malformed(e.to_string()))?;
    if kind != "EVENT" {
        return Err(CoreError::Malformed("not an EVENT frame".to_string()));
    }
    let id = hex_array::<32>(&ev.id, "id")?;
    let pubkey = hex_array::<32>(&ev.pubkey, "pubkey")?;
    let sig = hex_array::<64>(&ev.sig, "sig")?;
    let event = Event {
        id,
        pubkey,
        created_at: ev.created_at,
        kind: ev.kind,
        tags: ev.tags,
        content: ev.content,
        sig,
    };
    Ok((event, ev.id))
}

/// Parse a `["CLOSE","<sub_id>"]` frame without building a JSON tree.
pub fn parse_close_frame(raw: &str) -> Option<String> {
    #[derive(Deserialize)]
    struct RawCloseFrame(String, String);
    let RawCloseFrame(kind, sub_id) = serde_json::from_str(raw).ok()?;
    if kind != "CLOSE" {
        return None;
    }
    Some(sub_id)
}

#[cfg(test)]
mod tests {
    use super::*;
    use airfry_core::Keys;

    #[test]
    fn classifies_frames() {
        assert_eq!(frame_kind(r#"["EVENT",{}]"#), FrameKind::Event);
        assert_eq!(frame_kind("  [ \"REQ\", \"s\", {} ]"), FrameKind::Req);
        assert_eq!(frame_kind(r#"["COUNT","c",{}]"#), FrameKind::Count);
        assert_eq!(frame_kind(r#"["CLOSE","s"]"#), FrameKind::Close);
        assert_eq!(frame_kind(r#"["AUTH",{}]"#), FrameKind::Auth);
        assert_eq!(frame_kind(r#"["NOPE",1]"#), FrameKind::Other);
        assert_eq!(frame_kind("not json"), FrameKind::Other);
        assert_eq!(frame_kind("[]"), FrameKind::Other);
        assert_eq!(frame_kind(r#"[1,2]"#), FrameKind::Other);
    }

    #[test]
    fn parses_event_frame() {
        let keys = Keys::from_label("parse-test");
        let ev = keys.sign_event(1_700_000_000, 1, vec![vec!["t".into(), "x".into()]], "hello \"world\"\n");
        let frame = format!("[\"EVENT\",{}]", ev.to_json());
        let (parsed, id) = parse_event_frame(&frame).expect("fast parse");
        assert_eq!(parsed, ev);
        assert_eq!(id, hex::encode(ev.id));
    }

    #[test]
    fn rejects_non_canonical_frames() {
        assert!(parse_event_frame(r#"["EVENT",{}]"#).is_err());
        assert!(parse_event_frame(r#"["EVENT",{"id":"zz"}]"#).is_err());
        assert!(parse_event_frame(r#"["EVENT",{},1]"#).is_err());
        assert!(parse_event_frame("nope").is_err());
    }

    #[test]
    fn parses_close_frame() {
        assert_eq!(parse_close_frame(r#"["CLOSE","abc"]"#).as_deref(), Some("abc"));
        assert_eq!(parse_close_frame(r#"["CLOSE"]"#), None);
        assert_eq!(parse_close_frame(r#"["REQ","abc",{}]"#), None);
    }
}
