//! Core Nostr primitives: events, ids, schnorr signatures, filters and NIP-01 wire messages.
//!
//! This crate is the shared foundation for the relay, the storage engine and the benchmark
//! tooling. It is deliberately dependency-light and allocation-conscious: parsing and
//! verification are the hot path of any relay, so everything here is written to be fast and
//! to avoid unnecessary copies.
//!
//! # Hot-path design notes
//!
//! * `Event::compute_id` hashes the NIP-01 canonical serialization **directly into the SHA-256
//!   state** instead of materialising a `serde_json::Value` and a `String`. The escaping rules
//!   are byte-for-byte identical to `serde_json`'s compact output (verified by a test that
//!   cross-checks against `serde_json::to_string`).
//! * `Event::write_json` / `Event::to_json_bytes` emit compact JSON straight into a caller
//!   supplied buffer with a hex lookup table, so the relay can serialise an event once and fan
//!   the same bytes out to every matching subscriber.
//! * Signature verification uses the process-wide precomputed secp256k1 context
//!   (`secp256k1::SECP256K1`) rather than building a fresh context per call.

use serde::de::{self, Deserialize, Deserializer, MapAccess, Visitor};
use serde::ser::{Serialize, SerializeMap, Serializer};
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};
use std::fmt;

pub mod codec;
pub mod keys;
pub use keys::Keys;

/// Errors produced by this crate.
#[derive(Debug, thiserror::Error)]
pub enum CoreError {
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("invalid hex: {0}")]
    Hex(#[from] hex::FromHexError),
    #[error("invalid field `{field}`: {reason}")]
    Field { field: &'static str, reason: String },
    #[error("invalid signature")]
    BadSignature,
    #[error("event id does not match computed id")]
    IdMismatch,
    #[error("malformed message: {0}")]
    Malformed(String),
    #[error("unknown message type: {0}")]
    UnknownType(String),
}

fn field_err(field: &'static str, reason: impl Into<String>) -> CoreError {
    CoreError::Field {
        field,
        reason: reason.into(),
    }
}

/// Decode a fixed-size hex array.
pub fn hex_array<const N: usize>(s: &str, field: &'static str) -> Result<[u8; N], CoreError> {
    if s.len() != N * 2 {
        return Err(field_err(
            field,
            format!("expected {} hex chars, got {}", N * 2, s.len()),
        ));
    }
    let mut out = [0u8; N];
    hex::decode_to_slice(s, &mut out).map_err(|e| field_err(field, e.to_string()))?;
    Ok(out)
}

const HEX_LOWER: &[u8; 16] = b"0123456789abcdef";

/// Append the lowercase hex encoding of `bytes` to `out` without allocating.
#[inline]
pub fn write_hex(out: &mut Vec<u8>, bytes: &[u8]) {
    out.reserve(bytes.len() * 2);
    for b in bytes {
        out.push(HEX_LOWER[(b >> 4) as usize]);
        out.push(HEX_LOWER[(b & 0x0f) as usize]);
    }
}

/// Append a decimal integer to `out` without allocating.
#[inline]
pub fn write_u64(out: &mut Vec<u8>, mut v: u64) {
    let mut buf = [0u8; 20];
    let mut i = buf.len();
    loop {
        i -= 1;
        buf[i] = b'0' + (v % 10) as u8;
        v /= 10;
        if v == 0 {
            break;
        }
    }
    out.extend_from_slice(&buf[i..]);
}

/// Append a JSON string literal (including quotes) to `out`, matching `serde_json`'s compact
/// escaping exactly: `"` and `\` are escaped, `\b \t \n \f \r` use their short forms, other
/// control characters become `\u00xx`, and all other bytes (including multi-byte UTF-8) are
/// emitted verbatim.
#[inline]
pub fn write_json_string(out: &mut Vec<u8>, s: &str) {
    out.reserve(s.len() + 2);
    out.push(b'"');
    let bytes = s.as_bytes();
    let mut start = 0usize;
    for (i, &b) in bytes.iter().enumerate() {
        let esc: Option<&[u8]> = match b {
            b'"' => Some(b"\\\""),
            b'\\' => Some(b"\\\\"),
            0x08 => Some(b"\\b"),
            0x09 => Some(b"\\t"),
            0x0a => Some(b"\\n"),
            0x0c => Some(b"\\f"),
            0x0d => Some(b"\\r"),
            b if b < 0x20 => None,
            _ => continue,
        };
        if i > start {
            out.extend_from_slice(&bytes[start..i]);
        }
        match esc {
            Some(e) => out.extend_from_slice(e),
            None => {
                out.extend_from_slice(b"\\u00");
                out.push(HEX_LOWER[(b >> 4) as usize]);
                out.push(HEX_LOWER[(b & 0x0f) as usize]);
            }
        }
        start = i + 1;
    }
    if start < bytes.len() {
        out.extend_from_slice(&bytes[start..]);
    }
    out.push(b'"');
}

/// A Nostr event as defined by NIP-01.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Event {
    pub id: [u8; 32],
    pub pubkey: [u8; 32],
    pub created_at: u64,
    pub kind: u32,
    pub tags: Vec<Vec<String>>,
    pub content: String,
    pub sig: [u8; 64],
}

impl Event {
    /// Compute the canonical event id: sha256 over the NIP-01 serialization
    /// `[0, pubkey, created_at, kind, tags, content]`.
    ///
    /// This writes the canonical JSON directly into the hash state; no intermediate `String`
    /// or `serde_json::Value` is allocated.
    pub fn compute_id(&self) -> [u8; 32] {
        let mut buf: Vec<u8> = Vec::with_capacity(self.wire_size());
        self.write_canonical(&mut buf);
        let d = Sha256::digest(&buf);
        let mut out = [0u8; 32];
        out.copy_from_slice(&d);
        out
    }

    /// Append the NIP-01 canonical serialization
    /// `[0,"<pubkey hex>",<created_at>,<kind>,<tags>,<content>]` to `out`.
    ///
    /// This is byte-for-byte identical to `serde_json`'s compact output for the same array
    /// (see the `canonical_id_matches_serde_json` test), but avoids building a `Value` tree.
    pub fn write_canonical(&self, out: &mut Vec<u8>) {
        out.reserve(self.wire_size());
        out.extend_from_slice(b"[0,\"");
        write_hex(out, &self.pubkey);
        out.extend_from_slice(b"\",");
        write_u64(out, self.created_at);
        out.push(b',');
        write_u64(out, self.kind as u64);
        out.push(b',');
        write_tags_json(out, &self.tags);
        out.push(b',');
        write_json_string(out, &self.content);
        out.push(b']');
    }

    /// Verify both the id and the BIP-340 schnorr signature.
    pub fn verify(&self) -> Result<(), CoreError> {
        if self.compute_id() != self.id {
            return Err(CoreError::IdMismatch);
        }
        keys::verify_schnorr(&self.pubkey, &self.id, &self.sig)
    }

    /// Verify only the id (cheap, ~1µs). Lets a relay reject malformed events before paying
    /// for a ~30µs schnorr verification.
    pub fn verify_id(&self) -> Result<(), CoreError> {
        if self.compute_id() != self.id {
            return Err(CoreError::IdMismatch);
        }
        Ok(())
    }

    /// Cheap structural validation that does not touch the signature.
    pub fn validate_shape(&self, max_content: usize) -> Result<(), CoreError> {
        if self.created_at == 0 {
            return Err(field_err("created_at", "must be non-zero"));
        }
        if self.content.len() > max_content {
            return Err(field_err("content", "too large"));
        }
        for tag in &self.tags {
            if tag.is_empty() {
                return Err(field_err("tags", "empty tag"));
            }
        }
        Ok(())
    }

    /// Iterate over the values of every tag with the given name.
    pub fn tag_values<'a>(&'a self, name: &'a str) -> impl Iterator<Item = &'a str> + 'a {
        self.tags
            .iter()
            .filter(move |t| t.first().map(|s| s.as_str()) == Some(name))
            .filter_map(|t| t.get(1).map(|s| s.as_str()))
    }

    /// First value of a tag, if present.
    pub fn tag_value<'a>(&'a self, name: &'a str) -> Option<&'a str> {
        self.tag_values(name).next()
    }

    /// NIP-40 expiration timestamp, if the event carries one.
    pub fn expiration(&self) -> Option<u64> {
        self.tag_value("expiration").and_then(|v| v.parse().ok())
    }

    /// True when the event is expired relative to `now` (unix seconds).
    pub fn is_expired(&self, now: u64) -> bool {
        matches!(self.expiration(), Some(exp) if exp <= now)
    }

    /// Parse an event from a JSON string.
    pub fn from_json_str(s: &str) -> Result<Event, CoreError> {
        let v: Value = serde_json::from_str(s)?;
        Event::from_json_value(&v)
    }

    /// Build an event from a JSON value, validating field types.
    pub fn from_json_value(v: &Value) -> Result<Event, CoreError> {
        let obj = v
            .as_object()
            .ok_or_else(|| field_err("event", "expected object"))?;
        let get_str = |k: &'static str| -> Result<&str, CoreError> {
            obj.get(k)
                .and_then(|x| x.as_str())
                .ok_or_else(|| field_err(k, "missing or not a string"))
        };
        let id = hex_array::<32>(get_str("id")?, "id")?;
        let pubkey = hex_array::<32>(get_str("pubkey")?, "pubkey")?;
        let sig = hex_array::<64>(get_str("sig")?, "sig")?;
        let created_at = obj
            .get("created_at")
            .and_then(|x| x.as_u64())
            .ok_or_else(|| field_err("created_at", "missing or not a u64"))?;
        let kind = obj
            .get("kind")
            .and_then(|x| x.as_u64())
            .ok_or_else(|| field_err("kind", "missing or not a u64"))?;
        if kind > u32::MAX as u64 {
            return Err(field_err("kind", "out of range"));
        }
        let content = obj
            .get("content")
            .and_then(|x| x.as_str())
            .ok_or_else(|| field_err("content", "missing or not a string"))?
            .to_string();
        let tags_v = obj
            .get("tags")
            .and_then(|x| x.as_array())
            .ok_or_else(|| field_err("tags", "missing or not an array"))?;
        let mut tags = Vec::with_capacity(tags_v.len());
        for t in tags_v {
            let arr = t
                .as_array()
                .ok_or_else(|| field_err("tags", "tag is not an array"))?;
            let mut tag = Vec::with_capacity(arr.len());
            for item in arr {
                let s = item
                    .as_str()
                    .ok_or_else(|| field_err("tags", "tag item not a string"))?;
                tag.push(s.to_string());
            }
            tags.push(tag);
        }
        Ok(Event {
            id,
            pubkey,
            created_at,
            kind: kind as u32,
            tags,
            content,
            sig,
        })
    }

    /// Serialize to a JSON value.
    pub fn to_json_value(&self) -> Value {
        let mut m = Map::with_capacity(7);
        m.insert("id".into(), Value::String(hex::encode(self.id)));
        m.insert("pubkey".into(), Value::String(hex::encode(self.pubkey)));
        m.insert("created_at".into(), Value::from(self.created_at));
        m.insert("kind".into(), Value::from(self.kind));
        m.insert(
            "tags".into(),
            Value::Array(
                self.tags
                    .iter()
                    .map(|t| Value::Array(t.iter().cloned().map(Value::String).collect()))
                    .collect(),
            ),
        );
        m.insert("content".into(), Value::String(self.content.clone()));
        m.insert("sig".into(), Value::String(hex::encode(self.sig)));
        Value::Object(m)
    }

    /// Append the compact NIP-01 JSON object for this event to `out`.
    ///
    /// This is the zero-copy fanout primitive: serialise once, then share the resulting bytes
    /// with every subscriber.
    pub fn write_json(&self, out: &mut Vec<u8>) {
        out.reserve(self.wire_size());
        out.extend_from_slice(b"{\"id\":\"");
        write_hex(out, &self.id);
        out.extend_from_slice(b"\",\"pubkey\":\"");
        write_hex(out, &self.pubkey);
        out.extend_from_slice(b"\",\"created_at\":");
        write_u64(out, self.created_at);
        out.extend_from_slice(b",\"kind\":");
        write_u64(out, self.kind as u64);
        out.extend_from_slice(b",\"tags\":");
        write_tags_json(out, &self.tags);
        out.extend_from_slice(b",\"content\":");
        write_json_string(out, &self.content);
        out.extend_from_slice(b",\"sig\":\"");
        write_hex(out, &self.sig);
        out.extend_from_slice(b"\"}");
    }

    /// Compact JSON bytes for this event (single allocation, exact capacity).
    pub fn to_json_bytes(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(self.wire_size());
        self.write_json(&mut out);
        out
    }

    /// Serialize to a compact JSON string.
    pub fn to_json(&self) -> String {
        let bytes = self.to_json_bytes();
        // The writer only emits ASCII plus verbatim UTF-8 from validated `String` fields, so
        // this is always valid UTF-8.
        String::from_utf8(bytes).expect("event json is valid utf-8")
    }

    /// Approximate serialized size, used for accounting and limits.
    pub fn wire_size(&self) -> usize {
        self.content.len()
            + self
                .tags
                .iter()
                .map(|t| t.iter().map(|s| s.len() + 3).sum::<usize>())
                .sum::<usize>()
            + 200
    }
}

/// Append the JSON array-of-arrays representation of `tags` to `out`.
#[inline]
pub fn write_tags_json(out: &mut Vec<u8>, tags: &[Vec<String>]) {
    out.push(b'[');
    for (i, tag) in tags.iter().enumerate() {
        if i > 0 {
            out.push(b',');
        }
        out.push(b'[');
        for (j, item) in tag.iter().enumerate() {
            if j > 0 {
                out.push(b',');
            }
            write_json_string(out, item);
        }
        out.push(b']');
    }
    out.push(b']');
}

impl Serialize for Event {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let mut m = s.serialize_map(Some(7))?;
        m.serialize_entry("id", &hex::encode(self.id))?;
        m.serialize_entry("pubkey", &hex::encode(self.pubkey))?;
        m.serialize_entry("created_at", &self.created_at)?;
        m.serialize_entry("kind", &self.kind)?;
        m.serialize_entry("tags", &self.tags)?;
        m.serialize_entry("content", &self.content)?;
        m.serialize_entry("sig", &hex::encode(self.sig))?;
        m.end()
    }
}

impl<'de> Deserialize<'de> for Event {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Event, D::Error> {
        struct V;
        impl<'de> Visitor<'de> for V {
            type Value = Event;
            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                f.write_str("a nostr event object")
            }
            fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Event, A::Error> {
                let mut id = None;
                let mut pubkey = None;
                let mut created_at = None;
                let mut kind = None;
                let mut tags: Option<Vec<Vec<String>>> = None;
                let mut content = None;
                let mut sig = None;
                while let Some(k) = map.next_key::<String>()? {
                    match k.as_str() {
                        "id" => id = Some(map.next_value::<String>()?),
                        "pubkey" => pubkey = Some(map.next_value::<String>()?),
                        "created_at" => created_at = Some(map.next_value::<u64>()?),
                        "kind" => kind = Some(map.next_value::<u64>()?),
                        "tags" => tags = Some(map.next_value()?),
                        "content" => content = Some(map.next_value::<String>()?),
                        "sig" => sig = Some(map.next_value::<String>()?),
                        _ => {
                            let _ = map.next_value::<de::IgnoredAny>()?;
                        }
                    }
                }
                let conv = |s: Option<String>, f: &'static str| -> Result<String, A::Error> {
                    s.ok_or_else(|| de::Error::missing_field(f))
                };
                let id = hex_array::<32>(&conv(id, "id")?, "id").map_err(de::Error::custom)?;
                let pubkey = hex_array::<32>(&conv(pubkey, "pubkey")?, "pubkey")
                    .map_err(de::Error::custom)?;
                let sig = hex_array::<64>(&conv(sig, "sig")?, "sig").map_err(de::Error::custom)?;
                let kind = kind.ok_or_else(|| de::Error::missing_field("kind"))?;
                if kind > u32::MAX as u64 {
                    return Err(de::Error::custom("kind out of range"));
                }
                Ok(Event {
                    id,
                    pubkey,
                    created_at: created_at.ok_or_else(|| de::Error::missing_field("created_at"))?,
                    kind: kind as u32,
                    tags: tags.ok_or_else(|| de::Error::missing_field("tags"))?,
                    content: conv(content, "content")?,
                    sig,
                })
            }
        }
        d.deserialize_map(V)
    }
}

/// A NIP-01 subscription filter.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Filter {
    pub ids: Option<Vec<[u8; 32]>>,
    pub authors: Option<Vec<[u8; 32]>>,
    pub kinds: Option<Vec<u32>>,
    pub since: Option<u64>,
    pub until: Option<u64>,
    pub limit: Option<usize>,
    /// Single-letter tag filters: (tag name char, allowed values).
    pub tags: Vec<(char, Vec<String>)>,
    pub search: Option<String>,
}

impl Filter {
    /// True when the event satisfies every constraint of this filter.
    pub fn matches(&self, e: &Event) -> bool {
        if let Some(ids) = &self.ids {
            if !ids.iter().any(|i| i == &e.id) {
                return false;
            }
        }
        if let Some(authors) = &self.authors {
            if !authors.iter().any(|a| a == &e.pubkey) {
                return false;
            }
        }
        if let Some(kinds) = &self.kinds {
            if !kinds.contains(&e.kind) {
                return false;
            }
        }
        if let Some(since) = self.since {
            if e.created_at < since {
                return false;
            }
        }
        if let Some(until) = self.until {
            if e.created_at > until {
                return false;
            }
        }
        for (name, values) in &self.tags {
            let mut found = false;
            for t in &e.tags {
                if t.first()
                    .map(|s| s.len() == 1 && s.as_bytes()[0] as char == *name)
                    == Some(true)
                {
                    if let Some(v) = t.get(1) {
                        if values.iter().any(|x| x == v) {
                            found = true;
                            break;
                        }
                    }
                }
            }
            if !found {
                return false;
            }
        }
        if let Some(q) = &self.search {
            if !e.content.to_lowercase().contains(&q.to_lowercase()) {
                return false;
            }
        }
        true
    }

    /// True when the filter has no constraints at all (matches everything).
    pub fn is_empty(&self) -> bool {
        self.ids.is_none()
            && self.authors.is_none()
            && self.kinds.is_none()
            && self.since.is_none()
            && self.until.is_none()
            && self.tags.is_empty()
            && self.search.is_none()
    }

    /// Parse a filter from a JSON value.
    pub fn from_json_value(v: &Value) -> Result<Filter, CoreError> {
        let obj = v
            .as_object()
            .ok_or_else(|| field_err("filter", "expected object"))?;
        let mut f = Filter::default();
        for (k, val) in obj {
            match k.as_str() {
                "ids" => f.ids = Some(parse_hex_list::<32>(val, "ids")?),
                "authors" => f.authors = Some(parse_hex_list::<32>(val, "authors")?),
                "kinds" => {
                    let arr = val
                        .as_array()
                        .ok_or_else(|| field_err("kinds", "expected array"))?;
                    let mut out = Vec::with_capacity(arr.len());
                    for x in arr {
                        let n = x
                            .as_u64()
                            .ok_or_else(|| field_err("kinds", "expected u64"))?;
                        if n > u32::MAX as u64 {
                            return Err(field_err("kinds", "out of range"));
                        }
                        out.push(n as u32);
                    }
                    f.kinds = Some(out);
                }
                "since" => {
                    f.since = Some(
                        val.as_u64()
                            .ok_or_else(|| field_err("since", "expected u64"))?,
                    )
                }
                "until" => {
                    f.until = Some(
                        val.as_u64()
                            .ok_or_else(|| field_err("until", "expected u64"))?,
                    )
                }
                "limit" => {
                    f.limit = Some(
                        val.as_u64()
                            .ok_or_else(|| field_err("limit", "expected u64"))?
                            as usize,
                    )
                }
                "search" => {
                    f.search = Some(
                        val.as_str()
                            .ok_or_else(|| field_err("search", "expected string"))?
                            .to_string(),
                    )
                }
                _ => {
                    if let Some(name) = k.strip_prefix('#') {
                        let mut chars = name.chars();
                        let c = chars
                            .next()
                            .ok_or_else(|| field_err("filter", "empty tag name"))?;
                        if chars.next().is_some() {
                            // multi-letter tag names are not part of NIP-01 filters; ignore
                            continue;
                        }
                        let arr = val
                            .as_array()
                            .ok_or_else(|| field_err("tag", "expected array"))?;
                        let mut vals = Vec::with_capacity(arr.len());
                        for x in arr {
                            vals.push(
                                x.as_str()
                                    .ok_or_else(|| field_err("tag", "expected string"))?
                                    .to_string(),
                            );
                        }
                        f.tags.push((c, vals));
                    }
                }
            }
        }
        Ok(f)
    }

    /// Serialize back to a JSON value (used by COUNT and tests).
    pub fn to_json_value(&self) -> Value {
        let mut m = Map::new();
        if let Some(ids) = &self.ids {
            m.insert(
                "ids".into(),
                Value::Array(ids.iter().map(|i| Value::String(hex::encode(i))).collect()),
            );
        }
        if let Some(a) = &self.authors {
            m.insert(
                "authors".into(),
                Value::Array(a.iter().map(|i| Value::String(hex::encode(i))).collect()),
            );
        }
        if let Some(k) = &self.kinds {
            m.insert(
                "kinds".into(),
                Value::Array(k.iter().map(|x| Value::from(*x)).collect()),
            );
        }
        if let Some(s) = self.since {
            m.insert("since".into(), Value::from(s));
        }
        if let Some(u) = self.until {
            m.insert("until".into(), Value::from(u));
        }
        if let Some(l) = self.limit {
            m.insert("limit".into(), Value::from(l as u64));
        }
        for (name, vals) in &self.tags {
            m.insert(
                format!("#{}", name),
                Value::Array(vals.iter().cloned().map(Value::String).collect()),
            );
        }
        if let Some(s) = &self.search {
            m.insert("search".into(), Value::String(s.clone()));
        }
        Value::Object(m)
    }
}

fn parse_hex_list<const N: usize>(
    v: &Value,
    field: &'static str,
) -> Result<Vec<[u8; N]>, CoreError> {
    let arr = v
        .as_array()
        .ok_or_else(|| field_err(field, "expected array"))?;
    let mut out = Vec::with_capacity(arr.len());
    for x in arr {
        let s = x
            .as_str()
            .ok_or_else(|| field_err(field, "expected string"))?;
        out.push(hex_array::<N>(s, field)?);
    }
    Ok(out)
}

/// A message sent by a client to the relay.
#[derive(Clone, Debug)]
pub enum ClientMessage {
    Event(Event),
    Req {
        sub_id: String,
        filters: Vec<Filter>,
    },
    Close(String),
    Auth(Event),
    Count {
        sub_id: String,
        filters: Vec<Filter>,
    },
}

/// Parse a raw client frame.
pub fn parse_client_message(s: &str) -> Result<ClientMessage, CoreError> {
    let v: Value = serde_json::from_str(s)?;
    let arr = v
        .as_array()
        .ok_or_else(|| CoreError::Malformed("expected array".into()))?;
    let kind = arr
        .first()
        .and_then(|x| x.as_str())
        .ok_or_else(|| CoreError::Malformed("missing message type".into()))?;
    match kind {
        "EVENT" => {
            let ev = arr
                .get(1)
                .ok_or_else(|| CoreError::Malformed("EVENT missing event".into()))?;
            Ok(ClientMessage::Event(Event::from_json_value(ev)?))
        }
        "AUTH" => {
            let ev = arr
                .get(1)
                .ok_or_else(|| CoreError::Malformed("AUTH missing event".into()))?;
            Ok(ClientMessage::Auth(Event::from_json_value(ev)?))
        }
        "REQ" => {
            let sub_id = arr
                .get(1)
                .and_then(|x| x.as_str())
                .ok_or_else(|| CoreError::Malformed("REQ missing subscription id".into()))?
                .to_string();
            let mut filters = Vec::new();
            for f in arr.iter().skip(2) {
                filters.push(Filter::from_json_value(f)?);
            }
            if filters.is_empty() {
                return Err(CoreError::Malformed(
                    "REQ requires at least one filter".into(),
                ));
            }
            Ok(ClientMessage::Req { sub_id, filters })
        }
        "COUNT" => {
            let sub_id = arr
                .get(1)
                .and_then(|x| x.as_str())
                .ok_or_else(|| CoreError::Malformed("COUNT missing subscription id".into()))?
                .to_string();
            let mut filters = Vec::new();
            for f in arr.iter().skip(2) {
                filters.push(Filter::from_json_value(f)?);
            }
            if filters.is_empty() {
                return Err(CoreError::Malformed(
                    "COUNT requires at least one filter".into(),
                ));
            }
            Ok(ClientMessage::Count { sub_id, filters })
        }
        "CLOSE" => {
            let sub_id = arr
                .get(1)
                .and_then(|x| x.as_str())
                .ok_or_else(|| CoreError::Malformed("CLOSE missing subscription id".into()))?
                .to_string();
            Ok(ClientMessage::Close(sub_id))
        }
        other => Err(CoreError::UnknownType(other.to_string())),
    }
}

/// A message sent by the relay to a client.
#[derive(Clone, Debug)]
pub enum RelayMessage {
    Event {
        sub_id: String,
        event: Event,
    },
    Ok {
        id: String,
        ok: bool,
        message: String,
    },
    Eose {
        sub_id: String,
    },
    Closed {
        sub_id: String,
        message: String,
    },
    Notice(String),
    Auth(String),
    Count {
        sub_id: String,
        count: usize,
    },
}

impl RelayMessage {
    /// Append the compact NIP-01 frame for this message to `out`.
    pub fn write_json(&self, out: &mut Vec<u8>) {
        match self {
            RelayMessage::Event { sub_id, event } => {
                out.extend_from_slice(b"[\"EVENT\",");
                write_json_string(out, sub_id);
                out.push(b',');
                event.write_json(out);
                out.push(b']');
            }
            RelayMessage::Ok { id, ok, message } => {
                out.extend_from_slice(b"[\"OK\",");
                write_json_string(out, id);
                out.push(b',');
                out.extend_from_slice(if *ok { b"true" } else { b"false" });
                out.push(b',');
                write_json_string(out, message);
                out.push(b']');
            }
            RelayMessage::Eose { sub_id } => {
                out.extend_from_slice(b"[\"EOSE\",");
                write_json_string(out, sub_id);
                out.push(b']');
            }
            RelayMessage::Closed { sub_id, message } => {
                out.extend_from_slice(b"[\"CLOSED\",");
                write_json_string(out, sub_id);
                out.push(b',');
                write_json_string(out, message);
                out.push(b']');
            }
            RelayMessage::Notice(m) => {
                out.extend_from_slice(b"[\"NOTICE\",");
                write_json_string(out, m);
                out.push(b']');
            }
            RelayMessage::Auth(c) => {
                out.extend_from_slice(b"[\"AUTH\",");
                write_json_string(out, c);
                out.push(b']');
            }
            RelayMessage::Count { sub_id, count } => {
                out.extend_from_slice(b"[\"COUNT\",");
                write_json_string(out, sub_id);
                out.extend_from_slice(b",{\"count\":");
                write_u64(out, *count as u64);
                out.extend_from_slice(b"}]");
            }
        }
    }

    /// Compact NIP-01 frame bytes.
    pub fn to_json_bytes(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(64);
        self.write_json(&mut out);
        out
    }

    /// Serialize to a compact NIP-01 frame.
    pub fn to_json(&self) -> String {
        String::from_utf8(self.to_json_bytes()).expect("relay frame is valid utf-8")
    }
}

/// Build a `["EVENT",<sub_id>,<event_json>]` frame from pre-serialized event bytes.
///
/// This is the fanout fast path: the relay serialises an event once and reuses the same
/// `event_json` slice for every subscription, only re-writing the small prefix/suffix.
pub fn write_event_frame(out: &mut Vec<u8>, sub_id: &str, event_json: &[u8]) {
    out.reserve(sub_id.len() + event_json.len() + 16);
    out.extend_from_slice(b"[\"EVENT\",");
    write_json_string(out, sub_id);
    out.push(b',');
    out.extend_from_slice(event_json);
    out.push(b']');
}

/// Current unix time in seconds.
pub fn now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sign_and_verify_roundtrip() {
        let keys = Keys::generate();
        let ev = keys.sign_event(
            1_700_000_000,
            1,
            vec![vec!["t".into(), "nostr".into()]],
            "hello world",
        );
        ev.verify().expect("valid event");
        assert_eq!(ev.compute_id(), ev.id);
        let json = ev.to_json();
        let back = Event::from_json_str(&json).unwrap();
        assert_eq!(back, ev);
        back.verify().unwrap();
    }

    #[test]
    fn tampered_event_fails() {
        let keys = Keys::generate();
        let mut ev = keys.sign_event(1_700_000_000, 1, vec![], "hello");
        ev.content.push('!');
        assert!(matches!(ev.verify(), Err(CoreError::IdMismatch)));
    }

    #[test]
    fn filter_matching() {
        let keys = Keys::generate();
        let ev = keys.sign_event(
            1_700_000_000,
            1,
            vec![vec!["t".into(), "rust".into()]],
            "I love rust",
        );
        let f = Filter::from_json_value(&serde_json::json!({"kinds":[1],"#t":["rust"],"since":1}))
            .unwrap();
        assert!(f.matches(&ev));
        let f2 = Filter::from_json_value(&serde_json::json!({"authors":[hex::encode([9u8;32])]}))
            .unwrap();
        assert!(!f2.matches(&ev));
        let f3 = Filter::from_json_value(&serde_json::json!({"search":"LOVE"})).unwrap();
        assert!(f3.matches(&ev));
    }

    #[test]
    fn parse_messages() {
        let keys = Keys::generate();
        let ev = keys.sign_event(1_700_000_000, 1, vec![], "hi");
        let frame = format!("[\"EVENT\",{}]", ev.to_json());
        match parse_client_message(&frame).unwrap() {
            ClientMessage::Event(e) => assert_eq!(e.id, ev.id),
            _ => panic!("wrong variant"),
        }
        let req = "[\"REQ\",\"sub1\",{\"kinds\":[1],\"limit\":10}]";
        match parse_client_message(req).unwrap() {
            ClientMessage::Req { sub_id, filters } => {
                assert_eq!(sub_id, "sub1");
                assert_eq!(filters[0].limit, Some(10));
            }
            _ => panic!("wrong variant"),
        }
        assert!(parse_client_message("[\"CLOSE\",\"sub1\"]").is_ok());
        assert!(parse_client_message("[\"BOGUS\"]").is_err());
    }

    #[test]
    fn relay_message_frames() {
        let keys = Keys::generate();
        let ev = keys.sign_event(1_700_000_000, 1, vec![], "hi");
        let s = RelayMessage::Event {
            sub_id: "s".into(),
            event: ev.clone(),
        }
        .to_json();
        let v: Value = serde_json::from_str(&s).unwrap();
        assert_eq!(v[0], "EVENT");
        assert_eq!(v[1], "s");
        assert_eq!(Event::from_json_value(&v[2]).unwrap().id, ev.id);
        let ok = RelayMessage::Ok {
            id: hex::encode(ev.id),
            ok: true,
            message: String::new(),
        }
        .to_json();
        let v: Value = serde_json::from_str(&ok).unwrap();
        assert_eq!(v[0], "OK");
        assert_eq!(v[2], true);
        let c = RelayMessage::Count {
            sub_id: "s".into(),
            count: 7,
        }
        .to_json();
        let v: Value = serde_json::from_str(&c).unwrap();
        assert_eq!(v[2]["count"], 7);
    }

    #[test]
    fn expiration_tag() {
        let keys = Keys::generate();
        let ev = keys.sign_event(
            1_700_000_000,
            1,
            vec![vec!["expiration".into(), "1700000100".into()]],
            "",
        );
        assert_eq!(ev.expiration(), Some(1_700_000_100));
        assert!(ev.is_expired(1_700_000_200));
        assert!(!ev.is_expired(1_700_000_050));
    }

    /// The hand-written canonical serializer must agree byte-for-byte with `serde_json`'s
    /// compact output, including escaping of control characters and non-ASCII text.
    #[test]
    fn canonical_id_matches_serde_json() {
        let keys = Keys::from_label("canonical");
        let cases: Vec<(Vec<Vec<String>>, String)> = vec![
            (vec![], String::new()),
            (vec![vec!["t".into(), "rust".into()]], "plain".into()),
            (
                vec![
                    vec!["e".into(), "00".repeat(32)],
                    vec!["p".into(), "ab".repeat(32)],
                ],
                "quote \" backslash \\ newline \n tab \t cr \r bell \u{7} nul \u{0}".into(),
            ),
            (
                vec![vec!["d".into(), "slug".into()]],
                "unicode: \u{1F600} \u{00e9} \u{4e16}\u{754c}".into(),
            ),
            (
                vec![vec!["t".into(), "a".into(), "b".into()]],
                "\u{1}\u{1f}\u{7f}".into(),
            ),
        ];
        for (tags, content) in cases {
            let ev = keys.sign_event(1_700_000_123, 30023, tags.clone(), &content);
            // Reference implementation: exactly what NIP-01 specifies.
            let canonical = serde_json::to_string(&serde_json::json!([
                0,
                hex::encode(ev.pubkey),
                ev.created_at,
                ev.kind,
                tags,
                content
            ]))
            .unwrap();
            let mut h = Sha256::new();
            h.update(canonical.as_bytes());
            let d = h.finalize();
            let mut want = [0u8; 32];
            want.copy_from_slice(&d);
            assert_eq!(
                ev.compute_id(),
                want,
                "canonical id mismatch for {canonical}"
            );
            assert_eq!(ev.id, want);
        }
    }

    /// `to_json` must round-trip through `serde_json` for every escaping edge case.
    #[test]
    fn json_output_matches_serde_json() {
        let keys = Keys::from_label("json-out");
        let ev = keys.sign_event(
            1_700_000_123,
            1,
            vec![
                vec!["t".into(), "a\"b".into()],
                vec!["e".into(), "x\\y".into()],
            ],
            "line1\nline2\ttab \u{1F600} \u{0}\u{1f}",
        );
        let ours = ev.to_json();
        let theirs = serde_json::to_string(&ev).unwrap();
        assert_eq!(ours, theirs);
        let back = Event::from_json_str(&ours).unwrap();
        assert_eq!(back, ev);
        back.verify().unwrap();
    }

    #[test]
    fn event_frame_matches_relay_message() {
        let keys = Keys::from_label("frame");
        let ev = keys.sign_event(1_700_000_000, 1, vec![], "hi");
        let mut out = Vec::new();
        write_event_frame(&mut out, "sub\"x", &ev.to_json_bytes());
        let s = String::from_utf8(out).unwrap();
        let v: Value = serde_json::from_str(&s).unwrap();
        assert_eq!(v[0], "EVENT");
        assert_eq!(v[1], "sub\"x");
        assert_eq!(Event::from_json_value(&v[2]).unwrap().id, ev.id);
    }

    #[test]
    fn write_helpers() {
        let mut out = Vec::new();
        write_u64(&mut out, 0);
        write_u64(&mut out, 1_700_000_000);
        write_u64(&mut out, u64::MAX);
        assert_eq!(
            String::from_utf8(out).unwrap(),
            "0170000000018446744073709551615"
        );
        let mut out = Vec::new();
        write_hex(&mut out, &[0x00, 0xff, 0x1a]);
        assert_eq!(String::from_utf8(out).unwrap(), "00ff1a");
    }
}
