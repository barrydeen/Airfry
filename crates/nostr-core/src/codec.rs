//! Compact binary encoding for events, used by the storage engine and the wire fast-path.
//!
//! The format is deliberately simple and self-describing enough to be validated:
//!
//! ```text
//! u8   version (1)
//! [32] id
//! [32] pubkey
//! u64  created_at (LE)
//! u32  kind (LE)
//! u32  tag_count (LE)
//!   for each tag: u16 item_count (LE), then for each item: u32 len (LE) + bytes
//! u32  content_len (LE) + bytes
//! [64] sig
//! ```
//!
//! Decoding is bounds-checked and returns `CoreError::Malformed` on any inconsistency, so a
//! corrupt record can never panic the relay.

use crate::{CoreError, Event};

/// Current binary format version.
pub const BIN_VERSION: u8 = 1;

/// Upper bound on any single length field, to reject absurd allocations from corrupt input.
const MAX_FIELD: usize = 64 << 20;

fn put_u16(out: &mut Vec<u8>, v: u16) {
    out.extend_from_slice(&v.to_le_bytes());
}
fn put_u32(out: &mut Vec<u8>, v: u32) {
    out.extend_from_slice(&v.to_le_bytes());
}
fn put_u64(out: &mut Vec<u8>, v: u64) {
    out.extend_from_slice(&v.to_le_bytes());
}

/// Encode an event into the compact binary format.
pub fn encode_event(ev: &Event, out: &mut Vec<u8>) {
    out.push(BIN_VERSION);
    out.extend_from_slice(&ev.id);
    out.extend_from_slice(&ev.pubkey);
    put_u64(out, ev.created_at);
    put_u32(out, ev.kind);
    put_u32(out, ev.tags.len() as u32);
    for tag in &ev.tags {
        put_u16(out, tag.len() as u16);
        for item in tag {
            put_u32(out, item.len() as u32);
            out.extend_from_slice(item.as_bytes());
        }
    }
    put_u32(out, ev.content.len() as u32);
    out.extend_from_slice(ev.content.as_bytes());
    out.extend_from_slice(&ev.sig);
}

/// Encode an event into a fresh buffer.
pub fn encode_event_vec(ev: &Event) -> Vec<u8> {
    let mut out = Vec::with_capacity(ev.wire_size());
    encode_event(ev, &mut out);
    out
}

struct Cursor<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Cursor<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8], CoreError> {
        if self.pos + n > self.buf.len() {
            return Err(CoreError::Malformed("binary event truncated".into()));
        }
        let s = &self.buf[self.pos..self.pos + n];
        self.pos += n;
        Ok(s)
    }
    fn u8(&mut self) -> Result<u8, CoreError> {
        Ok(self.take(1)?[0])
    }
    fn u16(&mut self) -> Result<u16, CoreError> {
        let b = self.take(2)?;
        Ok(u16::from_le_bytes([b[0], b[1]]))
    }
    fn u32(&mut self) -> Result<u32, CoreError> {
        let b = self.take(4)?;
        Ok(u32::from_le_bytes([b[0], b[1], b[2], b[3]]))
    }
    fn u64(&mut self) -> Result<u64, CoreError> {
        let b = self.take(8)?;
        Ok(u64::from_le_bytes([
            b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7],
        ]))
    }
    fn string(&mut self) -> Result<String, CoreError> {
        let len = self.u32()? as usize;
        if len > MAX_FIELD {
            return Err(CoreError::Malformed("binary event field too large".into()));
        }
        let bytes = self.take(len)?;
        String::from_utf8(bytes.to_vec()).map_err(|_| CoreError::Malformed("invalid utf-8".into()))
    }
}

/// Decode an event from the compact binary format, returning the event and bytes consumed.
pub fn decode_event(buf: &[u8]) -> Result<(Event, usize), CoreError> {
    let mut c = Cursor { buf, pos: 0 };
    let version = c.u8()?;
    if version != BIN_VERSION {
        return Err(CoreError::Malformed(format!(
            "unsupported binary version {version}"
        )));
    }
    let mut id = [0u8; 32];
    id.copy_from_slice(c.take(32)?);
    let mut pubkey = [0u8; 32];
    pubkey.copy_from_slice(c.take(32)?);
    let created_at = c.u64()?;
    let kind = c.u32()?;
    let tag_count = c.u32()? as usize;
    if tag_count > 4096 {
        return Err(CoreError::Malformed("too many tags".into()));
    }
    let mut tags = Vec::with_capacity(tag_count);
    for _ in 0..tag_count {
        let items = c.u16()? as usize;
        let mut tag = Vec::with_capacity(items);
        for _ in 0..items {
            tag.push(c.string()?);
        }
        tags.push(tag);
    }
    let content = c.string()?;
    let mut sig = [0u8; 64];
    sig.copy_from_slice(c.take(64)?);
    Ok((
        Event {
            id,
            pubkey,
            created_at,
            kind,
            tags,
            content,
            sig,
        },
        c.pos,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Keys;

    #[test]
    fn roundtrip() {
        let k = Keys::from_label("codec");
        let ev = k.sign_event(
            1_700_000_123,
            30023,
            vec![
                vec!["d".into(), "slug".into()],
                vec!["t".into(), "rust".into(), "nostr".into()],
            ],
            "hello \u{1F600} world",
        );
        let buf = encode_event_vec(&ev);
        let (back, used) = decode_event(&buf).unwrap();
        assert_eq!(used, buf.len());
        assert_eq!(back, ev);
        back.verify().unwrap();
    }

    #[test]
    fn truncated_input_is_error() {
        let k = Keys::from_label("codec2");
        let ev = k.sign_event(1, 1, vec![vec!["p".into(), "abc".into()]], "x");
        let buf = encode_event_vec(&ev);
        for cut in [0usize, 1, 33, 70, buf.len() - 1] {
            assert!(decode_event(&buf[..cut]).is_err(), "cut {cut} should fail");
        }
    }

    #[test]
    fn bad_version_is_error() {
        let k = Keys::from_label("codec3");
        let ev = k.sign_event(1, 1, vec![], "");
        let mut buf = encode_event_vec(&ev);
        buf[0] = 9;
        assert!(decode_event(&buf).is_err());
    }

    #[test]
    fn empty_event_roundtrip() {
        let k = Keys::from_label("codec4");
        let ev = k.sign_event(1, 0, vec![], "");
        let buf = encode_event_vec(&ev);
        let (back, _) = decode_event(&buf).unwrap();
        assert_eq!(back, ev);
    }
}
