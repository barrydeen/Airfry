//! On-disk record framing for the append-only log.
//!
//! Every record is framed as:
//!
//! ```text
//! offset  size  field
//! 0       4     magic  = 0x4E535452 ("NSTR")
//! 4       1     record type (1 = event, 2 = delete tombstone)
//! 5       4     payload length (LE u32)
//! 9       4     crc32c over (record type byte || payload)  (LE u32)
//! 13      N     payload
//! ```
//!
//! The CRC covers the type byte and the payload so a torn or bit-flipped record is detected
//! before it is decoded. Recovery stops at the first record whose header is incomplete, whose
//! magic is wrong, whose length is implausible, or whose CRC does not match — everything after
//! that point is treated as a torn tail and discarded (the file is truncated back to the last
//! good record boundary so subsequent appends stay aligned).
//!
//! Payloads:
//! * Event records carry `nostr_core::codec::encode_event` output verbatim (binary, no JSON).
//! * Delete records carry the 32-byte event id followed by the 32-byte author pubkey that
//!   requested the deletion (so a replay can re-validate ownership).

/// Record magic ("NSTR" little-endian).
pub const MAGIC: u32 = 0x4E53_5452;
/// Fixed header size in bytes.
pub const HEADER_LEN: usize = 13;
/// Record type: a stored event.
pub const REC_EVENT: u8 = 1;
/// Record type: a NIP-09 deletion tombstone.
pub const REC_DELETE: u8 = 2;
/// Upper bound on a single record payload; anything larger is treated as corruption.
pub const MAX_PAYLOAD: usize = 64 << 20;

/// A parsed record header.
#[derive(Clone, Copy, Debug)]
pub struct Header {
    pub rec_type: u8,
    pub payload_len: u32,
    pub crc: u32,
}

/// Compute the CRC32C over the type byte and payload, matching what is written to disk.
#[inline]
pub fn record_crc(rec_type: u8, payload: &[u8]) -> u32 {
    let mut h = crc32fast::Hasher::new();
    h.update(&[rec_type]);
    h.update(payload);
    h.finalize()
}

/// Append a framed record to `out`.
#[inline]
pub fn write_record(out: &mut Vec<u8>, rec_type: u8, payload: &[u8]) {
    let crc = record_crc(rec_type, payload);
    out.extend_from_slice(&MAGIC.to_le_bytes());
    out.push(rec_type);
    out.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    out.extend_from_slice(&crc.to_le_bytes());
    out.extend_from_slice(payload);
}

/// Try to parse a header at `pos`. Returns `None` when the header is incomplete or invalid.
#[inline]
pub fn parse_header(buf: &[u8], pos: usize) -> Option<Header> {
    if pos + HEADER_LEN > buf.len() {
        return None;
    }
    let magic = u32::from_le_bytes([buf[pos], buf[pos + 1], buf[pos + 2], buf[pos + 3]]);
    if magic != MAGIC {
        return None;
    }
    let rec_type = buf[pos + 4];
    if rec_type != REC_EVENT && rec_type != REC_DELETE {
        return None;
    }
    let payload_len = u32::from_le_bytes([buf[pos + 5], buf[pos + 6], buf[pos + 7], buf[pos + 8]]);
    if payload_len as usize > MAX_PAYLOAD {
        return None;
    }
    let crc = u32::from_le_bytes([buf[pos + 9], buf[pos + 10], buf[pos + 11], buf[pos + 12]]);
    Some(Header {
        rec_type,
        payload_len,
        crc,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_header_and_crc() {
        let payload = b"hello world payload";
        let mut buf = Vec::new();
        write_record(&mut buf, REC_EVENT, payload);
        let h = parse_header(&buf, 0).expect("header parses");
        assert_eq!(h.rec_type, REC_EVENT);
        assert_eq!(h.payload_len as usize, payload.len());
        let body = &buf[HEADER_LEN..HEADER_LEN + payload.len()];
        assert_eq!(body, payload);
        assert_eq!(record_crc(h.rec_type, body), h.crc);
    }

    #[test]
    fn truncated_header_is_rejected() {
        let mut buf = Vec::new();
        write_record(&mut buf, REC_EVENT, b"abc");
        assert!(parse_header(&buf[..HEADER_LEN - 1], 0).is_none());
    }

    #[test]
    fn bad_magic_is_rejected() {
        let mut buf = Vec::new();
        write_record(&mut buf, REC_EVENT, b"abc");
        buf[0] ^= 0xFF;
        assert!(parse_header(&buf, 0).is_none());
    }

    #[test]
    fn crc_detects_bit_flip() {
        let mut buf = Vec::new();
        write_record(&mut buf, REC_EVENT, b"payload");
        let h = parse_header(&buf, 0).unwrap();
        let mut body = buf[HEADER_LEN..].to_vec();
        body[0] ^= 0x01;
        assert_ne!(record_crc(h.rec_type, &body), h.crc);
    }
}
