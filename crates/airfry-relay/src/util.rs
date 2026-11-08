//! Small helpers shared by the relay modules.

/// Generate a random hex string of `n` bytes (2*n hex chars).
pub fn random_hex(n: usize) -> String {
    let mut bytes = vec![0u8; n];
    for b in bytes.iter_mut() {
        *b = rand::random::<u8>();
    }
    hex::encode(bytes)
}

/// Cheaply pull the `"id"` field out of a raw EVENT frame without fully parsing it.
///
/// Used so that oversized / malformed events can still be answered with an `OK` frame
/// carrying the event id, as required by NIP-01.
pub fn extract_event_id(raw: &str) -> Option<String> {
    let bytes = raw.as_bytes();
    let mut i = 0usize;
    while i + 4 <= bytes.len() {
        if &bytes[i..i + 4] == b"\"id\"" {
            let mut j = i + 4;
            while j < bytes.len() && bytes[j].is_ascii_whitespace() {
                j += 1;
            }
            if j >= bytes.len() || bytes[j] != b':' {
                i += 1;
                continue;
            }
            j += 1;
            while j < bytes.len() && bytes[j].is_ascii_whitespace() {
                j += 1;
            }
            if j >= bytes.len() || bytes[j] != b'"' {
                i += 1;
                continue;
            }
            j += 1;
            let start = j;
            while j < bytes.len() && bytes[j].is_ascii_hexdigit() {
                j += 1;
            }
            if j - start == 64 && j < bytes.len() && bytes[j] == b'"' {
                return Some(raw[start..j].to_string());
            }
        }
        i += 1;
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extracts_id() {
        let raw = r#"["EVENT",{"id":"aa11","pubkey":"x"}]"#;
        assert_eq!(extract_event_id(raw), None);
        let id = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
        let raw = format!(r#"["EVENT",{{"id":"{}","pubkey":"x"}}]"#, id);
        assert_eq!(extract_event_id(&raw).as_deref(), Some(id));
    }

    #[test]
    fn random_hex_len() {
        assert_eq!(random_hex(32).len(), 64);
    }
}
