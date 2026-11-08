# airfry-core

Core Nostr primitives used by the [Airfry](https://github.com/barrydeen/Airfry) relay and
storage engine. This crate provides event parsing/validation, filters, NIP-01 client/relay
messages, a compact binary event codec, and schnorr key handling. It is the shared foundation
for the rest of the workspace.

## What it provides

- `Event` — parse from JSON (`Event::from_json_str`, `Event::from_json_value`), compute the
  canonical id (`compute_id`), verify id and signature (`verify`, `verify_id`), validate shape
  (`validate_shape`), and serialize back to JSON (`to_json`, `to_json_bytes`, `write_json`).
- `Filter` — parse (`Filter::from_json_value`), match (`Filter::matches`), and serialize
  (`to_json_value`). Supports `ids`, `authors`, `kinds`, `since`, `until`, `limit`, `#tag`
  filters and `search`.
- `ClientMessage` / `RelayMessage` — NIP-01 wire messages with allocation-free writers
  (`RelayMessage::write_json`, `write_event_frame`).
- `airfry_core::codec` — a compact binary event encoding (`encode_event`, `encode_event_vec`,
  `decode_event`) used on the storage hot path instead of JSON.
- `airfry_core::keys` — `Keys` (generate/from secret/from label), `sign_event`, and
  `verify_schnorr` / `verify_schnorr_batch`.
- Helpers: `now_secs`, `hex_array`, `write_hex`, `write_u64`, `write_json_string`.

## Usage

```rust
use airfry_core::{Event, Filter, Keys};

let keys = Keys::from_label("demo");
let ev = keys.sign_event(1, "hello nostr", &[], 1_700_000_000).unwrap();
assert!(ev.verify().is_ok());

let filter = Filter::from_json_value(&serde_json::json!({
    "kinds": [1], "limit": 10
})).unwrap();
assert!(filter.matches(&ev));

// Binary encoding for storage:
let mut buf = Vec::new();
airfry_core::codec::encode_event(&ev, &mut buf);
let (decoded, _n) = airfry_core::codec::decode_event(&buf).unwrap();
assert_eq!(decoded.id, ev.id);
```

## See also

- Repository and project overview: <https://github.com/barrydeen/Airfry>
- Benchmarks: [BENCHMARKS.md](https://github.com/barrydeen/Airfry/blob/main/BENCHMARKS.md)
- `airfry-store` (storage engine), `airfry-relay` (server), `airfry-bench` (load generator).

## License

MIT.
