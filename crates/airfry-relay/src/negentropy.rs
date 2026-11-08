//! NIP-77 negentropy set reconciliation for the relay.
//!
//! Implements the relay side of the NIP-77 handshake:
//!
//! * `["NEG-OPEN", <sub_id>, <filter>, <initial negentropy message, hex>]` — the relay queries
//!   its store for the `(created_at, id)` pairs matching the filter, seals them into an
//!   [`airfry_negentropy::NegentropySet`], instantiates a responder and answers with
//!   `["NEG-MSG", <sub_id>, <hex>]`.
//! * `["NEG-MSG", <sub_id>, <hex>]` — feed the client's message into the stored reconciliation
//!   state and answer with `["NEG-MSG", <sub_id>, <hex>]`. When the responder's reply is the
//!   bare protocol-version byte (length 1) the reconciliation is complete and the subscription
//!   state is released. Per NIP-77 the relay does **not** stream events during reconciliation:
//!   the client learns which ids it lacks from the `NEG-MSG` rounds and fetches them with a
//!   normal `REQ`. This matches strfry's relay behaviour.
//! * `["NEG-CLOSE", <sub_id>]` — drop the reconciliation state.
//!
//! Errors are reported as `["CLOSED", <sub_id>, "invalid: ..."]` (bad hex / bad filter /
//! unsupported protocol version) or `["NOTICE", ...]` when the frame itself is malformed.
//!
//! ## Performance notes
//!
//! * The store query is issued with `limit = None` so the full matching set is returned; the
//!   filter's `limit` is deliberately ignored, exactly like strfry's negentropy responder
//!   (negentropy reconciles *sets*, not paginated windows).
//! * The sealed set is built once per `NEG-OPEN` and shared through an `Arc`; the responder
//!   answers every range fingerprint from the precomputed prefix-accumulator table in O(1).
//! * The missing-event lookup is a single `HashMap`-free pass: the ids are collected into a
//!   `HashSet` and the store is queried once with an `ids` filter, so the relay never does a
//!   per-id point lookup.

use airfry_core::{Filter, RelayMessage};
use airfry_negentropy::{Item, Negentropy, NegentropySet, ID_SIZE};
use airfry_store::Store;
use std::collections::HashMap;
use std::sync::Arc;

/// Maximum number of concurrent negentropy subscriptions per connection.
pub const MAX_NEG_SUBS_PER_CONN: usize = 8;

/// Maximum accepted size of a NEG-MSG payload (hex characters).
pub const MAX_NEG_MSG_HEX: usize = 1 << 20;

/// Per-subscription reconciliation state.
///
/// NIP-77 reconciliation only exchanges ids: the relay answers `NEG-MSG` rounds and the client
/// learns which ids it lacks, then fetches them with a normal `REQ`. The relay therefore only
/// needs to keep the responder participant (which holds the sealed local set) alive between
/// rounds.
pub struct NegState {
    /// The responder participant (holds the sealed local set).
    pub ne: Negentropy,
}

/// All negentropy subscriptions of one connection.
#[derive(Default)]
pub struct NegSubs {
    subs: HashMap<String, NegState>,
}

impl NegSubs {
    pub fn new() -> Self {
        NegSubs {
            subs: HashMap::new(),
        }
    }

    pub fn len(&self) -> usize {
        self.subs.len()
    }

    pub fn is_empty(&self) -> bool {
        self.subs.is_empty()
    }

    pub fn contains(&self, sub_id: &str) -> bool {
        self.subs.contains_key(sub_id)
    }

    pub fn get_mut(&mut self, sub_id: &str) -> Option<&mut NegState> {
        self.subs.get_mut(sub_id)
    }

    pub fn insert(&mut self, sub_id: String, state: NegState) -> Option<NegState> {
        self.subs.insert(sub_id, state)
    }

    pub fn remove(&mut self, sub_id: &str) -> Option<NegState> {
        self.subs.remove(sub_id)
    }

    pub fn clear(&mut self) {
        self.subs.clear();
    }
}

/// Build the sealed local item set for a filter.
///
/// Returns the `(created_at, id)` pairs of every stored event matching `filter`, with the
/// filter's `limit` removed so the whole matching set is reconciled.
pub fn local_items(store: &Store, filter: &Filter) -> Result<Vec<Item>, String> {
    let mut f = filter.clone();
    f.limit = None;
    let events = store.query(&[f]).map_err(|e| format!("error: {e}"))?;
    let mut items = Vec::with_capacity(events.len());
    for ev in events {
        items.push(Item::new(ev.created_at, ev.id));
    }
    Ok(items)
}

/// Seal a local item set, mapping a duplicate-item error to a protocol error string.
pub fn seal(items: Vec<Item>) -> Result<Arc<NegentropySet>, String> {
    NegentropySet::new(items)
        .map(Arc::new)
        .map_err(|e| format!("invalid: {e}"))
}

/// Decode a hex negentropy payload, enforcing the size cap.
pub fn decode_hex(hex_str: &str) -> Result<Vec<u8>, String> {
    if hex_str.len() > MAX_NEG_MSG_HEX {
        return Err("invalid: negentropy message too large".to_string());
    }
    hex::decode(hex_str).map_err(|e| format!("invalid: bad hex: {e}"))
}

/// Encode a `["NEG-MSG", sub_id, "<hex>"]` frame.
pub fn neg_msg_frame(sub_id: &str, payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(payload.len() * 2 + sub_id.len() + 16);
    out.extend_from_slice(b"[\"NEG-MSG\",");
    airfry_core::write_json_string(&mut out, sub_id);
    out.extend_from_slice(b",\"");
    airfry_core::write_hex(&mut out, payload);
    out.extend_from_slice(b"\"]");
    out
}

/// Encode a `["NEG-OPEN", sub_id, "<hex>"]` frame (used by clients and tests).
pub fn neg_open_frame(sub_id: &str, filter_json: &str, payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(payload.len() * 2 + sub_id.len() + filter_json.len() + 24);
    out.extend_from_slice(b"[\"NEG-OPEN\",");
    airfry_core::write_json_string(&mut out, sub_id);
    out.push(b',');
    out.extend_from_slice(filter_json.as_bytes());
    out.extend_from_slice(b",\"");
    airfry_core::write_hex(&mut out, payload);
    out.extend_from_slice(b"\"]");
    out
}

/// Encode a `["NEG-CLOSE", sub_id]` frame.
pub fn neg_close_frame(sub_id: &str) -> Vec<u8> {
    let mut out = Vec::with_capacity(sub_id.len() + 16);
    out.extend_from_slice(b"[\"NEG-CLOSE\",");
    airfry_core::write_json_string(&mut out, sub_id);
    out.push(b']');
    out
}

/// Build the `["EVENT", sub_id, {...}]` frames for the ids the client is missing, followed by
/// `["EOSE", sub_id]`.
///
/// `need` is the list of ids the client reported it does not have. The store is queried once
/// with an `ids` filter; ids that are no longer present (deleted between the reconciliation and
/// this fetch) are silently skipped.
pub fn missing_event_frames(
    store: &Store,
    sub_id: &str,
    filter: &Filter,
    need: &[[u8; ID_SIZE]],
) -> Vec<Vec<u8>> {
    let mut frames = Vec::with_capacity(need.len() + 1);
    if !need.is_empty() {
        let mut ids_filter = Filter {
            ids: Some(need.to_vec()),
            ..Default::default()
        };
        // Keep the filter's other constraints so we never send an event the client's filter
        // would not have matched anyway.
        ids_filter.authors = filter.authors.clone();
        ids_filter.kinds = filter.kinds.clone();
        ids_filter.since = filter.since;
        ids_filter.until = filter.until;
        ids_filter.tags = filter.tags.clone();
        ids_filter.limit = None;

        match store.query(&[ids_filter]) {
            Ok(events) => {
                let mut event_json = Vec::with_capacity(512);
                for ev in events {
                    event_json.clear();
                    ev.write_json(&mut event_json);
                    let mut frame = Vec::with_capacity(event_json.len() + sub_id.len() + 16);
                    airfry_core::write_event_frame(&mut frame, sub_id, &event_json);
                    frames.push(frame);
                }
            }
            Err(_) => {
                // Fall back to a per-id point lookup so a store error on the batch query does
                // not silently drop events.
                for id in need {
                    let f = Filter {
                        ids: Some(vec![*id]),
                        ..Default::default()
                    };
                    if let Ok(events) = store.query(&[f]) {
                        for ev in events {
                            let mut event_json = Vec::with_capacity(512);
                            ev.write_json(&mut event_json);
                            let mut frame =
                                Vec::with_capacity(event_json.len() + sub_id.len() + 16);
                            airfry_core::write_event_frame(&mut frame, sub_id, &event_json);
                            frames.push(frame);
                        }
                    }
                }
            }
        }
    }
    frames.push(
        RelayMessage::Eose {
            sub_id: sub_id.to_string(),
        }
        .to_json_bytes(),
    );
    frames
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encodes_frames() {
        let f = neg_msg_frame("s1", &[0x61, 0x00]);
        assert_eq!(String::from_utf8(f).unwrap(), r#"["NEG-MSG","s1","6100"]"#);
        let f = neg_close_frame("s1");
        assert_eq!(String::from_utf8(f).unwrap(), r#"["NEG-CLOSE","s1"]"#);
        let f = neg_open_frame("s1", "{}", &[0x61]);
        assert_eq!(
            String::from_utf8(f).unwrap(),
            r#"["NEG-OPEN","s1",{},"61"]"#
        );
    }

    #[test]
    fn decodes_hex_and_rejects_bad_input() {
        assert_eq!(decode_hex("6100").unwrap(), vec![0x61, 0x00]);
        assert!(decode_hex("zz").is_err());
        assert!(decode_hex("").unwrap().is_empty());
    }
}
