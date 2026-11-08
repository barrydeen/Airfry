//! NIP-77 negentropy integration tests: a full reconciliation handshake against the real relay
//! over a WebSocket, plus the error paths (bad hex, unknown subscription, unsupported protocol
//! version, NEG-CLOSE).

mod common;

use airfry_core::{Filter, Keys};
use airfry_negentropy::{Item, Negentropy, NegentropySet};
use airfry_relay::config::Config;
use common::*;
use serde_json::{json, Value};
use std::sync::Arc;
use std::time::Duration;

const T: Duration = Duration::from_secs(10);

/// Publish `n` events of kind 1 from `keys` and return them.
async fn publish_events(c: &mut Client, keys: &Keys, n: usize) -> Vec<airfry_core::Event> {
    let mut out = Vec::with_capacity(n);
    for i in 0..n {
        let ev = keys.sign_event(
            airfry_core::now_secs() - (n as u64 - i as u64),
            1,
            vec![vec!["t".to_string(), "neg".to_string()]],
            &format!("negentropy test event {i}"),
        );
        c.send_raw(&event_frame(&ev)).await;
        let ok = c.wait_for("OK", T).await.expect("OK frame");
        assert_eq!(ok[2], true, "event accepted: {ok}");
        out.push(ev);
    }
    out
}

/// Drive a full NIP-77 handshake from the client side against the relay.
///
/// Per NIP-77 the relay only answers with `NEG-MSG` rounds; the client learns which ids it
/// lacks (`need`) from those rounds and then fetches them with a normal `REQ`. This helper
/// returns the ids the client determined it was missing.
async fn neg_handshake(
    c: &mut Client,
    sub_id: &str,
    filter: Value,
    client_items: Vec<Item>,
) -> Vec<[u8; 32]> {
    let client_set = Arc::new(NegentropySet::new(client_items).expect("client set seals"));
    let mut init = Negentropy::new(client_set, 0).expect("initiator");
    let mut msg = init.initiate().expect("initiate");

    c.send_raw(
        &format!(
            "[\"NEG-OPEN\",\"{sub_id}\",{filter},\"{}\"],",
            hex::encode(&msg)
        )
        .trim_end_matches(','),
    )
    .await;

    let mut have: Vec<[u8; 32]> = Vec::new();
    let mut need: Vec<[u8; 32]> = Vec::new();
    let mut rounds = 0usize;

    loop {
        rounds += 1;
        assert!(rounds < 100, "handshake did not terminate");

        let frame = c.wait_for("NEG-MSG", T).await.expect("NEG-MSG reply");
        assert_eq!(frame[1], sub_id, "reply addressed to our sub id");
        let payload = hex::decode(frame[2].as_str().expect("hex payload")).expect("valid hex");

        // A bare protocol-version byte from the relay means it is done.
        if payload.len() == 1 {
            break;
        }

        let reply = init
            .reconcile(&payload, &mut have, &mut need)
            .expect("client reconcile");
        if reply.len() == 1 {
            // We are done: send our final (version-only) message and wait for the relay's
            // completion reply.
            c.send_raw(&format!(
                "[\"NEG-MSG\",\"{sub_id}\",\"{}\"]",
                hex::encode(&reply)
            ))
            .await;
            continue;
        }
        msg = reply;
        c.send_raw(&format!(
            "[\"NEG-MSG\",\"{sub_id}\",\"{}\"]",
            hex::encode(&msg)
        ))
        .await;
    }

    need
}

#[tokio::test]
async fn neg_open_full_handshake_delivers_missing_events() {
    let relay = spawn_relay(Config::default()).await;
    let mut c = connect(relay.addr).await;
    let keys = Keys::from_label("neg-publisher");
    let events = publish_events(&mut c, &keys, 12).await;

    // The client knows about the first 6 events only; it must learn about the other 6.
    let client_items: Vec<Item> = events[..6]
        .iter()
        .map(|e| Item::new(e.created_at, e.id))
        .collect();

    let filter = json!({"authors": [keys.public_key_hex()], "kinds": [1]});
    let need = neg_handshake(&mut c, "neg1", filter.clone(), client_items).await;

    let mut got: Vec<String> = need.iter().map(|id| hex::encode(id)).collect();
    got.sort();
    let mut want: Vec<String> = events[6..].iter().map(|e| hex::encode(e.id)).collect();
    want.sort();
    assert_eq!(got, want, "client must learn exactly the ids it lacks");

    // NIP-77: the client then fetches the missing events with a normal REQ.
    let req = format!(
        "[\"REQ\",\"fetch1\",{{\"ids\":[{}]}}]",
        need.iter()
            .map(|id| format!("\"{}\"", hex::encode(id)))
            .collect::<Vec<_>>()
            .join(",")
    );
    c.send_raw(&req).await;
    let mut fetched: Vec<String> = Vec::new();
    loop {
        let v = c.next_json(T).await.expect("frame while draining REQ");
        match v.get(0).and_then(|x| x.as_str()) {
            Some("EVENT") => {
                fetched.push(v[2]["id"].as_str().expect("event id").to_string());
            }
            Some("EOSE") => break,
            _ => {}
        }
    }
    fetched.sort();
    assert_eq!(fetched, want, "REQ must return exactly the missing events");
}

#[tokio::test]
async fn neg_open_identical_sets_sends_no_events() {
    let relay = spawn_relay(Config::default()).await;
    let mut c = connect(relay.addr).await;
    let keys = Keys::from_label("neg-identical");
    let events = publish_events(&mut c, &keys, 5).await;

    let client_items: Vec<Item> = events
        .iter()
        .map(|e| Item::new(e.created_at, e.id))
        .collect();
    let filter = json!({"authors": [keys.public_key_hex()]});
    let need = neg_handshake(&mut c, "neg2", filter, client_items).await;
    assert!(
        need.is_empty(),
        "identical sets must yield no missing ids: {need:?}"
    );
}

#[tokio::test]
async fn neg_open_empty_client_gets_everything() {
    let relay = spawn_relay(Config::default()).await;
    let mut c = connect(relay.addr).await;
    let keys = Keys::from_label("neg-empty-client");
    let events = publish_events(&mut c, &keys, 8).await;

    let filter = json!({"authors": [keys.public_key_hex()]});
    let need = neg_handshake(&mut c, "neg3", filter, Vec::new()).await;
    assert_eq!(
        need.len(),
        events.len(),
        "client with an empty set learns all ids"
    );
}

#[tokio::test]
async fn neg_open_rejects_bad_hex() {
    let relay = spawn_relay(Config::default()).await;
    let mut c = connect(relay.addr).await;
    c.send_raw("[\"NEG-OPEN\",\"bad\",{},\"zzzz\"]").await;
    let v = c.wait_for("CLOSED", T).await.expect("CLOSED frame");
    assert_eq!(v[1], "bad");
    assert!(v[2].as_str().unwrap().contains("bad hex"), "message: {v}");
}

#[tokio::test]
async fn neg_open_rejects_unsupported_protocol_version() {
    let relay = spawn_relay(Config::default()).await;
    let mut c = connect(relay.addr).await;
    // 0x50 is outside the supported 0x60..=0x6f range.
    c.send_raw("[\"NEG-OPEN\",\"ver\",{},\"50\"]").await;
    let v = c.wait_for("CLOSED", T).await.expect("CLOSED frame");
    assert_eq!(v[1], "ver");
    assert!(
        v[2].as_str().unwrap().contains("protocol version"),
        "message: {v}"
    );
}

#[tokio::test]
async fn neg_msg_unknown_subscription_is_closed() {
    let relay = spawn_relay(Config::default()).await;
    let mut c = connect(relay.addr).await;
    c.send_raw("[\"NEG-MSG\",\"nope\",\"61\"]").await;
    let v = c.wait_for("CLOSED", T).await.expect("CLOSED frame");
    assert_eq!(v[1], "nope");
    assert!(
        v[2].as_str()
            .unwrap()
            .contains("unknown negentropy subscription"),
        "message: {v}"
    );
}

#[tokio::test]
async fn neg_close_drops_state() {
    let relay = spawn_relay(Config::default()).await;
    let mut c = connect(relay.addr).await;
    let keys = Keys::from_label("neg-close");
    publish_events(&mut c, &keys, 3).await;

    let client_set = Arc::new(NegentropySet::new(Vec::new()).expect("empty set"));
    let mut init = Negentropy::new(client_set, 0).expect("initiator");
    let msg = init.initiate().expect("initiate");
    let filter = json!({"authors": [keys.public_key_hex()]});
    c.send_raw(&format!(
        "[\"NEG-OPEN\",\"cl\",{filter},\"{}\"]",
        hex::encode(&msg)
    ))
    .await;
    let reply = c.wait_for("NEG-MSG", T).await.expect("NEG-MSG");
    assert_eq!(reply[1], "cl");

    c.send_raw("[\"NEG-CLOSE\",\"cl\"]").await;
    // After NEG-CLOSE the state is gone, so a further NEG-MSG is rejected.
    c.send_raw("[\"NEG-MSG\",\"cl\",\"61\"]").await;
    let v = c.wait_for("CLOSED", T).await.expect("CLOSED frame");
    assert_eq!(v[1], "cl");
    assert!(
        v[2].as_str()
            .unwrap()
            .contains("unknown negentropy subscription"),
        "message: {v}"
    );
}

#[tokio::test]
async fn neg_open_missing_fields_notice() {
    let relay = spawn_relay(Config::default()).await;
    let mut c = connect(relay.addr).await;
    c.send_raw("[\"NEG-OPEN\"]").await;
    let v = c.wait_for("NOTICE", T).await.expect("NOTICE frame");
    assert!(
        v[1].as_str().unwrap().contains("missing subscription id"),
        "message: {v}"
    );
}

#[tokio::test]
async fn neg_open_filter_limits_are_ignored() {
    // The relay must reconcile the whole matching set, not a `limit`-truncated window.
    let relay = spawn_relay(Config::default()).await;
    let mut c = connect(relay.addr).await;
    let keys = Keys::from_label("neg-limit");
    let events = publish_events(&mut c, &keys, 10).await;

    let filter = json!({"authors": [keys.public_key_hex()], "limit": 2});
    let need = neg_handshake(&mut c, "lim", filter, Vec::new()).await;
    assert_eq!(
        need.len(),
        events.len(),
        "limit must not truncate the reconciled set"
    );
}

#[tokio::test]
async fn neg_open_uses_store_query_without_limit() {
    // Direct unit-level check of the helper the handler uses.
    let relay = spawn_relay(Config::default()).await;
    let keys = Keys::from_label("neg-helper");
    for i in 0..4 {
        let ev = keys.sign_event(airfry_core::now_secs(), 1, vec![], &format!("h{i}"));
        relay.store().insert(&ev).expect("insert");
    }
    let filter = Filter {
        authors: Some(vec![keys.public_key]),
        limit: Some(1),
        ..Default::default()
    };
    let items = airfry_relay::negentropy::local_items(relay.store(), &filter).expect("items");
    assert_eq!(items.len(), 4, "limit is dropped for negentropy queries");
}
