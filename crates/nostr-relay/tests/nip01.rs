//! NIP-01 publish / query / fanout integration tests.

mod common;

use common::*;
use nostr_core::Keys;
use nostr_relay::config::Config;
use serde_json::json;
use std::time::Duration;

const T: Duration = Duration::from_secs(5);

#[tokio::test]
async fn publish_returns_ok_true() {
    let relay = spawn_relay(Config::default()).await;
    let mut c = connect(relay.addr).await;
    let keys = Keys::from_label("publisher-1");
    let ev = signed_event(&keys, 1, vec![], "hello relay");
    c.send_raw(&event_frame(&ev)).await;
    let ok = c.wait_for("OK", T).await.expect("OK frame");
    assert_eq!(ok[1], hex::encode(ev.id));
    assert_eq!(ok[2], true, "expected OK true, got {ok}");
    assert_eq!(relay.store().stats().events, 1);
}

#[tokio::test]
async fn duplicate_event_rejected() {
    let relay = spawn_relay(Config::default()).await;
    let mut c = connect(relay.addr).await;
    let keys = Keys::from_label("publisher-dup");
    let ev = signed_event(&keys, 1, vec![], "dupe");
    c.send_raw(&event_frame(&ev)).await;
    let ok = c.wait_for("OK", T).await.expect("first OK");
    assert_eq!(ok[2], true);
    c.send_raw(&event_frame(&ev)).await;
    let ok = c.wait_for("OK", T).await.expect("second OK");
    assert_eq!(ok[2], false);
    let msg = ok[3].as_str().unwrap_or("");
    assert!(
        msg.starts_with("duplicate:"),
        "expected duplicate: prefix, got {msg}"
    );
    assert_eq!(relay.store().stats().events, 1);
}

#[tokio::test]
async fn oversized_event_rejected() {
    let mut cfg = Config::default();
    cfg.max_event_bytes = 512;
    let relay = spawn_relay(cfg).await;
    let mut c = connect(relay.addr).await;
    let keys = Keys::from_label("publisher-big");
    let big = "x".repeat(2000);
    let ev = signed_event(&keys, 1, vec![], &big);
    c.send_raw(&event_frame(&ev)).await;
    let ok = c.wait_for("OK", T).await.expect("OK frame");
    assert_eq!(ok[1], hex::encode(ev.id));
    assert_eq!(ok[2], false);
    let msg = ok[3].as_str().unwrap_or("");
    assert!(
        msg.starts_with("invalid:"),
        "expected invalid: prefix, got {msg}"
    );
    assert_eq!(relay.store().stats().events, 0);
}

#[tokio::test]
async fn bad_signature_rejected() {
    let relay = spawn_relay(Config::default()).await;
    let mut c = connect(relay.addr).await;
    let keys = Keys::from_label("publisher-badsig");
    let mut ev = signed_event(&keys, 1, vec![], "tampered");
    ev.content.push_str("!");
    c.send_raw(&event_frame(&ev)).await;
    let ok = c.wait_for("OK", T).await.expect("OK frame");
    assert_eq!(ok[2], false);
    assert!(ok[3].as_str().unwrap_or("").starts_with("invalid:"));
}

#[tokio::test]
async fn req_returns_events_then_eose() {
    let relay = spawn_relay(Config::default()).await;
    let mut c = connect(relay.addr).await;
    let keys = Keys::from_label("req-author");
    let mut ids = Vec::new();
    for i in 0..3 {
        let ev = signed_event(&keys, 1, vec![], &format!("note {i}"));
        ids.push(hex::encode(ev.id));
        c.send_raw(&event_frame(&ev)).await;
        let ok = c.wait_for("OK", T).await.expect("OK");
        assert_eq!(ok[2], true);
    }
    c.send_json(json!(["REQ", "sub-req", {"authors": [keys.public_key_hex()], "kinds": [1]}]))
        .await;
    let events = c.collect_until_eose("sub-req", T).await;
    assert_eq!(events.len(), 3, "expected 3 events, got {}", events.len());
    let mut got: Vec<String> = events
        .iter()
        .map(|e| e["id"].as_str().unwrap().to_string())
        .collect();
    got.sort();
    ids.sort();
    assert_eq!(got, ids);
}

#[tokio::test]
async fn req_limit_is_clamped() {
    let mut cfg = Config::default();
    cfg.max_limit = 2;
    let relay = spawn_relay(cfg).await;
    let mut c = connect(relay.addr).await;
    let keys = Keys::from_label("limit-author");
    for i in 0..5 {
        let ev = signed_event(&keys, 1, vec![], &format!("note {i}"));
        c.send_raw(&event_frame(&ev)).await;
        let _ = c.wait_for("OK", T).await.expect("OK");
    }
    c.send_json(json!(["REQ", "sub-limit", {"authors": [keys.public_key_hex()], "limit": 100}]))
        .await;
    let events = c.collect_until_eose("sub-limit", T).await;
    assert_eq!(events.len(), 2, "limit should be clamped to 2");
}

#[tokio::test]
async fn req_with_too_many_filters_closed() {
    let mut cfg = Config::default();
    cfg.max_filters = 2;
    let relay = spawn_relay(cfg).await;
    let mut c = connect(relay.addr).await;
    c.send_json(json!(["REQ", "sub-many", {"kinds":[1]}, {"kinds":[2]}, {"kinds":[3]}]))
        .await;
    let closed = c.wait_for("CLOSED", T).await.expect("CLOSED frame");
    assert_eq!(closed[1], "sub-many");
    assert!(closed[2].as_str().unwrap_or("").starts_with("invalid:"));
}

#[tokio::test]
async fn live_fanout_to_second_connection() {
    let relay = spawn_relay(Config::default()).await;
    let mut subscriber = connect(relay.addr).await;
    let mut publisher = connect(relay.addr).await;
    let keys = Keys::from_label("fanout-author");
    subscriber
        .send_json(json!(["REQ", "live", {"authors": [keys.public_key_hex()], "kinds": [1]}]))
        .await;
    let initial = subscriber.collect_until_eose("live", T).await;
    assert!(initial.is_empty());

    let ev = signed_event(&keys, 1, vec![], "live note");
    publisher.send_raw(&event_frame(&ev)).await;
    let ok = publisher.wait_for("OK", T).await.expect("OK");
    assert_eq!(ok[2], true);

    let frame = subscriber.wait_for("EVENT", T).await.expect("live EVENT");
    assert_eq!(frame[1], "live");
    assert_eq!(frame[2]["id"], hex::encode(ev.id));
    assert_eq!(frame[2]["content"], "live note");
}

#[tokio::test]
async fn close_stops_delivery() {
    let relay = spawn_relay(Config::default()).await;
    let mut subscriber = connect(relay.addr).await;
    let mut publisher = connect(relay.addr).await;
    let keys = Keys::from_label("close-author");
    subscriber
        .send_json(json!(["REQ", "closeme", {"authors": [keys.public_key_hex()]}]))
        .await;
    let _ = subscriber.collect_until_eose("closeme", T).await;
    subscriber.send_json(json!(["CLOSE", "closeme"])).await;
    // Give the relay a moment to process the CLOSE.
    tokio::time::sleep(Duration::from_millis(100)).await;

    let ev = signed_event(&keys, 1, vec![], "after close");
    publisher.send_raw(&event_frame(&ev)).await;
    let ok = publisher.wait_for("OK", T).await.expect("OK");
    assert_eq!(ok[2], true);

    let frame = subscriber.next_json(Duration::from_millis(400)).await;
    assert!(
        frame.is_none(),
        "expected no frame after CLOSE, got {frame:?}"
    );
}

#[tokio::test]
async fn count_returns_number() {
    let relay = spawn_relay(Config::default()).await;
    let mut c = connect(relay.addr).await;
    let keys = Keys::from_label("count-author");
    for i in 0..4 {
        let ev = signed_event(&keys, 1, vec![], &format!("counted {i}"));
        c.send_raw(&event_frame(&ev)).await;
        let _ = c.wait_for("OK", T).await.expect("OK");
    }
    c.send_json(json!(["COUNT", "cnt", {"authors": [keys.public_key_hex()], "kinds": [1]}]))
        .await;
    let frame = c.wait_for("COUNT", T).await.expect("COUNT frame");
    assert_eq!(frame[1], "cnt");
    assert_eq!(frame[2]["count"], 4);
}

#[tokio::test]
async fn count_ignores_filter_limit() {
    let relay = spawn_relay(Config::default()).await;
    let mut c = connect(relay.addr).await;
    let keys = Keys::from_label("count-limit-author");
    for i in 0..5 {
        let ev = signed_event(&keys, 1, vec![], &format!("counted {i}"));
        c.send_raw(&event_frame(&ev)).await;
        let _ = c.wait_for("OK", T).await.expect("OK");
    }
    c.send_json(
        json!(["COUNT", "cntl", {"authors": [keys.public_key_hex()], "kinds": [1], "limit": 2}]),
    )
    .await;
    let frame = c.wait_for("COUNT", T).await.expect("COUNT frame");
    assert_eq!(frame[2]["count"], 5, "COUNT must ignore filter limit");
}

#[tokio::test]
async fn kind5_deletion_removes_event() {
    let relay = spawn_relay(Config::default()).await;
    let mut c = connect(relay.addr).await;
    let keys = Keys::from_label("delete-author");
    let target = signed_event(&keys, 1, vec![], "to be deleted");
    let target_id = hex::encode(target.id);
    c.send_raw(&event_frame(&target)).await;
    let ok = c.wait_for("OK", T).await.expect("OK");
    assert_eq!(ok[2], true);

    let deletion = signed_event(
        &keys,
        5,
        vec![vec!["e".to_string(), target_id.clone()]],
        "delete it",
    );
    c.send_raw(&event_frame(&deletion)).await;
    let ok = c.wait_for("OK", T).await.expect("deletion OK");
    assert_eq!(ok[2], true);

    c.send_json(json!(["REQ", "after-delete", {"ids": [target_id]}]))
        .await;
    let events = c.collect_until_eose("after-delete", T).await;
    assert!(
        events.is_empty(),
        "deleted event should not be returned: {events:?}"
    );
}

#[tokio::test]
async fn kind5_deletion_ignores_foreign_author() {
    let relay = spawn_relay(Config::default()).await;
    let mut c = connect(relay.addr).await;
    let owner = Keys::from_label("owner");
    let other = Keys::from_label("other");
    let target = signed_event(&owner, 1, vec![], "owned note");
    let target_id = hex::encode(target.id);
    c.send_raw(&event_frame(&target)).await;
    let _ = c.wait_for("OK", T).await.expect("OK");

    let deletion = signed_event(
        &other,
        5,
        vec![vec!["e".to_string(), target_id.clone()]],
        "not mine",
    );
    c.send_raw(&event_frame(&deletion)).await;
    let _ = c.wait_for("OK", T).await.expect("deletion OK");

    c.send_json(json!(["REQ", "still-there", {"ids": [target_id]}]))
        .await;
    let events = c.collect_until_eose("still-there", T).await;
    assert_eq!(
        events.len(),
        1,
        "foreign deletion must not remove the event"
    );
}

#[tokio::test]
async fn expired_event_rejected_and_filtered() {
    let relay = spawn_relay(Config::default()).await;
    let mut c = connect(relay.addr).await;
    let keys = Keys::from_label("expiry-author");
    let now = nostr_core::now_secs();
    let expired = keys.sign_event(
        now.saturating_sub(60),
        1,
        vec![vec!["expiration".to_string(), (now - 10).to_string()]],
        "already gone",
    );
    c.send_raw(&event_frame(&expired)).await;
    let ok = c.wait_for("OK", T).await.expect("OK");
    assert_eq!(ok[2], false);
    assert!(ok[3].as_str().unwrap_or("").starts_with("invalid:"));
    assert_eq!(relay.store().stats().events, 0);
}

#[tokio::test]
async fn malformed_frame_yields_notice() {
    let relay = spawn_relay(Config::default()).await;
    let mut c = connect(relay.addr).await;
    c.send_raw("this is not json").await;
    let notice = c.wait_for("NOTICE", T).await.expect("NOTICE frame");
    assert!(notice[1].as_str().unwrap_or("").starts_with("invalid:"));

    c.send_raw("[\"BOGUS\",1]").await;
    let notice = c.wait_for("NOTICE", T).await.expect("NOTICE frame");
    assert!(notice[1]
        .as_str()
        .unwrap_or("")
        .contains("unknown message type"));

    // The connection must still be usable.
    let keys = Keys::from_label("after-notice");
    let ev = signed_event(&keys, 1, vec![], "still alive");
    c.send_raw(&event_frame(&ev)).await;
    let ok = c.wait_for("OK", T).await.expect("OK after notices");
    assert_eq!(ok[2], true);
}

#[tokio::test]
async fn auth_challenge_and_verification() {
    let relay = spawn_relay(Config::default()).await;
    let mut c = connect(relay.addr).await;
    let keys = Keys::from_label("auth-user");
    let now = nostr_core::now_secs();
    let auth_ev = keys.sign_event(
        now,
        22242,
        vec![
            vec!["relay".to_string(), format!("ws://{}/", relay.addr)],
            vec!["challenge".to_string(), c.challenge.clone()],
        ],
        "",
    );
    c.send_json(json!(["AUTH", auth_ev.to_json_value()])).await;
    let ok = c.wait_for("OK", T).await.expect("AUTH OK");
    assert_eq!(ok[2], true, "AUTH should succeed: {ok}");

    // Wrong challenge must fail.
    let bad = keys.sign_event(
        now,
        22242,
        vec![vec!["challenge".to_string(), "deadbeef".to_string()]],
        "",
    );
    c.send_json(json!(["AUTH", bad.to_json_value()])).await;
    let ok = c.wait_for("OK", T).await.expect("AUTH OK");
    assert_eq!(ok[2], false);
}

#[tokio::test]
async fn require_auth_blocks_writes() {
    let mut cfg = Config::default();
    cfg.require_auth = true;
    let relay = spawn_relay(cfg).await;
    let mut c = connect(relay.addr).await;
    let keys = Keys::from_label("gated-writer");
    let ev = signed_event(&keys, 1, vec![], "needs auth");
    c.send_raw(&event_frame(&ev)).await;
    let ok = c.wait_for("OK", T).await.expect("OK");
    assert_eq!(ok[2], false);
    assert!(ok[3].as_str().unwrap_or("").starts_with("auth-required:"));

    let now = nostr_core::now_secs();
    let auth_ev = keys.sign_event(
        now,
        22242,
        vec![vec!["challenge".to_string(), c.challenge.clone()]],
        "",
    );
    c.send_json(json!(["AUTH", auth_ev.to_json_value()])).await;
    let ok = c.wait_for("OK", T).await.expect("AUTH OK");
    assert_eq!(ok[2], true);

    c.send_raw(&event_frame(&ev)).await;
    let ok = c.wait_for("OK", T).await.expect("OK after auth");
    assert_eq!(ok[2], true);
}
