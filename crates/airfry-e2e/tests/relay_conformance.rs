//! Black-box protocol conformance test: spawns the real `airfry-relay` binary and drives it over
//! websockets exactly like a Nostr client would. This is the independent check that the relay
//! actually speaks NIP-01 on the wire.

mod common;

use airfry_core::{Event, Keys, RelayMessage};
use common::*;
use serde_json::Value;
use std::time::Duration;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn relay_speaks_nip01_on_the_wire() {
    let relay = start_relay_with(&["--relay-name", "e2e-test-relay"]);
    let port = relay.port;
    let keys = Keys::from_label("e2e-publisher");
    let mut ws = connect(port).await;

    // --- publish a valid event ---
    let ev = keys.sign_event(
        airfry_core::now_secs(),
        1,
        vec![vec!["t".into(), "e2e".into()]],
        "hello from the conformance suite",
    );
    let ok = publish(&mut ws, &ev).await;
    assert_eq!(ok[2], true, "valid event must be accepted: {ok}");

    // --- duplicate must be rejected ---
    let ok = publish(&mut ws, &ev).await;
    assert_eq!(ok[2], false, "duplicate must be rejected: {ok}");

    // --- REQ returns the event then EOSE ---
    let got = req_collect(
        &mut ws,
        "sub-a",
        serde_json::json!({"authors": [keys.public_key_hex()], "limit": 10}),
    )
    .await;
    assert_eq!(
        got.len(),
        1,
        "REQ should return exactly the published event"
    );
    assert_eq!(got[0].id, ev.id);
    got[0].verify().expect("returned event verifies");

    // --- COUNT ---
    send(
        &mut ws,
        format!(
            "[\"COUNT\",\"cnt\",{{\"authors\":[\"{}\"]}}]",
            keys.public_key_hex()
        ),
    )
    .await;
    let cnt = read_until(&mut ws, |v| v[0] == "COUNT", Duration::from_secs(10)).await;
    assert_eq!(cnt[1], "cnt");
    assert_eq!(cnt[2]["count"], 1, "count frame: {cnt}");

    // --- malformed frame yields NOTICE, connection stays usable ---
    send(&mut ws, "[\"BOGUS\",1,2,3]").await;
    let notice = read_until(&mut ws, |v| v[0] == "NOTICE", Duration::from_secs(10)).await;
    assert!(notice[1].is_string());

    // --- invalid signature is rejected ---
    let mut bad = keys.sign_event(airfry_core::now_secs(), 1, vec![], "bad sig");
    bad.sig[0] ^= 0xff;
    let ok = publish(&mut ws, &bad).await;
    assert_eq!(ok[2], false, "bad signature must be rejected: {ok}");

    // --- live fanout to a second connection ---
    let mut ws2 = connect(port).await;
    send(
        &mut ws2,
        "[\"REQ\",\"live\",{\"kinds\":[1],\"#t\":[\"live\"]}]",
    )
    .await;
    let _ = read_until(&mut ws2, |v| v[0] == "EOSE", Duration::from_secs(10)).await;
    let live_ev = keys.sign_event(
        airfry_core::now_secs(),
        1,
        vec![vec!["t".into(), "live".into()]],
        "live!",
    );
    publish_ok(&mut ws, &live_ev).await;
    let pushed = read_until(&mut ws2, |v| v[0] == "EVENT", Duration::from_secs(10)).await;
    assert_eq!(pushed[1], "live");
    assert_eq!(Event::from_json_value(&pushed[2]).unwrap().id, live_ev.id);

    // --- CLOSE stops delivery ---
    send(&mut ws2, "[\"CLOSE\",\"live\"]").await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    let after = keys.sign_event(
        airfry_core::now_secs(),
        1,
        vec![vec!["t".into(), "live".into()]],
        "after close",
    );
    publish_ok(&mut ws, &after).await;
    let frames = drain(&mut ws2, Duration::from_millis(500)).await;
    for v in &frames {
        if v[0] == "EVENT" {
            assert_ne!(
                Event::from_json_value(&v[2]).unwrap().id,
                after.id,
                "CLOSE must stop delivery"
            );
        }
    }

    // --- NIP-11 relay info document ---
    let resp = http_get(port, "/", Some("application/nostr+json"));
    assert_eq!(
        resp.status(),
        200,
        "NIP-11 must return 200: {}",
        resp.status_line
    );
    let info = resp.json();
    assert_eq!(info["name"], "e2e-test-relay");
    assert!(
        info["supported_nips"].is_array(),
        "supported_nips required: {info}"
    );

    // --- /health ---
    let health = http_get(port, "/health", None);
    assert_eq!(
        health.status(),
        200,
        "health must return 200: {}",
        health.status_line
    );
    let h = health.json();
    assert!(
        h.get("events").is_some() || h.get("stats").is_some(),
        "health should report store stats: {h}"
    );

    // --- NIP-09 deletion ---
    let del = keys.sign_event(
        airfry_core::now_secs(),
        5,
        vec![vec!["e".into(), hex::encode(ev.id)]],
        "",
    );
    let ok = publish(&mut ws, &del).await;
    assert_eq!(ok[2], true, "deletion request must be accepted: {ok}");
    let got = req_collect(
        &mut ws,
        "after-del",
        serde_json::json!({"ids": [hex::encode(ev.id)]}),
    )
    .await;
    assert!(
        got.is_empty(),
        "deleted event must not be returned: {got:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn relay_rejects_oversized_events() {
    let relay = start_relay_with(&["--max-event-bytes", "65536"]);
    let keys = Keys::from_label("e2e-big");
    let mut ws = connect(relay.port).await;
    let big = "x".repeat(200_000);
    let ev = keys.sign_event(airfry_core::now_secs(), 1, vec![], &big);
    let ok = publish(&mut ws, &ev).await;
    assert_eq!(ok[2], false, "oversized event must be rejected: {ok}");
    let msg = ok[3].as_str().unwrap_or("");
    assert!(
        msg.contains("invalid") || msg.contains("too large") || msg.contains("error"),
        "rejection should carry a machine-readable prefix: {ok}"
    );
    // the connection must remain usable
    let small = keys.sign_event(airfry_core::now_secs(), 1, vec![], "small");
    let ok = publish(&mut ws, &small).await;
    assert_eq!(
        ok[2], true,
        "connection must stay usable after an oversized frame: {ok}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn relay_persists_across_restart() {
    let relay = start_relay();
    let port = relay.port;
    let dir = relay.dir.clone();
    let keys = Keys::from_label("e2e-persist");
    let ev = keys.sign_event(
        airfry_core::now_secs(),
        1,
        vec![vec!["t".into(), "persist".into()]],
        "survive restart",
    );
    {
        let mut ws = connect(port).await;
        publish_ok(&mut ws, &ev).await;
    }
    // stop the relay (Drop kills it) and start a new one on the same data dir.
    // Preserve the data dir so the restarted relay can recover from it.
    let mut relay = relay;
    relay.keep_dir = true;
    drop(relay);
    let proc2 = start_relay_on(&dir);
    let mut ws = connect(proc2.port).await;
    let got = req_collect(
        &mut ws,
        "persist",
        serde_json::json!({"ids": [hex::encode(ev.id)]}),
    )
    .await;
    assert_eq!(got.len(), 1, "event must survive restart");
    assert_eq!(got[0].id, ev.id);
    got[0].verify().expect("recovered event verifies");
    drop(proc2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn relay_survives_garbage_and_concurrent_clients() {
    let relay = start_relay();
    let port = relay.port;
    // raw garbage on a websocket
    let mut ws = connect(port).await;
    send(&mut ws, "not json at all").await;
    let _ = read_until(&mut ws, |v| v[0] == "NOTICE", Duration::from_secs(10)).await;
    send(&mut ws, "[]").await;
    let _ = read_until(&mut ws, |v| v[0] == "NOTICE", Duration::from_secs(10)).await;
    send(&mut ws, "[\"REQ\",\"x\"]").await;
    let _ = read_until(
        &mut ws,
        |v| v[0] == "NOTICE" || v[0] == "CLOSED",
        Duration::from_secs(10),
    )
    .await;

    // many concurrent publishers
    let mut tasks = Vec::new();
    for i in 0..8 {
        tasks.push(tokio::spawn(async move {
            let k = Keys::from_label(&format!("e2e-conc-{i}"));
            let mut ws = connect(port).await;
            for j in 0..25u64 {
                let ev = k.sign_event(
                    airfry_core::now_secs(),
                    1,
                    vec![vec!["t".into(), "conc".into()]],
                    &format!("c{i}-{j}"),
                );
                publish_ok(&mut ws, &ev).await;
            }
        }));
    }
    for t in tasks {
        t.await.unwrap();
    }
    let mut ws = connect(port).await;
    send(&mut ws, "[\"COUNT\",\"c\",{\"#t\":[\"conc\"]}]").await;
    let cnt = read_until(&mut ws, |v| v[0] == "COUNT", Duration::from_secs(10)).await;
    assert_eq!(
        cnt[2]["count"], 200,
        "all concurrent events must be stored: {cnt}"
    );
}

#[test]
fn relay_message_helpers_are_consistent() {
    // sanity check that the shared wire helpers the relay uses produce parseable frames
    let k = Keys::from_label("e2e-wire");
    let ev = k.sign_event(1, 1, vec![], "x");
    let frame = RelayMessage::Event {
        sub_id: "s".into(),
        event: ev.clone(),
    }
    .to_json();
    let v: Value = serde_json::from_str(&frame).unwrap();
    assert_eq!(v.as_array().unwrap().len(), 3);
    assert_eq!(Event::from_json_value(&v[2]).unwrap().id, ev.id);
}
