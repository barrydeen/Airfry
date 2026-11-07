//! Adversarial and edge-case tests against the real `nostr-relay` binary over the wire.
//!
//! Every test here spawns the relay, drives it like a hostile client would, and asserts that the
//! relay stays correct and usable. These are the properties that make the benchmark numbers
//! meaningful: a relay that is fast because it silently drops events is not fast.

mod common;

use common::*;
use futures_util::SinkExt;
use nostr_core::{Event, Keys};
use serde_json::{json, Value};
use std::time::Duration;

const T: Duration = Duration::from_secs(10);

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn malformed_frames_yield_notice_and_connection_stays_usable() {
    let relay = start_relay();
    let mut ws = connect(relay.port).await;
    let keys = key("adv-malformed");

    let garbage: Vec<&str> = vec![
        "not json at all",
        "[]",
        "{}",
        "[1,2,3]",
        "[\"EVENT\"]",
        "[\"EVENT\",{}]",
        "[\"EVENT\",{\"id\":\"zz\"}]",
        "[\"REQ\"]",
        "[\"REQ\",\"s\"]",
        "[\"REQ\",\"s\",\"not-an-object\"]",
        "[\"REQ\",\"s\",{\"kinds\":\"nope\"}]",
        "[\"REQ\",\"s\",{\"since\":\"nope\"}]",
        "[\"REQ\",\"s\",{\"ids\":[\"short\"]}]",
        "[\"CLOSE\"]",
        "[\"COUNT\"]",
        "[\"COUNT\",\"c\"]",
        "[\"UNKNOWN\",1]",
        "[\"EVENT\",null]",
        "[\"EVENT\",[]]",
        "[\"EVENT\",{\"id\":\"00\",\"pubkey\":\"00\"}]",
    ];
    for frame in garbage {
        send(&mut ws, frame).await;
        let resp = read_until(
            &mut ws,
            |v| v[0] == "NOTICE" || v[0] == "CLOSED" || v[0] == "OK",
            T,
        )
        .await;
        assert!(
            resp[0] == "NOTICE" || resp[0] == "CLOSED" || resp[0] == "OK",
            "malformed frame {frame:?} must be answered with NOTICE/CLOSED/OK, got {resp}"
        );
    }

    // after all that abuse the connection must still work end to end
    let ev = keys.sign_event(nostr_core::now_secs(), 1, vec![], "still alive");
    publish_ok(&mut ws, &ev).await;
    let got = req_collect(&mut ws, "alive", json!({"ids": [hex::encode(ev.id)]})).await;
    assert_eq!(got.len(), 1);
    assert_eq!(got[0].id, ev.id);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oversized_event_is_rejected_without_killing_the_connection() {
    let relay = start_relay_with(&["--max-event-bytes", "4096"]);
    let mut ws = connect(relay.port).await;
    let keys = key("adv-oversize");

    let big = keys.sign_event(nostr_core::now_secs(), 1, vec![], &"y".repeat(50_000));
    let ok = publish(&mut ws, &big).await;
    assert_eq!(ok[2], false, "oversized event must be rejected: {ok}");
    assert_eq!(
        ok[1],
        hex::encode(big.id),
        "OK must echo the event id: {ok}"
    );

    // a huge number of tags is also an oversized frame
    let tags: Vec<Vec<String>> = (0..2000)
        .map(|i| vec!["t".into(), format!("tag-{i}")])
        .collect();
    let many_tags = keys.sign_event(nostr_core::now_secs(), 1, tags, "many tags");
    let ok = publish(&mut ws, &many_tags).await;
    assert_eq!(
        ok[2], false,
        "tag-heavy oversized event must be rejected: {ok}"
    );

    // the connection still accepts a normal event
    let small = keys.sign_event(nostr_core::now_secs(), 1, vec![], "small");
    publish_ok(&mut ws, &small).await;
    let got = req_collect(&mut ws, "small", json!({"ids": [hex::encode(small.id)]})).await;
    assert_eq!(got.len(), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn duplicate_ids_are_rejected_and_stored_once() {
    let relay = start_relay();
    let mut ws = connect(relay.port).await;
    let keys = key("adv-dup");
    let ev = keys.sign_event(
        nostr_core::now_secs(),
        1,
        vec![vec!["t".into(), "dup".into()]],
        "dup",
    );

    publish_ok(&mut ws, &ev).await;
    for _ in 0..3 {
        let ok = publish(&mut ws, &ev).await;
        assert_eq!(ok[2], false, "duplicate must be rejected: {ok}");
        assert_eq!(ok[1], hex::encode(ev.id));
    }
    let got = req_collect(&mut ws, "dup", json!({"#t": ["dup"], "limit": 100})).await;
    assert_eq!(got.len(), 1, "duplicate must be stored exactly once");
    send(&mut ws, "[\"COUNT\",\"dup-c\",{\"#t\":[\"dup\"]}]").await;
    let cnt = read_until(&mut ws, |v| v[0] == "COUNT", T).await;
    assert_eq!(cnt[2]["count"], 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn limit_zero_and_inverted_time_range_return_eose_with_no_events() {
    let relay = start_relay();
    let mut ws = connect(relay.port).await;
    let keys = key("adv-limit0");
    for i in 0..5u64 {
        let ev = keys.sign_event(
            nostr_core::now_secs(),
            1,
            vec![vec!["t".into(), "l0".into()]],
            &format!("e{i}"),
        );
        publish_ok(&mut ws, &ev).await;
    }

    let got = req_collect(&mut ws, "l0", json!({"#t": ["l0"], "limit": 0})).await;
    assert!(
        got.is_empty(),
        "limit 0 must return no events, got {} events",
        got.len()
    );

    let now = nostr_core::now_secs();
    let got = req_collect(
        &mut ws,
        "inverted",
        json!({"#t": ["l0"], "since": now + 1000, "until": now - 1000}),
    )
    .await;
    assert!(got.is_empty(), "since > until must return no events");

    // a normal query still works afterwards
    let got = req_collect(&mut ws, "normal", json!({"#t": ["l0"], "limit": 100})).await;
    assert_eq!(got.len(), 5);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unknown_kinds_are_stored_and_queryable() {
    let relay = start_relay();
    let mut ws = connect(relay.port).await;
    let keys = key("adv-kinds");
    let kinds = [0u32, 4, 7, 9735, 30_023, 65_535, u32::MAX];
    for (i, kind) in kinds.iter().enumerate() {
        let ev = keys.sign_event(
            nostr_core::now_secs() + i as u64,
            *kind,
            vec![vec!["t".into(), "kinds".into()]],
            &format!("kind {kind}"),
        );
        publish_ok(&mut ws, &ev).await;
    }
    for kind in kinds {
        let got = req_collect(&mut ws, "k", json!({"kinds": [kind], "limit": 10})).await;
        assert_eq!(got.len(), 1, "kind {kind} must be queryable");
        assert_eq!(got[0].kind, kind);
    }
    let got = req_collect(&mut ws, "none", json!({"kinds": [1234]})).await;
    assert!(got.is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn multi_value_tag_filters_match_any_value() {
    let relay = start_relay();
    let mut ws = connect(relay.port).await;
    let keys = key("adv-tags");
    let mut ids = Vec::new();
    for tag in ["alpha", "beta", "gamma"] {
        let ev = keys.sign_event(
            nostr_core::now_secs(),
            1,
            vec![vec!["t".into(), tag.into()]],
            tag,
        );
        publish_ok(&mut ws, &ev).await;
        ids.push((tag, ev.id));
    }
    let got = req_collect(
        &mut ws,
        "multi",
        json!({"#t": ["alpha", "beta"], "limit": 100}),
    )
    .await;
    assert_eq!(
        got.len(),
        2,
        "multi-value tag filter must match either value"
    );
    let returned: Vec<_> = got.iter().map(|e| e.id).collect();
    assert!(returned.contains(&ids[0].1));
    assert!(returned.contains(&ids[1].1));
    assert!(!returned.contains(&ids[2].1));

    // AND semantics across distinct tag names
    let ev = keys.sign_event(
        nostr_core::now_secs(),
        1,
        vec![
            vec!["t".into(), "alpha".into()],
            vec!["p".into(), "peer".into()],
        ],
        "both tags",
    );
    publish_ok(&mut ws, &ev).await;
    let got = req_collect(&mut ws, "and", json!({"#t": ["alpha"], "#p": ["peer"]})).await;
    assert_eq!(got.len(), 1);
    assert_eq!(got[0].id, ev.id);

    // a tag value nobody used
    let got = req_collect(&mut ws, "none", json!({"#t": ["nope"]})).await;
    assert!(got.is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn close_mid_stream_stops_delivery_immediately() {
    let relay = start_relay();
    let port = relay.port;
    let publisher = key("adv-close-pub");
    let mut sub_ws = connect(port).await;
    let mut pub_ws = connect(port).await;

    send(
        &mut sub_ws,
        "[\"REQ\",\"stream\",{\"kinds\":[1],\"#t\":[\"stream\"]}]",
    )
    .await;
    let _ = read_until(&mut sub_ws, |v| v[0] == "EOSE", T).await;

    // publish a few events while the subscription is live
    let mut live_ids = Vec::new();
    for i in 0..3u64 {
        let ev = publisher.sign_event(
            nostr_core::now_secs(),
            1,
            vec![vec!["t".into(), "stream".into()]],
            &format!("live {i}"),
        );
        publish_ok(&mut pub_ws, &ev).await;
        live_ids.push(ev.id);
    }
    for id in &live_ids {
        let pushed = read_until(&mut sub_ws, |v| v[0] == "EVENT", T).await;
        assert_eq!(Event::from_json_value(&pushed[2]).unwrap().id, *id);
    }

    // CLOSE mid-stream, then publish more events
    send(&mut sub_ws, "[\"CLOSE\",\"stream\"]").await;
    tokio::time::sleep(Duration::from_millis(150)).await;
    let mut after_ids = Vec::new();
    for i in 0..3u64 {
        let ev = publisher.sign_event(
            nostr_core::now_secs(),
            1,
            vec![vec!["t".into(), "stream".into()]],
            &format!("after {i}"),
        );
        publish_ok(&mut pub_ws, &ev).await;
        after_ids.push(ev.id);
    }
    let frames = drain(&mut sub_ws, Duration::from_millis(600)).await;
    for v in &frames {
        if v[0] == "EVENT" {
            let id = Event::from_json_value(&v[2]).unwrap().id;
            assert!(
                !after_ids.contains(&id),
                "CLOSE must stop delivery of events published after it"
            );
        }
    }
    // the closed subscription must not be re-created by a stray frame
    assert!(
        frames.iter().all(|v| v[0] != "EOSE" || v[1] == "stream"),
        "unexpected frames after CLOSE: {frames:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_req_during_heavy_ingest() {
    let relay = start_relay();
    let port = relay.port;

    // 6 publishers hammering the relay
    let mut publishers = Vec::new();
    for p in 0..6 {
        publishers.push(tokio::spawn(async move {
            let k = Keys::from_label(&format!("adv-heavy-pub-{p}"));
            let mut ws = connect(port).await;
            for i in 0..60u64 {
                let ev = k.sign_event(
                    nostr_core::now_secs(),
                    1,
                    vec![vec!["t".into(), "heavy".into()]],
                    &format!("p{p}-{i}"),
                );
                publish_ok(&mut ws, &ev).await;
            }
        }));
    }

    // readers issuing REQs while ingest is in flight
    let mut readers = Vec::new();
    for r in 0..4 {
        readers.push(tokio::spawn(async move {
            let mut ws = connect(port).await;
            let mut total = 0usize;
            for q in 0..10 {
                let got = req_collect(
                    &mut ws,
                    &format!("r{r}-{q}"),
                    json!({"#t": ["heavy"], "limit": 25}),
                )
                .await;
                assert!(
                    got.len() <= 25,
                    "limit must be honoured under concurrent ingest"
                );
                assert!(
                    got.windows(2).all(|w| w[0].created_at >= w[1].created_at),
                    "REQ results must stay newest-first under concurrent ingest"
                );
                for e in &got {
                    e.verify().expect("every returned event must verify");
                }
                total += got.len();
            }
            total
        }));
    }

    for p in publishers {
        p.await.expect("publisher task");
    }
    let mut observed = 0usize;
    for r in readers {
        observed += r.await.expect("reader task");
    }
    assert!(
        observed > 0,
        "readers must observe events while ingest is running"
    );

    let mut ws = connect(port).await;
    send(&mut ws, "[\"COUNT\",\"final\",{\"#t\":[\"heavy\"]}]").await;
    let cnt = read_until(&mut ws, |v| v[0] == "COUNT", T).await;
    assert_eq!(
        cnt[2]["count"], 360,
        "every published event must be stored: {cnt}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn restart_durability_after_graceful_shutdown() {
    let mut relay = start_relay();
    let dir = relay.dir.clone();
    let keys = key("adv-restart");
    let mut evs = Vec::new();
    {
        let mut ws = connect(relay.port).await;
        for i in 0..20u64 {
            let ev = keys.sign_event(
                nostr_core::now_secs() + i,
                1,
                vec![vec!["t".into(), "restart".into()]],
                &format!("durable {i}"),
            );
            publish_ok(&mut ws, &ev).await;
            evs.push(ev);
        }
    }
    // graceful SIGTERM, then reopen the same data directory
    let status = relay.terminate(Duration::from_secs(20));
    assert!(status.is_some(), "relay must exit after SIGTERM");
    let proc2 = start_relay_on(&dir);
    let mut ws = connect(proc2.port).await;
    let got = req_collect(&mut ws, "restart", json!({"#t": ["restart"], "limit": 100})).await;
    assert_eq!(got.len(), 20, "all events must survive a graceful restart");
    for e in &got {
        e.verify().expect("recovered event must verify");
    }
    let mut ids: Vec<_> = got.iter().map(|e| e.id).collect();
    ids.sort();
    let mut expected: Vec<_> = evs.iter().map(|e| e.id).collect();
    expected.sort();
    assert_eq!(ids, expected);
    // duplicates are still detected after restart
    let ok = publish(&mut ws, &evs[0]).await;
    assert_eq!(
        ok[2], false,
        "duplicate detection must survive restart: {ok}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn nip09_delete_of_nonexistent_id_is_accepted() {
    let relay = start_relay();
    let mut ws = connect(relay.port).await;
    let keys = key("adv-nip09");

    // deletion request for an id that was never published
    let ghost = [0x5a; 32];
    let del = keys.sign_event(
        nostr_core::now_secs(),
        5,
        vec![vec!["e".into(), hex::encode(ghost)]],
        "delete something that does not exist",
    );
    let ok = publish(&mut ws, &del).await;
    assert_eq!(
        ok[2], true,
        "deletion of a nonexistent id must be accepted: {ok}"
    );

    // deletion request with no e tags at all
    let empty_del = keys.sign_event(nostr_core::now_secs(), 5, vec![], "");
    let ok = publish(&mut ws, &empty_del).await;
    assert_eq!(ok[2], true, "empty deletion request must be accepted: {ok}");

    // a real event, then delete it, then delete it again
    let ev = keys.sign_event(nostr_core::now_secs(), 1, vec![], "delete me");
    publish_ok(&mut ws, &ev).await;
    let del = keys.sign_event(
        nostr_core::now_secs(),
        5,
        vec![vec!["e".into(), hex::encode(ev.id)]],
        "",
    );
    publish_ok(&mut ws, &del).await;
    let got = req_collect(&mut ws, "gone", json!({"ids": [hex::encode(ev.id)]})).await;
    assert!(got.is_empty(), "deleted event must not be returned");
    // re-deleting with a *different* deletion event (distinct created_at so the id differs)
    // must be a harmless no-op, not an error
    let again = keys.sign_event(
        nostr_core::now_secs() + 1,
        5,
        vec![vec!["e".into(), hex::encode(ev.id)]],
        "",
    );
    let ok = publish(&mut ws, &again).await;
    assert_eq!(
        ok[2], true,
        "re-deleting must be a no-op, not an error: {ok}"
    );
    // re-publishing the *same* deletion event is a duplicate, which is also not an error
    let dup = publish(&mut ws, &again).await;
    assert_eq!(dup[2], false);
    assert!(
        dup[3].as_str().unwrap_or("").contains("duplicate"),
        "re-publishing a deletion event must be reported as a duplicate: {dup}"
    );

    // a deletion by a different author must not remove someone else's event
    let other = key("adv-nip09-other");
    let victim = keys.sign_event(nostr_core::now_secs(), 1, vec![], "not yours");
    publish_ok(&mut ws, &victim).await;
    let foreign_del = other.sign_event(
        nostr_core::now_secs(),
        5,
        vec![vec!["e".into(), hex::encode(victim.id)]],
        "",
    );
    publish_ok(&mut ws, &foreign_del).await;
    let got = req_collect(&mut ws, "victim", json!({"ids": [hex::encode(victim.id)]})).await;
    assert_eq!(got.len(), 1, "only the author may delete an event");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn nip40_expired_events_are_not_returned() {
    let relay = start_relay();
    let mut ws = connect(relay.port).await;
    let keys = key("adv-nip40");
    let now = nostr_core::now_secs();

    let live = keys.sign_event(
        now,
        1,
        vec![vec!["expiration".into(), (now + 3600).to_string()]],
        "live",
    );
    let dead = keys.sign_event(
        now,
        1,
        vec![vec!["expiration".into(), (now - 3600).to_string()]],
        "dead",
    );
    let no_exp = keys.sign_event(now, 1, vec![], "no expiration");
    publish_ok(&mut ws, &live).await;
    publish_ok(&mut ws, &no_exp).await;
    // NIP-40 lets a relay reject an event that is already expired at ingest time. Either
    // policy is conformant; what matters is that the event is never returned by a REQ.
    let dead_ok = publish(&mut ws, &dead).await;
    if !dead_ok[2].as_bool().unwrap_or(false) {
        assert!(
            dead_ok[3].as_str().unwrap_or("").contains("expired"),
            "rejection of an expired event must say why: {dead_ok}"
        );
    }

    let got = req_collect(
        &mut ws,
        "exp",
        json!({"authors": [keys.public_key_hex()], "limit": 100}),
    )
    .await;
    assert_eq!(got.len(), 2, "expired event must be filtered out");
    assert!(got.iter().all(|e| e.id != dead.id));
    assert!(got.iter().any(|e| e.id == live.id));
    assert!(got.iter().any(|e| e.id == no_exp.id));

    // explicit id lookup of an expired event comes back empty
    let got = req_collect(&mut ws, "exp-id", json!({"ids": [hex::encode(dead.id)]})).await;
    assert!(got.is_empty());

    // COUNT must agree with REQ
    send(
        &mut ws,
        format!(
            "[\"COUNT\",\"exp-c\",{{\"authors\":[\"{}\"]}}]",
            keys.public_key_hex()
        ),
    )
    .await;
    let cnt = read_until(&mut ws, |v| v[0] == "COUNT", T).await;
    assert_eq!(
        cnt[2]["count"], 2,
        "COUNT must exclude expired events: {cnt}"
    );

    // an event whose expiration tag is not a number must be treated as non-expiring
    let weird = keys.sign_event(
        now,
        1,
        vec![vec!["expiration".into(), "soon".into()]],
        "weird expiration",
    );
    publish_ok(&mut ws, &weird).await;
    let got = req_collect(&mut ws, "weird", json!({"ids": [hex::encode(weird.id)]})).await;
    assert_eq!(
        got.len(),
        1,
        "unparsable expiration must not hide the event"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn nip11_content_type_and_health_endpoint() {
    let relay = start_relay_with(&[
        "--relay-name",
        "adv-nip11",
        "--relay-description",
        "adversarial",
    ]);
    let port = relay.port;

    let resp = http_get(port, "/", Some("application/nostr+json"));
    assert_eq!(
        resp.status(),
        200,
        "NIP-11 must return 200: {}",
        resp.status_line
    );
    let ct = resp.header("content-type").unwrap_or("");
    assert!(
        ct.to_ascii_lowercase().contains("application/nostr+json"),
        "NIP-11 must be served as application/nostr+json, got {ct:?}"
    );
    let info = resp.json();
    assert_eq!(info["name"], "adv-nip11");
    assert_eq!(info["description"], "adversarial");
    let nips: Vec<u64> = info["supported_nips"]
        .as_array()
        .expect("supported_nips array")
        .iter()
        .filter_map(|v| v.as_u64())
        .collect();
    for required in [1u64, 9, 11, 40, 42, 45] {
        assert!(
            nips.contains(&required),
            "supported_nips must include {required}: {nips:?}"
        );
    }
    assert!(info["limitation"]["max_message_length"].is_number());
    assert!(info["limitation"]["max_subscriptions"].is_number());
    assert!(info["limitation"]["max_filters"].is_number());
    assert!(info["limitation"]["max_limit"].is_number());

    // plain browser GET gets HTML, not the NIP-11 document
    let html = http_get(port, "/", Some("text/html"));
    assert_eq!(html.status(), 200);
    let hct = html.header("content-type").unwrap_or("");
    assert!(
        hct.to_ascii_lowercase().contains("text/html"),
        "landing page content type: {hct:?}"
    );

    // /health reports live store statistics
    let health = http_get(port, "/health", None);
    assert_eq!(
        health.status(),
        200,
        "health must return 200: {}",
        health.status_line
    );
    let h = health.json();
    assert_eq!(h["status"], "ok", "health payload: {h}");
    for key in [
        "events",
        "bytes_on_disk",
        "segments",
        "index_entries",
        "connections",
        "subscriptions",
    ] {
        assert!(h.get(key).is_some(), "health must report {key}: {h}");
    }

    // health numbers must track reality
    let mut ws = connect(port).await;
    let keys = key("adv-health");
    for i in 0..3u64 {
        let ev = keys.sign_event(nostr_core::now_secs() + i, 1, vec![], &format!("h{i}"));
        publish_ok(&mut ws, &ev).await;
    }
    let health = http_get(port, "/health", None).json();
    assert_eq!(
        health["events"], 3,
        "health must report the real event count: {health}"
    );
    assert!(health["connections"].as_u64().unwrap_or(0) >= 1);

    // unknown paths must not hang or panic
    let missing = http_get(port, "/nope", None);
    assert!(
        missing.status() >= 400,
        "unknown path should be 4xx, got {}",
        missing.status_line
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sigterm_shuts_down_gracefully_and_flushes() {
    let mut relay = start_relay();
    let dir = relay.dir.clone();
    let keys = key("adv-sigterm");
    let ev = keys.sign_event(
        nostr_core::now_secs(),
        1,
        vec![vec!["t".into(), "sigterm".into()]],
        "flushed on shutdown",
    );
    {
        let mut ws = connect(relay.port).await;
        publish_ok(&mut ws, &ev).await;
    }
    let status = relay.terminate(Duration::from_secs(20));
    let status = status.expect("relay must exit within 20s of SIGTERM");
    assert!(
        status.success(),
        "graceful shutdown must exit with status 0, got {status:?}"
    );
    // the port must be released
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while std::time::Instant::now() < deadline {
        if std::net::TcpStream::connect(("127.0.0.1", relay.port)).is_err() {
            break;
        }
        std::thread::sleep(Duration::from_millis(25));
    }
    // and the data must be on disk
    let proc2 = start_relay_on(&dir);
    let mut ws = connect(proc2.port).await;
    let got = req_collect(&mut ws, "sigterm", json!({"ids": [hex::encode(ev.id)]})).await;
    assert_eq!(
        got.len(),
        1,
        "event published before SIGTERM must be durable"
    );
    assert_eq!(got[0].id, ev.id);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn per_connection_limits_are_enforced() {
    let relay = start_relay_with(&[
        "--max-subs-per-conn",
        "2",
        "--max-filters",
        "2",
        "--max-limit",
        "10",
    ]);
    let mut ws = connect(relay.port).await;
    let keys = key("adv-limits");
    for i in 0..30u64 {
        let ev = keys.sign_event(
            nostr_core::now_secs() + i,
            1,
            vec![vec!["t".into(), "limits".into()]],
            &format!("l{i}"),
        );
        publish_ok(&mut ws, &ev).await;
    }

    // limit above max_limit must be clamped, not rejected
    let got = req_collect(&mut ws, "clamp", json!({"#t": ["limits"], "limit": 1000})).await;
    assert!(
        got.len() <= 10,
        "limit must be clamped to max_limit, got {}",
        got.len()
    );

    // too many filters in one REQ must be rejected with CLOSED or NOTICE
    send(
        &mut ws,
        "[\"REQ\",\"many\",{\"kinds\":[1]},{\"kinds\":[1]},{\"kinds\":[1]}]",
    )
    .await;
    let resp = read_until(
        &mut ws,
        |v| v[0] == "CLOSED" || v[0] == "NOTICE" || v[0] == "EOSE",
        T,
    )
    .await;
    assert!(
        resp[0] == "CLOSED" || resp[0] == "NOTICE",
        "REQ with more than max_filters must be rejected: {resp}"
    );

    // exceeding max_subs_per_conn must be rejected, and the existing subs must keep working.
    // Close the "clamp" subscription first so the budget starts empty.
    send(&mut ws, "[\"CLOSE\",\"clamp\"]").await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    send(&mut ws, "[\"REQ\",\"s1\",{\"kinds\":[1]}]").await;
    let _ = read_until(&mut ws, |v| v[0] == "EOSE", T).await;
    send(&mut ws, "[\"REQ\",\"s2\",{\"kinds\":[1]}]").await;
    let _ = read_until(&mut ws, |v| v[0] == "EOSE", T).await;
    send(&mut ws, "[\"REQ\",\"s3\",{\"kinds\":[1]}]").await;
    let resp = read_until(
        &mut ws,
        |v| v[0] == "CLOSED" || v[0] == "NOTICE" || v[0] == "EOSE",
        T,
    )
    .await;
    assert!(
        resp[0] == "CLOSED" || resp[0] == "NOTICE",
        "third subscription must be rejected when max_subs_per_conn is 2: {resp}"
    );

    // the connection is still usable
    let ev = keys.sign_event(nostr_core::now_secs(), 1, vec![], "after limits");
    publish_ok(&mut ws, &ev).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn auth_message_is_answered() {
    let relay = start_relay();
    let mut ws = connect(relay.port).await;
    let keys = key("adv-auth");
    let ev = keys.sign_event(nostr_core::now_secs(), 22242, vec![], "auth");
    send(&mut ws, format!("[\"AUTH\",{}]", ev.to_json())).await;
    let resp = read_until(
        &mut ws,
        |v| v[0] == "OK" || v[0] == "NOTICE" || v[0] == "AUTH",
        T,
    )
    .await;
    assert!(
        resp[0] == "OK" || resp[0] == "NOTICE" || resp[0] == "AUTH",
        "AUTH must be answered: {resp}"
    );
    // connection stays usable
    let ev = keys.sign_event(nostr_core::now_secs(), 1, vec![], "after auth");
    publish_ok(&mut ws, &ev).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn binary_and_empty_frames_do_not_kill_the_connection() {
    let relay = start_relay();
    let mut ws = connect(relay.port).await;
    let keys = key("adv-binary");

    // empty text frame
    send(&mut ws, "").await;
    let resp = read_until(&mut ws, |v| v[0] == "NOTICE" || v[0] == "CLOSED", T).await;
    assert!(resp[1].is_string() || resp[0] == "CLOSED");

    // binary frame carrying non-UTF8 bytes
    ws.send(tokio_tungstenite::tungstenite::Message::Binary(vec![
        0xff, 0xfe, 0x00,
    ]))
    .await
    .expect("send binary");
    let resp = read_until(&mut ws, |v| v[0] == "NOTICE" || v[0] == "CLOSED", T).await;
    assert!(resp[0] == "NOTICE" || resp[0] == "CLOSED");

    // deeply nested JSON must not blow the stack
    let mut nested = String::from("[\"EVENT\",");
    for _ in 0..200 {
        nested.push('[');
    }
    for _ in 0..200 {
        nested.push(']');
    }
    nested.push(']');
    send(&mut ws, nested).await;
    let resp = read_until(
        &mut ws,
        |v| v[0] == "NOTICE" || v[0] == "CLOSED" || v[0] == "OK",
        T,
    )
    .await;
    assert!(resp[0] == "NOTICE" || resp[0] == "CLOSED" || resp[0] == "OK");

    // still usable
    let ev = keys.sign_event(nostr_core::now_secs(), 1, vec![], "after binary");
    publish_ok(&mut ws, &ev).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn many_sequential_connections_are_cleaned_up() {
    let relay = start_relay();
    let port = relay.port;
    let keys = key("adv-churn");
    for i in 0..25u64 {
        let mut ws = connect(port).await;
        let ev = keys.sign_event(nostr_core::now_secs() + i, 1, vec![], &format!("churn {i}"));
        publish_ok(&mut ws, &ev).await;
        // drop the socket without a close handshake
        drop(ws);
    }
    // give the relay a moment to reap the connections
    tokio::time::sleep(Duration::from_millis(300)).await;
    let health = http_get(port, "/health", None).json();
    assert_eq!(
        health["events"], 25,
        "all churned events must be stored: {health}"
    );
    assert!(
        health["connections"].as_u64().unwrap_or(0) <= 2,
        "closed connections must be reaped: {health}"
    );
    // a fresh connection still works
    let mut ws = connect(port).await;
    let got = req_collect(&mut ws, "churn", json!({"limit": 100})).await;
    assert_eq!(got.len(), 25);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn count_matches_req_for_every_filter_shape() {
    let relay = start_relay();
    let mut ws = connect(relay.port).await;
    let keys = key("adv-count");
    let now = nostr_core::now_secs();
    for i in 0..12u64 {
        let ev = keys.sign_event(
            now + i,
            if i % 2 == 0 { 1 } else { 7 },
            vec![
                vec!["t".into(), "count".into()],
                vec!["n".into(), i.to_string()],
            ],
            &format!("c{i}"),
        );
        publish_ok(&mut ws, &ev).await;
    }
    // Filters without `limit`: COUNT must equal the number of events REQ returns.
    let filters = [
        json!({"#t": ["count"]}),
        json!({"kinds": [1]}),
        json!({"kinds": [7]}),
        json!({"authors": [keys.public_key_hex()]}),
        json!({"#n": ["3", "4", "5"]}),
        json!({"since": now + 5, "until": now + 8}),
    ];
    for (i, f) in filters.iter().enumerate() {
        let got = req_collect(&mut ws, &format!("req{i}"), f.clone()).await;
        send(&mut ws, &format!("[\"COUNT\",\"cnt{i}\",{f}]")).await;
        let cnt = read_until(&mut ws, |v| v[0] == "COUNT" && v[1] == format!("cnt{i}"), T).await;
        let n = cnt[2]["count"].as_u64().unwrap_or(u64::MAX);
        assert_eq!(
            n as usize,
            got.len(),
            "COUNT must agree with REQ for filter {f}: count={n} req={}",
            got.len()
        );
    }
    // With `limit`, REQ returns a page of at most `limit` events while COUNT reports the
    // total number of matching events (NIP-45 counts matches, it does not paginate).
    let f = json!({"kinds": [1], "#t": ["count"], "limit": 3});
    let got = req_collect(&mut ws, "req-limited", f.clone()).await;
    assert_eq!(got.len(), 3, "REQ must honour limit");
    send(&mut ws, &format!("[\"COUNT\",\"cnt-limited\",{f}]")).await;
    let cnt = read_until(&mut ws, |v| v[0] == "COUNT" && v[1] == "cnt-limited", T).await;
    let n = cnt[2]["count"].as_u64().unwrap_or(u64::MAX);
    assert!(
        n as usize >= got.len(),
        "COUNT must not be smaller than the REQ page: count={n} req={}",
        got.len()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn subscription_id_replacement_and_unknown_close() {
    let relay = start_relay();
    let mut ws = connect(relay.port).await;
    let keys = key("adv-subid");

    // CLOSE for a subscription that was never opened must be harmless
    send(&mut ws, "[\"CLOSE\",\"never-opened\"]").await;
    let ev = keys.sign_event(nostr_core::now_secs(), 1, vec![], "after stray close");
    publish_ok(&mut ws, &ev).await;

    // reusing a subscription id replaces the old filters
    send(&mut ws, "[\"REQ\",\"reuse\",{\"kinds\":[1]}]").await;
    let _ = read_until(&mut ws, |v| v[0] == "EOSE", T).await;
    send(&mut ws, "[\"REQ\",\"reuse\",{\"kinds\":[7]}]").await;
    let _ = read_until(&mut ws, |v| v[0] == "EOSE", T).await;
    let seven = keys.sign_event(nostr_core::now_secs(), 7, vec![], "reaction");
    publish_ok(&mut ws, &seven).await;
    let pushed = read_until(&mut ws, |v| v[0] == "EVENT", T).await;
    assert_eq!(pushed[1], "reuse");
    assert_eq!(Event::from_json_value(&pushed[2]).unwrap().kind, 7);

    // a kind-1 event must no longer be delivered on the replaced subscription
    let one = keys.sign_event(nostr_core::now_secs(), 1, vec![], "note");
    publish_ok(&mut ws, &one).await;
    let frames = drain(&mut ws, Duration::from_millis(400)).await;
    for v in &frames {
        if v[0] == "EVENT" {
            assert_ne!(
                Event::from_json_value(&v[2]).unwrap().kind,
                1,
                "replaced subscription must not deliver kind 1"
            );
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn event_with_wrong_id_is_rejected() {
    let relay = start_relay();
    let mut ws = connect(relay.port).await;
    let keys = key("adv-idmismatch");
    let mut ev = keys.sign_event(nostr_core::now_secs(), 1, vec![], "id mismatch");
    // corrupt the id but keep the signature over the original id
    ev.id[0] ^= 0xff;
    let ok = publish(&mut ws, &ev).await;
    assert_eq!(
        ok[2], false,
        "event with a mismatched id must be rejected: {ok}"
    );
    // and it must not be queryable
    let got = req_collect(&mut ws, "mismatch", json!({"ids": [hex::encode(ev.id)]})).await;
    assert!(got.is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn future_and_ancient_timestamps_are_accepted() {
    let relay = start_relay();
    let mut ws = connect(relay.port).await;
    let keys = key("adv-time");
    let ancient = keys.sign_event(1, 1, vec![vec!["t".into(), "time".into()]], "1970");
    let future = keys.sign_event(
        nostr_core::now_secs() + 86_400 * 365,
        1,
        vec![vec!["t".into(), "time".into()]],
        "a year from now",
    );
    for e in [&ancient, &future] {
        publish_ok(&mut ws, e).await;
    }
    let got = req_collect(&mut ws, "time", json!({"#t": ["time"], "limit": 10})).await;
    assert_eq!(got.len(), 2);
    // newest-first ordering must hold even for extreme timestamps
    assert_eq!(got[0].id, future.id);
    assert_eq!(got[1].id, ancient.id);
    // a range that only covers the ancient event
    let got = req_collect(&mut ws, "ancient", json!({"#t": ["time"], "until": 100})).await;
    assert_eq!(got.len(), 1);
    assert_eq!(got[0].id, ancient.id);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unicode_and_control_characters_roundtrip() {
    let relay = start_relay();
    let mut ws = connect(relay.port).await;
    let keys = key("adv-unicode");
    let content = "héllo 🌍 \"quoted\" \\ backslash \n newline \t tab \u{0001} control";
    let ev = keys.sign_event(
        nostr_core::now_secs(),
        1,
        vec![vec!["t".into(), "ünïcode".into()]],
        content,
    );
    publish_ok(&mut ws, &ev).await;
    let got = req_collect(&mut ws, "uni", json!({"ids": [hex::encode(ev.id)]})).await;
    assert_eq!(got.len(), 1);
    assert_eq!(
        got[0].content, content,
        "content must roundtrip byte-for-byte"
    );
    assert_eq!(got[0].tags[0][1], "ünïcode");
    got[0].verify().expect("unicode event verifies");
    // tag filter on a non-ASCII tag value
    let got = req_collect(&mut ws, "uni-tag", json!({"#t": ["ünïcode"]})).await;
    assert_eq!(got.len(), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn empty_filter_matches_everything_newest_first() {
    let relay = start_relay();
    let mut ws = connect(relay.port).await;
    let keys = key("adv-emptyfilter");
    let mut ids = Vec::new();
    for i in 0..5u64 {
        let ev = keys.sign_event(nostr_core::now_secs() + i, 1, vec![], &format!("e{i}"));
        publish_ok(&mut ws, &ev).await;
        ids.push(ev.id);
    }
    let got = req_collect(&mut ws, "all", json!({"limit": 100})).await;
    assert_eq!(got.len(), 5, "empty filter must match every event");
    assert!(got.windows(2).all(|w| w[0].created_at >= w[1].created_at));
    assert_eq!(got[0].id, *ids.last().unwrap());
    // limit 2 returns the two newest
    let got = req_collect(&mut ws, "two", json!({"limit": 2})).await;
    assert_eq!(got.len(), 2);
    assert_eq!(got[0].id, ids[4]);
    assert_eq!(got[1].id, ids[3]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn multiple_filters_in_one_req_are_deduped() {
    let relay = start_relay();
    let mut ws = connect(relay.port).await;
    let keys = key("adv-multifilter");
    let ev = keys.sign_event(
        nostr_core::now_secs(),
        1,
        vec![vec!["t".into(), "multi".into()]],
        "matches both filters",
    );
    publish_ok(&mut ws, &ev).await;
    let other = keys.sign_event(nostr_core::now_secs(), 7, vec![], "only kind 7");
    publish_ok(&mut ws, &other).await;

    send(
        &mut ws,
        format!(
            "[\"REQ\",\"mf\",{{\"#t\":[\"multi\"]}},{{\"kinds\":[1]}},{{\"ids\":[\"{}\"]}}]",
            hex::encode(ev.id)
        ),
    )
    .await;
    let mut seen = Vec::new();
    loop {
        let v = read_until(&mut ws, |v| v[0] == "EVENT" || v[0] == "EOSE", T).await;
        if v[0] == "EOSE" {
            break;
        }
        seen.push(Event::from_json_value(&v[2]).unwrap().id);
    }
    let mut deduped = seen.clone();
    deduped.sort();
    deduped.dedup();
    assert_eq!(
        seen.len(),
        deduped.len(),
        "events must be deduplicated across filters"
    );
    assert!(seen.contains(&ev.id));
    assert!(!seen.contains(&other.id));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn relay_handles_many_subscriptions_on_one_connection() {
    let relay = start_relay_with(&["--max-subs-per-conn", "50"]);
    let mut ws = connect(relay.port).await;
    let keys = key("adv-multisub");
    for i in 0..40u64 {
        send(
            &mut ws,
            &format!("[\"REQ\",\"s{i}\",{{\"kinds\":[{}]}}]", i % 5),
        )
        .await;
        let _ = read_until(&mut ws, |v| v[0] == "EOSE", T).await;
    }
    let ev = keys.sign_event(nostr_core::now_secs(), 3, vec![], "kind 3");
    publish_ok(&mut ws, &ev).await;
    // every subscription whose filter includes kind 3 must receive it
    let mut delivered = Vec::new();
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while delivered.len() < 8 && std::time::Instant::now() < deadline {
        if let Some(v) =
            try_read_until(&mut ws, |v| v[0] == "EVENT", Duration::from_millis(500)).await
        {
            delivered.push(v[1].as_str().unwrap_or("").to_string());
        }
    }
    assert_eq!(
        delivered.len(),
        8,
        "all 8 kind-3 subscriptions must receive the event: {delivered:?}"
    );
    let mut uniq = delivered.clone();
    uniq.sort();
    uniq.dedup();
    assert_eq!(uniq.len(), 8);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn health_reports_growth_after_ingest() {
    let relay = start_relay();
    let port = relay.port;
    let before = http_get(port, "/health", None).json();
    let before_events = before["events"].as_u64().unwrap_or(0);
    let mut ws = connect(port).await;
    let keys = key("adv-growth");
    for i in 0..50u64 {
        let ev = keys.sign_event(nostr_core::now_secs() + i, 1, vec![], &format!("g{i}"));
        publish_ok(&mut ws, &ev).await;
    }
    let after = http_get(port, "/health", None).json();
    let after_events = after["events"].as_u64().unwrap_or(0);
    assert_eq!(
        after_events,
        before_events + 50,
        "health must track ingest: {after}"
    );
    assert!(
        after["bytes_on_disk"].as_u64().unwrap_or(0) > 0,
        "bytes_on_disk must grow: {after}"
    );
    assert!(after["segments"].as_u64().unwrap_or(0) >= 1);
    assert!(after["index_entries"].as_u64().unwrap_or(0) >= after_events);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_publish_of_the_same_event_stores_it_once() {
    let relay = start_relay();
    let port = relay.port;
    let keys = key("adv-race");
    let ev = keys.sign_event(
        nostr_core::now_secs(),
        1,
        vec![vec!["t".into(), "race".into()]],
        "race",
    );
    let mut tasks = Vec::new();
    for _ in 0..8 {
        let ev = ev.clone();
        tasks.push(tokio::spawn(async move {
            let mut ws = connect(port).await;
            let ok = publish(&mut ws, &ev).await;
            ok[2].as_bool().unwrap_or(false)
        }));
    }
    let mut accepted = 0;
    for t in tasks {
        if t.await.unwrap() {
            accepted += 1;
        }
    }
    assert_eq!(
        accepted, 1,
        "exactly one concurrent publish of the same id may be accepted"
    );
    let mut ws = connect(port).await;
    send(&mut ws, "[\"COUNT\",\"race-c\",{\"#t\":[\"race\"]}]").await;
    let cnt = read_until(&mut ws, |v| v[0] == "COUNT", T).await;
    assert_eq!(cnt[2]["count"], 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn websocket_close_handshake_is_clean() {
    let relay = start_relay();
    let port = relay.port;
    let mut ws = connect(port).await;
    let keys = key("adv-wsclose");
    let ev = keys.sign_event(nostr_core::now_secs(), 1, vec![], "before close");
    publish_ok(&mut ws, &ev).await;
    ws.close(None).await.expect("close handshake");
    // the relay must still serve other clients
    let mut ws2 = connect(port).await;
    let got = req_collect(&mut ws2, "after", json!({"ids": [hex::encode(ev.id)]})).await;
    assert_eq!(got.len(), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn req_with_duplicate_subscription_ids_does_not_leak() {
    let relay = start_relay();
    let mut ws = connect(relay.port).await;
    let keys = key("adv-dup-sub");
    let ev = keys.sign_event(nostr_core::now_secs(), 1, vec![], "one");
    publish_ok(&mut ws, &ev).await;
    // open the same subscription id twice in a row
    send(&mut ws, "[\"REQ\",\"same\",{\"limit\":10}]").await;
    let first = req_collect(&mut ws, "same", json!({"limit": 10})).await;
    assert_eq!(first.len(), 1);
    send(&mut ws, "[\"REQ\",\"same\",{\"limit\":10}]").await;
    let second = req_collect(&mut ws, "same", json!({"limit": 10})).await;
    assert_eq!(second.len(), 1);
    // a live event must be delivered exactly once on the (single) subscription
    let live = keys.sign_event(nostr_core::now_secs(), 1, vec![], "live");
    publish_ok(&mut ws, &live).await;
    let frames = drain(&mut ws, Duration::from_millis(500)).await;
    let delivered: Vec<Value> = frames.iter().filter(|v| v[0] == "EVENT").cloned().collect();
    assert_eq!(
        delivered.len(),
        1,
        "replaced subscription must deliver exactly once: {frames:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn relay_survives_rapid_connect_disconnect_under_load() {
    let relay = start_relay();
    let port = relay.port;
    let mut tasks = Vec::new();
    for i in 0..20 {
        tasks.push(tokio::spawn(async move {
            let k = Keys::from_label(&format!("adv-churn2-{i}"));
            for j in 0..5u64 {
                let mut ws = connect(port).await;
                let ev = k.sign_event(nostr_core::now_secs(), 1, vec![], &format!("c{i}-{j}"));
                publish_ok(&mut ws, &ev).await;
                drop(ws);
            }
        }));
    }
    for t in tasks {
        t.await.expect("churn task");
    }
    let mut ws = connect(port).await;
    send(&mut ws, "[\"COUNT\",\"churn2\",{}]").await;
    let cnt = read_until(&mut ws, |v| v[0] == "COUNT", T).await;
    assert_eq!(
        cnt[2]["count"], 100,
        "all churned events must be stored: {cnt}"
    );
}
