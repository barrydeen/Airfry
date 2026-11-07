//! HTTP surface tests: NIP-11 relay information document, `/health`, landing page, 404.

mod common;

use common::*;
use nostr_relay::config::Config;
use serde_json::Value;

#[tokio::test]
async fn nip11_document_served_with_accept_header() {
    let mut cfg = Config::default();
    cfg.relay_name = "test-relay".to_string();
    cfg.relay_description = "integration test relay".to_string();
    cfg.relay_pubkey = Some("aa".repeat(32));
    cfg.relay_contact = "mailto:ops@example.com".to_string();
    cfg.max_limit = 123;
    let relay = spawn_relay(cfg).await;

    let (status, head, body) = http_get(relay.addr, "/", Some("application/nostr+json")).await;
    assert!(status.starts_with("HTTP/1.1 200"), "status: {status}");
    assert!(
        head.to_lowercase()
            .contains("content-type: application/nostr+json"),
        "head: {head}"
    );
    let doc: Value = serde_json::from_str(&body).expect("valid json");
    assert_eq!(doc["name"], "test-relay");
    assert_eq!(doc["description"], "integration test relay");
    assert_eq!(doc["pubkey"], "aa".repeat(32));
    assert_eq!(doc["contact"], "mailto:ops@example.com");
    assert_eq!(doc["software"], "nostr-relay");
    assert!(doc["version"].is_string());
    let nips: Vec<u64> = doc["supported_nips"]
        .as_array()
        .expect("supported_nips array")
        .iter()
        .map(|v| v.as_u64().unwrap())
        .collect();
    for expected in [1u64, 9, 11, 40, 42, 45] {
        assert!(
            nips.contains(&expected),
            "missing NIP {expected} in {nips:?}"
        );
    }
    assert_eq!(doc["limitation"]["max_limit"], 123);
    assert_eq!(doc["limitation"]["auth_required"], false);
}

#[tokio::test]
async fn plain_get_returns_landing_page() {
    let relay = spawn_relay(Config::default()).await;
    let (status, head, body) = http_get(relay.addr, "/", None).await;
    assert!(status.starts_with("HTTP/1.1 200"), "status: {status}");
    assert!(
        head.to_lowercase().contains("content-type: text/html"),
        "head: {head}"
    );
    assert!(body.contains("<html>"));
    assert!(body.contains("Nostr relay"));
}

#[tokio::test]
async fn health_endpoint_reports_store_stats() {
    let relay = spawn_relay(Config::default()).await;
    let mut c = connect(relay.addr).await;
    let keys = nostr_core::Keys::from_label("health-author");
    let ev = signed_event(&keys, 1, vec![], "health check");
    c.send_raw(&event_frame(&ev)).await;
    let ok = c
        .wait_for("OK", std::time::Duration::from_secs(5))
        .await
        .expect("OK");
    assert_eq!(ok[2], true);

    let (status, _head, body) = http_get(relay.addr, "/health", None).await;
    assert!(status.starts_with("HTTP/1.1 200"), "status: {status}");
    let doc: Value = serde_json::from_str(&body).expect("valid json");
    assert_eq!(doc["status"], "ok");
    assert_eq!(doc["events"], 1);
    assert!(doc["connections"].as_u64().unwrap() >= 1);
    assert!(doc["bytes_on_disk"].is_number());
    assert!(doc["segments"].is_number());
}

#[tokio::test]
async fn unknown_path_returns_404() {
    let relay = spawn_relay(Config::default()).await;
    let (status, _head, _body) = http_get(relay.addr, "/nope", None).await;
    assert!(status.starts_with("HTTP/1.1 404"), "status: {status}");
}

#[tokio::test]
async fn websocket_upgrade_on_root_path() {
    let relay = spawn_relay(Config::default()).await;
    let c = connect(relay.addr).await;
    assert_eq!(
        c.challenge.len(),
        32,
        "challenge should be 16 random bytes hex"
    );
}
