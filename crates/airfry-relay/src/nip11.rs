//! NIP-11 relay information document and the `/health` endpoint payload.

use crate::config::Config;
use airfry_store::StoreStats;
use serde_json::{json, Value};

/// NIPs implemented by this relay.
pub const SUPPORTED_NIPS: &[u32] = &[1, 9, 11, 40, 42, 45];

/// Build the NIP-11 relay information document.
pub fn relay_info(cfg: &Config) -> Value {
    let mut doc = json!({
        "name": cfg.relay_name,
        "description": cfg.relay_description,
        "supported_nips": SUPPORTED_NIPS,
        "software": "airfry-relay",
        "version": env!("CARGO_PKG_VERSION"),
        "limitation": {
            "max_message_length": cfg.max_event_bytes,
            "max_subscriptions": cfg.max_subs_per_conn,
            "max_filters": cfg.max_filters,
            "max_limit": cfg.max_limit,
            "max_subid_length": 64,
            "min_pow_difficulty": 0,
            "auth_required": cfg.require_auth,
            "payment_required": false,
            "restricted_writes": cfg.require_auth,
        },
    });
    if let Some(pk) = &cfg.relay_pubkey {
        doc["pubkey"] = Value::String(pk.clone());
    }
    if !cfg.relay_contact.is_empty() {
        doc["contact"] = Value::String(cfg.relay_contact.clone());
    }
    doc
}

/// Build the `/health` payload from live store statistics.
pub fn health(stats: &StoreStats, connections: usize, subscriptions: usize) -> Value {
    json!({
        "status": "ok",
        "events": stats.events,
        "bytes_on_disk": stats.bytes_on_disk,
        "segments": stats.segments,
        "index_entries": stats.index_entries,
        "connections": connections,
        "subscriptions": subscriptions,
        "version": env!("CARGO_PKG_VERSION"),
    })
}

/// A small human-readable landing page for plain browser GETs.
pub fn landing_page(cfg: &Config) -> String {
    format!(
        "<!doctype html><html><head><meta charset=\"utf-8\"><title>{name}</title></head>\
<body><h1>{name}</h1><p>{desc}</p>\
<p>This is a Nostr relay. Connect with a WebSocket client at <code>ws://{bind}/</code>.</p>\
<p>Relay information document: <code>GET /</code> with <code>Accept: application/nostr+json</code>.</p>\
<p>Health: <code>GET /health</code>.</p>\
<p>Supported NIPs: {nips}</p></body></html>",
        name = html_escape(&cfg.relay_name),
        desc = html_escape(&cfg.relay_description),
        bind = cfg.bind,
        nips = SUPPORTED_NIPS.iter().map(|n| n.to_string()).collect::<Vec<_>>().join(", "),
    )
}

fn html_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn info_document_shape() {
        let cfg = Config::default();
        let doc = relay_info(&cfg);
        assert_eq!(doc["name"], cfg.relay_name);
        assert!(doc["supported_nips"]
            .as_array()
            .unwrap()
            .contains(&json!(1)));
        assert_eq!(doc["limitation"]["max_limit"], cfg.max_limit);
        assert_eq!(doc["limitation"]["auth_required"], false);
    }

    #[test]
    fn health_shape() {
        let stats = StoreStats {
            events: 3,
            bytes_on_disk: 100,
            segments: 1,
            index_entries: 9,
        };
        let h = health(&stats, 2, 5);
        assert_eq!(h["status"], "ok");
        assert_eq!(h["events"], 3);
        assert_eq!(h["connections"], 2);
    }
}
