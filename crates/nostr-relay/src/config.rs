//! Relay configuration. Built from CLI flags / environment variables in `main.rs`, and
//! constructed directly by integration tests.

use nostr_store::FsyncPolicy;
use std::net::SocketAddr;
use std::path::PathBuf;

#[derive(Clone, Debug)]
pub struct Config {
    /// Address the HTTP/WebSocket server binds to.
    pub bind: SocketAddr,
    /// Directory backing the event store.
    pub data_dir: PathBuf,
    /// Human readable relay name (NIP-11).
    pub relay_name: String,
    /// Relay operator pubkey, hex (NIP-11).
    pub relay_pubkey: Option<String>,
    /// Relay description (NIP-11).
    pub relay_description: String,
    /// Relay contact (NIP-11).
    pub relay_contact: String,
    /// Canonical relay URL, used for NIP-42 `relay` tag validation.
    pub relay_url: Option<String>,
    /// Maximum accepted EVENT frame size in bytes.
    pub max_event_bytes: usize,
    /// Maximum concurrent subscriptions per connection.
    pub max_subs_per_conn: usize,
    /// Maximum number of filters in a single REQ/COUNT.
    pub max_filters: usize,
    /// Maximum `limit` honoured per filter.
    pub max_limit: usize,
    /// Maximum concurrent connections (0 = unlimited).
    pub max_conns: usize,
    /// Whether to verify event ids and schnorr signatures on ingest.
    pub verify_events: bool,
    /// Durability policy handed to the store.
    pub fsync_policy: FsyncPolicy,
    /// When true, EVENT requires a NIP-42 authenticated session.
    pub require_auth: bool,
    /// Segment size handed to the store.
    pub segment_bytes: u64,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            bind: "127.0.0.1:0".parse().expect("valid default bind address"),
            data_dir: PathBuf::from("./nostr-data"),
            relay_name: "nostr-relay".to_string(),
            relay_pubkey: None,
            relay_description: "A fast Nostr relay backed by a custom storage engine".to_string(),
            relay_contact: String::new(),
            relay_url: None,
            max_event_bytes: 65_536,
            max_subs_per_conn: 64,
            max_filters: 10,
            max_limit: 500,
            max_conns: 0,
            verify_events: true,
            fsync_policy: FsyncPolicy::Never,
            require_auth: false,
            segment_bytes: 256 << 20,
        }
    }
}
