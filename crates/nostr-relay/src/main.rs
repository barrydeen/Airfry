//! `nostr-relay` — a fast Nostr relay backed by a custom storage engine.
//!
//! Speaks NIP-01 over WebSocket, serves the NIP-11 relay information document and a `/health`
//! endpoint over plain HTTP on the same port, and implements NIP-09 deletion, NIP-40
//! expiration, NIP-42 AUTH and NIP-45 COUNT.

use clap::Parser;
use nostr_relay::config::Config;
use nostr_relay::server::Relay;
use nostr_store::FsyncPolicy;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;
use tracing_subscriber::EnvFilter;

/// A fast Nostr relay backed by a custom Nostr-optimized storage engine.
#[derive(Parser, Debug)]
#[command(name = "nostr-relay", version, about, long_about = None)]
struct Cli {
    /// Address to bind the HTTP/WebSocket server to (e.g. 127.0.0.1:8080).
    #[arg(long, env = "NOSTR_RELAY_BIND", default_value = "127.0.0.1:8080")]
    bind: String,

    /// Directory backing the event store.
    #[arg(long, env = "NOSTR_RELAY_DATA_DIR", default_value = "./nostr-data")]
    data_dir: PathBuf,

    /// Relay name advertised in the NIP-11 document.
    #[arg(long, env = "NOSTR_RELAY_NAME", default_value = "nostr-relay")]
    relay_name: String,

    /// Relay operator pubkey (hex) advertised in the NIP-11 document.
    #[arg(long, env = "NOSTR_RELAY_PUBKEY")]
    relay_pubkey: Option<String>,

    /// Relay description advertised in the NIP-11 document.
    #[arg(
        long,
        env = "NOSTR_RELAY_DESCRIPTION",
        default_value = "A fast Nostr relay backed by a custom storage engine"
    )]
    relay_description: String,

    /// Relay contact advertised in the NIP-11 document.
    #[arg(long, env = "NOSTR_RELAY_CONTACT", default_value = "")]
    relay_contact: String,

    /// Canonical relay URL (e.g. wss://relay.example.com) used to validate NIP-42 `relay` tags.
    #[arg(long, env = "NOSTR_RELAY_URL")]
    relay_url: Option<String>,

    /// Maximum accepted EVENT frame size in bytes.
    #[arg(long, env = "NOSTR_RELAY_MAX_EVENT_BYTES", default_value_t = 65_536)]
    max_event_bytes: usize,

    /// Maximum concurrent subscriptions per connection.
    #[arg(long, env = "NOSTR_RELAY_MAX_SUBS_PER_CONN", default_value_t = 64)]
    max_subs_per_conn: usize,

    /// Maximum number of filters in a single REQ or COUNT.
    #[arg(long, env = "NOSTR_RELAY_MAX_FILTERS", default_value_t = 10)]
    max_filters: usize,

    /// Maximum `limit` honoured per filter.
    #[arg(long, env = "NOSTR_RELAY_MAX_LIMIT", default_value_t = 500)]
    max_limit: usize,

    /// Maximum concurrent connections (0 = unlimited).
    #[arg(long, env = "NOSTR_RELAY_MAX_CONNS", default_value_t = 0)]
    max_conns: usize,

    /// Verify event ids and schnorr signatures on ingest.
    #[arg(long, env = "NOSTR_RELAY_VERIFY_EVENTS", default_value_t = true, action = clap::ArgAction::Set)]
    verify_events: bool,

    /// Durability policy for the store: never | interval:<seconds> | always.
    #[arg(long, env = "NOSTR_RELAY_FSYNC_POLICY", default_value = "never")]
    fsync_policy: String,

    /// Require NIP-42 authentication before accepting EVENTs.
    #[arg(long, env = "NOSTR_RELAY_REQUIRE_AUTH", default_value_t = false, action = clap::ArgAction::Set)]
    require_auth: bool,

    /// Store segment size in bytes.
    #[arg(long, env = "NOSTR_RELAY_SEGMENT_BYTES", default_value_t = 268_435_456)]
    segment_bytes: u64,
}

fn parse_fsync_policy(s: &str) -> Result<FsyncPolicy, String> {
    let s = s.trim();
    if s.eq_ignore_ascii_case("never") {
        return Ok(FsyncPolicy::Never);
    }
    if s.eq_ignore_ascii_case("always") {
        return Ok(FsyncPolicy::Always);
    }
    if let Some(rest) = s.strip_prefix("interval:") {
        let secs: f64 = rest
            .parse()
            .map_err(|_| format!("invalid interval seconds: {rest}"))?;
        if secs < 0.0 {
            return Err("interval must be non-negative".to_string());
        }
        return Ok(FsyncPolicy::Interval(Duration::from_secs_f64(secs)));
    }
    Err(format!(
        "invalid fsync policy `{s}` (expected never, always or interval:<seconds>)"
    ))
}

fn init_tracing() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let _ = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .try_init();
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_tracing();
    let cli = Cli::parse();

    let bind: SocketAddr = cli
        .bind
        .parse()
        .map_err(|e| format!("invalid --bind `{}`: {e}", cli.bind))?;
    let fsync_policy = parse_fsync_policy(&cli.fsync_policy)?;

    let cfg = Config {
        bind,
        data_dir: cli.data_dir,
        relay_name: cli.relay_name,
        relay_pubkey: cli.relay_pubkey,
        relay_description: cli.relay_description,
        relay_contact: cli.relay_contact,
        relay_url: cli.relay_url,
        max_event_bytes: cli.max_event_bytes,
        max_subs_per_conn: cli.max_subs_per_conn,
        max_filters: cli.max_filters,
        max_limit: cli.max_limit,
        max_conns: cli.max_conns,
        verify_events: cli.verify_events,
        fsync_policy,
        require_auth: cli.require_auth,
        segment_bytes: cli.segment_bytes,
    };

    std::fs::create_dir_all(&cfg.data_dir)?;

    let relay = Relay::new(cfg)?;
    let listener = tokio::net::TcpListener::bind(relay.cfg.bind).await?;
    let local = listener.local_addr()?;
    tracing::info!(
        addr = %local,
        data_dir = %relay.cfg.data_dir.display(),
        verify_events = relay.cfg.verify_events,
        require_auth = relay.cfg.require_auth,
        "nostr-relay listening"
    );

    let relay_for_shutdown = relay.clone();
    let mut sigterm = signal_stream()?;

    let serve = {
        let relay = relay.clone();
        tokio::spawn(async move { relay.serve(listener).await })
    };

    tokio::select! {
        res = serve => {
            if let Err(e) = res? {
                tracing::error!(error = %e, "server error");
            }
        }
        _ = sigterm.recv() => {
            tracing::info!("received shutdown signal");
            relay_for_shutdown.shutdown();
        }
    }

    // Flush the store before exiting.
    if let Err(e) = relay_for_shutdown.store.flush() {
        tracing::error!(error = %e, "store flush failed");
    }
    tracing::info!("nostr-relay stopped");
    Ok(())
}

/// A stream of SIGINT/SIGTERM notifications.
fn signal_stream(
) -> Result<tokio::sync::mpsc::Receiver<()>, Box<dyn std::error::Error + Send + Sync>> {
    use tokio::signal::unix::{signal, SignalKind};
    let (tx, rx) = tokio::sync::mpsc::channel(4);
    let mut sigint = signal(SignalKind::interrupt())?;
    let mut sigterm = signal(SignalKind::terminate())?;
    tokio::spawn(async move {
        tokio::select! {
            _ = sigint.recv() => { let _ = tx.send(()).await; }
            _ = sigterm.recv() => { let _ = tx.send(()).await; }
        }
    });
    Ok(rx)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fsync_policy_parsing() {
        assert_eq!(parse_fsync_policy("never").unwrap(), FsyncPolicy::Never);
        assert_eq!(parse_fsync_policy("always").unwrap(), FsyncPolicy::Always);
        match parse_fsync_policy("interval:2").unwrap() {
            FsyncPolicy::Interval(d) => assert_eq!(d, Duration::from_secs(2)),
            _ => panic!("wrong variant"),
        }
        assert!(parse_fsync_policy("bogus").is_err());
    }

    #[test]
    fn cli_parses_defaults() {
        let cli = Cli::parse_from(["nostr-relay"]);
        assert_eq!(cli.bind, "127.0.0.1:8080");
        assert_eq!(cli.max_event_bytes, 65_536);
        assert_eq!(cli.max_subs_per_conn, 64);
        assert_eq!(cli.max_filters, 10);
        assert_eq!(cli.max_limit, 500);
        assert!(cli.verify_events);
        assert!(!cli.require_auth);
    }
}
