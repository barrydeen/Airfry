# nostr-relay

The high-throughput Nostr relay server for [Airfry](https://github.com/barrydeen/Airfry). It
serves NIP-01 WebSocket clients over a custom storage engine (`nostr-store`) and implements
NIP-01, NIP-09, NIP-11, NIP-40, NIP-42 and NIP-45.

## Public API

- `nostr_relay::Config` — relay configuration (bind address, data dir, NIP-11 metadata, limits,
  `fsync_policy`, `segment_bytes`, `require_auth`, `verify_events`, etc.).
- `nostr_relay::Relay` — `Relay::new(cfg)` builds the server; `Relay::serve(listener)` runs the
  accept loop; `Relay::shutdown()` triggers graceful shutdown; `shutdown_receiver()` exposes the
  broadcast signal.
- `nostr_relay::nip11` — `relay_info`, `health`, `landing_page`, `SUPPORTED_NIPS`.
- `nostr_relay::hub` — connection/subscription registry and live fanout.

The `nostr-relay` binary exposes all runtime flags via CLI/env (e.g. `--bind`, `--data-dir`,
`--relay-name`, `--max-event-bytes`, `--max-subs-per-conn`, `--max-filters`, `--max-limit`,
`--max-conns`, `--verify-events`, `--fsync-policy`, `--require-auth`, `--segment-bytes`).

## Usage

```rust
use nostr_relay::{Config, Relay};
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut cfg = Config::default();
    cfg.bind = "127.0.0.1:8080".parse().unwrap();
    cfg.data_dir = "./nostr-data".into();

    let relay = Relay::new(cfg)?;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:8080").await?;
    relay.clone().serve(listener).await?;
    Ok(())
}
```

Run the binary directly:

```sh
cargo run --release --bin nostr-relay -- --bind 127.0.0.1:8080 --data-dir ./nostr-data
```

## See also

- Repository and project overview: <https://github.com/barrydeen/Airfry>
- Benchmarks: [BENCHMARKS.md](https://github.com/barrydeen/Airfry/blob/main/BENCHMARKS.md)
- `nostr-bench` drives this relay with `smoke`, `load`, `req` and `mixed` workloads.

## License

MIT.
