# airfry-bench

Load generator and benchmark harness for [Airfry](https://github.com/barrydeen/Airfry). It
drives the relay over WebSocket and the storage engine directly, reporting throughput and
latency percentiles. It is used to produce the published benchmarks.

## Subcommands

The `airfry-bench` binary provides these subcommands:

- `smoke` — publish a single event and verify the relay accepts it (sanity check).
- `load` — publish a large pre-signed corpus over concurrent WebSocket connections with
  pipelining; reports ingest events/s.
- `req` — issue REQ queries against a filter and measure EOSE latency percentiles.
- `mixed` — run a combined publish/query workload for a fixed duration.
- `store` — drive `airfry-store` directly (insert + query) to measure raw engine throughput.
- `dump` — generate a deterministic corpus to a file for reproducible runs.

Common flags include `--url`, `--json`, `--events`, `--conns`, `--authors`, `--kinds`,
`--seed`, `--pipeline`, `--filter`, `--duration`, `--queries`.

## Usage

```sh
# Ingest throughput against a running relay
cargo run --release --bin airfry-bench -- load \
    --url ws://127.0.0.1:8080 --events 100000 --conns 16 --pipeline 64 --json

# Query latency
cargo run --release --bin airfry-bench -- req \
    --url ws://127.0.0.1:8080 --queries 1000 --filter '{"kinds":[1],"limit":100}'

# Raw storage engine benchmark (no relay)
cargo run --release --bin airfry-bench -- store --events 200000 --json
```

## See also

- Repository and project overview: <https://github.com/barrydeen/Airfry>
- Benchmarks: [BENCHMARKS.md](https://github.com/barrydeen/Airfry/blob/main/BENCHMARKS.md)

## License

MIT.
