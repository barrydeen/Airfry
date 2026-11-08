# Changelog

All notable changes to Airfry are documented here. The project is pre-1.0; the storage format and
CLI may change between minor versions.

## [0.1.0] — 2026-11-07

Initial public release.

### Relay (`crates/airfry-relay`, binary `airfry-relay`)

- NIP-01 over WebSocket: `EVENT`, `REQ` (multiple filters), `CLOSE`, `EOSE`, `OK`, `NOTICE`,
  `CLOSED`, plus live subscription fanout.
- NIP-09 deletion requests (author-only), NIP-11 relay information document served with
  `Content-Type: application/nostr+json`, NIP-40 expiration handling, NIP-42 `AUTH` with optional
  enforcement (`--require-auth`), and NIP-45 `COUNT`.
- Parallel BIP-340 signature verification across cores before events reach the store.
- Zero-copy fanout of pre-encoded frames to matching subscriptions.
- `GET /health` JSON statistics endpoint.
- Graceful shutdown on `SIGINT`/`SIGTERM` with store flush.
- 16 CLI flags (each with an environment-variable equivalent): `--bind`, `--data-dir`,
  `--relay-name`, `--relay-pubkey`, `--relay-description`, `--relay-contact`, `--relay-url`,
  `--max-event-bytes`, `--max-subs-per-conn`, `--max-filters`, `--max-limit`, `--max-conns`,
  `--verify-events`, `--fsync-policy`, `--require-auth`, `--segment-bytes`.

### Negentropy / NIP-77 (`crates/airfry-negentropy`, relay `NEG-*` handlers)

- NIP-77 negentropy set reconciliation: `NEG-OPEN`, `NEG-MSG`, `NEG-CLOSE` over the relay WebSocket,
  with the relay sending missing events as `EVENT` frames followed by `EOSE` on completion (matching
  strfry).
- `airfry-negentropy` crate: Negentropy Protocol V1 wire format, byte-for-byte compatible with the
  reference implementation, with O(1) range fingerprints via a precomputed prefix-accumulator table.
- Relay limits: at most 8 concurrent negentropy subscriptions per connection, 1 MiB maximum hex
  payload, protocol versions `0x60..=0x6f` accepted; filter `limit` ignored for the reconciled set.
- Interop tests against the vendored strfry/negentropy C++ reference harness, plus a
  `negentropy-bench` reconciliation benchmark.

### Storage engine (`crates/airfry-store`)

- Segmented append-only log with checksummed records and a compact binary event encoding.
- Crash recovery: validated replay with torn-tail truncation; the store never refuses to start
  because of a partial final record.
- In-memory indexes for id, author, kind, single-letter tag values and `created_at`; queries resolve
  through the indexes with no disk scan.
- Deletion tombstones that survive restart, expiration filtering at query time, and `count()` that
  ignores `limit`.
- Configurable durability (`--fsync-policy never | interval:<seconds> | always`) and segment size
  (`--segment-bytes`).

### Benchmark tooling (`crates/airfry-bench`, `scripts/`)

- `airfry-bench` load generator with `smoke`, `load`, `req`, `mixed` and `store` modes, JSON output
  and latency percentiles.
- `scripts/verify.sh` acceptance gate: release build, workspace tests, relay boot, smoke/load/req/
  store benchmarks, NIP-11 and restart-persistence checks, and the head-to-head comparison.
- `scripts/compare_strfry.sh` head-to-head benchmark against strfry v1-4cd3cf6 with an identical
  pre-signed corpus, identical concurrency and identical filters.
- `scripts/fetch_strfry.sh` fetches and builds strfry at the pinned commit for reproduction.

### Measured performance

Head-to-head against strfry v1-4cd3cf6 on a 32-core Linux host, 200,000-event corpus, 16 publisher
connections, 2,000 REQ queries over 8 connections, filter `{"kinds":[1],"limit":100}`:

| metric | Airfry | strfry v1-4cd3cf6 | speedup |
| --- | --- | --- | --- |
| publish events/s | 162,810 | 33,966 | 4.79x |
| REQ time-to-EOSE p99 ms | 0.126 | 11.305 | 89.7x |

Direct store ingest measured at ~1.3M events/s with sub-millisecond query p99. Full numbers, the
machine specification and the exact reproduction commands are in [BENCHMARKS.md](BENCHMARKS.md).

### Tests

- `crates/airfry-e2e` cross-crate conformance suite (`store_conformance.rs`, `relay_conformance.rs`)
  and adversarial suites (`adversarial_store.rs`, `adversarial_relay.rs`) covering malformed frames,
  oversized events, duplicate ids, `limit: 0`, `since > until`, unknown kinds, multi-value tag
  filters, CLOSE mid-stream, concurrent REQ during heavy ingest, restart durability, NIP-09 of a
  nonexistent id, NIP-40 expired events, NIP-11 content type, `/health`, and `SIGTERM` shutdown.

### Repository

- MIT license.
- CI workflow (`.github/workflows/ci.yml`): fmt check, clippy, release build, workspace tests.
- README, CONTRIBUTING, SECURITY, CODE_OF_CONDUCT, docs/DESIGN.md.
