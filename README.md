# Airfry — the fastest Nostr relay

**Airfry** is a [Nostr](https://github.com/nostr-protocol/nips) relay written in Rust, backed by a
purpose-built storage engine instead of a generic database. The design goal is simple and
measurable: **beat strfry** — the current fastest relay — on both ingest throughput (published
events/second) and REQ latency (p50/p99), on identical hardware, with an identical pre-signed event
corpus and identical client concurrency.

Measured head-to-head numbers, the exact commands used to produce them, and the machine
specification live in **[BENCHMARKS.md](BENCHMARKS.md)**.

> **Naming note.** The project is called **Airfry**. The Rust crates and the release binary keep
> their original names — `nostr-core`, `nostr-store`, `nostr-relay`, `nostr-bench`, `nostr-e2e` —
> and the server binary is `nostr-relay`. When you see `nostr-relay` below, that is Airfry's relay.

---

## Contents

- [Status / disclaimer](#status--disclaimer)
- [What it is](#what-it-is)
- [Architecture](#architecture)
- [Build](#build)
- [Run](#run)
- [CLI flags and environment variables](#cli-flags-and-environment-variables)
- [Supported NIPs](#supported-nips)
- [HTTP endpoints](#http-endpoints)
- [Storage format](#storage-format)
- [Tests](#tests)
- [Benchmarks](#benchmarks)
- [Performance vs strfry](#performance-vs-strfry)
- [Repository layout](#repository-layout)
- [License](#license)
- [Contributing](#contributing)
- [Security](#security)

---

## Status / disclaimer

Airfry is a **young project**. It is feature-complete for the NIPs listed below and passes its own
conformance and adversarial test suites plus a head-to-head benchmark against strfry, but it has not
yet seen years of adversarial production traffic. Before running it in front of real users:

- run the test suite and `scripts/verify.sh` yourself on your hardware,
- pick a durability policy that matches your risk tolerance (`--fsync-policy`),
- keep backups of the data directory,
- expect the storage format and CLI to evolve between minor versions until 1.0.

Bug reports and benchmark reproductions are very welcome — see [CONTRIBUTING.md](CONTRIBUTING.md).

## What it is

A single-binary relay that speaks NIP-01 over WebSocket, serves a NIP-11 relay information document
over HTTP on the same port, and persists events in an append-only segmented log with in-memory
indexes. There is no LMDB, no Postgres, no RocksDB: the storage engine is written for this workload
specifically, so the hot paths (event ingest, filter matching, subscription fanout) are ours to
optimise.

Key properties:

- **Parallel signature verification.** Incoming events are verified across all available cores
  before they touch the store. Verification is the dominant CPU cost of ingest; strfry serialises
  much of this work.
- **In-memory index queries.** REQ filters (`ids`, `authors`, `kinds`, `#tag`, time ranges, `limit`)
  are answered from in-memory indexes with no disk scan and no full-log scan.
- **Zero-copy fanout.** Live events are broadcast to matching subscriptions by sharing
  `Arc<Event>` handles and pre-encoded frames, not by re-serialising per subscriber.
- **Crash-safe by construction.** Every log record carries a checksum; recovery discards a torn tail
  instead of refusing to start.

## Architecture

```
                 ┌──────────────────────────────────────────────────────────────┐
   WebSocket     │  nostr-relay (Airfry)                                        │
   clients ─────►│  ├── HTTP/WS front end (tokio, per-connection tasks)          │
                 │  ├── frame parser (nostr-core::parse_client_message)         │
                 │  ├── verification pool (parallel BIP-340 schnorr checks)     │
                 │  ├── subscription hub (filter registry + live fanout)        │
                 │  └── NIP-11 / NIP-45 / NIP-09 / NIP-40 / NIP-42 handlers     │
                 └───────────────┬──────────────────────────────────────────────┘
                                 │ insert / query / delete / count
                 ┌───────────────▼──────────────────────────────────────────────┐
                 │  nostr-store                                                 │
                 │  ├── segmented append-only log (checksummed records)         │
                 │  ├── crash recovery (validated replay, torn-tail truncation) │
                 │  └── in-memory indexes: id, author, kind, tag, created_at    │
                 └──────────────────────────────────────────────────────────────┘
```

The workspace is split into five crates plus this documentation:

| Crate | Responsibility |
| --- | --- |
| `crates/nostr-core` | Event model, id computation, BIP-340 verification, filter matching, NIP-01 wire messages, compact binary event codec. |
| `crates/nostr-store` | The custom storage engine: segmented append-only log, checksums, recovery, in-memory indexes, query/count/delete. |
| `crates/nostr-relay` | The relay binary: WebSocket server, HTTP endpoints, subscription hub, NIP handlers, graceful shutdown. |
| `crates/nostr-bench` | Load generator (`smoke`, `load`, `req`, `mixed`, `store`) used for the strfry comparison. |
| `crates/nostr-e2e` | Cross-crate conformance and adversarial test suite (the independent check that the relay really speaks NIP-01 on the wire). |

For a deep dive into the storage engine, the relay concurrency model, and the concrete reasons this
design outperforms strfry, see **[docs/DESIGN.md](docs/DESIGN.md)**. The authoritative crate APIs and
ownership rules are in **[ARCHITECTURE.md](ARCHITECTURE.md)**.

## Build

Requires a Rust toolchain (1.80+). The workspace pins `lto = "fat"`, `codegen-units = 1` and
`panic = "abort"` for the release profile.

```sh
export PATH=$HOME/.cargo/bin:$PATH
cargo build --release --workspace
```

The relay binary is produced at `target/release/nostr-relay` and the load generator at
`target/release/nostr-bench`.

## Run

```sh
# minimal: listen on localhost, persist to ./nostr-data
target/release/nostr-relay --bind 127.0.0.1:7777 --data-dir ./nostr-data

# public-facing example with NIP-11 metadata and a write limit
target/release/nostr-relay \
  --bind 0.0.0.0:7777 \
  --data-dir /var/lib/nostr-relay \
  --relay-name "example relay" \
  --relay-pubkey <64-hex-operator-pubkey> \
  --relay-description "fast relay" \
  --relay-contact "mailto:ops@example.com" \
  --max-event-bytes 131072 \
  --max-limit 1000 \
  --fsync-policy interval:5
```

The relay logs structured events to stderr (`RUST_LOG=info` by default; set `RUST_LOG=debug` for
per-frame tracing). It shuts down gracefully on `SIGINT`/`SIGTERM`: listeners stop accepting,
in-flight frames are drained, and the store is flushed before exit.

## CLI flags and environment variables

Every flag has an equivalent environment variable so the relay can be configured in containers.
Boolean flags accept `true`/`false`. The table below mirrors `crates/nostr-relay/src/config.rs` and
`target/release/nostr-relay --help`; `--help` is authoritative for the binary you built.

| Flag | Env var | Default | Meaning |
| --- | --- | --- | --- |
| `--bind <ADDR>` | `NOSTR_RELAY_BIND` | `127.0.0.1:8080` | Address the HTTP/WebSocket server binds to. |
| `--data-dir <PATH>` | `NOSTR_RELAY_DATA_DIR` | `./nostr-data` | Directory backing the event store (segments + index metadata). |
| `--relay-name <NAME>` | `NOSTR_RELAY_NAME` | `nostr-relay` | Relay name published in the NIP-11 document. |
| `--relay-pubkey <HEX>` | `NOSTR_RELAY_PUBKEY` | unset | Operator pubkey (64 hex chars) published in NIP-11. |
| `--relay-description <TEXT>` | `NOSTR_RELAY_DESCRIPTION` | built-in description | NIP-11 description. |
| `--relay-contact <TEXT>` | `NOSTR_RELAY_CONTACT` | empty | NIP-11 contact (e.g. `mailto:` URI). |
| `--relay-url <URL>` | `NOSTR_RELAY_URL` | unset | Canonical relay URL, used for NIP-42 `relay` tag validation. |
| `--max-event-bytes <N>` | `NOSTR_RELAY_MAX_EVENT_BYTES` | `65536` | Maximum accepted EVENT frame size in bytes; larger frames get `OK false`. |
| `--max-subs-per-conn <N>` | `NOSTR_RELAY_MAX_SUBS_PER_CONN` | `64` | Maximum concurrent subscriptions per connection. |
| `--max-filters <N>` | `NOSTR_RELAY_MAX_FILTERS` | `10` | Maximum number of filters in a single REQ/COUNT. |
| `--max-limit <N>` | `NOSTR_RELAY_MAX_LIMIT` | `500` | Maximum `limit` honoured per filter (larger values are clamped). |
| `--max-conns <N>` | `NOSTR_RELAY_MAX_CONNS` | `0` (unlimited) | Maximum concurrent connections. |
| `--verify-events <BOOL>` | `NOSTR_RELAY_VERIFY_EVENTS` | `true` | Verify event ids and schnorr signatures on ingest. Disable only for trusted bulk import. |
| `--fsync-policy <POLICY>` | `NOSTR_RELAY_FSYNC_POLICY` | `never` | Durability policy for the store: `never`, `interval:<seconds>`, or `always`. |
| `--require-auth <BOOL>` | `NOSTR_RELAY_REQUIRE_AUTH` | `false` | When true, EVENT requires a NIP-42 authenticated session. |
| `--segment-bytes <N>` | `NOSTR_RELAY_SEGMENT_BYTES` | `268435456` (256 MiB) | Target size of a log segment before rotation. |

## Supported NIPs

| NIP | Status | Notes |
| --- | --- | --- |
| NIP-01 | supported | EVENT / REQ / CLOSE / EOSE / OK / NOTICE / CLOSED, multiple filters per REQ, live fanout. |
| NIP-09 | supported | Kind-5 deletion requests; only the event author may delete their own event. |
| NIP-11 | supported | Relay information document served on `GET /` with `Accept: application/nostr+json`. |
| NIP-40 | supported | Events with an `expiration` tag in the past are not returned and are not counted. |
| NIP-42 | supported | `AUTH` message and `["AUTH", <challenge>]`; optional enforcement via `--require-auth`. |
| NIP-45 | supported | `COUNT` requests return `["COUNT", <sub_id>, {"count": N}]`. |

The NIP-11 document advertises exactly this set (`supported_nips: [1, 9, 11, 40, 42, 45]`).

## HTTP endpoints

| Endpoint | Behaviour |
| --- | --- |
| `GET /` with `Accept: application/nostr+json` | NIP-11 relay information document (`Content-Type: application/nostr+json`). |
| `GET /` (other Accept) | Small HTML landing page. |
| `GET /health` | JSON health/statistics payload: `status`, `events`, `bytes_on_disk`, `segments`, `index_entries`, `connections`, `subscriptions`, `version`. |
| `GET /` with `Upgrade: websocket` | WebSocket upgrade; the Nostr protocol endpoint. |

## Storage format

The store is a segmented append-only log. Each segment is a file in the data directory; each record
is a length-prefixed, checksummed frame containing the event in the compact binary encoding from
`nostr_core::codec` (version byte, id, pubkey, `created_at`, kind, tags, content, signature). The
in-memory index maps ids, authors, kinds, single-letter tag values and `created_at` to record
locations, and is rebuilt from the log on startup.

- **Checksums** cover every record; a record that fails validation stops replay at that point.
- **Torn tails** (a partially written final record, e.g. after a power loss) are truncated and the
  store continues from the last valid record. It never refuses to start because of a torn write.
- **Deletions** are recorded as tombstones in the log and applied to the indexes, so they survive a
  restart.
- **No full-file scans on query.** Queries resolve through the in-memory indexes; the log is only
  read during recovery.

See [docs/DESIGN.md](docs/DESIGN.md) for the exact record layout, recovery algorithm and lock
strategy.

## Tests

```sh
export PATH=$HOME/.cargo/bin:$PATH

# whole workspace
cargo test --workspace

# the independent cross-crate conformance + adversarial suite
cargo test -p nostr-e2e
```

`crates/nostr-e2e` contains:

- `store_conformance.rs` — storage engine conformance against the documented API (filters, dedup,
  batch insert, reopen recovery, torn-tail recovery, deletion, expiry, concurrency).
- `relay_conformance.rs` — black-box protocol conformance: spawns the real binary and drives it over
  WebSockets (NIP-01 publish/REQ/EOSE/COUNT, live fanout, CLOSE, NIP-11, `/health`, NIP-09,
  restart durability, oversized events, garbage frames, concurrent clients).
- `adversarial_store.rs` — hostile inputs against the store: duplicate ids inside a batch, `limit: 0`,
  `since > until`, unknown kinds, multi-value tag filters, deleting nonexistent ids, expired events,
  concurrent queries during heavy ingest, reopen durability.
- `adversarial_relay.rs` — hostile inputs against the relay: malformed frames, oversized events,
  duplicate ids, `limit: 0`, `since > until`, unknown kinds, multi-value tag filters, CLOSE
  mid-stream, concurrent REQ during heavy ingest, restart durability, NIP-09 of a nonexistent id,
  NIP-40 expired events, NIP-11 content type, `/health`, and `SIGTERM` shutdown.

## Benchmarks

```sh
export PATH=$HOME/.cargo/bin:$PATH
cargo build --release --workspace

# end-to-end acceptance gate (builds, tests, boots the relay, runs smoke + load + req + store,
# and runs the head-to-head comparison against strfry)
scripts/verify.sh

# head-to-head only: our relay vs strfry, identical corpus and concurrency
scripts/fetch_strfry.sh          # clones + builds strfry v1-4cd3cf6 into third_party/strfry
scripts/compare_strfry.sh

# store-engine benchmark (1M-event corpus, no network)
cargo run --release -p nostr-store --bin store_bench 1000000

# individual load generator modes
target/release/nostr-bench smoke --url ws://127.0.0.1:7777
target/release/nostr-bench load  --url ws://127.0.0.1:7777 --events 200000 --conns 32
target/release/nostr-bench req   --url ws://127.0.0.1:7777 --queries 5000 --conns 16
target/release/nostr-bench mixed --url ws://127.0.0.1:7777 --duration 30
```

`nostr-bench` prints a JSON summary and a markdown table suitable for pasting into
`BENCHMARKS.md`.

**strfry is not vendored** (`third_party/strfry` is gitignored). `scripts/verify.sh` auto-runs
`scripts/fetch_strfry.sh` once if the strfry binary is missing; if the fetch/build fails (no network
or missing build deps) it prints a clear **SKIP** warning for the head-to-head check and continues.
When strfry is available the assertion is enforced: the gate exits non-zero if Airfry does not beat
strfry on publish events/s **and** REQ p99. See [CONTRIBUTING.md](CONTRIBUTING.md) for the strfry
build dependencies.

## Performance vs strfry

strfry is the reference point: it is the fastest relay in production use today, and the goal of this
project is to be measurably faster than it. The comparison is run head-to-head on the same machine
with:

- the same pre-signed event corpus (identical events, identical ids/signatures),
- the same client concurrency and the same filters,
- a tuned strfry configuration (`third_party/strfry/strfry-bench.conf`: auth disabled, no write
  policy, 10 TB LMDB mapsize, compression disabled, ingester=8 reqWorker=8),
- the same measurement tooling for both relays.

**Latest measured head-to-head (200,000-event corpus, 16 publisher connections, 2,000 REQ queries
over 8 connections, filter `{"kinds":[1],"limit":100}`, 32-core Linux host):**

| metric | Airfry (`nostr-relay`) | strfry v1-4cd3cf6 | speedup |
| --- | --- | --- | --- |
| **publish events/s** | **162,810** | **33,966** | **4.79x** |
| publish ack p99 ms | 6.21 | 39.36 | 6.3x lower |
| **REQ time-to-EOSE p99 ms** | **0.126** | **11.305** | **89.7x** |
| REQ events returned | 200,000 | 200,000 | identical |

Independent repeat runs measured 4.70x and 4.54x on publish throughput and 86.7x and 36.2x on REQ
p99; run-to-run variance on publish throughput is roughly ±10%, while the REQ p99 gap is one to two
orders of magnitude. The relay also passes its own acceptance gate at ~200k events/s publish
throughput with sub-millisecond REQ p99.

**Verdict: Airfry is measurably faster than strfry on both required metrics** — publish throughput
and REQ p99 latency — with identical results returned. The full run log, every historical run, the
exact commands, and the machine specification are in **[BENCHMARKS.md](BENCHMARKS.md)**.

At a design level the advantages come from three things strfry cannot do with LMDB:

1. **Parallel schnorr verification across cores** instead of serialised verification.
2. **In-memory index queries** instead of LMDB cursor scans over a B-tree.
3. **Zero-copy fanout** of pre-encoded frames instead of per-subscriber serialisation.

These are explained in detail in [docs/DESIGN.md](docs/DESIGN.md#why-this-beats-strfry).

## Repository layout

```
Cargo.toml                # workspace manifest
ARCHITECTURE.md           # authoritative crate APIs and ownership rules
BENCHMARKS.md             # measured head-to-head numbers vs strfry
README.md                 # this file
LICENSE                   # MIT
CONTRIBUTING.md           # build/test/bench instructions and PR expectations
SECURITY.md               # vulnerability reporting
CODE_OF_CONDUCT.md        # Contributor Covenant v2.1
CHANGELOG.md              # release notes
docs/DESIGN.md            # storage engine + relay design, and why it beats strfry
crates/nostr-core/        # events, schnorr verification, filters, wire codec
crates/nostr-store/       # custom storage engine
crates/nostr-relay/       # relay binary
crates/nostr-bench/       # load generator
crates/nostr-e2e/         # conformance + adversarial tests
scripts/verify.sh         # acceptance gate
scripts/compare_strfry.sh # head-to-head benchmark vs strfry
scripts/fetch_strfry.sh   # fetch + build strfry at the pinned commit
.github/workflows/ci.yml  # CI: fmt, clippy, build, test
third_party/strfry/       # strfry source + built binary (fetched, not committed)
```

## License

Airfry is released under the **MIT License**. See [LICENSE](LICENSE).
Copyright (c) 2026 Barry Deen.

## Contributing

Contributions are welcome. See [CONTRIBUTING.md](CONTRIBUTING.md) for build/test/benchmark
instructions, code style, and PR expectations. By participating you agree to the
[Code of Conduct](CODE_OF_CONDUCT.md).

## Security

Please report vulnerabilities privately — see [SECURITY.md](SECURITY.md).
