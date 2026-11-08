# Airfry — the fastest Nostr relay

**Airfry** is a [Nostr](https://github.com/nostr-protocol/nips) relay written in Rust, backed by a
purpose-built storage engine instead of a generic database. The design goal is simple and
measurable: **beat strfry** — the current fastest relay — on both ingest throughput (published
events/second) and REQ latency (p50/p99), on identical hardware, with an identical pre-signed event
corpus and identical client concurrency.

Measured head-to-head numbers, the exact commands used to produce them, and the machine
specification live in **[BENCHMARKS.md](BENCHMARKS.md)**.

> **Naming note.** The project is called **Airfry**, and every crate and binary in this repository
> is airfry-branded: `airfry-core`, `airfry-store`, `airfry-relay`, `airfry-bench`, `airfry-e2e`
> and `airfry-negentropy`. The server binary is `airfry-relay`; `airfry-relay` below is Airfry's relay.

---

## Contents

- [Status / disclaimer](#status--disclaimer)
- [What it is](#what-it-is)
- [Architecture](#architecture)
- [Build](#build)
- [Run](#run)
- [CLI flags and environment variables](#cli-flags-and-environment-variables)
- [Supported NIPs](#supported-nips)
- [Negentropy / NIP-77 set reconciliation](#negentropy--nip-77-set-reconciliation)
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
   WebSocket     │  airfry-relay (Airfry)                                        │
   clients ─────►│  ├── HTTP/WS front end (tokio, per-connection tasks)          │
                 │  ├── frame parser (airfry-core::parse_client_message)         │
                 │  ├── verification pool (parallel BIP-340 schnorr checks)     │
                 │  ├── subscription hub (filter registry + live fanout)        │
                 │  ├── NIP-11 / NIP-45 / NIP-09 / NIP-40 / NIP-42 handlers     │
                 │  └── NIP-77 negentropy handler (airfry-negentropy)           │
                 └───────────────┬──────────────────────────────────────────────┘
                                 │ insert / query / delete / count
                 ┌───────────────▼──────────────────────────────────────────────┐
                 │  airfry-store                                                 │
                 │  ├── segmented append-only log (checksummed records)         │
                 │  ├── crash recovery (validated replay, torn-tail truncation) │
                 │  └── in-memory indexes: id, author, kind, tag, created_at    │
                 └──────────────────────────────────────────────────────────────┘
```

The workspace is split into five crates plus this documentation:

| Crate | Responsibility |
| --- | --- |
| `crates/airfry-core` | Event model, id computation, BIP-340 verification, filter matching, NIP-01 wire messages, compact binary event codec. |
| `crates/airfry-store` | The custom storage engine: segmented append-only log, checksums, recovery, in-memory indexes, query/count/delete. |
| `crates/airfry-relay` | The relay binary: WebSocket server, HTTP endpoints, subscription hub, NIP handlers, graceful shutdown. |
| `crates/airfry-bench` | Load generator (`smoke`, `load`, `req`, `mixed`, `store`) used for the strfry comparison. |
| `crates/airfry-e2e` | Cross-crate conformance and adversarial test suite (the independent check that the relay really speaks NIP-01 on the wire). |
| `crates/airfry-negentropy` | NIP-77 negentropy set reconciliation: protocol-v1 wire format, O(1) range fingerprints, and a fast reconciler used by the relay's `NEG-*` handlers. |

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

The relay binary is produced at `target/release/airfry-relay` and the load generator at
`target/release/airfry-bench`.

## Run

```sh
# minimal: listen on localhost, persist to ./nostr-data
target/release/airfry-relay --bind 127.0.0.1:7777 --data-dir ./nostr-data

# public-facing example with NIP-11 metadata and a write limit
target/release/airfry-relay \
  --bind 0.0.0.0:7777 \
  --data-dir /var/lib/airfry-relay \
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
Boolean flags accept `true`/`false`. The table below mirrors `crates/airfry-relay/src/config.rs` and
`target/release/airfry-relay --help`; `--help` is authoritative for the binary you built.

| Flag | Env var | Default | Meaning |
| --- | --- | --- | --- |
| `--bind <ADDR>` | `NOSTR_RELAY_BIND` | `127.0.0.1:8080` | Address the HTTP/WebSocket server binds to. |
| `--data-dir <PATH>` | `NOSTR_RELAY_DATA_DIR` | `./nostr-data` | Directory backing the event store (segments + index metadata). |
| `--relay-name <NAME>` | `AIRFRY_RELAY_NAME` | `airfry-relay` | Relay name published in the NIP-11 document. |
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
| NIP-77 | supported | Negentropy set reconciliation: `NEG-OPEN`, `NEG-MSG`, `NEG-CLOSE`. Errors are reported via `CLOSED`/`NOTICE` (the relay does not emit `NEG-ERR`). See [Negentropy / NIP-77](#negentropy--nip-77-set-reconciliation). |

The NIP-11 document advertises exactly this set (`supported_nips: [1, 9, 11, 40, 42, 45, 77]`).

## Negentropy / NIP-77 set reconciliation

Airfry implements **[NIP-77](https://github.com/nostr-protocol/nips/blob/master/77.md)** (Negentropy
Syncing), a protocol extension for efficiently synchronising events between a client and a relay
(or between two relays). It is a Nostr wrapper around the [Negentropy](https://github.com/hoytech/negentropy)
protocol, which uses **Range-Based Set Reconciliation** ([RBSR](https://logperiodic.com/rbsr.html)).

### What it is and why it matters

Set reconciliation lets two parties that each hold a set of events (identified by a 64-bit
timestamp and a 256-bit event id) discover the differences between their sets **without
transferring the full event set or even the full list of ids**. When both sides share most of their
events — the common case for a client that has been syncing from a relay — the protocol exchanges
only a handful of small fingerprint messages and a short list of the ids that actually differ.

This matters because:

- **Bandwidth.** A naive sync uploads/downloads every id (32 bytes each) or every event. Negentropy
  narrows the search to the ranges that actually differ, so a client that is already up to date
  reconciles in a single round with a few hundred bytes on the wire.
- **Privacy and cost.** The client learns *which* ids it has and needs; it then fetches only the
  events it is missing with a normal `REQ`, and uploads only the events the relay is missing with
  `EVENT`. It never has to reveal its whole set.
- **Relay-to-relay mirroring.** The same protocol works between relays, which is how relays can
  cheaply keep each other in sync.

The reconciliation itself only produces id sets (`have` / `need`); event transfer still happens over
NIP-01 `EVENT`/`REQ` on the same WebSocket connection.

### Protocol messages

Airfry speaks the NIP-77 wire messages over the same WebSocket endpoint as NIP-01. Because the
Negentropy payload is a binary protocol, NIP-77 **hex-encodes** every payload. The messages are:

| Message | Direction | Purpose |
| --- | --- | --- |
| `NEG-OPEN` | client → relay | Start a reconciliation: subscription id, NIP-01 filter, and the hex-encoded initial negentropy message. |
| `NEG-MSG` | bidirectional | One reconciliation round: subscription id and a hex-encoded negentropy message. Client and relay alternate these. |
| `NEG-CLOSE` | client → relay | End the reconciliation and release the relay's state for that subscription id. |

The negentropy payloads are produced and consumed by the `airfry-negentropy` crate
(`Negentropy::initiate` / `Negentropy::reconcile`), which implements Negentropy Protocol V1
(protocol version byte `0x61`) byte-for-byte compatible with the reference implementation.

#### Example: `NEG-OPEN` (client → relay)

```json
["NEG-OPEN","neg-1",{"kinds":[1],"authors":["<64-hex-pubkey>"],"since":1700000000},"6100000000..."]
```

- `"neg-1"` is the subscription id (a separate namespace from `REQ` subscription ids).
- The filter is a normal NIP-01 filter; the relay selects the events it holds that match it. The
  filter's `limit` is **ignored** — negentropy reconciles the full matching *set*, not a paginated
  window (this matches strfry's negentropy responder).
- The last element is the hex-encoded initial negentropy message from `Negentropy::initiate()`.
  It always begins with the protocol version byte `61` (`0x61` = protocol v1).

The relay replies with a `NEG-MSG` carrying its first reconciliation message.

#### Example: `NEG-MSG` (relay → client, and client → relay)

```json
["NEG-MSG","neg-1","610000...<hex-encoded negentropy message>..."]
```

Both directions use the same format. The relay replies to `NEG-OPEN` with a `NEG-MSG`; the client
parses it to learn which ids it has that the relay needs and which ids it needs that the relay has,
then sends a further `NEG-MSG` if it wants to continue.

When the reconciliation completes (the responder's reply is the bare protocol-version byte, i.e. a
one-byte message), the relay sends the events the client is missing as `["EVENT", "neg-1", {...}]`
frames followed by `["EOSE", "neg-1"]`, exactly as strfry does. The client can then upload the ids
the relay is missing with `EVENT`.

#### Example: `NEG-CLOSE` (client → relay)

```json
["NEG-CLOSE","neg-1"]
```

`NEG-CLOSE` drops the relay's reconciliation state for that subscription id and produces no reply.

#### Errors

Airfry reports negentropy errors using the standard NIP-01 error frames rather than a separate
`NEG-ERR` message:

- `["CLOSED", "neg-1", "invalid: ..."]` — a bad hex payload, an unparseable filter, an unsupported
  protocol version (`unsupported: negentropy protocol version 0x..`), an unknown subscription id, or
  too many concurrent negentropy subscriptions.
- `["NOTICE", "invalid: ..."]` — a malformed frame (e.g. a missing subscription id).

### Using it from a client

The reconciliation is a stateful, multi-round exchange, so the easiest way to drive it is with a
library that implements NIP-77. With [`nak`](https://github.com/fiatjaf/nak) you can sync events
from a relay using negentropy:

```sh
# download events you are missing from a relay using negentropy sync
nak req -k 1 --author <64-hex-pubkey> wss://relay.example.com
```

A minimal raw-WebSocket sketch (JavaScript) shows the message flow — the hex payloads come from a
negentropy implementation, not from this snippet:

```js
const ws = new WebSocket("wss://relay.example.com");

ws.onopen = () => {
  // initialMessageHex = hex of Negentropy::initiate() over your local set
  ws.send(JSON.stringify(["NEG-OPEN", "neg-1", { kinds: [1] }, initialMessageHex]));
};

ws.onmessage = (ev) => {
  const msg = JSON.parse(ev.data);
  if (msg[0] === "NEG-MSG") {
    const [, subid, hex] = msg;
    // feed `hex` to your negentropy reconciler; it returns have/need id sets
    // and (optionally) a follow-up message to send back:
    const { have, need, nextMessageHex } = reconcile(hex);
    if (nextMessageHex) {
      ws.send(JSON.stringify(["NEG-MSG", subid, nextMessageHex]));
    } else {
      // reconciliation complete: the relay will send EVENT frames + EOSE for `need`
      ws.send(JSON.stringify(["NEG-CLOSE", subid]));
      // upload `have` with EVENT
    }
  } else if (msg[0] === "EVENT" || msg[0] === "EOSE") {
    // events the relay sent because you were missing them
  } else if (msg[0] === "CLOSED") {
    console.error("negentropy error:", msg[2]);
  }
};
```

In Rust, the `airfry-negentropy` crate exposes the same building blocks the relay uses:

```rust
use std::sync::Arc;
use airfry_negentropy::{Item, Negentropy, NegentropySet};

// Seal your local set of (timestamp, id) items.
let set = Arc::new(NegentropySet::new(items)?);
// frame_size_limit = 0 means unlimited; a non-zero value must be >= 4096.
let mut ne = Negentropy::new(set, 0)?;

let mut msg = ne.initiate()?;          // send as NEG-OPEN payload (hex-encoded)
let mut have = Vec::new();
let mut need = Vec::new();
// for each NEG-MSG reply from the relay:
let reply = ne.reconcile(&peer_msg, &mut have, &mut need)?;
// `have` = ids we have that the peer needs; `need` = ids the peer has that we need.
```

### Configuration and limits

The relay enforces the following limits (from `crates/airfry-relay/src/negentropy.rs` and
`crates/airfry-relay/src/server.rs`):

- **Concurrent negentropy subscriptions:** at most **8** per connection
  (`MAX_NEG_SUBS_PER_CONN`). Re-opening an existing subscription id replaces it and does not count
  against the limit; exceeding the limit returns `CLOSED` with `invalid: too many negentropy
  subscriptions`.
- **Message size:** a `NEG-MSG`/`NEG-OPEN` hex payload may be at most **1 MiB of hex characters**
  (`MAX_NEG_MSG_HEX = 1 << 20`); larger payloads are rejected with `invalid: negentropy message too
  large`.
- **Protocol version:** the initial message must carry a protocol version in the range `0x60..=0x6f`;
  other versions are rejected with `unsupported: negentropy protocol version 0x..`.
- **Frame size limit:** the relay constructs its responder with `frame_size_limit = 0` (unlimited),
  so it does not artificially cap its own outgoing messages. The crate itself requires any non-zero
  frame-size limit to be at least 4096 bytes and leaves 200 bytes of headroom below the limit.
- **Filter `limit`:** ignored for the reconciled set (the full matching set is reconciled).

The `airfry-negentropy` crate additionally enforces: ids must be exactly 32 bytes, timestamps may
not be `u64::MAX` (reserved as the "infinity" bound), a set may not contain duplicate
`(timestamp, id)` items, and range-bound id prefixes are truncated to 32 bytes for wire
compatibility.

### Performance

The `airfry-negentropy` crate is designed for speed: it seals a sorted `(timestamp, id)` set and
precomputes a prefix-sum table of the 256-bit id accumulators, so a range fingerprint is **O(1)**
(one 256-bit subtraction plus one SHA-256 over 32 bytes and a varint). The reference C++
implementation recomputes the accumulator by iterating every item in the range (or by walking LMDB
B-tree nodes in strfry's relay storage). The reconciler also never allocates per item — it walks the
sealed set by index and writes directly into the output buffer. On the relay side, the sealed set is
built once per `NEG-OPEN` and shared through an `Arc`, and the missing-event lookup is a single
store query over the collected id set (no per-id point lookups).

The crate ships a self-contained reconciliation benchmark:

```sh
export PATH=$HOME/.cargo/bin:$PATH
cargo run --release -p airfry-negentropy --bin negentropy-bench -- 10000 3
```

Measured on the development host (10,000-item sets, best of 3 rounds):

| scenario | best time | rounds | bytes on the wire |
| --- | --- | --- | --- |
| identical sets (no differences) | 0.027 ms | 1 | 324 |
| disjoint sets (full difference) | 0.547 ms | 4 | 322,582 |
| partial overlap (half shared) | 0.249 ms | 3 | 162,041 |

These numbers show the intended property: an already-synced client reconciles in a single round with
a few hundred bytes, and even a fully disjoint 10k-item set reconciles in well under a millisecond.

**Head-to-head vs strfry.** The negentropy benchmark drives a full initiator↔responder
reconciliation over identical item sets for both implementations. The baseline is the **vendored
hoytech/negentropy C++ library that strfry embeds** (`third_party/negentropy-ref`, built as
`test/cpp/bench.cpp`) — i.e. a faithful strfry baseline, not a proxy. Full methodology, the complete
scenario matrix (1k/10k/100k items × 0%/50%/99% overlap), and reproduction commands are in
**[BENCHMARK.md](BENCHMARK.md)**.

Measured with `scripts/bench-negentropy.sh 20` (best-of-20, fresh process per scenario, release
builds). Lower is better. **On a fresh clone, run `scripts/fetch_negentropy_ref.sh` first** —
`third_party/` is git-ignored, so the hoytech/negentropy C++ reference (and strfry) are not
present until that script fetches and builds them. Without the reference, the interop and
upstream conformance tests in `crates/airfry-negentropy/tests/` **SKIP** (they print a notice and
return) rather than fail.

| metric | Airfry (`airfry-negentropy`) | strfry-ref (hoytech C++) | speedup |
| --- | --- | --- | --- |
| negentropy reconcile, 10k identical/99% overlap (ms) | 0.0311 | 0.0751 | 2.42x |
| negentropy reconcile, 10k partial overlap 50% (ms) | 0.1956 | 0.7770 | 3.97x |
| negentropy reconcile, 10k disjoint items (ms) | 0.4626 | 1.4965 | 3.24x |

Wire traffic is identical between the two implementations (within one byte, the protocol-version
framing byte), so the speedup comes from the implementation, not from sending fewer bytes. Airfry is
faster than the strfry reference in **every** benchmarked scenario; the headline case (10k items,
50% overlap) is **0.196 ms vs 0.777 ms — a 3.97x speedup**. The crate-level reconciler is the
component the relay uses for every `NEG-MSG` round, so these numbers bound the per-round cost of a
relay reconciliation.

### References

- NIP-77 specification: <https://github.com/nostr-protocol/nips/blob/master/77.md>
- Negentropy protocol (hoytech): <https://github.com/hoytech/negentropy>
- strfry's negentropy reference implementation: <https://github.com/hoytech/strfry> (the interop
  tests use the hoytech/negentropy C++ reference at `third_party/negentropy-ref`, fetched by
  `scripts/fetch_negentropy_ref.sh` — it is not committed)
- Range-Based Set Reconciliation: <https://logperiodic.com/rbsr.html>

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
`airfry_core::codec` (version byte, id, pubkey, `created_at`, kind, tags, content, signature). The
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
cargo test -p airfry-e2e

# negentropy protocol conformance + strfry/negentropy reference interop
cargo test -p airfry-negentropy
```

`crates/airfry-e2e` contains:

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

`crates/airfry-negentropy` contains protocol conformance tests and byte-for-byte interop tests
against the vendored strfry/negentropy C++ reference harness (`tests/strfry_interop.rs`,
`tests/upstream_suite.rs`); the interop tests skip with a printed notice if the reference binary is
not built.

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
cargo run --release -p airfry-store --bin store_bench 1000000

# individual load generator modes
target/release/airfry-bench smoke --url ws://127.0.0.1:7777
target/release/airfry-bench load  --url ws://127.0.0.1:7777 --events 200000 --conns 32
target/release/airfry-bench req   --url ws://127.0.0.1:7777 --queries 5000 --conns 16
target/release/airfry-bench mixed --url ws://127.0.0.1:7777 --duration 30
```

`airfry-bench` prints a JSON summary and a markdown table suitable for pasting into
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

| metric | Airfry (`airfry-relay`) | strfry v1-4cd3cf6 | speedup |
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
crates/airfry-core/        # events, schnorr verification, filters, wire codec
crates/airfry-store/       # custom storage engine
crates/airfry-relay/       # relay binary
crates/airfry-bench/       # load generator
crates/airfry-e2e/         # conformance + adversarial tests
crates/airfry-negentropy/  # NIP-77 negentropy set reconciliation (protocol v1, O(1) fingerprints)
scripts/verify.sh         # acceptance gate
scripts/compare_strfry.sh # head-to-head benchmark vs strfry
scripts/fetch_strfry.sh   # fetch + build strfry at the pinned commit
scripts/fetch_negentropy_ref.sh # fetch + build the hoytech/negentropy C++ reference (bench + harness)
scripts/bench-negentropy.sh     # reproducible negentropy benchmark vs the strfry reference
.github/workflows/ci.yml  # CI: fmt, clippy, build, test
third_party/strfry/       # strfry source + built binary (fetched, not committed)
third_party/negentropy-ref/ # hoytech/negentropy C++ reference (fetched, not committed)
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
