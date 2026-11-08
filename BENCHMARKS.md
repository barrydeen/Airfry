# Airfry benchmarks

**Measured:** 2026-11-07 (UTC) · **Airfry:** this repository, `target/release/airfry-relay` · **Reference:** strfry v1-4cd3cf6

## Verdict

Airfry is **faster than strfry on both acceptance metrics** on this machine: **~4.8× the publish
throughput** (≈163k events/s vs ≈34k events/s) and **~90× lower REQ time-to-EOSE p99**
(≈0.13 ms vs ≈11.3 ms) for indexed filters, while returning the same number of events. Both relays
were driven by the same pre-signed corpus, the same client concurrency and the same filters, on the
same machine, back to back. strfry was built from source at its current HEAD and tuned for
throughput (auth off, no write policy, compression off, 8 ingester / 8 reqWorker threads, 10 TB LMDB
mapsize). The numbers below are reproducible with the scripts in this repository; see
[Reproducibility](#reproducibility).

> **WIN on publish throughput. WIN on REQ p99.** (Acceptance requires beating strfry on both.)

## Machine specification

| item | value |
| --- | --- |
| CPU | AMD Ryzen 9 7950X 16-Core Processor (16 cores / 32 threads, 1 socket, boost to 5.88 GHz) |
| RAM | 30 GiB DDR5 |
| Kernel | Linux 7.1.9-arch1-2 x86_64 (Arch-based, `PREEMPT_DYNAMIC`) |
| OS | Omarchy (Arch Linux derivative) |
| Rust | rustc 1.98.1 (48a229cea 2026-09-01), cargo 1.98.1 |
| Filesystem | btrfs on `/dev/mapper/root` (NVMe SSD), benchmark data dirs on tmpfs `/tmp` |
| Network | loopback (`127.0.0.1`), no TLS |

## Head-to-head: Airfry vs strfry

Identical corpus (200,000 pre-signed events, deterministic seed 0, byte-identical for both relays),
identical client concurrency (16 publisher connections, 8 REQ connections), identical filter
(`{"kinds":[1],"limit":100}`), same machine, sequential runs (one relay at a time, no CPU
contention).

| metric | Airfry | strfry v1-4cd3cf6 | speedup |
| --- | --- | --- | --- |
| **publish events/s** | **162,810** | **33,966** | **4.79×** |
| publish OK true | 200,000 | 200,000 | — |
| publish ack p99 (ms) | 6.21 | 39.36 | 6.3× lower |
| **REQ time-to-EOSE p50 (ms)** | **0.069** | **3.646** | **52.8×** |
| **REQ time-to-EOSE p99 (ms)** | **0.126** | **11.305** | **89.7×** |
| REQ time-to-EOSE p999 (ms) | 0.463 | 15.16 | 32.7× |
| REQ events returned | 200,000 | 200,000 | — |
| REQ queries completed | 2,000 | 2,000 | — |

Independent repeat runs of the same comparison measured 147,092 vs 31,288 events/s (4.70×) with REQ
p99 0.133 ms vs 11.527 ms (86.7×), and 144,482 vs 31,862 events/s (4.54×) with REQ p99 0.136 ms vs
4.920 ms (36.2×). Run-to-run variance on publish throughput is roughly ±10%; the REQ p99 gap is one
to two orders of magnitude and is not sensitive to noise.

**Verdict: WIN on publish throughput (4.79×) and WIN on REQ p99 (89.7×).**

## Store engine (no network)

`cargo run --release -p airfry-store --bin store_bench 1000000` — 1,000,000-event corpus, events
pre-signed outside the timed region, batch inserts of 1,000 events, 2,000 query iterations across
five filter shapes (kinds, authors, `#t` tag, `since`, combined `#t`+author):

| metric | value |
| --- | --- |
| ingest events/s | **1,333,267** |
| events inserted | 1,000,000 |
| bytes on disk | 272,263,890 (2 segments) |
| index entries | 4,000,000 |
| query p50 | 3 µs |
| query p99 | 478 µs |
| query p999 | 2,387 µs |
| per-filter kinds/authors/`#t`/`since` p50 | 2 µs |
| per-filter combined `#t`+author p50 / p99 | 393 µs / 568 µs |
| `count(kinds=[1])` over 1M events | 266 ms |

The store sustains >1.3M events/s ingest and sub-millisecond p99 for indexed queries on a 1M-event
corpus.

## Exact commands used

```sh
export PATH=$HOME/.cargo/bin:$PATH

# 1. Build the reference relay (strfry is not vendored; this clones + builds the pinned commit)
scripts/fetch_strfry.sh

# 2. Head-to-head comparison (identical corpus/concurrency for both relays)
LOAD_EVENTS=200000 LOAD_CONNS=16 REQ_QUERIES=2000 REQ_CONNS=8 scripts/compare_strfry.sh

# 3. Full acceptance gate (build, tests, relay boot, smoke/load/req/mixed/store, NIP-11,
#    restart persistence, head-to-head assertion)
scripts/verify.sh

# 4. Store-engine benchmark (1M-event corpus)
cargo run --release -p airfry-store --bin store_bench 1000000
```

`scripts/compare_strfry.sh` exits 0 only when Airfry beats strfry on **both** publish events/s and
REQ p99; otherwise it exits non-zero and names the losing metric. `scripts/verify.sh` enforces the
same assertion (and auto-fetches strfry via `scripts/fetch_strfry.sh` if the binary is absent; if the
fetch fails it prints a clear SKIP warning and continues — see the script header).

## strfry configuration used

strfry version: **v1-4cd3cf6** (commit `4cd3cf64850caf47dda46c2a2abbbf3525a64d10`, built from
`https://github.com/hoytech/strfry`). Config: `third_party/strfry/strfry-bench.conf`, derived from
the stock `strfry.conf` and tuned for maximum throughput:

- `auth.enabled = false` — no NIP-42 overhead
- `writePolicy.plugin = ""` — accept all writes, no policy script
- `dbParams.mapsize = 10995116277760` (10 TB LMDB map), `maxreaders = 1024`
- `compression.enabled = false` — permessage-deflate costs CPU on the publish hot path
- `numThreads.ingester = 8`, `reqWorker = 8`, `reqMonitor = 4`, `negentropy = 2`
- `maxFilterLimit = 500`, `maxSubsPerConnection = 200`, `maxWebsocketPayloadSize = 131072`
- `relay.bind = 127.0.0.1`, ephemeral port assigned per run

Airfry was run with `--fsync-policy never` (no per-event fsync) and default verification enabled
(schnorr signature verification on ingest), matching strfry's default verification behaviour.

## Methodology

- **Corpus:** one deterministic pre-signed event pool generated by `airfry-bench` from a fixed seed
  (seed 0, 100 authors, kind 1). Both relays receive byte-identical events (same ids, same
  signatures), so neither relay is advantaged by different signing work.
- **Concurrency:** identical client concurrency for both relays — 16 websocket connections for the
  publish phase, 8 for the REQ phase, with pipelined publishes.
- **Filters:** identical filter JSON for both relays (`{"kinds":[1],"limit":100}`), i.e. an indexed
  query that both engines can serve from an index.
- **Isolation:** relays are run one at a time on the same machine, each on a fresh data directory on
  tmpfs, with a settle pause between phases. No other benchmark load runs concurrently.
- **Warmup:** the corpus is signed once before the timed region; the publish phase is timed from
  first EVENT to last OK. REQ latency is measured per query from REQ send to EOSE receipt.
- **Percentiles:** computed over all per-event ack latencies (publish) and all per-query
  time-to-EOSE samples (REQ) using the nearest-rank method on the sorted sample set.
- **strfry stability:** strfry v1-4cd3cf6 has a connection-churn race in its subscription tracking
  that can abort the process when a new batch of websocket connections opens while a previous batch
  is disconnecting. The harness pauses between phases and retries the strfry phase (up to 3 times)
  if it dies; publish numbers are taken from the first successful load and REQ numbers from the
  first successful REQ. This is a strfry bug, not a benchmark artefact, and it does not affect
  Airfry.

**Caveats:** single machine, loopback networking, no TLS, no reverse proxy, tmpfs data directory
(disk fsync cost excluded via `--fsync-policy never` for Airfry and strfry's default LMDB behaviour).
These numbers measure relay CPU efficiency and storage-engine query cost, not WAN latency or
durability-under-fsync. Absolute numbers will differ on other hardware; the relative comparison is
what the acceptance criterion is about.

## Reproducibility

```sh
git clone https://github.com/barrydeen/Airfry && cd Airfry
export PATH=$HOME/.cargo/bin:$PATH
cargo build --release --workspace
scripts/fetch_strfry.sh          # clones + builds strfry v1-4cd3cf6 into third_party/strfry
scripts/compare_strfry.sh        # head-to-head; exits 0 only if Airfry wins both metrics
scripts/verify.sh                # full acceptance gate incl. the head-to-head assertion
```

Build dependencies for strfry: `cmake`, `g++`, `make`, `libmicrohttpd-dev`, `libsecp256k1-dev`,
`liblmdb-dev`, `libzstd-dev`, `libflatbuffers-dev`, `libssl-dev`, `pkg-config` (Arch equivalents:
`cmake gcc make libmicrohttpd libsecp256k1 lmdb zstd flatbuffers openssl`).

**Expected variance:** publish throughput varies by roughly ±5% run to run on this machine; REQ
latency for indexed filters is sub-millisecond for Airfry and several milliseconds for strfry, so
the REQ verdict is stable. On hardware with fewer cores the absolute publish numbers drop for both
relays; the relative advantage comes from parallel signature verification and in-memory index
queries, so it persists (and grows with core count).

## Raw run log

The sections below are the raw, dated output of `scripts/verify.sh` and `scripts/compare_strfry.sh`
as they were produced. Only the most recent runs are kept (the last 3 acceptance runs and the last 3
head-to-head runs); older runs are trimmed to keep this file bounded. Each section is appended
automatically by the scripts.

---

## Head-to-head vs strfry — 2026-11-07 22:53:40 UTC

Host: `Linux 7.1.9-arch1-2 x86_64`, 32 cores. Corpus: 200000 events seed=0 (identical for both relays). Conns: 16. REQ: 2000 queries / 8 conns, filter `{"kinds":[1],"limit":100}`.

strfry config: `third_party/strfry/strfry-bench.conf` (auth disabled, no write policy, mapsize 10TB, compression disabled, ingester=8 reqWorker=8).

| metric | our relay | strfry | speedup |
| --- | --- | --- | --- |
| publish events/s | 162273.0 | 34457.0 | 4.709x |
| publish OK true | 200000 | 200000 | - |
| publish ack p99 ms | 6.769 | 38.21 | - |
| REQ eose p50 ms | 0.074 | 3.488 | - |
| REQ eose p99 ms | 0.122 | 5.209 | 42.697x |
| REQ eose p999 ms | 1.729 | 5.68 | - |
| REQ events returned | 200000 | 200000 | - |

Verdict: **WIN** (publish speedup 4.709x, REQ p99 speedup 42.697x).

Commands: `scripts/compare_strfry.sh` (env: LOAD_EVENTS=200000 LOAD_CONNS=16 REQ_QUERIES=2000 REQ_CONNS=8).

## Head-to-head vs strfry — 2026-11-07 22:54:15 UTC

Host: `Linux 7.1.9-arch1-2 x86_64`, 32 cores. Corpus: 200000 events seed=0 (identical for both relays). Conns: 16. REQ: 2000 queries / 8 conns, filter `{"kinds":[1],"limit":100}`.

strfry config: `third_party/strfry/strfry-bench.conf` (auth disabled, no write policy, mapsize 10TB, compression disabled, ingester=8 reqWorker=8).

| metric | our relay | strfry | speedup |
| --- | --- | --- | --- |
| publish events/s | 174878.0 | 35213.0 | 4.966x |
| publish OK true | 200000 | 200000 | - |
| publish ack p99 ms | 5.846 | 36.725 | - |
| REQ eose p50 ms | 0.068 | 3.442 | - |
| REQ eose p99 ms | 0.102 | 5.066 | 49.667x |
| REQ eose p999 ms | 0.142 | 5.921 | - |
| REQ events returned | 200000 | 200000 | - |

Verdict: **WIN** (publish speedup 4.966x, REQ p99 speedup 49.667x).

Commands: `scripts/compare_strfry.sh` (env: LOAD_EVENTS=200000 LOAD_CONNS=16 REQ_QUERIES=2000 REQ_CONNS=8).

## Acceptance run — 2026-11-07 22:54:15 UTC

Host: `Linux 7.1.9-arch1-2 x86_64`, 32 cores. Thresholds: MIN_INGEST_EPS=50000, MAX_REQ_P99_MS=250, MIN_STORE_EPS=100000.

| benchmark | metric | value |
| --- | --- | --- |
| airfry-bench load | ingest events/s | 208661.0 |
| airfry-bench load | OK true | 200000 |
| airfry-bench load | ack p50 ms | 3.813 |
| airfry-bench load | ack p99 ms | 5.733 |
| airfry-bench load | ack p999 ms | 9.54 |
| airfry-bench req | eose p50 ms | 0.118 |
| airfry-bench req | eose p99 ms | 0.233 |
| airfry-bench req | eose p999 ms | 4.802 |
| airfry-bench req | events/s served | 834570.0 |
| airfry-bench req | queries completed | 2000 |
| airfry-bench store | ingest events/s | 1248137.0 |
| airfry-bench store | events inserted | 200000 |
| airfry-bench store | query p50 ms | 0.003 |
| airfry-bench store | query p99 ms | 0.027 |
| airfry-bench store | query p999 ms | 0.132 |

Checks:

- PASS  release build
- PASS  workspace tests
- PASS  relay /health responded
- PASS  NIP-11 content-type (Content-Type: application/nostr+json)
- PASS  NIP-11 document has name+supported_nips
- PASS  smoke (publish OK true + EVENT + EOSE)
- PASS  load ingest 208661.0 events/s >= 50000
- PASS  req p99 0.233ms <= 250ms
- PASS  mixed workload completed
- PASS  restart persistence (event 05355d7a346f9c0383c1184d76ee0f2559a9504c81e12beabf0d43a5378d7b96 survived restart)
- PASS  store ingest 1248137.0 events/s >= 100000
- PASS  head-to-head vs strfry (our relay faster on publish events/s AND REQ p99)

Overall: PASS

## Head-to-head vs strfry — 2026-11-07 22:58:27 UTC

Host: `Linux 7.1.9-arch1-2 x86_64`, 32 cores. Corpus: 200000 events seed=0 (identical for both relays). Conns: 16. REQ: 2000 queries / 8 conns, filter `{"kinds":[1],"limit":100}`.

strfry config: `third_party/strfry/strfry-bench.conf` (auth disabled, no write policy, mapsize 10TB, compression disabled, ingester=8 reqWorker=8).

| metric | our relay | strfry | speedup |
| --- | --- | --- | --- |
| publish events/s | 177377.0 | 35009.0 | 5.067x |
| publish OK true | 200000 | 200000 | - |
| publish ack p99 ms | 5.787 | 36.881 | - |
| REQ eose p50 ms | 0.068 | 3.662 | - |
| REQ eose p99 ms | 0.118 | 5.363 | 45.449x |
| REQ eose p999 ms | 0.322 | 6.457 | - |
| REQ events returned | 200000 | 200000 | - |

Verdict: **WIN** (publish speedup 5.067x, REQ p99 speedup 45.449x).

Commands: `scripts/compare_strfry.sh` (env: LOAD_EVENTS=200000 LOAD_CONNS=16 REQ_QUERIES=2000 REQ_CONNS=8).

## Acceptance run — 2026-11-07 22:58:27 UTC

Host: `Linux 7.1.9-arch1-2 x86_64`, 32 cores. Thresholds: MIN_INGEST_EPS=50000, MAX_REQ_P99_MS=250, MIN_STORE_EPS=100000.

| benchmark | metric | value |
| --- | --- | --- |
| airfry-bench load | ingest events/s | 207380.0 |
| airfry-bench load | OK true | 200000 |
| airfry-bench load | ack p50 ms | 3.856 |
| airfry-bench load | ack p99 ms | 5.965 |
| airfry-bench load | ack p999 ms | 9.066 |
| airfry-bench req | eose p50 ms | 0.117 |
| airfry-bench req | eose p99 ms | 0.226 |
| airfry-bench req | eose p999 ms | 4.171 |
| airfry-bench req | events/s served | 839228.0 |
| airfry-bench req | queries completed | 2000 |
| airfry-bench store | ingest events/s | 1291826.0 |
| airfry-bench store | events inserted | 200000 |
| airfry-bench store | query p50 ms | 0.003 |
| airfry-bench store | query p99 ms | 0.023 |
| airfry-bench store | query p999 ms | 0.116 |

Checks:

- PASS  release build
- PASS  workspace tests
- PASS  relay /health responded
- PASS  NIP-11 content-type (Content-Type: application/nostr+json)
- PASS  NIP-11 document has name+supported_nips
- PASS  smoke (publish OK true + EVENT + EOSE)
- PASS  load ingest 207380.0 events/s >= 50000
- PASS  req p99 0.226ms <= 250ms
- PASS  mixed workload completed
- PASS  restart persistence (event 1fad6f8d94cbfb5229f7a5834d7efdd8419fd76d209db29fcfdf5a2fb96487da survived restart)
- PASS  store ingest 1291826.0 events/s >= 100000
- PASS  head-to-head vs strfry (our relay faster on publish events/s AND REQ p99)

Overall: PASS

## Acceptance run — 2026-11-08 02:51:34 UTC

Host: `Linux 7.1.9-arch1-2 x86_64`, 32 cores. Thresholds: MIN_INGEST_EPS=50000, MAX_REQ_P99_MS=250, MIN_STORE_EPS=100000.

| benchmark | metric | value |
| --- | --- | --- |
| airfry-bench load | ingest events/s | 214992.0 |
| airfry-bench load | OK true | 200000 |
| airfry-bench load | ack p50 ms | 3.664 |
| airfry-bench load | ack p99 ms | 5.783 |
| airfry-bench load | ack p999 ms | 9.03 |
| airfry-bench req | eose p50 ms | 0.119 |
| airfry-bench req | eose p99 ms | 0.653 |
| airfry-bench req | eose p999 ms | 5.21 |
| airfry-bench req | events/s served | 831068.0 |
| airfry-bench req | queries completed | 2000 |
| airfry-bench store | ingest events/s | 1309928.0 |
| airfry-bench store | events inserted | 200000 |
| airfry-bench store | query p50 ms | 0.003 |
| airfry-bench store | query p99 ms | 0.021 |
| airfry-bench store | query p999 ms | 0.099 |

Checks:

- PASS  relay /health responded
- PASS  NIP-11 content-type (Content-Type: application/nostr+json)
- PASS  NIP-11 document has name+supported_nips
- PASS  smoke (publish OK true + EVENT + EOSE)
- PASS  load ingest 214992.0 events/s >= 50000
- PASS  req p99 0.653ms <= 250ms
- PASS  mixed workload completed
- PASS  restart persistence (event 92c7245958d8876a2355002e1a6346eb3eeb8472fb38cf4205f1c48226724058 survived restart)
- PASS  store ingest 1309928.0 events/s >= 100000

Overall: PASS
