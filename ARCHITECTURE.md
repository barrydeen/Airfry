# Airfry — architecture: fastest Nostr relay in Rust with a custom Nostr-optimized storage engine

> Airfry is the project name. The crates (`nostr-core`, `nostr-store`, `nostr-relay`, `nostr-bench`,
> `nostr-e2e`) and the release binary (`nostr-relay`) keep their names.

## Goal
A production-shaped Nostr relay (`nostr-relay`) backed by a purpose-built storage engine
(`nostr-store`) that beats generic DB-backed relays on ingest throughput and REQ latency.
Everything must build with `cargo build --release` and pass `cargo test --workspace`.

## Workspace layout (fixed, do not rename)
```
Cargo.toml                # workspace root (owned by Director)
crates/nostr-core/        # event types, schnorr verification, filter matching, NIP-01 wire codec (Director)
crates/nostr-store/       # custom storage engine (agent: storage)
crates/nostr-relay/       # websocket relay server binary (agent: relay)
crates/nostr-bench/       # load generator + smoke client (agent: bench)
scripts/verify.sh         # end-to-end verification (agent: bench)
BENCHMARKS.md             # measured numbers (agent: bench)
README.md                 # usage (agent: hardening)
```

## Ownership rules
- Only edit files inside your own crate (plus your assigned docs/scripts). Never edit
  `crates/nostr-core` or the workspace `Cargo.toml`; if you need a change there, message the Director.
- Adding a dependency: add it to **your crate's** `Cargo.toml` only.
- Keep `cargo build --workspace` green at all times. Commit-quality code, no `todo!()` left behind.

## nostr-core public API (authoritative; already implemented)
```rust
pub struct Event { pub id: [u8;32], pub pubkey: [u8;32], pub created_at: u64, pub kind: u32,
                   pub tags: Vec<Vec<String>>, pub content: String, pub sig: [u8;64] }
impl Event {
  pub fn compute_id(&self) -> [u8;32];
  pub fn verify(&self) -> Result<(), CoreError>;      // schnorr + id check
  pub fn from_json_str(s: &str) -> Result<Event, CoreError>;
  pub fn to_json(&self) -> String;
  pub fn to_json_value(&self) -> serde_json::Value;
  pub fn tag_values(&self, name: &str) -> impl Iterator<Item=&str>;
}
pub struct Filter { pub ids: Option<Vec<[u8;32]>>, pub authors: Option<Vec<[u8;32]>>,
                    pub kinds: Option<Vec<u32>>, pub since: Option<u64>, pub until: Option<u64>,
                    pub limit: Option<usize>, pub tags: Vec<(char, Vec<String>)>, pub search: Option<String> }
impl Filter {
  pub fn matches(&self, e: &Event) -> bool;
  pub fn from_json_value(v: &serde_json::Value) -> Result<Filter, CoreError>;
}
pub enum ClientMessage { Event(Event), Req { sub_id: String, filters: Vec<Filter> },
                         Close(String), Auth(Event), Count { sub_id: String, filters: Vec<Filter> } }
pub fn parse_client_message(s: &str) -> Result<ClientMessage, CoreError>;
pub enum RelayMessage { Event { sub_id: String, event: Event }, Ok { id: String, ok: bool, message: String },
                        Eose { sub_id: String }, Closed { sub_id: String, message: String },
                        Notice(String), Auth(String), Count { sub_id: String, count: usize } }
impl RelayMessage { pub fn to_json(&self) -> String; }
```

## nostr-store public API (agent: storage must implement exactly this)
```rust
pub struct StoreConfig { pub path: std::path::PathBuf, pub fsync: FsyncPolicy, pub segment_bytes: u64 }
pub enum FsyncPolicy { Never, Interval(std::time::Duration), Always }
pub enum InsertOutcome { Stored, Duplicate, Rejected(&'static str) }
pub struct StoreStats { pub events: u64, pub bytes_on_disk: u64, pub segments: u64, pub index_entries: u64 }
pub struct Store { /* ... */ }
impl Store {
  pub fn open(cfg: StoreConfig) -> Result<Store, StoreError>;
  pub fn insert(&self, ev: &Event) -> Result<InsertOutcome, StoreError>;
  pub fn insert_batch(&self, evs: &[Event]) -> Result<Vec<InsertOutcome>, StoreError>;
  pub fn query(&self, filters: &[Filter]) -> Result<Vec<std::sync::Arc<Event>>, StoreError>; // newest-first, dedup across filters, per-filter limit
  pub fn count(&self, filters: &[Filter]) -> Result<usize, StoreError>;
  pub fn delete(&self, id: &[u8;32], pubkey: &[u8;32]) -> Result<bool, StoreError>; // NIP-09
  pub fn stats(&self) -> StoreStats;
  pub fn flush(&self) -> Result<(), StoreError>;
}
```
Requirements: append-only segmented log with checksums, crash-safe recovery, in-memory indexes
(id, author, kind, tag name/value, created_at), no external DB, no full-file scans on query,
`insert` must be lock-light and batchable, queries must never block writers for long.
Target: >= 100k events/s ingest on this machine (32 cores) and sub-millisecond in-memory REQ.

## nostr-relay (agent: relay)
- Binary `nostr-relay`, config via CLI flags + env: `--bind`, `--data-dir`, `--relay-name`,
  `--relay-pubkey`, `--max-event-bytes`, `--max-subs-per-conn`, `--max-filters`, `--max-limit`.
- NIP-01: EVENT/REQ/CLOSE/EOSE/OK/NOTICE/CLOSED, multiple filters per REQ, live fanout of new
  events to matching open subscriptions, per-connection limits, backpressure.
- NIP-11 relay info document served on HTTP GET with `Accept: application/nostr+json`.
- NIP-45 COUNT, NIP-09 deletion, NIP-40 expiration filtering, NIP-42 AUTH (optional but wire it).
- Graceful shutdown on SIGINT/SIGTERM, structured logs, `/health` endpoint.

## nostr-bench (agent: bench)
- Binary `nostr-bench` with subcommands: `smoke`, `load`, `req`, `mixed`.
- `load`: N concurrent publishers signing events (pre-signed pool), measure events/s.
- `req`: measure REQ latency p50/p99/p999 for filters over the ingested corpus.
- `mixed`: realistic blend, report JSON + markdown table.
- `scripts/verify.sh`: build release, run workspace tests, boot relay on a temp dir, run smoke +
  load + req, assert thresholds, exit non-zero on failure. This is the mission's acceptance gate.

## Definition of done
1. `cargo build --release --workspace` succeeds with no warnings-as-errors surprises.
2. `cargo test --workspace` passes.
3. `scripts/verify.sh` exits 0 and prints measured throughput/latency.
4. `BENCHMARKS.md` contains real measured numbers from this machine.
