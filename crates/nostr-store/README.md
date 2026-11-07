# nostr-store

The Nostr-optimized storage engine behind the [Airfry](https://github.com/barrydeen/Airfry)
relay. It is an append-only segmented log with in-memory indexes built for Nostr query
patterns, designed for very high ingest throughput and sub-millisecond indexed queries.

## Public API

- `Store::open(StoreConfig)` — open (or recover) a store at a directory.
- `Store::insert(&Event)` / `Store::insert_batch(&[Event])` — append events; batch is the fast
  path (single append + fsync amortization).
- `Store::query(&[Filter])` — newest-first results, per-filter `limit`, dedup across filters,
  honoring `ids`/`authors`/`kinds`/`since`/`until`/`#tag` and `search`.
- `Store::count(&[Filter])` — count matching events (ignores `limit`).
- `Store::delete(&id, &pubkey)` — NIP-09 tombstone (owner-checked).
- `Store::stats()` — real `StoreStats` (`events`, `bytes_on_disk`, `segments`, `index_entries`).
- `Store::flush()` — force durability.
- `StoreConfig { path, fsync, segment_bytes }` with `FsyncPolicy::{Never, Interval, Always}`.
- `InsertOutcome::{Stored, Duplicate, Rejected}`.

Expired events (NIP-40) are filtered out at query time; `Store::is_expired(ev, now)` is exposed
as a helper.

## On-disk format

Segments are named `seg-<10-digit index>.log`. Each record is framed as:

```text
offset  size  field
0       4     magic  = 0x4E535452 ("NSTR")
4       1     record type (1 = event, 2 = delete tombstone)
5       4     payload length (LE u32)
9       4     crc32c over (type || payload) (LE u32)
13      N     payload
```

Event payloads are `nostr_core::codec::encode_event` output (binary, not JSON). Delete records
carry the 32-byte event id followed by the 32-byte author pubkey. On open the engine scans
segments, validates magic/length/CRC, and discards a torn or corrupt tail record (truncating
back to the last good boundary) so a crash never makes the store unopenable.

## Usage

```rust
use nostr_core::{Event, Filter};
use nostr_store::{Store, StoreConfig, FsyncPolicy};

let mut cfg = StoreConfig::new("./nostr-data");
cfg.fsync = FsyncPolicy::Interval(std::time::Duration::from_millis(200));
let store = Store::open(cfg).unwrap();

// insert_batch is the high-throughput path
store.insert_batch(&events).unwrap();

let filter = Filter::from_json_value(&serde_json::json!({
    "kinds": [1], "limit": 50
})).unwrap();
let hits = store.query(&[filter]).unwrap();
println!("{} events, {} bytes on disk", store.stats().events, store.stats().bytes_on_disk);
```

## Benchmarking

`cargo run --release --bin store_bench` measures raw store ingest and query latency. The
`nostr-bench store` subcommand also drives this engine directly.

## See also

- Repository and project overview: <https://github.com/barrydeen/Airfry>
- Benchmarks: [BENCHMARKS.md](https://github.com/barrydeen/Airfry/blob/main/BENCHMARKS.md)

## License

MIT.
