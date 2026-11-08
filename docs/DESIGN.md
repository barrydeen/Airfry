# Airfry design: airfry-relay and airfry-store

> Airfry is the project name; the crates and the release binary are airfry-branded (`airfry-*`).

This document describes how the relay and its storage engine are built, and why the design is
faster than strfry. It is written to be read alongside the code: file paths are given for every
component so claims can be checked against the implementation.

- Relay: `crates/airfry-relay/src/` (`main.rs`, `config.rs`, `http.rs`, `hub.rs`, `nip11.rs`, `util.rs`)
- Storage: `crates/airfry-store/src/`
- Shared primitives: `crates/airfry-core/src/` (`lib.rs`, `keys.rs`, `codec.rs`)

---

## 1. Threat model and design constraints

A public Nostr relay is an adversarial environment. Any client can open a socket and send anything.
The design therefore assumes:

- **Hostile input.** Frames may be truncated, non-UTF-8, deeply nested, enormous, or syntactically
  valid but semantically wrong (bad signature, id mismatch, `since > until`, `limit: 0`).
- **Hostile load.** A single client may open thousands of subscriptions, or publish as fast as its
  socket allows. A relay must never let one client starve the others.
- **Unclean shutdown.** Power loss and `SIGKILL` happen. The store must recover to a consistent state
  without operator intervention.
- **No trusted writers.** Every event is verified before it is persisted, unless the operator
  explicitly opts out (`--verify-events false`) for bulk import.

Performance is a first-class requirement, but never at the cost of correctness: an event that is
returned by a REQ must be the same event that was accepted, with a valid signature.

## 2. Storage engine design (`airfry-store`)

### 2.1 Segmented append-only log

The store persists events in a sequence of segment files inside the configured data directory. A
segment is rotated when it reaches `segment_bytes` (default 256 MiB, configurable via
`--segment-bytes`). Rotation is a metadata operation: a new file is created and subsequent appends
target it; existing segments are immutable once closed.

Each record is a self-describing frame written by `crates/airfry-store/src/framing.rs`
(`write_record`), with the exact on-disk layout:

```
offset  size  field
0       4     magic = 0x4E535452 ("NSTR", LE u32)
4       1     record type (1 = event, 2 = delete tombstone)
5       4     payload length (LE u32)
9       4     crc32c over (record type byte || payload)  (LE u32)
13      N     payload
```

The magic is checked first, then the record type must be 1 or 2, then the length must be at most
`MAX_PAYLOAD` (64 MiB), then the CRC must match before the payload is decoded. Segment files are
named `seg-<10-digit index>.log` so they sort lexicographically in write order.

The payload uses `airfry_core::codec` (`encode_event` / `decode_event`), a compact binary encoding
that is bounds-checked on decode and rejects truncation, bad version bytes and invalid UTF-8. Using a
binary encoding rather than JSON on the hot path removes the JSON parser from ingest entirely.

Record types distinguish:

- **event records** — a full event (id, pubkey, `created_at`, kind, tags, content, signature);
- **tombstone records** — a NIP-09 deletion (target id + author pubkey), so deletions survive
  restart.

### 2.2 Checksums and integrity

Every record carries a CRC32C over its record-type byte and payload (`framing::record_crc`). On
replay, a record whose magic is wrong, whose type is unknown, whose length is implausible, whose
checksum does not match, or whose payload fails `decode_event` terminates replay at that offset. This is the mechanism that makes a torn tail
harmless: the partially written final record is simply not part of the log.

Checksums are not a security boundary (an attacker who can rewrite the file can rewrite the
checksum); they are a crash-consistency boundary. Signature verification is the security boundary,
and it happens before the record is written.

### 2.3 Recovery

Recovery runs at `Store::open`:

1. Enumerate segment files in the data directory in creation order.
2. Replay each segment record by record, validating length, checksum and payload.
3. On the first invalid record, stop replay for that segment and truncate the file to the last valid
   offset (torn-tail handling). Later segments are still replayed — a corrupt segment does not
   discard the rest of the log.
4. Rebuild the in-memory indexes from the replayed events, applying tombstones as they are
   encountered.
5. Open the final segment for appending.

The recovery path is deliberately the *only* place the log is read sequentially. Normal operation
never scans the log.

### 2.4 In-memory indexes

The query path is served entirely from memory. The index set covers every filter dimension NIP-01
allows:

- **id index** — `[u8; 32] -> record location` (also the duplicate-detection set).
- **author index** — `[u8; 32] -> ordered set of events` (newest-first ordering maintained).
- **kind index** — `u32 -> ordered set of events`.
- **tag index** — `(tag name, tag value) -> ordered set of events`, for single-letter tags.
- **time index** — `created_at -> events`, used for `since`/`until` range narrowing and for
  newest-first iteration when no other constraint is present.

The index (`crates/airfry-store/src/index.rs`) holds every stored event as an `Arc<Event>` in
`by_id`, and the secondary lists (`by_author`, `by_kind`, `by_tag`, `by_time`) hold cloned
`Arc<Event>` handles to the same allocation. Because the event bodies are shared by reference
rather than copied per index entry, the resident set is one copy of each event plus the posting
lists — a multi-million-event corpus fits in RAM on a 32-core machine. Per-key lists are appended
unsorted and sorted newest-first lazily on first read after a mutation (`EventList::ensure_sorted`),
so the write path stays a handful of hash-map operations per event while reads still get ordered
iteration and can stop early on `limit`.

Query planning picks the most selective available index for each filter (id > author > tag > kind >
time), intersects with the remaining constraints, applies `limit` per filter, deduplicates across
filters by id, and returns results newest-first. Expired events (NIP-40) are excluded at query time
and at count time.

### 2.5 Lock strategy

The store is designed so that writers and readers do not contend on a single global lock:

- **Append path.** A single writer appends to the active segment. The append itself is serialised by
  a short-held mutex around the file handle and the segment offset; the expensive parts (encoding,
  checksumming, index insertion) happen outside that critical section where possible.
- **Index updates.** The whole `Index` lives behind a single `parking_lot::RwLock` in `Store`.
  Writers take the write lock only to publish a batch of already-encoded events (the encoding,
  checksumming and framing happen before the lock is taken); readers take the read lock and clone
  `Arc`s out of the posting lists. Because the index is append-only (events are tombstoned, never
  removed from the lists), a reader holding the read lock always sees a consistent snapshot.
- **Query path.** Queries take the read lock, walk only the posting lists selected by the filter
  plan, and clone out the `Arc<Event>` results; they never hold a lock while serialising results to
  the client.
- **Batch inserts.** `insert_batch` amortises the append lock over many events and is the path the
  relay uses for pipelined ingest.

The invariant is: **no lock is held across an await point, a network write, or a JSON
serialisation.**

### 2.6 Durability policy

`FsyncPolicy` is explicit and operator-controlled:

- `Never` — the default for benchmarks and for relays that can tolerate losing the last few seconds
  of writes on power loss.
- `Interval(d)` — background fsync every `d`.
- `Always` — fsync per batch; the safe choice for archival relays.

Durability is a policy decision, not a hidden cost. The benchmark comparison against strfry uses the
same policy on both sides so the numbers are comparable.

## 3. Relay concurrency model (`airfry-relay`)

### 3.1 Front end

The relay is a tokio application. The listener accepts connections and spawns a task per connection.
Each connection task owns its socket and its outbound queue; there is no shared writer.

The HTTP layer (`http.rs`) parses the request head itself so it can serve NIP-11 and `/health` on the
same port as the WebSocket endpoint, then hands the socket (with any over-read bytes replayed via
`PrefixedStream`) to the WebSocket upgrade machinery.

### 3.2 Ingest pipeline

1. **Frame admission.** The frame is size-checked against `--max-event-bytes` before parsing. An
   oversized frame is answered with `["OK", <id>, false, "invalid: ..."]` using a cheap id
   extraction (`util::extract_event_id`) so the client still gets a correlated response.
2. **Parse.** `airfry_core::parse_client_message` produces a typed message; malformed input yields a
   `NOTICE` and the connection stays usable.
3. **Verification.** Event id recomputation and BIP-340 schnorr verification run on a worker pool
   sized to the machine's cores. Verification is the dominant CPU cost of ingest, and it is
   embarrassingly parallel: each event is independent. Results are returned to the connection task
   in order.
4. **Persistence.** Verified events are inserted through `Store::insert_batch`, which is the
   lock-amortised path.
5. **Acknowledgement.** The client receives `["OK", <id>, true, ""]` (or `false` with a machine
   readable prefix such as `invalid:`, `duplicate:`, `blocked:`).
6. **Fanout.** The event is offered to every open subscription whose filters match, via the hub.

### 3.3 Subscription hub and fanout

`hub.rs` maintains a registry of connections and their subscriptions. Fanout works by:

- matching the event against registered filters (the same `Filter::matches` used by the store, so
  live and historical results are consistent);
- encoding the `["EVENT", <sub_id>, <event>]` frame once per distinct subscription id and pushing it
  into the target connection's bounded `mpsc` queue;
- marking a connection as a slow consumer when its queue is full, and closing it rather than growing
  memory without bound.

Per-connection limits (`--max-subs-per-conn`, `--max-filters`, `--max-limit`) bound the work a single
client can create.

### 3.4 Shutdown

`SIGINT`/`SIGTERM` trigger a graceful shutdown: the listener stops accepting, connection tasks are
notified through the hub, in-flight frames are drained, and the store is flushed. The process exits
with status 0.

### 3.5 Negentropy / NIP-77 set reconciliation

The relay implements the relay side of NIP-77 (`crates/airfry-relay/src/negentropy.rs`) on top of the
`airfry-negentropy` crate. A `NEG-OPEN` queries the store for the `(created_at, id)` pairs matching
the client's filter (the filter's `limit` is ignored — negentropy reconciles sets, not paginated
windows), seals them into a `NegentropySet`, and answers with a `NEG-MSG`. Subsequent `NEG-MSG`
frames drive the reconciliation state machine; when it completes the relay sends the events the
client is missing as `EVENT` frames followed by `EOSE`, matching strfry.

The crate's speed advantage over the reference C++ implementation comes from the sealed-set layout:
it precomputes a prefix-sum table of the 256-bit id accumulators so a range fingerprint is O(1) (one
256-bit subtraction plus one SHA-256), whereas the reference recomputes the accumulator by iterating
the range (or walking LMDB B-tree nodes in strfry's relay storage). The reconciler never allocates
per item, and the relay builds the sealed set once per `NEG-OPEN` and shares it through an `Arc`.

Relay limits: at most 8 concurrent negentropy subscriptions per connection, a 1 MiB maximum hex
payload, and protocol versions `0x60..=0x6f` accepted. See the README's
[Negentropy / NIP-77 section](../README.md#negentropy--nip-77-set-reconciliation) for the wire
messages and client usage.

## 4. Why this beats strfry

strfry is the reference implementation of a fast relay. It is built on LMDB, and its performance
ceiling is set by that choice. The three structural advantages below are not micro-optimisations;
they are differences in architecture.

### 4.1 Parallel schnorr verification across cores

Signature verification is the single largest CPU cost in relay ingest: a BIP-340 verification is
roughly tens of microseconds of elliptic-curve arithmetic, and every event needs one. On a 32-core
machine the theoretical ingest ceiling is therefore set by how many verifications can run
concurrently.

- **strfry** verifies events in its writer path; verification is effectively serialised with respect
  to the LMDB write transaction, so adding cores does not proportionally add ingest throughput.
- **airfry-relay** runs verification on a worker pool sized to the core count, with each event
  independent. Ingest throughput scales with cores until the append path becomes the bottleneck —
  and the append path is a single sequential write, which is far cheaper than a verification.

This is the largest single contributor to the ingest gap, and it is why the benchmark reports both
"verification enabled" and "verification disabled" numbers: the disabled number isolates the storage
engine's ceiling, the enabled number is what a real relay does.

### 4.2 In-memory index queries instead of LMDB scans

LMDB is a B-tree on disk with a page cache. A REQ that matches a tag or a time range requires cursor
traversal of the B-tree, page faults on cold data, and copy-out of values. Even in the best case
(warm cache) the traversal cost is proportional to the number of B-tree pages touched.

`airfry-store` answers the same query from purpose-built in-memory indexes:

- the index is keyed exactly the way NIP-01 filters are shaped (id, author, kind, tag, time), so a
  filter lookup is a hash lookup plus an ordered-set walk, not a range scan over a generic B-tree;
- results are ordered newest-first by construction, so no sort is needed for the common case;
- the event body is materialised only for events that survive `limit`, so a `limit: 10` query over a
  million-event corpus touches ten event bodies, not a million index entries.

The measured effect is that REQ p99 stays in the sub-millisecond range for indexed filters, versus
the millisecond-plus tail strfry shows under the same load.

### 4.3 Zero-copy fanout

Live fanout is the other hot path: a popular relay pushes every new event to every matching
subscription.

- **strfry** serialises the event for each subscriber (or at least re-walks the subscription set per
  event) in its writer path.
- **airfry-relay** shares `Arc<Event>` handles and encodes each distinct frame once, then hands the
  same `String` to every matching connection's queue. The per-subscriber cost is a queue push, not a
  serialisation.

Combined with the bounded per-connection queues, this keeps fanout O(subscribers) in cheap operations
rather than O(subscribers × serialisation cost).

### 4.4 What we do *not* claim

- We do not claim a faster cryptographic primitive: both relays use libsecp256k1-class verification.
  The advantage is parallelism, not a better algorithm.
- We do not claim better durability defaults: strfry's LMDB is a mature, crash-safe store. Our
  advantage is that we can choose the durability policy per deployment without paying for it in the
  query path.
- We do not claim the numbers are universal. They are measured on this machine, with this corpus,
  with both relays tuned. See `BENCHMARKS.md` for the exact commands and the raw output.

## 5. Correctness and adversarial behaviour

The relay is tested against hostile input by `crates/airfry-e2e`:

- **Malformed frames** — non-JSON, empty arrays, missing fields, unknown message types: answered with
  `NOTICE`/`CLOSED`, connection stays usable.
- **Oversized events** — rejected with `OK false` before parsing, using a cheap id extraction.
- **Duplicate ids** — rejected with `OK false` and never double-stored.
- **`limit: 0`** — returns no events, then `EOSE`.
- **`since > until`** — returns no events, then `EOSE` (not an error).
- **Unknown kinds** — accepted and stored; filters on them work normally.
- **Multi-value tag filters** — `{"#t": ["a", "b"]}` matches events carrying either value.
- **CLOSE mid-stream** — stops delivery immediately, including for events published after the CLOSE.
- **Concurrent REQ during heavy ingest** — queries never block writers for long and always return
  consistent results.
- **Restart/reopen durability** — events survive a process restart; torn tails are discarded.
- **NIP-09 deletion of a nonexistent id** — accepted, no-op, no error.
- **NIP-40 expired events** — never returned, never counted.
- **NIP-11 content type** — `application/nostr+json` on `GET /` with the right `Accept` header.
- **`/health`** — returns live store statistics.
- **SIGTERM shutdown** — the process exits cleanly and the store is flushed.

These are the properties that make the performance numbers meaningful: a relay that is fast because
it silently drops events is not fast.
