# airfry-negentropy

Negentropy protocol v1 ([NIP-77](https://github.com/nostr-protocol/nips/blob/master/77.md))
set reconciliation for the [Airfry](https://github.com/barrydeen/airfry) relay. This crate
implements the wire format and the reconciliation algorithm specified in
`third_party/negentropy-ref/docs/negentropy-protocol-v1.md` (the hoytech/negentropy reference),
with a storage layout designed for speed. It is the engine behind the relay's `NEG-OPEN`,
`NEG-MSG` and `NEG-CLOSE` handlers.

## What it provides

- `NegentropySet` — seal a sorted `(timestamp, id)` set and precompute a prefix-sum table of the
  256-bit little-endian id accumulators. A range fingerprint is therefore **O(1)**: one 256-bit
  subtraction plus one SHA-256 over 32 bytes and a varint. The reference C++ implementation
  recomputes the accumulator by iterating every item in the range (Vector storage) or by walking
  B-tree nodes with LMDB page reads (strfry's relay storage).
- `Negentropy` — the protocol participant. The initiator calls `initiate()` and then
  `reconcile()` with each response; the responder only calls `reconcile()` with the initiator's
  messages. The reconciler never allocates per item: it walks the sealed set by index and writes
  directly into the output buffer.
- `Item`, `Bound`, `Mode`, `Fingerprint` — the protocol data types.
- `encode_varint` / `decode_varint`, `encode_bound` / `decode_bound`, `minimal_bound` — the
  wire-format primitives, exposed for testing and for callers that need to inspect frames.

The public API is frozen: `airfry-relay` codes against it.

## Public API

```rust
use std::sync::Arc;
use airfry_negentropy::{Item, Negentropy, NegentropySet, ID_SIZE};

// 1. Seal the local set (sorted by (timestamp, id); duplicate items are rejected).
let items: Vec<Item> = /* Item::new(timestamp, id) for each event you hold */;
let set = Arc::new(NegentropySet::new(items)?);

// 2. Create a participant. `frame_size_limit` of 0 means unlimited; a non-zero limit
//    must be at least 4096.
let mut ne = Negentropy::new(set, 0)?;

// 3. Initiator: produce the first message.
let msg: Vec<u8> = ne.initiate()?;

// 4. Feed peer messages back in. On the initiator side, `have`/`need` are filled in
//    once the protocol completes; a reply of length 1 (just the version byte) means done.
let mut have: Vec<[u8; ID_SIZE]> = Vec::new();
let mut need: Vec<[u8; ID_SIZE]> = Vec::new();
let reply: Vec<u8> = ne.reconcile(&peer_msg, &mut have, &mut need)?;
```

Signatures, exactly as implemented in `src/lib.rs`:

- `NegentropySet::new(items: Vec<Item>) -> Result<NegentropySet>` — sorts the set and builds the
  prefix-accumulator table; returns `Error::DuplicateItem` if two identical `(timestamp, id)`
  items are present.
- `Negentropy::new(set: Arc<NegentropySet>, frame_size_limit: u64) -> Result<Negentropy>` —
  returns `Error::FrameSizeLimitTooSmall` for a non-zero limit below 4096.
- `Negentropy::initiate(&mut self) -> Result<Vec<u8>>` — returns `Error::AlreadyInitiated` if
  called twice.
- `Negentropy::reconcile(&mut self, query: &[u8], have: &mut Vec<[u8; ID_SIZE]>, need: &mut Vec<[u8; ID_SIZE]>) -> Result<Vec<u8>>`.
- `Negentropy::is_initiator()`, `Negentropy::set()`.

Errors are reported through `airfry_negentropy::Error` (`PrematureEnd`, `BadVarint`,
`BadProtocolVersion`, `BadBoundPrefix`, `BadMode`, `DuplicateItem`, `BadIdSize`,
`FrameSizeLimitTooSmall`, `AlreadyInitiated`).

## Protocol v1 wire format

The crate speaks negentropy protocol v1, byte-for-byte compatible with the hoytech/negentropy
reference and therefore with strfry:

- **Version byte**: `0x61` (`PROTOCOL_VERSION`). The relay accepts the range `0x60..=0x6f` and
  rejects anything else.
- **Message types**: `0` = `Skip`, `1` = `Fingerprint`, `2` = `IdList` (`Mode`).
- **Varints**: base-128, most-significant digit first, high bit set on all but the last byte.
- **Ids**: 32-byte truncated SHA-256 event ids (`ID_SIZE = 32`).
- **Fingerprints**: 16 bytes (`FINGERPRINT_SIZE = 16`) — the first 16 bytes of
  `SHA-256(accumulator || varint(count))`, where the accumulator is the 256-bit modular sum of
  the ids in the range.
- **Bounds**: timestamp delta plus a (possibly truncated) id prefix, encoded exactly as the
  reference does, including the `MAX_U64` "infinity" bound and the wrapping timestamp-delta
  arithmetic.

The reconciler mirrors the reference implementation's `reconcileAux` operation order exactly
(including consuming an `IdList`'s ids on the responder side and emitting a pending `Skip`
before a range split), because any deviation changes the bytes on the wire and breaks interop.

## Compatibility

- **NIP-77**: <https://github.com/nostr-protocol/nips/blob/master/77.md>
- **hoytech/negentropy** (reference implementation and protocol spec):
  <https://github.com/hoytech/negentropy>
- **strfry**: <https://github.com/hoytech/strfry> — the relay's `NEG-*` handlers use this crate
  as the responder, and the interop tests drive strfry's vendored C++ harness against ours.

## Binaries

### `negentropy-harness`

A stdio line-based conformance harness that mirrors
`third_party/strfry/external/negentropy/test/cpp/harness.cpp`, so the upstream Perl test-suite
(`test.pl`, `fuzz.pl`, `protoversion.pl`) can drive this crate. Commands (one per line,
comma-separated):

```
item,<created_at>,<id_hex>   insert an item into the local set
seal                         seal the set and construct the reconciler
initiate                     emit the initial message (initiator role)
msg[,<hex>]                  process a peer message and emit our reply / have / need
```

`FRAMESIZELIMIT` in the environment sets the frame size limit (0 = unlimited).

```sh
cargo run --release -p airfry-negentropy --bin negentropy-harness
```

### `negentropy-bench`

End-to-end reconciliation timing for this crate. The output format is intentionally identical to
the strfry reference benchmark (`third_party/negentropy-ref/test/cpp/bench.cpp`) so the two can
be compared directly:

```
impl=airfry n=<n> overlap=<pct> best_ms=<ms> rounds=<r> bytes=<b>
```

Usage: `negentropy-bench <n_items> <overlap_pct> <rounds>`, where `overlap_pct` is `0` for
disjoint sets, `50` for half shared, `99` for 99% shared. With no arguments it runs the full
matrix (1k/10k/100k × 0/50/99%), executing each scenario in a fresh child process so allocator
state from a previous large scenario cannot pollute the next one.

```sh
cargo run --release -p airfry-negentropy --bin negentropy-bench -- 10000 50 20
```

Head-to-head numbers against the strfry reference are in
[BENCHMARK.md](https://github.com/barrydeen/airfry/blob/main/BENCHMARK.md); the reproducible
driver is `scripts/bench-negentropy.sh`.

## Tests

```sh
# Unit/debug tests and the strfry interop suite.
cargo test -p airfry-negentropy

# The upstream Perl conformance suite (fuzz.pl + protoversion.pl) runs as part of the above
# when third_party/negentropy-ref is present.
```

- `tests/dbg.rs` — small deterministic reconciliation scenarios.
- `tests/strfry_interop.rs` — byte-for-byte interop against the upstream C++ reference harness
  (`third_party/negentropy-ref/test/cpp/harness`): identical `initiate` output and full
  cross-implementation reconciliation in both directions.
- `tests/upstream_suite.rs` — runs the *upstream* Perl suites (`fuzz.pl`, `protoversion.pl`)
  against our harness, including frame-size limits, clustered timestamps, duplicate handling and
  protocol-version fallback.
- `tests/fuzz.py` — a Python port of the upstream `fuzz.pl` interop driver, usable standalone:
  `python3 tests/fuzz.py <harness1> <harness2> [--recs N] [--p1 P] ...`.

**Fresh clone:** `third_party/` is git-ignored, so a fresh clone has no C++ reference. The
interop and upstream conformance tests **SKIP** (print a notice and return) rather than fail when
`third_party/negentropy-ref` is absent. To run them, fetch and build the reference first:

```sh
scripts/fetch_negentropy_ref.sh   # fetch + build the hoytech/negentropy C++ reference
cargo test -p airfry-negentropy
```

## See also

- Repository and project overview: <https://github.com/barrydeen/Airfry>
- Relay integration (NIP-77 handlers, limits): [`airfry-relay`](../airfry-relay/README.md) and
  the "Negentropy / NIP-77" section of the top-level README.
- Benchmarks: [BENCHMARK.md](https://github.com/barrydeen/Airfry/blob/main/BENCHMARK.md)

## License

MIT.
