# airfry-e2e

End-to-end and adversarial conformance tests for the [Airfry](https://github.com/barrydeen/airfry)
relay and storage engine. This crate is `publish = false` and intentionally contains no library
code: it exists so that integration tests can exercise `airfry-core`, `airfry-store` and the
`airfry-relay` binary together without creating dependency cycles between the implementation
crates. `src/lib.rs` only re-exports `airfry_core` and `airfry_store` for convenience in tests.

## What it covers

- `tests/relay_conformance.rs` — black-box NIP-01 protocol conformance: spawns the real
  `airfry-relay` binary and drives it over WebSockets exactly like a Nostr client would
  (publish, `REQ`/`EVENT`/`EOSE`, `CLOSE`, `COUNT`, NIP-09 deletes, NIP-11 metadata and health,
  NIP-40 expiry, NIP-42 `AUTH`, per-connection limits, graceful shutdown and restart durability).
- `tests/adversarial_relay.rs` — hostile-client tests against the real relay over the wire:
  malformed frames, oversized events, duplicate ids, binary/empty frames, `limit: 0` and inverted
  time ranges, concurrent `REQ` during heavy ingest, rapid connect/disconnect, and clean
  WebSocket close handshakes. These assert the relay stays correct and usable under abuse — the
  property that makes the benchmark numbers meaningful.
- `tests/store_conformance.rs` — independent conformance suite for the documented `airfry-store`
  API (`Store`, `StoreConfig`, `FsyncPolicy`, `InsertOutcome`), written against
  `ARCHITECTURE.md` and deliberately avoiding storage internals so it stays valid as the engine
  evolves.
- `tests/adversarial_store.rs` — hostile and degenerate inputs against the storage engine,
  asserting it stays correct, bounded and crash-free.
- `tests/common/mod.rs` — shared helpers: spawning the relay binary on an ephemeral port,
  WebSocket/HTTP clients, and a `RelayProc` guard that kills the process and removes its data
  directory on drop.

## Running

```sh
# The whole suite (spawns real relay processes; needs a built workspace).
cargo test -p airfry-e2e

# A single file.
cargo test -p airfry-e2e --test relay_conformance
cargo test -p airfry-e2e --test adversarial_relay
```

The relay tests build and spawn `target/<profile>/airfry-relay`, so run `cargo build --workspace`
first (or let `cargo test` build it). Each test uses its own temporary data directory and an
ephemeral port, so tests can run in parallel.

## See also

- Repository and project overview: <https://github.com/barrydeen/Airfry>
- `airfry-core` (Nostr primitives), `airfry-store` (storage engine), `airfry-relay` (server),
  `airfry-negentropy` (NIP-77 set reconciliation).
- Architecture: [ARCHITECTURE.md](https://github.com/barrydeen/Airfry/blob/main/ARCHITECTURE.md)

## License

MIT.
