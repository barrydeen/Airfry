# Contributing to Airfry

Thanks for your interest. Airfry is a Rust Nostr relay backed by a custom storage engine; the
project's goal is to be measurably faster than strfry on ingest throughput and REQ latency, so
performance regressions are treated as bugs.

By participating you agree to the [Code of Conduct](CODE_OF_CONDUCT.md).

## Getting started

```sh
export PATH=$HOME/.cargo/bin:$PATH
git clone https://github.com/barrydeen/Airfry.git
cd Airfry
cargo build --workspace
```

Requirements:

- Rust stable (1.80+). `rustup` is the supported way to install it.
- Linux or macOS. The relay uses tokio and standard sockets; Windows is untested.
- For the head-to-head benchmark only: a C++ toolchain, `cmake`, `libmicrohttpd`, `flatbuffers`,
  `libsecp256k1`, `lmdb` and `zstd` (see [Fetching strfry](#fetching-strfry-for-the-head-to-head)).

## Build, test, benchmark

```sh
export PATH=$HOME/.cargo/bin:$PATH

# debug build of every crate
cargo build --workspace

# release build (this is what benchmarks use)
cargo build --release --workspace

# full test suite (unit + integration + cross-crate conformance + adversarial)
cargo test --workspace

# the independent cross-crate conformance/adversarial suite only
cargo test -p nostr-e2e

# acceptance gate: builds, tests, boots the relay, runs smoke/load/req/store, and
# runs the head-to-head comparison against strfry. Exits 0 only if everything passes.
scripts/verify.sh

# head-to-head benchmark only (our relay vs strfry, identical corpus and concurrency)
scripts/compare_strfry.sh
```

`scripts/verify.sh` appends its measured numbers to `BENCHMARKS.md`. If you do not have strfry
built locally, run it with `SKIP_STRFRY=1` (CI does this) — the rest of the gate still runs.

Useful environment overrides for the benchmark scripts are documented at the top of
`scripts/verify.sh` and `scripts/compare_strfry.sh` (`LOAD_EVENTS`, `LOAD_CONNS`, `REQ_QUERIES`,
`REQ_CONNS`, `REQ_FILTER`, `MIN_INGEST_EPS`, `MAX_REQ_P99_MS`, `MIN_STORE_EPS`, `SKIP_BUILD`,
`SKIP_TESTS`, `SKIP_STRFRY`, `STRFRY_BIN`, `STRFRY_CONF`).

## Fetching strfry for the head-to-head

The comparison uses strfry pinned at commit `4cd3cf64850caf47dda46c2a2abbbf3525a64d10` (v1-4cd3cf6).
Fetch and build it with:

```sh
scripts/fetch_strfry.sh
```

The script is idempotent: it clones `https://github.com/hoytech/strfry` into `third_party/strfry`,
checks out the pinned commit, builds it with `make -j$(nproc)`, and prints the binary path
(`third_party/strfry/strfry`). Re-running it is safe; it will not re-clone or rebuild if the pinned
commit is already built. `third_party/` is git-ignored.

The tuned benchmark configuration used for strfry lives at `third_party/strfry/strfry-bench.conf`
and is committed to the repository (it is not part of the strfry checkout).

## Code style

- Run `cargo fmt --all` before submitting; CI checks formatting.
- Keep `cargo clippy --workspace --all-targets` free of *new* warnings. CI treats clippy warnings as
  non-blocking, but reviewers will ask you to fix them.
- No `todo!()`, `unimplemented!()`, or dead code left behind in a PR.
- Prefer explicit error handling over `unwrap()`/`expect()` on paths reachable from network input.
- Hot paths (event ingest, filter matching, fanout) must not allocate per event where avoidable and
  must not serialise JSON per subscriber. If you touch them, run `scripts/compare_strfry.sh` before
  and after and include the numbers in the PR.
- Document public APIs with `///` doc comments; the crates are meant to be readable.

## Pull requests

- One logical change per PR. Keep diffs reviewable.
- Describe *what* changed and *why*, and include benchmark numbers if the change touches the hot
  path or the storage engine.
- Add or update tests. Cross-crate behaviour belongs in `crates/nostr-e2e/tests/`; unit tests belong
  next to the code they test.
- CI must be green: `cargo fmt --check`, `cargo build --workspace --release`, `cargo test
  --workspace`.
- Do not commit `target/`, `third_party/`, `nostr-data/`, or log files (see `.gitignore`).
- Do not commit absolute paths from your machine into docs or scripts.

## Reporting bugs

Open an issue with:

- the relay version (`target/release/nostr-relay --version`) and the exact flags you ran,
- the platform and Rust version,
- the client message(s) that triggered the problem (redact keys if needed),
- relevant stderr output with `RUST_LOG=debug`.

Security issues should go through [SECURITY.md](SECURITY.md), not the public issue tracker.
