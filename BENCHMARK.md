# Negentropy Benchmark: airfry vs strfry

This document reports a head-to-head benchmark of **airfry-negentropy** (the Rust NIP-77
negentropy implementation used by the airfry relay) against the **strfry reference C++
implementation**.

## Baseline provenance

The baseline is the *exact* C++ negentropy library that strfry vendors and calls
(`hoytech/negentropy`, `Negentropy<StorageBase>::reconcile`). It is fetched into the git-ignored
`third_party/negentropy-ref/` by `scripts/fetch_negentropy_ref.sh` (pinned commit
`fe0bb8ad4d636776e2e16c4f2369c4eae47f1b34`) and built as
`third_party/negentropy-ref/test/cpp/bench.cpp`; the benchmark source itself is kept in
`scripts/negentropy-ref/bench.cpp` and copied into the checkout by that script.
This is a **faithful strfry baseline**, not a proxy: strfry's `dbutils negentropy` command
drives the same `Negentropy<StorageBase>::reconcile` code path over its LMDB-backed storage.
The benchmark harness (`bench.cpp`) uses the in-memory `Vector` storage backend for both
implementations so the comparison isolates the reconciliation algorithm rather than disk I/O.

> Note: the strfry binary in `third_party/strfry/strfry` in this repo is a build that exposes
> `dbutils negentropy` (not the standalone `negentropy-bench` subcommand found in newer strfry
> releases). The vendored C++ library benchmark is the canonical way to measure strfry's
> reconciliation performance and is what we use here.

## Methodology

- **Workload**: a full initiator↔responder reconciliation over in-memory sets of `n` items
  (32-byte ids, `created_at` timestamps). The client set is built from seed offset 0; the
  server set shares `overlap`% of its items with the client and draws the rest from a disjoint
  seed range.
- **Scenarios**: `n ∈ {1k, 10k, 100k}` × `overlap ∈ {0%, 50%, 99%}`.
- **Metric**: best-of-20 wall-clock time for one complete reconciliation (all rounds), in
  milliseconds. `bytes` is the total wire bytes exchanged across all rounds.
- **Isolation**: each scenario is executed in a **fresh process**. Running multiple large
  scenarios in one process lets glibc allocator fragmentation from a prior 100k-item set
  inflate the next scenario's timing by up to ~10x (observed: 100k/99% measured 4.8ms in a
  shared process vs 0.40ms in a fresh one). The `negentropy-bench` binary's matrix mode
  re-execs itself per scenario for exactly this reason.
- **Hardware/OS**: Linux x86_64, release builds (`-O3` for C++, `--release` for Rust).

## Results

Measured with `scripts/bench-negentropy.sh 20` on the reference machine. Lower is better.

| impl        | n      | overlap | best_ms | rounds | bytes   |
|-------------|--------|---------|---------|--------|---------|
| airfry      | 1000   | 0%      | 0.0480  | 3      | 33723   |
| strfry-ref  | 1000   | 0%      | 0.1510  | 3      | 33722   |
| airfry      | 1000   | 50%     | 0.0332  | 3      | 18339   |
| strfry-ref  | 1000   | 50%     | 0.1107  | 3      | 18338   |
| airfry      | 1000   | 99%     | 0.0067  | 2      | 1569    |
| strfry-ref  | 1000   | 99%     | 0.0186  | 2      | 1568    |
| airfry      | 10000  | 0%      | 0.4626  | 4      | 322582  |
| strfry-ref  | 10000  | 0%      | 1.4965  | 4      | 322581  |
| **airfry**  | **10000** | **50%** | **0.1956** | **3** | **162041** |
| **strfry-ref** | **10000** | **50%** | **0.7770** | **3** | **162040** |
| airfry      | 10000  | 99%     | 0.0311  | 2      | 4263    |
| strfry-ref  | 10000  | 99%     | 0.0751  | 2      | 4262    |
| airfry      | 100000 | 0%      | 6.4167  | 4      | 3203378 |
| strfry-ref  | 100000 | 0%      | 15.7507 | 4      | 3203377 |
| airfry      | 100000 | 50%     | 2.3058  | 4      | 1603352 |
| strfry-ref  | 100000 | 50%     | 6.3383  | 4      | 1603351 |
| airfry      | 100000 | 99%     | 0.3966  | 3      | 34564   |
| strfry-ref  | 100000 | 99%     | 0.5513  | 3      | 34563   |

### Speedup (strfry-ref time / airfry time)

| n      | overlap | speedup |
|--------|---------|---------|
| 1000   | 0%      | 3.15x   |
| 1000   | 50%     | 3.33x   |
| 1000   | 99%     | 2.78x   |
| 10000  | 0%      | 3.24x   |
| **10000** | **50%** | **3.97x** |
| 10000  | 99%     | 2.42x   |
| 100000 | 0%      | 2.45x   |
| 100000 | 50%     | 2.75x   |
| 100000 | 99%     | 1.39x   |

## Verdict

**airfry-negentropy is faster than the strfry reference implementation in every benchmarked
scenario**, including the headline case (10k items, 50% overlap), where airfry completes the
reconciliation in **0.196 ms vs strfry's 0.777 ms — a 3.97x speedup**.

Wire traffic is identical between the two implementations (within 1 byte, which is the
protocol-version framing byte), confirming that the speedup comes from the implementation, not
from sending fewer bytes. The airfry implementation achieves this via:

- O(1) range fingerprints using a precomputed prefix-accumulator table (`NegentropySet::prefix`)
  instead of recomputing hashes over the range.
- Binary-search range bounds (`find_lower_bound`) over the sorted item vector.
- Preallocated output buffers and batched id-list encoding.

## Reproducing

### Fresh clone: fetch the reference first

`third_party/` is **git-ignored**, so a fresh clone of this repository contains neither strfry
nor the hoytech/negentropy C++ reference. Before running the benchmark you must fetch and build
the reference:

```bash
# From the repository root, on a fresh clone:
scripts/fetch_negentropy_ref.sh   # clones hoytech/negentropy at the pinned commit and builds
                                  # test/cpp/bench + test/cpp/harness
```

`scripts/fetch_negentropy_ref.sh` is idempotent: it does nothing if the checkout is already at
the pinned commit with both binaries built, and it prints the binary paths on success. It needs
`git`, `make`, a C++ toolchain and OpenSSL headers (`libssl-dev`).

Only after that can the head-to-head benchmark run:

```bash
scripts/bench-negentropy.sh 20
```

This builds `airfry-negentropy`'s `negentropy-bench` binary in release mode, builds the strfry
reference `bench` binary if needed, and runs every scenario in a fresh process, printing
machine-parseable lines of the form:

```
impl=<impl> n=<items> overlap=<pct> best_ms=<ms> rounds=<r> bytes=<b>
```

To run a single scenario directly:

```bash
# airfry
cargo build --release -p airfry-negentropy --bin negentropy-bench
./target/release/negentropy-bench 10000 50 20

# strfry reference (requires scripts/fetch_negentropy_ref.sh first)
make -C third_party/negentropy-ref/test/cpp bench
./third_party/negentropy-ref/test/cpp/bench 10000 50 20
```

### Conformance tests without the reference

The cross-implementation conformance tests in `crates/airfry-negentropy/tests/`
(`strfry_interop.rs`, `upstream_suite.rs`) drive the reference C++ harness at
`third_party/negentropy-ref/test/cpp/harness`. On a fresh clone without the reference they
**SKIP** — they print a `SKIP: reference harness not built ...` notice and return successfully —
rather than failing. Run `scripts/fetch_negentropy_ref.sh` first if you want them to actually
execute:

```bash
scripts/fetch_negentropy_ref.sh
cargo test -p airfry-negentropy
```

The pure-Rust unit tests and the `negentropy-bench` binary do not need the reference and always
run.
