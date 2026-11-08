#!/usr/bin/env bash
# Reproducible negentropy benchmark: airfry-negentropy vs the strfry reference C++ implementation.
#
# The strfry reference binary (third_party/negentropy-ref/test/cpp/bench) is built from the
# exact hoytech/negentropy C++ library that strfry vendors and calls via
# Negentropy<StorageBase>::reconcile. It is therefore a faithful strfry baseline.
#
# The reference is NOT vendored (third_party/ is gitignored). When it is missing this script
# runs scripts/fetch_negentropy_ref.sh once to clone the pinned hoytech/negentropy commit and
# build test/cpp/{bench,harness}.
#
# Usage: scripts/bench-negentropy.sh [rounds]
#   rounds: number of repetitions per scenario (best-of-N is reported). Default 20.
#
# Each scenario is run in a fresh process to avoid allocator-state contamination between
# scenarios (see BENCHMARK.md for why this matters).

set -euo pipefail

ROUNDS="${1:-20}"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

export PATH="$HOME/.cargo/bin:$PATH"

AIRFRY_BIN="$ROOT/target/release/negentropy-bench"
STRFRY_BIN="$ROOT/third_party/negentropy-ref/test/cpp/bench"

echo "# Building airfry-negentropy benchmark (release)..."
cargo build --release -p airfry-negentropy --bin negentropy-bench >/dev/null 2>&1

if [[ ! -x "$STRFRY_BIN" ]]; then
  echo "# strfry reference benchmark not built — running scripts/fetch_negentropy_ref.sh"
  bash "$ROOT/scripts/fetch_negentropy_ref.sh" || {
    echo "ERROR: could not fetch/build the strfry reference benchmark" >&2
    exit 1
  }
fi

[[ -x "$STRFRY_BIN" ]] || {
  echo "ERROR: strfry reference benchmark missing at $STRFRY_BIN" >&2
  exit 1
}

echo "# rounds=$ROUNDS"
echo "# format: impl=<impl> n=<items> overlap=<pct> best_ms=<ms> rounds=<r> bytes=<b>"
echo

for n in 1000 10000 100000; do
  for o in 0 50 99; do
    "$AIRFRY_BIN" "$n" "$o" "$ROUNDS"
    "$STRFRY_BIN" "$n" "$o" "$ROUNDS"
  done
done
