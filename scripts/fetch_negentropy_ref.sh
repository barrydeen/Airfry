#!/usr/bin/env bash
# =============================================================================
# scripts/fetch_negentropy_ref.sh — fetch and build the hoytech/negentropy C++
# reference implementation used as the strfry baseline for the negentropy
# benchmark (BENCHMARK.md) and for the cross-implementation conformance tests
# (crates/airfry-negentropy/tests/{strfry_interop,upstream_suite}.rs).
#
# The reference is NOT vendored into the repository (third_party/ is
# git-ignored); this script reproduces the exact checkout the published numbers
# and the interop tests were produced with.
#
# Pinned commit: fe0bb8ad4d636776e2e16c4f2369c4eae47f1b34 (hoytech/negentropy
# master at the time the benchmark was measured). This is the same C++ library
# that strfry vendors and drives via Negentropy<StorageBase>::reconcile, so the
# resulting `test/cpp/bench` binary is a faithful strfry baseline.
#
# Idempotent and safe to re-run:
#   - if third_party/negentropy-ref already exists at the pinned commit and both
#     binaries are built, it does nothing and prints their paths;
#   - if the checkout exists but is at a different commit, it checks out the
#     pinned commit and rebuilds;
#   - if the checkout is missing, it clones, initialises the hoytech-cpp/lmdbxx
#     submodules and builds.
#
# bench.cpp is not part of upstream hoytech/negentropy; the benchmark source used
# for BENCHMARK.md is kept in scripts/negentropy-ref/bench.cpp and copied into the
# checkout by this script.
#
# Requirements (Debian/Ubuntu package names in parentheses):
#   git, make, a C++ toolchain (build-essential), libssl-dev (OpenSSL headers
#   for -lcrypto).
#
# Env overrides:
#   NEGENTROPY_REPO   git URL to clone (default https://github.com/hoytech/negentropy)
#   NEGENTROPY_DIR    checkout directory (default <repo>/third_party/negentropy-ref)
#   NEGENTROPY_COMMIT commit to check out (default fe0bb8ad4d636776e2e16c4f2369c4eae47f1b34)
#   HOYTECH_CPP_COMMIT hoytech-cpp submodule revision (default 1d33d3596aceac359b83924867673349c0c18a85)
#   JOBS              parallel build jobs (default: nproc)
#   FORCE_REBUILD=1   rebuild even if the binaries already exist
#
# Exit code: 0 on success (both binaries present), non-zero with a clear message
# otherwise.
# =============================================================================

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
NEGENTROPY_REPO="${NEGENTROPY_REPO:-https://github.com/hoytech/negentropy}"
NEGENTROPY_DIR="${NEGENTROPY_DIR:-$ROOT/third_party/negentropy-ref}"
NEGENTROPY_COMMIT="${NEGENTROPY_COMMIT:-fe0bb8ad4d636776e2e16c4f2369c4eae47f1b34}"
HOYTECH_CPP_COMMIT="${HOYTECH_CPP_COMMIT:-1d33d3596aceac359b83924867673349c0c18a85}"
JOBS="${JOBS:-$(nproc 2>/dev/null || echo 4)}"
FORCE_REBUILD="${FORCE_REBUILD:-0}"
CPP_DIR="$NEGENTROPY_DIR/test/cpp"
BENCH_BIN="$CPP_DIR/bench"
HARNESS_BIN="$CPP_DIR/harness"

say() { printf '==> %s\n' "$*"; }
die() { printf 'ERROR: %s\n' "$*" >&2; exit 1; }

command -v git >/dev/null 2>&1 || die "git is required"
command -v make >/dev/null 2>&1 || die "make is required"
command -v "${CXX:-g++}" >/dev/null 2>&1 || die "a C++ compiler (g++) is required"

# ---------------------------------------------------------------------------
# 1. Ensure the checkout exists at the pinned commit.
# ---------------------------------------------------------------------------
if [[ -d "$NEGENTROPY_DIR/.git" ]]; then
    say "negentropy reference checkout already present at $NEGENTROPY_DIR"
    CURRENT="$(git -C "$NEGENTROPY_DIR" rev-parse HEAD 2>/dev/null || true)"
    if [[ "$CURRENT" != "$NEGENTROPY_COMMIT" ]]; then
        say "checkout is at ${CURRENT:-unknown}, switching to pinned commit $NEGENTROPY_COMMIT"
        git -C "$NEGENTROPY_DIR" fetch --tags origin || die "git fetch failed in $NEGENTROPY_DIR"
        git -C "$NEGENTROPY_DIR" checkout --force "$NEGENTROPY_COMMIT" \
            || die "could not check out $NEGENTROPY_COMMIT (is the commit reachable from origin?)"
    else
        say "checkout is already at pinned commit $NEGENTROPY_COMMIT"
    fi
elif [[ -d "$NEGENTROPY_DIR" ]]; then
    # A plain directory (e.g. a tarball extract) is acceptable as long as the
    # sources we need are there; we cannot verify the commit without .git.
    say "$NEGENTROPY_DIR exists but is not a git checkout; using it as-is"
    [[ -f "$CPP_DIR/bench.cpp" ]] || die "$CPP_DIR/bench.cpp missing; remove $NEGENTROPY_DIR and re-run"
else
    say "cloning $NEGENTROPY_REPO into $NEGENTROPY_DIR"
    mkdir -p "$(dirname "$NEGENTROPY_DIR")"
    git clone "$NEGENTROPY_REPO" "$NEGENTROPY_DIR" || die "git clone failed"
    say "checking out pinned commit $NEGENTROPY_COMMIT"
    git -C "$NEGENTROPY_DIR" checkout --force "$NEGENTROPY_COMMIT" \
        || die "could not check out $NEGENTROPY_COMMIT"
fi

# The upstream repo carries hoytech-cpp and lmdbxx as git submodules; the C++
# sources include headers from them, so they must be initialised.
if [[ -d "$NEGENTROPY_DIR/.git" ]]; then
    say "initialising git submodules (hoytech-cpp, lmdbxx)"
    git -C "$NEGENTROPY_DIR" submodule update --init --recursive \
        || die "git submodule update failed in $NEGENTROPY_DIR"
    # The hoytech-cpp revision recorded in the pinned negentropy commit does not
    # compile with modern libstdc++ (missing <cstdint> in hex.h). Pin the
    # submodule to the revision the published benchmark was built with.
    HOYTECH_CPP_DIR="$CPP_DIR/hoytech-cpp"
    if [[ -d "$HOYTECH_CPP_DIR/.git" || -f "$HOYTECH_CPP_DIR/.git" ]]; then
        CURRENT_HC="$(git -C "$HOYTECH_CPP_DIR" rev-parse HEAD 2>/dev/null || true)"
        if [[ "$CURRENT_HC" != "$HOYTECH_CPP_COMMIT" ]]; then
            say "pinning hoytech-cpp submodule to $HOYTECH_CPP_COMMIT (buildable revision)"
            git -C "$HOYTECH_CPP_DIR" fetch --tags origin \
                || die "git fetch failed in $HOYTECH_CPP_DIR"
            git -C "$HOYTECH_CPP_DIR" checkout --force "$HOYTECH_CPP_COMMIT" \
                || die "could not check out hoytech-cpp $HOYTECH_CPP_COMMIT"
        fi
    fi
fi

[[ -f "$CPP_DIR/harness.cpp" ]] || die "$CPP_DIR/harness.cpp missing after checkout"
[[ -f "$CPP_DIR/hoytech-cpp/hoytech/error.h" ]] \
    || die "$CPP_DIR/hoytech-cpp submodule missing; run git submodule update --init --recursive in $NEGENTROPY_DIR"

# bench.cpp is a local addition (the upstream test/cpp/Makefile has no `bench`
# target). It is kept in scripts/negentropy-ref/bench.cpp so a fresh clone can
# reproduce the published benchmark numbers; copy it into the checkout.
BENCH_SRC="$ROOT/scripts/negentropy-ref/bench.cpp"
if [[ -f "$BENCH_SRC" ]]; then
    say "installing benchmark source from $BENCH_SRC"
    cp "$BENCH_SRC" "$CPP_DIR/bench.cpp"
fi
[[ -f "$CPP_DIR/bench.cpp" ]] || die "$CPP_DIR/bench.cpp missing (expected $BENCH_SRC)"

# ---------------------------------------------------------------------------
# 2. Build the benchmark and conformance harness binaries.
#
#    The upstream test/cpp/Makefile has no `bench` target (bench.cpp is a
#    local addition used by BENCHMARK.md), so we compile it directly with the
#    same flags documented at the top of bench.cpp. `harness` is built through
#    the upstream Makefile.
# ---------------------------------------------------------------------------
if [[ -x "$BENCH_BIN" && -x "$HARNESS_BIN" && "$FORCE_REBUILD" != "1" ]]; then
    say "reference binaries already built; skipping rebuild (set FORCE_REBUILD=1 to force)"
else
    say "building reference harness with make -j$JOBS"
    ( cd "$CPP_DIR" && make -j"$JOBS" harness ) \
        || die "reference harness build failed (check the dependencies listed at the top of this script)"

    say "building reference benchmark (bench.cpp)"
    ( cd "$CPP_DIR" && "${CXX:-g++}" -std=c++20 -O3 -Wall \
        -I../../cpp -I./hoytech-cpp -I../../cpp/vendor/lmdbxx/include \
        bench.cpp -lcrypto -o bench ) \
        || die "reference benchmark build failed (is libssl-dev installed?)"
fi

[[ -x "$BENCH_BIN" ]] || die "build finished but $BENCH_BIN is missing or not executable"
[[ -x "$HARNESS_BIN" ]] || die "build finished but $HARNESS_BIN is missing or not executable"

# ---------------------------------------------------------------------------
# 3. Report.
# ---------------------------------------------------------------------------
say "negentropy reference is ready"
printf 'bench binary:   %s\n' "$BENCH_BIN"
printf 'harness binary: %s\n' "$HARNESS_BIN"
if [[ -d "$NEGENTROPY_DIR/.git" ]]; then
    printf 'reference commit: %s\n' "$(git -C "$NEGENTROPY_DIR" rev-parse HEAD)"
else
    printf 'reference commit: %s (unverified: not a git checkout)\n' "$NEGENTROPY_COMMIT"
fi
printf '\nNext: scripts/bench-negentropy.sh  (head-to-head vs the strfry reference)\n'
