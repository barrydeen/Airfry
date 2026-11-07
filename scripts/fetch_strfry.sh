#!/usr/bin/env bash
# =============================================================================
# scripts/fetch_strfry.sh — fetch and build strfry at the pinned commit used for
# the head-to-head benchmark in BENCHMARKS.md.
#
# strfry is the reference relay we compare against. It is NOT vendored into the
# repository (third_party/ is git-ignored); this script reproduces the exact
# build the published numbers were measured with.
#
# Pinned commit: 4cd3cf64850caf47dda46c2a2abbbf3525a64d10 (strfry v1-4cd3cf6)
#
# Idempotent and safe to re-run:
#   - if third_party/strfry already exists at the pinned commit and the binary
#     is built, it does nothing and prints the binary path;
#   - if the checkout exists but is at a different commit, it checks out the
#     pinned commit and rebuilds;
#   - if the checkout is missing, it clones and builds.
#
# Requirements (Debian/Ubuntu package names in parentheses):
#   git, make, a C++ toolchain (build-essential), cmake, libmicrohttpd-dev,
#   libflatbuffers-dev / flatbuffers-compiler, libsecp256k1-dev, liblmdb-dev,
#   libzstd-dev, libssl-dev, pkg-config, zlib1g-dev.
#
# Env overrides:
#   STRFRY_REPO   git URL to clone (default https://github.com/hoytech/strfry)
#   STRFRY_DIR    checkout directory (default <repo>/third_party/strfry)
#   STRFRY_COMMIT commit to check out (default 4cd3cf64850caf47dda46c2a2abbbf3525a64d10)
#   JOBS          parallel build jobs (default: nproc)
#   FORCE_REBUILD=1  rebuild even if the binary already exists
#
# Exit code: 0 on success (binary present), non-zero with a clear message otherwise.
# =============================================================================

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
STRFRY_REPO="${STRFRY_REPO:-https://github.com/hoytech/strfry}"
STRFRY_DIR="${STRFRY_DIR:-$ROOT/third_party/strfry}"
STRFRY_COMMIT="${STRFRY_COMMIT:-4cd3cf64850caf47dda46c2a2abbbf3525a64d10}"
JOBS="${JOBS:-$(nproc 2>/dev/null || echo 4)}"
FORCE_REBUILD="${FORCE_REBUILD:-0}"
STRFRY_BIN="$STRFRY_DIR/strfry"

say() { printf '==> %s\n' "$*"; }
die() { printf 'ERROR: %s\n' "$*" >&2; exit 1; }

command -v git >/dev/null 2>&1 || die "git is required"
command -v make >/dev/null 2>&1 || die "make is required"

# ---------------------------------------------------------------------------
# 1. Ensure the checkout exists at the pinned commit.
# ---------------------------------------------------------------------------
if [[ -d "$STRFRY_DIR/.git" ]]; then
    say "strfry checkout already present at $STRFRY_DIR"
    CURRENT="$(git -C "$STRFRY_DIR" rev-parse HEAD 2>/dev/null || true)"
    if [[ "$CURRENT" != "$STRFRY_COMMIT" ]]; then
        say "checkout is at ${CURRENT:-unknown}, switching to pinned commit $STRFRY_COMMIT"
        git -C "$STRFRY_DIR" fetch --tags origin || die "git fetch failed in $STRFRY_DIR"
        git -C "$STRFRY_DIR" checkout --force "$STRFRY_COMMIT" \
            || die "could not check out $STRFRY_COMMIT (is the commit reachable from origin?)"
    else
        say "checkout is already at pinned commit $STRFRY_COMMIT"
    fi
elif [[ -d "$STRFRY_DIR" ]]; then
    die "$STRFRY_DIR exists but is not a git checkout; remove it and re-run"
else
    say "cloning $STRFRY_REPO into $STRFRY_DIR"
    mkdir -p "$(dirname "$STRFRY_DIR")"
    git clone "$STRFRY_REPO" "$STRFRY_DIR" || die "git clone failed"
    say "checking out pinned commit $STRFRY_COMMIT"
    git -C "$STRFRY_DIR" checkout --force "$STRFRY_COMMIT" \
        || die "could not check out $STRFRY_COMMIT"
fi

# ---------------------------------------------------------------------------
# 2. Build (unless already built and not forced).
# ---------------------------------------------------------------------------
if [[ -x "$STRFRY_BIN" && "$FORCE_REBUILD" != "1" ]]; then
    say "strfry binary already built; skipping rebuild (set FORCE_REBUILD=1 to force)"
else
    say "building strfry with make -j$JOBS (this takes a few minutes)"
    ( cd "$STRFRY_DIR" && make -j"$JOBS" ) || die "strfry build failed (check the dependencies listed at the top of this script)"
fi

[[ -x "$STRFRY_BIN" ]] || die "build finished but $STRFRY_BIN is missing or not executable"

# ---------------------------------------------------------------------------
# 3. Report.
# ---------------------------------------------------------------------------
say "strfry is ready"
printf 'strfry binary: %s\n' "$STRFRY_BIN"
printf 'strfry commit: %s\n' "$(git -C "$STRFRY_DIR" rev-parse HEAD)"
printf 'benchmark config: %s\n' "$ROOT/third_party/strfry/strfry-bench.conf"
printf '\nNext: scripts/compare_strfry.sh  (head-to-head vs our relay)\n'
