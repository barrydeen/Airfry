#!/usr/bin/env bash
# Mission acceptance gate for the nostr relay.
#
# Builds the workspace in release mode, runs the test suite, boots the relay on an ephemeral
# port with a throwaway data directory, and runs the nostr-bench smoke/load/req/store
# benchmarks. Asserts throughput/latency thresholds, verifies NIP-11 and restart persistence,
# appends the measured numbers to BENCHMARKS.md and exits 0 only if everything passes.
#
# Thresholds are overridable via environment variables:
#   MIN_INGEST_EPS   minimum publish throughput (events/s) for `nostr-bench load`   (default 50000)
#   MAX_REQ_P99_MS   maximum REQ time-to-EOSE p99 in milliseconds                   (default 250)
#   MIN_STORE_EPS    minimum direct-store ingest throughput (events/s)              (default 100000)
#   LOAD_EVENTS      number of events for the load benchmark                        (default 200000)
#   LOAD_CONNS       websocket connections for the load benchmark                   (default 16)
#   REQ_QUERIES      number of REQ queries                                          (default 2000)
#   REQ_CONNS        websocket connections for the REQ benchmark                    (default 8)
#   STORE_EVENTS     number of events for the direct-store benchmark                (default 200000)
#   RELAY_BIN        relay binary path                                              (default target/release/nostr-relay)
#   BENCH_BIN        bench binary path                                              (default target/release/nostr-bench)
#   SKIP_BUILD=1     skip cargo build/test (reuse existing binaries)
#   SKIP_TESTS=1     skip cargo test
#   SKIP_STRFRY=1    skip the head-to-head strfry comparison (not recommended)
#   STRFRY_BIN       strfry binary path (default third_party/strfry/strfry)
#   STRFRY_CONF      strfry config path (default third_party/strfry/strfry-bench.conf)
#
# Head-to-head vs strfry:
#   strfry is NOT vendored in this repository (third_party/strfry is gitignored).
#   When $STRFRY_BIN is missing this script runs scripts/fetch_strfry.sh once to
#   clone and build the pinned strfry commit. If that fetch/build fails (no
#   network, missing build deps) the script prints a clear SKIP warning for the
#   head-to-head check and CONTINUES — the rest of the gate still runs. When
#   strfry IS available the assertion is enforced: the gate exits non-zero if
#   our relay does not beat strfry on publish events/s AND REQ p99.
#
# The script is re-runnable: every run uses a fresh temp data dir and kills the relay on exit.

set -euo pipefail

export PATH="$HOME/.cargo/bin:$PATH"

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

MIN_INGEST_EPS="${MIN_INGEST_EPS:-50000}"
MAX_REQ_P99_MS="${MAX_REQ_P99_MS:-250}"
MIN_STORE_EPS="${MIN_STORE_EPS:-100000}"
LOAD_EVENTS="${LOAD_EVENTS:-200000}"
LOAD_CONNS="${LOAD_CONNS:-16}"
REQ_QUERIES="${REQ_QUERIES:-2000}"
REQ_CONNS="${REQ_CONNS:-8}"
STORE_EVENTS="${STORE_EVENTS:-200000}"
RELAY_BIN="${RELAY_BIN:-$ROOT/target/release/nostr-relay}"
BENCH_BIN="${BENCH_BIN:-$ROOT/target/release/nostr-bench}"
SKIP_BUILD="${SKIP_BUILD:-0}"
SKIP_TESTS="${SKIP_TESTS:-0}"
SKIP_STRFRY="${SKIP_STRFRY:-0}"
STRFRY_BIN="${STRFRY_BIN:-$ROOT/third_party/strfry/strfry}"
STRFRY_CONF="${STRFRY_CONF:-$ROOT/third_party/strfry/strfry-bench.conf}"

WORK="$(mktemp -d "${TMPDIR:-/tmp}/nostr-verify.XXXXXX")"
DATA_DIR="$WORK/data"
LOG="$WORK/relay.log"
RELAY_PID=""
FAILURES=0
declare -a SUMMARY=()

cleanup() {
    if [[ -n "$RELAY_PID" ]] && kill -0 "$RELAY_PID" 2>/dev/null; then
        kill -TERM "$RELAY_PID" 2>/dev/null || true
        for _ in $(seq 1 50); do
            kill -0 "$RELAY_PID" 2>/dev/null || break
            sleep 0.1
        done
        kill -KILL "$RELAY_PID" 2>/dev/null || true
        wait "$RELAY_PID" 2>/dev/null || true
    fi
    rm -rf "$WORK"
}
trap cleanup EXIT INT TERM

say() { printf '\n=== %s ===\n' "$*"; }
pass() { SUMMARY+=("PASS  $*"); printf 'PASS  %s\n' "$*"; }
fail() { SUMMARY+=("FAIL  $*"); printf 'FAIL  %s\n' "$*"; FAILURES=$((FAILURES + 1)); }

# Extract a numeric field from a JSON document on stdin.
json_num() {
    python3 -c 'import json,sys; d=json.load(sys.stdin); print(d.get(sys.argv[1], 0))' "$1"
}

# ---------------------------------------------------------------------------
# Trim BENCHMARKS.md so it stays bounded: keep the publish-ready summary (the
# part before the first "## " raw-run heading) plus the most recent
# KEEP_ACCEPTANCE acceptance runs and KEEP_H2H head-to-head runs. Older raw
# runs are dropped. Set KEEP_ACCEPTANCE / KEEP_H2H to change the retention.
# ---------------------------------------------------------------------------
trim_benchmarks() {
    local file="$1"
    local keep_acc="${KEEP_ACCEPTANCE:-3}"
    local keep_h2h="${KEEP_H2H:-3}"
    [[ -f "$file" ]] || return 0
    python3 - "$file" "$keep_acc" "$keep_h2h" <<'PYEOF'
import re, sys
path, keep_acc, keep_h2h = sys.argv[1], int(sys.argv[2]), int(sys.argv[3])
text = open(path, encoding="utf-8").read()
# Split into: preamble (summary) + list of raw-run sections.
# A raw-run section starts at a line beginning with "## " that is either
# "## Acceptance run" or "## Head-to-head vs strfry".
lines = text.splitlines(keepends=True)
starts = []
for i, ln in enumerate(lines):
    if ln.startswith("## Acceptance run") or ln.startswith("## Head-to-head vs strfry"):
        starts.append(i)
if not starts:
    sys.exit(0)
preamble = "".join(lines[:starts[0]])
sections = []
for idx, s in enumerate(starts):
    e = starts[idx + 1] if idx + 1 < len(starts) else len(lines)
    sections.append("".join(lines[s:e]))
acc = [s for s in sections if s.startswith("## Acceptance run")]
h2h = [s for s in sections if s.startswith("## Head-to-head vs strfry")]
kept_acc = acc[-keep_acc:] if keep_acc > 0 else []
kept_h2h = h2h[-keep_h2h:] if keep_h2h > 0 else []
# Re-emit in original chronological order, keeping only the retained sections.
kept = set(id(x) for x in kept_acc + kept_h2h)
out = [preamble]
for s in sections:
    if any(s is k for k in kept_acc + kept_h2h):
        out.append(s)
new = "".join(out)
if new != text:
    open(path, "w", encoding="utf-8").write(new)
PYEOF
}

# ---------------------------------------------------------------------------
# 1. Build
# ---------------------------------------------------------------------------
if [[ "$SKIP_BUILD" != "1" ]]; then
    say "cargo build --release --workspace"
    if cargo build --release --workspace; then
        pass "release build"
    else
        fail "release build"
        printf '\nBuild failed; aborting.\n'
        exit 1
    fi
fi

for b in "$RELAY_BIN" "$BENCH_BIN"; do
    if [[ ! -x "$b" ]]; then
        fail "missing binary: $b"
        printf '\nMissing binary %s; aborting.\n' "$b"
        exit 1
    fi
done

# ---------------------------------------------------------------------------
# 2. Tests
# ---------------------------------------------------------------------------
if [[ "$SKIP_TESTS" != "1" ]]; then
    say "cargo test --workspace"
    if cargo test --workspace; then
        pass "workspace tests"
    else
        fail "workspace tests"
    fi
fi

# ---------------------------------------------------------------------------
# Relay helpers
# ---------------------------------------------------------------------------
pick_port() {
    python3 - <<'PY'
import socket
s = socket.socket()
s.bind(("127.0.0.1", 0))
print(s.getsockname()[1])
s.close()
PY
}

start_relay() {
    local port="$1"
    local data="$2"
    mkdir -p "$data"
    "$RELAY_BIN" --bind "127.0.0.1:$port" --data-dir "$data" --fsync-policy never \
        >>"$LOG" 2>&1 &
    RELAY_PID=$!
    local deadline=$((SECONDS + 30))
    while (( SECONDS < deadline )); do
        if ! kill -0 "$RELAY_PID" 2>/dev/null; then
            printf 'relay exited early; log:\n'
            tail -40 "$LOG" || true
            return 1
        fi
        if curl -fsS --max-time 1 "http://127.0.0.1:$port/health" >/dev/null 2>&1; then
            return 0
        fi
        sleep 0.2
    done
    printf 'relay did not become healthy within 30s; log:\n'
    tail -40 "$LOG" || true
    return 1
}

stop_relay() {
    if [[ -n "$RELAY_PID" ]] && kill -0 "$RELAY_PID" 2>/dev/null; then
        kill -TERM "$RELAY_PID" 2>/dev/null || true
        for _ in $(seq 1 100); do
            kill -0 "$RELAY_PID" 2>/dev/null || break
            sleep 0.1
        done
        kill -KILL "$RELAY_PID" 2>/dev/null || true
        wait "$RELAY_PID" 2>/dev/null || true
    fi
    RELAY_PID=""
}

# ---------------------------------------------------------------------------
# 3. Boot relay and run benchmarks
# ---------------------------------------------------------------------------
PORT="$(pick_port)"
URL="ws://127.0.0.1:$PORT"
HTTP="http://127.0.0.1:$PORT"

say "starting relay on $URL (data dir $DATA_DIR)"
if start_relay "$PORT" "$DATA_DIR"; then
    pass "relay /health responded"
else
    fail "relay /health"
    printf '\nRelay failed to start; aborting.\n'
    exit 1
fi

# --- NIP-11 ---
say "NIP-11 relay information document"
NIP11_HEADERS="$WORK/nip11.headers"
NIP11_BODY="$WORK/nip11.json"
if curl -fsS --max-time 5 -D "$NIP11_HEADERS" -H 'Accept: application/nostr+json' "$HTTP/" -o "$NIP11_BODY"; then
    CT="$(grep -i '^content-type:' "$NIP11_HEADERS" | tr -d '\r' | head -1 || true)"
    if grep -qi 'application/nostr+json' <<<"$CT"; then
        pass "NIP-11 content-type ($CT)"
    else
        fail "NIP-11 content-type (got: ${CT:-none})"
    fi
    if python3 -c 'import json,sys; d=json.load(open(sys.argv[1])); assert "supported_nips" in d and "name" in d' "$NIP11_BODY" 2>/dev/null; then
        pass "NIP-11 document has name+supported_nips"
    else
        fail "NIP-11 document shape"
    fi
else
    fail "NIP-11 fetch"
fi

# --- smoke ---
say "nostr-bench smoke"
SMOKE_JSON="$WORK/smoke.json"
if "$BENCH_BIN" smoke --url "$URL" --json >"$SMOKE_JSON" 2>"$WORK/smoke.err"; then
    pass "smoke (publish OK true + EVENT + EOSE)"
else
    fail "smoke"
    cat "$WORK/smoke.err" || true
    cat "$SMOKE_JSON" || true
fi

# --- load ---
say "nostr-bench load ($LOAD_EVENTS events, $LOAD_CONNS conns)"
LOAD_JSON="$WORK/load.json"
LOAD_OK=0
if "$BENCH_BIN" load --url "$URL" --events "$LOAD_EVENTS" --conns "$LOAD_CONNS" \
        --authors 100 --kinds 1,7 --json >"$LOAD_JSON" 2>"$WORK/load.err"; then
    LOAD_OK=1
else
    fail "load run"
    tail -20 "$WORK/load.err" || true
fi
LOAD_EPS=0
LOAD_P50=0
LOAD_P99=0
LOAD_P999=0
LOAD_OKTRUE=0
if [[ "$LOAD_OK" == "1" ]]; then
    LOAD_EPS="$(json_num events_per_s <"$LOAD_JSON")"
    LOAD_OKTRUE="$(json_num ok_true <"$LOAD_JSON")"
    LOAD_P50="$(python3 -c 'import json,sys;print(json.load(sys.stdin)["ack_latency"]["p50_ms"])' <"$LOAD_JSON")"
    LOAD_P99="$(python3 -c 'import json,sys;print(json.load(sys.stdin)["ack_latency"]["p99_ms"])' <"$LOAD_JSON")"
    LOAD_P999="$(python3 -c 'import json,sys;print(json.load(sys.stdin)["ack_latency"]["p999_ms"])' <"$LOAD_JSON")"
    if python3 -c "import sys; sys.exit(0 if float('$LOAD_EPS') >= float('$MIN_INGEST_EPS') else 1)"; then
        pass "load ingest ${LOAD_EPS} events/s >= ${MIN_INGEST_EPS}"
    else
        fail "load ingest ${LOAD_EPS} events/s < ${MIN_INGEST_EPS}"
    fi
fi

# --- req ---
say "nostr-bench req ($REQ_QUERIES queries, $REQ_CONNS conns)"
REQ_JSON="$WORK/req.json"
REQ_OK=0
if "$BENCH_BIN" req --url "$URL" --queries "$REQ_QUERIES" --conns "$REQ_CONNS" \
        --filter '{"kinds":[1],"limit":100}' --json >"$REQ_JSON" 2>"$WORK/req.err"; then
    REQ_OK=1
else
    fail "req run"
    tail -20 "$WORK/req.err" || true
fi
REQ_P50=0
REQ_P99=0
REQ_P999=0
REQ_EPS=0
REQ_COMPLETED=0
if [[ "$REQ_OK" == "1" ]]; then
    REQ_P50="$(python3 -c 'import json,sys;print(json.load(sys.stdin)["eose_latency"]["p50_ms"])' <"$REQ_JSON")"
    REQ_P99="$(python3 -c 'import json,sys;print(json.load(sys.stdin)["eose_latency"]["p99_ms"])' <"$REQ_JSON")"
    REQ_P999="$(python3 -c 'import json,sys;print(json.load(sys.stdin)["eose_latency"]["p999_ms"])' <"$REQ_JSON")"
    REQ_EPS="$(json_num events_per_s <"$REQ_JSON")"
    REQ_COMPLETED="$(json_num completed <"$REQ_JSON")"
    if python3 -c "import sys; sys.exit(0 if float('$REQ_P99') <= float('$MAX_REQ_P99_MS') else 1)"; then
        pass "req p99 ${REQ_P99}ms <= ${MAX_REQ_P99_MS}ms"
    else
        fail "req p99 ${REQ_P99}ms > ${MAX_REQ_P99_MS}ms"
    fi
fi

# --- mixed (informational) ---
say "nostr-bench mixed (5s, informational)"
MIXED_JSON="$WORK/mixed.json"
if "$BENCH_BIN" mixed --url "$URL" --duration 5 --conns 8 --json >"$MIXED_JSON" 2>"$WORK/mixed.err"; then
    pass "mixed workload completed"
else
    fail "mixed workload"
    tail -20 "$WORK/mixed.err" || true
fi

# --- restart persistence ---
say "restart persistence check"
PERSIST_OK=0
PERSIST_ID=""
if "$BENCH_BIN" smoke --url "$URL" --author bench-persist --json >"$WORK/persist.json" 2>/dev/null; then
    PERSIST_ID="$(json_num event_id <"$WORK/persist.json" 2>/dev/null || true)"
    PERSIST_ID="$(python3 -c 'import json,sys;print(json.load(sys.stdin).get("event_id",""))' <"$WORK/persist.json")"
fi
stop_relay
if start_relay "$PORT" "$DATA_DIR"; then
    if [[ -n "$PERSIST_ID" ]]; then
        if "$BENCH_BIN" req --url "$URL" --queries 1 --conns 1 \
                --filter "{\"ids\":[\"$PERSIST_ID\"],\"limit\":1}" --json >"$WORK/persist_req.json" 2>/dev/null; then
            RETURNED="$(json_num events_returned <"$WORK/persist_req.json")"
            if [[ "$RETURNED" -ge 1 ]]; then
                pass "restart persistence (event $PERSIST_ID survived restart)"
                PERSIST_OK=1
            else
                fail "restart persistence (event not returned after restart)"
            fi
        else
            fail "restart persistence REQ"
        fi
    else
        fail "restart persistence (no event id from pre-restart publish)"
    fi
else
    fail "relay restart"
fi

# --- store (direct, no network) ---
say "nostr-bench store ($STORE_EVENTS events)"
STORE_JSON="$WORK/store.json"
STORE_OK=0
if "$BENCH_BIN" store --events "$STORE_EVENTS" --queries 2000 --json >"$STORE_JSON" 2>"$WORK/store.err"; then
    STORE_OK=1
else
    fail "store run"
    tail -20 "$WORK/store.err" || true
fi
STORE_EPS=0
STORE_Q50=0
STORE_Q99=0
STORE_Q999=0
STORE_INSERTED=0
if [[ "$STORE_OK" == "1" ]]; then
    STORE_EPS="$(json_num ingest_events_per_s <"$STORE_JSON")"
    STORE_INSERTED="$(json_num inserted <"$STORE_JSON")"
    STORE_Q50="$(python3 -c 'import json,sys;print(json.load(sys.stdin)["query_latency"]["p50_ms"])' <"$STORE_JSON")"
    STORE_Q99="$(python3 -c 'import json,sys;print(json.load(sys.stdin)["query_latency"]["p99_ms"])' <"$STORE_JSON")"
    STORE_Q999="$(python3 -c 'import json,sys;print(json.load(sys.stdin)["query_latency"]["p999_ms"])' <"$STORE_JSON")"
    if python3 -c "import sys; sys.exit(0 if float('$STORE_EPS') >= float('$MIN_STORE_EPS') else 1)"; then
        pass "store ingest ${STORE_EPS} events/s >= ${MIN_STORE_EPS}"
    else
        fail "store ingest ${STORE_EPS} events/s < ${MIN_STORE_EPS}"
    fi
fi

stop_relay

# ---------------------------------------------------------------------------
# 4b. Head-to-head vs strfry (binding human directive: we must be FASTER).
#     Runs scripts/compare_strfry.sh, which drives an identical corpus and
#     client concurrency against both relays and exits 0 only when our relay
#     beats strfry on publish events/s AND REQ p99. Its numbers are appended
#     to BENCHMARKS.md by that script.
# ---------------------------------------------------------------------------
if [[ "$SKIP_STRFRY" != "1" ]]; then
    say "Head-to-head vs strfry (scripts/compare_strfry.sh)"
    # strfry is not vendored (third_party/strfry is gitignored). On a fresh clone
    # the binary is absent: try to fetch+build it once via scripts/fetch_strfry.sh.
    # If that fails (no network / missing build deps) print a clear SKIP warning
    # and continue WITHOUT failing the gate. When strfry IS available the
    # assertion is enforced: exit non-zero if we lose on publish events/s or REQ p99.
    if [[ ! -x "$STRFRY_BIN" ]]; then
        printf '\nstrfry binary not found at %s — attempting scripts/fetch_strfry.sh\n' "$STRFRY_BIN"
        if bash "$ROOT/scripts/fetch_strfry.sh"; then
            pass "strfry fetched/built (scripts/fetch_strfry.sh)"
        else
            printf '\nWARNING: SKIP head-to-head vs strfry — could not fetch/build strfry.\n'
            printf '         Install build deps (cmake, g++, libmicrohttpd-dev, libsecp256k1-dev,\n'
            printf '         liblmdb-dev, libzstd-dev, libflatbuffers-dev, libssl-dev) and re-run,\n'
            printf '         or set STRFRY_BIN to an existing strfry binary.\n'
            SUMMARY+=("SKIP  head-to-head vs strfry (strfry unavailable: fetch/build failed)")
        fi
    fi
    if [[ -x "$STRFRY_BIN" ]]; then
        if [[ ! -f "$STRFRY_CONF" ]]; then
            fail "strfry config missing: $STRFRY_CONF"
        else
            if SKIP_BUILD=1 LOAD_EVENTS="$LOAD_EVENTS" LOAD_CONNS="$LOAD_CONNS" \
                REQ_QUERIES="$REQ_QUERIES" REQ_CONNS="$REQ_CONNS" \
                bash "$ROOT/scripts/compare_strfry.sh"; then
                pass "head-to-head vs strfry (our relay faster on publish events/s AND REQ p99)"
            else
                fail "head-to-head vs strfry (our relay did NOT beat strfry on both metrics)"
            fi
        fi
    fi
else
    printf '\n(skipping strfry comparison: SKIP_STRFRY=1)\n'
fi

# ---------------------------------------------------------------------------
# 5. Summary + BENCHMARKS.md
# ---------------------------------------------------------------------------
say "SUMMARY"
printf '%-6s %s\n' "RESULT" "CHECK"
for line in "${SUMMARY[@]}"; do
    printf '%s\n' "$line"
done

printf '\nMeasured numbers:\n'
printf '  load ingest:      %s events/s (OK true: %s, p50 %sms, p99 %sms, p999 %sms)\n' \
    "$LOAD_EPS" "$LOAD_OKTRUE" "$LOAD_P50" "$LOAD_P99" "$LOAD_P999"
printf '  req:              p50 %sms, p99 %sms, p999 %sms, %s events/s served (%s queries completed)\n' \
    "$REQ_P50" "$REQ_P99" "$REQ_P999" "$REQ_EPS" "$REQ_COMPLETED"
printf '  store ingest:     %s events/s (%s inserted), query p50 %sms p99 %sms p999 %sms\n' \
    "$STORE_EPS" "$STORE_INSERTED" "$STORE_Q50" "$STORE_Q99" "$STORE_Q999"

BENCH_FILE="$ROOT/BENCHMARKS.md"
DATE="$(date -u '+%Y-%m-%d %H:%M:%S UTC')"
HOST="$(uname -srm)"
CORES="$(nproc 2>/dev/null || echo unknown)"
{
    printf '\n## Acceptance run — %s\n\n' "$DATE"
    printf 'Host: `%s`, %s cores. Thresholds: MIN_INGEST_EPS=%s, MAX_REQ_P99_MS=%s, MIN_STORE_EPS=%s.\n\n' \
        "$HOST" "$CORES" "$MIN_INGEST_EPS" "$MAX_REQ_P99_MS" "$MIN_STORE_EPS"
    printf '| benchmark | metric | value |\n| --- | --- | --- |\n'
    printf '| nostr-bench load | ingest events/s | %s |\n' "$LOAD_EPS"
    printf '| nostr-bench load | OK true | %s |\n' "$LOAD_OKTRUE"
    printf '| nostr-bench load | ack p50 ms | %s |\n' "$LOAD_P50"
    printf '| nostr-bench load | ack p99 ms | %s |\n' "$LOAD_P99"
    printf '| nostr-bench load | ack p999 ms | %s |\n' "$LOAD_P999"
    printf '| nostr-bench req | eose p50 ms | %s |\n' "$REQ_P50"
    printf '| nostr-bench req | eose p99 ms | %s |\n' "$REQ_P99"
    printf '| nostr-bench req | eose p999 ms | %s |\n' "$REQ_P999"
    printf '| nostr-bench req | events/s served | %s |\n' "$REQ_EPS"
    printf '| nostr-bench req | queries completed | %s |\n' "$REQ_COMPLETED"
    printf '| nostr-bench store | ingest events/s | %s |\n' "$STORE_EPS"
    printf '| nostr-bench store | events inserted | %s |\n' "$STORE_INSERTED"
    printf '| nostr-bench store | query p50 ms | %s |\n' "$STORE_Q50"
    printf '| nostr-bench store | query p99 ms | %s |\n' "$STORE_Q99"
    printf '| nostr-bench store | query p999 ms | %s |\n' "$STORE_Q999"
    printf '\nChecks:\n\n'
    for line in "${SUMMARY[@]}"; do
        printf -- '- %s\n' "$line"
    done
    printf '\nOverall: %s\n' "$([[ "$FAILURES" -eq 0 ]] && echo PASS || echo "FAIL ($FAILURES failures)")"
} >>"$BENCH_FILE"

trim_benchmarks "$BENCH_FILE"
printf '\nAppended results to %s\n' "$BENCH_FILE"

if [[ "$FAILURES" -eq 0 ]]; then
    printf '\nALL CHECKS PASSED\n'
    exit 0
fi
printf '\n%d CHECK(S) FAILED\n' "$FAILURES"
exit 1
