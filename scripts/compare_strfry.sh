#!/usr/bin/env bash
# =============================================================================
# scripts/compare_strfry.sh — head-to-head benchmark: our relay vs strfry.
#
# Binding human directive: strfry is the current fastest Nostr relay; we must
# not stop until our relay is measurably FASTER than strfry. This script drives
# an IDENTICAL pre-signed event corpus, IDENTICAL client concurrency and
# IDENTICAL filters against both relays on this machine and prints a
# side-by-side table with speedup factors.
#
# Exit code: 0 ONLY when our relay beats strfry on BOTH
#   (a) publish events/s, and
#   (b) REQ p99 latency for indexed filters.
# Otherwise exits non-zero with a clear message naming the losing metric(s).
#
# Re-runnable and self-cleaning: both relay processes and all temp dirs are
# removed on exit via trap.
#
# Env overrides:
#   LOAD_EVENTS   number of events to publish per relay   (default 200000)
#   LOAD_CONNS    concurrent publisher connections        (default 16)
#   REQ_QUERIES   number of REQ queries per relay         (default 2000)
#   REQ_CONNS     concurrent REQ connections              (default 8)
#   REQ_FILTER    filter JSON for the indexed REQ test    (default kinds+limit)
#   SETTLE_SECS   pause between phases (strfry churn race) (default 3)
#   CONNECT_STAGGER_MS  stagger connection opens to avoid strfry open race (default 20)
#   STRFRY_RETRIES retries if strfry crashes on its phmap race (default 3)
#   STRFRY_BIN    strfry binary path                      (default third_party/strfry/strfry)
#   STRFRY_CONF   strfry config path                      (default third_party/strfry/strfry-bench.conf)
#   RELAY_BIN     our relay binary path                   (default target/release/airfry-relay)
#   BENCH_BIN     airfry-bench binary path                 (default target/release/airfry-bench)
#   SKIP_BUILD=1  skip cargo build (use existing binaries)
# =============================================================================
set -euo pipefail

export PATH="$HOME/.cargo/bin:$PATH"

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

LOAD_EVENTS="${LOAD_EVENTS:-200000}"
LOAD_CONNS="${LOAD_CONNS:-16}"
REQ_QUERIES="${REQ_QUERIES:-2000}"
REQ_CONNS="${REQ_CONNS:-8}"
REQ_FILTER="${REQ_FILTER:-{\"kinds\":[1],\"limit\":100}}"
SETTLE_SECS="${SETTLE_SECS:-3}"
CONNECT_STAGGER_MS="${CONNECT_STAGGER_MS:-20}"
STRFRY_BIN="${STRFRY_BIN:-$ROOT/third_party/strfry/strfry}"
STRFRY_CONF="${STRFRY_CONF:-$ROOT/third_party/strfry/strfry-bench.conf}"
RELAY_BIN="${RELAY_BIN:-$ROOT/target/release/airfry-relay}"
BENCH_BIN="${BENCH_BIN:-$ROOT/target/release/airfry-bench}"
SKIP_BUILD="${SKIP_BUILD:-0}"

WORK="$(mktemp -d "${TMPDIR:-/tmp}/compare-strfry.XXXXXX")"
OUR_PID=""
STRFRY_PID=""
cleanup() {
    local rc=$?
    if [[ -n "$OUR_PID" ]] && kill -0 "$OUR_PID" 2>/dev/null; then
        kill -TERM "$OUR_PID" 2>/dev/null || true
        for _ in $(seq 1 50); do kill -0 "$OUR_PID" 2>/dev/null || break; sleep 0.1; done
        kill -KILL "$OUR_PID" 2>/dev/null || true
        wait "$OUR_PID" 2>/dev/null || true
    fi
    if [[ -n "$STRFRY_PID" ]] && kill -0 "$STRFRY_PID" 2>/dev/null; then
        kill -TERM "$STRFRY_PID" 2>/dev/null || true
        for _ in $(seq 1 50); do kill -0 "$STRFRY_PID" 2>/dev/null || break; sleep 0.1; done
        kill -KILL "$STRFRY_PID" 2>/dev/null || true
        wait "$STRFRY_PID" 2>/dev/null || true
    fi
    rm -rf "$WORK"
    exit "$rc"
}
trap cleanup EXIT INT TERM

say() { printf '\n=== %s ===\n' "$*"; }
die() { printf 'ERROR: %s\n' "$*" >&2; exit 1; }

pick_port() {
    python3 - <<'PY'
import socket
s = socket.socket()
s.bind(("127.0.0.1", 0))
print(s.getsockname()[1])
s.close()
PY
}

# Extract a numeric field from airfry-bench JSON on stdin.
jget() {
    python3 -c "import sys,json; d=json.load(sys.stdin); print(d$1)" 2>/dev/null || echo "0"
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
# 0. Build
# ---------------------------------------------------------------------------
if [[ "$SKIP_BUILD" != "1" ]]; then
    say "Building our relay + bench (release)"
    cargo build --release -p airfry-relay -p airfry-bench 2>&1 | tail -3
fi
[[ -x "$RELAY_BIN" ]] || die "our relay binary missing: $RELAY_BIN"
[[ -x "$BENCH_BIN" ]] || die "airfry-bench binary missing: $BENCH_BIN"
[[ -x "$STRFRY_BIN" ]] || die "strfry binary missing: $STRFRY_BIN (build with: cd third_party/strfry && make -j32)"
[[ -f "$STRFRY_CONF" ]] || die "strfry config missing: $STRFRY_CONF"

# ---------------------------------------------------------------------------
# 1. Generate ONE pre-signed corpus (identical for both relays).
#    airfry-bench signs deterministically from a fixed seed, so both relays
#    receive byte-identical events. We sign once here to warm the pool and
#    report signing time; the actual publish uses the same seed so the corpus
#    is identical.
# ---------------------------------------------------------------------------
say "Corpus: $LOAD_EVENTS events, $LOAD_CONNS conns, seed=0 (deterministic, identical for both relays)"
CORPUS_SEED=0

# ---------------------------------------------------------------------------
# 2. Our relay
# ---------------------------------------------------------------------------
OUR_PORT="$(pick_port)"
OUR_URL="ws://127.0.0.1:$OUR_PORT"
OUR_DATA="$WORK/our-data"
mkdir -p "$OUR_DATA"
OUR_LOG="$WORK/our-relay.log"

say "Starting OUR relay on port $OUR_PORT"
"$RELAY_BIN" --bind "127.0.0.1:$OUR_PORT" --data-dir "$OUR_DATA" --fsync-policy never \
    >"$OUR_LOG" 2>&1 &
OUR_PID=$!
deadline=$((SECONDS + 30))
while (( SECONDS < deadline )); do
    kill -0 "$OUR_PID" 2>/dev/null || { echo "our relay exited early:"; tail -30 "$OUR_LOG"; die "our relay failed to start"; }
    curl -fsS --max-time 1 "http://127.0.0.1:$OUR_PORT/health" >/dev/null 2>&1 && break
    sleep 0.2
done
curl -fsS --max-time 1 "http://127.0.0.1:$OUR_PORT/health" >/dev/null 2>&1 || die "our relay not healthy within 30s"

say "OUR relay: publish load ($LOAD_EVENTS events / $LOAD_CONNS conns)"
OUR_LOAD_JSON="$WORK/our-load.json"
"$BENCH_BIN" load --url "$OUR_URL" --events "$LOAD_EVENTS" --conns "$LOAD_CONNS" \
    --authors 100 --kinds 1 --seed "$CORPUS_SEED" --connect-stagger-ms "$CONNECT_STAGGER_MS" --json >"$OUR_LOAD_JSON" 2>"$WORK/our-load.err" || true
OUR_EPS="$(jget "['events_per_s']" <"$OUR_LOAD_JSON")"
OUR_OK="$(jget "['ok_true']" <"$OUR_LOAD_JSON")"
OUR_ACK_P99="$(jget "['ack_latency']['p99_ms']" <"$OUR_LOAD_JSON")"

sleep "$SETTLE_SECS"

say "OUR relay: indexed REQ ($REQ_QUERIES queries / $REQ_CONNS conns, filter=$REQ_FILTER)"
OUR_REQ_JSON="$WORK/our-req.json"
"$BENCH_BIN" req --url "$OUR_URL" --queries "$REQ_QUERIES" --conns "$REQ_CONNS" \
    --filter "$REQ_FILTER" --connect-stagger-ms "$CONNECT_STAGGER_MS" --json >"$OUR_REQ_JSON" 2>"$WORK/our-req.err" || true
OUR_REQ_P50="$(jget "['eose_latency']['p50_ms']" <"$OUR_REQ_JSON")"
OUR_REQ_P99="$(jget "['eose_latency']['p99_ms']" <"$OUR_REQ_JSON")"
OUR_REQ_P999="$(jget "['eose_latency']['p999_ms']" <"$OUR_REQ_JSON")"
OUR_REQ_EVENTS="$(jget "['events_returned']" <"$OUR_REQ_JSON")"
OUR_REQ_COMPLETED="$(jget "['completed']" <"$OUR_REQ_JSON")"

# Stop our relay before starting strfry (avoid CPU contention).
kill -TERM "$OUR_PID" 2>/dev/null || true
for _ in $(seq 1 50); do kill -0 "$OUR_PID" 2>/dev/null || break; sleep 0.1; done
kill -KILL "$OUR_PID" 2>/dev/null || true
wait "$OUR_PID" 2>/dev/null || true
OUR_PID=""
sleep 1

# ---------------------------------------------------------------------------
# 3. strfry
#
# strfry v1-4cd3cf6 has a connection-churn race: its golpe/phmap subscription
# tracking asserts (`i < capacity_`) and SIGABRTs when a new batch of websocket
# connections opens while a previous batch is disconnecting. This is a strfry
# bug, not a benchmark artefact. To get a fair measurement we (a) pause between
# phases and (b) if strfry dies during the REQ phase, restart it, reload the
# identical corpus, and retry the REQ (up to STRFRY_RETRIES attempts). The
# publish numbers are taken from the first successful load; the REQ numbers
# from the first successful REQ.
# ---------------------------------------------------------------------------
STRFRY_RETRIES="${STRFRY_RETRIES:-3}"
STRFRY_PORT="$(pick_port)"
STRFRY_URL="ws://127.0.0.1:$STRFRY_PORT"
STRFRY_DATA="$WORK/strfry-data"
STRFRY_LOG="$WORK/strfry.log"

start_strfry() {
    mkdir -p "$STRFRY_DATA"
    : >"$STRFRY_LOG"
    "$STRFRY_BIN" --config "$STRFRY_CONF" --set db="$STRFRY_DATA/" --set relay.port="$STRFRY_PORT" relay \
        >>"$STRFRY_LOG" 2>&1 &
    STRFRY_PID=$!
    local deadline=$((SECONDS + 30))
    while (( SECONDS < deadline )); do
        kill -0 "$STRFRY_PID" 2>/dev/null || { echo "strfry exited early:"; tail -30 "$STRFRY_LOG"; return 1; }
        curl -fsS --max-time 1 "http://127.0.0.1:$STRFRY_PORT/" >/dev/null 2>&1 && return 0
        sleep 0.2
    done
    echo "strfry not healthy within 30s:"; tail -30 "$STRFRY_LOG"
    return 1
}

stop_strfry() {
    if [[ -n "$STRFRY_PID" ]] && kill -0 "$STRFRY_PID" 2>/dev/null; then
        kill -TERM "$STRFRY_PID" 2>/dev/null || true
        for _ in $(seq 1 50); do kill -0 "$STRFRY_PID" 2>/dev/null || break; sleep 0.1; done
        kill -KILL "$STRFRY_PID" 2>/dev/null || true
        wait "$STRFRY_PID" 2>/dev/null || true
    fi
    STRFRY_PID=""
}

STRFRY_EPS="0"; STRFRY_OK="0"; STRFRY_ACK_P99="0"
STRFRY_REQ_P50="0"; STRFRY_REQ_P99="0"; STRFRY_REQ_P999="0"
STRFRY_REQ_EVENTS="0"; STRFRY_REQ_COMPLETED="0"
STRFRY_LOAD_JSON="$WORK/strfry-load.json"
STRFRY_REQ_JSON="$WORK/strfry-req.json"

strfry_attempt=0
while (( strfry_attempt < STRFRY_RETRIES )); do
    strfry_attempt=$((strfry_attempt + 1))
    say "STRFRY attempt $strfry_attempt/$STRFRY_RETRIES: start on port $STRFRY_PORT"
    rm -rf "$STRFRY_DATA"; mkdir -p "$STRFRY_DATA"
    if ! start_strfry; then
        echo "WARNING: strfry failed to start (attempt $strfry_attempt)"
        stop_strfry; sleep 1; continue
    fi

    say "STRFRY: publish load ($LOAD_EVENTS events / $LOAD_CONNS conns)"
    "$BENCH_BIN" load --url "$STRFRY_URL" --events "$LOAD_EVENTS" --conns "$LOAD_CONNS" \
        --authors 100 --kinds 1 --seed "$CORPUS_SEED" --connect-stagger-ms "$CONNECT_STAGGER_MS" --json >"$STRFRY_LOAD_JSON" 2>"$WORK/strfry-load.err" || true
    STRFRY_EPS="$(jget "['events_per_s']" <"$STRFRY_LOAD_JSON")"
    STRFRY_OK="$(jget "['ok_true']" <"$STRFRY_LOAD_JSON")"
    STRFRY_ACK_P99="$(jget "['ack_latency']['p99_ms']" <"$STRFRY_LOAD_JSON")"

    # Settle pause: let strfry finish tearing down the load connections before
    # the REQ batch opens new ones (avoids the churn race).
    sleep "$SETTLE_SECS"
    if ! kill -0 "$STRFRY_PID" 2>/dev/null; then
        echo "WARNING: strfry died after load phase (connection-churn race); retrying"
        stop_strfry; sleep 1; continue
    fi

    say "STRFRY: indexed REQ ($REQ_QUERIES queries / $REQ_CONNS conns, filter=$REQ_FILTER)"
    "$BENCH_BIN" req --url "$STRFRY_URL" --queries "$REQ_QUERIES" --conns "$REQ_CONNS" \
        --filter "$REQ_FILTER" --connect-stagger-ms "$CONNECT_STAGGER_MS" --json >"$STRFRY_REQ_JSON" 2>"$WORK/strfry-req.err" || true
    STRFRY_REQ_P50="$(jget "['eose_latency']['p50_ms']" <"$STRFRY_REQ_JSON")"
    STRFRY_REQ_P99="$(jget "['eose_latency']['p99_ms']" <"$STRFRY_REQ_JSON")"
    STRFRY_REQ_P999="$(jget "['eose_latency']['p999_ms']" <"$STRFRY_REQ_JSON")"
    STRFRY_REQ_EVENTS="$(jget "['events_returned']" <"$STRFRY_REQ_JSON")"
    STRFRY_REQ_COMPLETED="$(jget "['completed']" <"$STRFRY_REQ_JSON")"

    if ! kill -0 "$STRFRY_PID" 2>/dev/null; then
        echo "WARNING: strfry died during REQ phase (connection-churn race); retrying"
        stop_strfry; sleep 1; continue
    fi
    if python3 -c "import sys; sys.exit(0 if float('$STRFRY_REQ_COMPLETED') > 0 else 1)" 2>/dev/null; then
        break
    fi
    echo "WARNING: strfry REQ completed=0 (attempt $strfry_attempt); retrying"
    stop_strfry; sleep 1
done

if ! python3 -c "import sys; sys.exit(0 if float('$STRFRY_REQ_COMPLETED') > 0 else 1)" 2>/dev/null; then
    echo "WARNING: strfry REQ never completed after $STRFRY_RETRIES attempts; REQ comparison unreliable"
fi

stop_strfry

# ---------------------------------------------------------------------------
# 4. Side-by-side table + verdict
# ---------------------------------------------------------------------------
HOST="$(uname -srm)"
CORES="$(nproc 2>/dev/null || echo '?')"
DATE="$(date -u '+%Y-%m-%d %H:%M:%S UTC')"

# Compute speedup factors with python (guarded against zero/empty).
read -r PUB_SPEEDUP REQ_SPEEDUP VERDICT <<EOF
$(python3 - "$OUR_EPS" "$STRFRY_EPS" "$OUR_REQ_P99" "$STRFRY_REQ_P99" <<'PY'
import sys
def f(x):
    try: return float(x)
    except Exception: return 0.0
our_eps, str_eps, our_p99, str_p99 = f(sys.argv[1]), f(sys.argv[2]), f(sys.argv[3]), f(sys.argv[4])
pub = (our_eps / str_eps) if str_eps > 0 else 0.0
req = (str_p99 / our_p99) if our_p99 > 0 else 0.0
win_pub = our_eps > str_eps and str_eps > 0
win_req = our_p99 < str_p99 and our_p99 > 0 and str_p99 > 0
if win_pub and win_req:
    verdict = "WIN"
elif win_pub:
    verdict = "LOSE_REQ"
elif win_req:
    verdict = "LOSE_PUB"
else:
    verdict = "LOSE_BOTH"
print(f"{pub:.3f} {req:.3f} {verdict}")
PY
)
EOF

printf '\n'
printf '=====================================================================\n'
printf ' HEAD-TO-HEAD: our relay vs strfry v1-4cd3cf6\n'
printf ' Host: %s, %s cores   %s\n' "$HOST" "$CORES" "$DATE"
printf ' Corpus: %s events, seed=%s (identical for both)   Conns: %s\n' "$LOAD_EVENTS" "$CORPUS_SEED" "$LOAD_CONNS"
printf ' REQ: %s queries / %s conns, filter=%s\n' "$REQ_QUERIES" "$REQ_CONNS" "$REQ_FILTER"
printf '=====================================================================\n'
printf '%-32s %18s %18s %14s\n' "metric" "OUR RELAY" "STRFRY" "speedup"
printf '%-32s %18s %18s %14s\n' "--------------------------------" "------------------" "------------------" "--------------"
printf '%-32s %18s %18s %14s\n' "publish events/s" "$OUR_EPS" "$STRFRY_EPS" "${PUB_SPEEDUP}x"
printf '%-32s %18s %18s %14s\n' "publish OK true" "$OUR_OK" "$STRFRY_OK" "-"
printf '%-32s %18s %18s %14s\n' "publish ack p99 ms" "$OUR_ACK_P99" "$STRFRY_ACK_P99" "-"
printf '%-32s %18s %18s %14s\n' "REQ eose p50 ms" "$OUR_REQ_P50" "$STRFRY_REQ_P50" "-"
printf '%-32s %18s %18s %14s\n' "REQ eose p99 ms" "$OUR_REQ_P99" "$STRFRY_REQ_P99" "${REQ_SPEEDUP}x"
printf '%-32s %18s %18s %14s\n' "REQ eose p999 ms" "$OUR_REQ_P999" "$STRFRY_REQ_P999" "-"
printf '%-32s %18s %18s %14s\n' "REQ events returned" "$OUR_REQ_EVENTS" "$STRFRY_REQ_EVENTS" "-"
printf '%-32s %18s %18s %14s\n' "REQ queries completed" "$OUR_REQ_COMPLETED" "$STRFRY_REQ_COMPLETED" "-"
printf '=====================================================================\n'
printf ' Publish speedup (ours/strfry): %sx\n' "$PUB_SPEEDUP"
printf ' REQ p99 speedup (strfry/ours): %sx\n' "$REQ_SPEEDUP"
printf '=====================================================================\n'

# ---------------------------------------------------------------------------
# 5. Append to BENCHMARKS.md
# ---------------------------------------------------------------------------
BENCH_FILE="$ROOT/BENCHMARKS.md"
{
    printf '\n## Head-to-head vs strfry — %s\n\n' "$DATE"
    printf 'Host: `%s`, %s cores. Corpus: %s events seed=%s (identical for both relays). Conns: %s. REQ: %s queries / %s conns, filter `%s`.\n\n' \
        "$HOST" "$CORES" "$LOAD_EVENTS" "$CORPUS_SEED" "$LOAD_CONNS" "$REQ_QUERIES" "$REQ_CONNS" "$REQ_FILTER"
    printf 'strfry config: `third_party/strfry/strfry-bench.conf` (auth disabled, no write policy, mapsize 10TB, compression disabled, ingester=8 reqWorker=8).\n\n'
    printf '| metric | our relay | strfry | speedup |\n'
    printf '| --- | --- | --- | --- |\n'
    printf '| publish events/s | %s | %s | %sx |\n' "$OUR_EPS" "$STRFRY_EPS" "$PUB_SPEEDUP"
    printf '| publish OK true | %s | %s | - |\n' "$OUR_OK" "$STRFRY_OK"
    printf '| publish ack p99 ms | %s | %s | - |\n' "$OUR_ACK_P99" "$STRFRY_ACK_P99"
    printf '| REQ eose p50 ms | %s | %s | - |\n' "$OUR_REQ_P50" "$STRFRY_REQ_P50"
    printf '| REQ eose p99 ms | %s | %s | %sx |\n' "$OUR_REQ_P99" "$STRFRY_REQ_P99" "$REQ_SPEEDUP"
    printf '| REQ eose p999 ms | %s | %s | - |\n' "$OUR_REQ_P999" "$STRFRY_REQ_P999"
    printf '| REQ events returned | %s | %s | - |\n' "$OUR_REQ_EVENTS" "$STRFRY_REQ_EVENTS"
    printf '\nVerdict: **%s** (publish speedup %sx, REQ p99 speedup %sx).\n\n' "$VERDICT" "$PUB_SPEEDUP" "$REQ_SPEEDUP"
    printf 'Commands: `scripts/compare_strfry.sh` (env: LOAD_EVENTS=%s LOAD_CONNS=%s REQ_QUERIES=%s REQ_CONNS=%s).\n' \
        "$LOAD_EVENTS" "$LOAD_CONNS" "$REQ_QUERIES" "$REQ_CONNS"
} >>"$BENCH_FILE"
trim_benchmarks "$BENCH_FILE"

# ---------------------------------------------------------------------------
# 6. Exit status
# ---------------------------------------------------------------------------
case "$VERDICT" in
    WIN)
        printf '\nRESULT: PASS — our relay is FASTER than strfry on both publish events/s and REQ p99.\n'
        exit 0
        ;;
    LOSE_PUB)
        printf '\nRESULT: FAIL — our relay LOSES on publish events/s (ours=%s, strfry=%s, %sx).\n' "$OUR_EPS" "$STRFRY_EPS" "$PUB_SPEEDUP"
        exit 1
        ;;
    LOSE_REQ)
        printf '\nRESULT: FAIL — our relay LOSES on REQ p99 (ours=%sms, strfry=%sms, %sx).\n' "$OUR_REQ_P99" "$STRFRY_REQ_P99" "$REQ_SPEEDUP"
        exit 1
        ;;
    *)
        printf '\nRESULT: FAIL — our relay LOSES on BOTH publish events/s (%sx) and REQ p99 (%sx).\n' "$PUB_SPEEDUP" "$REQ_SPEEDUP"
        exit 1
        ;;
esac
