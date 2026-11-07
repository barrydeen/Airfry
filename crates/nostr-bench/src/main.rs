//! `nostr-bench` — load generator and acceptance harness for the nostr relay.
//!
//! Subcommands:
//!   smoke  — publish one signed event, expect OK true, REQ it back, expect EVENT+EOSE.
//!   load   — pre-sign a deterministic pool, publish it concurrently over C websockets.
//!   req    — issue REQ queries concurrently, measure time-to-EOSE and events served.
//!   mixed  — realistic blend of publishes and REQs for a fixed duration.
//!   store  — benchmark nostr-store directly (no network).
//!
//! Every subcommand supports `--json` for machine-readable output on stdout.

mod client;
mod filters;
mod pool;
mod report;
mod stats;

use clap::{Args, Parser, Subcommand};
use futures_util::{SinkExt, StreamExt};
use nostr_core::{Event, Filter, Keys};
use serde::Serialize;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio_tungstenite::tungstenite::Message;

use client::{classify, connect, event_frame, message_text, req_frame, send_text, RelayFrame};
use report::{f2, f3, print_json, print_table, LatencyReport};
use stats::{dur_us, summarize};

// ---------------------------------------------------------------------------
// CLI
// ---------------------------------------------------------------------------

#[derive(Parser, Debug)]
#[command(
    name = "nostr-bench",
    version,
    about = "Load generator for the nostr relay"
)]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand, Debug)]
enum Cmd {
    /// Publish one event, read it back, verify OK/EVENT/EOSE.
    Smoke(SmokeArgs),
    /// Publish a pre-signed event pool concurrently and measure throughput/latency.
    Load(LoadArgs),
    /// Issue REQ queries concurrently and measure time-to-EOSE and events served.
    Req(ReqArgs),
    /// Realistic blend of publishes and REQs for a fixed duration.
    Mixed(MixedArgs),
    /// Benchmark nostr-store directly (no network).
    Store(StoreArgs),
    /// Dump the deterministic pre-signed corpus as JSONL (one event per line)
    /// to stdout. Used to feed an identical corpus to another relay (strfry import).
    Dump(DumpArgs),
}

#[derive(Args, Debug, Clone)]
struct Common {
    /// Relay websocket URL, e.g. ws://127.0.0.1:8080
    #[arg(long, default_value = "ws://127.0.0.1:8080")]
    url: String,
    /// Emit machine-readable JSON on stdout.
    #[arg(long)]
    json: bool,
}

#[derive(Args, Debug, Clone)]
struct SmokeArgs {
    #[command(flatten)]
    common: Common,
    /// Author label used to derive the deterministic signing key.
    #[arg(long, default_value = "bench-smoke")]
    author: String,
    /// Event kind to publish.
    #[arg(long, default_value_t = 1)]
    kind: u32,
    /// Seconds to wait for each expected relay frame.
    #[arg(long, default_value_t = 10)]
    timeout: u64,
}

#[derive(Args, Debug, Clone)]
struct LoadArgs {
    #[command(flatten)]
    common: Common,
    /// Number of events to publish.
    #[arg(long, default_value_t = 100_000)]
    events: usize,
    /// Number of concurrent websocket connections.
    #[arg(long, default_value_t = 16)]
    conns: usize,
    /// Number of distinct author keypairs.
    #[arg(long, default_value_t = 100)]
    authors: usize,
    /// Comma-separated event kinds to cycle through.
    #[arg(long, default_value = "1")]
    kinds: String,
    /// Seed mixed into the corpus so repeated runs produce fresh ids.
    #[arg(long, default_value_t = 0)]
    seed: u64,
    /// Maximum number of in-flight publishes per connection (pipelining depth).
    #[arg(long, default_value_t = 64)]
    pipeline: usize,
    /// Milliseconds to stagger connection opens (0 = all at once). Some relays
    /// (e.g. strfry) have a race when many websockets open simultaneously.
    #[arg(long, default_value_t = 0)]
    connect_stagger_ms: u64,
    /// Seconds to wait for the final acks before giving up.
    #[arg(long, default_value_t = 120)]
    timeout: u64,
}

#[derive(Args, Debug, Clone)]
struct ReqArgs {
    #[command(flatten)]
    common: Common,
    /// Number of REQ queries to issue.
    #[arg(long, default_value_t = 1_000)]
    queries: usize,
    /// Number of concurrent websocket connections.
    #[arg(long, default_value_t = 8)]
    conns: usize,
    /// Filter JSON, e.g. '{"kinds":[1],"limit":100}'.
    #[arg(long, default_value = r#"{"kinds":[1],"limit":100}"#)]
    filter: String,
    /// Milliseconds to stagger connection opens (0 = all at once). Some relays
    /// (e.g. strfry) have a race when many websockets open simultaneously.
    #[arg(long, default_value_t = 0)]
    connect_stagger_ms: u64,
    /// Seconds to wait for each EOSE.
    #[arg(long, default_value_t = 30)]
    timeout: u64,
}

#[derive(Args, Debug, Clone)]
struct MixedArgs {
    #[command(flatten)]
    common: Common,
    /// Duration of the mixed workload in seconds.
    #[arg(long, default_value_t = 10)]
    duration: u64,
    /// Number of concurrent connections.
    #[arg(long, default_value_t = 16)]
    conns: usize,
    /// Number of distinct author keypairs.
    #[arg(long, default_value_t = 100)]
    authors: usize,
    /// Fraction of operations that are publishes (0.0-1.0).
    #[arg(long, default_value_t = 0.8)]
    publish_ratio: f64,
    /// Seed for the corpus.
    #[arg(long, default_value_t = 0)]
    seed: u64,
}

#[derive(Args, Debug, Clone)]
struct StoreArgs {
    /// Number of events to insert.
    #[arg(long, default_value_t = 200_000)]
    events: usize,
    /// Number of distinct author keypairs.
    #[arg(long, default_value_t = 100)]
    authors: usize,
    /// Comma-separated event kinds to cycle through.
    #[arg(long, default_value = "1")]
    kinds: String,
    /// Number of queries to run after ingest.
    #[arg(long, default_value_t = 2_000)]
    queries: usize,
    /// Seed for the corpus.
    #[arg(long, default_value_t = 0)]
    seed: u64,
    /// Emit machine-readable JSON on stdout.
    #[arg(long)]
    json: bool,
}

#[derive(Args, Debug, Clone)]
struct DumpArgs {
    /// Number of events to generate.
    #[arg(long, default_value_t = 100_000)]
    events: usize,
    /// Number of distinct author keypairs.
    #[arg(long, default_value_t = 100)]
    authors: usize,
    /// Comma-separated event kinds to cycle through.
    #[arg(long, default_value = "1")]
    kinds: String,
    /// Seed for the corpus (must match the load/req seed for identical events).
    #[arg(long, default_value_t = 0)]
    seed: u64,
    /// Output file path (default: stdout).
    #[arg(long)]
    out: Option<String>,
}

fn parse_kinds(s: &str) -> Result<Vec<u32>, String> {
    let mut out = Vec::new();
    for part in s.split(',') {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        out.push(
            part.parse::<u32>()
                .map_err(|e| format!("invalid kind {part:?}: {e}"))?,
        );
    }
    if out.is_empty() {
        return Err("no kinds given".to_string());
    }
    Ok(out)
}

// ---------------------------------------------------------------------------
// smoke
// ---------------------------------------------------------------------------

#[derive(Serialize)]
struct SmokeResult {
    pass: bool,
    url: String,
    event_id: String,
    ok_true: bool,
    event_returned: bool,
    eose: bool,
    error: Option<String>,
}

async fn run_smoke(a: &SmokeArgs) -> SmokeResult {
    let mut res = SmokeResult {
        pass: false,
        url: a.common.url.clone(),
        event_id: String::new(),
        ok_true: false,
        event_returned: false,
        eose: false,
        error: None,
    };
    match smoke_inner(a, &mut res).await {
        Ok(()) => {
            res.pass = res.ok_true && res.event_returned && res.eose;
        }
        Err(e) => res.error = Some(e),
    }
    res
}

async fn smoke_inner(a: &SmokeArgs, res: &mut SmokeResult) -> Result<(), String> {
    let keys = Keys::from_label(&a.author);
    let created_at = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(pool::BASE_CREATED_AT);
    let ev = keys.sign_event(
        created_at,
        a.kind,
        vec![vec!["t".to_string(), "bench-smoke".to_string()]],
        "nostr-bench smoke test",
    );
    res.event_id = hex::encode(ev.id);
    let id_hex = res.event_id.clone();

    let mut ws = connect(&a.common.url).await?;
    let budget = Duration::from_secs(a.timeout);

    send_text(&mut ws, event_frame(&ev.to_json())).await?;

    // Wait for OK true for our id.
    let deadline = tokio::time::Instant::now() + budget;
    loop {
        match tokio::time::timeout_at(deadline, ws.next()).await {
            Ok(Some(Ok(msg))) => {
                let Some(text) = message_text(&msg) else {
                    continue;
                };
                match classify(text) {
                    RelayFrame::Ok { id, ok } if id == id_hex => {
                        if !ok {
                            return Err(format!("relay rejected event: {text}"));
                        }
                        res.ok_true = true;
                        break;
                    }
                    RelayFrame::Notice => return Err(format!("relay NOTICE: {text}")),
                    RelayFrame::Closed(_) => return Err(format!("relay CLOSED: {text}")),
                    _ => {}
                }
            }
            Ok(Some(Err(e))) => return Err(format!("websocket error: {e}")),
            Ok(None) => return Err("relay closed the connection".to_string()),
            Err(_) => return Err("timeout waiting for OK".to_string()),
        }
    }

    // REQ the event back by id.
    let filter = Filter::from_json_value(&serde_json::json!({ "ids": [id_hex], "limit": 1 }))
        .map_err(|e| format!("filter: {e}"))?;
    let filter_json = serde_json::to_string(&filter.to_json_value()).map_err(|e| e.to_string())?;
    send_text(&mut ws, req_frame("smoke", &[filter_json])).await?;

    let deadline = tokio::time::Instant::now() + budget;
    loop {
        match tokio::time::timeout_at(deadline, ws.next()).await {
            Ok(Some(Ok(msg))) => {
                let Some(text) = message_text(&msg) else {
                    continue;
                };
                match classify(text) {
                    RelayFrame::Event => res.event_returned = true,
                    RelayFrame::Eose(_) => {
                        res.eose = true;
                        break;
                    }
                    RelayFrame::Closed(_) => return Err(format!("subscription closed: {text}")),
                    RelayFrame::Notice => return Err(format!("relay NOTICE: {text}")),
                    _ => {}
                }
            }
            Ok(Some(Err(e))) => return Err(format!("websocket error: {e}")),
            Ok(None) => return Err("relay closed the connection".to_string()),
            Err(_) => return Err("timeout waiting for EOSE".to_string()),
        }
    }

    let _ = ws.send(Message::Text(client::close_frame("smoke"))).await;
    let _ = ws.close(None).await;
    Ok(())
}

// ---------------------------------------------------------------------------
// load
// ---------------------------------------------------------------------------

#[derive(Serialize)]
struct LoadResult {
    url: String,
    events_requested: usize,
    conns: usize,
    authors: usize,
    kinds: Vec<u32>,
    pipeline: usize,
    ok_true: u64,
    ok_false: u64,
    errors: u64,
    elapsed_s: f64,
    events_per_s: f64,
    ack_latency: LatencyReport,
    sign_seconds: f64,
    pass: bool,
}

async fn run_load(a: &LoadArgs) -> Result<LoadResult, String> {
    let kinds = parse_kinds(&a.kinds)?;
    let conns = a.conns.max(1);
    let pipeline = a.pipeline.max(1);

    eprintln!(
        "[load] signing {} events ({} authors, kinds {:?})...",
        a.events, a.authors, kinds
    );
    let t_sign = Instant::now();
    let cfg = pool::PoolConfig::new(a.events, a.authors, kinds.clone(), a.seed);
    let pool = Arc::new(pool::build_pool(&cfg));
    let sign_seconds = t_sign.elapsed().as_secs_f64();
    eprintln!(
        "[load] signed {} events in {:.2}s",
        pool.len(),
        sign_seconds
    );

    if pool.is_empty() {
        return Err("event pool is empty".to_string());
    }

    let ok_true = Arc::new(AtomicU64::new(0));
    let ok_false = Arc::new(AtomicU64::new(0));
    let errors = Arc::new(AtomicU64::new(0));
    let latencies: Arc<parking_lot::Mutex<Vec<u64>>> =
        Arc::new(parking_lot::Mutex::new(Vec::new()));

    let url = a.common.url.clone();
    let total = pool.len();
    let timeout = Duration::from_secs(a.timeout);
    let stagger = Duration::from_millis(a.connect_stagger_ms);

    let start = Instant::now();
    let mut handles = Vec::with_capacity(conns);
    for c in 0..conns {
        let pool = Arc::clone(&pool);
        let url = url.clone();
        let ok_true = Arc::clone(&ok_true);
        let ok_false = Arc::clone(&ok_false);
        let errors = Arc::clone(&errors);
        let latencies = Arc::clone(&latencies);
        handles.push(tokio::spawn(async move {
            // Interleave the corpus across connections so every connection publishes a
            // contiguous, disjoint slice (no duplicate ids).
            let lo = total * c / conns;
            let hi = total * (c + 1) / conns;
            if lo >= hi {
                return;
            }
            if !stagger.is_zero() && c > 0 {
                tokio::time::sleep(stagger * c as u32).await;
            }
            let mut ws = match connect(&url).await {
                Ok(w) => w,
                Err(e) => {
                    eprintln!("[load] conn {c}: {e}");
                    errors.fetch_add((hi - lo) as u64, Ordering::Relaxed);
                    return;
                }
            };
            // Drain the initial AUTH challenge.
            let _ = client::drain_initial(&mut ws, Duration::from_millis(200)).await;

            let mut idx = lo;
            let mut inflight: std::collections::VecDeque<(String, Instant)> =
                std::collections::VecDeque::with_capacity(pipeline);
            let mut local_lat: Vec<u64> = Vec::with_capacity(hi - lo);
            let deadline = tokio::time::Instant::now() + timeout;

            loop {
                // Fill the pipeline.
                while inflight.len() < pipeline && idx < hi {
                    let frame = pool.frames[idx].clone();
                    let id = hex::encode(pool.ids[idx]);
                    let sent = Instant::now();
                    if let Err(e) = send_text(&mut ws, frame).await {
                        eprintln!("[load] conn {c}: send error: {e}");
                        errors.fetch_add(1, Ordering::Relaxed);
                        idx += 1;
                        continue;
                    }
                    inflight.push_back((id, sent));
                    idx += 1;
                }
                if inflight.is_empty() && idx >= hi {
                    break;
                }
                // Read acks.
                match tokio::time::timeout_at(deadline, ws.next()).await {
                    Ok(Some(Ok(msg))) => {
                        let Some(text) = message_text(&msg) else {
                            continue;
                        };
                        match classify(text) {
                            RelayFrame::Ok { id, ok } => {
                                if let Some(pos) = inflight.iter().position(|(i, _)| i == id) {
                                    let (_, sent) = inflight.remove(pos).unwrap();
                                    local_lat.push(dur_us(sent.elapsed()));
                                }
                                if ok {
                                    ok_true.fetch_add(1, Ordering::Relaxed);
                                } else {
                                    ok_false.fetch_add(1, Ordering::Relaxed);
                                }
                            }
                            RelayFrame::Notice | RelayFrame::Closed(_) => {}
                            _ => {}
                        }
                    }
                    Ok(Some(Err(e))) => {
                        eprintln!("[load] conn {c}: ws error: {e}");
                        errors.fetch_add(inflight.len() as u64, Ordering::Relaxed);
                        break;
                    }
                    Ok(None) => {
                        errors.fetch_add(inflight.len() as u64, Ordering::Relaxed);
                        break;
                    }
                    Err(_) => {
                        eprintln!("[load] conn {c}: timeout with {} in flight", inflight.len());
                        errors.fetch_add(inflight.len() as u64, Ordering::Relaxed);
                        break;
                    }
                }
            }
            latencies.lock().extend(local_lat);
            let _ = ws.close(None).await;
        }));
    }
    for h in handles {
        let _ = h.await;
    }
    let elapsed = start.elapsed().as_secs_f64().max(1e-9);

    let ok = ok_true.load(Ordering::Relaxed);
    let lat = latencies.lock().clone();
    let summary = summarize(lat);
    let eps = ok as f64 / elapsed;

    Ok(LoadResult {
        url: a.common.url.clone(),
        events_requested: a.events,
        conns,
        authors: a.authors,
        kinds,
        pipeline,
        ok_true: ok,
        ok_false: ok_false.load(Ordering::Relaxed),
        errors: errors.load(Ordering::Relaxed),
        elapsed_s: f3(elapsed).parse().unwrap_or(elapsed),
        events_per_s: eps.round(),
        ack_latency: LatencyReport::from(&summary),
        sign_seconds: f3(sign_seconds).parse().unwrap_or(sign_seconds),
        pass: ok > 0 && eps > 0.0,
    })
}

// ---------------------------------------------------------------------------
// req
// ---------------------------------------------------------------------------

#[derive(Serialize)]
struct ReqResult {
    url: String,
    queries_requested: usize,
    conns: usize,
    filter: String,
    completed: u64,
    events_returned: u64,
    errors: u64,
    elapsed_s: f64,
    queries_per_s: f64,
    events_per_s: f64,
    eose_latency: LatencyReport,
    pass: bool,
}

async fn run_req(a: &ReqArgs) -> Result<ReqResult, String> {
    let spec = filters::parse_filter(&a.filter)?;
    let conns = a.conns.max(1);
    let queries = a.queries;

    let completed = Arc::new(AtomicU64::new(0));
    let events_returned = Arc::new(AtomicU64::new(0));
    let errors = Arc::new(AtomicU64::new(0));
    let latencies: Arc<parking_lot::Mutex<Vec<u64>>> =
        Arc::new(parking_lot::Mutex::new(Vec::new()));

    let url = a.common.url.clone();
    let timeout = Duration::from_secs(a.timeout);
    let stagger = Duration::from_millis(a.connect_stagger_ms);
    let start = Instant::now();
    let mut handles = Vec::with_capacity(conns);
    for c in 0..conns {
        let url = url.clone();
        let filter_json = spec.json.clone();
        let completed = Arc::clone(&completed);
        let events_returned = Arc::clone(&events_returned);
        let errors = Arc::clone(&errors);
        let latencies = Arc::clone(&latencies);
        handles.push(tokio::spawn(async move {
            let lo = queries * c / conns;
            let hi = queries * (c + 1) / conns;
            if lo >= hi {
                return;
            }
            if !stagger.is_zero() && c > 0 {
                tokio::time::sleep(stagger * c as u32).await;
            }
            let mut ws = match connect(&url).await {
                Ok(w) => w,
                Err(e) => {
                    eprintln!("[req] conn {c}: {e}");
                    errors.fetch_add((hi - lo) as u64, Ordering::Relaxed);
                    return;
                }
            };
            let _ = client::drain_initial(&mut ws, Duration::from_millis(200)).await;
            let mut local_lat = Vec::with_capacity(hi - lo);
            let mut local_events = 0u64;
            let mut local_done = 0u64;

            for q in lo..hi {
                let sub = format!("q{c}-{q}");
                let sent = Instant::now();
                if let Err(e) =
                    send_text(&mut ws, req_frame(&sub, std::slice::from_ref(&filter_json))).await
                {
                    eprintln!("[req] conn {c}: send error: {e}");
                    errors.fetch_add(1, Ordering::Relaxed);
                    continue;
                }
                let deadline = tokio::time::Instant::now() + timeout;
                let mut got_eose = false;
                loop {
                    match tokio::time::timeout_at(deadline, ws.next()).await {
                        Ok(Some(Ok(msg))) => {
                            let Some(text) = message_text(&msg) else {
                                continue;
                            };
                            match classify(text) {
                                RelayFrame::Event => local_events += 1,
                                RelayFrame::Eose(s) if s == sub => {
                                    local_lat.push(dur_us(sent.elapsed()));
                                    local_done += 1;
                                    got_eose = true;
                                    break;
                                }
                                RelayFrame::Closed(s) if s == sub => {
                                    errors.fetch_add(1, Ordering::Relaxed);
                                    break;
                                }
                                _ => {}
                            }
                        }
                        Ok(Some(Err(e))) => {
                            eprintln!("[req] conn {c}: ws error: {e}");
                            errors.fetch_add(1, Ordering::Relaxed);
                            break;
                        }
                        Ok(None) => {
                            errors.fetch_add(1, Ordering::Relaxed);
                            break;
                        }
                        Err(_) => {
                            eprintln!("[req] conn {c}: timeout waiting for EOSE on {sub}");
                            errors.fetch_add(1, Ordering::Relaxed);
                            break;
                        }
                    }
                }
                if got_eose {
                    let _ = send_text(&mut ws, client::close_frame(&sub)).await;
                }
            }
            latencies.lock().extend(local_lat);
            events_returned.fetch_add(local_events, Ordering::Relaxed);
            completed.fetch_add(local_done, Ordering::Relaxed);
            let _ = ws.close(None).await;
        }));
    }
    for h in handles {
        let _ = h.await;
    }
    let elapsed = start.elapsed().as_secs_f64().max(1e-9);
    let done = completed.load(Ordering::Relaxed);
    let evs = events_returned.load(Ordering::Relaxed);
    let summary = summarize(latencies.lock().clone());

    Ok(ReqResult {
        url: a.common.url.clone(),
        queries_requested: queries,
        conns,
        filter: spec.json.clone(),
        completed: done,
        events_returned: evs,
        errors: errors.load(Ordering::Relaxed),
        elapsed_s: f3(elapsed).parse().unwrap_or(elapsed),
        queries_per_s: (done as f64 / elapsed).round(),
        events_per_s: (evs as f64 / elapsed).round(),
        eose_latency: LatencyReport::from(&summary),
        pass: done > 0,
    })
}

// ---------------------------------------------------------------------------
// mixed
// ---------------------------------------------------------------------------

#[derive(Serialize)]
struct MixedResult {
    url: String,
    duration_s: u64,
    conns: usize,
    publish_ratio: f64,
    publishes_ok: u64,
    publishes_failed: u64,
    queries_completed: u64,
    events_returned: u64,
    errors: u64,
    publish_eps: f64,
    query_qps: f64,
    publish_latency: LatencyReport,
    query_latency: LatencyReport,
    pass: bool,
}

async fn run_mixed(a: &MixedArgs) -> Result<MixedResult, String> {
    let conns = a.conns.max(1);
    let duration = Duration::from_secs(a.duration.max(1));
    // Pre-sign enough events to keep every connection busy for the whole run.
    let per_conn = (a.duration as usize).saturating_mul(20_000).max(2_000);
    let total_events = per_conn.saturating_mul(conns);
    eprintln!("[mixed] signing {} events...", total_events);
    let cfg = pool::PoolConfig::new(total_events, a.authors, vec![1, 7], a.seed);
    let pool = Arc::new(pool::build_pool(&cfg));
    let authors: Vec<String> = (0..a.authors.min(8))
        .map(|i| Keys::from_label(&pool::author_label(i)).public_key_hex())
        .collect();
    let specs = Arc::new(filters::rotating_filters(
        &authors,
        &filters::topic_values(),
    ));

    let publishes_ok = Arc::new(AtomicU64::new(0));
    let publishes_failed = Arc::new(AtomicU64::new(0));
    let queries_completed = Arc::new(AtomicU64::new(0));
    let events_returned = Arc::new(AtomicU64::new(0));
    let errors = Arc::new(AtomicU64::new(0));
    let pub_lat: Arc<parking_lot::Mutex<Vec<u64>>> = Arc::new(parking_lot::Mutex::new(Vec::new()));
    let req_lat: Arc<parking_lot::Mutex<Vec<u64>>> = Arc::new(parking_lot::Mutex::new(Vec::new()));

    let url = a.common.url.clone();
    let ratio = a.publish_ratio.clamp(0.0, 1.0);
    let start = Instant::now();
    let end = start + duration;
    let mut handles = Vec::with_capacity(conns);
    for c in 0..conns {
        let pool = Arc::clone(&pool);
        let specs = Arc::clone(&specs);
        let url = url.clone();
        let publishes_ok = Arc::clone(&publishes_ok);
        let publishes_failed = Arc::clone(&publishes_failed);
        let queries_completed = Arc::clone(&queries_completed);
        let events_returned = Arc::clone(&events_returned);
        let errors = Arc::clone(&errors);
        let pub_lat = Arc::clone(&pub_lat);
        let req_lat = Arc::clone(&req_lat);
        handles.push(tokio::spawn(async move {
            let mut ws = match connect(&url).await {
                Ok(w) => w,
                Err(e) => {
                    eprintln!("[mixed] conn {c}: {e}");
                    errors.fetch_add(1, Ordering::Relaxed);
                    return;
                }
            };
            let _ = client::drain_initial(&mut ws, Duration::from_millis(200)).await;
            let mut idx = pool.len() * c / conns;
            let mut q = 0usize;
            let mut local_pub = Vec::new();
            let mut local_req = Vec::new();
            let mut rng_state = 0x9E3779B97F4A7C15u64 ^ (c as u64 + 1);
            let mut next_rand = || {
                rng_state ^= rng_state << 13;
                rng_state ^= rng_state >> 7;
                rng_state ^= rng_state << 17;
                rng_state
            };
            while Instant::now() < end {
                let do_publish = (next_rand() % 1000) as f64 / 1000.0 < ratio;
                if do_publish {
                    if idx >= pool.len() {
                        idx = 0;
                    }
                    let frame = pool.frames[idx].clone();
                    let id = hex::encode(pool.ids[idx]);
                    idx += 1;
                    let sent = Instant::now();
                    if send_text(&mut ws, frame).await.is_err() {
                        errors.fetch_add(1, Ordering::Relaxed);
                        continue;
                    }
                    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
                    loop {
                        match tokio::time::timeout_at(deadline, ws.next()).await {
                            Ok(Some(Ok(msg))) => {
                                let Some(text) = message_text(&msg) else {
                                    continue;
                                };
                                if let RelayFrame::Ok { id: rid, ok } = classify(text) {
                                    if rid == id {
                                        local_pub.push(dur_us(sent.elapsed()));
                                        if ok {
                                            publishes_ok.fetch_add(1, Ordering::Relaxed);
                                        } else {
                                            publishes_failed.fetch_add(1, Ordering::Relaxed);
                                        }
                                        break;
                                    }
                                }
                            }
                            Ok(Some(Err(_))) | Ok(None) | Err(_) => {
                                errors.fetch_add(1, Ordering::Relaxed);
                                break;
                            }
                        }
                    }
                } else {
                    let spec = &specs[q % specs.len()];
                    q += 1;
                    let sub = format!("m{c}-{q}");
                    let sent = Instant::now();
                    if send_text(&mut ws, req_frame(&sub, std::slice::from_ref(&spec.json)))
                        .await
                        .is_err()
                    {
                        errors.fetch_add(1, Ordering::Relaxed);
                        continue;
                    }
                    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
                    let mut n = 0u64;
                    loop {
                        match tokio::time::timeout_at(deadline, ws.next()).await {
                            Ok(Some(Ok(msg))) => {
                                let Some(text) = message_text(&msg) else {
                                    continue;
                                };
                                match classify(text) {
                                    RelayFrame::Event => n += 1,
                                    RelayFrame::Eose(s) if s == sub => {
                                        local_req.push(dur_us(sent.elapsed()));
                                        queries_completed.fetch_add(1, Ordering::Relaxed);
                                        events_returned.fetch_add(n, Ordering::Relaxed);
                                        break;
                                    }
                                    RelayFrame::Closed(s) if s == sub => {
                                        errors.fetch_add(1, Ordering::Relaxed);
                                        break;
                                    }
                                    _ => {}
                                }
                            }
                            Ok(Some(Err(_))) | Ok(None) | Err(_) => {
                                errors.fetch_add(1, Ordering::Relaxed);
                                break;
                            }
                        }
                    }
                    let _ = send_text(&mut ws, client::close_frame(&sub)).await;
                }
            }
            pub_lat.lock().extend(local_pub);
            req_lat.lock().extend(local_req);
            let _ = ws.close(None).await;
        }));
    }
    for h in handles {
        let _ = h.await;
    }
    let elapsed = start.elapsed().as_secs_f64().max(1e-9);
    let pok = publishes_ok.load(Ordering::Relaxed);
    let qdone = queries_completed.load(Ordering::Relaxed);
    let pub_summary = summarize(pub_lat.lock().clone());
    let req_summary = summarize(req_lat.lock().clone());

    Ok(MixedResult {
        url: a.common.url.clone(),
        duration_s: a.duration,
        conns,
        publish_ratio: ratio,
        publishes_ok: pok,
        publishes_failed: publishes_failed.load(Ordering::Relaxed),
        queries_completed: qdone,
        events_returned: events_returned.load(Ordering::Relaxed),
        errors: errors.load(Ordering::Relaxed),
        publish_eps: (pok as f64 / elapsed).round(),
        query_qps: (qdone as f64 / elapsed).round(),
        publish_latency: LatencyReport::from(&pub_summary),
        query_latency: LatencyReport::from(&req_summary),
        pass: pok > 0 && qdone > 0,
    })
}

// ---------------------------------------------------------------------------
// store
// ---------------------------------------------------------------------------

#[derive(Serialize)]
struct StoreResult {
    events_requested: usize,
    inserted: u64,
    duplicates: u64,
    rejected: u64,
    ingest_s: f64,
    ingest_events_per_s: f64,
    bytes_on_disk: u64,
    segments: u64,
    index_entries: u64,
    queries_run: usize,
    query_events_returned: u64,
    query_latency: LatencyReport,
    query_events_per_s: f64,
    pass: bool,
}

fn run_store(a: &StoreArgs) -> Result<StoreResult, String> {
    use nostr_store::{InsertOutcome, Store, StoreConfig};

    let kinds = parse_kinds(&a.kinds)?;
    eprintln!("[store] signing {} events...", a.events);
    let cfg = pool::PoolConfig::new(a.events, a.authors, kinds, a.seed);
    let events: Vec<Event> = pool::build_events(&cfg);

    let dir = std::env::temp_dir().join(format!(
        "nostr-bench-store-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0)
    ));
    std::fs::create_dir_all(&dir).map_err(|e| format!("create temp dir: {e}"))?;
    let store = Store::open(StoreConfig::new(&dir)).map_err(|e| format!("open store: {e}"))?;

    let mut inserted = 0u64;
    let mut duplicates = 0u64;
    let mut rejected = 0u64;
    let t0 = Instant::now();
    // Insert in batches of 4096 to exercise the batch path.
    for chunk in events.chunks(4096) {
        let outcomes = store
            .insert_batch(chunk)
            .map_err(|e| format!("insert_batch: {e}"))?;
        for o in outcomes {
            match o {
                InsertOutcome::Stored => inserted += 1,
                InsertOutcome::Duplicate => duplicates += 1,
                InsertOutcome::Rejected(_) => rejected += 1,
            }
        }
    }
    let ingest_s = t0.elapsed().as_secs_f64().max(1e-9);
    let _ = store.flush();
    let stats = store.stats();

    // Query benchmark: rotate through realistic filter shapes.
    let authors: Vec<String> = (0..a.authors.min(8))
        .map(|i| Keys::from_label(&pool::author_label(i)).public_key_hex())
        .collect();
    let specs = filters::rotating_filters(&authors, &filters::topic_values());
    let mut lat = Vec::with_capacity(a.queries);
    let mut returned = 0u64;
    let tq = Instant::now();
    for q in 0..a.queries {
        let spec = &specs[q % specs.len()];
        let filters: Vec<Filter> = vec![spec.filter.clone()];
        let t = Instant::now();
        let res = store.query(&filters).map_err(|e| format!("query: {e}"))?;
        lat.push(dur_us(t.elapsed()));
        returned += res.len() as u64;
    }
    let query_s = tq.elapsed().as_secs_f64().max(1e-9);
    let summary = summarize(lat);
    let ingest_eps = inserted as f64 / ingest_s;

    let _ = std::fs::remove_dir_all(&dir);

    Ok(StoreResult {
        events_requested: a.events,
        inserted,
        duplicates,
        rejected,
        ingest_s: f3(ingest_s).parse().unwrap_or(ingest_s),
        ingest_events_per_s: ingest_eps.round(),
        bytes_on_disk: stats.bytes_on_disk,
        segments: stats.segments,
        index_entries: stats.index_entries,
        queries_run: a.queries,
        query_events_returned: returned,
        query_latency: LatencyReport::from(&summary),
        query_events_per_s: (returned as f64 / query_s).round(),
        pass: inserted > 0 && ingest_eps > 0.0,
    })
}

// ---------------------------------------------------------------------------
// dump
// ---------------------------------------------------------------------------

fn run_dump(a: &DumpArgs) -> Result<(), String> {
    let kinds = parse_kinds(&a.kinds)?;
    let cfg = pool::PoolConfig::new(a.events, a.authors, kinds, a.seed);
    let events = pool::build_events(&cfg);
    use std::io::Write;
    let mut out: Box<dyn Write> = match &a.out {
        Some(path) => Box::new(std::io::BufWriter::new(
            std::fs::File::create(path).map_err(|e| format!("create {path}: {e}"))?,
        )),
        None => Box::new(std::io::BufWriter::new(std::io::stdout())),
    };
    for ev in &events {
        writeln!(out, "{}", ev.to_json()).map_err(|e| format!("write: {e}"))?;
    }
    out.flush().map_err(|e| format!("flush: {e}"))?;
    Ok(())
}

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    let code = match cli.cmd {
        Cmd::Smoke(a) => {
            let r = run_smoke(&a).await;
            if a.common.json {
                print_json(&r);
            } else {
                println!("smoke: {}", if r.pass { "PASS" } else { "FAIL" });
                println!("  url: {}", r.url);
                println!("  event_id: {}", r.event_id);
                println!("  ok_true: {}", r.ok_true);
                println!("  event_returned: {}", r.event_returned);
                println!("  eose: {}", r.eose);
                if let Some(e) = &r.error {
                    println!("  error: {e}");
                }
            }
            if r.pass {
                0
            } else {
                1
            }
        }
        Cmd::Load(a) => match run_load(&a).await {
            Ok(r) => {
                if a.common.json {
                    print_json(&r);
                } else {
                    print_table(
                        "nostr-bench load",
                        &[
                            ("url", r.url.clone()),
                            ("events requested", r.events_requested.to_string()),
                            ("connections", r.conns.to_string()),
                            ("authors", r.authors.to_string()),
                            ("kinds", format!("{:?}", r.kinds)),
                            ("pipeline depth", r.pipeline.to_string()),
                            ("OK true", r.ok_true.to_string()),
                            ("OK false", r.ok_false.to_string()),
                            ("errors", r.errors.to_string()),
                            ("elapsed s", f3(r.elapsed_s)),
                            ("events/s", format!("{:.0}", r.events_per_s)),
                            ("ack p50 ms", f3(r.ack_latency.p50_ms)),
                            ("ack p99 ms", f3(r.ack_latency.p99_ms)),
                            ("ack p999 ms", f3(r.ack_latency.p999_ms)),
                            ("ack max ms", f3(r.ack_latency.max_ms)),
                            ("sign s", f3(r.sign_seconds)),
                        ],
                    );
                }
                if r.pass {
                    0
                } else {
                    1
                }
            }
            Err(e) => {
                eprintln!("load: FAIL: {e}");
                1
            }
        },
        Cmd::Req(a) => match run_req(&a).await {
            Ok(r) => {
                if a.common.json {
                    print_json(&r);
                } else {
                    print_table(
                        "nostr-bench req",
                        &[
                            ("url", r.url.clone()),
                            ("queries requested", r.queries_requested.to_string()),
                            ("connections", r.conns.to_string()),
                            ("filter", r.filter.clone()),
                            ("completed", r.completed.to_string()),
                            ("events returned", r.events_returned.to_string()),
                            ("errors", r.errors.to_string()),
                            ("elapsed s", f3(r.elapsed_s)),
                            ("queries/s", format!("{:.0}", r.queries_per_s)),
                            ("events/s served", format!("{:.0}", r.events_per_s)),
                            ("eose p50 ms", f3(r.eose_latency.p50_ms)),
                            ("eose p99 ms", f3(r.eose_latency.p99_ms)),
                            ("eose p999 ms", f3(r.eose_latency.p999_ms)),
                            ("eose max ms", f3(r.eose_latency.max_ms)),
                        ],
                    );
                }
                if r.pass {
                    0
                } else {
                    1
                }
            }
            Err(e) => {
                eprintln!("req: FAIL: {e}");
                1
            }
        },
        Cmd::Mixed(a) => match run_mixed(&a).await {
            Ok(r) => {
                if a.common.json {
                    print_json(&r);
                } else {
                    print_table(
                        "nostr-bench mixed",
                        &[
                            ("url", r.url.clone()),
                            ("duration s", r.duration_s.to_string()),
                            ("connections", r.conns.to_string()),
                            ("publish ratio", f2(r.publish_ratio)),
                            ("publishes OK", r.publishes_ok.to_string()),
                            ("publishes failed", r.publishes_failed.to_string()),
                            ("queries completed", r.queries_completed.to_string()),
                            ("events returned", r.events_returned.to_string()),
                            ("errors", r.errors.to_string()),
                            ("publish events/s", format!("{:.0}", r.publish_eps)),
                            ("query qps", format!("{:.0}", r.query_qps)),
                            ("publish p50 ms", f3(r.publish_latency.p50_ms)),
                            ("publish p99 ms", f3(r.publish_latency.p99_ms)),
                            ("query p50 ms", f3(r.query_latency.p50_ms)),
                            ("query p99 ms", f3(r.query_latency.p99_ms)),
                        ],
                    );
                }
                if r.pass {
                    0
                } else {
                    1
                }
            }
            Err(e) => {
                eprintln!("mixed: FAIL: {e}");
                1
            }
        },
        Cmd::Dump(a) => match run_dump(&a) {
            Ok(()) => 0,
            Err(e) => {
                eprintln!("dump: FAIL: {e}");
                1
            }
        },
        Cmd::Store(a) => match run_store(&a) {
            Ok(r) => {
                if a.json {
                    print_json(&r);
                } else {
                    print_table(
                        "nostr-bench store",
                        &[
                            ("events requested", r.events_requested.to_string()),
                            ("inserted", r.inserted.to_string()),
                            ("duplicates", r.duplicates.to_string()),
                            ("rejected", r.rejected.to_string()),
                            ("ingest s", f3(r.ingest_s)),
                            ("ingest events/s", format!("{:.0}", r.ingest_events_per_s)),
                            ("bytes on disk", r.bytes_on_disk.to_string()),
                            ("segments", r.segments.to_string()),
                            ("index entries", r.index_entries.to_string()),
                            ("queries run", r.queries_run.to_string()),
                            ("query events returned", r.query_events_returned.to_string()),
                            ("query p50 ms", f3(r.query_latency.p50_ms)),
                            ("query p99 ms", f3(r.query_latency.p99_ms)),
                            ("query p999 ms", f3(r.query_latency.p999_ms)),
                            ("query events/s", format!("{:.0}", r.query_events_per_s)),
                        ],
                    );
                }
                if r.pass {
                    0
                } else {
                    1
                }
            }
            Err(e) => {
                eprintln!("store: FAIL: {e}");
                1
            }
        },
    };
    std::process::exit(code);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_kinds_ok() {
        assert_eq!(parse_kinds("1,7,30023").unwrap(), vec![1, 7, 30023]);
        assert_eq!(parse_kinds(" 1 , 7 ").unwrap(), vec![1, 7]);
    }

    #[test]
    fn parse_kinds_rejects_bad() {
        assert!(parse_kinds("").is_err());
        assert!(parse_kinds("abc").is_err());
        assert!(parse_kinds("1,-2").is_err());
    }

    #[test]
    fn cli_parses_smoke() {
        let cli =
            Cli::try_parse_from(["nostr-bench", "smoke", "--url", "ws://x:1", "--json"]).unwrap();
        match cli.cmd {
            Cmd::Smoke(a) => {
                assert_eq!(a.common.url, "ws://x:1");
                assert!(a.common.json);
            }
            _ => panic!("expected smoke"),
        }
    }

    #[test]
    fn cli_parses_load_defaults() {
        let cli = Cli::try_parse_from(["nostr-bench", "load"]).unwrap();
        match cli.cmd {
            Cmd::Load(a) => {
                assert_eq!(a.events, 100_000);
                assert_eq!(a.conns, 16);
                assert_eq!(a.pipeline, 64);
            }
            _ => panic!("expected load"),
        }
    }

    #[test]
    fn cli_parses_store() {
        let cli =
            Cli::try_parse_from(["nostr-bench", "store", "--events", "10", "--json"]).unwrap();
        match cli.cmd {
            Cmd::Store(a) => {
                assert_eq!(a.events, 10);
                assert!(a.json);
            }
            _ => panic!("expected store"),
        }
    }

    #[test]
    fn cli_rejects_unknown_subcommand() {
        assert!(Cli::try_parse_from(["nostr-bench", "nope"]).is_err());
    }
}
