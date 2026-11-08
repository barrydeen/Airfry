//! Raw store ingest and query latency benchmark.
//!
//! Run with: `cargo run --release -p airfry-store --bin store_bench`
//!
//! Prints measured ingest events/s and query p50/p99/p999 latencies over an in-memory corpus.
//! Events are pre-signed once (outside the timed region) so the numbers reflect the storage
//! engine, not schnorr signing.

use airfry_core::{Event, Filter, Keys};
use airfry_store::{FsyncPolicy, Store, StoreConfig};
use std::sync::Arc;
use std::time::Instant;

fn tmpdir() -> std::path::PathBuf {
    let mut p = std::env::temp_dir();
    p.push(format!(
        "airfry-store-bench-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&p).unwrap();
    p
}

fn pct(sorted: &[u128], p: f64) -> u128 {
    if sorted.is_empty() {
        return 0;
    }
    let idx = ((sorted.len() as f64 - 1.0) * p).round() as usize;
    sorted[idx.min(sorted.len() - 1)]
}

fn main() {
    let n: usize = std::env::args()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(1_000_000);
    let dir = tmpdir();
    let mut cfg = StoreConfig::new(&dir);
    cfg.fsync = FsyncPolicy::Never;
    cfg.segment_bytes = 256 << 20;
    let store = Arc::new(Store::open(cfg).expect("store opens"));

    println!(
        "airfry-store bench: corpus = {n} events, dir = {}",
        dir.display()
    );

    // Pre-sign the corpus outside the timed region.
    let keys: Vec<Keys> = (0..64)
        .map(|i| Keys::from_label(&format!("bench-{i}")))
        .collect();
    let t0 = Instant::now();
    let mut corpus: Vec<Event> = Vec::with_capacity(n);
    for i in 0..n {
        let k = &keys[i % keys.len()];
        let ts = 1_700_000_000 + (i as u64 % 100_000);
        let tags = vec![
            vec!["t".to_string(), format!("topic{}", i % 16)],
            vec!["p".to_string(), keys[(i + 1) % keys.len()].public_key_hex()],
        ];
        corpus.push(k.sign_event(ts, 1, tags, &format!("bench event {i}")));
    }
    println!("pre-signed {n} events in {:?}", t0.elapsed());

    // Ingest: batch inserts of 1000 events.
    let batch = 1000usize;
    let t = Instant::now();
    let mut stored = 0usize;
    for chunk in corpus.chunks(batch) {
        let outcomes = store.insert_batch(chunk).expect("batch insert");
        stored += outcomes
            .iter()
            .filter(|o| **o == airfry_store::InsertOutcome::Stored)
            .count();
    }
    let ingest_elapsed = t.elapsed();
    let eps = stored as f64 / ingest_elapsed.as_secs_f64();
    println!(
        "ingest: {stored} events in {:?} => {:.0} events/s",
        ingest_elapsed, eps
    );
    let st = store.stats();
    println!(
        "stats: events={} bytes_on_disk={} segments={} index_entries={}",
        st.events, st.bytes_on_disk, st.segments, st.index_entries
    );

    // Query latency: a mix of indexed filter shapes.
    let filters: Vec<Filter> = vec![
        Filter::from_json_value(&serde_json::json!({"kinds":[1],"limit":50})).unwrap(),
        Filter::from_json_value(
            &serde_json::json!({"authors":[keys[0].public_key_hex()],"limit":50}),
        )
        .unwrap(),
        Filter::from_json_value(&serde_json::json!({"#t":["topic3"],"limit":50})).unwrap(),
        Filter::from_json_value(&serde_json::json!({"kinds":[1],"since":1_700_050_000,"limit":50}))
            .unwrap(),
        Filter::from_json_value(
            &serde_json::json!({"#t":["topic7"],"authors":[keys[5].public_key_hex()],"limit":50}),
        )
        .unwrap(),
    ];
    let iters = 2000usize;
    let mut lat: Vec<u128> = Vec::with_capacity(iters);
    let mut total_results = 0usize;
    for i in 0..iters {
        let f = &filters[i % filters.len()];
        let t = Instant::now();
        let r = store.query(std::slice::from_ref(f)).expect("query");
        lat.push(t.elapsed().as_nanos());
        total_results += r.len();
    }
    lat.sort_unstable();
    println!(
        "query: {} iters, results={}, p50={}us p99={}us p999={}us max={}us",
        iters,
        total_results,
        pct(&lat, 0.50) / 1000,
        pct(&lat, 0.99) / 1000,
        pct(&lat, 0.999) / 1000,
        lat.last().copied().unwrap_or(0) / 1000
    );
    // Per-filter breakdown.
    for (fi, f) in filters.iter().enumerate() {
        let mut l: Vec<u128> = Vec::with_capacity(200);
        for _ in 0..200 {
            let t = Instant::now();
            let _ = store.query(std::slice::from_ref(f)).expect("query");
            l.push(t.elapsed().as_nanos());
        }
        l.sort_unstable();
        println!(
            "  filter[{}]: p50={}us p99={}us max={}us",
            fi,
            pct(&l, 0.5) / 1000,
            pct(&l, 0.99) / 1000,
            l.last().copied().unwrap_or(0) / 1000
        );
    }

    // COUNT latency.
    let cf = Filter::from_json_value(&serde_json::json!({"kinds":[1]})).unwrap();
    let t = Instant::now();
    let c = store.count(std::slice::from_ref(&cf)).expect("count");
    println!("count(kinds=[1]) = {c} in {:?}", t.elapsed());

    let _ = std::fs::remove_dir_all(&dir);
}
