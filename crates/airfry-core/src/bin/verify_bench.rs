//! Micro-benchmark for the event verification hot path.
//!
//! Measures single-threaded and multi-threaded schnorr verification throughput so the relay
//! team can size the verifier pool. Run with:
//! `cargo run --release -p airfry-core --bin verify_bench`

use airfry_core::{Event, Keys};
use std::time::Instant;

fn make_events(n: usize) -> Vec<Event> {
    let keys = Keys::from_label("verify-bench");
    let now = airfry_core::now_secs();
    (0..n)
        .map(|i| {
            keys.sign_event(
                now - (i as u64 % 600),
                1,
                vec![
                    vec!["t".into(), "nostr".into()],
                    vec!["p".into(), "ab".repeat(32)],
                ],
                "benchmark event content that is roughly two hundred bytes long so that the \
                 hashing and parsing costs are representative of real traffic on a relay",
            )
        })
        .collect()
}

fn main() {
    let n: usize = std::env::args()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(20_000);
    let threads: usize = std::env::args()
        .nth(2)
        .and_then(|s| s.parse().ok())
        .unwrap_or(num_cpus());

    let events = make_events(n);

    // Warm up.
    for e in events.iter().take(200) {
        e.verify().unwrap();
    }

    let t0 = Instant::now();
    for e in &events {
        e.verify().unwrap();
    }
    let single = t0.elapsed();
    println!(
        "single-thread verify: {} events in {:?} => {:.0} events/s",
        n,
        single,
        n as f64 / single.as_secs_f64()
    );

    let t0 = Instant::now();
    let per = n / threads.max(1);
    std::thread::scope(|s| {
        for chunk in events.chunks(per.max(1)) {
            s.spawn(move || {
                for e in chunk {
                    e.verify().unwrap();
                }
            });
        }
    });
    let multi = t0.elapsed();
    println!(
        "{} threads verify: {} events in {:?} => {:.0} events/s",
        threads,
        n,
        multi,
        n as f64 / multi.as_secs_f64()
    );

    let t0 = Instant::now();
    let mut bytes = 0usize;
    for e in &events {
        bytes += e.to_json().len();
    }
    let ser = t0.elapsed();
    println!(
        "json serialize: {} events in {:?} => {:.0} events/s ({} bytes)",
        n,
        ser,
        n as f64 / ser.as_secs_f64(),
        bytes
    );

    let frames: Vec<String> = events
        .iter()
        .map(|e| format!("[\"EVENT\",{}]", e.to_json()))
        .collect();
    let t0 = Instant::now();
    let mut ok = 0usize;
    for f in &frames {
        if let Ok(airfry_core::ClientMessage::Event(_)) = airfry_core::parse_client_message(f) {
            ok += 1;
        }
    }
    let parse = t0.elapsed();
    println!(
        "parse EVENT frame: {} frames in {:?} => {:.0} frames/s (ok={})",
        n,
        parse,
        n as f64 / parse.as_secs_f64(),
        ok
    );
}

fn num_cpus() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1)
}
