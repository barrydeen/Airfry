//! Integration tests for the airfry-store engine.
//!
//! Covers: roundtrip insert/query for every filter type, newest-first ordering, per-filter limit,
//! dedup across filters, duplicate detection, batch insert, reopen-and-recover (including a
//! deliberately truncated tail record), delete/tombstone persistence, NIP-40 expiry, concurrent
//! insert+query stress, and a throughput smoke test.

use airfry_core::{Event, Filter, Keys};
use airfry_store::{FsyncPolicy, InsertOutcome, Store, StoreConfig};
use std::sync::Arc;

fn tmpdir(tag: &str) -> std::path::PathBuf {
    let mut p = std::env::temp_dir();
    p.push(format!(
        "airfry-store-it-{}-{}-{}",
        tag,
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&p).unwrap();
    p
}

fn open(dir: &std::path::Path) -> Store {
    let mut cfg = StoreConfig::new(dir);
    cfg.fsync = FsyncPolicy::Never;
    cfg.segment_bytes = 1 << 20;
    Store::open(cfg).expect("store opens")
}

fn ev(k: &Keys, ts: u64, kind: u32, tags: Vec<Vec<String>>, content: &str) -> Event {
    k.sign_event(ts, kind, tags, content)
}

fn filter(v: serde_json::Value) -> Filter {
    Filter::from_json_value(&v).expect("filter parses")
}

#[test]
fn roundtrip_every_filter_type() {
    let dir = tmpdir("filters");
    let s = open(&dir);
    let alice = Keys::from_label("it-alice");
    let bob = Keys::from_label("it-bob");
    let e1 = ev(
        &alice,
        1_700_000_100,
        1,
        vec![vec!["t".into(), "rust".into()]],
        "hello rust",
    );
    let e2 = ev(
        &bob,
        1_700_000_200,
        7,
        vec![vec!["e".into(), "abc".into()]],
        "reaction",
    );
    let e3 = ev(
        &alice,
        1_700_000_300,
        30023,
        vec![vec!["d".into(), "post-1".into()]],
        "long form",
    );
    for e in [&e1, &e2, &e3] {
        assert_eq!(s.insert(e).unwrap(), InsertOutcome::Stored);
    }
    // ids
    assert_eq!(
        s.query(&[filter(serde_json::json!({"ids":[hex::encode(e2.id)]}))])
            .unwrap()
            .len(),
        1
    );
    // authors
    let r = s
        .query(&[filter(
            serde_json::json!({"authors":[alice.public_key_hex()]}),
        )])
        .unwrap();
    assert_eq!(r.len(), 2);
    assert!(r.iter().all(|e| e.pubkey == alice.public_key));
    // kinds
    assert_eq!(
        s.query(&[filter(serde_json::json!({"kinds":[7]}))])
            .unwrap()
            .len(),
        1
    );
    // tags
    assert_eq!(
        s.query(&[filter(serde_json::json!({"#t":["rust"]}))])
            .unwrap()
            .len(),
        1
    );
    // time range
    let r = s
        .query(&[filter(
            serde_json::json!({"since":1_700_000_150,"until":1_700_000_250}),
        )])
        .unwrap();
    assert_eq!(r.len(), 1);
    assert_eq!(r[0].id, e2.id);
    // search substring
    let r = s
        .query(&[filter(serde_json::json!({"search":"RUST"}))])
        .unwrap();
    assert_eq!(r.len(), 1);
    assert_eq!(r[0].id, e1.id);
    // stats
    let st = s.stats();
    assert_eq!(st.events, 3);
    assert!(st.bytes_on_disk > 0);
    assert!(st.segments >= 1);
    assert!(st.index_entries >= 3);
}

#[test]
fn newest_first_and_limit() {
    let dir = tmpdir("order");
    let s = open(&dir);
    let k = Keys::from_label("it-order");
    for i in 0..20u64 {
        s.insert(&ev(&k, 1_700_000_000 + i, 1, vec![], &format!("e{i}")))
            .unwrap();
    }
    let r = s
        .query(&[filter(serde_json::json!({"authors":[k.public_key_hex()]}))])
        .unwrap();
    assert_eq!(r.len(), 20);
    assert!(r.windows(2).all(|w| w[0].created_at >= w[1].created_at));
    let r = s.query(&[filter(serde_json::json!({"limit":5}))]).unwrap();
    assert_eq!(r.len(), 5);
    assert_eq!(r[0].created_at, 1_700_000_019);
    assert!(r.windows(2).all(|w| w[0].created_at >= w[1].created_at));
}

#[test]
fn dedup_across_filters() {
    let dir = tmpdir("dedup");
    let s = open(&dir);
    let k1 = Keys::from_label("it-dedup-a");
    let k2 = Keys::from_label("it-dedup-b");
    for i in 0..10u64 {
        s.insert(&ev(
            &k1,
            1_700_000_000 + i,
            1,
            vec![vec!["t".into(), "x".into()]],
            "a",
        ))
        .unwrap();
        s.insert(&ev(
            &k2,
            1_700_000_000 + i,
            1,
            vec![vec!["t".into(), "x".into()]],
            "b",
        ))
        .unwrap();
    }
    let f1 = filter(serde_json::json!({"authors":[k1.public_key_hex()],"limit":5}));
    let f2 = filter(serde_json::json!({"authors":[k2.public_key_hex()],"limit":5}));
    let r = s.query(&[f1, f2]).unwrap();
    assert_eq!(r.len(), 10);
    let mut ids: Vec<_> = r.iter().map(|e| e.id).collect();
    ids.sort();
    ids.dedup();
    assert_eq!(ids.len(), 10);
    // identical filters dedup
    let f = filter(serde_json::json!({"#t":["x"],"limit":4}));
    let r = s.query(&[f.clone(), f]).unwrap();
    assert_eq!(r.len(), 4);
}

#[test]
fn duplicate_detection() {
    let dir = tmpdir("dup");
    let s = open(&dir);
    let k = Keys::from_label("it-dup");
    let e = ev(&k, 1_700_000_000, 1, vec![], "same");
    assert_eq!(s.insert(&e).unwrap(), InsertOutcome::Stored);
    assert_eq!(s.insert(&e).unwrap(), InsertOutcome::Duplicate);
    assert_eq!(s.stats().events, 1);
}

#[test]
fn batch_insert_and_duplicates_within_batch() {
    let dir = tmpdir("batch");
    let s = open(&dir);
    let k = Keys::from_label("it-batch");
    let evs: Vec<Event> = (0..100u64)
        .map(|i| {
            ev(
                &k,
                1_700_000_000 + i,
                1,
                vec![vec!["t".into(), "b".into()]],
                &format!("b{i}"),
            )
        })
        .collect();
    let outcomes = s.insert_batch(&evs).unwrap();
    assert!(outcomes.iter().all(|o| *o == InsertOutcome::Stored));
    assert_eq!(s.stats().events, 100);
    // duplicate within one batch
    let dup = vec![evs[0].clone(), evs[0].clone()];
    let outcomes = s.insert_batch(&dup).unwrap();
    assert_eq!(outcomes[0], InsertOutcome::Duplicate);
    assert_eq!(outcomes[1], InsertOutcome::Duplicate);
}

#[test]
fn reopen_recovers_and_truncated_tail_discarded() {
    let dir = tmpdir("reopen");
    let k = Keys::from_label("it-reopen");
    let evs: Vec<Event> = (0..30u64)
        .map(|i| {
            ev(
                &k,
                1_700_000_000 + i,
                1,
                vec![vec!["t".into(), "r".into()]],
                &format!("r{i}"),
            )
        })
        .collect();
    {
        let s = open(&dir);
        s.insert_batch(&evs).unwrap();
        s.flush().unwrap();
    }
    // Truncate the tail to simulate a torn write.
    let mut segs: Vec<_> = std::fs::read_dir(&dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.is_file())
        .collect();
    segs.sort();
    let last = segs.last().unwrap().clone();
    let len = std::fs::metadata(&last).unwrap().len();
    let f = std::fs::OpenOptions::new().write(true).open(&last).unwrap();
    f.set_len(len - 5).unwrap();
    drop(f);

    let s2 = open(&dir);
    let st = s2.stats();
    assert!(
        st.events >= 29,
        "recovery keeps all but at most the torn record, got {}",
        st.events
    );
    let r = s2
        .query(&[filter(serde_json::json!({"limit":100}))])
        .unwrap();
    assert_eq!(r.len() as u64, st.events);
    for e in &r {
        e.verify().expect("recovered event verifies");
    }
    // store still accepts writes after recovery
    let extra = ev(&k, 1_800_000_000, 1, vec![], "after recovery");
    assert_eq!(s2.insert(&extra).unwrap(), InsertOutcome::Stored);
}

#[test]
fn delete_tombstone_persists() {
    let dir = tmpdir("delete");
    let s = open(&dir);
    let k = Keys::from_label("it-del");
    let e = ev(&k, 1_700_000_000, 1, vec![], "to be deleted");
    s.insert(&e).unwrap();
    assert!(s.delete(&e.id, &k.public_key).unwrap());
    assert_eq!(
        s.query(&[filter(serde_json::json!({"ids":[hex::encode(e.id)]}))])
            .unwrap()
            .len(),
        0
    );
    assert!(!s.delete(&e.id, &k.public_key).unwrap());
    // wrong author cannot delete
    let other = Keys::from_label("it-del-other");
    let e2 = ev(&k, 1_700_000_001, 1, vec![], "keep");
    s.insert(&e2).unwrap();
    assert!(!s.delete(&e2.id, &other.public_key).unwrap());
    assert_eq!(s.stats().events, 1);
    // tombstone survives reopen
    s.flush().unwrap();
    drop(s);
    let s2 = open(&dir);
    assert_eq!(
        s2.query(&[filter(serde_json::json!({"ids":[hex::encode(e.id)]}))])
            .unwrap()
            .len(),
        0
    );
    assert_eq!(s2.stats().events, 1);
}

#[test]
fn expired_events_excluded() {
    let dir = tmpdir("expire");
    let s = open(&dir);
    let k = Keys::from_label("it-exp");
    let now = airfry_core::now_secs();
    let live = ev(
        &k,
        now,
        1,
        vec![vec!["expiration".into(), (now + 3600).to_string()]],
        "live",
    );
    let dead = ev(
        &k,
        now,
        1,
        vec![vec!["expiration".into(), (now - 10).to_string()]],
        "dead",
    );
    s.insert(&live).unwrap();
    s.insert(&dead).unwrap();
    let f = filter(serde_json::json!({"authors":[k.public_key_hex()]}));
    let r = s.query(&[f.clone()]).unwrap();
    assert_eq!(r.len(), 1);
    assert_eq!(r[0].id, live.id);
    assert_eq!(s.count(&[f]).unwrap(), 1);
}

#[test]
fn count_ignores_limit() {
    let dir = tmpdir("count");
    let s = open(&dir);
    let k = Keys::from_label("it-count");
    for i in 0..10u64 {
        s.insert(&ev(&k, 1_700_000_000 + i, 1, vec![], "x"))
            .unwrap();
    }
    let f = filter(serde_json::json!({"kinds":[1],"limit":3}));
    assert_eq!(
        s.count(&[f]).unwrap(),
        10,
        "COUNT must ignore the filter limit"
    );
}

#[test]
fn concurrent_insert_and_query_stress() {
    let dir = tmpdir("stress");
    let s = Arc::new(open(&dir));
    let writers = 8;
    let per_writer = 300u64;
    let mut handles = Vec::new();
    for w in 0..writers {
        let s = Arc::clone(&s);
        handles.push(std::thread::spawn(move || {
            let k = Keys::from_label(&format!("it-stress-{w}"));
            let mut evs = Vec::with_capacity(per_writer as usize);
            for i in 0..per_writer {
                evs.push(ev(
                    &k,
                    1_700_000_000 + i,
                    1,
                    vec![vec!["t".into(), "s".into()]],
                    "stress",
                ));
            }
            s.insert_batch(&evs).unwrap();
        }));
    }
    let done = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let reader = {
        let s = Arc::clone(&s);
        let done = Arc::clone(&done);
        std::thread::spawn(move || {
            let f = filter(serde_json::json!({"#t":["s"],"limit":100}));
            let mut seen = 0usize;
            let mut iters = 0usize;
            while !done.load(std::sync::atomic::Ordering::Relaxed) || iters < 50 {
                let r = s.query(&[f.clone()]).unwrap();
                assert!(r.len() <= 100);
                assert!(r.windows(2).all(|w| w[0].created_at >= w[1].created_at));
                seen = seen.max(r.len());
                iters += 1;
            }
            seen
        })
    };
    for h in handles {
        h.join().unwrap();
    }
    done.store(true, std::sync::atomic::Ordering::Relaxed);
    let seen = reader.join().unwrap();
    assert!(seen > 0);
    assert_eq!(s.stats().events, writers * per_writer);
}

#[test]
fn fsync_always_and_interval_policies() {
    for policy in [
        FsyncPolicy::Always,
        FsyncPolicy::Interval(std::time::Duration::from_millis(20)),
    ] {
        let dir = tmpdir("fsync");
        let mut cfg = StoreConfig::new(&dir);
        cfg.fsync = policy;
        cfg.segment_bytes = 1 << 20;
        let s = Store::open(cfg).unwrap();
        let k = Keys::from_label("it-fsync");
        for i in 0..10u64 {
            s.insert(&ev(&k, 1_700_000_000 + i, 1, vec![], "x"))
                .unwrap();
        }
        s.flush().unwrap();
        drop(s);
        let s2 = open(&dir);
        assert_eq!(s2.stats().events, 10);
    }
}

#[test]
fn segment_rotation() {
    let dir = tmpdir("rotate");
    let mut cfg = StoreConfig::new(&dir);
    cfg.fsync = FsyncPolicy::Never;
    cfg.segment_bytes = 4096; // tiny so we rotate quickly
    let s = Store::open(cfg).unwrap();
    let k = Keys::from_label("it-rotate");
    for i in 0..200u64 {
        s.insert(&ev(
            &k,
            1_700_000_000 + i,
            1,
            vec![],
            &format!("event number {i} with some content"),
        ))
        .unwrap();
    }
    s.flush().unwrap();
    let st = s.stats();
    assert!(
        st.segments > 1,
        "should have rotated into multiple segments, got {}",
        st.segments
    );
    drop(s);
    let s2 = open(&dir);
    assert_eq!(s2.stats().events, 200);
}

/// Debug-friendly throughput smoke test: asserts a modest rate that debug builds can hit.
#[test]
fn throughput_smoke() {
    let dir = tmpdir("throughput");
    let s = open(&dir);
    let k = Keys::from_label("it-throughput");
    let n = 20_000u64;
    let evs: Vec<Event> = (0..n)
        .map(|i| {
            ev(
                &k,
                1_700_000_000 + i,
                1,
                vec![vec!["t".into(), "tp".into()]],
                &format!("tp{i}"),
            )
        })
        .collect();
    let t = std::time::Instant::now();
    for chunk in evs.chunks(1000) {
        s.insert_batch(chunk).unwrap();
    }
    let elapsed = t.elapsed();
    let eps = n as f64 / elapsed.as_secs_f64();
    println!("debug throughput: {:.0} events/s", eps);
    // Debug builds are ~10x slower than release; require at least 20k/s here.
    assert!(eps > 20_000.0, "ingest too slow: {:.0} events/s", eps);
    assert_eq!(s.stats().events, n);
}

/// Release-only throughput assertion: > 50k events/s. Run with `--release --ignored`.
#[test]
#[ignore]
fn throughput_release() {
    let dir = tmpdir("throughput-rel");
    let s = open(&dir);
    let k = Keys::from_label("it-throughput-rel");
    let n = 200_000u64;
    let evs: Vec<Event> = (0..n)
        .map(|i| {
            ev(
                &k,
                1_700_000_000 + i,
                1,
                vec![vec!["t".into(), "tp".into()]],
                &format!("tp{i}"),
            )
        })
        .collect();
    let t = std::time::Instant::now();
    for chunk in evs.chunks(1000) {
        s.insert_batch(chunk).unwrap();
    }
    let eps = n as f64 / t.elapsed().as_secs_f64();
    println!("release throughput: {:.0} events/s", eps);
    assert!(eps > 50_000.0, "ingest too slow: {:.0} events/s", eps);
}
