//! Independent conformance suite for the documented `nostr-store` API.
//!
//! These tests are written by the Director against ARCHITECTURE.md and must pass against the
//! real storage engine. They deliberately avoid touching storage internals so they stay valid
//! as the engine evolves.

use nostr_core::{Event, Filter, Keys};
use nostr_store::{FsyncPolicy, InsertOutcome, Store, StoreConfig};
use std::sync::Arc;

fn tmpdir(tag: &str) -> std::path::PathBuf {
    let mut p = std::env::temp_dir();
    let uniq = format!(
        "nostr-store-conf-{}-{}-{}",
        tag,
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    p.push(uniq);
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

#[test]
fn insert_query_roundtrip_all_filter_shapes() {
    let dir = tmpdir("filters");
    let s = open(&dir);
    let alice = Keys::from_label("conf-alice");
    let bob = Keys::from_label("conf-bob");

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

    // by id
    let f = Filter::from_json_value(&serde_json::json!({"ids":[hex::encode(e2.id)]})).unwrap();
    let r = s.query(&[f]).unwrap();
    assert_eq!(r.len(), 1);
    assert_eq!(r[0].id, e2.id);

    // by author
    let f =
        Filter::from_json_value(&serde_json::json!({"authors":[alice.public_key_hex()]})).unwrap();
    let r = s.query(&[f]).unwrap();
    assert_eq!(r.len(), 2);
    assert!(r.iter().all(|e| e.pubkey == alice.public_key));

    // by kind
    let f = Filter::from_json_value(&serde_json::json!({"kinds":[7]})).unwrap();
    assert_eq!(s.query(&[f]).unwrap().len(), 1);

    // by tag
    let f = Filter::from_json_value(&serde_json::json!({"#t":["rust"]})).unwrap();
    assert_eq!(s.query(&[f]).unwrap().len(), 1);

    // time range
    let f =
        Filter::from_json_value(&serde_json::json!({"since":1_700_000_150,"until":1_700_000_250}))
            .unwrap();
    let r = s.query(&[f]).unwrap();
    assert_eq!(r.len(), 1);
    assert_eq!(r[0].id, e2.id);

    // newest-first ordering
    let f =
        Filter::from_json_value(&serde_json::json!({"authors":[alice.public_key_hex()]})).unwrap();
    let r = s.query(&[f]).unwrap();
    assert!(r[0].created_at >= r[1].created_at);

    // limit
    let f = Filter::from_json_value(&serde_json::json!({"limit":1})).unwrap();
    let r = s.query(&[f]).unwrap();
    assert_eq!(r.len(), 1);
    assert_eq!(r[0].created_at, 1_700_000_300);

    // count
    let f = Filter::from_json_value(&serde_json::json!({"kinds":[1,7,30023]})).unwrap();
    assert_eq!(s.count(&[f]).unwrap(), 3);

    // stats
    let st = s.stats();
    assert_eq!(st.events, 3);
    assert!(st.bytes_on_disk > 0);
    assert!(st.segments >= 1);
    assert!(st.index_entries >= 3);
}

#[test]
fn dedup_across_filters_and_per_filter_limit() {
    let dir = tmpdir("dedup");
    let s = open(&dir);
    let k1 = Keys::from_label("conf-dedup-a");
    let k2 = Keys::from_label("conf-dedup-b");
    for i in 0..10u64 {
        let e = ev(
            &k1,
            1_700_000_000 + i,
            1,
            vec![vec!["t".into(), "x".into()]],
            &format!("a{i}"),
        );
        s.insert(&e).unwrap();
        let e = ev(
            &k2,
            1_700_000_000 + i,
            1,
            vec![vec!["t".into(), "x".into()]],
            &format!("b{i}"),
        );
        s.insert(&e).unwrap();
    }

    // Two disjoint filters, each with its own limit: the result is the union, deduplicated
    // by id, so 5 + 5 = 10 distinct events.
    let f1 =
        Filter::from_json_value(&serde_json::json!({"authors":[k1.public_key_hex()],"limit":5}))
            .unwrap();
    let f2 =
        Filter::from_json_value(&serde_json::json!({"authors":[k2.public_key_hex()],"limit":5}))
            .unwrap();
    let r = s.query(&[f1, f2]).unwrap();
    assert_eq!(
        r.len(),
        10,
        "per-filter limit must apply to each filter independently"
    );
    let mut ids: Vec<_> = r.iter().map(|e| e.id).collect();
    ids.sort();
    ids.dedup();
    assert_eq!(ids.len(), 10, "results must be unique across filters");
    assert!(r
        .iter()
        .all(|e| e.pubkey == k1.public_key || e.pubkey == k2.public_key));

    // Overlapping filters must not duplicate events: the same filter twice yields one copy.
    let f = Filter::from_json_value(&serde_json::json!({"#t":["x"],"limit":4})).unwrap();
    let r = s.query(&[f.clone(), f.clone()]).unwrap();
    assert_eq!(
        r.len(),
        4,
        "identical filters must dedup to a single result set"
    );
    let mut ids: Vec<_> = r.iter().map(|e| e.id).collect();
    ids.sort();
    ids.dedup();
    assert_eq!(ids.len(), 4);

    // A filter that is a strict subset of another must not add duplicates.
    let broad = Filter::from_json_value(&serde_json::json!({"#t":["x"],"limit":10})).unwrap();
    let narrow =
        Filter::from_json_value(&serde_json::json!({"authors":[k1.public_key_hex()],"limit":3}))
            .unwrap();
    let r = s.query(&[broad, narrow]).unwrap();
    let mut ids: Vec<_> = r.iter().map(|e| e.id).collect();
    ids.sort();
    ids.dedup();
    assert_eq!(
        ids.len(),
        r.len(),
        "overlapping filters must be deduplicated by id"
    );
    assert_eq!(
        r.len(),
        10,
        "the broad filter's 10 events subsume the narrow filter's 3"
    );
}

#[test]
fn duplicates_are_rejected() {
    let dir = tmpdir("dup");
    let s = open(&dir);
    let k = Keys::from_label("conf-dup");
    let e = ev(&k, 1_700_000_000, 1, vec![], "same");
    assert_eq!(s.insert(&e).unwrap(), InsertOutcome::Stored);
    assert_eq!(s.insert(&e).unwrap(), InsertOutcome::Duplicate);
    assert_eq!(s.stats().events, 1);
}

#[test]
fn batch_insert_matches_single_insert() {
    let dir = tmpdir("batch");
    let s = open(&dir);
    let k = Keys::from_label("conf-batch");
    let evs: Vec<Event> = (0..200u64)
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
    assert_eq!(outcomes.len(), 200);
    assert!(outcomes.iter().all(|o| *o == InsertOutcome::Stored));
    assert_eq!(s.stats().events, 200);
    let f = Filter::from_json_value(&serde_json::json!({"#t":["b"],"limit":500})).unwrap();
    assert_eq!(s.query(&[f]).unwrap().len(), 200);
    // re-inserting the same batch is all duplicates
    let outcomes = s.insert_batch(&evs).unwrap();
    assert!(outcomes.iter().all(|o| *o == InsertOutcome::Duplicate));
}

#[test]
fn reopen_recovers_all_events() {
    let dir = tmpdir("reopen");
    let k = Keys::from_label("conf-reopen");
    let evs: Vec<Event> = (0..50u64)
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
    let s2 = open(&dir);
    assert_eq!(s2.stats().events, 50);
    let f = Filter::from_json_value(&serde_json::json!({"#t":["r"],"limit":100})).unwrap();
    let r = s2.query(&[f]).unwrap();
    assert_eq!(r.len(), 50);
    for e in &r {
        e.verify().expect("recovered event must still verify");
    }
    // duplicates still detected after reopen
    assert_eq!(s2.insert(&evs[0]).unwrap(), InsertOutcome::Duplicate);
}

#[test]
fn truncated_tail_is_discarded_not_fatal() {
    let dir = tmpdir("truncate");
    let k = Keys::from_label("conf-trunc");
    let evs: Vec<Event> = (0..20u64)
        .map(|i| ev(&k, 1_700_000_000 + i, 1, vec![], &format!("t{i}")))
        .collect();
    {
        let s = open(&dir);
        s.insert_batch(&evs).unwrap();
        s.flush().unwrap();
    }
    // Chop the last few bytes off the newest segment to simulate a torn write.
    let mut segs: Vec<_> = std::fs::read_dir(&dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.is_file())
        .collect();
    segs.sort();
    let last = segs.last().expect("at least one segment file").clone();
    let len = std::fs::metadata(&last).unwrap().len();
    assert!(len > 8, "segment should have content");
    let f = std::fs::OpenOptions::new().write(true).open(&last).unwrap();
    f.set_len(len - 5).unwrap();
    drop(f);

    let s2 = open(&dir);
    let st = s2.stats();
    assert!(
        st.events >= 19,
        "recovery should keep all but at most the torn record, got {}",
        st.events
    );
    let f = Filter::from_json_value(&serde_json::json!({"limit":100})).unwrap();
    let r = s2.query(&[f]).unwrap();
    assert_eq!(r.len() as u64, st.events);
    for e in &r {
        e.verify().expect("recovered event must verify");
    }
    // store must still accept writes after recovery
    let extra = ev(&k, 1_800_000_000, 1, vec![], "after recovery");
    assert_eq!(s2.insert(&extra).unwrap(), InsertOutcome::Stored);
}

#[test]
fn delete_removes_from_queries() {
    let dir = tmpdir("delete");
    let s = open(&dir);
    let k = Keys::from_label("conf-del");
    let e = ev(&k, 1_700_000_000, 1, vec![], "to be deleted");
    s.insert(&e).unwrap();
    assert!(s.delete(&e.id, &k.public_key).unwrap());
    let f = Filter::from_json_value(&serde_json::json!({"ids":[hex::encode(e.id)]})).unwrap();
    assert_eq!(s.query(std::slice::from_ref(&f)).unwrap().len(), 0);
    assert_eq!(s.count(&[f]).unwrap(), 0);
    // deleting again is a no-op
    assert!(!s.delete(&e.id, &k.public_key).unwrap());
    // tombstone survives reopen
    s.flush().unwrap();
    drop(s);
    let s2 = open(&dir);
    let f = Filter::from_json_value(&serde_json::json!({"ids":[hex::encode(e.id)]})).unwrap();
    assert_eq!(s2.query(&[f]).unwrap().len(), 0);
}

#[test]
fn concurrent_insert_and_query_stress() {
    let dir = tmpdir("stress");
    let s = Arc::new(open(&dir));
    let writers = 8;
    let per_writer = 500u64;
    let mut handles = Vec::new();
    for w in 0..writers {
        let s = Arc::clone(&s);
        handles.push(std::thread::spawn(move || {
            let k = Keys::from_label(&format!("conf-stress-{w}"));
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
    // readers hammer the store while writers run. The reader keeps querying until the
    // writers are done so the two genuinely overlap instead of racing ahead of ingest.
    let writers_done = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let reader = {
        let s = Arc::clone(&s);
        let writers_done = Arc::clone(&writers_done);
        std::thread::spawn(move || {
            let f = Filter::from_json_value(&serde_json::json!({"#t":["s"],"limit":100})).unwrap();
            let mut seen = 0usize;
            let mut iterations = 0usize;
            while !writers_done.load(std::sync::atomic::Ordering::Relaxed) || iterations < 50 {
                let r = s.query(std::slice::from_ref(&f)).unwrap();
                assert!(
                    r.len() <= 100,
                    "limit must be honoured under concurrent ingest"
                );
                assert!(
                    r.windows(2).all(|w| w[0].created_at >= w[1].created_at),
                    "results must stay newest-first under concurrent ingest"
                );
                seen = seen.max(r.len());
                iterations += 1;
            }
            seen
        })
    };
    for h in handles {
        h.join().unwrap();
    }
    writers_done.store(true, std::sync::atomic::Ordering::Relaxed);
    let seen = reader.join().unwrap();
    assert!(seen > 0, "reader should observe events while writers run");
    assert_eq!(s.stats().events, writers * per_writer);
}

#[test]
fn expired_events_are_not_returned() {
    let dir = tmpdir("expire");
    let s = open(&dir);
    let k = Keys::from_label("conf-exp");
    let now = nostr_core::now_secs();
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
    let f = Filter::from_json_value(&serde_json::json!({"authors":[k.public_key_hex()]})).unwrap();
    let r = s.query(&[f]).unwrap();
    assert_eq!(r.len(), 1, "expired event must be filtered out");
    assert_eq!(r[0].id, live.id);
}

#[test]
fn empty_filter_returns_newest_first() {
    let dir = tmpdir("empty");
    let s = open(&dir);
    let k = Keys::from_label("conf-empty");
    for i in 0..5u64 {
        s.insert(&ev(&k, 1_700_000_000 + i, 1, vec![], "e"))
            .unwrap();
    }
    let f = Filter::from_json_value(&serde_json::json!({"limit":3})).unwrap();
    let r = s.query(&[f]).unwrap();
    assert_eq!(r.len(), 3);
    assert!(r.windows(2).all(|w| w[0].created_at >= w[1].created_at));
    assert_eq!(r[0].created_at, 1_700_000_004);
}
