//! Adversarial and edge-case tests against the documented `nostr-store` API.
//!
//! These complement `store_conformance.rs`: where that file checks the happy path of every
//! documented operation, this file throws hostile and degenerate inputs at the engine and asserts
//! that it stays correct, bounded and crash-free.

use nostr_core::{Event, Filter, Keys};
use nostr_store::{FsyncPolicy, InsertOutcome, Store, StoreConfig};
use std::sync::Arc;

fn tmpdir(tag: &str) -> std::path::PathBuf {
    let mut p = std::env::temp_dir();
    p.push(format!(
        "nostr-store-adv-{}-{}-{}",
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
fn duplicate_ids_inside_one_batch_are_deduplicated() {
    let dir = tmpdir("batch-dup");
    let s = open(&dir);
    let k = Keys::from_label("adv-batch-dup");
    let e = ev(&k, 1_700_000_000, 1, vec![], "same event");
    let batch = vec![e.clone(), e.clone(), e.clone()];
    let outcomes = s.insert_batch(&batch).expect("batch insert");
    assert_eq!(outcomes.len(), 3);
    assert_eq!(outcomes[0], InsertOutcome::Stored);
    assert_eq!(outcomes[1], InsertOutcome::Duplicate);
    assert_eq!(outcomes[2], InsertOutcome::Duplicate);
    assert_eq!(
        s.stats().events,
        1,
        "duplicate ids must not be stored twice"
    );
    let r = s
        .query(&[filter(serde_json::json!({"ids": [hex::encode(e.id)]}))])
        .unwrap();
    assert_eq!(r.len(), 1);
}

#[test]
fn limit_zero_returns_nothing() {
    let dir = tmpdir("limit0");
    let s = open(&dir);
    let k = Keys::from_label("adv-limit0");
    for i in 0..10u64 {
        s.insert(&ev(&k, 1_700_000_000 + i, 1, vec![], "x"))
            .unwrap();
    }
    let f = filter(serde_json::json!({"limit": 0}));
    assert_eq!(f.limit, Some(0));
    assert!(
        s.query(std::slice::from_ref(&f)).unwrap().is_empty(),
        "limit 0 must return no events"
    );
    assert_eq!(s.count(&[f]).unwrap(), 0);
}

#[test]
fn since_after_until_returns_nothing() {
    let dir = tmpdir("since-until");
    let s = open(&dir);
    let k = Keys::from_label("adv-range");
    for i in 0..5u64 {
        s.insert(&ev(&k, 1_700_000_000 + i, 1, vec![], "x"))
            .unwrap();
    }
    let f = filter(serde_json::json!({"since": 1_700_000_100, "until": 1_700_000_000}));
    assert!(
        s.query(std::slice::from_ref(&f)).unwrap().is_empty(),
        "inverted range must match nothing"
    );
    assert_eq!(s.count(&[f]).unwrap(), 0);
    // boundary: since == until is an inclusive single-second window
    let f = filter(serde_json::json!({"since": 1_700_000_002, "until": 1_700_000_002}));
    let r = s.query(&[f]).unwrap();
    assert_eq!(r.len(), 1);
    assert_eq!(r[0].created_at, 1_700_000_002);
}

#[test]
fn unknown_and_extreme_kinds_are_stored_and_queryable() {
    let dir = tmpdir("kinds");
    let s = open(&dir);
    let k = Keys::from_label("adv-kinds");
    let kinds = [0u32, 1, 4, 7, 9735, 30_023, 65_535, u32::MAX];
    for (i, kind) in kinds.iter().enumerate() {
        let e = ev(
            &k,
            1_700_000_000 + i as u64,
            *kind,
            vec![],
            &format!("kind {kind}"),
        );
        assert_eq!(s.insert(&e).unwrap(), InsertOutcome::Stored);
    }
    for kind in kinds {
        let f = filter(serde_json::json!({"kinds": [kind]}));
        let r = s.query(&[f]).unwrap();
        assert_eq!(r.len(), 1, "kind {kind} should be queryable");
        assert_eq!(r[0].kind, kind);
    }
    // a kind that was never inserted
    let f = filter(serde_json::json!({"kinds": [1234]}));
    assert!(s.query(&[f]).unwrap().is_empty());
}

#[test]
fn multi_value_tag_filters_match_any_value() {
    let dir = tmpdir("tags");
    let s = open(&dir);
    let k = Keys::from_label("adv-tags");
    let a = ev(
        &k,
        1_700_000_001,
        1,
        vec![vec!["t".into(), "alpha".into()]],
        "a",
    );
    let b = ev(
        &k,
        1_700_000_002,
        1,
        vec![vec!["t".into(), "beta".into()]],
        "b",
    );
    let c = ev(
        &k,
        1_700_000_003,
        1,
        vec![vec!["t".into(), "gamma".into()]],
        "c",
    );
    for e in [&a, &b, &c] {
        s.insert(e).unwrap();
    }
    let f = filter(serde_json::json!({"#t": ["alpha", "beta"]}));
    let r = s.query(&[f]).unwrap();
    assert_eq!(r.len(), 2, "multi-value tag filter must match either value");
    let ids: Vec<_> = r.iter().map(|e| e.id).collect();
    assert!(ids.contains(&a.id) && ids.contains(&b.id));
    assert!(!ids.contains(&c.id));

    // multiple distinct tag constraints must all be satisfied (AND semantics)
    let d = ev(
        &k,
        1_700_000_004,
        1,
        vec![
            vec!["t".into(), "alpha".into()],
            vec!["p".into(), "someone".into()],
        ],
        "d",
    );
    s.insert(&d).unwrap();
    let f = filter(serde_json::json!({"#t": ["alpha"], "#p": ["someone"]}));
    let r = s.query(&[f]).unwrap();
    assert_eq!(r.len(), 1);
    assert_eq!(r[0].id, d.id);

    // tag filter for a value nobody used
    let f = filter(serde_json::json!({"#t": ["nope"]}));
    assert!(s.query(&[f]).unwrap().is_empty());
}

#[test]
fn delete_of_nonexistent_id_is_a_noop() {
    let dir = tmpdir("del-missing");
    let s = open(&dir);
    let k = Keys::from_label("adv-del");
    let e = ev(&k, 1_700_000_000, 1, vec![], "present");
    s.insert(&e).unwrap();
    let missing = [0xab; 32];
    assert!(
        !s.delete(&missing, &k.public_key).unwrap(),
        "deleting an unknown id returns false"
    );
    assert_eq!(s.stats().events, 1, "no-op delete must not remove anything");
    // deleting with the wrong author must not remove the event either
    let other = Keys::from_label("adv-del-other");
    assert!(!s.delete(&e.id, &other.public_key).unwrap());
    assert_eq!(s.stats().events, 1);
    // the rightful author can delete it
    assert!(s.delete(&e.id, &k.public_key).unwrap());
    assert_eq!(s.stats().events, 0);
}

#[test]
fn expired_events_are_excluded_from_query_and_count() {
    let dir = tmpdir("expired");
    let s = open(&dir);
    let k = Keys::from_label("adv-expired");
    let now = nostr_core::now_secs();
    let live = ev(
        &k,
        now,
        1,
        vec![vec!["expiration".into(), (now + 600).to_string()]],
        "live",
    );
    let dead = ev(
        &k,
        now,
        1,
        vec![vec!["expiration".into(), (now - 600).to_string()]],
        "dead",
    );
    let no_exp = ev(&k, now, 1, vec![], "no expiration tag");
    for e in [&live, &dead, &no_exp] {
        s.insert(e).unwrap();
    }
    let f = filter(serde_json::json!({"authors": [k.public_key_hex()]}));
    let r = s.query(std::slice::from_ref(&f)).unwrap();
    assert_eq!(r.len(), 2, "expired event must be filtered out");
    assert!(r.iter().all(|e| e.id != dead.id));
    assert_eq!(s.count(&[f]).unwrap(), 2);
    // an explicit id lookup of an expired event must also come back empty
    let f = filter(serde_json::json!({"ids": [hex::encode(dead.id)]}));
    assert!(s.query(&[f]).unwrap().is_empty());
    // a malformed expiration tag must not crash the query path
    let weird = ev(
        &k,
        now,
        1,
        vec![vec!["expiration".into(), "not-a-number".into()]],
        "weird",
    );
    s.insert(&weird).unwrap();
    let f = filter(serde_json::json!({"authors": [k.public_key_hex()]}));
    let r = s.query(&[f]).unwrap();
    assert!(
        r.iter().any(|e| e.id == weird.id),
        "unparsable expiration must be treated as absent"
    );
}

#[test]
fn concurrent_queries_during_heavy_ingest_stay_consistent() {
    let dir = tmpdir("heavy");
    let s = Arc::new(open(&dir));
    let writers = 6usize;
    let per_writer = 400u64;

    let mut writer_handles = Vec::new();
    for w in 0..writers {
        let s = Arc::clone(&s);
        writer_handles.push(std::thread::spawn(move || {
            let k = Keys::from_label(&format!("adv-heavy-{w}"));
            for chunk in 0..8u64 {
                let evs: Vec<Event> = (0..(per_writer / 8))
                    .map(|i| {
                        ev(
                            &k,
                            1_700_000_000 + chunk * 100 + i,
                            1,
                            vec![vec!["t".into(), "heavy".into()]],
                            &format!("w{w}-c{chunk}-{i}"),
                        )
                    })
                    .collect();
                s.insert_batch(&evs).expect("batch insert under load");
            }
        }));
    }

    let writers_done = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let mut reader_handles = Vec::new();
    for _ in 0..4 {
        let s = Arc::clone(&s);
        let writers_done = Arc::clone(&writers_done);
        reader_handles.push(std::thread::spawn(move || {
            let f = filter(serde_json::json!({"#t": ["heavy"], "limit": 50}));
            let mut max_seen = 0usize;
            let mut iterations = 0usize;
            // keep querying until the writers are finished, so the readers genuinely overlap
            // with ingest instead of racing ahead of it
            while !writers_done.load(std::sync::atomic::Ordering::Relaxed) || iterations < 50 {
                let res = s.query(std::slice::from_ref(&f)).expect("query under load");
                assert!(
                    res.len() <= 50,
                    "limit must be honoured under concurrent ingest"
                );
                assert!(
                    res.windows(2).all(|w| w[0].created_at >= w[1].created_at),
                    "results must stay newest-first under concurrent ingest"
                );
                for e in &res {
                    assert_eq!(e.kind, 1);
                }
                max_seen = max_seen.max(res.len());
                iterations += 1;
            }
            max_seen
        }));
    }

    for h in writer_handles {
        h.join().expect("writer thread");
    }
    writers_done.store(true, std::sync::atomic::Ordering::Relaxed);
    let mut saw_any = false;
    for h in reader_handles {
        saw_any |= h.join().expect("reader thread") > 0;
    }
    assert!(saw_any, "readers must observe events while writers run");
    assert_eq!(s.stats().events, (writers as u64) * per_writer);
    let f = filter(serde_json::json!({"#t": ["heavy"], "limit": 10_000}));
    assert_eq!(
        s.query(&[f]).unwrap().len(),
        (writers * per_writer as usize)
    );
}

#[test]
fn reopen_after_heavy_ingest_preserves_every_event() {
    let dir = tmpdir("reopen-heavy");
    let k = Keys::from_label("adv-reopen");
    let evs: Vec<Event> = (0..300u64)
        .map(|i| {
            ev(
                &k,
                1_700_000_000 + i,
                1,
                vec![
                    vec!["t".into(), "reopen".into()],
                    vec!["n".into(), i.to_string()],
                ],
                &format!("event {i}"),
            )
        })
        .collect();
    {
        let s = open(&dir);
        let outcomes = s.insert_batch(&evs).unwrap();
        assert!(outcomes.iter().all(|o| *o == InsertOutcome::Stored));
        s.flush().unwrap();
    }
    let s2 = open(&dir);
    assert_eq!(s2.stats().events, 300, "all events must survive reopen");
    let f = filter(serde_json::json!({"#t": ["reopen"], "limit": 1000}));
    let r = s2.query(&[f]).unwrap();
    assert_eq!(r.len(), 300);
    for e in &r {
        e.verify().expect("recovered event must verify");
    }
    // index lookups by tag value still work after recovery
    let f = filter(serde_json::json!({"#n": ["42"]}));
    let r = s2.query(&[f]).unwrap();
    assert_eq!(r.len(), 1);
    assert_eq!(r[0].created_at, 1_700_000_042);
    // duplicates are still detected after recovery
    assert_eq!(s2.insert(&evs[0]).unwrap(), InsertOutcome::Duplicate);
}

#[test]
fn empty_and_oversized_filter_lists_are_handled() {
    let dir = tmpdir("filter-lists");
    let s = open(&dir);
    let k = Keys::from_label("adv-filter-lists");
    for i in 0..5u64 {
        s.insert(&ev(&k, 1_700_000_000 + i, 1, vec![], "x"))
            .unwrap();
    }
    // no filters at all: the documented contract is an empty result, not an error
    assert!(s.query(&[]).unwrap().is_empty());
    assert_eq!(s.count(&[]).unwrap(), 0);
    // many filters: dedup across them must still hold
    let filters: Vec<Filter> = (0..20)
        .map(|_| filter(serde_json::json!({"kinds": [1], "limit": 5})))
        .collect();
    let r = s.query(&filters).unwrap();
    assert_eq!(
        r.len(),
        5,
        "identical filters must dedup to the same 5 events"
    );
    let mut ids: Vec<_> = r.iter().map(|e| e.id).collect();
    ids.sort();
    ids.dedup();
    assert_eq!(ids.len(), 5);
}

#[test]
fn zero_length_content_and_empty_tags_roundtrip() {
    let dir = tmpdir("empty-content");
    let s = open(&dir);
    let k = Keys::from_label("adv-empty");
    let e = ev(&k, 1_700_000_000, 1, vec![], "");
    assert_eq!(s.insert(&e).unwrap(), InsertOutcome::Stored);
    let f = filter(serde_json::json!({"ids": [hex::encode(e.id)]}));
    let r = s.query(&[f]).unwrap();
    assert_eq!(r.len(), 1);
    assert_eq!(r[0].content, "");
    assert!(r[0].tags.is_empty());
    r[0].verify().expect("empty-content event verifies");
    // survives reopen
    s.flush().unwrap();
    drop(s);
    let s2 = open(&dir);
    let f = filter(serde_json::json!({"ids": [hex::encode(e.id)]}));
    let r = s2.query(&[f]).unwrap();
    assert_eq!(r.len(), 1);
    assert_eq!(r[0].content, "");
}
