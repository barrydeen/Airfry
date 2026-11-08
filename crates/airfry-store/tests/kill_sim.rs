use airfry_core::{Event, Filter, Keys};
use airfry_store::{FsyncPolicy, Store, StoreConfig};

#[test]
fn survives_sigkill_simulation() {
    let mut dir = std::env::temp_dir();
    dir.push(format!(
        "airfry-kill-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let mut cfg = StoreConfig::new(&dir);
    cfg.fsync = FsyncPolicy::Never;
    let s = Store::open(cfg).unwrap();
    let k = Keys::from_label("kill");
    let e = k.sign_event(1_700_000_000, 1, vec![], "survive");
    s.insert(&e).unwrap();
    // Simulate SIGKILL: forget the store so Drop never runs.
    std::mem::forget(s);
    let mut cfg = StoreConfig::new(&dir);
    cfg.fsync = FsyncPolicy::Never;
    let s2 = Store::open(cfg).unwrap();
    let f = Filter::from_json_value(&serde_json::json!({"ids":[hex::encode(e.id)]})).unwrap();
    let r = s2.query(&[f]).unwrap();
    assert_eq!(r.len(), 1, "event must survive a SIGKILL (no Drop)");
    let _ = std::fs::remove_dir_all(&dir);
}
