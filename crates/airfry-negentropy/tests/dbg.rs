use airfry_negentropy::{Item, Negentropy, NegentropySet, ID_SIZE};
use sha2::{Digest, Sha256};
use std::sync::Arc;
fn make_id(seed: u64) -> [u8; ID_SIZE] {
    let mut h = Sha256::new();
    h.update(seed.to_le_bytes());
    let d = h.finalize();
    let mut id = [0u8; ID_SIZE];
    id.copy_from_slice(&d);
    id
}
fn make_set(n: usize, offset: u64) -> NegentropySet {
    let mut items = Vec::new();
    for i in 0..n {
        let s = offset + i as u64;
        items.push(Item::new(1_700_000_000 + s, make_id(s)));
    }
    NegentropySet::new(items).unwrap()
}
#[test]
fn dbg_disjoint() {
    let a = make_set(100, 0);
    let b = make_set(100, 100);
    eprintln!("a fp {:?}", hex::encode(a.fingerprint(0, a.len())));
    eprintln!("b fp {:?}", hex::encode(b.fingerprint(0, b.len())));
    let mut init = Negentropy::new(Arc::new(a), 0).unwrap();
    let mut resp = Negentropy::new(Arc::new(b), 0).unwrap();
    let msg = init.initiate().unwrap();
    eprintln!("init msg {:?}", hex::encode(&msg));
    let reply = resp
        .reconcile(&msg, &mut Vec::new(), &mut Vec::new())
        .unwrap();
    eprintln!("resp reply {:?}", hex::encode(&reply));
}
