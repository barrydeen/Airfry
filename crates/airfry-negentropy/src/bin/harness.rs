//! Stdio line-based conformance harness for the negentropy test-suite.
//!
//! This mirrors `third_party/strfry/external/negentropy/test/cpp/harness.cpp` so that the
//! upstream perl test-suite (`test.pl`, `fuzz.pl`, `protoversion.pl`) can drive this crate.
//!
//! Commands (one per line, comma separated):
//!   item,<created_at>,<id_hex>   insert an item into the local set
//!   seal                         seal the set and construct the reconciler
//!   initiate                     emit the initial message (initiator role)
//!   msg[,<hex>]                  process a peer message and emit our reply / have / need
//!
//! Environment: `FRAMESIZELIMIT` sets the frame size limit (0 = unlimited).

use std::io::{self, BufRead, Write};
use std::sync::Arc;

use airfry_negentropy::{Item, Negentropy, NegentropySet, ID_SIZE};

fn hex_to_id(s: &str) -> [u8; ID_SIZE] {
    let bytes = hex::decode(s).expect("bad hex id");
    assert_eq!(bytes.len(), ID_SIZE, "bad id size");
    let mut id = [0u8; ID_SIZE];
    id.copy_from_slice(&bytes);
    id
}

fn main() {
    let frame_size_limit: u64 = std::env::var("FRAMESIZELIMIT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);

    let mut items: Vec<Item> = Vec::new();
    let mut ne: Option<Negentropy> = None;

    let stdin = io::stdin();
    let stdout = io::stdout();
    let mut out = stdout.lock();

    for line in stdin.lock().lines() {
        let line = match line {
            Ok(l) => l,
            Err(_) => break,
        };
        if line.is_empty() {
            continue;
        }
        let parts: Vec<&str> = line.split(',').collect();
        match parts[0] {
            "item" => {
                assert_eq!(parts.len(), 3, "wrong num of fields");
                let created: u64 = parts[1].parse().expect("bad timestamp");
                let id = hex_to_id(parts[2]);
                items.push(Item::new(created, id));
            }
            "seal" => {
                let set = NegentropySet::new(std::mem::take(&mut items)).expect("seal failed");
                ne =
                    Some(Negentropy::new(Arc::new(set), frame_size_limit).expect("negentropy new"));
            }
            "initiate" => {
                let n = ne.as_mut().expect("not sealed");
                let q = n.initiate().expect("initiate failed");
                if frame_size_limit != 0 && q.len() as u64 > frame_size_limit {
                    panic!(
                        "initiate frameSizeLimit exceeded: {} > {}",
                        q.len(),
                        frame_size_limit
                    );
                }
                writeln!(out, "msg,{}", hex::encode(&q)).unwrap();
            }
            "msg" => {
                let n = ne.as_mut().expect("not sealed");
                let q: Vec<u8> = if parts.len() >= 2 {
                    hex::decode(parts[1]).expect("bad hex msg")
                } else {
                    Vec::new()
                };

                if n.is_initiator() {
                    let mut have: Vec<[u8; ID_SIZE]> = Vec::new();
                    let mut need: Vec<[u8; ID_SIZE]> = Vec::new();
                    let resp = n
                        .reconcile(&q, &mut have, &mut need)
                        .expect("reconcile failed");

                    for id in &have {
                        writeln!(out, "have,{}", hex::encode(id)).unwrap();
                    }
                    for id in &need {
                        writeln!(out, "need,{}", hex::encode(id)).unwrap();
                    }

                    if resp.len() == 1 {
                        writeln!(out, "done").unwrap();
                        continue;
                    }
                    if frame_size_limit != 0 && resp.len() as u64 > frame_size_limit {
                        panic!(
                            "frameSizeLimit exceeded: {} > {}",
                            resp.len(),
                            frame_size_limit
                        );
                    }
                    writeln!(out, "msg,{}", hex::encode(&resp)).unwrap();
                } else {
                    let mut have: Vec<[u8; ID_SIZE]> = Vec::new();
                    let mut need: Vec<[u8; ID_SIZE]> = Vec::new();
                    let resp = n
                        .reconcile(&q, &mut have, &mut need)
                        .expect("reconcile failed");
                    if frame_size_limit != 0 && resp.len() as u64 > frame_size_limit {
                        panic!(
                            "frameSizeLimit exceeded: {} > {}",
                            resp.len(),
                            frame_size_limit
                        );
                    }
                    writeln!(out, "msg,{}", hex::encode(&resp)).unwrap();
                }
            }
            other => panic!("unknown cmd: {other}"),
        }
        out.flush().unwrap();
    }
}
