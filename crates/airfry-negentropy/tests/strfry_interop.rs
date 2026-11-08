//! Byte-for-byte interop tests against the upstream strfry/negentropy C++ reference harness.
//!
//! The reference harness binary lives at `third_party/negentropy-ref/test/cpp/harness` and speaks
//! the same line protocol as our `negentropy-harness` bin. These tests drive both implementations
//! through identical scenarios and require:
//!
//! 1. identical `initiate` output (wire-format conformance), and
//! 2. a full cross-implementation reconciliation that terminates with the correct `have`/`need`
//!    sets (protocol interop).
//!
//! If the reference binary is missing (e.g. a source-only checkout without a C++ toolchain), the
//! tests skip with a printed notice instead of failing.

use std::io::{BufRead, BufReader, Write};
use std::path::PathBuf;
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};

use airfry_negentropy::{Item, Negentropy, NegentropySet, ID_SIZE};
use sha2::{Digest, Sha256};

fn repo_root() -> PathBuf {
    // CARGO_MANIFEST_DIR = <repo>/crates/airfry-negentropy
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .to_path_buf()
}

fn ref_harness() -> Option<PathBuf> {
    let p = repo_root().join("third_party/negentropy-ref/test/cpp/harness");
    if p.exists() {
        Some(p)
    } else {
        None
    }
}

fn our_harness() -> PathBuf {
    // Built by cargo for this package; the test binary dir is target/<profile>/deps.
    let mut p = std::env::current_exe().unwrap();
    p.pop(); // deps
    p.pop(); // profile dir
    p.push("negentropy-harness");
    p
}

struct Harness {
    child: Child,
    stdin: ChildStdin,
    stdout: BufReader<ChildStdout>,
}

impl Harness {
    fn spawn(cmd: &PathBuf, frame_limit: u64) -> Harness {
        let mut c = Command::new(cmd);
        c.stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit());
        if frame_limit != 0 {
            c.env("FRAMESIZELIMIT", frame_limit.to_string());
        }
        let mut child = c.spawn().expect("spawn harness");
        let stdin = child.stdin.take().unwrap();
        let stdout = BufReader::new(child.stdout.take().unwrap());
        Harness {
            child,
            stdin,
            stdout,
        }
    }

    fn send(&mut self, line: &str) {
        writeln!(self.stdin, "{line}").unwrap();
        self.stdin.flush().unwrap();
    }

    fn recv(&mut self) -> String {
        let mut s = String::new();
        self.stdout.read_line(&mut s).expect("read harness line");
        assert!(!s.is_empty(), "harness closed stdout unexpectedly");
        s.trim_end().to_string()
    }
}

impl Drop for Harness {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn make_id(seed: u64) -> [u8; ID_SIZE] {
    let mut h = Sha256::new();
    h.update(seed.to_le_bytes());
    let d = h.finalize();
    let mut id = [0u8; ID_SIZE];
    id.copy_from_slice(&d);
    id
}

/// Deterministic pseudo-random set generator (xorshift), mirroring the fuzz.pl distributions:
/// `p_shared` of items go to both sides, the rest split between the two.
fn gen_pair(n: usize, seed: u64, p_shared: u32) -> (Vec<Item>, Vec<Item>) {
    let mut s = seed | 1;
    let mut next = || {
        s ^= s << 13;
        s ^= s >> 7;
        s ^= s << 17;
        s
    };
    let mut a = Vec::new();
    let mut b = Vec::new();
    for i in 0..n {
        let r = (next() % 100) as u32;
        let id = make_id(next());
        let ts = 1_677_970_534 + (next() % (n as u64 + 1));
        let item = Item::new(ts, id);
        if r < p_shared {
            a.push(item);
            b.push(item);
        } else if r < p_shared + (100 - p_shared) / 2 {
            a.push(item);
        } else {
            b.push(item);
        }
        let _ = i;
    }
    (a, b)
}

fn feed(h: &mut Harness, items: &[Item]) {
    for it in items {
        h.send(&format!("item,{},{}", it.timestamp, hex::encode(it.id)));
    }
    h.send("seal");
}

/// Run a full reconciliation between two harness processes; returns (have, need) as seen by the
/// initiator (harness 1).
fn run_interop(h1: &mut Harness, h2: &mut Harness) -> (Vec<String>, Vec<String>) {
    let mut have = Vec::new();
    let mut need = Vec::new();
    h1.send("initiate");
    let mut msg = h1.recv();
    let mut rounds = 0;
    loop {
        rounds += 1;
        assert!(rounds < 10_000, "reconciliation did not terminate");
        assert!(
            msg.starts_with("msg,"),
            "expected msg from initiator, got {msg}"
        );
        h2.send(&msg);
        let reply = h2.recv();
        assert!(
            reply.starts_with("msg,"),
            "expected msg from responder, got {reply}"
        );
        h1.send(&reply);
        loop {
            let line = h1.recv();
            if let Some(id) = line.strip_prefix("have,") {
                have.push(id.to_string());
            } else if let Some(id) = line.strip_prefix("need,") {
                need.push(id.to_string());
            } else if line == "done" {
                return (have, need);
            } else if line.starts_with("msg,") {
                msg = line;
                break;
            } else {
                panic!("unexpected line from initiator: {line}");
            }
        }
    }
}

fn expected_diff(a: &[Item], b: &[Item]) -> (Vec<String>, Vec<String>) {
    let bset: std::collections::HashSet<[u8; ID_SIZE]> = b.iter().map(|i| i.id).collect();
    let aset: std::collections::HashSet<[u8; ID_SIZE]> = a.iter().map(|i| i.id).collect();
    let mut have: Vec<String> = a
        .iter()
        .filter(|i| !bset.contains(&i.id))
        .map(|i| hex::encode(i.id))
        .collect();
    let mut need: Vec<String> = b
        .iter()
        .filter(|i| !aset.contains(&i.id))
        .map(|i| hex::encode(i.id))
        .collect();
    have.sort();
    need.sort();
    (have, need)
}

fn sorted(mut v: Vec<String>) -> Vec<String> {
    v.sort();
    v
}

#[test]
fn initiate_matches_reference_wire_format() {
    let Some(refp) = ref_harness() else {
        eprintln!(
            "SKIP: reference harness not built at third_party/negentropy-ref/test/cpp/harness"
        );
        return;
    };
    let ours = our_harness();
    assert!(
        ours.exists(),
        "our harness bin not built: {}",
        ours.display()
    );

    // Small set (id-list path) and large set (16-bucket fingerprint path), plus a clustered set
    // with many duplicate timestamps to exercise minimal-bound splitting.
    let scenarios: Vec<Vec<Item>> = vec![
        vec![
            Item::new(1_677_970_534, make_id(1)),
            Item::new(1_677_970_535, make_id(2)),
        ],
        (0..40u64)
            .map(|i| Item::new(1_677_970_534 + i, make_id(i)))
            .collect(),
        (0..1000u64)
            .map(|i| Item::new(1_677_970_534 + i / 10, make_id(i)))
            .collect(),
    ];

    for (idx, items) in scenarios.iter().enumerate() {
        let mut r = Harness::spawn(&refp, 0);
        let mut o = Harness::spawn(&ours, 0);
        feed(&mut r, items);
        feed(&mut o, items);
        r.send("initiate");
        o.send("initiate");
        let rmsg = r.recv();
        let omsg = o.recv();
        assert_eq!(
            rmsg, omsg,
            "scenario {idx}: initiate output differs from reference"
        );
        eprintln!(
            "scenario {idx}: initiate matches reference ({} bytes)",
            rmsg.len() / 2
        );
    }
}

#[test]
fn interop_against_reference_responder_and_initiator() {
    let Some(refp) = ref_harness() else {
        eprintln!("SKIP: reference harness not built");
        return;
    };
    let ours = our_harness();

    let cases: Vec<(usize, u64, u32)> = vec![
        (50, 7, 0),    // disjoint
        (50, 11, 100), // identical
        (500, 13, 50), // half shared
        (2000, 17, 90),
    ];

    for (n, seed, p_shared) in cases {
        let (a, b) = gen_pair(n, seed, p_shared);
        let (exp_have, exp_need) = expected_diff(&a, &b);

        // (1) airfry initiator <-> reference responder
        {
            let mut o = Harness::spawn(&ours, 0);
            let mut r = Harness::spawn(&refp, 0);
            feed(&mut o, &a);
            feed(&mut r, &b);
            let (have, need) = run_interop(&mut o, &mut r);
            assert_eq!(
                sorted(have),
                exp_have,
                "n={n} seed={seed}: have mismatch vs reference responder"
            );
            assert_eq!(
                sorted(need),
                exp_need,
                "n={n} seed={seed}: need mismatch vs reference responder"
            );
        }

        // (2) reference initiator <-> airfry responder
        {
            let mut r = Harness::spawn(&refp, 0);
            let mut o = Harness::spawn(&ours, 0);
            feed(&mut r, &a);
            feed(&mut o, &b);
            let (have, need) = run_interop(&mut r, &mut o);
            assert_eq!(
                sorted(have),
                exp_have,
                "n={n} seed={seed}: have mismatch vs reference initiator"
            );
            assert_eq!(
                sorted(need),
                exp_need,
                "n={n} seed={seed}: need mismatch vs reference initiator"
            );
        }
        eprintln!(
            "interop n={n} seed={seed} p_shared={p_shared}: OK ({} have, {} need)",
            exp_have.len(),
            exp_need.len()
        );
    }
}

#[test]
fn interop_with_frame_size_limits() {
    let Some(refp) = ref_harness() else {
        eprintln!("SKIP: reference harness not built");
        return;
    };
    let ours = our_harness();
    let (a, b) = gen_pair(5000, 23, 30);
    let (exp_have, exp_need) = expected_diff(&a, &b);

    // Reference initiator with a small frame limit talking to our responder with a large one.
    let mut r = Harness::spawn(&refp, 60_000);
    let mut o = Harness::spawn(&ours, 500_000);
    feed(&mut r, &a);
    feed(&mut o, &b);
    let (have, need) = run_interop(&mut r, &mut o);
    assert_eq!(sorted(have), exp_have);
    assert_eq!(sorted(need), exp_need);

    // Our initiator with a small frame limit talking to the reference responder.
    let mut o = Harness::spawn(&ours, 60_000);
    let mut r = Harness::spawn(&refp, 500_000);
    feed(&mut o, &a);
    feed(&mut r, &b);
    let (have, need) = run_interop(&mut o, &mut r);
    assert_eq!(sorted(have), exp_have);
    assert_eq!(sorted(need), exp_need);
    eprintln!("frame-size-limited interop: OK");
}

/// In-process sanity check that the sealed-set API agrees with the harness path.
#[test]
fn in_process_roundtrip_matches_expected_diff() {
    let (a, b) = gen_pair(300, 31, 40);
    let (exp_have, exp_need) = expected_diff(&a, &b);
    let sa = NegentropySet::new(a).unwrap();
    let sb = NegentropySet::new(b).unwrap();
    let mut init = Negentropy::new(std::sync::Arc::new(sa), 0).unwrap();
    let mut resp = Negentropy::new(std::sync::Arc::new(sb), 0).unwrap();
    let mut have = Vec::new();
    let mut need = Vec::new();
    let mut msg = init.initiate().unwrap();
    loop {
        let reply = resp
            .reconcile(&msg, &mut Vec::new(), &mut Vec::new())
            .unwrap();
        if reply.len() == 1 {
            break;
        }
        let next = init.reconcile(&reply, &mut have, &mut need).unwrap();
        if next.len() == 1 {
            break;
        }
        msg = next;
    }
    let have: Vec<String> = sorted(have.iter().map(|i| hex::encode(i)).collect());
    let need: Vec<String> = sorted(need.iter().map(|i| hex::encode(i)).collect());
    assert_eq!(have, exp_have);
    assert_eq!(need, exp_need);
}
