//! Benchmark: end-to-end negentropy reconciliation timing for airfry-negentropy.
//!
//! Runs a full client<->server reconciliation over in-memory sets and reports wall-clock
//! timings. The output format is intentionally identical to the strfry reference benchmark
//! (`third_party/negentropy-ref/test/cpp/bench.cpp`) so the two can be compared directly:
//!
//!   impl=airfry n=<n> overlap=<pct> best_ms=<ms> rounds=<r> bytes=<b>
//!
//! Usage: `negentropy-bench <n_items> <overlap_pct> <rounds>`
//!   overlap_pct: 0 = disjoint, 50 = half shared, 99 = 99% shared.
//!
//! With no arguments it runs the full matrix (1k/10k/100k x 0/50/99%). Each scenario in the
//! matrix is executed in a *fresh child process* (re-exec of this binary) so that allocator
//! state from a previous large scenario cannot pollute the next one — this is required for
//! reproducible numbers (see BENCHMARK.md).

use std::process::Command;
use std::sync::Arc;
use std::time::Instant;

use airfry_negentropy::{Item, Negentropy, NegentropySet, ID_SIZE};
use sha2::{Digest, Sha256};

fn make_id(seed: u64) -> [u8; ID_SIZE] {
    let mut h = Sha256::new();
    h.update(seed.to_le_bytes());
    let d = h.finalize();
    let mut id = [0u8; ID_SIZE];
    id.copy_from_slice(&d);
    id
}

/// Build a sealed set of `n` items. `offset` shifts the seed space so two sets can be made
/// disjoint or partially overlapping.
fn make_set(n: usize, offset: u64) -> NegentropySet {
    let mut items = Vec::with_capacity(n);
    for i in 0..n {
        let seed = offset + i as u64;
        items.push(Item::new(1_700_000_000 + seed, make_id(seed)));
    }
    NegentropySet::new(items).expect("seal")
}

/// Build a server set that shares `overlap_pct`% of its items with the client set built from
/// offset 0. The remaining items are drawn from a disjoint seed range.
fn make_overlap_set(n: usize, overlap_pct: u64) -> NegentropySet {
    let shared = (n as u64 * overlap_pct / 100) as usize;
    let mut items = Vec::with_capacity(n);
    for i in 0..n {
        let seed = if i < shared {
            i as u64 // shared with client (client uses offset 0)
        } else {
            (n as u64) + i as u64 // disjoint
        };
        items.push(Item::new(1_700_000_000 + seed, make_id(seed)));
    }
    NegentropySet::new(items).expect("seal")
}

/// Drive a full reconciliation between an initiator and responder, returning
/// (have, need, rounds, total_bytes).
fn reconcile(
    client: &NegentropySet,
    server: &NegentropySet,
) -> (Vec<[u8; ID_SIZE]>, Vec<[u8; ID_SIZE]>, usize, usize) {
    let mut init = Negentropy::new(Arc::new(client.clone()), 0).expect("init");
    let mut resp = Negentropy::new(Arc::new(server.clone()), 0).expect("resp");

    let mut have = Vec::new();
    let mut need = Vec::new();
    let mut rounds = 0usize;
    let mut bytes = 0usize;

    let mut msg = init.initiate().expect("initiate");
    bytes += msg.len();

    loop {
        rounds += 1;
        let reply = resp
            .reconcile(&msg, &mut Vec::new(), &mut Vec::new())
            .expect("resp reconcile");
        bytes += reply.len();
        if reply.len() == 1 {
            break;
        }
        let next = init
            .reconcile(&reply, &mut have, &mut need)
            .expect("init reconcile");
        bytes += next.len();
        if next.len() == 1 {
            break;
        }
        msg = next;
        if rounds > 10_000 {
            panic!("reconciliation did not terminate");
        }
    }

    (have, need, rounds, bytes)
}

/// Run one benchmark scenario and print a machine-parseable line.
fn run_scenario(n: usize, overlap_pct: u64, rounds: usize) {
    let client = make_set(n, 0);
    let server = make_overlap_set(n, overlap_pct);

    let mut best = f64::MAX;
    let mut best_rounds = 0usize;
    let mut best_bytes = 0usize;
    for _ in 0..rounds {
        let t = Instant::now();
        let (_have, _need, r, bytes) = reconcile(&client, &server);
        let ms = t.elapsed().as_secs_f64() * 1000.0;
        if ms < best {
            best = ms;
            best_rounds = r;
            best_bytes = bytes;
        }
    }

    println!(
        "impl=airfry n={n} overlap={overlap_pct} best_ms={best:.4} rounds={best_rounds} bytes={best_bytes}"
    );
}

fn main() {
    let args: Vec<String> = std::env::args().collect();

    if args.len() >= 4 {
        let n: usize = args[1].parse().expect("n_items");
        let overlap: u64 = args[2].parse().expect("overlap_pct");
        let rounds: usize = args[3].parse().expect("rounds");
        run_scenario(n, overlap, rounds);
        return;
    }

    // No args: run the full matrix, each scenario in a fresh child process so allocator
    // state does not leak between scenarios.
    let rounds: usize = args.get(1).and_then(|s| s.parse().ok()).unwrap_or(20);
    let exe = std::env::current_exe().expect("current_exe");
    for n in [1_000usize, 10_000, 100_000] {
        for overlap in [0u64, 50, 99] {
            let out = Command::new(&exe)
                .arg(n.to_string())
                .arg(overlap.to_string())
                .arg(rounds.to_string())
                .output()
                .expect("spawn child");
            print!("{}", String::from_utf8_lossy(&out.stdout));
        }
    }
}
