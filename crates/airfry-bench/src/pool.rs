//! Deterministic pre-signed event pool generation.
//!
//! Signing is the expensive part of a publish benchmark (a BIP-340 schnorr signature costs tens
//! of microseconds), so the pool is built once, up front, in parallel across all cores and then
//! reused by the concurrent publishers. Everything is derived from a seed and the event index so
//! that two runs with the same arguments produce byte-identical events (and therefore identical
//! ids), which makes results reproducible and lets us detect duplicate-rejection behaviour.

use airfry_core::{Event, Keys};
use std::sync::Arc;

/// Base timestamp for generated events. Fixed so runs are reproducible.
pub const BASE_CREATED_AT: u64 = 1_700_000_000;

/// Number of distinct `t` tag topics used by the generated corpus.
pub const TOPIC_COUNT: u64 = 16;

/// Parameters for a generated corpus.
#[derive(Clone, Debug)]
pub struct PoolConfig {
    /// Number of events to generate.
    pub events: usize,
    /// Number of distinct author keypairs.
    pub authors: usize,
    /// Event kinds to cycle through.
    pub kinds: Vec<u32>,
    /// Seed mixed into content/created_at so repeated runs produce fresh ids.
    pub seed: u64,
}

impl PoolConfig {
    pub fn new(events: usize, authors: usize, kinds: Vec<u32>, seed: u64) -> PoolConfig {
        PoolConfig {
            events,
            authors: authors.max(1),
            kinds: if kinds.is_empty() { vec![1] } else { kinds },
            seed,
        }
    }
}

/// A pre-signed, pre-serialized corpus ready to be pushed onto the wire.
pub struct EventPool {
    /// `["EVENT",{...}]` frames, one per event, in generation order.
    pub frames: Vec<String>,
    /// Event ids, aligned with `frames`.
    pub ids: Vec<[u8; 32]>,
    /// Author pubkeys, aligned with `frames`.
    pub pubkeys: Vec<[u8; 32]>,
    /// Event kinds, aligned with `frames`.
    pub kinds: Vec<u32>,
    /// Creation timestamps, aligned with `frames`.
    pub created_at: Vec<u64>,
    /// The signed events themselves (needed by the direct-store benchmark).
    pub events: Vec<Arc<Event>>,
}

impl EventPool {
    pub fn len(&self) -> usize {
        self.frames.len()
    }

    pub fn is_empty(&self) -> bool {
        self.frames.is_empty()
    }

    /// Earliest `created_at` in the pool.
    pub fn min_created_at(&self) -> u64 {
        self.created_at
            .iter()
            .copied()
            .min()
            .unwrap_or(BASE_CREATED_AT)
    }

    /// Latest `created_at` in the pool.
    pub fn max_created_at(&self) -> u64 {
        self.created_at
            .iter()
            .copied()
            .max()
            .unwrap_or(BASE_CREATED_AT)
    }
}

/// Deterministic author label for author index `i`.
pub fn author_label(i: usize) -> String {
    format!("bench-author-{i}")
}

/// Deterministic content for event `i`.
pub fn event_content(i: usize, seed: u64) -> String {
    format!("bench event #{i} seed={seed} lorem ipsum dolor sit amet consectetur adipiscing elit")
}

/// Deterministic tags for event `i`.
pub fn event_tags(i: usize, author_pubkey: [u8; 32], seed: u64) -> Vec<Vec<String>> {
    let topic = (i as u64 + seed) % TOPIC_COUNT;
    vec![
        vec!["t".to_string(), format!("topic{topic}")],
        vec!["p".to_string(), hex::encode(author_pubkey)],
    ]
}

/// Build a signed event for index `i` using the given key set.
pub fn build_event(keys: &Keys, i: usize, kind: u32, seed: u64) -> Event {
    let created_at = BASE_CREATED_AT + seed.wrapping_mul(1_000_000) + i as u64;
    let tags = event_tags(i, keys.public_key, seed);
    keys.sign_event(created_at, kind, tags, &event_content(i, seed))
}

/// Sign `[start, end)` in parallel across all cores, returning `(index, event)` pairs in order.
fn sign_range(cfg: &PoolConfig, start: usize, end: usize) -> Vec<(usize, Event)> {
    let threads = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4)
        .min(32);
    let total = end.saturating_sub(start);
    let threads = threads.max(1).min(total.max(1));
    let chunk = total.div_ceil(threads);
    let mut out: Vec<(usize, Event)> = Vec::with_capacity(total);

    std::thread::scope(|scope| {
        let mut receivers = Vec::new();
        let mut handles = Vec::new();
        for t in 0..threads {
            let s = start + t * chunk;
            let e = (start + (t + 1) * chunk).min(end);
            if s >= e {
                continue;
            }
            let (tx, rx) = std::sync::mpsc::channel::<(usize, Event)>();
            receivers.push(rx);
            handles.push(scope.spawn(move || {
                let keys: Vec<Keys> = (0..cfg.authors)
                    .map(|a| Keys::from_label(&author_label(a)))
                    .collect();
                for i in s..e {
                    let author = i % cfg.authors;
                    let kind = cfg.kinds[i % cfg.kinds.len()];
                    let ev = build_event(&keys[author], i, kind, cfg.seed);
                    if tx.send((i, ev)).is_err() {
                        return;
                    }
                }
            }));
        }
        for rx in receivers {
            for pair in rx {
                out.push(pair);
            }
        }
        for h in handles {
            let _ = h.join();
        }
    });

    out.sort_unstable_by_key(|(i, _)| *i);
    out
}

/// Generate the corpus, signing in parallel across all available cores.
pub fn build_pool(cfg: &PoolConfig) -> EventPool {
    let signed = sign_range(cfg, 0, cfg.events);
    let mut frames = Vec::with_capacity(signed.len());
    let mut ids = Vec::with_capacity(signed.len());
    let mut pubkeys = Vec::with_capacity(signed.len());
    let mut kinds = Vec::with_capacity(signed.len());
    let mut created_at = Vec::with_capacity(signed.len());
    let mut events = Vec::with_capacity(signed.len());
    for (_, ev) in signed {
        frames.push(format!("[\"EVENT\",{}]", ev.to_json()));
        ids.push(ev.id);
        pubkeys.push(ev.pubkey);
        kinds.push(ev.kind);
        created_at.push(ev.created_at);
        events.push(Arc::new(ev));
    }
    EventPool {
        frames,
        ids,
        pubkeys,
        kinds,
        created_at,
        events,
    }
}

/// Generate only the signed events (no wire frames). Used by the direct-store benchmark.
pub fn build_events(cfg: &PoolConfig) -> Vec<Event> {
    sign_range(cfg, 0, cfg.events)
        .into_iter()
        .map(|(_, ev)| ev)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pool_is_deterministic() {
        let cfg = PoolConfig::new(8, 2, vec![1, 7], 0);
        let a = build_pool(&cfg);
        let b = build_pool(&cfg);
        assert_eq!(a.len(), 8);
        assert_eq!(a.ids, b.ids);
        assert_eq!(a.frames, b.frames);
    }

    #[test]
    fn pool_ids_are_unique_and_valid() {
        let cfg = PoolConfig::new(32, 3, vec![1], 0);
        let pool = build_pool(&cfg);
        let mut seen = std::collections::HashSet::new();
        for ev in &pool.events {
            assert!(seen.insert(ev.id), "duplicate id");
            ev.verify().expect("pool events must be validly signed");
        }
        assert_eq!(seen.len(), 32);
    }

    #[test]
    fn pool_respects_author_and_kind_distribution() {
        let cfg = PoolConfig::new(12, 4, vec![1, 7, 30023], 0);
        let pool = build_pool(&cfg);
        let authors: std::collections::HashSet<_> = pool.pubkeys.iter().collect();
        assert_eq!(authors.len(), 4);
        let kinds: std::collections::HashSet<_> = pool.kinds.iter().copied().collect();
        assert_eq!(kinds.len(), 3);
        assert!(pool.min_created_at() < pool.max_created_at());
    }

    #[test]
    fn seed_changes_ids() {
        let a = build_pool(&PoolConfig::new(4, 1, vec![1], 0));
        let b = build_pool(&PoolConfig::new(4, 1, vec![1], 1));
        assert_ne!(a.ids, b.ids);
    }

    #[test]
    fn frames_are_event_frames() {
        let pool = build_pool(&PoolConfig::new(2, 1, vec![1], 0));
        for f in &pool.frames {
            assert!(f.starts_with("[\"EVENT\",{"));
            assert!(f.ends_with("}]"));
        }
    }

    #[test]
    fn empty_pool() {
        let pool = build_pool(&PoolConfig::new(0, 1, vec![1], 0));
        assert!(pool.is_empty());
        assert_eq!(pool.min_created_at(), BASE_CREATED_AT);
    }

    #[test]
    fn build_events_matches_pool() {
        let cfg = PoolConfig::new(6, 2, vec![1], 0);
        let pool = build_pool(&cfg);
        let evs = build_events(&cfg);
        assert_eq!(evs.len(), pool.len());
        for (a, b) in evs.iter().zip(pool.events.iter()) {
            assert_eq!(a.id, b.id);
        }
    }
}
