//! In-memory indexes built for Nostr query patterns.
//!
//! The engine keeps every stored event as an `Arc<Event>` and maintains secondary indexes that
//! let a REQ be answered without ever scanning the log or the whole corpus:
//!
//! * `by_id`: id -> `Arc<Event>` (primary lookup, duplicate detection, NIP-09 targeting).
//! * `by_author`: pubkey -> events (newest-first).
//! * `by_kind`: kind -> events (newest-first).
//! * `by_tag`: (single-letter tag name, value) -> events (newest-first).
//! * `by_time`: a global created_at-ordered list used for time-range and empty filters.
//!
//! # Posting lists
//!
//! Each posting list is an [`EventList`] backed by an append-only `Vec<Arc<Event>>` plus a
//! lazily-built newest-first snapshot. Appending is O(1) (a single push) so the ingest hot path
//! never pays for tree-node allocation or log-n insertion. The newest-first snapshot is built on
//! first read after a mutation and cached; because the store is append-only (events are never
//! removed from the lists, only tombstoned), a cached snapshot remains a valid prefix of the live
//! set and is only rebuilt when new events arrive. A newest-first walk with a `limit` stops after
//! K events, so query cost is proportional to the number of returned events, not the corpus size.
//!
//! Concurrency: the whole index lives behind a single `parking_lot::RwLock` owned by `Store`.
//! Writers take the write lock only to publish a batch of already-encoded events; readers take the
//! read lock and clone `Arc`s out of the lists. Because the index is append-only, a reader holding
//! the read lock always sees a consistent snapshot.

use nostr_core::Event;
use std::collections::HashMap;
use std::sync::Arc;

/// A posting list of events for one index key, kept append-only with a lazily-built newest-first
/// snapshot.
#[derive(Default)]
pub struct EventList {
    events: Vec<Arc<Event>>,
    /// Cached newest-first snapshot. `None` when stale (a mutation happened since it was built).
    sorted: Option<Vec<Arc<Event>>>,
}

impl EventList {
    #[inline]
    fn push(&mut self, ev: Arc<Event>) {
        self.events.push(ev);
        self.sorted = None;
    }

    /// Build (or return the cached) newest-first snapshot.
    #[inline]
    pub fn sorted(&mut self) -> &[Arc<Event>] {
        if self.sorted.is_none() {
            let mut v = self.events.clone();
            v.sort_unstable_by(|a, b| b.created_at.cmp(&a.created_at));
            self.sorted = Some(v);
        }
        self.sorted.as_deref().unwrap()
    }

    /// Immutable access to the cached newest-first snapshot. Must be called after `sorted()`
    /// has built it (see `Store::prepare`). Returns an empty slice if not yet built.
    #[inline]
    pub fn sorted_ref(&self) -> &[Arc<Event>] {
        self.sorted.as_deref().unwrap_or(&[])
    }

    /// Read the cached newest-first snapshot without rebuilding it. Callers must have invoked
    /// `sorted()` first (which populates the cache). Returns an empty slice if not yet built.
    #[inline]
    pub fn sorted_slice(&self) -> &[Arc<Event>] {
        self.sorted.as_deref().unwrap_or(&[])
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.events.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.events.is_empty()
    }
}

/// Append-only global time index. The global list is only consulted for empty filters and
/// pure time-range queries, which are rare, so it is kept as an append-only vector and sorted
/// lazily into a cached snapshot on first read after a mutation. This keeps the hot insert path
/// free of the largest structure (one entry per event).
#[derive(Default)]
pub struct TimeIndex {
    events: Vec<Arc<Event>>,
    sorted: Option<Vec<Arc<Event>>>,
}

impl TimeIndex {
    #[inline]
    fn push(&mut self, ev: Arc<Event>) {
        self.events.push(ev);
        self.sorted = None;
    }

    /// Return a newest-first snapshot, building it lazily and caching it.
    pub fn sorted(&mut self) -> &[Arc<Event>] {
        if self.sorted.is_none() {
            let mut v = self.events.clone();
            v.sort_unstable_by(|a, b| b.created_at.cmp(&a.created_at));
            self.sorted = Some(v);
        }
        self.sorted.as_deref().unwrap()
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.events.len()
    }
}

/// The full in-memory index. All access goes through the owning `RwLock` in `Store`.
#[derive(Default)]
pub struct Index {
    pub by_id: HashMap<[u8; 32], Arc<Event>>,
    pub by_author: HashMap<[u8; 32], EventList>,
    pub by_kind: HashMap<u32, EventList>,
    pub by_tag: HashMap<(u8, Box<str>), EventList>,
    /// Global created_at-ordered list of every stored event (lazily sorted).
    pub by_time: TimeIndex,
    /// Number of secondary index entries (author/kind/tag postings) for stats.
    pub index_entries: u64,
}

impl Index {
    /// Insert an event into every index. Returns false if the id was already present.
    ///
    /// The caller must hold the write lock.
    pub fn insert(&mut self, ev: Arc<Event>) -> bool {
        if self.by_id.contains_key(&ev.id) {
            return false;
        }
        // Author posting.
        self.by_author
            .entry(ev.pubkey)
            .or_default()
            .push(ev.clone());
        self.index_entries += 1;
        // Kind posting.
        self.by_kind.entry(ev.kind).or_default().push(ev.clone());
        self.index_entries += 1;
        // Single-letter tag postings (NIP-01 addressable filters use single-letter names).
        for tag in &ev.tags {
            if tag.len() >= 2 && tag[0].len() == 1 {
                let name = tag[0].as_bytes()[0];
                let key: (u8, Box<str>) = (name, Box::from(tag[1].as_str()));
                self.by_tag.entry(key).or_default().push(ev.clone());
                self.index_entries += 1;
            }
        }
        // Global time list (append-only, lazily sorted).
        self.by_time.push(ev.clone());
        // Primary map last so a partially-built event is never visible by id.
        self.by_id.insert(ev.id, ev);
        true
    }

    /// No-op: lists are always ordered. Kept for API compatibility with recovery.
    pub fn finalize(&mut self) {}

    #[inline]
    pub fn get(&self, id: &[u8; 32]) -> Option<&Arc<Event>> {
        self.by_id.get(id)
    }

    #[inline]
    pub fn author_list(&self, pk: &[u8; 32]) -> Option<&EventList> {
        self.by_author.get(pk)
    }

    #[inline]
    pub fn kind_list(&self, kind: u32) -> Option<&EventList> {
        self.by_kind.get(&kind)
    }

    #[inline]
    pub fn tag_list(&self, name: u8, value: &str) -> Option<&EventList> {
        self.by_tag.get(&(name, Box::from(value)))
    }

    /// Build the lazily-sorted global time snapshot if it is stale. Requires `&mut self`.
    #[inline]
    pub fn ensure_time_sorted(&mut self) {
        let _ = self.by_time.sorted();
    }

    /// Read the cached newest-first global time snapshot. Must be called after
    /// `ensure_time_sorted`.
    #[inline]
    pub fn time_slice(&self) -> &[Arc<Event>] {
        self.by_time.sorted.as_deref().unwrap_or(&[])
    }
}
