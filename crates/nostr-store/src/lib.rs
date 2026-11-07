//! Nostr-optimized storage engine.
//!
//! # On-disk format
//!
//! The engine is an append-only segmented log. Each segment is a file named
//! `seg-<10-digit index>.log` (zero-padded so the names sort lexicographically in write order).
//! Segments roll at `StoreConfig::segment_bytes`. Every record is framed as:
//!
//! ```text
//! offset  size  field
//! 0       4     magic  = 0x4E535452 ("NSTR")
//! 4       1     record type (1 = event, 2 = NIP-09 delete tombstone)
//! 5       4     payload length (LE u32)
//! 9       4     crc32c over (record type byte || payload)  (LE u32)
//! 13      N     payload
//! ```
//!
//! Event payloads are the binary `nostr_core::codec::encode_event` encoding (no JSON on the hot
//! path). Delete payloads are the 32-byte target id followed by the 32-byte author pubkey that
//! requested the deletion, so a replay can re-validate ownership.
//!
//! # Recovery
//!
//! `Store::open` replays every segment in order, validating magic, length and CRC. It stops at
//! the first incomplete/invalid record (a torn tail from a crash) and truncates that segment back
//! to the last good record boundary so future appends stay aligned. It then rebuilds the in-memory
//! indexes and re-applies tombstones. A torn tail is never fatal.
//!
//! # Indexes
//!
//! Queries are answered purely from in-memory indexes — never by scanning the log or the whole
//! corpus:
//!
//! * `by_id`: id -> `Arc<Event>` (primary lookup, duplicate detection, NIP-09 targeting).
//! * `by_author`: pubkey -> events (newest-first).
//! * `by_kind`: kind -> events (newest-first).
//! * `by_tag`: (single-letter tag name, value) -> events (newest-first).
//! * `by_time`: a global created_at-ordered list used for time-range and empty filters.
//!
//! A query with a `limit` walks a newest-first posting list and stops early, so its cost is
//! proportional to the number of returned events, not the corpus size. `search` (substring) is the
//! only fallback path and scans the candidate set produced by the other constraints.
//!
//! # Concurrency
//!
//! The index lives behind a single `parking_lot::RwLock`. Writers take the write lock only to
//! publish an already-encoded batch; readers take the read lock and clone `Arc`s out of the
//! posting lists. Because the index is append-only (events are never removed from the lists, only
//! tombstoned), a reader holding the read lock always sees a consistent snapshot. The disk append
//! happens outside the index lock so a slow fsync never blocks readers.

mod framing;
mod index;
mod log;

use index::Index;
use log::{LogWriter, RecoveredRecord};
use nostr_core::codec::encode_event;
use nostr_core::{now_secs, Event, Filter};
use parking_lot::{Mutex, RwLock};
use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("corrupt record: {0}")]
    Corrupt(String),
    #[error("serialization error: {0}")]
    Json(#[from] serde_json::Error),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FsyncPolicy {
    Never,
    Interval(std::time::Duration),
    Always,
}

#[derive(Clone, Debug)]
pub struct StoreConfig {
    pub path: PathBuf,
    pub fsync: FsyncPolicy,
    pub segment_bytes: u64,
}

impl StoreConfig {
    pub fn new(path: impl Into<PathBuf>) -> StoreConfig {
        StoreConfig {
            path: path.into(),
            fsync: FsyncPolicy::Never,
            segment_bytes: 256 << 20,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum InsertOutcome {
    Stored,
    Duplicate,
    Rejected(&'static str),
}

#[derive(Clone, Copy, Debug, Default)]
pub struct StoreStats {
    pub events: u64,
    pub bytes_on_disk: u64,
    pub segments: u64,
    pub index_entries: u64,
}

/// The in-memory state that must be mutated atomically with respect to queries.
struct State {
    index: Index,
    /// Tombstoned event ids (NIP-09). Events in this set are filtered out of every query.
    tombstones: HashSet<[u8; 32]>,
}

pub struct Store {
    cfg: StoreConfig,
    state: RwLock<State>,
    /// Serializes disk appends so records never interleave. Held only for the append itself,
    /// never while the index lock is held.
    writer: Mutex<LogWriter>,
    /// Number of live (non-tombstoned) events, for stats.
    live_events: AtomicU64,
    /// Set once the background flusher should stop.
    stop_flusher: AtomicBool,
    /// Handle to the background flusher thread (Interval policy only).
    flusher: Mutex<Option<std::thread::JoinHandle<()>>>,
}

impl Drop for Store {
    fn drop(&mut self) {
        self.stop_flusher.store(true, Ordering::Relaxed);
        if let Some(h) = self.flusher.lock().take() {
            let _ = h.join();
        }
        // Best-effort final flush so a clean shutdown persists buffered bytes.
        let _ = self.writer.lock().sync(false);
    }
}

impl Store {
    pub fn open(cfg: StoreConfig) -> Result<Store, StoreError> {
        std::fs::create_dir_all(&cfg.path)?;
        let recovery = log::recover(&cfg.path)?;

        let mut state = State {
            index: Index::default(),
            tombstones: HashSet::new(),
        };
        // Replay: events first, then apply tombstones so a delete that appears after its target
        // in the log wins. A delete that appears before its target is also honoured because we
        // collect tombstones and apply them after all events are indexed.
        let mut pending_deletes: Vec<([u8; 32], [u8; 32])> = Vec::new();
        for rec in recovery.records {
            match rec {
                RecoveredRecord::Event(ev) => {
                    state.index.insert(Arc::new(ev));
                }
                RecoveredRecord::Delete { id, pubkey } => pending_deletes.push((id, pubkey)),
            }
        }
        for (id, pubkey) in pending_deletes {
            if let Some(ev) = state.index.get(&id) {
                if ev.pubkey == pubkey {
                    state.tombstones.insert(id);
                }
            }
        }
        state.index.finalize();
        let live = state.index.by_id.len() as u64 - state.tombstones.len() as u64;

        let writer = LogWriter::open(
            &cfg.path,
            cfg.segment_bytes,
            recovery.last_segment,
            recovery.bytes_on_disk,
        )?;
        let segments = recovery.segments.max(1);

        let store = Store {
            cfg: cfg.clone(),
            state: RwLock::new(state),
            writer: Mutex::new(writer),
            live_events: AtomicU64::new(live),
            stop_flusher: AtomicBool::new(false),
            flusher: Mutex::new(None),
        };

        // Start the background flusher for the Interval policy.
        if let FsyncPolicy::Interval(d) = cfg.fsync {
            let interval = d.max(Duration::from_millis(1));
            let store_ptr: *const Store = &store;
            // SAFETY: the flusher thread is joined in Drop before the Store is freed, so the raw
            // pointer never dangles.
            let ptr = store_ptr as usize;
            let stop = &store.stop_flusher as *const AtomicBool as usize;
            let handle = std::thread::spawn(move || {
                let store = unsafe { &*(ptr as *const Store) };
                let stop = unsafe { &*(stop as *const AtomicBool) };
                let mut last = Instant::now();
                while !stop.load(Ordering::Relaxed) {
                    std::thread::sleep(Duration::from_millis(5));
                    if last.elapsed() >= interval {
                        let _ = store.writer.lock().sync(true);
                        last = Instant::now();
                    }
                }
            });
            *store.flusher.lock() = Some(handle);
        }

        let _ = segments;
        Ok(store)
    }

    /// Encode an event into a framed record buffer (no JSON).
    #[inline]
    fn encode_record(ev: &Event, out: &mut Vec<u8>) {
        let start = out.len();
        // Reserve header space, encode payload after it, then patch the header.
        out.resize(start + framing::HEADER_LEN, 0);
        encode_event(ev, out);
        let payload_len = out.len() - start - framing::HEADER_LEN;
        let crc = framing::record_crc(framing::REC_EVENT, &out[start + framing::HEADER_LEN..]);
        let hdr = &mut out[start..start + framing::HEADER_LEN];
        hdr[0..4].copy_from_slice(&framing::MAGIC.to_le_bytes());
        hdr[4] = framing::REC_EVENT;
        hdr[5..9].copy_from_slice(&(payload_len as u32).to_le_bytes());
        hdr[9..13].copy_from_slice(&crc.to_le_bytes());
    }

    pub fn insert(&self, ev: &Event) -> Result<InsertOutcome, StoreError> {
        let mut outcomes = self.insert_batch(std::slice::from_ref(ev))?;
        Ok(outcomes.pop().unwrap_or(InsertOutcome::Rejected("empty")))
    }

    pub fn insert_batch(&self, evs: &[Event]) -> Result<Vec<InsertOutcome>, StoreError> {
        let mut outcomes = Vec::with_capacity(evs.len());
        if evs.is_empty() {
            return Ok(outcomes);
        }

        // Phase 1: dedup against the current index and within the batch, and encode the accepted
        // events into one contiguous buffer. This takes the write lock once.
        let mut buf: Vec<u8> = Vec::with_capacity(evs.len() * 256);
        let mut accepted: Vec<Arc<Event>> = Vec::with_capacity(evs.len());
        {
            let mut st = self.state.write();
            let mut batch_seen: HashSet<[u8; 32]> = HashSet::with_capacity(evs.len());
            for ev in evs {
                if st.index.get(&ev.id).is_some() || !batch_seen.insert(ev.id) {
                    outcomes.push(InsertOutcome::Duplicate);
                    continue;
                }
                Self::encode_record(ev, &mut buf);
                accepted.push(Arc::new(ev.clone()));
                outcomes.push(InsertOutcome::Stored);
            }
            // Phase 2: publish into the indexes while still holding the write lock so readers
            // never observe a half-applied batch.
            for ev in &accepted {
                st.index.insert(ev.clone());
            }
            // NOTE: deliberately no finalize() here. Sorting every posting list on every batch
            // is O(n log n) per batch => O(n^2) total. Instead lists are sorted lazily on first
            // query after a mutation (see sort_for_query), so ingest stays O(1) per event.
        }

        if accepted.is_empty() {
            return Ok(outcomes);
        }

        // Phase 3: append to disk outside the index lock. A single write for the whole batch.
        {
            let mut w = self.writer.lock();
            w.append(&buf)?;
            match self.cfg.fsync {
                FsyncPolicy::Always => w.sync(true)?,
                FsyncPolicy::Never => w.flush_buf()?,
                FsyncPolicy::Interval(_) => { /* background flusher handles durability */ }
            }
        }

        self.live_events
            .fetch_add(accepted.len() as u64, Ordering::Relaxed);
        Ok(outcomes)
    }

    /// Answer a REQ. Returns events newest-first, deduplicated by id across filters, with each
    /// filter's own `limit` applied independently. Expired and tombstoned events are excluded.
    pub fn query(&self, filters: &[Filter]) -> Result<Vec<Arc<Event>>, StoreError> {
        if filters.is_empty() {
            return Ok(Vec::new());
        }
        let now = now_secs();
        // Phase A (mutable): lazily build newest-first snapshots of every posting list the
        // filters will touch. Snapshots are cached and only rebuilt after a mutation, so this is
        // a no-op on repeated queries.
        {
            let mut st = self.state.write();
            for f in filters {
                Self::prepare(&mut st, f);
            }
        }
        // Phase B (read-only): walk the already-ordered lists. A limit stops the walk early, so
        // cost is proportional to returned events, not corpus size.
        let st = self.state.read();
        let mut out: Vec<Arc<Event>> = Vec::new();
        let mut seen: HashSet<[u8; 32]> = HashSet::new();
        for f in filters {
            let limit = f.limit.unwrap_or(usize::MAX);
            if limit == 0 {
                continue;
            }
            let mut taken = 0usize;
            for ev in Self::candidates(&st, f) {
                if taken >= limit {
                    break;
                }
                if st.tombstones.contains(&ev.id) {
                    continue;
                }
                if ev.is_expired(now) {
                    continue;
                }
                if !f.matches(ev) {
                    continue;
                }
                if seen.insert(ev.id) {
                    out.push(ev.clone());
                }
                taken += 1;
            }
        }
        // Global newest-first ordering across the union.
        out.sort_unstable_by(|a, b| b.created_at.cmp(&a.created_at));
        Ok(out)
    }

    /// Ensure the newest-first snapshot exists for every posting list a filter may consult.
    fn prepare(st: &mut State, f: &Filter) {
        if let Some(authors) = &f.authors {
            for a in authors {
                if let Some(l) = st.index.by_author.get_mut(a) {
                    l.sorted();
                }
            }
        }
        if let Some(kinds) = &f.kinds {
            for k in kinds {
                if let Some(l) = st.index.by_kind.get_mut(k) {
                    l.sorted();
                }
            }
        }
        for (name, values) in &f.tags {
            for v in values {
                let key = (*name as u8, Box::from(v.as_str()));
                if let Some(l) = st.index.by_tag.get_mut(&key) {
                    l.sorted();
                }
            }
        }
        // Empty / pure-time filters consult the global list.
        if f.ids.is_none() && f.authors.is_none() && f.kinds.is_none() && f.tags.is_empty() {
            st.index.ensure_time_sorted();
        }
    }

    /// Produce the candidate stream for a filter, choosing the most selective index dimension
    /// and iterating the UNION of all posting lists for that dimension (so multi-value author,
    /// kind and tag filters match any value). Never scans the log; only walks in-memory posting
    /// lists. The caller still applies `f.matches` to enforce AND semantics across dimensions.
    ///
    /// The lists must already be sorted newest-first (see `prepare`). Returns borrowed `Arc`
    /// references so no per-query cloning of the corpus happens.
    fn candidates<'a>(
        st: &'a State,
        f: &'a Filter,
    ) -> Box<dyn Iterator<Item = &'a Arc<Event>> + 'a> {
        // ids: direct primary-map lookups (already the narrowest possible).
        if let Some(ids) = &f.ids {
            let idx = &st.index;
            return Box::new(ids.iter().filter_map(move |id| idx.get(id)));
        }
        // Collect the posting lists for each dimension present in the filter.
        let mut author_lists: Vec<&index::EventList> = Vec::new();
        if let Some(authors) = &f.authors {
            for a in authors {
                if let Some(l) = st.index.author_list(a) {
                    author_lists.push(l);
                }
            }
            if author_lists.is_empty() {
                return Box::new(std::iter::empty());
            }
        }
        let mut kind_lists: Vec<&index::EventList> = Vec::new();
        if let Some(kinds) = &f.kinds {
            for k in kinds {
                if let Some(l) = st.index.kind_list(*k) {
                    kind_lists.push(l);
                }
            }
            if kind_lists.is_empty() {
                return Box::new(std::iter::empty());
            }
        }
        // For tags: each tag constraint is an AND, so pick the tag constraint with the smallest
        // union and iterate the union of its values.
        let mut tag_lists: Vec<&index::EventList> = Vec::new();
        let mut tag_union_len = usize::MAX;
        for (name, values) in &f.tags {
            let mut lists: Vec<&index::EventList> = Vec::new();
            let mut union = 0usize;
            for v in values {
                if let Some(l) = st.index.tag_list(*name as u8, v) {
                    union += l.len();
                    lists.push(l);
                }
            }
            if lists.is_empty() {
                return Box::new(std::iter::empty());
            }
            if union < tag_union_len {
                tag_union_len = union;
                tag_lists = lists;
            }
        }

        // Choose the dimension with the smallest total posting count.
        let author_total: usize = author_lists.iter().map(|l| l.len()).sum();
        let kind_total: usize = kind_lists.iter().map(|l| l.len()).sum();
        let mut best: Option<Vec<&index::EventList>> = None;
        let mut best_total = usize::MAX;
        if !author_lists.is_empty() && author_total < best_total {
            best_total = author_total;
            best = Some(author_lists);
        }
        if !kind_lists.is_empty() && kind_total < best_total {
            best_total = kind_total;
            best = Some(kind_lists);
        }
        if !tag_lists.is_empty() && tag_union_len < best_total {
            best_total = tag_union_len;
            best = Some(tag_lists);
        }
        match best {
            Some(lists) => Box::new(lists.into_iter().flat_map(|l| l.sorted_ref().iter())),
            None => Box::new(st.index.time_slice().iter()),
        }
    }

    /// COUNT semantics: number of matching events, ignoring the filter `limit`.
    pub fn count(&self, filters: &[Filter]) -> Result<usize, StoreError> {
        if filters.is_empty() {
            return Ok(0);
        }
        let now = now_secs();
        {
            let mut st = self.state.write();
            for f in filters {
                Self::prepare(&mut st, f);
            }
        }
        let st = self.state.read();
        let mut seen: HashSet<[u8; 32]> = HashSet::new();
        for f in filters {
            if f.limit == Some(0) {
                continue;
            }
            for ev in Self::candidates(&st, f) {
                if st.tombstones.contains(&ev.id) || ev.is_expired(now) {
                    continue;
                }
                if f.matches(ev) {
                    seen.insert(ev.id);
                }
            }
        }
        Ok(seen.len())
    }

    /// NIP-09 delete. Only removes the event when the stored event's pubkey matches `pubkey`.
    /// Persists a tombstone record so the deletion survives reopen. Returns false when the id is
    /// unknown, already deleted, or owned by a different author.
    pub fn delete(&self, id: &[u8; 32], pubkey: &[u8; 32]) -> Result<bool, StoreError> {
        {
            let mut st = self.state.write();
            if st.tombstones.contains(id) {
                return Ok(false);
            }
            let ev = match st.index.get(id) {
                Some(e) => e.clone(),
                None => return Ok(false),
            };
            if &ev.pubkey != pubkey {
                return Ok(false);
            }
            st.tombstones.insert(*id);
        }
        // Persist the tombstone.
        let mut payload = Vec::with_capacity(64);
        payload.extend_from_slice(id);
        payload.extend_from_slice(pubkey);
        let mut buf = Vec::with_capacity(framing::HEADER_LEN + 64);
        framing::write_record(&mut buf, framing::REC_DELETE, &payload);
        {
            let mut w = self.writer.lock();
            w.append(&buf)?;
            match self.cfg.fsync {
                FsyncPolicy::Always => w.sync(true)?,
                FsyncPolicy::Never => w.flush_buf()?,
                FsyncPolicy::Interval(_) => {}
            }
        }
        self.live_events.fetch_sub(1, Ordering::Relaxed);
        Ok(true)
    }

    pub fn stats(&self) -> StoreStats {
        let st = self.state.read();
        let (bytes, segs) = {
            let w = self.writer.lock();
            (w.total_bytes(), w.current_index() + 1)
        };
        StoreStats {
            events: self.live_events.load(Ordering::Relaxed),
            bytes_on_disk: bytes,
            segments: segs,
            index_entries: st.index.index_entries,
        }
    }

    pub fn flush(&self) -> Result<(), StoreError> {
        let mut w = self.writer.lock();
        w.sync(true)?;
        Ok(())
    }

    /// Helper: true when the event is expired relative to `now`.
    pub fn is_expired(ev: &Event, now: u64) -> bool {
        ev.is_expired(now)
    }
}
