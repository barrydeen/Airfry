//! Connection registry, subscription registry and live event fanout.
//!
//! Every accepted event is broadcast to all open subscriptions whose filters match. Each
//! connection owns a bounded `mpsc` channel; when a consumer cannot keep up the send fails
//! (`Full`) and the connection is marked as a slow consumer and asked to close, rather than
//! letting memory grow without bound.
//!
//! Frames are carried as pre-serialized UTF-8 JSON bytes so that the fanout path serializes an
//! event exactly once and only re-writes the small `["EVENT","<sub>",` prefix per subscription.

use nostr_core::{Event, Filter};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::sync::{mpsc, Notify};

/// Capacity of the per-connection outbound queue.
pub const OUTBOUND_QUEUE_CAPACITY: usize = 1024;

/// A live subscription registered on a connection.
#[derive(Clone)]
pub struct Subscription {
    pub sub_id: String,
    pub filters: Vec<Filter>,
}

/// Handle to one connected client.
pub struct ConnHandle {
    pub conn_id: u64,
    pub tx: mpsc::Sender<Vec<u8>>,
    /// Set when the outbound queue overflowed; the connection should be closed.
    pub slow: AtomicBool,
    /// Woken when the connection should be torn down (slow consumer / shutdown).
    pub close: Notify,
}

impl ConnHandle {
    /// Try to enqueue a frame. Returns false when the queue is full (slow consumer).
    pub fn try_send(&self, frame: Vec<u8>) -> bool {
        match self.tx.try_send(frame) {
            Ok(()) => true,
            Err(mpsc::error::TrySendError::Full(_)) => {
                self.slow.store(true, Ordering::Relaxed);
                false
            }
            Err(mpsc::error::TrySendError::Closed(_)) => false,
        }
    }

    pub fn is_slow(&self) -> bool {
        self.slow.load(Ordering::Relaxed)
    }

    /// Ask the owning connection task to close.
    pub fn request_close(&self) {
        self.close.notify_one();
    }
}

/// Shared relay state: connections and their subscriptions.
pub struct Hub {
    conns: parking_lot::RwLock<HashMap<u64, Arc<ConnHandle>>>,
    subs: parking_lot::RwLock<HashMap<u64, Vec<Subscription>>>,
    next_conn_id: AtomicU64,
    total_subs: AtomicUsize,
}

impl Default for Hub {
    fn default() -> Self {
        Hub::new()
    }
}

impl Hub {
    pub fn new() -> Hub {
        Hub {
            conns: parking_lot::RwLock::new(HashMap::new()),
            subs: parking_lot::RwLock::new(HashMap::new()),
            next_conn_id: AtomicU64::new(1),
            total_subs: AtomicUsize::new(0),
        }
    }

    /// Register a connection with a caller-provided channel (used by the server loop).
    pub fn register_with(&self, tx: mpsc::Sender<Vec<u8>>) -> (u64, Arc<ConnHandle>) {
        let id = self.next_conn_id.fetch_add(1, Ordering::Relaxed);
        let handle = Arc::new(ConnHandle {
            conn_id: id,
            tx,
            slow: AtomicBool::new(false),
            close: Notify::new(),
        });
        self.conns.write().insert(id, handle.clone());
        (id, handle)
    }

    /// Remove a connection and all of its subscriptions.
    pub fn unregister(&self, conn_id: u64) {
        self.conns.write().remove(&conn_id);
        let removed = self
            .subs
            .write()
            .remove(&conn_id)
            .map(|v| v.len())
            .unwrap_or(0);
        if removed > 0 {
            self.total_subs.fetch_sub(removed, Ordering::Relaxed);
        }
    }

    /// Add (or replace) a subscription for a connection. Returns false when the per-connection
    /// subscription limit would be exceeded.
    pub fn add_subscription(&self, conn_id: u64, sub: Subscription, max_subs: usize) -> bool {
        let mut subs = self.subs.write();
        let entry = subs.entry(conn_id).or_default();
        if let Some(existing) = entry.iter_mut().find(|s| s.sub_id == sub.sub_id) {
            *existing = sub;
            return true;
        }
        if entry.len() >= max_subs {
            return false;
        }
        entry.push(sub);
        drop(subs);
        self.total_subs.fetch_add(1, Ordering::Relaxed);
        true
    }

    /// Remove a subscription. Returns true when it existed.
    pub fn remove_subscription(&self, conn_id: u64, sub_id: &str) -> bool {
        let mut subs = self.subs.write();
        let Some(entry) = subs.get_mut(&conn_id) else {
            return false;
        };
        let before = entry.len();
        entry.retain(|s| s.sub_id != sub_id);
        let removed = before - entry.len();
        drop(subs);
        if removed > 0 {
            self.total_subs.fetch_sub(removed, Ordering::Relaxed);
        }
        removed > 0
    }

    /// True when the connection already has a subscription with this id.
    pub fn has_subscription(&self, conn_id: u64, sub_id: &str) -> bool {
        self.subs
            .read()
            .get(&conn_id)
            .map(|v| v.iter().any(|s| s.sub_id == sub_id))
            .unwrap_or(false)
    }

    /// Look up a connection handle by id.
    pub fn handle(&self, conn_id: u64) -> Option<Arc<ConnHandle>> {
        self.conns.read().get(&conn_id).cloned()
    }

    /// Snapshot of all connection handles (used for shutdown).
    pub fn handles(&self) -> Vec<Arc<ConnHandle>> {
        self.conns.read().values().cloned().collect()
    }

    /// Number of live subscriptions for a connection.
    pub fn subscription_count(&self, conn_id: u64) -> usize {
        self.subs.read().get(&conn_id).map(|v| v.len()).unwrap_or(0)
    }

    pub fn connection_count(&self) -> usize {
        self.conns.read().len()
    }

    pub fn total_subscriptions(&self) -> usize {
        self.total_subs.load(Ordering::Relaxed)
    }

    /// Fan an accepted event out to every matching subscription.
    ///
    /// The event JSON is serialized once and reused for every subscription frame; only the
    /// `["EVENT","<sub_id>",` prefix differs. Returns the number of frames enqueued and the ids
    /// of connections that turned out to be slow consumers (the caller closes them).
    pub fn fanout(&self, event: &Event) -> (usize, Vec<u64>) {
        let conns = self.conns.read();
        let subs = self.subs.read();
        if subs.is_empty() {
            return (0, Vec::new());
        }
        let mut event_json = Vec::with_capacity(event.wire_size());
        event.write_json(&mut event_json);

        let mut sent = 0usize;
        let mut slow = Vec::new();
        let mut frame = Vec::with_capacity(event_json.len() + 64);
        for (conn_id, list) in subs.iter() {
            let Some(handle) = conns.get(conn_id) else {
                continue;
            };
            for sub in list {
                if sub.filters.iter().any(|f| f.matches(event)) {
                    frame.clear();
                    nostr_core::write_event_frame(&mut frame, &sub.sub_id, &event_json);
                    if handle.try_send(frame.clone()) {
                        sent += 1;
                    } else if handle.is_slow() {
                        slow.push(*conn_id);
                        break;
                    }
                }
            }
        }
        (sent, slow)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nostr_core::Keys;

    fn filter_kind(kind: u32) -> Filter {
        Filter {
            kinds: Some(vec![kind]),
            ..Default::default()
        }
    }

    fn text(frame: Vec<u8>) -> String {
        String::from_utf8(frame).expect("frame is utf-8")
    }

    #[tokio::test]
    async fn fanout_matches_only_matching_subs() {
        let hub = Hub::new();
        let (tx1, mut rx1) = mpsc::channel(OUTBOUND_QUEUE_CAPACITY);
        let (tx2, mut rx2) = mpsc::channel(OUTBOUND_QUEUE_CAPACITY);
        let (id1, _) = hub.register_with(tx1);
        let (id2, _) = hub.register_with(tx2);
        assert!(hub.add_subscription(
            id1,
            Subscription {
                sub_id: "a".into(),
                filters: vec![filter_kind(1)]
            },
            8
        ));
        assert!(hub.add_subscription(
            id2,
            Subscription {
                sub_id: "b".into(),
                filters: vec![filter_kind(7)]
            },
            8
        ));
        let keys = Keys::from_label("hub-test");
        let ev = keys.sign_event(1_700_000_000, 1, vec![], "hi");
        let (sent, slow) = hub.fanout(&ev);
        assert_eq!(sent, 1);
        assert!(slow.is_empty());
        let frame = text(rx1.try_recv().expect("frame for matching sub"));
        assert!(frame.starts_with("[\"EVENT\",\"a\",{"));
        assert!(frame.contains(&hex::encode(ev.id)));
        assert!(rx2.try_recv().is_err());
    }

    #[tokio::test]
    async fn fanout_serializes_event_once_for_many_subs() {
        let hub = Hub::new();
        let (tx, mut rx) = mpsc::channel(OUTBOUND_QUEUE_CAPACITY);
        let (id, _) = hub.register_with(tx);
        for sub in ["s1", "s2", "s3"] {
            assert!(hub.add_subscription(
                id,
                Subscription {
                    sub_id: sub.into(),
                    filters: vec![Filter::default()]
                },
                8
            ));
        }
        let keys = Keys::from_label("hub-multi");
        let ev = keys.sign_event(1_700_000_000, 1, vec![], "multi");
        let (sent, slow) = hub.fanout(&ev);
        assert_eq!(sent, 3);
        assert!(slow.is_empty());
        for sub in ["s1", "s2", "s3"] {
            let frame = text(rx.try_recv().expect("frame"));
            assert!(frame.starts_with(&format!("[\"EVENT\",\"{sub}\",{{")));
        }
    }

    #[tokio::test]
    async fn subscription_limit_enforced() {
        let hub = Hub::new();
        let (tx, _rx) = mpsc::channel(OUTBOUND_QUEUE_CAPACITY);
        let (id, _) = hub.register_with(tx);
        assert!(hub.add_subscription(
            id,
            Subscription {
                sub_id: "a".into(),
                filters: vec![]
            },
            1
        ));
        assert!(!hub.add_subscription(
            id,
            Subscription {
                sub_id: "b".into(),
                filters: vec![]
            },
            1
        ));
        // replacing an existing sub is always allowed
        assert!(hub.add_subscription(
            id,
            Subscription {
                sub_id: "a".into(),
                filters: vec![]
            },
            1
        ));
        assert!(hub.remove_subscription(id, "a"));
        assert_eq!(hub.subscription_count(id), 0);
    }

    #[tokio::test]
    async fn slow_consumer_detected() {
        let hub = Hub::new();
        let (tx, _rx) = mpsc::channel(1);
        let (id, _) = hub.register_with(tx);
        assert!(hub.add_subscription(
            id,
            Subscription {
                sub_id: "a".into(),
                filters: vec![Filter::default()]
            },
            8
        ));
        let keys = Keys::from_label("slow");
        let ev = keys.sign_event(1_700_000_000, 1, vec![], "hi");
        let (sent1, slow1) = hub.fanout(&ev);
        assert_eq!(sent1, 1);
        assert!(slow1.is_empty());
        let (sent2, slow2) = hub.fanout(&ev);
        assert_eq!(sent2, 0);
        assert_eq!(slow2, vec![id]);
    }
}
