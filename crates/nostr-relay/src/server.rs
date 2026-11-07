//! Relay server: TCP accept loop, HTTP routing (NIP-11 / health), WebSocket upgrade and the
//! per-connection NIP-01 message loop.
//!
//! Design notes for throughput:
//! * Frames are built as raw UTF-8 JSON bytes with `nostr_core`'s allocation-free writers, so
//!   the hot paths never build a `serde_json::Value` for outbound messages.
//! * Live fanout serializes an event once and reuses it for every matching subscription.
//! * Outbound frames are drained from the per-connection queue in batches and written with a
//!   single flush, which keeps syscall count low under load.

use crate::config::Config;
use crate::http::{read_request_head, HttpError, HttpRequest, HttpResponse, PrefixedStream};
use crate::hub::{Hub, Subscription, OUTBOUND_QUEUE_CAPACITY};
use crate::nip11;
use crate::util::{extract_event_id, random_hex};
use futures_util::{SinkExt, StreamExt};
use nostr_core::{now_secs, Event, Filter, RelayMessage};
use nostr_store::{InsertOutcome, Store, StoreConfig};
use serde_json::Value;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{broadcast, mpsc};
use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;
use tokio_tungstenite::tungstenite::Message;
use tracing::{debug, info, warn};

/// Maximum size of an HTTP request head we are willing to read.
const MAX_HTTP_HEAD: usize = 16 * 1024;

/// Maximum number of queued frames written per flush.
const MAX_WRITE_BATCH: usize = 256;

/// Shared relay state.
pub struct Relay {
    pub cfg: Arc<Config>,
    pub store: Arc<Store>,
    pub hub: Arc<Hub>,
    shutdown_tx: broadcast::Sender<()>,
    /// Number of connections that have been accepted (monotonic, for metrics).
    pub accepted: AtomicU64,
}

impl Relay {
    /// Open the store and build the relay.
    pub fn new(cfg: Config) -> Result<Arc<Relay>, Box<dyn std::error::Error + Send + Sync>> {
        let store_cfg = StoreConfig {
            path: cfg.data_dir.clone(),
            fsync: cfg.fsync_policy,
            segment_bytes: cfg.segment_bytes,
        };
        let store = Store::open(store_cfg)?;
        let (shutdown_tx, _) = broadcast::channel(8);
        Ok(Arc::new(Relay {
            cfg: Arc::new(cfg),
            store: Arc::new(store),
            hub: Arc::new(Hub::new()),
            shutdown_tx,
            accepted: AtomicU64::new(0),
        }))
    }

    /// Subscribe to the shutdown signal.
    pub fn shutdown_receiver(&self) -> broadcast::Receiver<()> {
        self.shutdown_tx.subscribe()
    }

    /// Trigger graceful shutdown: stop accepting, close connections, flush the store.
    pub fn shutdown(&self) {
        let _ = self.shutdown_tx.send(());
        for handle in self.hub.handles() {
            handle.request_close();
        }
    }

    /// Run the accept loop until shutdown is signalled.
    pub async fn serve(self: Arc<Self>, listener: TcpListener) -> std::io::Result<()> {
        let mut shutdown = self.shutdown_receiver();
        loop {
            tokio::select! {
                biased;
                _ = shutdown.recv() => {
                    info!("shutdown signalled, stopping accept loop");
                    break;
                }
                accepted = listener.accept() => {
                    match accepted {
                        Ok((stream, peer)) => {
                            self.accepted.fetch_add(1, Ordering::Relaxed);
                            let relay = self.clone();
                            tokio::spawn(async move {
                                if let Err(e) = relay.handle_connection(stream, peer).await {
                                    debug!(%peer, error = %e, "connection ended with error");
                                }
                            });
                        }
                        Err(e) => {
                            warn!(error = %e, "accept failed");
                        }
                    }
                }
            }
        }
        // Give in-flight connections a moment to observe the close notification.
        let _ = self.store.flush();
        Ok(())
    }

    async fn handle_connection(
        self: Arc<Self>,
        mut stream: TcpStream,
        peer: SocketAddr,
    ) -> std::io::Result<()> {
        let _ = stream.set_nodelay(true);

        let parsed = match read_request_head(&mut stream, MAX_HTTP_HEAD).await {
            Ok(v) => v,
            Err(HttpError::Eof) => return Ok(()),
            Err(e) => {
                debug!(%peer, error = %e, "bad http request");
                let resp = HttpResponse::text(400, "Bad Request", format!("bad request: {e}\n"));
                let _ = stream.write_all(&resp.to_bytes()).await;
                let _ = stream.shutdown().await;
                return Ok(());
            }
        };

        if parsed.request.is_websocket_upgrade() {
            // tungstenite parses the request itself, so replay every byte we consumed.
            return self
                .handle_websocket(stream, parsed.raw, parsed.request, peer)
                .await;
        }

        let resp = self.route_http(&parsed.request);
        let _ = stream.write_all(&resp.to_bytes()).await;
        let _ = stream.shutdown().await;
        Ok(())
    }

    /// Route a plain HTTP request to the NIP-11 document, `/health`, or the landing page.
    fn route_http(&self, req: &HttpRequest) -> HttpResponse {
        if req.method != "GET" && req.method != "HEAD" {
            return HttpResponse::text(405, "Method Not Allowed", "method not allowed\n");
        }
        match req.path.as_str() {
            "/health" => {
                let stats = self.store.stats();
                let body = nip11::health(
                    &stats,
                    self.hub.connection_count(),
                    self.hub.total_subscriptions(),
                );
                HttpResponse::json(200, "OK", body.to_string())
            }
            "/" => {
                if req.wants_nostr_json() {
                    let body = nip11::relay_info(&self.cfg);
                    HttpResponse::nostr_json(200, "OK", body.to_string())
                        .with_header("Access-Control-Allow-Headers", "Accept")
                } else {
                    HttpResponse::html(200, "OK", nip11::landing_page(&self.cfg))
                }
            }
            _ => HttpResponse::text(404, "Not Found", "not found\n"),
        }
    }

    async fn handle_websocket(
        self: Arc<Self>,
        stream: TcpStream,
        leftover: Vec<u8>,
        req: HttpRequest,
        peer: SocketAddr,
    ) -> std::io::Result<()> {
        if self.cfg.max_conns > 0 && self.hub.connection_count() >= self.cfg.max_conns {
            let mut stream = stream;
            let resp = HttpResponse::text(503, "Service Unavailable", "too many connections\n");
            let _ = stream.write_all(&resp.to_bytes()).await;
            let _ = stream.shutdown().await;
            return Ok(());
        }

        let max_msg = self.cfg.max_event_bytes.saturating_mul(4).max(1 << 20);
        let ws_cfg = WebSocketConfig {
            write_buffer_size: 64 * 1024,
            max_write_buffer_size: 8 * 1024 * 1024,
            max_message_size: Some(max_msg),
            max_frame_size: Some(max_msg),
            ..Default::default()
        };

        let path_ok = req.path == "/";
        let upgraded = tokio_tungstenite::accept_hdr_async_with_config(
            PrefixedStream::new(leftover, stream),
            move |_request: &tokio_tungstenite::tungstenite::handshake::server::Request,
                  response: tokio_tungstenite::tungstenite::handshake::server::Response| {
                if !path_ok {
                    let mut err = tokio_tungstenite::tungstenite::handshake::server::ErrorResponse::new(
                        Some("not found".to_string()),
                    );
                    *err.status_mut() = tokio_tungstenite::tungstenite::http::StatusCode::NOT_FOUND;
                    return Err(err);
                }
                Ok(response)
            },
            Some(ws_cfg),
        )
        .await;

        let ws = match upgraded {
            Ok(ws) => ws,
            Err(e) => {
                debug!(%peer, error = %e, "websocket handshake failed");
                return Ok(());
            }
        };

        self.run_session(ws, peer).await;
        Ok(())
    }

    async fn run_session<S>(
        self: Arc<Self>,
        ws: tokio_tungstenite::WebSocketStream<S>,
        peer: SocketAddr,
    ) where
        S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
    {
        let (mut sink, mut incoming) = ws.split();
        let (tx, mut rx) = mpsc::channel::<Vec<u8>>(OUTBOUND_QUEUE_CAPACITY);
        let (conn_id, handle) = self.hub.register_with(tx);
        let mut shutdown = self.shutdown_receiver();
        let challenge = random_hex(16);
        let mut authed_pubkey: Option<[u8; 32]> = None;

        info!(%peer, conn_id, "websocket connected");

        // NIP-42: send the challenge immediately on connect.
        let auth_frame = RelayMessage::Auth(challenge.clone()).to_json_bytes();
        if send_batch(&mut sink, vec![auth_frame]).await.is_some() {
            self.hub.unregister(conn_id);
            return;
        }

        let mut close_reason: Option<String> = None;

        loop {
            tokio::select! {
                biased;
                _ = shutdown.recv() => {
                    close_reason = Some("relay shutting down".to_string());
                    break;
                }
                _ = handle.close.notified() => {
                    close_reason = Some("slow consumer".to_string());
                    break;
                }
                outbound = rx.recv() => {
                    match outbound {
                        Some(frame) => {
                            // Drain whatever else is queued so we can write it in one flush.
                            let mut batch = Vec::with_capacity(8);
                            batch.push(frame);
                            while batch.len() < MAX_WRITE_BATCH {
                                match rx.try_recv() {
                                    Ok(f) => batch.push(f),
                                    Err(_) => break,
                                }
                            }
                            if let Some(reason) = send_batch(&mut sink, batch).await {
                                debug!(%peer, conn_id, reason = %reason, "write failed");
                                close_reason = Some(reason);
                                break;
                            }
                        }
                        None => break,
                    }
                }
                msg = incoming.next() => {
                    match msg {
                        Some(Ok(Message::Text(text))) => {
                            match self.handle_frame(conn_id, &challenge, &mut authed_pubkey, &text).await {
                                Ok(frames) => {
                                    if let Some(reason) = send_batch(&mut sink, frames).await {
                                        close_reason = Some(reason);
                                        break;
                                    }
                                }
                                Err(reason) => {
                                    close_reason = Some(reason);
                                    break;
                                }
                            }
                        }
                        Some(Ok(Message::Binary(bin))) => {
                            match String::from_utf8(bin) {
                                Ok(text) => {
                                    match self.handle_frame(conn_id, &challenge, &mut authed_pubkey, &text).await {
                                        Ok(frames) => {
                                            if let Some(reason) = send_batch(&mut sink, frames).await {
                                                close_reason = Some(reason);
                                                break;
                                            }
                                        }
                                        Err(reason) => {
                                            close_reason = Some(reason);
                                            break;
                                        }
                                    }
                                }
                                Err(_) => {
                                    let notice = RelayMessage::Notice("invalid: binary frame is not utf-8".into()).to_json_bytes();
                                    if let Some(reason) = send_batch(&mut sink, vec![notice]).await {
                                        close_reason = Some(reason);
                                        break;
                                    }
                                }
                            }
                        }
                        Some(Ok(Message::Ping(p))) => {
                            if sink.send(Message::Pong(p)).await.is_err() {
                                break;
                            }
                        }
                        Some(Ok(Message::Pong(_))) => {}
                        Some(Ok(Message::Close(_))) => break,
                        Some(Ok(Message::Frame(_))) => {}
                        Some(Err(e)) => {
                            debug!(%peer, conn_id, error = %e, "websocket read error");
                            break;
                        }
                        None => break,
                    }
                }
            }
        }

        self.hub.unregister(conn_id);
        let _ = sink.send(Message::Close(None)).await;
        let _ = sink.close().await;
        info!(%peer, conn_id, reason = close_reason.as_deref().unwrap_or("client closed"), "websocket disconnected");
    }

    /// Handle one client frame.
    ///
    /// Returns the frames to write back, or `Err(reason)` when the connection must be closed.
    async fn handle_frame(
        self: &Arc<Self>,
        conn_id: u64,
        challenge: &str,
        authed_pubkey: &mut Option<[u8; 32]>,
        raw: &str,
    ) -> Result<Vec<Vec<u8>>, String> {
        let value: Value = match serde_json::from_str(raw) {
            Ok(v) => v,
            Err(e) => {
                return Ok(vec![
                    RelayMessage::Notice(format!("invalid: {e}")).to_json_bytes()
                ]);
            }
        };
        let Some(arr) = value.as_array() else {
            return Ok(vec![RelayMessage::Notice(
                "invalid: expected a JSON array".into(),
            )
            .to_json_bytes()]);
        };
        let Some(msg_type) = arr.first().and_then(|x| x.as_str()) else {
            return Ok(vec![RelayMessage::Notice(
                "invalid: missing message type".into(),
            )
            .to_json_bytes()]);
        };

        match msg_type {
            "EVENT" => Ok(vec![self.handle_event(
                conn_id,
                authed_pubkey,
                raw,
                arr.get(1),
            )]),
            "REQ" => Ok(self.handle_req(conn_id, arr)),
            "COUNT" => Ok(self.handle_count(arr)),
            "CLOSE" => match arr.get(1).and_then(|x| x.as_str()) {
                Some(sub_id) => {
                    self.hub.remove_subscription(conn_id, sub_id);
                    Ok(Vec::new())
                }
                None => Ok(vec![RelayMessage::Notice(
                    "invalid: CLOSE missing subscription id".into(),
                )
                .to_json_bytes()]),
            },
            "AUTH" => Ok(vec![self.handle_auth(challenge, authed_pubkey, arr.get(1))]),
            other => Ok(vec![RelayMessage::Notice(format!(
                "invalid: unknown message type `{other}`"
            ))
            .to_json_bytes()]),
        }
    }

    /// Handle an EVENT frame, returning the OK frame to send.
    fn handle_event(
        &self,
        conn_id: u64,
        authed_pubkey: &Option<[u8; 32]>,
        raw: &str,
        payload: Option<&Value>,
    ) -> Vec<u8> {
        // Echo the event id back in the OK frame even when the payload is unusable, as NIP-01
        // requires. Fall back to a cheap scan of the raw frame for oversized/malformed events.
        let id = payload
            .and_then(|v| v.get("id"))
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .or_else(|| extract_event_id(raw))
            .unwrap_or_default();
        let ok = |ok: bool, msg: &str| {
            RelayMessage::Ok {
                id: id.clone(),
                ok,
                message: msg.to_string(),
            }
            .to_json_bytes()
        };

        if raw.len() > self.cfg.max_event_bytes {
            return ok(false, "invalid: event too large");
        }

        let Some(payload) = payload else {
            return ok(false, "invalid: EVENT missing event object");
        };

        let ev = match Event::from_json_value(payload) {
            Ok(ev) => ev,
            Err(e) => return ok(false, &format!("invalid: {e}")),
        };

        if let Err(e) = ev.validate_shape(self.cfg.max_event_bytes) {
            return ok(false, &format!("invalid: {e}"));
        }

        if self.cfg.verify_events {
            if let Err(e) = ev.verify() {
                let msg = match e {
                    nostr_core::CoreError::IdMismatch => {
                        "invalid: event id does not match".to_string()
                    }
                    nostr_core::CoreError::BadSignature => "invalid: bad signature".to_string(),
                    other => format!("invalid: {other}"),
                };
                return ok(false, &msg);
            }
        }

        // NIP-40: reject expired events.
        let now = now_secs();
        if ev.is_expired(now) {
            return ok(false, "invalid: event expired");
        }

        // NIP-42 write restriction.
        if self.cfg.require_auth && authed_pubkey.is_none() {
            return ok(false, "auth-required: authenticate with AUTH first");
        }

        // NIP-09: kind 5 deletion requests. Only the author of a stored event may delete it,
        // so we look the target up first and compare pubkeys before calling into the store.
        if ev.kind == 5 {
            let mut deleted = 0usize;
            for target in ev.tag_values("e") {
                let Ok(bytes) = nostr_core::hex_array::<32>(target, "e") else {
                    continue;
                };
                let filter = Filter {
                    ids: Some(vec![bytes]),
                    ..Default::default()
                };
                let owned = match self.store.query(&[filter]) {
                    Ok(found) => found.iter().any(|stored| stored.pubkey == ev.pubkey),
                    Err(e) => {
                        warn!(error = %e, "delete lookup failed");
                        false
                    }
                };
                if !owned {
                    continue;
                }
                match self.store.delete(&bytes, &ev.pubkey) {
                    Ok(true) => deleted += 1,
                    Ok(false) => {}
                    Err(e) => warn!(error = %e, "delete failed"),
                }
            }
            debug!(conn_id, deleted, "processed deletion request");
        }

        match self.store.insert(&ev) {
            Ok(InsertOutcome::Stored) => {
                self.fanout(&ev);
                ok(true, "")
            }
            Ok(InsertOutcome::Duplicate) => ok(false, "duplicate: already have this event"),
            Ok(InsertOutcome::Rejected(r)) => ok(false, &format!("blocked: {r}")),
            Err(e) => ok(false, &format!("error: {e}")),
        }
    }

    /// Fan an accepted event out to live subscriptions, closing slow consumers.
    fn fanout(&self, ev: &Event) {
        let (sent, slow) = self.hub.fanout(ev);
        if sent > 0 {
            debug!(sent, "fanned out event");
        }
        for conn_id in slow {
            if let Some(h) = self.hub.handle(conn_id) {
                warn!(conn_id = h.conn_id, "closing slow consumer");
                h.request_close();
            }
        }
    }

    /// Handle a REQ frame, returning the frames to send in order.
    fn handle_req(&self, conn_id: u64, arr: &[Value]) -> Vec<Vec<u8>> {
        let Some(sub_id) = arr.get(1).and_then(|x| x.as_str()) else {
            return vec![
                RelayMessage::Notice("invalid: REQ missing subscription id".into()).to_json_bytes(),
            ];
        };
        let sub_id = sub_id.to_string();

        let mut filters = Vec::new();
        for f in arr.iter().skip(2) {
            match Filter::from_json_value(f) {
                Ok(f) => filters.push(f),
                Err(e) => {
                    return vec![RelayMessage::Closed {
                        sub_id,
                        message: format!("invalid: {e}"),
                    }
                    .to_json_bytes()]
                }
            }
        }
        if filters.is_empty() {
            return vec![RelayMessage::Closed {
                sub_id,
                message: "invalid: REQ requires at least one filter".into(),
            }
            .to_json_bytes()];
        }
        if filters.len() > self.cfg.max_filters {
            return vec![RelayMessage::Closed {
                sub_id,
                message: format!("invalid: too many filters (max {})", self.cfg.max_filters),
            }
            .to_json_bytes()];
        }

        clamp_filters(&mut filters, self.cfg.max_limit);

        let is_new = !self.hub.has_subscription(conn_id, &sub_id);
        if is_new && self.hub.subscription_count(conn_id) >= self.cfg.max_subs_per_conn {
            return vec![RelayMessage::Closed {
                sub_id,
                message: format!(
                    "invalid: too many subscriptions (max {})",
                    self.cfg.max_subs_per_conn
                ),
            }
            .to_json_bytes()];
        }

        let now = now_secs();
        let events = match self.store.query(&filters) {
            Ok(evs) => evs,
            Err(e) => {
                return vec![RelayMessage::Closed {
                    sub_id,
                    message: format!("error: {e}"),
                }
                .to_json_bytes()]
            }
        };

        let mut frames = Vec::with_capacity(events.len() + 1);
        let mut event_json = Vec::with_capacity(512);
        for ev in events {
            // NIP-40: never send expired events.
            if ev.is_expired(now) {
                continue;
            }
            event_json.clear();
            ev.write_json(&mut event_json);
            let mut frame = Vec::with_capacity(event_json.len() + sub_id.len() + 16);
            nostr_core::write_event_frame(&mut frame, &sub_id, &event_json);
            frames.push(frame);
        }
        frames.push(
            RelayMessage::Eose {
                sub_id: sub_id.clone(),
            }
            .to_json_bytes(),
        );

        if !self.hub.add_subscription(
            conn_id,
            Subscription { sub_id, filters },
            self.cfg.max_subs_per_conn,
        ) {
            warn!(conn_id, "subscription limit reached");
        }
        frames
    }

    /// Handle a COUNT frame (NIP-45).
    fn handle_count(&self, arr: &[Value]) -> Vec<Vec<u8>> {
        let Some(sub_id) = arr.get(1).and_then(|x| x.as_str()) else {
            return vec![
                RelayMessage::Notice("invalid: COUNT missing subscription id".into())
                    .to_json_bytes(),
            ];
        };
        let sub_id = sub_id.to_string();

        let mut filters = Vec::new();
        for f in arr.iter().skip(2) {
            match Filter::from_json_value(f) {
                Ok(f) => filters.push(f),
                Err(e) => {
                    return vec![RelayMessage::Closed {
                        sub_id,
                        message: format!("invalid: {e}"),
                    }
                    .to_json_bytes()]
                }
            }
        }
        if filters.is_empty() {
            return vec![RelayMessage::Closed {
                sub_id,
                message: "invalid: COUNT requires at least one filter".into(),
            }
            .to_json_bytes()];
        }
        if filters.len() > self.cfg.max_filters {
            return vec![RelayMessage::Closed {
                sub_id,
                message: format!("invalid: too many filters (max {})", self.cfg.max_filters),
            }
            .to_json_bytes()];
        }
        // NIP-45: COUNT reports the total number of matching events, so the filter `limit`
        // must not truncate the result set.
        for f in filters.iter_mut() {
            f.limit = None;
        }
        match self.store.count(&filters) {
            Ok(count) => vec![RelayMessage::Count { sub_id, count }.to_json_bytes()],
            Err(e) => vec![RelayMessage::Closed {
                sub_id,
                message: format!("error: {e}"),
            }
            .to_json_bytes()],
        }
    }

    /// Handle a NIP-42 AUTH frame.
    fn handle_auth(
        &self,
        challenge: &str,
        authed_pubkey: &mut Option<[u8; 32]>,
        payload: Option<&Value>,
    ) -> Vec<u8> {
        let Some(payload) = payload else {
            return RelayMessage::Notice("invalid: AUTH missing event".into()).to_json_bytes();
        };
        let ev = match Event::from_json_value(payload) {
            Ok(ev) => ev,
            Err(e) => return RelayMessage::Notice(format!("invalid: {e}")).to_json_bytes(),
        };
        let fail = |msg: &str| {
            RelayMessage::Ok {
                id: hex::encode(ev.id),
                ok: false,
                message: msg.to_string(),
            }
            .to_json_bytes()
        };
        if ev.kind != 22242 {
            return fail("invalid: AUTH event must be kind 22242");
        }
        let now = now_secs();
        if ev.created_at.abs_diff(now) > 600 {
            return fail("invalid: AUTH event timestamp out of range");
        }
        if ev.tag_value("challenge") != Some(challenge) {
            return fail("invalid: challenge mismatch");
        }
        if let Some(url) = &self.cfg.relay_url {
            if ev.tag_value("relay") != Some(url.as_str()) {
                return fail("invalid: relay tag mismatch");
            }
        }
        if let Err(e) = ev.verify() {
            return fail(&format!("invalid: {e}"));
        }
        *authed_pubkey = Some(ev.pubkey);
        RelayMessage::Ok {
            id: hex::encode(ev.id),
            ok: true,
            message: String::new(),
        }
        .to_json_bytes()
    }
}

/// Write a batch of frames, flushing once at the end. Returns `Some(reason)` on failure.
async fn send_batch<S>(
    sink: &mut futures_util::stream::SplitSink<tokio_tungstenite::WebSocketStream<S>, Message>,
    frames: Vec<Vec<u8>>,
) -> Option<String>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    let n = frames.len();
    for (i, frame) in frames.into_iter().enumerate() {
        // Every frame is produced by nostr_core's JSON writers, which only emit valid UTF-8.
        let text = match String::from_utf8(frame) {
            Ok(t) => t,
            Err(e) => return Some(format!("internal: non-utf8 frame: {e}")),
        };
        let msg = Message::Text(text);
        let res = if i + 1 == n {
            sink.send(msg).await
        } else {
            sink.feed(msg).await
        };
        if let Err(e) = res {
            return Some(format!("write failed: {e}"));
        }
    }
    None
}

/// Clamp per-filter limits to the relay maximum.
fn clamp_filters(filters: &mut [Filter], max_limit: usize) {
    for f in filters.iter_mut() {
        let requested = f.limit.unwrap_or(max_limit);
        f.limit = Some(requested.min(max_limit));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn clamps_limits() {
        let mut f = vec![Filter {
            limit: Some(10_000),
            ..Default::default()
        }];
        clamp_filters(&mut f, 500);
        assert_eq!(f[0].limit, Some(500));
        let mut f = vec![Filter::default()];
        clamp_filters(&mut f, 500);
        assert_eq!(f[0].limit, Some(500));
    }
}
