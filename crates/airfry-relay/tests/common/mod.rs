//! Shared helpers for the relay integration tests: boot a relay in-process on an ephemeral
//! port, connect WebSocket clients, and do raw HTTP requests.

#![allow(dead_code)]

use airfry_relay::config::Config;
use airfry_relay::server::Relay;
use futures_util::{SinkExt, StreamExt};
use serde_json::Value;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::task::JoinHandle;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::WebSocketStream;

/// A relay running in-process. Aborts the server task on drop.
pub struct TestRelay {
    pub relay: Arc<Relay>,
    pub addr: SocketAddr,
    handle: JoinHandle<()>,
    data_dir: std::path::PathBuf,
}

impl TestRelay {
    /// Access the underlying store (for assertions on persisted state).
    pub fn store(&self) -> &airfry_store::Store {
        &self.relay.store
    }
}

impl Drop for TestRelay {
    fn drop(&mut self) {
        self.handle.abort();
        let _ = std::fs::remove_dir_all(&self.data_dir);
    }
}

/// Boot a relay with the given config on an ephemeral port.
pub async fn spawn_relay(mut cfg: Config) -> TestRelay {
    let data_dir = std::env::temp_dir().join(format!(
        "airfry-relay-test-{}-{}",
        std::process::id(),
        airfry_relay::util::random_hex(8)
    ));
    std::fs::create_dir_all(&data_dir).expect("create temp data dir");
    cfg.data_dir = data_dir.clone();
    cfg.bind = "127.0.0.1:0".parse().unwrap();

    let relay = Relay::new(cfg).expect("relay boots");
    let listener = tokio::net::TcpListener::bind(relay.cfg.bind)
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("local addr");
    let relay_clone = relay.clone();
    let handle = tokio::spawn(async move {
        let _ = relay_clone.serve(listener).await;
    });
    TestRelay {
        relay,
        addr,
        handle,
        data_dir,
    }
}

/// A connected WebSocket client. The NIP-42 AUTH challenge sent on connect is captured.
pub struct Client {
    pub ws: WebSocketStream<tokio_tungstenite::MaybeTlsStream<TcpStream>>,
    pub challenge: String,
}

impl Client {
    /// Send a raw text frame.
    pub async fn send_raw(&mut self, frame: &str) {
        self.ws
            .send(Message::Text(frame.to_string()))
            .await
            .expect("send frame");
    }

    /// Send a JSON value as a text frame.
    pub async fn send_json(&mut self, v: Value) {
        self.send_raw(&v.to_string()).await;
    }

    /// Receive the next JSON frame, with a timeout.
    pub async fn next_json(&mut self, timeout: Duration) -> Option<Value> {
        let fut = async {
            loop {
                match self.ws.next().await {
                    Some(Ok(Message::Text(t))) => {
                        return Some(serde_json::from_str(&t).expect("relay frame is valid json"));
                    }
                    Some(Ok(Message::Binary(b))) => {
                        return Some(
                            serde_json::from_slice(&b).expect("relay frame is valid json"),
                        );
                    }
                    Some(Ok(Message::Ping(_))) | Some(Ok(Message::Pong(_))) => continue,
                    Some(Ok(Message::Close(_))) | None => return None,
                    Some(Ok(Message::Frame(_))) => continue,
                    Some(Err(_)) => return None,
                }
            }
        };
        tokio::time::timeout(timeout, fut).await.ok().flatten()
    }

    /// Receive frames until one with the given type arrives, returning it.
    pub async fn wait_for(&mut self, msg_type: &str, timeout: Duration) -> Option<Value> {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                return None;
            }
            let v = self.next_json(remaining).await?;
            if v.get(0).and_then(|x| x.as_str()) == Some(msg_type) {
                return Some(v);
            }
        }
    }

    /// Collect frames until EOSE for `sub_id`, returning the EVENT payloads seen.
    pub async fn collect_until_eose(&mut self, sub_id: &str, timeout: Duration) -> Vec<Value> {
        let deadline = tokio::time::Instant::now() + timeout;
        let mut events = Vec::new();
        loop {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                panic!("timed out waiting for EOSE on sub {sub_id}");
            }
            let Some(v) = self.next_json(remaining).await else {
                panic!("connection closed while waiting for EOSE on sub {sub_id}");
            };
            match v.get(0).and_then(|x| x.as_str()) {
                Some("EVENT") if v.get(1).and_then(|x| x.as_str()) == Some(sub_id) => {
                    events.push(v.get(2).cloned().unwrap_or(Value::Null));
                }
                Some("EOSE") if v.get(1).and_then(|x| x.as_str()) == Some(sub_id) => return events,
                Some("CLOSED") if v.get(1).and_then(|x| x.as_str()) == Some(sub_id) => {
                    panic!("subscription closed: {v}");
                }
                _ => {}
            }
        }
    }
}

/// Connect a WebSocket client to the relay and consume the initial AUTH challenge.
pub async fn connect(addr: SocketAddr) -> Client {
    let url = format!("ws://{addr}/");
    let (mut ws, _resp) = tokio_tungstenite::connect_async(url)
        .await
        .expect("websocket connect");
    let challenge = loop {
        match ws.next().await {
            Some(Ok(Message::Text(t))) => {
                let v: Value = serde_json::from_str(&t).expect("valid json");
                if v.get(0).and_then(|x| x.as_str()) == Some("AUTH") {
                    break v
                        .get(1)
                        .and_then(|x| x.as_str())
                        .expect("challenge string")
                        .to_string();
                }
            }
            Some(Ok(Message::Ping(_))) | Some(Ok(Message::Pong(_))) => continue,
            other => panic!("expected AUTH challenge on connect, got {other:?}"),
        }
    };
    Client { ws, challenge }
}

/// Perform a raw HTTP/1.1 GET and return (status line, headers, body).
pub async fn http_get(
    addr: SocketAddr,
    path: &str,
    accept: Option<&str>,
) -> (String, String, String) {
    let mut stream = TcpStream::connect(addr).await.expect("tcp connect");
    let mut req = format!("GET {path} HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\n");
    if let Some(a) = accept {
        req.push_str(&format!("Accept: {a}\r\n"));
    }
    req.push_str("\r\n");
    stream
        .write_all(req.as_bytes())
        .await
        .expect("write request");
    let mut buf = Vec::new();
    stream.read_to_end(&mut buf).await.expect("read response");
    let text = String::from_utf8_lossy(&buf).to_string();
    let (head, body) = match text.split_once("\r\n\r\n") {
        Some((h, b)) => (h.to_string(), b.to_string()),
        None => (text.clone(), String::new()),
    };
    let status = head.lines().next().unwrap_or("").to_string();
    (status, head, body)
}

/// Build a signed event with the given kind/tags/content.
pub fn signed_event(
    keys: &airfry_core::Keys,
    kind: u32,
    tags: Vec<Vec<String>>,
    content: &str,
) -> airfry_core::Event {
    keys.sign_event(airfry_core::now_secs(), kind, tags, content)
}

/// Wrap an event in an EVENT frame.
pub fn event_frame(ev: &airfry_core::Event) -> String {
    format!("[\"EVENT\",{}]", ev.to_json())
}
