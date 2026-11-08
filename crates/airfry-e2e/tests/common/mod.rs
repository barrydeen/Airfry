//! Shared helpers for the black-box relay tests: spawning the real `airfry-relay` binary,
//! driving it over WebSockets, and talking plain HTTP to it.
//!
//! This module is included by several integration test binaries, so it is deliberately
//! self-contained and tolerant of unused helpers in any single binary.

#![allow(dead_code)]

use airfry_core::{Event, Keys};
use futures_util::{SinkExt, StreamExt};
use serde_json::Value;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

pub type Ws = WebSocketStream<MaybeTlsStream<TcpStream>>;

/// A relay process under test. Killed and its data directory removed on drop.
pub struct RelayProc {
    pub child: Child,
    pub port: u16,
    pub dir: PathBuf,
    /// When true, the data directory is preserved on drop (restart-durability tests).
    pub keep_dir: bool,
}

impl Drop for RelayProc {
    fn drop(&mut self) {
        // If the test already reaped the child this is a no-op.
        let _ = self.child.kill();
        let _ = self.child.wait();
        if !self.keep_dir {
            let _ = std::fs::remove_dir_all(&self.dir);
        }
    }
}

impl RelayProc {
    pub fn pid(&self) -> u32 {
        self.child.id()
    }

    /// Send SIGTERM and wait for the process to exit. Returns the exit status if it exited
    /// within `timeout`.
    pub fn terminate(&mut self, timeout: Duration) -> Option<std::process::ExitStatus> {
        unsafe {
            libc::kill(self.child.id() as libc::pid_t, libc::SIGTERM);
        }
        let deadline = Instant::now() + timeout;
        loop {
            match self.child.try_wait() {
                Ok(Some(status)) => return Some(status),
                Ok(None) => {}
                Err(_) => return None,
            }
            if Instant::now() >= deadline {
                return None;
            }
            std::thread::sleep(Duration::from_millis(25));
        }
    }
}

pub fn free_port() -> u16 {
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let p = l.local_addr().unwrap().port();
    drop(l);
    p
}

fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../..")
}

/// Locate the relay binary, building it if the test runner has not done so yet.
///
/// `cargo test -p airfry-e2e` does not build other packages' binaries, so we fall back to
/// invoking cargo once. This keeps the suite runnable with a single command.
pub fn relay_binary() -> PathBuf {
    let root = workspace_root();
    for profile in ["debug", "release"] {
        let p = root.join("target").join(profile).join("airfry-relay");
        if p.exists() {
            return p;
        }
    }
    let cargo = std::env::var("CARGO").unwrap_or_else(|_| "cargo".to_string());
    let status = Command::new(cargo)
        .arg("build")
        .arg("-p")
        .arg("airfry-relay")
        .current_dir(&root)
        .status()
        .expect("cargo build -p airfry-relay runs");
    assert!(status.success(), "cargo build -p airfry-relay failed");
    let p = root.join("target").join("debug").join("airfry-relay");
    assert!(
        p.exists(),
        "airfry-relay binary missing after build: {}",
        p.display()
    );
    p
}

/// Start a relay with the given extra CLI arguments on a fresh data directory.
///
/// `free_port()` can race with a sibling test that grabs the same ephemeral port between
/// the probe bind and the relay's own bind. When that happens the relay exits immediately
/// without ever listening, so we retry on a fresh port a few times before giving up.
pub fn start_relay_with(extra: &[&str]) -> RelayProc {
    let mut last_err = String::new();
    for _ in 0..16 {
        let port = free_port();
        let mut dir = std::env::temp_dir();
        dir.push(format!(
            "airfry-relay-e2e-{}-{}-{}",
            std::process::id(),
            port,
            Instant::now().elapsed().as_nanos()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let mut cmd = Command::new(relay_binary());
        cmd.arg("--bind")
            .arg(format!("127.0.0.1:{port}"))
            .arg("--data-dir")
            .arg(&dir)
            .env("RUST_LOG", "warn")
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        for a in extra {
            cmd.arg(a);
        }
        let child = cmd.spawn().expect("relay spawns");
        let mut proc = RelayProc {
            child,
            port,
            dir,
            keep_dir: false,
        };
        if wait_listening(&mut proc, Duration::from_secs(30)) {
            return proc;
        }
        // Port was stolen or the relay died on startup: reap and retry on a new port.
        last_err = format!("relay failed to listen on port {port}");
        let _ = proc.child.kill();
        let _ = proc.child.wait();
        let _ = std::fs::remove_dir_all(&proc.dir);
    }
    panic!("relay did not start listening after retries: {last_err}");
}

/// Start a relay with default settings.
pub fn start_relay() -> RelayProc {
    start_relay_with(&[])
}

/// Start a relay on an existing data directory (restart durability tests).
pub fn start_relay_on(dir: &Path) -> RelayProc {
    let mut last_err = String::new();
    for _ in 0..16 {
        let port = free_port();
        let child = Command::new(relay_binary())
            .arg("--bind")
            .arg(format!("127.0.0.1:{port}"))
            .arg("--data-dir")
            .arg(dir)
            .env("RUST_LOG", "warn")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("relay respawns");
        let mut proc = RelayProc {
            child,
            port,
            dir: dir.to_path_buf(),
            keep_dir: false,
        };
        if wait_listening(&mut proc, Duration::from_secs(30)) {
            return proc;
        }
        last_err = format!("relay failed to listen on port {port}");
        let _ = proc.child.kill();
        let _ = proc.child.wait();
    }
    panic!("relay did not start listening after retries: {last_err}");
}

/// Wait until the relay is actually serving WebSocket upgrades. A bare TCP connect is not
/// enough: the listener can be bound before the accept loop is ready, in which case the first
/// upgrade attempt is reset. We probe with a real HTTP Upgrade handshake and only report ready
/// once it succeeds. Returns `false` if the child exited or the timeout elapsed first.
fn wait_listening(proc: &mut RelayProc, timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        // If the child already exited (e.g. bind failed), stop waiting immediately.
        if let Ok(Some(_)) = proc.child.try_wait() {
            return false;
        }
        if ws_probe(proc.port) {
            return true;
        }
        std::thread::sleep(Duration::from_millis(25));
    }
    false
}

/// Perform a minimal WebSocket upgrade handshake against `port`. Returns true only if the
/// server answers with `101 Switching Protocols`. The socket is closed immediately after.
fn ws_probe(port: u16) -> bool {
    let mut s = match std::net::TcpStream::connect(("127.0.0.1", port)) {
        Ok(s) => s,
        Err(_) => return false,
    };
    let _ = s.set_read_timeout(Some(Duration::from_secs(2)));
    let _ = s.set_write_timeout(Some(Duration::from_secs(2)));
    let req = format!(
        "GET / HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\r\n"
    );
    if s.write_all(req.as_bytes()).is_err() {
        return false;
    }
    let mut buf = [0u8; 64];
    match s.read(&mut buf) {
        Ok(n) if n > 0 => {
            let head = String::from_utf8_lossy(&buf[..n]);
            head.starts_with("HTTP/1.1 101") || head.contains(" 101 ")
        }
        _ => false,
    }
}

pub async fn connect(port: u16) -> Ws {
    let url = format!("ws://127.0.0.1:{port}/");
    // A freshly started relay can reset the very first upgrade attempt while its accept loop
    // warms up; retry a few times with a short backoff before failing the test.
    let mut last = None;
    for _ in 0..20 {
        match tokio_tungstenite::connect_async(&url).await {
            Ok((ws, _)) => return ws,
            Err(e) => {
                last = Some(e);
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        }
    }
    panic!(
        "websocket connects: {:?}",
        last.expect("at least one attempt")
    );
}

pub async fn send(ws: &mut Ws, s: impl Into<String>) {
    ws.send(Message::Text(s.into())).await.expect("send frame");
}

/// Read frames until one satisfies `pred`, or time out. Panics on timeout.
pub async fn read_until<F>(ws: &mut Ws, mut pred: F, timeout: Duration) -> Value
where
    F: FnMut(&Value) -> bool,
{
    let deadline = Instant::now() + timeout;
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            panic!("timed out waiting for expected frame");
        }
        let msg = tokio::time::timeout(remaining, ws.next())
            .await
            .expect("frame within timeout")
            .expect("stream open")
            .expect("frame ok");
        match msg {
            Message::Text(t) => {
                let v: Value = serde_json::from_str(&t).expect("relay frame is JSON");
                if pred(&v) {
                    return v;
                }
            }
            Message::Ping(_) | Message::Pong(_) => continue,
            Message::Close(_) => panic!("relay closed connection unexpectedly"),
            _ => continue,
        }
    }
}

/// Read frames until one satisfies `pred`, returning `None` on timeout instead of panicking.
pub async fn try_read_until<F>(ws: &mut Ws, mut pred: F, timeout: Duration) -> Option<Value>
where
    F: FnMut(&Value) -> bool,
{
    let deadline = Instant::now() + timeout;
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return None;
        }
        let timed = tokio::time::timeout(remaining, ws.next()).await.ok()?;
        let msg = timed?.ok()?;
        match msg {
            Message::Text(t) => {
                let v: Value = serde_json::from_str(&t).ok()?;
                if pred(&v) {
                    return Some(v);
                }
            }
            Message::Ping(_) | Message::Pong(_) => continue,
            Message::Close(_) => return None,
            _ => continue,
        }
    }
}

/// Collect every frame received within `window`, ignoring ping/pong.
pub async fn drain(ws: &mut Ws, window: Duration) -> Vec<Value> {
    let mut out = Vec::new();
    let deadline = Instant::now() + window;
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return out;
        }
        match tokio::time::timeout(remaining, ws.next()).await {
            Ok(Some(Ok(Message::Text(t)))) => {
                if let Ok(v) = serde_json::from_str::<Value>(&t) {
                    out.push(v);
                }
            }
            Ok(Some(Ok(Message::Ping(_) | Message::Pong(_)))) => continue,
            Ok(Some(Ok(Message::Close(_)))) | Ok(None) | Err(_) => return out,
            Ok(Some(Ok(_))) => continue,
            Ok(Some(Err(_))) => return out,
        }
    }
}

/// A parsed plain-HTTP response.
pub struct HttpResponse {
    pub status_line: String,
    pub headers: Vec<(String, String)>,
    pub body: String,
}

impl HttpResponse {
    pub fn status(&self) -> u16 {
        self.status_line
            .split_whitespace()
            .nth(1)
            .and_then(|s| s.parse().ok())
            .unwrap_or(0)
    }

    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str())
    }

    pub fn json(&self) -> Value {
        let start = self
            .body
            .find('{')
            .unwrap_or_else(|| panic!("no JSON body: {}", self.body));
        serde_json::from_str(&self.body[start..])
            .unwrap_or_else(|e| panic!("bad JSON body ({e}): {}", self.body))
    }
}

/// Perform a plain HTTP/1.1 GET against the relay's HTTP port.
pub fn http_get(port: u16, path: &str, accept: Option<&str>) -> HttpResponse {
    let mut s = std::net::TcpStream::connect(("127.0.0.1", port)).expect("http connect");
    s.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
    let mut req = format!("GET {path} HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\nConnection: close\r\n");
    if let Some(a) = accept {
        req.push_str(&format!("Accept: {a}\r\n"));
    }
    req.push_str("\r\n");
    s.write_all(req.as_bytes()).unwrap();
    let mut buf = Vec::new();
    s.read_to_end(&mut buf).unwrap();
    let raw = String::from_utf8_lossy(&buf).to_string();
    let (head, body) = match raw.find("\r\n\r\n") {
        Some(i) => (&raw[..i], raw[i + 4..].to_string()),
        None => (raw.as_str(), String::new()),
    };
    let mut lines = head.split("\r\n");
    let status_line = lines.next().unwrap_or("").to_string();
    let headers = lines
        .filter_map(|l| l.split_once(':'))
        .map(|(k, v)| (k.trim().to_string(), v.trim().to_string()))
        .collect();
    HttpResponse {
        status_line,
        headers,
        body,
    }
}

/// Publish an event and return the relay's OK frame.
pub async fn publish(ws: &mut Ws, ev: &Event) -> Value {
    send(ws, format!("[\"EVENT\",{}]", ev.to_json())).await;
    read_until(
        ws,
        |v| v[0] == "OK" && v[1] == hex::encode(ev.id),
        Duration::from_secs(10),
    )
    .await
}

/// Publish an event and assert it was accepted.
pub async fn publish_ok(ws: &mut Ws, ev: &Event) {
    let ok = publish(ws, ev).await;
    assert_eq!(ok[2], true, "event should be accepted: {ok}");
}

/// Run a REQ and collect the events returned before EOSE.
///
/// A connection may legitimately have several live subscriptions at once, so live fanout
/// frames addressed to *other* subscription ids can be interleaved with the stored-event
/// replay for the subscription we just opened. Those frames are skipped here (they are
/// asserted on by the tests that own those subscriptions); only frames for `sub_id` are
/// collected. `CLOSED`/`NOTICE` for `sub_id` are treated as failures.
pub async fn req_collect(ws: &mut Ws, sub_id: &str, filter: Value) -> Vec<Event> {
    send(ws, format!("[\"REQ\",\"{sub_id}\",{filter}]")).await;
    let mut out = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        assert!(
            !remaining.is_zero(),
            "timed out waiting for EOSE on {sub_id}"
        );
        let msg = tokio::time::timeout(remaining, ws.next())
            .await
            .expect("frame within timeout")
            .expect("stream open")
            .expect("frame ok");
        match msg {
            Message::Text(t) => {
                let v: Value = serde_json::from_str(&t).expect("relay frame is JSON");
                let frame_sub = v.get(1).and_then(|x| x.as_str()).unwrap_or("");
                match v[0].as_str() {
                    Some("EVENT") => {
                        if frame_sub != sub_id {
                            // live fanout for another open subscription on this connection
                            continue;
                        }
                        out.push(Event::from_json_value(&v[2]).expect("event parses"));
                    }
                    Some("EOSE") => {
                        if frame_sub != sub_id {
                            continue;
                        }
                        return out;
                    }
                    Some("CLOSED") => {
                        if frame_sub != sub_id {
                            continue;
                        }
                        panic!("subscription closed: {v}");
                    }
                    Some("NOTICE") => panic!("notice during REQ: {v}"),
                    _ => continue,
                }
            }
            Message::Ping(_) | Message::Pong(_) => continue,
            Message::Close(_) => panic!("relay closed connection during REQ"),
            _ => continue,
        }
    }
}

/// A deterministic publisher key for a test.
pub fn key(label: &str) -> Keys {
    Keys::from_label(label)
}
