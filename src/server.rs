//! OCShell relay server.
//!
//! One agent (the in-game computer) and one client (the CLI) are bridged
//! together. The server is deliberately dumb: it forwards WebSocket messages
//! between the two roles. All shell semantics live in the agent and client.
//!
//! WARNING: no authentication. See DISCLAIMER.md.

use std::collections::VecDeque;
use std::io;
use std::net::{IpAddr, SocketAddr};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{ConnectInfo, State};
use axum::http::{header, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::Router;
use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpListener;
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::{mpsc, oneshot, Mutex};

use crate::ws::MAX_PAYLOAD;

pub const BACKLOG_LIMIT: usize = 1024 * 1024;

/// Outbound messages queued for one attached peer. Agent output arrives in
/// 4 KiB chunks, so this is roughly 4 MiB before a stalled peer is dropped.
const PEER_QUEUE: usize = 1024;

/// The in-game agent, embedded so the binary is self-contained.
pub const AGENT_SOURCE: &str = include_str!("../agent/ocshell.lua");

static NEXT_PEER_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    Agent,
    Client,
}

impl Role {
    fn as_str(self) -> &'static str {
        match self {
            Role::Agent => "agent",
            Role::Client => "client",
        }
    }
}

/// Handle given to one connection task. Dropping the handle (because the peer
/// was replaced or detached) makes the task stop through the `cancel` channel.
struct PeerHandle {
    id: u64,
    role: Role,
    tx: mpsc::Sender<Message>,
    _cancel: oneshot::Sender<()>,
}

struct Delivery {
    role: Role,
    id: u64,
    tx: mpsc::Sender<Message>,
}

#[derive(Debug, Clone, Copy)]
pub struct Status {
    pub agent: bool,
    pub client: bool,
    pub backlog_bytes: usize,
    pub uptime_secs: f64,
}

pub struct RelayState {
    agent: Option<PeerHandle>,
    client: Option<PeerHandle>,
    backlog: VecDeque<Message>,
    backlog_bytes: usize,
    started: Instant,
}

impl Default for RelayState {
    fn default() -> Self {
        Self::new()
    }
}

impl RelayState {
    pub fn new() -> Self {
        Self {
            agent: None,
            client: None,
            backlog: VecDeque::new(),
            backlog_bytes: 0,
            started: Instant::now(),
        }
    }

    fn attach(&mut self, peer: PeerHandle, ip: IpAddr) -> Vec<Message> {
        let role = peer.role;
        let old = match role {
            Role::Agent => self.agent.replace(peer),
            Role::Client => self.client.replace(peer),
        };
        // Dropping the old handle closes its cancel channel; its task exits.
        drop(old);

        println!("[ocs] {} connected ({ip})", role.as_str());

        if role == Role::Client {
            self.backlog_bytes = 0;
            self.backlog.drain(..).collect()
        } else {
            Vec::new()
        }
    }

    fn detach(&mut self, role: Role, id: u64) -> bool {
        let removed = match role {
            Role::Agent => {
                if self.agent.as_ref().is_some_and(|peer| peer.id == id) {
                    self.agent = None;
                    true
                } else {
                    false
                }
            }
            Role::Client => {
                if self.client.as_ref().is_some_and(|peer| peer.id == id) {
                    self.client = None;
                    true
                } else {
                    false
                }
            }
        };
        if removed {
            println!("[ocs] {} disconnected", role.as_str());
        }
        removed
    }

    /// Forward one data message. Never panics and never waits: if the target's
    /// task is gone, the frame is dropped and the dead peer is removed. This is
    /// important when a client vanishes while the agent is still sending.
    ///
    /// A peer that stops draining its outbound queue is treated the same way:
    /// it is dropped rather than growing the server's memory without bound.
    fn relay(&mut self, from: Role, message: Message) {
        match from {
            Role::Agent => {
                let target = self.client.as_ref().map(|peer| Delivery {
                    role: Role::Client,
                    id: peer.id,
                    tx: peer.tx.clone(),
                });
                if let Some(target) = target {
                    match target.tx.try_send(message) {
                        Ok(()) => {}
                        Err(TrySendError::Full(_)) | Err(TrySendError::Closed(_)) => {
                            self.detach(target.role, target.id);
                        }
                    }
                    return;
                }
                self.push_backlog(message);
            }
            Role::Client => {
                let target = self.agent.as_ref().map(|peer| Delivery {
                    role: Role::Agent,
                    id: peer.id,
                    tx: peer.tx.clone(),
                });
                if let Some(target) = target {
                    match target.tx.try_send(message) {
                        Ok(()) => {}
                        Err(TrySendError::Full(_)) | Err(TrySendError::Closed(_)) => {
                            self.detach(target.role, target.id);
                        }
                    }
                }
            }
        }
    }

    fn push_backlog(&mut self, message: Message) {
        let length = payload_len(&message);
        self.backlog.push_back(message);
        self.backlog_bytes += length;
        while self.backlog_bytes > BACKLOG_LIMIT {
            let Some(dropped) = self.backlog.pop_front() else {
                break;
            };
            self.backlog_bytes = self.backlog_bytes.saturating_sub(payload_len(&dropped));
        }
    }

    pub fn status(&self) -> Status {
        Status {
            agent: self.agent.is_some(),
            client: self.client.is_some(),
            backlog_bytes: self.backlog_bytes,
            uptime_secs: self.started.elapsed().as_secs_f64(),
        }
    }
}

fn payload_len(message: &Message) -> usize {
    match message {
        Message::Text(text) => text.as_str().len(),
        Message::Binary(data) => data.len(),
        _ => 0,
    }
}

#[derive(Clone)]
pub struct AppState {
    relay: Arc<Mutex<RelayState>>,
    public_host: String,
    port: u16,
}

fn router(state: AppState) -> Router {
    Router::new()
        .route("/", get(status_page))
        .route("/index.html", get(status_page))
        .route("/healthz", get(health))
        .route("/health", get(health))
        .route("/agent.lua", get(agent_lua))
        .route("/agent", get(agent_socket))
        .route("/agent/", get(agent_socket))
        .route("/client", get(client_socket))
        .route("/client/", get(client_socket))
        .fallback(not_found)
        .with_state(state)
}

async fn status_page(State(state): State<AppState>) -> Response {
    let status = { state.relay.lock().await.status() };
    let agent = if status.agent { "connected" } else { "waiting" };
    let client = if status.client {
        "connected"
    } else {
        "waiting"
    };
    let host = escape_html(&state.public_host);
    let port = state.port;

    let body = format!(
        "<!doctype html>\n\
         <html lang=\"en\"><head><meta charset=\"utf-8\">\n\
         <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">\n\
         <title>OCShell</title>\n\
         <style>\n\
         body {{ background:#0d1117; color:#c9d1d9; font:14px/1.5 ui-monospace, Menlo, monospace;\n\
                margin:2rem; }}\n\
         h1 {{ font-family:ui-sans-serif, system-ui, sans-serif; margin:0 0 .25rem; }}\n\
         .warn {{ border:1px solid #d29922; background:#2b2111; color:#e3b341;\n\
                  padding:.75rem 1rem; margin:1.5rem 0; }}\n\
         code {{ color:#79c0ff; }}\n\
         table {{ border-collapse:collapse; }}\n\
         td {{ padding:.25rem .75rem .25rem 0; }}\n\
         </style></head><body>\n\
         <h1>OCShell</h1>\n\
         <p>single-session OpenComputers shell relay</p>\n\
         <div class=\"warn\"><strong>LLM-generated and unauthenticated.</strong>\n\
         Anyone who can reach this port gets a shell. See DISCLAIMER.md.</div>\n\
         <table>\n\
         <tr><td>agent</td><td><code>{agent}</code></td></tr>\n\
         <tr><td>client</td><td><code>{client}</code></td></tr>\n\
         <tr><td>backlog</td><td><code>{}</code> bytes</td></tr>\n\
         </table>\n\
         <p>Install in-game:<br>\n\
         <code>wget -f http://{host}:{port}/agent.lua /bin/ocshell.lua</code><br>\n\
         <code>ocshell ws://{host}:{port}/agent</code></p>\n\
         <p>Client: <code>ocs attach ws://{host}:{port}/client</code></p>\n\
         </body></html>\n",
        status.backlog_bytes
    );

    let mut response = body.into_response();
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("text/html; charset=utf-8"),
    );
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response
        .headers_mut()
        .insert(header::CONNECTION, HeaderValue::from_static("close"));
    response
}

async fn health() -> Response {
    text_response(StatusCode::OK, "ok\n".to_string())
}

async fn agent_lua() -> Response {
    text_response(StatusCode::OK, AGENT_SOURCE.to_string())
}

async fn not_found() -> Response {
    text_response(StatusCode::NOT_FOUND, "not found\n".to_string())
}

fn text_response(status: StatusCode, body: String) -> Response {
    let mut response = (status, body).into_response();
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("text/plain; charset=utf-8"),
    );
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response
        .headers_mut()
        .insert(header::CONNECTION, HeaderValue::from_static("close"));
    response
}

fn escape_html(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len());
    for character in value.chars() {
        match character {
            '&' => escaped.push_str("&amp;"),
            '<' => escaped.push_str("&lt;"),
            '>' => escaped.push_str("&gt;"),
            '"' => escaped.push_str("&quot;"),
            '\'' => escaped.push_str("&#x27;"),
            other => escaped.push(other),
        }
    }
    escaped
}

async fn agent_socket(
    ws: WebSocketUpgrade,
    State(state): State<AppState>,
    ConnectInfo(address): ConnectInfo<SocketAddr>,
) -> Response {
    upgrade(ws, Role::Agent, state, address)
}

async fn client_socket(
    ws: WebSocketUpgrade,
    State(state): State<AppState>,
    ConnectInfo(address): ConnectInfo<SocketAddr>,
) -> Response {
    upgrade(ws, Role::Client, state, address)
}

fn upgrade(ws: WebSocketUpgrade, role: Role, state: AppState, address: SocketAddr) -> Response {
    ws.max_message_size(MAX_PAYLOAD)
        .max_frame_size(MAX_PAYLOAD)
        .on_upgrade(move |socket| handle_socket(socket, role, state, address))
}

async fn handle_socket(socket: WebSocket, role: Role, state: AppState, address: SocketAddr) {
    let id = NEXT_PEER_ID.fetch_add(1, Ordering::Relaxed);
    let (tx, mut rx) = mpsc::channel::<Message>(PEER_QUEUE);
    let (cancel, mut cancelled) = oneshot::channel::<()>();

    let replay = {
        let mut relay = state.relay.lock().await;
        relay.attach(
            PeerHandle {
                id,
                role,
                tx,
                _cancel: cancel,
            },
            address.ip(),
        )
    };

    let (mut sender, mut receiver) = socket.split();

    for message in replay {
        if sender.send(message).await.is_err() {
            state.relay.lock().await.detach(role, id);
            return;
        }
    }

    loop {
        tokio::select! {
            biased;
            _ = &mut cancelled => break,
            outgoing = rx.recv() => {
                match outgoing {
                    Some(message) => {
                        if sender.send(message).await.is_err() {
                            break;
                        }
                    }
                    None => break,
                }
            }
            incoming = receiver.next() => {
                match incoming {
                    Some(Ok(message)) => match message {
                        forwarded @ (Message::Text(_) | Message::Binary(_)) => {
                            state.relay.lock().await.relay(role, forwarded);
                        }
                        Message::Close(_) => {
                            // Tungstenite queues the close reply while reading;
                            // flush it before dropping the socket.
                            let _ = sender.flush().await;
                            break;
                        }
                        Message::Ping(_) => {
                            // Tungstenite queues the pong while reading.
                            if sender.flush().await.is_err() {
                                break;
                            }
                        }
                        Message::Pong(_) => {}
                    },
                    Some(Err(_)) | None => break,
                }
            }
        }
    }

    state.relay.lock().await.detach(role, id);
}

/// A bound relay server.
pub struct Server {
    listener: TcpListener,
    app: Router,
    address: SocketAddr,
    public_host: String,
}

impl Server {
    /// Bind the listener. `public_host` defaults to the bind host, with
    /// wildcard binds advertised as 127.0.0.1.
    pub async fn bind(host: &str, port: u16, public_host: Option<String>) -> io::Result<Self> {
        let listener = TcpListener::bind((host, port)).await?;
        let address = listener.local_addr()?;
        let public_host = public_host.unwrap_or_else(|| {
            if host == "0.0.0.0" || host == "::" || host == "[::]" {
                "127.0.0.1".to_string()
            } else {
                host.to_string()
            }
        });
        // Bracket bare IPv6 literals so the advertised URLs parse.
        let public_host = if public_host.contains(':') && !public_host.starts_with('[') {
            format!("[{public_host}]")
        } else {
            public_host
        };
        let state = AppState {
            relay: Arc::new(Mutex::new(RelayState::new())),
            public_host: public_host.clone(),
            port: address.port(),
        };
        Ok(Self {
            listener,
            app: router(state),
            address,
            public_host,
        })
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.address
    }

    pub fn public_host(&self) -> &str {
        &self.public_host
    }

    pub fn port(&self) -> u16 {
        self.address.port()
    }

    /// Serve until Ctrl+C. Connection tasks keep running until it fires.
    pub async fn run(self) -> io::Result<()> {
        axum::serve(
            self.listener,
            self.app.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
            eprintln!("\nbye");
        })
        .await
        .map_err(io::Error::other)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    use axum::body::Bytes;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;
    use tokio::task::JoinHandle;

    use crate::ws::{WsStream, OP_BINARY, OP_CLOSE, OP_PING, OP_PONG, OP_TEXT};

    struct TestServer {
        port: u16,
        task: JoinHandle<()>,
    }

    impl TestServer {
        async fn start() -> Self {
            let server = Server::bind("127.0.0.1", 0, Some("127.0.0.1".to_string()))
                .await
                .unwrap();
            let port = server.port();
            let task = tokio::spawn(async move {
                let _ = server.run().await;
            });
            Self { port, task }
        }

        fn url(&self, path: &str) -> String {
            format!("ws://127.0.0.1:{}{}", self.port, path)
        }

        async fn connect(&self, path: &str) -> WsStream<TcpStream> {
            let (host, port, path) = crate::ws::parse_ws_url(&self.url(path)).unwrap();
            WsStream::connect(&host, port, &path, Duration::from_secs(5))
                .await
                .unwrap()
        }
    }

    impl Drop for TestServer {
        fn drop(&mut self) {
            self.task.abort();
        }
    }

    async fn http_get(port: u16, path: &str) -> (u16, String, String) {
        let mut stream = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
        let request =
            format!("GET {path} HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n");
        stream.write_all(request.as_bytes()).await.unwrap();
        let mut data = Vec::new();
        stream.read_to_end(&mut data).await.unwrap();
        let text = String::from_utf8_lossy(&data).to_string();
        let (head, body) = text.split_once("\r\n\r\n").unwrap_or((&text, ""));
        let status = head
            .lines()
            .next()
            .and_then(|line| line.split_whitespace().nth(1))
            .and_then(|code| code.parse().ok())
            .unwrap_or(0);
        (status, head.to_string(), body.to_string())
    }

    async fn recv_text(stream: &mut WsStream<TcpStream>) -> Vec<u8> {
        tokio::time::timeout(Duration::from_secs(2), stream.recv_frame())
            .await
            .expect("timed out waiting for a frame")
            .unwrap()
            .payload
    }

    #[tokio::test]
    async fn http_status_page_and_health() {
        let server = TestServer::start().await;

        let (status, headers, body) = http_get(server.port, "/healthz").await;
        assert_eq!(status, 200);
        assert!(headers.to_lowercase().contains("text/plain"), "{headers}");
        assert_eq!(body, "ok\n");

        let (status, _, body) = http_get(server.port, "/").await;
        assert_eq!(status, 200);
        assert!(body.contains("OCShell"));
        assert!(body.contains("DISCLAIMER"));
        assert!(body.contains("LLM-generated"));

        let (status, _, body) = http_get(server.port, "/does-not-exist").await;
        assert_eq!(status, 404);
        assert_eq!(body, "not found\n");
    }

    #[tokio::test]
    async fn agent_source_served_byte_for_byte() {
        let server = TestServer::start().await;
        let (status, _, body) = http_get(server.port, "/agent.lua").await;
        assert_eq!(status, 200);
        assert_eq!(body.as_bytes(), AGENT_SOURCE.as_bytes());
        assert!(body.contains("OCShell agent"));
    }

    #[tokio::test]
    async fn relay_both_directions() {
        let server = TestServer::start().await;
        let mut agent = server.connect("/agent").await;
        let mut client = server.connect("/client").await;

        agent
            .send_frame(OP_BINARY, b"terminal-bytes")
            .await
            .unwrap();
        let frame = tokio::time::timeout(Duration::from_secs(2), client.recv_frame())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(frame.opcode, OP_BINARY);
        assert_eq!(frame.payload, b"terminal-bytes");

        client.send_text("K 28 13 1").await.unwrap();
        let frame = tokio::time::timeout(Duration::from_secs(2), agent.recv_frame())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(frame.opcode, OP_TEXT);
        assert_eq!(frame.payload, b"K 28 13 1");
    }

    #[tokio::test]
    async fn backlog_replayed_on_client_attach() {
        let server = TestServer::start().await;
        let mut agent = server.connect("/agent").await;
        agent.send_frame(OP_BINARY, b"before-client").await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;

        let mut client = server.connect("/client").await;
        assert_eq!(recv_text(&mut client).await, b"before-client");
    }

    #[tokio::test]
    async fn second_client_replaces_first() {
        let server = TestServer::start().await;
        let mut agent = server.connect("/agent").await;
        let mut first = server.connect("/client").await;
        first.send_text("hello").await.unwrap();
        assert_eq!(recv_text(&mut agent).await, b"hello");

        let mut second = server.connect("/client").await;
        agent.send_frame(OP_BINARY, b"for-second").await.unwrap();
        assert_eq!(recv_text(&mut second).await, b"for-second");
    }

    #[tokio::test]
    async fn status_reports_roles() {
        let server = TestServer::start().await;
        let (_, _, body) = http_get(server.port, "/").await;
        assert!(body.contains("<td>agent</td><td><code>waiting</code>"));
        assert!(body.contains("<td>client</td><td><code>waiting</code>"));

        let mut agent = server.connect("/agent").await;
        let mut client = server.connect("/client").await;
        tokio::time::sleep(Duration::from_millis(100)).await;

        let (_, _, body) = http_get(server.port, "/").await;
        assert!(body.contains("<td>agent</td><td><code>connected</code>"));
        assert!(body.contains("<td>client</td><td><code>connected</code>"));

        agent.send_frame(OP_BINARY, b"x").await.unwrap();
        let _ = recv_text(&mut client).await;
    }

    #[tokio::test]
    async fn ping_gets_pong_and_close_is_acknowledged() {
        let server = TestServer::start().await;
        let mut client = server.connect("/client").await;

        client.send_frame(OP_PING, b"hi").await.unwrap();
        let frame = tokio::time::timeout(Duration::from_secs(2), client.recv_frame())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(frame.opcode, OP_PONG);
        assert_eq!(frame.payload, b"hi");

        let extra = tokio::time::timeout(Duration::from_millis(150), client.recv_frame()).await;
        assert!(extra.is_err(), "pong was sent more than once");

        client.send_frame(OP_CLOSE, &[]).await.unwrap();
        let frame = tokio::time::timeout(Duration::from_secs(2), client.recv_frame())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(frame.opcode, OP_CLOSE);
    }

    #[tokio::test]
    async fn backlog_is_limited_to_one_mib() {
        let mut state = RelayState::new();
        let chunk = vec![0u8; 256 * 1024];
        for _ in 0..8 {
            state.relay(Role::Agent, Message::Binary(Bytes::from(chunk.clone())));
        }
        let status = state.status();
        assert_eq!(status.backlog_bytes, BACKLOG_LIMIT);
    }

    #[tokio::test]
    async fn relay_to_dead_client_is_dropped_without_panicking() {
        let mut state = RelayState::new();
        let (tx, rx) = mpsc::channel(1);
        let (cancel, _cancelled) = oneshot::channel();
        state.attach(
            PeerHandle {
                id: 1,
                role: Role::Client,
                tx,
                _cancel: cancel,
            },
            IpAddr::from([127, 0, 0, 1]),
        );
        // Simulate the client task having exited without detaching yet.
        drop(rx);

        state.relay(Role::Agent, Message::Binary(Bytes::from_static(b"lost")));
        assert!(!state.status().client);

        // The agent keeps working: later output is buffered for the next client.
        state.relay(
            Role::Agent,
            Message::Binary(Bytes::from_static(b"buffered")),
        );
        assert_eq!(state.status().backlog_bytes, 8);
    }

    #[tokio::test]
    async fn client_disconnects_while_agent_sends() {
        let server = TestServer::start().await;
        let mut agent = server.connect("/agent").await;
        let mut client = server.connect("/client").await;
        client.send_text("hello").await.unwrap();
        assert_eq!(recv_text(&mut agent).await, b"hello");

        // Abruptly vanish. The relay may still try to write buffered frames to
        // this peer while the agent keeps sending.
        let (reader, writer) = client.into_split();
        drop(reader);
        drop(writer);

        for _ in 0..32 {
            agent
                .send_frame(OP_BINARY, b"frame-after-client-left")
                .await
                .unwrap();
            tokio::task::yield_now().await;
        }

        // The server and the agent connection are still alive.
        let (status, _, body) = http_get(server.port, "/healthz").await;
        assert_eq!(status, 200);
        assert_eq!(body, "ok\n");

        let mut second = server.connect("/client").await;
        agent
            .send_frame(OP_BINARY, b"after-reconnect")
            .await
            .unwrap();

        let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
        loop {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            let frame = tokio::time::timeout(remaining, second.recv_frame())
                .await
                .expect("new client never received the post-reconnect frame")
                .unwrap();
            if frame.payload == b"after-reconnect" {
                break;
            }
        }
    }
}
