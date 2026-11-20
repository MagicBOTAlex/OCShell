//! OCShell relay server.
//!
//! One agent (the in-game computer) and one client (the CLI) are bridged
//! together. The server is deliberately dumb: it forwards WebSocket messages
//! between the two roles. All shell semantics live in the agent and client.
//!
//! WARNING: no authentication. See DISCLAIMER.md.

use std::collections::{HashMap, VecDeque};
use std::io;
use std::net::{IpAddr, SocketAddr};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::body::Bytes;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{ConnectInfo, Query, State};
use axum::http::{header, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::Router;
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use tokio::net::TcpListener;
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::{mpsc, oneshot, Mutex};

use crate::ws::MAX_PAYLOAD;

pub const BACKLOG_LIMIT: usize = 1024 * 1024;

/// Outbound messages queued for one attached peer. Agent output arrives in
/// 4 KiB chunks, so this is roughly 4 MiB before a stalled peer is dropped.
const PEER_QUEUE: usize = 1024;

/// An agent using the HTTP(S) polling fallback instead of a WebSocket.
struct HttpAgent {
    id: String,
    queue: VecDeque<Message>,
    last_seen: Instant,
}

const HTTP_AGENT_TIMEOUT: Duration = Duration::from_secs(20);

/// The in-game agent, embedded so the binary is self-contained.
pub const AGENT_SOURCE: &str = include_str!("../agent/ocshell.lua");

static NEXT_PEER_ID: AtomicU64 = AtomicU64::new(1);
static NEXT_EXEC_ID: AtomicU64 = AtomicU64::new(1);

const EXEC_TIMEOUT: Duration = Duration::from_secs(30);

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
    http_agent: Option<HttpAgent>,
    client: Option<PeerHandle>,
    backlog: VecDeque<Message>,
    backlog_bytes: usize,
    exec_waiters: HashMap<u64, oneshot::Sender<String>>,
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
            http_agent: None,
            client: None,
            backlog: VecDeque::new(),
            backlog_bytes: 0,
            exec_waiters: HashMap::new(),
            started: Instant::now(),
        }
    }

    fn attach(&mut self, peer: PeerHandle, ip: IpAddr) -> Vec<Message> {
        let role = peer.role;
        let old = match role {
            Role::Agent => {
                self.http_agent = None;
                self.agent.replace(peer)
            }
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
                // Exec results are routed to their waiting request instead of
                // the terminal client.
                if let Message::Text(text) = &message {
                    if let Some(rest) = text.as_str().strip_prefix("R ") {
                        if let Some((id, encoded)) = rest.split_once(' ') {
                            if let (Ok(id), Ok(bytes)) =
                                (id.parse::<u64>(), hex::decode(encoded.trim()))
                            {
                                println!("[ocs] exec {id} result: {} bytes", bytes.len());
                                self.complete_exec(id, String::from_utf8_lossy(&bytes).to_string());
                            } else {
                                println!("[ocs] malformed exec result");
                            }
                        }
                        return;
                    }
                }
                self.deliver_to_client(message)
            }
            Role::Client => {
                if let Some(http) = self.http_agent.as_mut() {
                    if http.last_seen.elapsed() <= HTTP_AGENT_TIMEOUT {
                        if let Message::Text(text) = message {
                            if http.queue.len() >= PEER_QUEUE {
                                http.queue.pop_front();
                            }
                            http.queue.push_back(Message::Text(text));
                        }
                        return;
                    }
                    self.http_agent = None;
                    println!("[ocs] http agent timed out");
                }

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

    fn request_exec(&mut self, command: &str) -> Result<(u64, oneshot::Receiver<String>), String> {
        let id = NEXT_EXEC_ID.fetch_add(1, Ordering::Relaxed);
        println!("[ocs] exec {id}: {} bytes", command.len());
        let message = Message::Text(format!("E {id} {}\n", hex::encode(command)).into());

        if let Some(agent) = self.agent.as_ref() {
            agent
                .tx
                .try_send(message)
                .map_err(|_| "agent is not accepting commands".to_string())?;
        } else if let Some(http) = self.http_agent.as_mut() {
            if http.last_seen.elapsed() > HTTP_AGENT_TIMEOUT {
                self.http_agent = None;
                return Err("no agent connected".to_string());
            }
            if http.queue.len() >= PEER_QUEUE {
                return Err("agent queue is full".to_string());
            }
            http.queue.push_back(message);
        } else {
            return Err("no agent connected".to_string());
        }

        let (sender, receiver) = oneshot::channel();
        self.exec_waiters.insert(id, sender);
        Ok((id, receiver))
    }

    fn complete_exec(&mut self, id: u64, output: String) {
        if let Some(sender) = self.exec_waiters.remove(&id) {
            let _ = sender.send(output);
        }
    }

    fn deliver_to_client(&mut self, message: Message) {
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

    /// Handle one HTTP polling request from the agent. The body is terminal
    /// output; the response is the concatenated client command lines.
    fn http_poll(&mut self, id: String, body: Vec<u8>) -> String {
        if self.agent.take().is_some() {
            println!("[ocs] websocket agent replaced by http agent");
        }

        let replace = self
            .http_agent
            .as_ref()
            .map_or(true, |agent| agent.id != id);
        if replace {
            self.http_agent = Some(HttpAgent {
                id: id.clone(),
                queue: VecDeque::new(),
                last_seen: Instant::now(),
            });
            println!("[ocs] http agent connected ({id})");
        } else if let Some(agent) = self.http_agent.as_mut() {
            agent.last_seen = Instant::now();
        }

        if !body.is_empty() {
            self.deliver_to_client(Message::Binary(body.into()));
        }

        let mut commands = String::new();
        if let Some(agent) = self.http_agent.as_mut() {
            while let Some(message) = agent.queue.pop_front() {
                if let Message::Text(text) = message {
                    commands.push_str(text.as_str());
                    if !commands.ends_with('\n') {
                        commands.push('\n');
                    }
                }
            }
        }
        commands
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
        let http_agent = self
            .http_agent
            .as_ref()
            .is_some_and(|agent| agent.last_seen.elapsed() <= HTTP_AGENT_TIMEOUT);
        Status {
            agent: self.agent.is_some() || http_agent,
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
        .route("/api/agent/poll", post(agent_poll))
        .route("/api/agent/result", post(agent_exec_result))
        .route("/api/exec", post(exec_http))
        .route("/exec", get(exec_socket))
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
         <code>ocshell ws://{host}:{port}/agent</code> (direct TCP)<br>\n\
         <code>ocshell https://{host}</code> (HTTPS polling fallback)</p>\n\
         <p>Client: <code>ocs attach ws://{host}:{port}/client</code></p>\n\
         <p>Exec: <code>ocs exec --url ws://{host}:{port}/exec \"ls\"</code><br>\n\
         or <code>curl -X POST --data 'ls' http://{host}:{port}/api/exec</code></p>\n\
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

async fn exec_request(state: &AppState, command: String) -> Result<String, String> {
    let (id, receiver) = { state.relay.lock().await.request_exec(&command)? };
    match tokio::time::timeout(EXEC_TIMEOUT, receiver).await {
        Ok(Ok(output)) => Ok(output),
        Ok(Err(_)) => Err("agent disconnected".to_string()),
        Err(_) => {
            state.relay.lock().await.exec_waiters.remove(&id);
            Err("command timed out".to_string())
        }
    }
}

async fn exec_http(State(state): State<AppState>, body: Bytes) -> Response {
    let command = String::from_utf8_lossy(&body).trim().to_string();
    if command.is_empty() {
        return text_response(StatusCode::BAD_REQUEST, "empty command\n".to_string());
    }
    match exec_request(&state, command).await {
        Ok(output) => text_response(StatusCode::OK, output),
        Err(error) => text_response(StatusCode::SERVICE_UNAVAILABLE, format!("ERROR: {error}\n")),
    }
}

async fn exec_socket(ws: WebSocketUpgrade, State(state): State<AppState>) -> Response {
    ws.max_message_size(MAX_PAYLOAD)
        .on_upgrade(move |socket| handle_exec_socket(socket, state))
}

async fn handle_exec_socket(mut socket: WebSocket, state: AppState) {
    let command = match socket.recv().await {
        Some(Ok(Message::Text(text))) => text.as_str().to_string(),
        _ => return,
    };
    let result = if command.trim().is_empty() {
        "ERROR: empty command\n".to_string()
    } else {
        match exec_request(&state, command).await {
            Ok(output) => output,
            Err(error) => format!("ERROR: {error}\n"),
        }
    };
    let _ = socket.send(Message::Text(result.into())).await;
    let _ = socket.send(Message::Close(None)).await;
}

#[derive(Deserialize)]
struct ExecResultQuery {
    id: u64,
}

async fn agent_exec_result(
    State(state): State<AppState>,
    Query(query): Query<ExecResultQuery>,
    body: Bytes,
) -> Response {
    let output = String::from_utf8_lossy(&body).to_string();
    state.relay.lock().await.complete_exec(query.id, output);
    text_response(StatusCode::OK, "ok\n".to_string())
}

#[derive(Deserialize)]
struct AgentPollQuery {
    agent: Option<String>,
}

async fn agent_poll(
    State(state): State<AppState>,
    Query(query): Query<AgentPollQuery>,
    body: Bytes,
) -> Response {
    let id = query.agent.unwrap_or_else(|| "http-agent".to_string());
    let commands = state.relay.lock().await.http_poll(id, body.to_vec());
    text_response(StatusCode::OK, commands)
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

    async fn http_post(port: u16, path: &str, body: &[u8]) -> String {
        let mut stream = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
        let head = format!(
            "POST {path} HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Length: {}\r\n\
             Content-Type: application/octet-stream\r\nConnection: close\r\n\r\n",
            body.len()
        );
        stream.write_all(head.as_bytes()).await.unwrap();
        stream.write_all(body).await.unwrap();
        let mut data = Vec::new();
        stream.read_to_end(&mut data).await.unwrap();
        let text = String::from_utf8_lossy(&data).to_string();
        let (_, body) = text.split_once("\r\n\r\n").unwrap_or((&text, ""));
        body.to_string()
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
    async fn exec_result_is_routed_to_the_request() {
        let server = TestServer::start().await;
        let mut agent = server.connect("/agent").await;
        let mut exec = server.connect("/exec").await;

        exec.send_text("echo hi").await.unwrap();
        let frame = tokio::time::timeout(Duration::from_secs(2), agent.recv_frame())
            .await
            .unwrap()
            .unwrap();
        let request = String::from_utf8(frame.payload).unwrap();
        let (id, encoded) = request
            .trim()
            .strip_prefix("E ")
            .unwrap()
            .split_once(' ')
            .unwrap();
        assert_eq!(hex::decode(encoded).unwrap(), b"echo hi");

        agent
            .send_frame(OP_TEXT, format!("R {id} 6869").as_bytes())
            .await
            .unwrap();
        let result = tokio::time::timeout(Duration::from_secs(2), exec.recv_frame())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(result.payload, b"hi");
    }

    #[tokio::test]
    async fn http_agent_relays_both_directions() {
        let server = TestServer::start().await;
        let mut client = server.connect("/client").await;

        // Agent output arrives in the POST body.
        http_post(server.port, "/api/agent/poll?agent=test", b"http-output").await;
        let frame = tokio::time::timeout(Duration::from_secs(2), client.recv_frame())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(frame.opcode, OP_BINARY);
        assert_eq!(frame.payload, b"http-output");

        // Client input is returned in the next poll response.
        client.send_text("K 28 13 1").await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
        let commands = http_post(server.port, "/api/agent/poll?agent=test", b"").await;
        assert!(commands.contains("K 28 13 1"), "{commands}");

        let (_, _, body) = http_get(server.port, "/").await;
        assert!(body.contains("<td>agent</td><td><code>connected</code>"));
    }

    #[tokio::test]
    async fn http_agent_output_is_backlogged_before_client_attach() {
        let server = TestServer::start().await;
        http_post(server.port, "/api/agent/poll?agent=test", b"early-output").await;
        let mut client = server.connect("/client").await;
        let frame = tokio::time::timeout(Duration::from_secs(2), client.recv_frame())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(frame.payload, b"early-output");
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
