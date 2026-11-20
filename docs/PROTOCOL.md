# OCShell wire protocol

OCShell is a single-session relay between an in-game OpenComputers agent and a
terminal client. The protocol is deliberately small: WebSocket frames are
relayed verbatim and all shell semantics live in the two endpoints.

Version: `0.1.0` (unchanged by the Rust rewrite).

## Transport

- RFC 6455 WebSocket over plain TCP. `ws://` only; there is no TLS or `wss://`
  support, because the OpenComputers internet card has no TLS stack here.
- No subprotocols, extensions, or compression.
- Client-to-server frames are masked; server-to-client frames are unmasked.
- Text frames carry UTF-8 command lines. Binary frames carry raw terminal
  bytes.
- Incoming data frames may be fragmented; receivers reassemble them. Control
  frames are not fragmented and have payloads of at most 125 bytes.
- Maximum data-frame payload: 16 MiB (both server and client). The server
  rejects larger WebSocket messages.

## Roles and routes

| Path      | Role   | Direction                  |
| --------- | ------ | -------------------------- |
| `/agent`  | agent  | in-game OpenOS computer    |
| `/client` | client | `ocs attach` terminal      |

There is exactly one agent slot and one client slot.

- A new connection for a role replaces the old connection for that role. The
  old socket is closed.
- If no client is attached, agent data frames are buffered (see Backlog). If no
  agent is attached, client frames are dropped.
- The server does not originate protocol messages. It only relays frames,
  answers WebSocket control frames, and serves the HTTP endpoints.

## HTTPS polling agent

The agent can replace the `/agent` WebSocket with a single HTTP endpoint:

```http
POST /api/agent/poll?agent=<id>&state=<state>&version=<version>&uptime=<seconds>
Content-Type: application/octet-stream

<raw terminal output bytes>
```

- The request body is the terminal output accumulated since the previous poll.
  The server forwards it to the client as a binary WebSocket frame (or into the
  backlog when no client is attached).
- The response is `text/plain`; each line is a client command exactly as it
  would have been sent over the WebSocket (`K`, `T`, `X`, `S`).
- `agent` is `computer.address()`; a new id replaces the previous HTTP agent.
- There is one agent slot. A WebSocket agent and an HTTP agent replace each
  other; an HTTP agent is considered disconnected after 20 seconds without a
  poll.
- The agent polls at roughly 150 ms while the shell is running. Each request
  is a complete HTTPS round trip, so latency is higher than the direct
  WebSocket path.
- The same address filtering and `internet.enableHttp` rules apply as for any
  other `internet.request` call.

## Handshake

A normal WebSocket upgrade on `/agent` or `/client`:

```http
GET /client HTTP/1.1
Host: 127.0.0.1:5235
Upgrade: websocket
Connection: Upgrade
Sec-WebSocket-Key: <base64 nonce>
Sec-WebSocket-Version: 13

```

The server replies `101 Switching Protocols` with
`Sec-WebSocket-Accept: base64(SHA1(key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"))`.
Persistent HTTP endpoints are served on the same port:

| Endpoint        | Response                                                    |
| --------------- | ----------------------------------------------------------- |
| `GET /`         | HTML status page (agent/client state, URLs, LLM warning)     |
| `GET /healthz`  | `ok\n`                                                      |
| `GET /health`   | `ok\n` (alias)                                              |
| `GET /agent.lua`| the embedded `agent/ocshell.lua` source, for `wget`         |

The agent uses a fixed `Sec-WebSocket-Key` (`dGhlIHNhbXBsZSBub25jZQ==`) and
only checks that the status line is `101`. The Rust client verifies the accept
value.

## Client to agent

All client messages are WebSocket **text** frames. One frame may contain
several lines separated by `\n` or `\r\n`; the agent processes every non-empty
line. Lines are `<command> <arguments>`.

### `S <cols> <rows>`

Terminal size, sent once immediately after connecting (currently `S 80 25`).
The agent resizes the GPU/tty to that size, clamped to `gpu.maxResolution()`,
and clears the screen. The client renders a local 80x25 VT100 screen, so a
wider local terminal cannot change line wrapping.

### `T <hex>`

Printable text. `<hex>` is lowercase hexadecimal: two hex digits per byte. The
agent emits, for each byte, a `key_down` followed by a `key_up` with character
= byte and key code = 0:

```text
T 6c73          # "ls"
T 6c73202d6c61  # "ls -la"
```

### `K <code> <char> <down>`

A single key state change. `code` is an OpenComputers scancode, `char` is the
Unicode codepoint (0 when not applicable), and `down` is `1` for press or `0`
for release. The agent calls:

```lua
computer.pushSignal("key_down" or "key_up", keyboard_address, char, code)
```

`keyboard_address` is `tty.keyboard()`, so OpenOS accepts the event.

| Input              | Message                         |
| ------------------ | ------------------------------- |
| Enter              | `K 28 13 1`                     |
| Backspace          | `K 14 0 1`                      |
| Tab                | `K 15 0 1`                      |
| Escape             | `K 1 0 1`                       |
| Up / Down          | `K 200 0 1` / `K 208 0 1`       |
| Left / Right       | `K 203 0 1` / `K 205 0 1`       |
| Home / End         | `K 199 0 1` / `K 207 0 1`       |
| Insert / Delete    | `K 210 0 1` / `K 211 0 1`       |
| PageUp / PageDown  | `K 201 0 1` / `K 209 0 1`       |
| Non-ASCII codepoint| `K <cp> <cp> 1` then `K <cp> <cp> 0` |

Printable ASCII is batched into `T`; control keys, escape sequences, and
non-ASCII codepoints are sent individually. Arrow and navigation sequences are
recognised in both CSI (`ESC [ ...`) and SS3 (`ESC O ...`) forms. A lone
`Escape` is delivered after a short (100 ms) idle period.

### Ctrl combinations

`Ctrl+A` … `Ctrl+Z` are sent as a left-control press, the letter press and
release, then a left-control release:

```text
K 29 0 1
K <letter code> 0 1
K <letter code> 0 0
K 29 0 0
```

`Ctrl+C` is special: it sends `X` instead.

### `X`

Interrupt. The agent calls `computer.pushSignal("interrupted")`.

### Local detach

`Ctrl+]` never reaches the agent. The client sends a WebSocket close (code
1000) and restores the terminal.

## Agent to client

### Binary: terminal stream

Binary frames are raw terminal output bytes, forwarded verbatim. The agent
captures `tty.stream.write` (and synthesises ANSI for `term.clear` and
`term.setCursor`) and sends chunks of at most 4096 bytes. The stream is not a
GPU mirror; it is the byte stream an ANSI terminal would have received.

### Text: size and status

The agent sends one text frame at startup:

```text
SIZE <width> <height>\n
```

Clients ignore it. Clients also tolerate (and ignore) `AGENT_ONLINE\n`,
`AGENT_OFFLINE\n`, and `WAITING\n`, which older/other server implementations
may send. The current relay does not send them.

## Backlog

While no client is attached, the server buffers agent data frames in order.

- The limit is **1 MiB**, counted as payload bytes.
- After appending a frame, whole frames are dropped from the front until the
  total is at most 1 MiB. A single larger-than-limit frame is therefore
  dropped.
- On client attach the backlog is replayed in order on the new connection and
  cleared. Client-to-agent frames are not buffered.

## WebSocket control frames and failures

- **Ping:** answered with a Pong carrying the same payload. Pongs are ignored.
- **Close:** answered with a Close, then the connection is dropped.
- **Send failure:** a failed write to a peer (closed socket, connection reset,
  vanished client) drops that frame, marks the peer dead, and removes it from
  the relay state. It never panics and never affects the other role. The agent
  keeps running when the client disappears, and vice versa.
- **Outbound queue:** each attached peer has a bounded queue of 1024 messages
  (roughly 4 MiB for the agent's 4 KiB terminal chunks). A peer that stops
  draining its queue is disconnected rather than letting the server buffer
  without bound. The other role is unaffected.

## Agent-side limits

These are properties of the Lua agent, not of the relay:

- Outgoing buffer capped at 1 MiB. On overflow the agent drops the oldest data
  and logs a warning, rather than freezing the computer.
- WebSocket mode flushes and polls at least every 50 ms while the shell waits
  on an event. HTTPS mode polls at roughly 150 ms and sends the buffered
  terminal output in the request body.
- If the WebSocket accepts no writes for 10 seconds, the agent disconnects.
  In HTTPS mode, 20 consecutive poll failures disconnect the agent.
- OpenComputers cannot preempt Lua. A program that never yields will not
  stream output and cannot be interrupted.
