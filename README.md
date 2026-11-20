```text
################################################################################
################################################################################
##                                                                            ##
##      W A R N I N G :  1 0 0 %   L L M - G E N E R A T E D   C O D E       ##
##                                                                            ##
##   THIS ENTIRE PROJECT WAS WRITTEN BY A LARGE LANGUAGE MODEL.               ##
##                                                                            ##
##   Not written by a human. Not reviewed by a human. Not audited.            ##
##   Not security-tested. Not production software. Probably wrong.            ##
##                                                                            ##
##   Model:    DeepSeek V4.1 Flash 1017                                       ##
##   Provider: DeepSeek                                                       ##
##   Method:   autonomous coding agent, 20 November 2026                      ##
##                                                                            ##
##   It exposes an interactive OpenComputers shell over a WebSocket.          ##
##   It has NO AUTHENTICATION. Anyone who can reach the port gets a shell     ##
##   on your in-game computer, with access to every attached device.          ##
##   It injects keystrokes into the machine and streams terminal output       ##
##   over plain TCP with no encryption.                                       ##
##                                                                            ##
##   Read DISCLAIMER.md before running any of this.                           ##
##                                                                            ##
################################################################################
################################################################################
```

# OCShell

An interactive OpenComputers 1 shell over HTTP/WebSocket.

```
 in-game OpenOS  ── ws://host:5235/agent  ──┐
                └─ https://host/api/agent/poll ─┤   relay server (no auth)
 your terminal  ── ws://host:5235/client ──┘
```

- The in-game agent connects **out**, starts a nested `/bin/sh`, streams the
  terminal output and injects remote keystrokes.
- Two agent transports:
  - `ws://host:port/agent` — raw TCP WebSocket (lowest latency, needs
    `enableTcp` and a plain TCP port).
  - `https://host` — HTTPS polling fallback using `internet.request`, for
    deployments where only a TLS reverse proxy is exposed (higher latency).
- The relay is a single-session bridge: one agent, one client.
- The CLI is a raw-terminal client: `ocs attach ws://host:5235/client`.

The relay and CLI are one Rust crate producing one binary, `ocs`. The Lua agent
and the wire protocol are unchanged from the 0.1.0 implementation. See
[`docs/PROTOCOL.md`](docs/PROTOCOL.md).

This targets **OpenComputers 1** (GTNH). It was developed against
**OS2Playground**, which now has raw TCP support (`internet.connect`). It has
not been run on a real server yet.

> **No authentication.** That is a deliberate choice, not an oversight. Do not
> put this on the open internet until you add a proxy with auth and TLS.

---

## Requirements

In-game:

- An internet card.
- `internet.enableHttp = true` (required for the agent download and the HTTPS
  polling fallback).
- `internet.enableTcp = true` for the raw WebSocket mode (OpenComputers
  defaults to true, but packs change it). Not needed for HTTPS polling.
- The host/port must pass OpenComputers' address filtering rules. Private
  addresses are denied by default; use a public host or add an allow rule.
  For local emulator tests add `allow ip:127.0.0.1/8` before the deny rules.
- A screen and keyboard attached to the computer (the agent mirrors the real
  terminal).

Host:

- A Rust toolchain (1.75 or newer) and Cargo.
- The TCP/HTTP-enabled OS2Playground binary for emulator testing.

---

## Build

```sh
cargo build --release
```

The binary is `target/release/ocs`. `./bin/ocs` is a thin wrapper that builds
the release binary (quietly, as a no-op when it is current) and executes it:

```sh
./bin/ocs --help
```

---

## Quick start

### 1. Host

```sh
./bin/ocs serve --host 0.0.0.0 --port 5235
```

It prints:

```text
agent:  ws://<host>:5235/agent
agent:  ocshell https://<host>   (HTTPS polling fallback)
client: ocs attach ws://<host>:5235/client
install: wget -f http://<host>:5235/agent.lua /bin/ocshell.lua
install: wget -f https://<host>/agent.lua /bin/ocshell.lua   (via TLS proxy)
```

### 2. In-game

At an OpenOS shell, direct TCP mode:

```sh
wget -f http://<host>:5235/agent.lua /bin/ocshell.lua
ocshell ws://<host>:5235/agent
```

or, when only the HTTPS reverse proxy is reachable:

```sh
wget -f https://<host>/agent.lua /bin/ocshell.lua
ocshell https://<host>
```

`ocshell` connects, starts a nested shell, and mirrors it. The computer's own
screen keeps working normally.

The HTTPS mode posts terminal output to `/api/agent/poll` and receives queued
keystrokes in the response. It does not need a raw TCP port or `enableTcp`,
but adds one HTTP round trip plus the polling interval (~150 ms in the current
agent) to every interaction.

### 3. Attach

```sh
./bin/ocs attach ws://<host>:5235/client
```

You get a live terminal. Type normally. `Ctrl+]` detaches; `Ctrl+C` is sent to
the in-game shell as an interrupt. Arrow keys, Home/End/Delete/PageUp/PageDown,
Enter, Tab and Backspace are mapped to OpenComputers scancodes.

If the client attaches after the agent, the server replays the buffered output
(up to 1 MiB), so you still see the prompt.

---

## One-shot commands

For automation, skip the terminal and run a command in a fresh in-game shell:

```sh
./target/release/ocs exec --url ws://oc.deprived.dev:5234/exec "ls /home"
```

or over plain HTTP:

```sh
curl -X POST --data 'ls /home' http://oc.deprived.dev:5234/api/exec
```

- Each call runs in a **new shell process**; `cd`/environment changes do not
  persist. The working directory starts at `/home` (the agent's cwd).
- The command’s stdout/stderr are captured to a temporary file, returned as
  plain text, and the file is removed.
- Output is capped at 256 KiB; the request times out after 30 seconds.
- No agent connected returns an error immediately.
- This executes arbitrary commands and has no authentication, exactly like the
  terminal.

---

## How it works

- **Relay:** Tokio + axum. `/agent` and `/client` are WebSocket upgrades; data
  frames are forwarded verbatim in both directions. The relay keeps at most
  1 MiB of agent output while no client is attached and replays it on attach.
  Replacing a peer closes the old connection; a failed write drops the frame
  and removes only that peer.
- **Agent transport (direct):** `component.internet.connect(host, port)` + a
  hand written RFC 6455 client. The OC handle is used with **dot calls**
  (`sock.read`, `sock.write`, `sock.finishConnect`, `sock.close`), matching
  real OpenComputers; colon calls are an emulator-only extension.
- **Agent transport (HTTPS):** `internet.request` POSTs to
  `/api/agent/poll?agent=...` with accumulated terminal output as the body;
  the response is the queued client commands. The server treats this as a
  virtual agent and bridges it to the WebSocket client, including backlog.
- **Terminal capture:** the agent wraps `tty.stream.write`, which is the sink
  behind `io.write`, `term.write`, prompts and command output. It sends that
  byte stream as WebSocket binary frames (or the HTTP POST body).
- **Colour/clear/cursor:** `term.clear` and `term.setCursor` are wrapped to
  emit ANSI sequences, because they draw directly to the GPU.
- **Input:** client text becomes `computer.pushSignal("key_down", keyboard,
  char, code)` inside a wrapper around `computer.pullSignal`. The keyboard
  address matters: OpenOS filters key events by `tty.keyboard()`.
- **Client rendering:** the CLI keeps a local 80x25 VT100 screen (`vt100`
  crate), feeds it the agent's output, and renders that screen into your
  terminal with explicit row positioning. A 120-column local terminal
  therefore does not change the OC line breaks. The agent also resizes the
  OpenComputers GPU to 80x25 (clamped to its maximum) on startup and when the
  client sends `S 80 25`.
- **Cooperative:** OpenComputers cannot preempt Lua. Input and output are
  serviced when the shell waits on an event (which it normally does). A
  program that never yields will not stream and cannot be interrupted.

---

## Limits

- **Terminal size:** the client needs at least an 80x25 terminal. It renders a
  fixed 80x25 screen; extra columns/rows are unused. The in-game GPU is resized
  to 80x25 if its maximum allows.
- **Raw terminal stream, not a GPU mirror.** Programs that draw directly with
  `gpu.set` instead of using `term`/`tty` will not render through OCShell.
  Normal shell commands, `lua`, and most `term`-based programs work.
- **No TLS on the raw WebSocket path.** `ws://` only; the OC TCP socket has no
  TLS stack. Use the HTTPS polling mode behind a TLS proxy if you need
  encrypted transport.
- **No auth.** Single agent, single client, anyone can attach.
- **Single session.** A new agent/client replaces the old one.
- **ASCII/UTF-8:** printable ASCII is batched; non-ASCII is sent one codepoint
  at a time. Wide characters are not fully modelled.
- **10-second output stall guard:** if the socket stops accepting writes for
  10 seconds, the agent disconnects rather than hanging the computer.
- **HTTPS polling latency:** output and input wait for the poll interval and
  one HTTPS round trip. Fine for a shell, unpleasant for interactive games.
- **Emulator:** OS2Playground now implements TCP, `internet_ready`, connection
  limits and OC-style filtering. Remaining gaps are tracked in that repo.

---

## Tests

```sh
cargo test
```

This runs the WebSocket framing/handshake, relay, backlog, HTTP polling and
key-mapping tests. Two end-to-end tests boot OpenOS, run the agent over both
transports, attach a client, type `ls`, and assert the listing. They are
skipped unless `OCPLAY` points at the TCP/HTTP-enabled `ocplay` binary:

```sh
OCPLAY=/path/to/OS2Playground/target/release/ocplay \
  cargo test --test ingame -- --nocapture
```

---

## Layout

```text
agent/ocshell.lua       in-game WebSocket/HTTPS agent + nested shell wrapper
Cargo.toml              single crate, binary `ocs`
src/main.rs             entry point
src/cli.rs              ocs serve / attach
src/server.rs           axum relay, /agent.lua and status page
src/client.rs           raw ANSI terminal client and key mapping
src/ws.rs               RFC 6455 framing, handshake and URL parsing
tests/computer.yaml     ocplay machine configuration
tests/ingame.rs         ocplay end-to-end test
docs/PROTOCOL.md        wire format
bin/ocs                 build-and-run wrapper
```

No license is granted. See `DISCLAIMER.md`.
