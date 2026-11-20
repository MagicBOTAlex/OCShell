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
 in-game OpenOS  ── ws://host:5235/agent ──┐
                                           │   relay server (no auth)
 your terminal  ── ws://host:5235/client ──┘
```

- The in-game agent connects **out** to the relay over a raw TCP WebSocket,
  starts a nested `/bin/sh`, streams the terminal output and injects remote
  keystrokes.
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
- `internet.enableTcp = true` (OpenComputers defaults to true, but packs
  change it).
- `internet.enableHttp = true` (only to download the agent with `wget`).
- The host/port must pass OpenComputers' address filtering rules. Private
  addresses are denied by default; use a public host or add an allow rule.
- A screen and keyboard attached to the computer (the agent mirrors the real
  terminal).

Host:

- A Rust toolchain (1.75 or newer) and Cargo.
- The TCP-enabled OS2Playground binary for emulator testing.

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
client: ocs attach ws://<host>:5235/client
install: wget -f http://<host>:5235/agent.lua /bin/ocshell.lua
```

### 2. In-game

At an OpenOS shell:

```sh
wget -f http://<host>:5235/agent.lua /bin/ocshell.lua
ocshell ws://<host>:5235/agent
```

`ocshell` connects, starts a nested shell, and mirrors it. The computer's own
screen keeps working normally.

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

## How it works

- **Relay:** Tokio + axum. `/agent` and `/client` are WebSocket upgrades; data
  frames are forwarded verbatim in both directions. The relay keeps at most
  1 MiB of agent output while no client is attached and replays it on attach.
  Replacing a peer closes the old connection; a failed write drops the frame
  and removes only that peer.
- **Agent transport:** `component.internet.connect(host, port)` + a hand
  written RFC 6455 client. The OC handle is used with **dot calls**
  (`sock.read`, `sock.write`, `sock.finishConnect`, `sock.close`), matching
  real OpenComputers; colon calls are an emulator-only extension.
- **Terminal capture:** the agent wraps `tty.stream.write`, which is the sink
  behind `io.write`, `term.write`, prompts and command output. It sends that
  byte stream as WebSocket binary frames.
- **Colour/clear/cursor:** `term.clear` and `term.setCursor` are wrapped to
  emit ANSI sequences, because they draw directly to the GPU.
- **Input:** client text becomes `computer.pushSignal("key_down", keyboard,
  char, code)` inside a wrapper around `computer.pullSignal`. The keyboard
  address matters: OpenOS filters key events by `tty.keyboard()`.
- **Cooperative:** OpenComputers cannot preempt Lua. Input and output are
  serviced when the shell waits on an event (which it normally does). A
  program that never yields will not stream and cannot be interrupted.

---

## Limits

- **Raw terminal stream, not a GPU mirror.** Programs that draw directly with
  `gpu.set` instead of using `term`/`tty` will not render through OCShell.
  Normal shell commands, `lua`, and most `term`-based programs work.
- **No TLS.** `ws://` only. The internet card has no TLS stack here.
- **No auth.** Single agent, single client, anyone can attach.
- **Single session.** A new agent/client replaces the old one.
- **ASCII/UTF-8:** printable ASCII is batched; non-ASCII is sent one codepoint
  at a time. Wide characters are not fully modelled.
- **10-second output stall guard:** if the socket stops accepting writes for
  10 seconds, the agent disconnects rather than hanging the computer.
- **Emulator gaps:** OS2Playground's TCP support is not 1:1 with the mod yet.
  See its README and the parity audit. Most importantly, the emulator has no
  address filtering and no `internet_ready` signal; OCShell polls instead.

---

## Tests

```sh
cargo test
```

This runs the WebSocket framing/handshake, relay, backlog, HTTP and key-mapping
tests. The end-to-end test boots OpenOS, runs the agent over TCP, attaches a
client, types `ls`, and asserts the listing. It is skipped unless `OCPLAY`
points at the TCP-enabled `ocplay` binary:

```sh
OCPLAY=/path/to/OS2Playground/target/release/ocplay \
  cargo test --test ingame -- --nocapture
```

---

## Layout

```text
agent/ocshell.lua       in-game WebSocket agent + nested shell wrapper
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
