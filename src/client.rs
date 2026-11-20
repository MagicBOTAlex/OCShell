//! Interactive raw-terminal client for OCShell.

use std::io::{self, IsTerminal, Read, Write};
use std::time::Duration;

use crossterm::terminal;
use tokio::sync::{mpsc, watch};
use tokio::time::Instant;

use crate::ws::{
    parse_ws_url, WsReader, WsStream, WsWriter, OP_BINARY, OP_CLOSE, OP_PING, OP_PONG, OP_TEXT,
};

const ESC_IDLE: Duration = Duration::from_millis(100);

fn key_code(name: &str) -> u16 {
    match name {
        "enter" => 28,
        "backspace" => 14,
        "tab" => 15,
        "up" => 200,
        "down" => 208,
        "left" => 203,
        "right" => 205,
        "home" => 199,
        "end" => 207,
        "delete" => 211,
        "pageup" => 201,
        "pagedown" => 209,
        "insert" => 210,
        "escape" => 1,
        "lcontrol" => 29,
        other => unreachable!("unknown key {other}"),
    }
}

fn letter_code(letter: u8) -> Option<u16> {
    let code = match letter {
        b'a' => 30,
        b'b' => 48,
        b'c' => 46,
        b'd' => 32,
        b'e' => 18,
        b'f' => 33,
        b'g' => 34,
        b'h' => 35,
        b'i' => 23,
        b'j' => 36,
        b'k' => 37,
        b'l' => 38,
        b'm' => 50,
        b'n' => 49,
        b'o' => 24,
        b'p' => 25,
        b'q' => 16,
        b'r' => 19,
        b's' => 31,
        b't' => 20,
        b'u' => 22,
        b'v' => 47,
        b'w' => 17,
        b'x' => 45,
        b'y' => 21,
        b'z' => 44,
        _ => return None,
    };
    Some(code)
}

fn key_message(name: &str) -> String {
    let code = key_code(name);
    let character = if name == "enter" { 13 } else { 0 };
    format!("K {code} {character} 1")
}

fn hex_lower(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push_str(&format!("{byte:02x}"));
    }
    out
}

fn text_message(bytes: &[u8]) -> String {
    format!("T {}", hex_lower(bytes))
}

fn control_message(letter: u8, out: &mut Vec<String>) {
    if letter == b'c' {
        out.push("X".to_string());
        return;
    }
    let Some(code) = letter_code(letter) else {
        return;
    };
    // Ctrl+A..Z: the agent sees lcontrol down, key down/up, lcontrol up.
    out.push(format!("K {} 0 1", key_code("lcontrol")));
    out.push(format!("K {code} 0 1"));
    out.push(format!("K {code} 0 0"));
    out.push(format!("K {} 0 0", key_code("lcontrol")));
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Escape {
    Incomplete,
    Key(&'static str, usize),
    Unknown(usize),
    Bare,
}

fn parse_escape(data: &[u8]) -> Escape {
    if data.len() < 2 {
        return Escape::Incomplete;
    }
    match data[1] {
        b'[' => {
            let mut end = 2usize;
            while end < data.len() && !(0x40..=0x7E).contains(&data[end]) {
                end += 1;
            }
            if end >= data.len() {
                return Escape::Incomplete;
            }
            let name = match &data[2..=end] {
                b"A" => Some("up"),
                b"B" => Some("down"),
                b"C" => Some("right"),
                b"D" => Some("left"),
                b"H" => Some("home"),
                b"F" => Some("end"),
                b"1~" => Some("home"),
                b"2~" => Some("insert"),
                b"3~" => Some("delete"),
                b"4~" => Some("end"),
                b"5~" => Some("pageup"),
                b"6~" => Some("pagedown"),
                b"7~" => Some("home"),
                b"8~" => Some("end"),
                _ => None,
            };
            let consumed = end + 1;
            match name {
                Some(name) => Escape::Key(name, consumed),
                None => Escape::Unknown(consumed),
            }
        }
        b'O' => {
            if data.len() < 3 {
                return Escape::Incomplete;
            }
            let name = match data[2] {
                b'A' => Some("up"),
                b'B' => Some("down"),
                b'C' => Some("right"),
                b'D' => Some("left"),
                _ => None,
            };
            match name {
                Some(name) => Escape::Key(name, 3),
                None => Escape::Unknown(3),
            }
        }
        // Escape followed by an ordinary byte: send Escape, then let the outer
        // loop process that byte normally.
        _ => Escape::Bare,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Outcome {
    Continue,
    Detach,
}

/// Translate raw terminal bytes into OCShell agent messages.
///
/// Complete sequences are consumed; an incomplete escape or UTF-8 sequence is
/// left in `pending` for the next read. This is the same mapping as the Python
/// client (printable ASCII batched, other codepoints as `K`).
fn process_input(pending: &mut Vec<u8>, out: &mut Vec<String>) -> Outcome {
    let data = std::mem::take(pending);
    let mut text = Vec::new();
    let mut index = 0usize;

    while index < data.len() {
        let byte = data[index];

        if byte == 0x1D {
            // Ctrl+] detaches locally.
            if !text.is_empty() {
                out.push(text_message(&text));
            }
            return Outcome::Detach;
        }

        if byte == 0x1B {
            match parse_escape(&data[index..]) {
                Escape::Incomplete => {
                    if !text.is_empty() {
                        out.push(text_message(&text));
                    }
                    pending.extend_from_slice(&data[index..]);
                    return Outcome::Continue;
                }
                Escape::Key(name, consumed) => {
                    if !text.is_empty() {
                        out.push(text_message(&text));
                        text.clear();
                    }
                    out.push(key_message(name));
                    index += consumed;
                }
                Escape::Unknown(consumed) => {
                    if !text.is_empty() {
                        out.push(text_message(&text));
                        text.clear();
                    }
                    index += consumed;
                }
                Escape::Bare => {
                    if !text.is_empty() {
                        out.push(text_message(&text));
                        text.clear();
                    }
                    out.push(key_message("escape"));
                    index += 1;
                }
            }
            continue;
        }

        if byte < 0x20 {
            if !text.is_empty() {
                out.push(text_message(&text));
                text.clear();
            }
            match byte {
                0x0D | 0x0A => out.push(key_message("enter")),
                0x09 => out.push(key_message("tab")),
                1..=26 => control_message(byte + 0x60, out),
                _ => {}
            }
            index += 1;
            continue;
        }

        if byte < 0x7F {
            text.push(byte);
            index += 1;
            continue;
        }

        if byte == 0x7F {
            if !text.is_empty() {
                out.push(text_message(&text));
                text.clear();
            }
            out.push(key_message("backspace"));
            index += 1;
            continue;
        }

        // Non-ASCII: emit one codepoint at a time.
        if !text.is_empty() {
            out.push(text_message(&text));
            text.clear();
        }
        let length = if (byte & 0xE0) == 0xC0 {
            2
        } else if (byte & 0xF0) == 0xE0 {
            3
        } else if (byte & 0xF8) == 0xF0 {
            4
        } else {
            1
        };
        if index + length > data.len() {
            pending.extend_from_slice(&data[index..]);
            return Outcome::Continue;
        }
        match std::str::from_utf8(&data[index..index + length]) {
            Ok(character) if character.chars().count() == 1 => {
                let codepoint = character.chars().next().unwrap() as u32;
                out.push(format!("K {codepoint} {codepoint} 1"));
                out.push(format!("K {codepoint} {codepoint} 0"));
                index += length;
            }
            _ => index += 1,
        }
    }

    if !text.is_empty() {
        out.push(text_message(&text));
    }
    Outcome::Continue
}

struct RawTerminal;

impl RawTerminal {
    fn enter() -> io::Result<Self> {
        terminal::enable_raw_mode()?;
        let mut stdout = io::stdout();
        stdout.write_all(b"\x1b[?1049h\x1b[?25l\x1b[2J\x1b[H")?;
        stdout.flush()?;
        Ok(Self)
    }
}

impl Drop for RawTerminal {
    fn drop(&mut self) {
        let mut stdout = io::stdout();
        let _ = stdout.write_all(b"\x1b[?25h\x1b[?1049l");
        let _ = stdout.flush();
        let _ = terminal::disable_raw_mode();
    }
}

enum Outgoing {
    Text(String),
    Pong(Vec<u8>),
    Close,
}

fn is_status_text(payload: &[u8]) -> bool {
    payload == b"AGENT_ONLINE\n"
        || payload == b"AGENT_OFFLINE\n"
        || payload == b"WAITING\n"
        || payload.starts_with(b"SIZE ")
}

async fn reader_task(
    mut reader: WsReader<tokio::io::ReadHalf<tokio::net::TcpStream>>,
    outgoing: mpsc::UnboundedSender<Outgoing>,
    stop: watch::Sender<bool>,
) {
    let mut saw_output = false;
    let mut stdout = io::stdout();

    while let Ok(frame) = reader.recv_frame().await {
        match frame.opcode {
            OP_TEXT | OP_BINARY => {
                if frame.opcode == OP_TEXT && is_status_text(&frame.payload) {
                    if !saw_output && !frame.payload.starts_with(b"SIZE ") {
                        let text = std::str::from_utf8(&frame.payload)
                            .unwrap_or_default()
                            .trim();
                        let _ = stdout.write_all(format!("\r\n[ocs] {text}\r\n").as_bytes());
                        let _ = stdout.flush();
                    }
                    continue;
                }
                saw_output = true;
                if stdout.write_all(&frame.payload).is_err() {
                    break;
                }
                if stdout.flush().is_err() {
                    break;
                }
            }
            OP_PING => {
                if outgoing.send(Outgoing::Pong(frame.payload)).is_err() {
                    break;
                }
            }
            OP_CLOSE => break,
            _ => {}
        }
    }

    let _ = stop.send(true);
}

async fn writer_task(
    mut writer: WsWriter<tokio::io::WriteHalf<tokio::net::TcpStream>>,
    mut outgoing: mpsc::UnboundedReceiver<Outgoing>,
    stop: watch::Sender<bool>,
) {
    while let Some(message) = outgoing.recv().await {
        let result = match message {
            Outgoing::Text(text) => writer.send_text(&text).await,
            Outgoing::Pong(payload) => writer.send_frame(OP_PONG, &payload).await,
            Outgoing::Close => {
                let _ = writer.send_frame(OP_CLOSE, &[0x03, 0xE8]).await;
                break;
            }
        };
        if result.is_err() {
            break;
        }
    }
    let _ = stop.send(true);
}

/// Attach the current terminal to an OCShell relay.
pub async fn attach(url: &str) -> i32 {
    let (host, port, path) = match parse_ws_url(url) {
        Ok(parsed) => parsed,
        Err(error) => {
            eprintln!("error: cannot connect to {url}: {error}");
            return 1;
        }
    };

    let stream = match WsStream::connect(&host, port, &path, Duration::from_secs(10)).await {
        Ok(stream) => stream,
        Err(error) => {
            eprintln!("error: cannot connect to {url}: {error}");
            return 1;
        }
    };

    if !io::stdin().is_terminal() || !io::stdout().is_terminal() {
        eprintln!("error: ocs attach needs an interactive terminal");
        return 1;
    }

    eprintln!("OCShell attached to {url} (Ctrl+] detaches)");

    let (cols, rows) = terminal::size().unwrap_or((80, 25));
    let mut stream = stream;
    if let Err(error) = stream.send_text(&format!("S {cols} {rows}")).await {
        eprintln!("error: cannot talk to {url}: {error}");
        return 1;
    }

    let guard = match RawTerminal::enter() {
        Ok(guard) => guard,
        Err(error) => {
            eprintln!("error: cannot enter raw mode: {error}");
            return 1;
        }
    };

    let (reader, writer) = stream.into_split();
    let (outgoing, outgoing_rx) = mpsc::unbounded_channel();
    let (stop, mut stopped) = watch::channel(false);

    let reader_handle = tokio::spawn(reader_task(reader, outgoing.clone(), stop.clone()));
    let mut writer_handle = tokio::spawn(writer_task(writer, outgoing_rx, stop.clone()));

    let (stdin_tx, mut stdin_rx) = mpsc::channel::<Vec<u8>>(64);
    std::thread::spawn(move || {
        let mut stdin = io::stdin();
        let mut chunk = [0u8; 1024];
        loop {
            match stdin.read(&mut chunk) {
                Ok(0) | Err(_) => break,
                Ok(count) => {
                    if stdin_tx.blocking_send(chunk[..count].to_vec()).is_err() {
                        break;
                    }
                }
            }
        }
    });

    let mut pending: Vec<u8> = Vec::new();
    let mut escape_deadline: Option<Instant> = None;

    'session: loop {
        if *stopped.borrow() {
            break;
        }
        let deadline = escape_deadline;
        let escape_sleep = async move {
            match deadline {
                Some(deadline) => tokio::time::sleep_until(deadline).await,
                None => std::future::pending::<()>().await,
            }
        };

        tokio::select! {
            result = stopped.changed() => {
                let _ = result;
                break;
            }
            chunk = stdin_rx.recv() => {
                let Some(chunk) = chunk else { break };
                pending.extend_from_slice(&chunk);
                let mut messages = Vec::new();
                let outcome = process_input(&mut pending, &mut messages);
                for message in messages {
                    if outgoing.send(Outgoing::Text(message)).is_err() {
                        break 'session;
                    }
                }
                if outcome == Outcome::Detach {
                    break;
                }
                escape_deadline = if pending.as_slice() == b"\x1b" {
                    Some(Instant::now() + ESC_IDLE)
                } else {
                    None
                };
            }
            _ = escape_sleep, if escape_deadline.is_some() => {
                if pending.as_slice() == b"\x1b" {
                    pending.clear();
                    let _ = outgoing.send(Outgoing::Text(key_message("escape")));
                }
                escape_deadline = None;
            }
        }
    }

    let _ = outgoing.send(Outgoing::Close);
    drop(outgoing);
    let _ = tokio::time::timeout(Duration::from_millis(500), &mut writer_handle).await;
    writer_handle.abort();
    reader_handle.abort();
    drop(guard);
    0
}

#[cfg(test)]
mod tests {
    use super::*;

    fn messages(input: &[u8]) -> Vec<String> {
        let mut pending = Vec::new();
        let mut out = Vec::new();
        pending.extend_from_slice(input);
        assert_eq!(process_input(&mut pending, &mut out), Outcome::Continue);
        assert!(pending.is_empty(), "unexpected pending bytes: {pending:?}");
        out
    }

    #[test]
    fn printable_ascii_is_batched() {
        assert_eq!(messages(b"abc"), vec!["T 616263"]);
        assert_eq!(messages(b"ls -la"), vec!["T 6c73202d6c61"]);
    }

    #[test]
    fn enter_backspace_tab() {
        assert_eq!(messages(b"\r"), vec!["K 28 13 1"]);
        assert_eq!(messages(b"\n"), vec!["K 28 13 1"]);
        assert_eq!(messages(&[0x7F]), vec!["K 14 0 1"]);
        assert_eq!(messages(b"\t"), vec!["K 15 0 1"]);
    }

    #[test]
    fn ctrl_h_is_control_h_not_backspace() {
        // In raw mode a real Backspace key sends 0x7F; 0x08 stays Ctrl+H,
        // matching the Python client's control-letter path.
        assert_eq!(
            messages(&[0x08]),
            vec!["K 29 0 1", "K 35 0 1", "K 35 0 0", "K 29 0 0"]
        );
    }

    #[test]
    fn control_letters_and_interrupt() {
        assert_eq!(
            messages(&[1]),
            vec!["K 29 0 1", "K 30 0 1", "K 30 0 0", "K 29 0 0"]
        );
        assert_eq!(messages(&[3]), vec!["X"]);
        assert_eq!(
            messages(&[26]),
            vec!["K 29 0 1", "K 44 0 1", "K 44 0 0", "K 29 0 0"]
        );
    }

    #[test]
    fn ctrl_right_bracket_detaches() {
        let mut pending = Vec::new();
        let mut out = Vec::new();
        pending.extend_from_slice(b"ab");
        pending.push(0x1D);
        assert_eq!(process_input(&mut pending, &mut out), Outcome::Detach);
        assert_eq!(out, vec!["T 6162"]);
    }

    #[test]
    fn arrows_and_navigation() {
        assert_eq!(messages(b"\x1b[A"), vec!["K 200 0 1"]);
        assert_eq!(messages(b"\x1b[B"), vec!["K 208 0 1"]);
        assert_eq!(messages(b"\x1b[C"), vec!["K 205 0 1"]);
        assert_eq!(messages(b"\x1b[D"), vec!["K 203 0 1"]);
        assert_eq!(messages(b"\x1b[H"), vec!["K 199 0 1"]);
        assert_eq!(messages(b"\x1b[F"), vec!["K 207 0 1"]);
        assert_eq!(messages(b"\x1b[1~"), vec!["K 199 0 1"]);
        assert_eq!(messages(b"\x1b[2~"), vec!["K 210 0 1"]);
        assert_eq!(messages(b"\x1b[3~"), vec!["K 211 0 1"]);
        assert_eq!(messages(b"\x1b[4~"), vec!["K 207 0 1"]);
        assert_eq!(messages(b"\x1b[5~"), vec!["K 201 0 1"]);
        assert_eq!(messages(b"\x1b[6~"), vec!["K 209 0 1"]);
        assert_eq!(messages(b"\x1bOA"), vec!["K 200 0 1"]);
    }

    #[test]
    fn incomplete_escape_is_pending() {
        let mut pending = b"\x1b".to_vec();
        let mut out = Vec::new();
        assert_eq!(process_input(&mut pending, &mut out), Outcome::Continue);
        assert_eq!(pending, b"\x1b");
        assert!(out.is_empty());

        pending.extend_from_slice(b"[");
        assert_eq!(process_input(&mut pending, &mut out), Outcome::Continue);
        assert_eq!(pending, b"\x1b[");
        assert!(out.is_empty());

        pending.push(b'A');
        assert_eq!(process_input(&mut pending, &mut out), Outcome::Continue);
        assert!(pending.is_empty());
        assert_eq!(out, vec!["K 200 0 1"]);
    }

    #[test]
    fn bare_escape_then_byte() {
        assert_eq!(messages(b"\x1bx"), vec!["K 1 0 1", "T 78"]);
    }

    #[test]
    fn unknown_csi_is_swallowed() {
        assert_eq!(messages(b"\x1b[1;5A"), Vec::<String>::new());
    }

    #[test]
    fn unicode_codepoints() {
        assert_eq!(messages("é".as_bytes()), vec!["K 233 233 1", "K 233 233 0"]);
        assert_eq!(
            messages("€".as_bytes()),
            vec!["K 8364 8364 1", "K 8364 8364 0"]
        );
        assert_eq!(
            messages(b"a\xc3\xa9b"),
            vec!["T 61", "K 233 233 1", "K 233 233 0", "T 62",]
        );
    }

    #[test]
    fn incomplete_utf8_is_pending() {
        let mut pending = vec![0xC3];
        let mut out = Vec::new();
        assert_eq!(process_input(&mut pending, &mut out), Outcome::Continue);
        assert_eq!(pending, vec![0xC3]);
        pending.push(0xA9);
        assert_eq!(process_input(&mut pending, &mut out), Outcome::Continue);
        assert!(pending.is_empty());
        assert_eq!(out, vec!["K 233 233 1", "K 233 233 0"]);
    }
}
