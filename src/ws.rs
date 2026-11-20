//! Minimal RFC 6455 WebSocket client used by the terminal client and tests.
//!
//! The relay server uses axum's WebSocket support; this module keeps the exact
//! wire behaviour of the original hand-written implementation: ws:// only, no
//! extensions, masked client frames, 16 MiB payload limit and fragment
//! reassembly.

use std::io;
use std::time::Duration;

use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine as _;
use rand::RngCore as _;
use sha1::{Digest, Sha1};
use tokio::io::{split, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadHalf, WriteHalf};
use tokio::net::TcpStream;

pub const GUID: &str = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";

pub const OP_CONT: u8 = 0x0;
pub const OP_TEXT: u8 = 0x1;
pub const OP_BINARY: u8 = 0x2;
pub const OP_CLOSE: u8 = 0x8;
pub const OP_PING: u8 = 0x9;
pub const OP_PONG: u8 = 0xA;

/// Largest data-frame payload accepted, matching the Python implementation.
pub const MAX_PAYLOAD: usize = 16 * 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Frame {
    pub opcode: u8,
    pub payload: Vec<u8>,
}

fn protocol_error(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.into())
}

/// Compute the `Sec-WebSocket-Accept` value for a client key.
pub fn accept_key(key: &str) -> String {
    let mut hasher = Sha1::new();
    hasher.update(key.as_bytes());
    hasher.update(GUID.as_bytes());
    BASE64.encode(hasher.finalize())
}

/// Encode one complete frame. `mask` is a 4-byte key when the payload should be
/// masked (client side), or `None` for server side frames.
pub fn encode_frame(opcode: u8, payload: &[u8], mask: Option<[u8; 4]>) -> Vec<u8> {
    let length = payload.len();
    let mut header = Vec::with_capacity(14 + length);
    header.push(0x80 | (opcode & 0x0F));

    let mask_bit = if mask.is_some() { 0x80 } else { 0x00 };
    if length < 126 {
        header.push(mask_bit | length as u8);
    } else if length < 65536 {
        header.push(mask_bit | 126);
        header.extend_from_slice(&(length as u16).to_be_bytes());
    } else {
        header.push(mask_bit | 127);
        header.extend_from_slice(&(length as u64).to_be_bytes());
    }

    if let Some(key) = mask {
        header.extend_from_slice(&key);
        let mut body = Vec::with_capacity(length);
        body.extend(
            payload
                .iter()
                .enumerate()
                .map(|(index, byte)| byte ^ key[index % 4]),
        );
        header.extend_from_slice(&body);
    } else {
        header.extend_from_slice(payload);
    }
    header
}

/// Read one complete data message, reassembling fragments. Control frames are
/// returned one at a time. `buffer` carries bytes read past the previous frame.
pub async fn recv_frame<R: AsyncRead + Unpin>(
    io: &mut R,
    buffer: &mut Vec<u8>,
) -> io::Result<Frame> {
    let mut fragments: Vec<u8> = Vec::new();
    let mut message_opcode: Option<u8> = None;

    loop {
        let (fin, opcode, payload) = read_one(io, buffer).await?;

        if opcode >= 0x8 {
            if !fin || payload.len() > 125 {
                return Err(protocol_error("invalid control frame"));
            }
            return Ok(Frame { opcode, payload });
        }

        match opcode {
            OP_CONT => {
                let message_opcode = message_opcode
                    .ok_or_else(|| protocol_error("unexpected continuation frame"))?;
                fragments.extend_from_slice(&payload);
                if fin {
                    return Ok(Frame {
                        opcode: message_opcode,
                        payload: fragments,
                    });
                }
            }
            OP_TEXT | OP_BINARY => {
                if message_opcode.is_some() {
                    return Err(protocol_error("new message before continuation finished"));
                }
                message_opcode = Some(opcode);
                fragments = payload;
                if fin {
                    return Ok(Frame {
                        opcode,
                        payload: fragments,
                    });
                }
            }
            other => {
                return Err(protocol_error(format!("unsupported opcode {other}")));
            }
        }
    }
}

async fn fill<R: AsyncRead + Unpin>(
    io: &mut R,
    buffer: &mut Vec<u8>,
    need: usize,
) -> io::Result<()> {
    let mut chunk = [0u8; 4096];
    while buffer.len() < need {
        let count = io.read(&mut chunk).await?;
        if count == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "websocket closed",
            ));
        }
        buffer.extend_from_slice(&chunk[..count]);
    }
    Ok(())
}

async fn read_one<R: AsyncRead + Unpin>(
    io: &mut R,
    buffer: &mut Vec<u8>,
) -> io::Result<(bool, u8, Vec<u8>)> {
    fill(io, buffer, 2).await?;
    let first = buffer[0];
    let second = buffer[1];
    let fin = first & 0x80 != 0;
    let opcode = first & 0x0F;
    let masked = second & 0x80 != 0;
    let mut length = (second & 0x7F) as u64;
    let mut offset = 2usize;

    if length == 126 {
        fill(io, buffer, 4).await?;
        length = u16::from_be_bytes([buffer[2], buffer[3]]) as u64;
        offset = 4;
    } else if length == 127 {
        fill(io, buffer, 10).await?;
        let mut bytes = [0u8; 8];
        bytes.copy_from_slice(&buffer[2..10]);
        length = u64::from_be_bytes(bytes);
        offset = 10;
    }

    if length > MAX_PAYLOAD as u64 {
        return Err(protocol_error(format!("frame too large: {length}")));
    }
    let length = length as usize;

    let mask = if masked {
        fill(io, buffer, offset + 4).await?;
        let key = [
            buffer[offset],
            buffer[offset + 1],
            buffer[offset + 2],
            buffer[offset + 3],
        ];
        offset += 4;
        Some(key)
    } else {
        None
    };

    fill(io, buffer, offset + length).await?;
    let mut payload = buffer[offset..offset + length].to_vec();
    buffer.drain(..offset + length);
    if let Some(key) = mask {
        for (index, byte) in payload.iter_mut().enumerate() {
            *byte ^= key[index % 4];
        }
    }
    Ok((fin, opcode, payload))
}

/// Write one masked client frame.
pub async fn send_frame<W: AsyncWrite + Unpin>(
    io: &mut W,
    opcode: u8,
    payload: &[u8],
) -> io::Result<()> {
    let mut key = [0u8; 4];
    rand::thread_rng().fill_bytes(&mut key);
    let data = encode_frame(opcode, payload, Some(key));
    io.write_all(&data).await?;
    io.flush().await
}

/// Write one unmasked server frame (used by tests and simple fixtures).
pub async fn send_frame_unmasked<W: AsyncWrite + Unpin>(
    io: &mut W,
    opcode: u8,
    payload: &[u8],
) -> io::Result<()> {
    let data = encode_frame(opcode, payload, None);
    io.write_all(&data).await?;
    io.flush().await
}

/// A connected client-side WebSocket stream.
pub struct WsStream<S> {
    io: S,
    read_buf: Vec<u8>,
}

impl WsStream<TcpStream> {
    /// Connect to a `ws://` endpoint and perform the upgrade handshake.
    pub async fn connect(host: &str, port: u16, path: &str, timeout: Duration) -> io::Result<Self> {
        let io = tokio::time::timeout(timeout, TcpStream::connect((host, port)))
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "connect timed out"))??;
        io.set_nodelay(true).ok();
        Self::client_handshake(io, host, port, path).await
    }
}

impl<S: AsyncRead + AsyncWrite + Unpin> WsStream<S> {
    /// Send `GET path` with an upgrade request, validate the 101 response and
    /// keep any bytes that arrived after the headers.
    pub async fn client_handshake(
        mut io: S,
        host: &str,
        port: u16,
        path: &str,
    ) -> io::Result<Self> {
        let mut key_bytes = [0u8; 16];
        rand::thread_rng().fill_bytes(&mut key_bytes);
        let key = BASE64.encode(key_bytes);
        let request = format!(
            "GET {path} HTTP/1.1\r\n\
             Host: {host}:{port}\r\n\
             Upgrade: websocket\r\n\
             Connection: Upgrade\r\n\
             Sec-WebSocket-Key: {key}\r\n\
             Sec-WebSocket-Version: 13\r\n\
             \r\n"
        );
        io.write_all(request.as_bytes()).await?;
        io.flush().await?;

        let mut read_buf = Vec::new();
        let head_end = loop {
            if let Some(position) = find_bytes(&read_buf, b"\r\n\r\n") {
                break position;
            }
            let mut chunk = [0u8; 4096];
            let count = io.read(&mut chunk).await?;
            if count == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "server closed during handshake",
                ));
            }
            read_buf.extend_from_slice(&chunk[..count]);
            if read_buf.len() > 64 * 1024 {
                return Err(protocol_error("handshake headers too large"));
            }
        };

        let head = String::from_utf8_lossy(&read_buf[..head_end]).to_string();
        read_buf.drain(..head_end + 4);

        let mut lines = head.split("\r\n");
        let status = lines.next().unwrap_or_default();
        if status.split_whitespace().nth(1) != Some("101") {
            return Err(protocol_error(format!("handshake failed: {status}")));
        }

        let expected = accept_key(&key);
        for line in lines {
            let Some((name, value)) = line.split_once(':') else {
                continue;
            };
            if name.trim().eq_ignore_ascii_case("sec-websocket-accept") && value.trim() != expected
            {
                return Err(protocol_error("bad Sec-WebSocket-Accept"));
            }
        }

        Ok(Self { io, read_buf })
    }

    pub async fn recv_frame(&mut self) -> io::Result<Frame> {
        recv_frame(&mut self.io, &mut self.read_buf).await
    }

    pub async fn send_frame(&mut self, opcode: u8, payload: &[u8]) -> io::Result<()> {
        send_frame(&mut self.io, opcode, payload).await
    }

    pub async fn send_text(&mut self, text: &str) -> io::Result<()> {
        self.send_frame(OP_TEXT, text.as_bytes()).await
    }

    pub fn into_split(self) -> (WsReader<ReadHalf<S>>, WsWriter<WriteHalf<S>>) {
        let (reader, writer) = split(self.io);
        (
            WsReader {
                io: reader,
                buffer: self.read_buf,
            },
            WsWriter { io: writer },
        )
    }
}

pub struct WsReader<R> {
    io: R,
    buffer: Vec<u8>,
}

impl<R: AsyncRead + Unpin> WsReader<R> {
    pub async fn recv_frame(&mut self) -> io::Result<Frame> {
        recv_frame(&mut self.io, &mut self.buffer).await
    }
}

pub struct WsWriter<W> {
    io: W,
}

impl<W: AsyncWrite + Unpin> WsWriter<W> {
    pub async fn send_frame(&mut self, opcode: u8, payload: &[u8]) -> io::Result<()> {
        send_frame(&mut self.io, opcode, payload).await
    }

    pub async fn send_text(&mut self, text: &str) -> io::Result<()> {
        self.send_frame(OP_TEXT, text.as_bytes()).await
    }
}

fn find_bytes(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    if needle.is_empty() || haystack.len() < needle.len() {
        return None;
    }
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

/// Parse `ws://host[:port][/path]`, rejecting `wss://` (the internet card has
/// no TLS stack).
pub fn parse_ws_url(url: &str) -> io::Result<(String, u16, String)> {
    let Some(rest) = url.strip_prefix("ws://") else {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "only ws:// URLs are supported (no TLS in OpenComputers)",
        ));
    };

    let (authority, path) = match rest.split_once('/') {
        Some((authority, tail)) => (authority, format!("/{tail}")),
        None => (rest, "/".to_string()),
    };

    if authority.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "missing host in websocket URL",
        ));
    }

    if let Some(rest) = authority.strip_prefix('[') {
        let Some(end) = rest.find(']') else {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "bad IPv6 host"));
        };
        let host = rest[..end].to_string();
        let remainder = &rest[end + 1..];
        let port = if let Some(port_text) = remainder.strip_prefix(':') {
            port_text.parse().map_err(|_| {
                io::Error::new(io::ErrorKind::InvalidInput, "bad port in websocket URL")
            })?
        } else if remainder.is_empty() {
            80
        } else {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "bad IPv6 host"));
        };
        return Ok((host, port, path));
    }

    if let Some((host, port_text)) = authority.rsplit_once(':') {
        let port = port_text.parse().map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidInput, "bad port in websocket URL")
        })?;
        return Ok((host.to_string(), port, path));
    }

    Ok((authority.to_string(), 80, path))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::duplex;

    async fn decode(data: &[u8]) -> io::Result<Frame> {
        let (mut writer, mut reader) = duplex(1024 * 1024);
        writer.write_all(data).await.unwrap();
        writer.shutdown().await.unwrap();
        recv_frame(&mut reader, &mut Vec::new()).await
    }

    #[tokio::test]
    async fn accept_key_vector() {
        assert_eq!(
            accept_key("dGhlIHNhbXBsZSBub25jZQ=="),
            "s3pPLMBiTxaQ9kYGzzhZRbK+xOo="
        );
    }

    #[tokio::test]
    async fn roundtrip_text_masked() {
        let data = encode_frame(OP_TEXT, b"hello", Some([1, 2, 3, 4]));
        let frame = decode(&data).await.unwrap();
        assert_eq!(frame.opcode, OP_TEXT);
        assert_eq!(frame.payload, b"hello");
    }

    #[tokio::test]
    async fn roundtrip_binary_and_lengths() {
        for size in [0usize, 1, 125, 126, 65535, 65536] {
            let payload = vec![(size % 251) as u8; size];
            let data = encode_frame(OP_BINARY, &payload, None);
            let frame = decode(&data).await.unwrap();
            assert_eq!(frame.opcode, OP_BINARY);
            assert_eq!(frame.payload, payload);
        }
    }

    #[tokio::test]
    async fn fragmentation() {
        let mut first = encode_frame(OP_TEXT, b"hel", None);
        first[0] &= 0x7F;
        let second = encode_frame(OP_CONT, b"lo", None);
        let frame = decode(&[first, second].concat()).await.unwrap();
        assert_eq!(frame.opcode, OP_TEXT);
        assert_eq!(frame.payload, b"hello");
    }

    #[tokio::test]
    async fn bad_continuation() {
        let data = encode_frame(OP_CONT, b"x", None);
        let error = decode(&data).await.unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    }

    #[tokio::test]
    async fn rejects_oversized_frames() {
        let mut header = vec![0x82, 0x7F];
        header.extend_from_slice(&((MAX_PAYLOAD as u64) + 1).to_be_bytes());
        let error = decode(&header).await.unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    }

    #[test]
    fn parse_url() {
        assert_eq!(
            parse_ws_url("ws://127.0.0.1:5235/agent").unwrap(),
            ("127.0.0.1".to_string(), 5235, "/agent".to_string())
        );
        assert_eq!(
            parse_ws_url("ws://example.test/agent").unwrap(),
            ("example.test".to_string(), 80, "/agent".to_string())
        );
        assert_eq!(
            parse_ws_url("ws://[::1]:5235/client").unwrap(),
            ("::1".to_string(), 5235, "/client".to_string())
        );
        assert_eq!(
            parse_ws_url("ws://example.test").unwrap(),
            ("example.test".to_string(), 80, "/".to_string())
        );
        assert!(parse_ws_url("wss://example.test/agent").is_err());
    }

    #[tokio::test]
    async fn client_handshake_validates_and_keeps_leftovers() {
        let (client_io, mut server_io) = duplex(4096);
        let server = tokio::spawn(async move {
            let mut request = Vec::new();
            let mut chunk = [0u8; 1024];
            while !find_bytes(&request, b"\r\n\r\n").is_some() {
                let count = server_io.read(&mut chunk).await.unwrap();
                assert!(count > 0);
                request.extend_from_slice(&chunk[..count]);
            }
            let text = String::from_utf8_lossy(&request);
            let key = text
                .lines()
                .find_map(|line| {
                    line.strip_prefix("Sec-WebSocket-Key:")
                        .map(|value| value.trim().to_string())
                })
                .unwrap();
            let response = format!(
                "HTTP/1.1 101 Switching Protocols\r\n\
                 Upgrade: websocket\r\n\
                 Connection: Upgrade\r\n\
                 Sec-WebSocket-Accept: {}\r\n\
                 \r\n",
                accept_key(&key)
            );
            server_io.write_all(response.as_bytes()).await.unwrap();
            server_io
                .write_all(&encode_frame(OP_TEXT, b"early", None))
                .await
                .unwrap();
            server_io.flush().await.unwrap();
        });

        let mut stream = WsStream::client_handshake(client_io, "127.0.0.1", 5235, "/client")
            .await
            .unwrap();
        let frame = stream.recv_frame().await.unwrap();
        assert_eq!(frame.payload, b"early");
        server.await.unwrap();
    }
}
