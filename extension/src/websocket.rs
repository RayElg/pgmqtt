//! WebSocket (RFC 6455) transport for MQTT-over-WebSocket (port 9001).
//!
//! Provides:
//!   - `handshake()` — performs the HTTP Upgrade negotiation on a raw TcpStream.
//!   - `WsStream`    — wraps a TcpStream and implements Read/Write in terms of
//!                     WebSocket data frames, so the rest of the server can treat
//!                     it identically to a plain TCP connection.
//!
//! Only binary/text data frames and the three control frames (Ping, Pong, Close)
//! are handled.  All frames from the client are masked (RFC 6455 §5.1);
//! server-to-client frames are unmasked.

use std::io::{self, Read, Write};

// ── Constants ─────────────────────────────────────────────────────────────────

const WS_MAGIC: &str = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";

/// Maximum WebSocket frame payload we will allocate.  A client claiming a
/// larger frame is almost certainly malicious; reject it before touching the
/// heap.  65 536 bytes is well above any realistic MQTT packet size.
const MAX_WS_FRAME_SIZE: usize = 65_536;

// Opcodes
const OP_CONT: u8 = 0x0;
const OP_TEXT: u8 = 0x1;
const OP_BIN: u8 = 0x2;
const OP_CLOSE: u8 = 0x8;
const OP_PING: u8 = 0x9;
const OP_PONG: u8 = 0xA;

// ── Handshake ─────────────────────────────────────────────────────────────────

/// Read the HTTP Upgrade request from `stream` and send a `101 Switching
/// Protocols` response.  Returns `Ok((leftover_bytes, jwt_token))` on success,
/// where `jwt_token` is extracted from `?jwt=<token>` in the request-line query
/// string or the `Authorization: Bearer <token>` header.
///
/// We intentionally use a hand-rolled parser instead of httparse to avoid
/// strict token-character validation that rejects valid MQTT-over-WebSocket
/// clients (e.g. `Connection: Upgrade, keep-alive`).
pub fn handshake<S: Read + Write>(stream: &mut S) -> io::Result<(Vec<u8>, Option<String>)> {
    // Accumulate bytes until we see the end-of-headers marker \r\n\r\n.
    let mut buf: Vec<u8> = Vec::with_capacity(2048);
    let mut tmp = [0u8; 1024];

    let header_end_pos = loop {
        let n = stream.read(&mut tmp)?;
        if n == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "connection closed during WebSocket handshake",
            ));
        }
        buf.extend_from_slice(&tmp[..n]);

        // Check for end-of-headers
        if let Some(pos) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
            break pos + 4;
        }

        // Bail if the request is unreasonably large
        if buf.len() > 16_384 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "WebSocket Upgrade request too large",
            ));
        }
    };

    // Any bytes after \r\n\r\n are part of the first WebSocket frame(s).
    let leftover = buf[header_end_pos..].to_vec();

    // Convert to string — allow lossy UTF-8 so non-ASCII header values don't abort
    let request = String::from_utf8_lossy(&buf[..header_end_pos]);

    // Extract Sec-WebSocket-Key by scanning header lines.
    // Also extract JWT token from query param or Authorization header.
    let mut ws_key: Option<String> = None;
    let mut jwt_token: Option<String> = None;
    let mut has_mqtt_protocol = false;
    let mut lines = request.lines();

    // Parse request line for ?jwt= query param: e.g. "GET /?jwt=<token> HTTP/1.1"
    if let Some(request_line) = lines.next() {
        if let Some(query_start) = request_line.find('?') {
            let query = &request_line[query_start + 1..];
            // Strip trailing " HTTP/1.1" if present
            let query = if let Some(space) = query.find(' ') { &query[..space] } else { query };
            for param in query.split('&') {
                if let Some(val) = param.strip_prefix("jwt=") {
                    if !val.is_empty() {
                        jwt_token = Some(val.to_string());
                    }
                    break;
                }
            }
        }
    }

    for line in lines {
        let lower = line.to_ascii_lowercase();
        if lower.starts_with("sec-websocket-key:") {
            if let Some(idx) = line.find(':') {
                ws_key = Some(line[idx + 1..].trim().to_string());
            }
        } else if lower.starts_with("sec-websocket-protocol:") {
            // Check if the client offered "mqtt" as a subprotocol
            if let Some(idx) = line.find(':') {
                let val = line[idx + 1..].trim();
                for proto in val.split(',') {
                    if proto.trim().eq_ignore_ascii_case("mqtt") {
                        has_mqtt_protocol = true;
                        break;
                    }
                }
            }
        } else if jwt_token.is_none() && lower.starts_with("authorization:") {
            if let Some(idx) = line.find(':') {
                let val = line[idx + 1..].trim();
                if let Some(bearer) = val.strip_prefix("Bearer ").or_else(|| val.strip_prefix("bearer ")) {
                    if !bearer.is_empty() {
                        jwt_token = Some(bearer.to_string());
                    }
                }
            }
        }
    }

    let key = ws_key.ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "WebSocket Upgrade missing Sec-WebSocket-Key header",
        )
    })?;

    // Compute Sec-WebSocket-Accept = base64(SHA-1(key + magic))
    let accept = compute_accept(&key);

    // RFC 6455 §4.2.2: only include Sec-WebSocket-Protocol if the client offered it.
    let protocol_line = if has_mqtt_protocol {
        "Sec-WebSocket-Protocol: mqtt\r\n"
    } else {
        ""
    };

    // Send 101 response
    let response = format!(
        "HTTP/1.1 101 Switching Protocols\r\n\
         Upgrade: websocket\r\n\
         Connection: Upgrade\r\n\
         Sec-WebSocket-Accept: {}\r\n\
         {}\
         \r\n",
        accept, protocol_line
    );
    stream.write_all(response.as_bytes())?;
    Ok((leftover, jwt_token))
}

fn compute_accept(key: &str) -> String {
    use sha1::{Digest, Sha1};
    let mut hasher = Sha1::new();
    hasher.update(key.trim().as_bytes());
    hasher.update(WS_MAGIC.as_bytes());
    let hash = hasher.finalize();
    base64::Engine::encode(&base64::engine::general_purpose::STANDARD, hash)
}

// ── WsStream ──────────────────────────────────────────────────────────────────

/// A thin WebSocket framing wrapper around a `TcpStream`.
///
/// `Read` returns the payload bytes of incoming data frames (Text or Binary),
/// stripped of their frame header and unmasked.
///
/// `Write` wraps the supplied bytes as a single unmasked Binary data frame.
pub struct WsStream<S: Read + Write> {
    inner: S,
    /// Raw wire bytes from the handshake that have not yet been decoded as
    /// WebSocket frames.  `read_exact_inner` drains this before reading from
    /// the underlying stream.
    wire_buf: Vec<u8>,
    /// Decoded payload bytes left over from a previous `read` call (i.e. frame
    /// payload that did not fit in the caller's buffer).
    read_buf: Vec<u8>,
    /// Reassembly buffer for fragmented messages (RFC 6455 §5.4).
    frag_buf: Vec<u8>,
    /// True once a Close frame has been sent or received.
    closed: bool,
}

impl<S: Read + Write> WsStream<S> {
    pub fn new(stream: S, initial_data: Vec<u8>) -> Self {
        Self {
            inner: stream,
            wire_buf: initial_data,
            read_buf: Vec::new(),
            frag_buf: Vec::new(),
            closed: false,
        }
    }

    /// Borrow the underlying stream (e.g. to set timeouts / non-blocking mode).
    pub fn get_ref(&self) -> &S {
        &self.inner
    }

    // ── Frame reader ──────────────────────────────────────────────────────────

    /// Read exactly `n` bytes into `dst`, first draining `wire_buf` then
    /// falling through to the underlying stream.
    fn read_exact_inner(&mut self, dst: &mut [u8]) -> io::Result<()> {
        let mut pos = 0;

        // Drain wire_buf first
        if !self.wire_buf.is_empty() {
            let n = dst.len().min(self.wire_buf.len());
            dst[..n].copy_from_slice(&self.wire_buf[..n]);
            self.wire_buf.drain(..n);
            pos = n;
        }

        while pos < dst.len() {
            match self.inner.read(&mut dst[pos..]) {
                Ok(0) => return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "ws eof")),
                Ok(n) => pos += n,
                Err(e) if e.kind() == io::ErrorKind::WouldBlock && pos == 0 => {
                    return Err(e);
                }
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                    // partial read — keep going (blocking for the rest is fine
                    // because we know there should be more bytes coming)
                    std::thread::yield_now();
                }
                Err(e) => return Err(e),
            }
        }
        Ok(())
    }

    /// Read one complete WebSocket frame and return `(opcode, payload)`.
    /// Control frames (Ping/Pong/Close) are handled inline; data frames are
    /// returned to the caller.  Fragmented messages are reassembled per
    /// RFC 6455 §5.4.
    fn read_frame(&mut self) -> io::Result<(u8, Vec<u8>)> {
        loop {
            // ── Fixed 2-byte header ───────────────────────────────────────────
            let mut hdr = [0u8; 2];
            self.read_exact_inner(&mut hdr)?;

            let fin = (hdr[0] & 0x80) != 0;
            let rsv = hdr[0] & 0x70;
            let opcode = hdr[0] & 0x0F;
            let masked = (hdr[1] & 0x80) != 0;
            let len7 = (hdr[1] & 0x7F) as usize;

            // RFC 6455 §5.2: RSV bits MUST be 0 unless an extension is negotiated.
            if rsv != 0 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("ws non-zero RSV bits: 0x{:02x}", rsv),
                ));
            }

            // RFC 6455 §5.1: client frames MUST be masked.
            if !masked {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "ws client frame not masked",
                ));
            }

            // ── Extended payload length ───────────────────────────────────────
            let payload_len: usize = match len7 {
                126 => {
                    let mut ext = [0u8; 2];
                    self.read_exact_inner(&mut ext)?;
                    u16::from_be_bytes(ext) as usize
                }
                127 => {
                    let mut ext = [0u8; 8];
                    self.read_exact_inner(&mut ext)?;
                    u64::from_be_bytes(ext) as usize
                }
                n => n,
            };

            // ── Masking key ───────────────────────────────────────────────────
            let mut mask = [0u8; 4];
            self.read_exact_inner(&mut mask)?;

            // ── Payload ───────────────────────────────────────────────────────
            if payload_len > MAX_WS_FRAME_SIZE {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "ws frame too large: {} bytes (max {})",
                        payload_len, MAX_WS_FRAME_SIZE
                    ),
                ));
            }
            let mut payload = vec![0u8; payload_len];
            if payload_len > 0 {
                self.read_exact_inner(&mut payload)?;
            }
            for (i, byte) in payload.iter_mut().enumerate() {
                *byte ^= mask[i % 4];
            }

            // ── Dispatch by opcode ────────────────────────────────────────────
            match opcode {
                OP_TEXT | OP_BIN => {
                    if fin {
                        // Unfragmented message — return immediately.
                        // If there was an abandoned fragment in progress, discard it
                        // (the spec says interleaving is not allowed).
                        self.frag_buf.clear();
                        return Ok((opcode, payload));
                    }
                    // Start of a fragmented message.
                    self.frag_buf.clear();
                    self.frag_buf.extend_from_slice(&payload);
                }
                OP_CONT => {
                    // Continuation frame — append to the reassembly buffer.
                    self.frag_buf.extend_from_slice(&payload);
                    if fin {
                        let assembled = std::mem::take(&mut self.frag_buf);
                        return Ok((OP_BIN, assembled));
                    }
                }
                OP_PING => {
                    // Respond with Pong carrying the same payload
                    self.write_frame(OP_PONG, &payload)?;
                }
                OP_PONG => {
                    // Unsolicited pong — ignore
                }
                OP_CLOSE => {
                    // Echo close frame and signal EOF
                    if !self.closed {
                        self.closed = true;
                        let _ = self.write_frame(OP_CLOSE, &payload);
                    }
                    return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "ws close"));
                }
                _ => {
                    // Unknown opcode — treat as EOF
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("unknown ws opcode 0x{:02x}", opcode),
                    ));
                }
            }
        }
    }

    // ── Frame writer ──────────────────────────────────────────────────────────

    /// Write a single unmasked WebSocket frame with the given opcode.
    fn write_frame(&mut self, opcode: u8, payload: &[u8]) -> io::Result<()> {
        let mut frame = Vec::with_capacity(10 + payload.len());

        // FIN + opcode
        frame.push(0x80 | opcode);

        // Payload length (server→client frames are always unmasked)
        let len = payload.len();
        if len < 126 {
            frame.push(len as u8);
        } else if len < 65536 {
            frame.push(126);
            frame.extend_from_slice(&(len as u16).to_be_bytes());
        } else {
            frame.push(127);
            frame.extend_from_slice(&(len as u64).to_be_bytes());
        }

        frame.extend_from_slice(payload);
        self.inner.write_all(&frame)
    }
}

// ── std::io::Read / Write impls ───────────────────────────────────────────────

impl<S: Read + Write> Read for WsStream<S> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        // If we have leftover decoded payload from a previous frame, drain it first.
        if !self.read_buf.is_empty() {
            let n = buf.len().min(self.read_buf.len());
            buf[..n].copy_from_slice(&self.read_buf[..n]);
            self.read_buf.drain(..n);
            return Ok(n);
        }

        if self.closed {
            return Ok(0);
        }

        // Read the next frame
        let (_opcode, payload) = self.read_frame()?;

        if payload.is_empty() {
            return Ok(0);
        }

        let n = buf.len().min(payload.len());
        buf[..n].copy_from_slice(&payload[..n]);
        if payload.len() > n {
            self.read_buf.extend_from_slice(&payload[n..]);
        }
        Ok(n)
    }
}

impl<S: Read + Write> Write for WsStream<S> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.write_frame(OP_BIN, buf)?;
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}
