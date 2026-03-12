//! Transport abstraction layer.
//!
//! Wraps either a raw TCP stream or a WebSocket-framed stream so that
//! all MQTT packet I/O can stay identical regardless of transport.

use crate::websocket;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::Duration;

/// Unified transport for MQTT — TCP or WebSocket.
pub enum Transport {
    Raw(TcpStream),
    Ws(websocket::WsStream),
}

impl Transport {
    pub fn set_read_timeout(&self, dur: Option<Duration>) -> std::io::Result<()> {
        match self {
            Transport::Raw(s) => s.set_read_timeout(dur),
            Transport::Ws(ws) => ws.get_ref().set_read_timeout(dur),
        }
    }

    pub fn set_write_timeout(&self, dur: Option<Duration>) -> std::io::Result<()> {
        match self {
            Transport::Raw(s) => s.set_write_timeout(dur),
            Transport::Ws(ws) => ws.get_ref().set_write_timeout(dur),
        }
    }

    pub fn set_nonblocking(&self, nb: bool) -> std::io::Result<()> {
        match self {
            Transport::Raw(s) => s.set_nonblocking(nb),
            Transport::Ws(ws) => ws.get_ref().set_nonblocking(nb),
        }
    }
}

impl Read for Transport {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        match self {
            Transport::Raw(s) => s.read(buf),
            Transport::Ws(ws) => ws.read(buf),
        }
    }
}

impl Write for Transport {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        match self {
            Transport::Raw(s) => s.write(buf),
            Transport::Ws(ws) => ws.write(buf),
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        match self {
            Transport::Raw(s) => s.flush(),
            Transport::Ws(ws) => ws.flush(),
        }
    }
}
