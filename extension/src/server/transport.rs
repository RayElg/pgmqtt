//! Transport abstraction layer.
//!
//! Wraps either a raw TCP stream or a WebSocket-framed stream so that
//! all MQTT packet I/O can stay identical regardless of transport.

use crate::websocket;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::Arc;
use std::time::Duration;

/// Unified transport for MQTT — TCP, WebSocket, TLS, or WSS.
pub enum Transport {
    Raw(TcpStream),
    Ws(websocket::WsStream<TcpStream>),
    Tls(rustls::StreamOwned<rustls::ServerConnection, TcpStream>),
    Wss(websocket::WsStream<rustls::StreamOwned<rustls::ServerConnection, TcpStream>>),
}

impl Transport {
    pub fn new_tls(
        stream: TcpStream,
        config: Arc<rustls::ServerConfig>,
    ) -> Result<Self, rustls::Error> {
        let conn = rustls::ServerConnection::new(config)?;
        Ok(Transport::Tls(rustls::StreamOwned::new(conn, stream)))
    }

    pub fn set_read_timeout(&self, dur: Option<Duration>) -> std::io::Result<()> {
        match self {
            Transport::Raw(s) => s.set_read_timeout(dur),
            Transport::Ws(ws) => ws.get_ref().set_read_timeout(dur),
            Transport::Tls(tls) => tls.get_ref().set_read_timeout(dur),
            Transport::Wss(wss) => wss.get_ref().get_ref().set_read_timeout(dur),
        }
    }

    pub fn set_write_timeout(&self, dur: Option<Duration>) -> std::io::Result<()> {
        match self {
            Transport::Raw(s) => s.set_write_timeout(dur),
            Transport::Ws(ws) => ws.get_ref().set_write_timeout(dur),
            Transport::Tls(tls) => tls.get_ref().set_write_timeout(dur),
            Transport::Wss(wss) => wss.get_ref().get_ref().set_write_timeout(dur),
        }
    }

    pub fn set_nonblocking(&self, nb: bool) -> std::io::Result<()> {
        match self {
            Transport::Raw(s) => s.set_nonblocking(nb),
            Transport::Ws(ws) => ws.get_ref().set_nonblocking(nb),
            Transport::Tls(tls) => tls.get_ref().set_nonblocking(nb),
            Transport::Wss(wss) => wss.get_ref().get_ref().set_nonblocking(nb),
        }
    }
}

impl Read for Transport {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        match self {
            Transport::Raw(s) => s.read(buf),
            Transport::Ws(ws) => ws.read(buf),
            Transport::Tls(tls) => tls.read(buf),
            Transport::Wss(wss) => wss.read(buf),
        }
    }
}

impl Write for Transport {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        match self {
            Transport::Raw(s) => s.write(buf),
            Transport::Ws(ws) => ws.write(buf),
            Transport::Tls(tls) => tls.write(buf),
            Transport::Wss(wss) => wss.write(buf),
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        match self {
            Transport::Raw(s) => s.flush(),
            Transport::Ws(ws) => ws.flush(),
            Transport::Tls(tls) => tls.flush(),
            Transport::Wss(wss) => wss.flush(),
        }
    }
}
