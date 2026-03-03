use crate::mqtt;
use crate::subscriptions;
use crate::topic_buffer;

use crate::websocket;
use pgrx::bgworkers::BackgroundWorker;
use pgrx::log;

use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;
use std::time::Duration;

static NEXT_AUTO_CLIENT_ID: AtomicUsize = AtomicUsize::new(0);

/// How often the BGW latch wakes to poll for connections.
const LATCH_INTERVAL: Duration = Duration::from_millis(250);

/// Timeout for HTTP client read/write operations.
const CLIENT_TIMEOUT: Duration = Duration::from_secs(2);

/// Maximum bytes to read from a client request.
const MAX_REQUEST_BYTES: usize = 65536;

/// Maximum number of unacked QoS 1 messages per client.
const MAX_INFLIGHT_MESSAGES: usize = 500;

// ── Transport abstraction ─────────────────────────────────────────────────────

/// Wraps either a raw TCP stream or a WebSocket-framed stream so that
/// all MQTT packet I/O can stay identical regardless of transport.
enum Transport {
    Raw(TcpStream),
    Ws(websocket::WsStream),
}

impl Transport {
    fn set_read_timeout(&self, dur: Option<Duration>) -> std::io::Result<()> {
        match self {
            Transport::Raw(s) => s.set_read_timeout(dur),
            Transport::Ws(ws) => ws.get_ref().set_read_timeout(dur),
        }
    }
    fn set_write_timeout(&self, dur: Option<Duration>) -> std::io::Result<()> {
        match self {
            Transport::Raw(s) => s.set_write_timeout(dur),
            Transport::Ws(ws) => ws.get_ref().set_write_timeout(dur),
        }
    }
    fn set_nonblocking(&self, nb: bool) -> std::io::Result<()> {
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

// ── Session tracking ──────────────────────────────────────────────────────────

#[derive(Clone)]
struct MqttSession {
    next_packet_id: u16,
    /// Outgoing packet_id → (topic, payload, msg_id, sent_at)
    inflight: HashMap<u16, (String, Vec<u8>, Option<i64>, std::time::Instant)>,
}

impl MqttSession {
    fn new() -> Self {
        Self {
            next_packet_id: 1,
            inflight: HashMap::new(),
        }
    }
}

static SESSIONS: Mutex<Option<HashMap<String, MqttSession>>> = Mutex::new(None);

fn with_sessions<F, R>(f: F) -> R
where
    F: FnOnce(&mut HashMap<String, MqttSession>) -> R,
{
    let mut lock = SESSIONS.lock().expect("sessions: poisoned mutex");
    if lock.is_none() {
        *lock = Some(HashMap::new());
    }
    f(lock.as_mut().unwrap())
}

struct MqttClient {
    transport: Transport,
    client_id: String,
    buf: Vec<u8>,
    will: Option<mqtt::Will>,
    keep_alive: u16,
    last_received_at: std::time::Instant,
}

impl MqttClient {
    fn new(
        transport: Transport,
        client_id: String,
        will: Option<mqtt::Will>,
        keep_alive: u16,
    ) -> Self {
        Self {
            transport,
            client_id,
            buf: Vec::with_capacity(MAX_REQUEST_BYTES),
            will,
            keep_alive,
            last_received_at: std::time::Instant::now(),
        }
    }
}

// ── HTTP server ──────────────────────────────────────────────────────────────

/// Run the HTTP-only healthcheck server (port 8080).
pub fn run_http(port: u16) {
    let addr = format!("0.0.0.0:{}", port);
    let listener = match TcpListener::bind(&addr) {
        Ok(l) => l,
        Err(e) => {
            log!("pgmqtt http: failed to bind {}: {}", addr, e);
            return;
        }
    };

    if let Err(e) = listener.set_nonblocking(true) {
        log!("pgmqtt http: failed to set non-blocking: {}", e);
        return;
    }

    log!("pgmqtt http: listening on {}", addr);

    while BackgroundWorker::wait_latch(Some(LATCH_INTERVAL)) {
        if BackgroundWorker::sighup_received() {
            log!("pgmqtt http: SIGHUP received");
        }
        drain_http_connections(&listener);
    }

    log!("pgmqtt http: shutting down");
}

fn drain_http_connections(listener: &TcpListener) {
    loop {
        match listener.accept() {
            Ok((stream, _)) => handle_http_connection(stream),
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
            Err(e) => {
                log!("pgmqtt http: accept error: {}", e);
                break;
            }
        }
    }
}

fn handle_http_connection(mut stream: TcpStream) {
    let _ = stream.set_read_timeout(Some(CLIENT_TIMEOUT));
    let _ = stream.set_write_timeout(Some(CLIENT_TIMEOUT));

    let mut buf = [0u8; 4096];
    let n = match stream.read(&mut buf) {
        Ok(0) | Err(_) => return,
        Ok(n) => n,
    };

    let request = match std::str::from_utf8(&buf[..n]) {
        Ok(s) => s,
        Err(_) => {
            let _ = stream.write_all(HTTP_400);
            return;
        }
    };

    let first_line = match request.lines().next() {
        Some(l) => l,
        None => {
            let _ = stream.write_all(HTTP_400);
            return;
        }
    };

    let mut parts = first_line.split_whitespace();
    let method = parts.next().unwrap_or("");
    let path = parts.next().unwrap_or("");

    if method != "GET" {
        let _ = stream.write_all(HTTP_405);
        return;
    }

    let response = match path {
        "/health" => HTTP_200,
        _ => HTTP_404,
    };
    let _ = stream.write_all(response);
}

const HTTP_200: &[u8] = b"HTTP/1.1 200 OK\r\n\
Content-Type: application/json\r\n\
Content-Length: 15\r\n\
Connection: close\r\n\
\r\n\
{\"status\":\"ok\"}";

const HTTP_404: &[u8] = b"HTTP/1.1 404 Not Found\r\n\
Content-Type: text/plain\r\n\
Content-Length: 9\r\n\
Connection: close\r\n\
\r\n\
not found";

const HTTP_400: &[u8] = b"HTTP/1.1 400 Bad Request\r\n\
Content-Type: text/plain\r\n\
Content-Length: 11\r\n\
Connection: close\r\n\
\r\n\
bad request";

const HTTP_405: &[u8] = b"HTTP/1.1 405 Method Not Allowed\r\n\
Content-Type: text/plain\r\n\
Content-Length: 18\r\n\
Connection: close\r\n\
\r\n\
method not allowed";

// ── MQTT server ──────────────────────────────────────────────────────────────

/// Run the MQTT broker server + CDC consumer.
///
/// Binds two listeners:
///   - `mqtt_port`  (1883) — raw TCP MQTT
///   - `ws_port`    (9001) — MQTT over WebSocket (RFC 6455)
///
/// Both share the same `clients` map and topic_buffer, so CDC events are
/// delivered to all connected clients regardless of transport.
pub fn run_mqtt_cdc(mqtt_port: u16, ws_port: u16, slot_name: &str) {
    let mqtt_addr = format!("0.0.0.0:{}", mqtt_port);
    let ws_addr = format!("0.0.0.0:{}", ws_port);

    let mqtt_listener = match TcpListener::bind(&mqtt_addr) {
        Ok(l) => l,
        Err(e) => {
            log!("pgmqtt mqtt: failed to bind {}: {}", mqtt_addr, e);
            return;
        }
    };
    let ws_listener = match TcpListener::bind(&ws_addr) {
        Ok(l) => l,
        Err(e) => {
            log!("pgmqtt ws: failed to bind {}: {}", ws_addr, e);
            return;
        }
    };

    if let Err(e) = mqtt_listener.set_nonblocking(true) {
        log!("pgmqtt mqtt: failed to set non-blocking: {}", e);
        return;
    }
    if let Err(e) = ws_listener.set_nonblocking(true) {
        log!("pgmqtt ws: failed to set non-blocking: {}", e);
        return;
    }

    log!("pgmqtt mqtt+cdc: listening on {} (raw TCP)", mqtt_addr);
    log!("pgmqtt ws: listening on {} (WebSocket)", ws_addr);

    // client_id → MqttClient
    let mut clients: HashMap<String, MqttClient> = HashMap::new();

    while BackgroundWorker::wait_latch(Some(LATCH_INTERVAL)) {
        if BackgroundWorker::sighup_received() {
            log!("pgmqtt mqtt+cdc: SIGHUP received");
        }

        // ── MQTT: accept raw TCP, accept WebSocket, poll ──
        accept_mqtt_connections(&mqtt_listener, &mut clients);
        accept_ws_connections(&ws_listener, &mut clients);
        poll_mqtt_clients(&mut clients);

        // ── CDC: load mappings, advance slot, drain ring buffer ──
        cdc_tick(slot_name);

        // Send pending messages to clients
        publish_pending_messages(&mut clients);

        // Periodically resend unacked QoS 1 messages
        redeliver_unacked_messages(&mut clients);
    }

    // Clean up
    for (id, _) in clients.drain() {
        subscriptions::remove_client(&id);
    }
    log!("pgmqtt mqtt+cdc: shutting down");
}

/// One CDC tick: load mappings from DB, advance slot, drain ring buffer → render → topic_buffer.
fn cdc_tick(slot_name: &str) {
    use crate::ring_buffer;
    use crate::topic_map;
    use pgrx::spi::{self, Spi};

    // Load topic mappings from DB → in-process cache
    BackgroundWorker::transaction(|| {
        if let Ok(mappings) = Spi::connect(|client| {
            // Check if table exists before querying
            let table_exists = client
                .select(
                    "SELECT to_regclass('pgmqtt_topic_mappings')::text",
                    None,
                    &[],
                )?
                .first()
                .get_one::<String>()?
                .is_some();

            if !table_exists {
                return Ok::<_, spi::Error>(Vec::new());
            }

            let mut rows = Vec::new();
            if let Ok(table) = client.select(
                "SELECT schema_name, table_name, topic_template, payload_template, qos FROM pgmqtt_topic_mappings",
                None, &[],
            ) {
                for row in table {
                    // Safely handle missing columns or type mismatches (prevents panic)
                    let s: String = row.get_by_name("schema_name").ok().flatten().unwrap_or_default();
                    let t: String = row.get_by_name("table_name").ok().flatten().unwrap_or_default();
                    let tt: String = row.get_by_name("topic_template").ok().flatten().unwrap_or_default();
                    let pt: String = row.get_by_name("payload_template").ok().flatten().unwrap_or_default();
                    let q: i32 = row.get_by_name("qos").ok().flatten().unwrap_or_default();
                    rows.push(topic_map::TopicMapping {
                        schema: s, table: t, topic_template: tt, payload_template: pt, qos: q as u8,
                    });
                }
            }
            Ok::<_, spi::Error>(rows)
        }) {
            let count = mappings.len();
            topic_map::set_mappings(mappings);
            if count > 0 {
                log!("pgmqtt: loaded {} topic mappings from DB", count);
            }
        }
    });

    // Advance the slot — triggers pg_decode_change which pushes to ring_buffer
    BackgroundWorker::transaction(|| {
        let advance_query = format!(
            "SELECT * FROM pg_logical_slot_get_changes('{}', NULL, NULL)",
            slot_name
        );
        if let Err(e) = Spi::connect(|client| {
            if let Ok(table) = client.select(&advance_query, None, &[]) {
                let mut count = 0;
                for _ in table {
                    count += 1;
                }
                if count > 0 {
                    log!(
                        "pgmqtt: slot advanced, fetched {} raw logical messages",
                        count
                    );
                }
            }
            Ok::<_, spi::Error>(())
        }) {
            log!("pgmqtt: error advancing slot: {:?}", e);
        }
    });

    // Drain CDC ring buffer → render templates → push to topic_buffer
    let events = ring_buffer::drain();
    if !events.is_empty() {
        log!("pgmqtt: drained {} events from ring buffer", events.len());
    }

    for event in &events {
        match topic_map::render(&event.schema, &event.table, event.op, &event.columns) {
            Some(rendered) => {
                let topic_str = rendered.topic.clone();
                let payload_clone = rendered.payload.clone();

                if rendered.qos > 0 {
                    BackgroundWorker::transaction(|| {
                        let result = pgrx::spi::Spi::connect_mut(|client| {
                            let payload_arg = if payload_clone.is_empty() {
                                None
                            } else {
                                Some(payload_clone.as_slice())
                            };

                            let spi_args: Vec<pgrx::datum::DatumWithOid> = vec![
                                topic_str.as_str().into(),
                                payload_arg.into(),
                                (rendered.qos as i32).into(), // CDC messages use mapping QOS
                                false.into(),                 // CDC messages are not retained
                            ];

                            let mut msg_id_opt: Option<i64> = None;
                            if let Ok(table) = client.update(
                                "INSERT INTO pgmqtt_messages (topic, payload, qos, retain) VALUES ($1, $2, $3, $4) RETURNING id",
                                None,
                                &spi_args,
                            ) {
                                for row in table {
                                    if let Ok(Some(id)) = row.get_by_name::<i64, _>("id") {
                                        msg_id_opt = Some(id);
                                    }
                                    break;
                                }
                            }
                            Ok::<_, pgrx::spi::Error>(msg_id_opt)
                        });

                        match result {
                            Ok(msg_id) => {
                                log!(
                                    "pgmqtt cdc: pushing message to topic '{}' with qos={}",
                                    rendered.topic,
                                    rendered.qos
                                );
                                topic_buffer::push(topic_buffer::MqttMessage {
                                    id: msg_id,
                                    topic: rendered.topic,
                                    payload: rendered.payload,
                                    qos: rendered.qos,
                                });
                            }
                            Err(e) => {
                                log!(
                                    "pgmqtt cdc: error persisting event to topic '{}': {:?}",
                                    topic_str,
                                    e
                                );
                            }
                        }
                    });
                } else {
                    log!(
                        "pgmqtt cdc: pushing transient message to topic '{}' with qos=0",
                        rendered.topic
                    );
                    topic_buffer::push(topic_buffer::MqttMessage {
                        id: None,
                        topic: rendered.topic,
                        payload: rendered.payload,
                        qos: 0,
                    });
                }

                log!(
                    "pgmqtt: published {} on {}.{} → topic='{}'",
                    event.op,
                    event.schema,
                    event.table,
                    topic_str
                );
            }
            None => {
                // Topic map render returns None if no mapping matches
                log!(
                    "pgmqtt: no mapping match for {}.{}",
                    event.schema,
                    event.table
                );
            }
        }
    }
}

/// Accept new raw TCP MQTT connections and perform the MQTT CONNECT handshake.
fn accept_mqtt_connections(listener: &TcpListener, clients: &mut HashMap<String, MqttClient>) {
    loop {
        match listener.accept() {
            Ok((stream, addr)) => {
                log!("pgmqtt mqtt: new TCP connection from {}", addr);
                handle_new_connection(Transport::Raw(stream), clients);
            }
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
            Err(e) => {
                log!("pgmqtt mqtt: accept error: {}", e);
                break;
            }
        }
    }
}

/// Accept new WebSocket connections, perform the HTTP Upgrade handshake, then
/// the MQTT CONNECT handshake.
fn accept_ws_connections(listener: &TcpListener, clients: &mut HashMap<String, MqttClient>) {
    loop {
        match listener.accept() {
            Ok((mut stream, addr)) => {
                log!("pgmqtt ws: new WebSocket connection from {}", addr);

                // Set a generous timeout for the HTTP upgrade
                let _ = stream.set_read_timeout(Some(Duration::from_secs(5)));
                let _ = stream.set_write_timeout(Some(Duration::from_secs(5)));

                match websocket::handshake(&mut stream) {
                    Ok(leftover) => {
                        log!("pgmqtt ws: WebSocket upgrade complete for {}", addr);
                        let ws = websocket::WsStream::new(stream, leftover);
                        handle_new_connection(Transport::Ws(ws), clients);
                    }
                    Err(e) => {
                        log!("pgmqtt ws: upgrade failed for {}: {}", addr, e);
                        // stream dropped → connection closed
                    }
                }
            }
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
            Err(e) => {
                log!("pgmqtt ws: accept error: {}", e);
                break;
            }
        }
    }
}

/// Common MQTT CONNECT handshake for both TCP and WebSocket transports.
fn handle_new_connection(mut transport: Transport, clients: &mut HashMap<String, MqttClient>) {
    let _ = transport.set_read_timeout(Some(Duration::from_secs(5)));
    let _ = transport.set_write_timeout(Some(Duration::from_secs(5)));

    // Read until we have a complete MQTT CONNECT packet
    let mut connect_buf = Vec::new();
    let mut tmp = [0u8; 1024];

    loop {
        let n = match transport.read(&mut tmp) {
            Ok(0) => return,
            Ok(n) => n,
            Err(e) => {
                log!("pgmqtt mqtt: client handshake read error: {}", e);
                return;
            }
        };
        connect_buf.extend_from_slice(&tmp[..n]);

        match mqtt::parse_packet(&connect_buf) {
            Ok((mqtt::InboundPacket::Connect(connect), _consumed)) => {
                // Success! Proceed to register the client.
                finish_connect(transport, connect, clients);
                return;
            }
            Ok(_) => {
                log!("pgmqtt mqtt: expected CONNECT, got something else");
                let _ = transport.write_all(&mqtt::build_disconnect(mqtt::reason::PROTOCOL_ERROR));
                return;
            }
            Err(mqtt::MqttError::Incomplete) => {
                // Keep reading
                if connect_buf.len() > MAX_REQUEST_BYTES {
                    log!("pgmqtt mqtt: CONNECT packet too large");
                    return;
                }
                continue;
            }
            Err(e) => {
                log!("pgmqtt mqtt: CONNECT parse error: {}", e);
                let _ = transport
                    .write_all(&mqtt::build_connack(false, mqtt::reason::MALFORMED_PACKET));
                return;
            }
        }
    }
}

fn finish_connect(
    mut transport: Transport,
    packet: mqtt::ConnectPacket,
    clients: &mut HashMap<String, MqttClient>,
) {
    let client_id = if packet.client_id.is_empty() {
        let id = NEXT_AUTO_CLIENT_ID.fetch_add(1, Ordering::Relaxed);
        format!("pgmqtt-auto-{}", id)
    } else {
        packet.client_id.clone()
    };

    log!("pgmqtt mqtt: CONNECT from client '{}'", client_id);

    // If clean_start, remove any previous session
    if packet.clean_start {
        subscriptions::remove_client(&client_id);
        with_sessions(|s| {
            s.remove(&client_id);
        });
    }

    let session_present = with_sessions(|s| s.contains_key(&client_id));
    if !session_present {
        with_sessions(|s| {
            s.insert(client_id.clone(), MqttSession::new());
        });
    }

    // Send CONNACK with success
    let connack = mqtt::build_connack(session_present, mqtt::reason::SUCCESS);
    if transport.write_all(&connack).is_err() {
        log!("pgmqtt mqtt: failed to send CONNACK to '{}'", client_id);
        return;
    }

    // Set non-blocking for ongoing reads
    let _ = transport.set_nonblocking(true);

    log!("pgmqtt mqtt: client '{}' ready for polling", client_id);
    clients.insert(
        client_id.clone(),
        MqttClient::new(transport, client_id, packet.will, packet.keep_alive),
    );
}

/// Poll all connected clients for incoming MQTT packets.
fn poll_mqtt_clients(clients: &mut HashMap<String, MqttClient>) {
    let mut to_remove = Vec::new();

    for (client_id, client) in clients.iter_mut() {
        // Try to read data
        let mut tmp = [0u8; MAX_REQUEST_BYTES];
        match client.transport.read(&mut tmp) {
            Ok(0) => {
                log!("pgmqtt mqtt: client '{}' disconnected (EOF)", client_id);
                to_remove.push(client_id.clone());
                continue;
            }
            Ok(n) => {
                if client.buf.len() + n > MAX_REQUEST_BYTES {
                    log!("pgmqtt mqtt: client '{}' exceeded max buffer size ({} bytes). Disconnecting.", client_id, MAX_REQUEST_BYTES);
                    let _ = client
                        .transport
                        .write_all(&mqtt::build_disconnect(mqtt::reason::MALFORMED_PACKET));
                    to_remove.push(client_id.clone());
                    continue;
                }
                client.buf.extend_from_slice(&tmp[..n]);
                client.last_received_at = std::time::Instant::now();
            }
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                // No data available, continue
            }
            Err(e) => {
                log!("pgmqtt mqtt: read error from '{}': {}", client_id, e);
                to_remove.push(client_id.clone());
                continue;
            }
        }

        // Process all complete packets in the buffer
        loop {
            if client.buf.is_empty() {
                break;
            }

            // Check if we have a full packet
            let pkt_len = match mqtt::packet_len(&client.buf) {
                Ok(len) => len,
                Err(mqtt::MqttError::Incomplete) => break,
                Err(e) => {
                    log!("pgmqtt mqtt: packet error from '{}': {}", client_id, e);
                    let _ = client
                        .transport
                        .write_all(&mqtt::build_disconnect(mqtt::reason::MALFORMED_PACKET));
                    to_remove.push(client_id.clone());
                    break;
                }
            };

            let pkt_data: Vec<u8> = client.buf.drain(..pkt_len).collect();

            match mqtt::parse_packet(&pkt_data) {
                Ok((packet, _)) => {
                    if !handle_mqtt_packet(client, packet) {
                        to_remove.push(client_id.clone());
                        break;
                    }
                }
                Err(e) => {
                    log!("pgmqtt mqtt: parse error from '{}': {}", client_id, e);
                    let _ = client
                        .transport
                        .write_all(&mqtt::build_disconnect(mqtt::reason::MALFORMED_PACKET));
                    to_remove.push(client_id.clone());
                    break;
                }
            }
        }

        // Check for keep-alive timeout (1.5 * keep_alive)
        // Occurs *after* reading in pings, so we shouldn't
        // really need to worry about CPU starvation causing false positives
        if client.keep_alive > 0 {
            let timeout = std::time::Duration::from_secs((client.keep_alive as u64 * 3) / 2);
            // Uses instant -> strictly increasing, shouldn't be broken
            // By time changing out from underneath us
            if client.last_received_at.elapsed() > timeout {
                log!(
                    "pgmqtt mqtt: client '{}' keep-alive timeout exceeded. Disconnecting.",
                    client_id
                );
                to_remove.push(client_id.clone());
            }
        }
    }

    // Clean up disconnected clients
    for id in to_remove {
        if let Some(mut client) = clients.remove(&id) {
            if let Some(will) = client.will.take() {
                log!(
                    "pgmqtt mqtt: client '{}' disconnected unexpectedly, publishing Will",
                    id
                );
                publish_message(
                    will.topic,
                    will.payload,
                    will.qos,
                    will.retain,
                    &format!("{} (Will)", id),
                );
            }
        }
    }
}

fn publish_message(topic: String, payload: Vec<u8>, qos: u8, retain: bool, log_sender: &str) {
    let payload_clone = payload.clone();
    let topic_clone = topic.clone();
    if qos > 0 || retain {
        BackgroundWorker::transaction(|| {
            let result = pgrx::spi::Spi::connect_mut(|client| {
                let payload_arg = if payload_clone.is_empty() {
                    None
                } else {
                    Some(payload_clone.as_slice())
                };

                let insert_msg = "INSERT INTO pgmqtt_messages (topic, payload, qos, retain) VALUES ($1, $2, $3, $4) RETURNING id";
                let mut msg_id_opt: Option<i64> = None;

                let spi_args: Vec<pgrx::datum::DatumWithOid> = vec![
                    topic_clone.as_str().into(),
                    payload_arg.into(),
                    (qos as i32).into(),
                    retain.into(),
                ];

                if let Ok(table) = client.update(insert_msg, None, &spi_args) {
                    for row in table {
                        if let Ok(Some(id)) = row.get_by_name::<i64, _>("id") {
                            msg_id_opt = Some(id);
                        }
                        break;
                    }
                }

                if retain {
                    if payload_clone.is_empty() {
                        let args: Vec<pgrx::datum::DatumWithOid> =
                            vec![topic_clone.as_str().into()];
                        let _ = client.update(
                            "DELETE FROM pgmqtt_retained WHERE topic = $1",
                            None,
                            &args,
                        );
                    } else if let Some(msg_id) = msg_id_opt {
                        let args: Vec<pgrx::datum::DatumWithOid> =
                            vec![topic_clone.as_str().into(), msg_id.into()];
                        let _ = client.update("INSERT INTO pgmqtt_retained (topic, message_id) VALUES ($1, $2) ON CONFLICT (topic) DO UPDATE SET message_id = EXCLUDED.message_id", None, &args);
                    }
                }
                Ok::<_, pgrx::spi::Error>(msg_id_opt)
            });

            match result {
                Ok(msg_id) => {
                    log!(
                        "pgmqtt: pushing message from '{}' to topic '{}' with qos={}",
                        log_sender,
                        topic,
                        qos
                    );
                    topic_buffer::push(topic_buffer::MqttMessage {
                        id: msg_id,
                        topic: topic,
                        payload: payload_clone,
                        qos: qos,
                    });
                }
                Err(e) => {
                    log!(
                        "pgmqtt: error persisting PUBLISH from '{}': {:?}",
                        log_sender,
                        e
                    );
                }
            }
        });
    } else {
        log!(
            "pgmqtt: pushing transient message from '{}' to topic '{}' with qos=0",
            log_sender,
            topic
        );
        topic_buffer::push(topic_buffer::MqttMessage {
            id: None,
            topic: topic,
            payload: payload_clone,
            qos: 0,
        });
    }
}

/// Handle a single parsed MQTT packet. Returns `false` if connection should close.
fn handle_mqtt_packet(client: &mut MqttClient, packet: mqtt::InboundPacket) -> bool {
    let client_id = client.client_id.clone();
    let transport = &mut client.transport;
    match packet {
        mqtt::InboundPacket::Puback(packet_id) => {
            let res = with_sessions(|sessions| {
                if let Some(session) = sessions.get_mut(&client_id) {
                    session.inflight.remove(&packet_id)
                } else {
                    None
                }
            });
            if let Some((_, _, msg_id, _)) = res {
                log!(
                    "pgmqtt mqtt: '{}' acked packet_id={} (msg_id={:?})",
                    client_id,
                    packet_id,
                    msg_id
                );
            } else {
                log!(
                    "pgmqtt mqtt: '{}' sent PUBACK for unknown packet_id={}",
                    client_id,
                    packet_id
                );
            }
            true
        }
        mqtt::InboundPacket::Subscribe(sub) => {
            let mut reason_codes = Vec::with_capacity(sub.topics.len());
            for (topic_filter, requested_qos) in &sub.topics {
                let granted = subscriptions::subscribe(&client_id, topic_filter, *requested_qos);
                reason_codes.push(granted);
                log!(
                    "pgmqtt mqtt: '{}' subscribed to '{}' (QoS {})",
                    client_id,
                    topic_filter,
                    granted
                );
            }
            let suback = mqtt::build_suback(sub.packet_id, &reason_codes);
            if transport.write_all(&suback).is_err() {
                return false;
            }
            true
        }
        mqtt::InboundPacket::Unsubscribe(unsub) => {
            let mut reason_codes = Vec::with_capacity(unsub.topics.len());
            for topic_filter in &unsub.topics {
                let existed = subscriptions::unsubscribe(&client_id, topic_filter);
                reason_codes.push(if existed {
                    mqtt::reason::SUCCESS
                } else {
                    0x11 // No Subscription Existed
                });
                log!(
                    "pgmqtt mqtt: '{}' unsubscribed from '{}'",
                    client_id,
                    topic_filter
                );
            }
            let unsuback = mqtt::build_unsuback(unsub.packet_id, &reason_codes);
            if transport.write_all(&unsuback).is_err() {
                return false;
            }
            true
        }
        mqtt::InboundPacket::Pingreq => {
            let resp = mqtt::build_pingresp();
            transport.write_all(&resp).is_ok()
        }
        mqtt::InboundPacket::Disconnect(reason_code) => {
            log!(
                "pgmqtt mqtt: '{}' sent DISCONNECT (reason_code=0x{:02x})",
                client_id,
                reason_code
            );
            if reason_code == mqtt::reason::NORMAL_DISCONNECT {
                client.will = None;
            }
            false // signal removal
        }
        mqtt::InboundPacket::Publish(pub_pkt) => {
            publish_message(
                pub_pkt.topic,
                pub_pkt.payload,
                pub_pkt.qos,
                pub_pkt.retain,
                &client_id,
            );

            if pub_pkt.qos == 1 {
                if let Some(pid) = pub_pkt.packet_id {
                    let puback = mqtt::build_puback(pid);
                    let _ = transport.write_all(&puback);
                }
            }
            true
        }
        mqtt::InboundPacket::Connect(_) => {
            // Second CONNECT on an already-connected client is a protocol error
            log!(
                "pgmqtt mqtt: '{}' sent second CONNECT — protocol error",
                client_id
            );
            let _ = transport.write_all(&mqtt::build_disconnect(mqtt::reason::PROTOCOL_ERROR));
            false
        }
    }
}

/// Drain the topic_buffer and publish messages to matching subscribers.
fn publish_pending_messages(clients: &mut HashMap<String, MqttClient>) {
    let messages = topic_buffer::drain_all();
    if messages.is_empty() {
        return;
    }

    log!("pgmqtt: publishing {} messages to clients", messages.len());

    let mut to_remove = Vec::new();

    for msg in &messages {
        let subscriber_ids = subscriptions::match_topic(&msg.topic);
        if subscriber_ids.is_empty() {
            log!(
                "pgmqtt: no subscribers for topic='{}' (active_filters={:?})",
                msg.topic,
                subscriptions::active_filters()
            );
        }
        for (sub_id, granted_qos) in &subscriber_ids {
            let pkt_res = with_sessions(|sessions| {
                if let Some(session) = sessions.get_mut(sub_id) {
                    let delivery_qos = std::cmp::min(msg.qos, *granted_qos);
                    if delivery_qos == 1 {
                        if session.inflight.len() >= MAX_INFLIGHT_MESSAGES {
                            pgrx::log!("pgmqtt: client '{}' hit max inflight QoS 1 messages ({}), dropping new message.", sub_id, MAX_INFLIGHT_MESSAGES);
                            None
                        } else {
                            let pid = session.next_packet_id;
                            session.next_packet_id = session.next_packet_id.wrapping_add(1);
                            if session.next_packet_id == 0 {
                                session.next_packet_id = 1;
                            }
                            session.inflight.insert(
                                pid,
                                (
                                    msg.topic.clone(),
                                    msg.payload.clone(),
                                    msg.id,
                                    std::time::Instant::now(),
                                ),
                            );
                            Some(mqtt::build_publish_qos1(&msg.topic, &msg.payload, pid))
                        }
                    } else {
                        Some(mqtt::build_publish_qos0(&msg.topic, &msg.payload))
                    }
                } else {
                    None
                }
            });

            if let Some(pkt) = pkt_res {
                if let Some(client) = clients.get_mut(sub_id) {
                    if client.transport.write_all(&pkt).is_err() {
                        to_remove.push(sub_id.clone());
                    }
                }
            }
        }
    }

    for id in to_remove {
        clients.remove(&id);
    }
}

/// Periodic check for unacked QoS 1 messages and redelivery.
fn redeliver_unacked_messages(clients: &mut HashMap<String, MqttClient>) {
    let now = std::time::Instant::now();
    let timeout = std::time::Duration::from_secs(5);

    let mut to_resend = Vec::new();

    with_sessions(|sessions| {
        for (client_id, session) in sessions.iter_mut() {
            if clients.contains_key(client_id) {
                for (pid, (topic, payload, _msg_id, sent_at)) in &mut session.inflight {
                    if now.duration_since(*sent_at) > timeout {
                        to_resend.push((client_id.clone(), *pid, topic.clone(), payload.clone()));
                        *sent_at = now; // update timer for next redelivery
                    }
                }
            }
        }
    });

    for (cid, pid, topic, payload) in to_resend {
        if let Some(client) = clients.get_mut(&cid) {
            log!("pgmqtt mqtt: redelivering packet_id={} to '{}'", pid, cid);
            let pkt = mqtt::build_publish_qos1_dup(&topic, &payload, pid);
            let _ = client.transport.write_all(&pkt);
        }
    }
}
