use crate::mqtt;
use crate::subscriptions;
use crate::topic_buffer;

use crate::websocket;
use pgrx::bgworkers::BackgroundWorker;
use pgrx::log;

use std::collections::{BTreeMap, HashMap, VecDeque};
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
    inflight: BTreeMap<u16, (String, Vec<u8>, Option<i64>, std::time::Instant)>,
    /// Messages waiting to be promoted into inflight once a slot opens up.
    queue: VecDeque<topic_buffer::MqttMessage>,
    /// MQTT 5.0 Session Expiry Interval in seconds. 0 = end at disconnect.
    expiry_interval: u32,
    /// Set when the client disconnects (used by the session sweeper).
    disconnected_at: Option<std::time::Instant>,
}

impl MqttSession {
    fn new() -> Self {
        Self {
            next_packet_id: 1,
            inflight: BTreeMap::new(),
            queue: VecDeque::new(),
            expiry_interval: 0,
            disconnected_at: None,
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

    subscriptions::load_from_db();
    load_sessions_from_db();

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

        // Sweep sessions whose Session Expiry Interval has elapsed
        sweep_expired_sessions();
    }

    // Clean up
    for (id, _) in clients.drain() {
        subscriptions::remove_client(&id);
    }
    log!("pgmqtt mqtt+cdc: shutting down");
}

fn load_sessions_from_db() {
    let _ = BackgroundWorker::transaction(|| {
        pgrx::spi::Spi::connect_mut(|client| {
            // Check if tables exist
            let table_exists = client
                .select("SELECT to_regclass('pgmqtt_sessions')::text", None, &[])
                .and_then(|t| Ok(t.first().get_one::<String>()?.is_some()))
                .unwrap_or(false);

            if !table_exists {
                return Ok::<_, pgrx::spi::Error>(());
            }

            // Treat any crashed connections as disconnecting now
            client.update(
                "UPDATE pgmqtt_sessions SET disconnected_at = NOW() WHERE disconnected_at IS NULL",
                None,
                &[],
            )?;
            // Delete mathematically expired sessions
            client.update(
                "DELETE FROM pgmqtt_sessions WHERE disconnected_at + (expiry_interval || ' seconds')::interval < NOW()",
                None,
                &[],
            )?;

            // Loop through remaining sessions
            if let Ok(table) = client.select(
                "SELECT client_id, expiry_interval FROM pgmqtt_sessions",
                None,
                &[],
            ) {
                with_sessions(|sessions| {
                    for row in table {
                        if let (Ok(Some(client_id)), Ok(Some(expiry))) = (
                            row.get_by_name::<String, _>("client_id"),
                            row.get_by_name::<i64, _>("expiry_interval"),
                        ) {
                            let mut sess = MqttSession::new();
                            sess.expiry_interval = expiry as u32;
                            // Since they were loaded from DB during startup, consider them disconnected right now
                            sess.disconnected_at = Some(std::time::Instant::now());
                            sessions.insert(client_id, sess);
                        }
                    }
                });
            }

            // Now load the offline queue for these sessions
            if let Ok(table) = client.select(
                "SELECT q.client_id, q.message_id, q.packet_id, m.topic, m.payload, m.qos \
                 FROM pgmqtt_session_queue q \
                 JOIN pgmqtt_messages m ON q.message_id = m.id \
                 ORDER BY q.message_id ASC",
                None,
                &[],
            ) {
                with_sessions(|sessions| {
                    for row in table {
                        if let (
                            Ok(Some(client_id)),
                            Ok(Some(msg_id)),
                            Ok(packet_id_opt),
                            Ok(Some(topic)),
                            Ok(payload_opt),
                            Ok(Some(qos)),
                        ) = (
                            row.get_by_name::<String, _>("client_id"),
                            row.get_by_name::<i64, _>("message_id"),
                            row.get_by_name::<i32, _>("packet_id"),
                            row.get_by_name::<String, _>("topic"),
                            row.get_by_name::<Vec<u8>, _>("payload"),
                            row.get_by_name::<i32, _>("qos"),
                        ) {
                            if let Some(sess) = sessions.get_mut(&client_id) {
                                let payload = payload_opt.unwrap_or_default();
                                if let Some(pid) = packet_id_opt {
                                    sess.inflight.insert(
                                        pid as u16,
                                        (topic, payload, Some(msg_id), std::time::Instant::now()),
                                    );
                                } else {
                                    sess.queue.push_back(topic_buffer::MqttMessage {
                                        id: Some(msg_id),
                                        topic,
                                        payload,
                                        qos: qos as u8,
                                    });
                                }
                            }
                        }
                    }

                    // Recover next_packet_id
                    for sess in sessions.values_mut() {
                        let max_pid = sess.inflight.keys().max().copied().unwrap_or(0);
                        sess.next_packet_id = max_pid.wrapping_add(1);
                        if sess.next_packet_id == 0 {
                            sess.next_packet_id = 1;
                        }
                    }
                });
            }
            Ok::<_, pgrx::spi::Error>(())
        })
    });
}

/// Maximum number of WAL events to consume per `cdc_tick` batch transaction.
///
/// Each batch is one atomic PostgreSQL transaction: the replication slot LSN
/// only advances when **all** QOS ≥ 1 messages from that batch have been
/// durably inserted into `pgmqtt_messages`. Smaller values reduce the retry
/// cost if a batch fails; larger values reduce per-batch transaction overhead.
const CDC_BATCH_SIZE: usize = 256;

/// One CDC tick: load mappings from DB, then drain the WAL slot in atomic
/// batches, persisting QOS ≥ 1 messages within the same transaction.
///
/// # Atomicity guarantee
///
/// For each batch the sequence is:
///   1. `pg_logical_slot_get_changes(..., upto_nchanges = CDC_BATCH_SIZE)`
///      fires the output plugin for each event, which pushes raw `ChangeEvent`s
///      into the in-memory `ring_buffer`.
///   2. The ring buffer is drained; every QOS ≥ 1 rendered message is
///      `INSERT`ed into `pgmqtt_messages` **within the same transaction**.
///   3. On commit, the slot's `confirmed_flush_lsn` advances to cover exactly
///      those events — and only those events.
///
/// A crash between the two SPI calls is impossible: they share one transaction.
/// If the inserts fail the whole batch rolls back; the slot does not advance;
/// the same events will be re-read next tick (at-least-once delivery).
///
/// QOS 0 messages are not persisted; they are collected during the transaction
/// and pushed to the `topic_buffer` after the commit (fire-and-forget).
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

    // ── Batched, atomic CDC drain loop ───────────────────────────────────────
    //
    // BackgroundWorker::transaction requires UnwindSafe + RefUnwindSafe, which
    // &mut T does NOT satisfy.  The fix: all mutable state lives *inside* the
    // closure (local variables, not captured).  The closure returns a
    // (messages, batch_count, ok) tuple that we destructure after the commit.
    //
    // topic_buffer::push is called ONLY after the transaction commits so that
    // messages never enter the delivery queue before they are durably persisted.
    loop {
        let (to_publish, batch_count, batch_ok) =
            BackgroundWorker::transaction(|| -> (Vec<topic_buffer::MqttMessage>, usize, bool) {
                let mut to_publish: Vec<topic_buffer::MqttMessage> = Vec::new();
                let mut batch_count: usize = 0;

                // ── Step 1: advance the slot by at most CDC_BATCH_SIZE events ──
                //
                // The output plugin (pg_decode_change) fires synchronously for each
                // row, pushing a ChangeEvent into ring_buffer.  Because this runs
                // inside the same transaction as the inserts below, the slot's
                // confirmed_flush_lsn only moves forward on COMMIT.
                let advance_query = format!(
                    "SELECT * FROM pg_logical_slot_get_changes('{}', NULL, {})",
                    slot_name, CDC_BATCH_SIZE
                );
                match Spi::connect(|client| {
                    let mut n = 0usize;
                    if let Ok(table) = client.select(&advance_query, None, &[]) {
                        for _ in table {
                            n += 1;
                        }
                    }
                    Ok::<usize, spi::Error>(n)
                }) {
                    Ok(n) => {
                        batch_count = n;
                        if n > 0 {
                            log!("pgmqtt: slot batch fetched {} raw logical messages", n);
                        }
                    }
                    Err(e) => {
                        log!("pgmqtt: error advancing slot: {:?} — skipping batch", e);
                        return (to_publish, batch_count, false);
                    }
                }

                // ── Step 2: drain ring_buffer; persist QOS ≥ 1 inside this tx ──
                let events = ring_buffer::drain();

                for event in &events {
                    match topic_map::render(&event.schema, &event.table, event.op, &event.columns) {
                        Some(rendered) => {
                            let topic_str = rendered.topic.clone();

                            if rendered.qos > 0 {
                                // Persist within this transaction — committed atomically
                                // with the slot advance above.
                                let payload_clone = rendered.payload.clone();
                                let result = pgrx::spi::Spi::connect_mut(|client| {
                                    let payload_arg = if payload_clone.is_empty() {
                                        None
                                    } else {
                                        Some(payload_clone.as_slice())
                                    };

                                    let spi_args: Vec<pgrx::datum::DatumWithOid> = vec![
                                        topic_str.as_str().into(),
                                        payload_arg.into(),
                                        (rendered.qos as i32).into(),
                                        false.into(), // CDC messages are not retained
                                    ];

                                    let mut msg_id_opt: Option<i64> = None;
                                    if let Ok(table) = client.update(
                                        "INSERT INTO pgmqtt_messages (topic, payload, qos, retain) \
                                         VALUES ($1, $2, $3, $4) RETURNING id",
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
                                            "pgmqtt cdc: persisted QOS {} to '{}' (msg_id={:?})",
                                            rendered.qos,
                                            rendered.topic,
                                            msg_id
                                        );
                                        // Collected — pushed to topic_buffer after commit.
                                        to_publish.push(topic_buffer::MqttMessage {
                                            id: msg_id,
                                            topic: rendered.topic,
                                            payload: rendered.payload,
                                            qos: rendered.qos,
                                        });
                                    }
                                    Err(e) => {
                                        log!(
                                            "pgmqtt cdc: error persisting QOS {} to '{}': {:?} \
                                             — batch rolls back, events retried next tick",
                                            rendered.qos,
                                            topic_str,
                                            e
                                        );
                                        // Return false → caller drops to_publish and stops.
                                        return (Vec::new(), batch_count, false);
                                    }
                                }
                            } else {
                                // QOS 0 — collected for post-commit push (fire-and-forget).
                                to_publish.push(topic_buffer::MqttMessage {
                                    id: None,
                                    topic: rendered.topic,
                                    payload: rendered.payload,
                                    qos: 0,
                                });
                            }

                            log!(
                                "pgmqtt: processed {} on {}.{} → topic='{}'",
                                event.op,
                                event.schema,
                                event.table,
                                topic_str
                            );
                        }
                        None => {
                            log!(
                                "pgmqtt: no mapping match for {}.{}",
                                event.schema,
                                event.table
                            );
                        }
                    }
                }

                (to_publish, batch_count, true)
            });
        // ↑ COMMIT: slot LSN advances IFF all QOS ≥ 1 inserts committed.
        //   batch_ok=false means the transaction rolled back; slot unchanged.

        if batch_ok {
            // Push all messages to the delivery queue only after the commit:
            //  • QOS ≥ 1 messages are now durably in pgmqtt_messages.
            //  • QOS 0 messages are fire-and-forget anyway.
            for msg in to_publish {
                topic_buffer::push(msg);
            }
        }

        // Stop when the batch was smaller than the limit — WAL fully drained.
        if batch_count < CDC_BATCH_SIZE {
            break;
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
        let _ = BackgroundWorker::transaction(|| {
            pgrx::spi::Spi::connect_mut(|client| {
                let args: Vec<pgrx::datum::DatumWithOid> = vec![client_id.as_str().into()];
                client.update(
                    "DELETE FROM pgmqtt_sessions WHERE client_id = $1",
                    None,
                    &args,
                )?;
                Ok::<_, pgrx::spi::Error>(())
            })
        });
        subscriptions::remove_client(&client_id);
        with_sessions(|s| {
            s.remove(&client_id);
        });
    }

    let session_present = with_sessions(|s| {
        // Treat a session as "not present" if it had already expired
        if let Some(sess) = s.get(&client_id) {
            if sess.expiry_interval == 0 {
                return false; // zero = session ended at disconnect
            }
            true
        } else {
            false
        }
    });

    let expiry = with_sessions(|s| {
        let sess = s.entry(client_id.clone()).or_insert_with(MqttSession::new);
        if let Some(e) = packet.session_expiry_interval {
            sess.expiry_interval = e;
        } else if packet.clean_start {
            sess.expiry_interval = 0;
        }
        // Clear the disconnected timestamp since the client is reconnecting
        sess.disconnected_at = None;
        sess.expiry_interval
    });

    let _ = BackgroundWorker::transaction(|| {
        pgrx::spi::Spi::connect_mut(|client| {
            let args: Vec<pgrx::datum::DatumWithOid> =
                vec![client_id.as_str().into(), (expiry as i64).into()];
            client.update(
                "INSERT INTO pgmqtt_sessions (client_id, expiry_interval, disconnected_at) \
                 VALUES ($1, $2, NULL) \
                 ON CONFLICT (client_id) DO UPDATE \
                 SET expiry_interval = EXCLUDED.expiry_interval, disconnected_at = NULL",
                None,
                &args,
            )?;
            Ok::<_, pgrx::spi::Error>(())
        })
    });

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
        MqttClient::new(transport, client_id.clone(), packet.will, packet.keep_alive),
    );

    // ── Session resumption: redeliver inflight + drain queue ─────────────────
    // MQTT 5.0 §4.4: The broker MUST retransmit all unacknowledged PUBLISH
    // packets (with DUP=1) and deliver any queued messages after a reconnect
    // with session_present=true.
    if session_present {
        // Collect what needs to be sent while holding the session lock.
        let (to_send, db_updates) = with_sessions(|sessions| {
            let mut pkts = Vec::new();
            let mut updates = Vec::new();
            if let Some(session) = sessions.get_mut(&client_id) {
                // 1. Redeliver inflight messages (DUP=1) so the client can PUBACK them.
                for (pid, (topic, payload, _msg_id, sent_at)) in session.inflight.iter_mut() {
                    pkts.push(mqtt::build_publish_qos1_dup(topic, payload, *pid));
                    // Reset the timer so redeliver_unacked_messages doesn't fire immediately.
                    *sent_at = std::time::Instant::now();
                }
                // 2. Drain the pending queue into inflight and add to send list.
                while session.inflight.len() < MAX_INFLIGHT_MESSAGES {
                    if let Some(queued) = session.queue.pop_front() {
                        let pid = session.next_packet_id;
                        session.next_packet_id = session.next_packet_id.wrapping_add(1);
                        if session.next_packet_id == 0 {
                            session.next_packet_id = 1;
                        }
                        session.inflight.insert(
                            pid,
                            (
                                queued.topic.clone(),
                                queued.payload.clone(),
                                queued.id,
                                std::time::Instant::now(),
                            ),
                        );
                        if let Some(mid) = queued.id {
                            updates.push((mid, pid as i32));
                        }
                        pkts.push(mqtt::build_publish_qos1(
                            &queued.topic,
                            &queued.payload,
                            pid,
                        ));
                    } else {
                        break;
                    }
                }
            }
            (pkts, updates)
        });

        if !db_updates.is_empty() {
            let _ = BackgroundWorker::transaction(|| {
                pgrx::spi::Spi::connect_mut(|client| {
                    for (mid, pid) in db_updates {
                        let args: Vec<pgrx::datum::DatumWithOid> =
                            vec![pid.into(), client_id.as_str().into(), mid.into()];
                        client.update(
                            "UPDATE pgmqtt_session_queue SET packet_id = $1 WHERE client_id = $2 AND message_id = $3",
                            None,
                            &args,
                        )?;
                    }
                    Ok::<_, pgrx::spi::Error>(())
                })
            });
        }

        if !to_send.is_empty() {
            log!(
                "pgmqtt mqtt: resuming session for '{}': redelivering {} packet(s)",
                client_id,
                to_send.len()
            );
            if let Some(client) = clients.get_mut(&client_id) {
                for pkt in to_send {
                    let _ = client.transport.write_all(&pkt);
                }
            }
        }
    }
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
        // Mark session as disconnected so the sweeper can reap it later.
        // If expiry_interval == 0 the session should end immediately.
        let should_remove = with_sessions(|s| {
            s.get(&id)
                .map(|sess| sess.expiry_interval == 0)
                .unwrap_or(false)
        });

        if should_remove {
            let _ = BackgroundWorker::transaction(|| {
                pgrx::spi::Spi::connect_mut(|client| {
                    let args: Vec<pgrx::datum::DatumWithOid> = vec![id.as_str().into()];
                    client.update(
                        "DELETE FROM pgmqtt_sessions WHERE client_id = $1",
                        None,
                        &args,
                    )?;
                    Ok::<_, pgrx::spi::Error>(())
                })
            });
            with_sessions(|s| {
                s.remove(&id);
            });
            subscriptions::remove_client(&id);
        } else {
            let _ = BackgroundWorker::transaction(|| {
                pgrx::spi::Spi::connect_mut(|client| {
                    let args: Vec<pgrx::datum::DatumWithOid> = vec![id.as_str().into()];
                    client.update(
                        "UPDATE pgmqtt_sessions SET disconnected_at = NOW() WHERE client_id = $1",
                        None,
                        &args,
                    )?;
                    Ok::<_, pgrx::spi::Error>(())
                })
            });
            with_sessions(|s| {
                if let Some(sess) = s.get_mut(&id) {
                    sess.disconnected_at = Some(std::time::Instant::now());
                }
            });
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

                let table = client.update(insert_msg, None, &spi_args)?;
                for row in table {
                    if let Ok(Some(id)) = row.get_by_name::<i64, _>("id") {
                        msg_id_opt = Some(id);
                    }
                    break;
                }

                if retain {
                    if payload_clone.is_empty() {
                        let args: Vec<pgrx::datum::DatumWithOid> =
                            vec![topic_clone.as_str().into()];
                        client.update(
                            "DELETE FROM pgmqtt_retained WHERE topic = $1",
                            None,
                            &args,
                        )?;
                    } else if let Some(msg_id) = msg_id_opt {
                        let args: Vec<pgrx::datum::DatumWithOid> =
                            vec![topic_clone.as_str().into(), msg_id.into()];
                        client.update(
                            "INSERT INTO pgmqtt_retained (topic, message_id) VALUES ($1, $2) ON CONFLICT (topic) DO UPDATE SET message_id = EXCLUDED.message_id", 
                            None, 
                            &args
                        )?;
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
            if let Some((_, _, Some(msg_id), _)) = res {
                let _ = BackgroundWorker::transaction(|| {
                    pgrx::spi::Spi::connect_mut(|client| {
                        let args: Vec<pgrx::datum::DatumWithOid> =
                            vec![client_id.as_str().into(), msg_id.into()];
                        client.update(
                            "DELETE FROM pgmqtt_session_queue WHERE client_id = $1 AND message_id = $2",
                            None,
                            &args,
                        )?;
                        Ok::<_, pgrx::spi::Error>(())
                    })
                });
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
            // Slot freed — promote next queued message into inflight.
            let (next_pkt, db_update) = with_sessions(|sessions| {
                if let Some(session) = sessions.get_mut(&client_id) {
                    if let Some(queued) = session.queue.pop_front() {
                        let pid = session.next_packet_id;
                        session.next_packet_id = session.next_packet_id.wrapping_add(1);
                        if session.next_packet_id == 0 {
                            session.next_packet_id = 1;
                        }
                        session.inflight.insert(
                            pid,
                            (
                                queued.topic.clone(),
                                queued.payload.clone(),
                                queued.id,
                                std::time::Instant::now(),
                            ),
                        );
                        let update = queued.id.map(|mid| (mid, pid as i32));
                        (
                            Some(mqtt::build_publish_qos1(
                                &queued.topic,
                                &queued.payload,
                                pid,
                            )),
                            update,
                        )
                    } else {
                        (None, None)
                    }
                } else {
                    (None, None)
                }
            });

            if let Some((mid, pid)) = db_update {
                let _ = BackgroundWorker::transaction(|| {
                    pgrx::spi::Spi::connect_mut(|client| {
                        let args: Vec<pgrx::datum::DatumWithOid> =
                            vec![pid.into(), client_id.as_str().into(), mid.into()];
                        client.update(
                            "UPDATE pgmqtt_session_queue SET packet_id = $1 WHERE client_id = $2 AND message_id = $3",
                            None,
                            &args,
                        )?;
                        Ok::<_, pgrx::spi::Error>(())
                    })
                });
            }

            if let Some(pkt) = next_pkt {
                let _ = transport.write_all(&pkt);
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

/// Sweep all sessions whose Session Expiry Interval has elapsed.
/// Must only be called for sessions whose client is not actively connected.
fn sweep_expired_sessions() {
    let now = std::time::Instant::now();
    let mut expired_ids: Vec<String> = Vec::new();
    with_sessions(|sessions| {
        for (id, sess) in sessions.iter() {
            if let Some(disconnected_at) = sess.disconnected_at {
                let elapsed = now.duration_since(disconnected_at).as_secs();
                if elapsed >= sess.expiry_interval as u64 {
                    expired_ids.push(id.clone());
                }
            }
        }
        for id in &expired_ids {
            sessions.remove(id);
        }
    });

    if expired_ids.is_empty() {
        return;
    }

    let _ = BackgroundWorker::transaction(|| {
        pgrx::spi::Spi::connect_mut(|client| {
            for id in &expired_ids {
                let args: Vec<pgrx::datum::DatumWithOid> = vec![id.as_str().into()];
                client.update(
                    "DELETE FROM pgmqtt_sessions WHERE client_id = $1",
                    None,
                    &args,
                )?;
            }
            Ok::<_, pgrx::spi::Error>(())
        })
    });

    for id in &expired_ids {
        log!(
            "pgmqtt mqtt: session for client '{}' expired, cleaning up",
            id
        );
        subscriptions::remove_client(id);
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
    let mut db_queue_inserts: Vec<(String, i64, Option<i32>)> = Vec::new();

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
            let (pkt_res, db_insert) = with_sessions(|sessions| {
                if let Some(session) = sessions.get_mut(sub_id) {
                    let delivery_qos = std::cmp::min(msg.qos, *granted_qos);
                    if delivery_qos == 1 {
                        if session.inflight.len() >= MAX_INFLIGHT_MESSAGES {
                            // Queue instead of drop to preserve at-least-once semantics.
                            session.queue.push_back(topic_buffer::MqttMessage {
                                id: msg.id,
                                topic: msg.topic.clone(),
                                payload: msg.payload.clone(),
                                qos: delivery_qos,
                            });
                            if session.queue.len() > 10_000 {
                                pgrx::log!(
                                    "pgmqtt: client '{}' queue exceeded 10 000 messages ({}). Consider increasing MAX_INFLIGHT_MESSAGES or investigating client health.",
                                    sub_id,
                                    session.queue.len()
                                );
                            }
                            let ins = msg.id.map(|mid| (sub_id.clone(), mid, None));
                            (None, ins)
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
                            let ins = msg.id.map(|mid| (sub_id.clone(), mid, Some(pid as i32)));
                            (
                                Some(mqtt::build_publish_qos1(&msg.topic, &msg.payload, pid)),
                                ins,
                            )
                        }
                    } else {
                        (
                            Some(mqtt::build_publish_qos0(&msg.topic, &msg.payload)),
                            None,
                        )
                    }
                } else {
                    (None, None)
                }
            });

            if let Some(ins) = db_insert {
                db_queue_inserts.push(ins);
            }

            if let Some(pkt) = pkt_res {
                if let Some(client) = clients.get_mut(sub_id) {
                    if client.transport.write_all(&pkt).is_err() {
                        to_remove.push(sub_id.clone());
                    }
                }
            }
        }
    }

    if !db_queue_inserts.is_empty() {
        let _ = BackgroundWorker::transaction(|| {
            pgrx::spi::Spi::connect_mut(|client| {
                for (cid, mid, pid_opt) in db_queue_inserts {
                    let mut args: Vec<pgrx::datum::DatumWithOid> =
                        vec![cid.as_str().into(), mid.into()];
                    if let Some(pid) = pid_opt {
                        args.push(pid.into());
                        client.update(
                            "INSERT INTO pgmqtt_session_queue (client_id, message_id, packet_id) VALUES ($1, $2, $3)",
                            None,
                            &args,
                        )?;
                    } else {
                        client.update(
                            "INSERT INTO pgmqtt_session_queue (client_id, message_id, packet_id) VALUES ($1, $2, NULL)",
                            None,
                            &args,
                        )?;
                    }
                }
                Ok::<_, pgrx::spi::Error>(())
            })
        });
    }

    for id in to_remove {
        // ... handled in poll loop ...
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
