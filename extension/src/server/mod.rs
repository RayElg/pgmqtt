//! MQTT broker main event loop and client polling.

pub mod db_action;
pub mod session;
pub mod transport;

pub use db_action::{execute_session_db_actions, SessionDbAction};
pub use session::{with_sessions, MqttSession};
pub use transport::Transport;
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
const LATCH_INTERVAL: Duration = Duration::from_millis(80);

/// Timeout for HTTP client read/write operations.
const CLIENT_TIMEOUT: Duration = Duration::from_secs(2);

/// Maximum bytes to read from a client request.
const MAX_REQUEST_BYTES: usize = 65536;

/// Maximum number of unacked QoS 1 messages per client.
const MAX_INFLIGHT_MESSAGES: usize = 800;

/// Threshold for warning when a client's message queue exceeds this size.
const QUEUE_WARNING_THRESHOLD: usize = 10_000;

// Individual db_* functions were refactored into execute_session_db_actions.

fn db_load_sessions_on_startup() {
    BackgroundWorker::transaction(|| {
        let _ = pgrx::spi::Spi::connect(|client| {
            // Check if tables exist
            let table_exists = client
                .select(
                    "SELECT to_regclass('pgmqtt_sessions')::text AS exists_str",
                    None,
                    &[],
                )
                .map(|t| {
                    if let Some(row) = t.into_iter().next() {
                        let val: Option<String> = row.get_by_name("exists_str").ok().flatten();
                        val.is_some()
                    } else {
                        false
                    }
                })
                .unwrap_or(false);

            if !table_exists {
                return Ok::<_, pgrx::spi::Error>(());
            }

            let mut loaded_count = 0;

            // Load sessions
            if let Ok(table) = client.select(
                "SELECT client_id, next_packet_id, expiry_interval, disconnected_at::text \
                 FROM pgmqtt_sessions",
                None,
                &[],
            ) {
                with_sessions(|s| {
                    for row in table {
                        let client_id: String = row
                            .get_by_name("client_id")
                            .ok()
                            .flatten()
                            .unwrap_or_default();
                        let next_pid: i32 = row
                            .get_by_name("next_packet_id")
                            .ok()
                            .flatten()
                            .unwrap_or(1);
                        let expiry: i32 = row
                            .get_by_name("expiry_interval")
                            .ok()
                            .flatten()
                            .unwrap_or(0);
                        let disconnected_str: Option<String> =
                            row.get_by_name("disconnected_at").ok().flatten();

                        let disconnected_at = if disconnected_str.is_some() {
                            Some(std::time::Instant::now()) // Restart expiry timer on broker startup
                        } else {
                            None
                        };

                        let mut sess = MqttSession::new();
                        sess.next_packet_id = next_pid as u16;
                        sess.expiry_interval = expiry as u32;
                        sess.disconnected_at = disconnected_at;

                        s.insert(client_id, sess);
                        loaded_count += 1;
                    }
                });
            }

            if loaded_count > 0 {
                pgrx::log!("pgmqtt: loaded {} sessions from DB", loaded_count);
            }

            // Load messages
            if let Ok(table) = client.select(
                "SELECT m.client_id, m.message_id, m.packet_id, m.sent_at::text, \
                        pm.topic, pm.payload, pm.qos \
                 FROM pgmqtt_session_messages m \
                 JOIN pgmqtt_messages pm ON m.message_id = pm.id \
                 ORDER BY m.created_at ASC",
                None,
                &[],
            ) {
                with_sessions(|s| {
                    let mut msg_count = 0;
                    for row in table {
                        let client_id: String = row
                            .get_by_name("client_id")
                            .ok()
                            .flatten()
                            .unwrap_or_default();
                        let message_id: i64 =
                            row.get_by_name("message_id").ok().flatten().unwrap_or(0);
                        let packet_id_opt: Option<i32> =
                            row.get_by_name("packet_id").ok().flatten();
                        let topic: String =
                            row.get_by_name("topic").ok().flatten().unwrap_or_default();
                        let payload: Option<Vec<u8>> = row.get_by_name("payload").ok().flatten();
                        let qos: i32 = row.get_by_name("qos").ok().flatten().unwrap_or(1);

                        if let Some(sess) = s.get_mut(&client_id) {
                            if let Some(pid) = packet_id_opt {
                                // Inflight
                                sess.inflight.insert(
                                    pid as u16,
                                    (
                                        topic,
                                        payload.unwrap_or_default(),
                                        Some(message_id),
                                        std::time::Instant::now(), // Reset timer
                                    ),
                                );
                            } else {
                                // Queued
                                sess.queue.push_back(topic_buffer::MqttMessage {
                                    id: Some(message_id),
                                    topic,
                                    payload: payload.unwrap_or_default(),
                                    qos: qos as u8,
                                });
                            }
                            msg_count += 1;
                        }
                    }
                    if msg_count > 0 {
                        pgrx::log!(
                            "pgmqtt: loaded {} pending messages into sessions",
                            msg_count
                        );
                    }

                    // Advance next_packet_id past the highest loaded pid to avoid collisions
                    for sess in s.values_mut() {
                        if let Some(&max_pid) = sess.inflight.keys().max() {
                            sess.next_packet_id = if max_pid == 65535 { 1 } else { max_pid + 1 };
                        }
                    }
                });
            }

            // Load subscriptions
            if let Ok(table) = client.select(
                "SELECT client_id, topic_filter, qos FROM pgmqtt_subscriptions",
                None,
                &[],
            ) {
                let mut sub_count = 0;
                for row in table {
                    let client_id: String = row
                        .get_by_name("client_id")
                        .ok()
                        .flatten()
                        .unwrap_or_default();
                    let topic_filter: String = row
                        .get_by_name("topic_filter")
                        .ok()
                        .flatten()
                        .unwrap_or_default();
                    let qos: i32 = row.get_by_name("qos").ok().flatten().unwrap_or(0);

                    subscriptions::subscribe(&client_id, &topic_filter, qos as u8);
                    sub_count += 1;
                }
                if sub_count > 0 {
                    pgrx::log!("pgmqtt: restored {} subscriptions from DB", sub_count);
                }
            }

            Ok::<_, pgrx::spi::Error>(())
        });
    });
}

struct MqttClient {
    transport: Transport,
    client_id: String,
    buf: Vec<u8>,
    will: Option<mqtt::Will>,
    keep_alive: u16,
    last_received_at: std::time::Instant,
    receive_maximum: u16,
}

impl MqttClient {
    fn new(
        transport: Transport,
        client_id: String,
        will: Option<mqtt::Will>,
        keep_alive: u16,
        receive_maximum: u16,
    ) -> Self {
        Self {
            transport,
            client_id,
            buf: Vec::with_capacity(MAX_REQUEST_BYTES),
            will,
            keep_alive,
            last_received_at: std::time::Instant::now(),
            receive_maximum,
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
    db_load_sessions_on_startup();

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
        let mut session_db_actions = Vec::new();

        accept_mqtt_connections(&mqtt_listener, &mut clients, &mut session_db_actions);
        accept_ws_connections(&ws_listener, &mut clients, &mut session_db_actions);

        let mut publishes = Vec::new();
        poll_mqtt_clients(
            &mut clients,
            &mut publishes,
            &mut session_db_actions,
        );

        // ── CDC: load mappings, advance slot, drain ring buffer ──
        cdc_tick(slot_name);

        // Send pending messages to clients
        publish_pending_messages(
            &mut clients,
            &mut publishes,
            &mut session_db_actions,
        );

        // Periodically resend unacked QoS 1 messages
        redeliver_unacked_messages(&mut clients, &mut publishes, &mut session_db_actions);

        publish_messages_batch(publishes, &mut clients);

        // Sweep sessions whose Session Expiry Interval has elapsed
        sweep_expired_sessions(&mut session_db_actions);

        // Execute all collected session DB actions in one transaction
        execute_session_db_actions(session_db_actions);
    }

    // Clean up
    for (id, _) in clients.drain() {
        subscriptions::remove_client(&id);
    }
    log!("pgmqtt mqtt+cdc: shutting down");
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

    // Load topic mappings from DB → in-process cache (at most every 5 s)
    static LAST_MAPPING_LOAD: Mutex<Option<std::time::Instant>> = Mutex::new(None);
    const MAPPING_RELOAD_INTERVAL: Duration = Duration::from_secs(5);

    let should_reload = {
        let last = LAST_MAPPING_LOAD.lock().expect("mapping reload mutex");
        last.map_or(true, |t| t.elapsed() >= MAPPING_RELOAD_INTERVAL)
    };

    if should_reload {
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
    *LAST_MAPPING_LOAD.lock().expect("mapping reload mutex") = Some(std::time::Instant::now());
    } // should_reload

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

                            // Case: no subscribers and not a retained message (CDC events are not retained)
                            // We can skip rendering and persistence entirely.
                            if subscriptions::match_topic(&topic_str).is_empty() {
                                log!(
                                    "pgmqtt cdc: no subscribers for rendered topic '{}', skipping",
                                    topic_str
                                );
                                continue;
                            }

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

        if !batch_ok {
            log!("pgmqtt cdc: batch transaction failed or rolled back — events will be retried");
        }

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
fn accept_mqtt_connections(
    listener: &TcpListener,
    clients: &mut HashMap<String, MqttClient>,
    session_db_actions: &mut Vec<SessionDbAction>,
) {
    loop {
        match listener.accept() {
            Ok((stream, addr)) => {
                log!("pgmqtt mqtt: new TCP connection from {}", addr);
                handle_new_connection(Transport::Raw(stream), clients, session_db_actions);
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
fn accept_ws_connections(
    listener: &TcpListener,
    clients: &mut HashMap<String, MqttClient>,
    session_db_actions: &mut Vec<SessionDbAction>,
) {
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
                        handle_new_connection(Transport::Ws(ws), clients, session_db_actions);
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
fn handle_new_connection(
    mut transport: Transport,
    clients: &mut HashMap<String, MqttClient>,
    session_db_actions: &mut Vec<SessionDbAction>,
) {
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
                finish_connect(transport, connect, clients, session_db_actions);
                return;
            }
            Ok(_) => {
                log!("pgmqtt mqtt: expected CONNECT, got something else");
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
    session_db_actions: &mut Vec<SessionDbAction>,
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
        session_db_actions.push(SessionDbAction::DeleteSession {
            client_id: client_id.clone(),
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

    let (next_pid, expiry) = with_sessions(|s| {
        let sess = s.entry(client_id.clone()).or_insert_with(MqttSession::new);
        // Stamp the new expiry interval from the fresh CONNECT
        sess.expiry_interval = packet.session_expiry_interval;
        // Stamp the receive_maximum from the CONNECT (needed to enforce per-session limits)
        sess.receive_maximum = packet.receive_maximum;
        // Clear the disconnected timestamp since the client is reconnecting
        sess.disconnected_at = None;
        (sess.next_packet_id, sess.expiry_interval)
    });
    session_db_actions.push(SessionDbAction::UpsertSession {
        client_id: client_id.clone(),
        next_packet_id: next_pid,
        expiry_interval: expiry,
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
        MqttClient::new(transport, client_id.clone(), packet.will, packet.keep_alive, packet.receive_maximum),
    );

    // ── Session resumption: redeliver inflight + drain queue ─────────────────
    // MQTT 5.0 §4.4: The broker MUST retransmit all unacknowledged PUBLISH
    // packets (with DUP=1) and deliver any queued messages after a reconnect
    // with session_present=true.
    if session_present {
        // Collect what needs to be sent while holding the session lock.
        let to_send: Vec<Vec<u8>> = with_sessions(|sessions| {
            let mut pkts = Vec::new();
            if let Some(session) = sessions.get_mut(&client_id) {
                // 1. Redeliver inflight messages (DUP=1) so the client can PUBACK them.
                for (pid, (topic, payload, _msg_id, sent_at)) in session.inflight.iter_mut() {
                    pkts.push(mqtt::build_publish_qos1_dup(topic, payload, *pid));
                    // Reset the timer so redeliver_unacked_messages doesn't fire immediately.
                    *sent_at = std::time::Instant::now();
                }
                // 2. Drain the pending queue into inflight and add to send list.
                let inflight_limit = std::cmp::min(MAX_INFLIGHT_MESSAGES, session.receive_maximum as usize);
                while session.inflight.len() < inflight_limit {
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
            pkts
        });

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

struct PendingPublish {
    topic: String,
    payload: Vec<u8>,
    qos: u8,
    retain: bool,
    log_sender: String,
    packet_id: Option<u16>,
}

fn disconnect_client(
    id: &str,
    clients: &mut HashMap<String, MqttClient>,
    pending_publishes: &mut Vec<PendingPublish>,
    session_db_actions: &mut Vec<SessionDbAction>,
) {
    if let Some(mut client) = clients.remove(id) {
        if let Some(will) = client.will.take() {
            log!(
                "pgmqtt mqtt: client '{}' disconnected unexpectedly, buffering Will",
                id
            );
            pending_publishes.push(PendingPublish {
                topic: will.topic,
                payload: will.payload,
                qos: will.qos,
                retain: will.retain,
                log_sender: format!("{} (Will)", id),
                packet_id: None,
            });
        }
    }
    // Mark session as disconnected so the sweeper can reap it later.
    // If expiry_interval == 0 the session should end immediately.
    let (should_remove, mark_disconnected) = with_sessions(|s| {
        let remove = s
            .get(id)
            .map(|sess| sess.expiry_interval == 0)
            .unwrap_or(false);
        if remove {
            s.remove(id);
            subscriptions::remove_client(id);
        } else if let Some(sess) = s.get_mut(id) {
            sess.disconnected_at = Some(std::time::Instant::now());
        }
        (remove, !remove)
    });
    if should_remove {
        session_db_actions.push(SessionDbAction::DeleteSession {
            client_id: id.to_string(),
        });
    } else if mark_disconnected {
        session_db_actions.push(SessionDbAction::MarkDisconnected {
            client_id: id.to_string(),
        });
    }
}

/// Poll all connected clients for incoming MQTT packets.
fn poll_mqtt_clients(
    clients: &mut HashMap<String, MqttClient>,
    pending_publishes: &mut Vec<PendingPublish>,
    session_db_actions: &mut Vec<SessionDbAction>,
) {
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
                    if !handle_mqtt_packet(client, packet, pending_publishes, session_db_actions) {
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
        disconnect_client(&id, clients, pending_publishes, session_db_actions);
    }
}

fn publish_messages_batch(pending: Vec<PendingPublish>, clients: &mut HashMap<String, MqttClient>) {
    if pending.is_empty() {
        return;
    }

    let mut persistent = Vec::new();
    let mut transient = Vec::new();

    for p in pending {
        if p.qos > 0 || p.retain {
            persistent.push(p);
        } else {
            transient.push(p);
        }
    }

    if !persistent.is_empty() {
        let (to_publish, ok) = BackgroundWorker::transaction(|| {
            let mut to_publish = Vec::new();
            pgrx::spi::Spi::connect_mut(|client| {
                for p in &persistent {
                    let payload_arg = if p.payload.is_empty() {
                        None
                    } else {
                        Some(p.payload.as_slice())
                    };

                    let insert_msg = "INSERT INTO pgmqtt_messages (topic, payload, qos, retain) VALUES ($1, $2, $3, $4) RETURNING id";
                    let mut msg_id_opt: Option<i64> = None;

                    let spi_args: Vec<pgrx::datum::DatumWithOid> = vec![
                        p.topic.as_str().into(),
                        payload_arg.into(),
                        (p.qos as i32).into(),
                        p.retain.into(),
                    ];

                    if let Ok(table) = client.update(insert_msg, None, &spi_args) {
                        for row in table {
                            if let Ok(Some(id)) = row.get_by_name::<i64, _>("id") {
                                msg_id_opt = Some(id);
                            }
                            break;
                        }
                    }

                    if p.retain {
                        if p.payload.is_empty() {
                            let args: Vec<pgrx::datum::DatumWithOid> =
                                vec![p.topic.as_str().into()];
                            let _ = client.update(
                                "DELETE FROM pgmqtt_retained WHERE topic = $1",
                                None,
                                &args,
                            );
                        } else if let Some(msg_id) = msg_id_opt {
                            let args: Vec<pgrx::datum::DatumWithOid> =
                                vec![p.topic.as_str().into(), msg_id.into()];
                            let _ = client.update("INSERT INTO pgmqtt_retained (topic, message_id) VALUES ($1, $2) ON CONFLICT (topic) DO UPDATE SET message_id = EXCLUDED.message_id", None, &args);
                        }
                    }

                    log!(
                        "pgmqtt: pushing message from '{}' to topic '{}' with qos={}",
                        p.log_sender,
                        p.topic,
                        p.qos
                    );
                    to_publish.push(topic_buffer::MqttMessage {
                        id: msg_id_opt,
                        topic: p.topic.clone(),
                        payload: p.payload.clone(),
                        qos: p.qos,
                    });
                }
                Ok::<_, pgrx::spi::Error>(())
            })?;
            Ok::<(Vec<topic_buffer::MqttMessage>, bool), pgrx::spi::Error>((to_publish, true))
        })
        .unwrap_or((Vec::new(), false));

        if ok {
            for msg in to_publish {
                topic_buffer::push(msg);
            }

            // Send PUBACKs only after successful commit — MQTT at-least-once semantics.
            for p in persistent {
                if p.qos == 1 {
                    if let Some(pid) = p.packet_id {
                        if let Some(client) = clients.get_mut(&p.log_sender) {
                            let puback = mqtt::build_puback(pid);
                            let _ = client.transport.write_all(&puback);
                        }
                    }
                }
            }
        }
    }

    for p in transient {
        log!(
            "pgmqtt: pushing transient message from '{}' to topic '{}' with qos=0",
            p.log_sender,
            p.topic
        );
        topic_buffer::push(topic_buffer::MqttMessage {
            id: None,
            topic: p.topic,
            payload: p.payload,
            qos: 0,
        });
    }
}

/// Handle a single parsed MQTT packet. Returns `false` if connection should close.
fn handle_mqtt_packet(
    client: &mut MqttClient,
    packet: mqtt::InboundPacket,
    pending_publishes: &mut Vec<PendingPublish>,
    session_db_actions: &mut Vec<SessionDbAction>,
) -> bool {
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
                if let Some(id) = msg_id {
                    session_db_actions.push(SessionDbAction::DeleteMessage {
                        client_id: client_id.clone(),
                        message_id: id,
                    });
                }
            } else {
                log!(
                    "pgmqtt mqtt: '{}' sent PUBACK for unknown packet_id={}",
                    client_id,
                    packet_id
                );
            }
            // Slot freed — promote next queued message into inflight (if within receive_maximum).
            let (next_pkt, db_action) = with_sessions(|sessions| {
                if let Some(session) = sessions.get_mut(&client_id) {
                    let inflight_limit = std::cmp::min(MAX_INFLIGHT_MESSAGES, session.receive_maximum as usize);
                    // Only promote if there's space and a queued message
                    if session.inflight.len() < inflight_limit && !session.queue.is_empty() {
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
                            (
                                Some(mqtt::build_publish_qos1(
                                    &queued.topic,
                                    &queued.payload,
                                    pid,
                                )),
                                queued.id.map(|id| (id, pid)),
                            )
                        } else {
                            (None, None)
                        }
                    } else {
                        (None, None)
                    }
                } else {
                    (None, None)
                }
            });

            if let Some((msg_id, pid)) = db_action {
                session_db_actions.push(SessionDbAction::UpdateMessageInflight {
                    client_id: client_id.clone(),
                    message_id: msg_id,
                    packet_id: pid,
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
                if granted <= 0x02 {
                    // Success (QoS 0, 1, or 2)
                    session_db_actions.push(SessionDbAction::InsertSubscription {
                        client_id: client_id.clone(),
                        topic_filter: topic_filter.clone(),
                        qos: granted,
                    });
                }
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
                    session_db_actions.push(SessionDbAction::DeleteSubscription {
                        client_id: client_id.clone(),
                        topic_filter: topic_filter.clone(),
                    });
                    mqtt::reason::SUCCESS
                } else {
                    mqtt::reason::NO_SUBSCRIPTION_EXISTED
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
        mqtt::InboundPacket::Disconnect(reason_code, expiry_override) => {
            log!(
                "pgmqtt mqtt: '{}' sent DISCONNECT (reason_code=0x{:02x})",
                client_id,
                reason_code
            );
            if reason_code == mqtt::reason::NORMAL_DISCONNECT {
                client.will = None;
            }
            // MQTT 5.0 §3.14.2.2.2: client may override Session Expiry Interval at DISCONNECT
            if let Some(new_expiry) = expiry_override {
                with_sessions(|sessions| {
                    if let Some(sess) = sessions.get_mut(&client_id) {
                        sess.expiry_interval = new_expiry;
                    }
                });
                session_db_actions.push(SessionDbAction::UpsertSession {
                    client_id: client_id.clone(),
                    next_packet_id: 0, // will be overwritten by the MarkDisconnected path
                    expiry_interval: new_expiry,
                });
            }
            false // signal removal
        }
        mqtt::InboundPacket::Publish(pub_pkt) => {
            // Optimization: skip all processing if no one is listening (and not retained)
            // But for QoS 1, we still owe the client a PUBACK.
            let subscriber_ids = subscriptions::match_topic(&pub_pkt.topic);
            if !pub_pkt.retain && subscriber_ids.is_empty() {
                if pub_pkt.qos == 1 {
                    if let Some(pid) = pub_pkt.packet_id {
                        log!(
                            "pgmqtt mqtt: '{}' published to '{}' (QoS 1) with no subscribers. Sending PUBACK and skipping persistence.",
                            client_id,
                            pub_pkt.topic
                        );
                        let puback = mqtt::build_puback(pid);
                        let _ = transport.write_all(&puback);
                    }
                } else {
                    log!(
                        "pgmqtt mqtt: '{}' published to '{}' (QoS 0) with no subscribers. Skipping.",
                        client_id,
                        pub_pkt.topic
                    );
                }
                return true;
            }

            pending_publishes.push(PendingPublish {
                topic: pub_pkt.topic,
                payload: pub_pkt.payload,
                qos: pub_pkt.qos,
                retain: pub_pkt.retain,
                log_sender: client_id,
                packet_id: pub_pkt.packet_id,
            });
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
fn sweep_expired_sessions(session_db_actions: &mut Vec<SessionDbAction>) {
    let now = std::time::Instant::now();
    let mut expired_ids: Vec<String> = Vec::new();
    with_sessions(|sessions| {
        for (id, sess) in sessions.iter() {
            if let Some(disconnected_at) = sess.disconnected_at {
                let elapsed = now
                    .checked_duration_since(disconnected_at)
                    .map(|d| d.as_secs())
                    .unwrap_or(0);
                if elapsed >= sess.expiry_interval as u64 {
                    expired_ids.push(id.clone());
                }
            }
        }
        for id in &expired_ids {
            sessions.remove(id);
        }
    });
    for id in &expired_ids {
        log!(
            "pgmqtt mqtt: session for client '{}' expired, cleaning up",
            id
        );
        subscriptions::remove_client(id);
        session_db_actions.push(SessionDbAction::DeleteSession {
            client_id: id.clone(),
        });
    }
}

/// Drain the topic_buffer and publish messages to matching subscribers.
fn publish_pending_messages(
    clients: &mut HashMap<String, MqttClient>,
    pending_publishes: &mut Vec<PendingPublish>,
    session_db_actions: &mut Vec<SessionDbAction>,
) {
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

        // Batch database actions by message_id to minimize writes
        let mut batch_entries: Vec<(String, Option<u16>)> = Vec::new();

        for (sub_id, granted_qos) in &subscriber_ids {
            let receive_maximum = clients.get(sub_id).map(|c| c.receive_maximum).unwrap_or(65535);
            let inflight_limit = std::cmp::min(MAX_INFLIGHT_MESSAGES, receive_maximum as usize);

            enum DbAction {
                None,
                Queue,
                Inflight(u16),
            }
            let (pkt_res, db_action) = with_sessions(|sessions| {
                if let Some(session) = sessions.get_mut(sub_id) {
                    let delivery_qos = std::cmp::min(msg.qos, *granted_qos);
                    if delivery_qos == 1 {
                        if session.inflight.len() >= inflight_limit {
                            // Queue instead of drop to preserve at-least-once semantics.
                            session.queue.push_back(topic_buffer::MqttMessage {
                                id: msg.id,
                                topic: msg.topic.clone(),
                                payload: msg.payload.clone(),
                                qos: delivery_qos,
                            });
                            if session.queue.len() > QUEUE_WARNING_THRESHOLD {
                                pgrx::log!(
                                    "pgmqtt: client '{}' queue exceeded {} messages ({}). Consider increasing MAX_INFLIGHT_MESSAGES or investigating client health.",
                                    sub_id,
                                    QUEUE_WARNING_THRESHOLD,
                                    session.queue.len()
                                );
                            }
                            (None, DbAction::Queue)
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
                            (
                                Some(mqtt::build_publish_qos1(&msg.topic, &msg.payload, pid)),
                                DbAction::Inflight(pid),
                            )
                        }
                    } else {
                        (
                            Some(mqtt::build_publish_qos0(&msg.topic, &msg.payload)),
                            DbAction::None,
                        )
                    }
                } else {
                    (None, DbAction::None)
                }
            });

            match db_action {
                DbAction::Queue => {
                    if let Some(msg_id) = msg.id {
                        batch_entries.push((sub_id.clone(), None));
                    }
                }
                DbAction::Inflight(pid) => {
                    if let Some(msg_id) = msg.id {
                        batch_entries.push((sub_id.clone(), Some(pid)));
                    }
                }
                DbAction::None => {}
            }

            if let Some(pkt) = pkt_res {
                if let Some(client) = clients.get_mut(sub_id) {
                    if client.transport.write_all(&pkt).is_err() {
                        to_remove.push(sub_id.clone());
                    }
                }
            }
        }

        // Execute batch insert if there are entries for this message
        if !batch_entries.is_empty() {
            if let Some(msg_id) = msg.id {
                session_db_actions.push(SessionDbAction::InsertMessageBatch {
                    message_id: msg_id,
                    entries: batch_entries,
                });
            }
        }
    }

    for id in to_remove {
        disconnect_client(&id, clients, pending_publishes, session_db_actions);
    }
}

/// Periodic check for unacked QoS 1 messages and redelivery.
fn redeliver_unacked_messages(
    clients: &mut HashMap<String, MqttClient>,
    pending_publishes: &mut Vec<PendingPublish>,
    session_db_actions: &mut Vec<SessionDbAction>,
) {
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

    let mut to_remove = Vec::new();
    for (cid, pid, topic, payload) in to_resend {
        if let Some(client) = clients.get_mut(&cid) {
            log!("pgmqtt mqtt: redelivering packet_id={} to '{}'", pid, cid);
            let pkt = mqtt::build_publish_qos1_dup(&topic, &payload, pid);
            if client.transport.write_all(&pkt).is_err() {
                to_remove.push(cid);
            }
        }
    }

    for id in to_remove {
        disconnect_client(&id, clients, pending_publishes, session_db_actions);
    }
}
