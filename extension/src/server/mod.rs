//! MQTT broker main event loop and client polling.

pub mod db_action;
pub mod session;
pub mod transport;

pub use db_action::{execute_session_db_actions, SessionDbAction};
pub use session::{with_sessions, MqttSession};
pub use transport::Transport;
use crate::inbound_map;
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
use std::sync::Arc;
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

/// Hard cap on the per-client pending queue.  Clients that exceed this are
/// disconnected to prevent unbounded memory growth inside the PostgreSQL process.
const MAX_QUEUE_SIZE: usize = 50_000;

// Individual db_* functions were refactored into execute_session_db_actions.

/// On startup, mark all sessions that have no `disconnected_at` as disconnected now.
///
/// After a crash, sessions keep `disconnected_at = NULL` because the broker never
/// reached the normal disconnect path. This causes two problems:
///   1. `pgmqtt_status()` reports them as active connections indefinitely.
///   2. Session expiry timers never start, so stale sessions accumulate forever.
///
/// Setting `disconnected_at = now()` for all NULL-disconnected sessions fixes both:
/// the status view is accurate, and expiry timers begin from broker restart.
/// `db_load_sessions_on_startup` then reads the updated timestamps and
/// correctly restores in-memory expiry state.
fn db_mark_sessions_disconnected_on_startup() {
    BackgroundWorker::transaction(|| {
        let _ = pgrx::spi::Spi::connect_mut(|client| {
            let _ = client.update(
                "UPDATE pgmqtt_sessions SET disconnected_at = now() WHERE disconnected_at IS NULL",
                None,
                &[],
            );
            Ok::<_, pgrx::spi::Error>(())
        });
    });
}

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

/// Load inbound mappings from pgmqtt_inbound_mappings into the in-memory cache.
fn load_inbound_mappings() {
    BackgroundWorker::transaction(|| {
        let _ = pgrx::spi::Spi::connect(|client| {
            let table_exists = client
                .select(
                    "SELECT to_regclass('pgmqtt_inbound_mappings')::text",
                    None,
                    &[],
                )?
                .first()
                .get_one::<String>()?
                .is_some();

            if !table_exists {
                inbound_map::set_mappings(Vec::new());
                return Ok::<_, pgrx::spi::Error>(());
            }

            let mut mappings = Vec::new();
            if let Ok(table) = client.select(
                "SELECT mapping_name, topic_pattern, target_schema, target_table, \
                        column_map::text, op, conflict_columns \
                 FROM pgmqtt_inbound_mappings",
                None,
                &[],
            ) {
                for row in table {
                    let mn: String = row.get_by_name("mapping_name").ok().flatten().unwrap_or_default();
                    let tp: String = row.get_by_name("topic_pattern").ok().flatten().unwrap_or_default();
                    let ts: String = row.get_by_name("target_schema").ok().flatten().unwrap_or_else(|| "public".to_string());
                    let tt: String = row.get_by_name("target_table").ok().flatten().unwrap_or_default();
                    let cm_str: String = row.get_by_name("column_map").ok().flatten().unwrap_or_else(|| "{}".to_string());
                    let op_str: String = row.get_by_name("op").ok().flatten().unwrap_or_else(|| "insert".to_string());
                    let cc: Option<Vec<String>> = row.get_by_name("conflict_columns").ok().flatten();

                    // Parse the mapping at load time
                    let segments = match inbound_map::parse_pattern(&tp) {
                        Ok(s) => s,
                        Err(e) => {
                            log!("pgmqtt: skipping inbound mapping '{}': invalid pattern: {}", mn, e);
                            continue;
                        }
                    };

                    let inbound_op = match inbound_map::InboundOp::from_str(&op_str) {
                        Ok(o) => o,
                        Err(e) => {
                            log!("pgmqtt: skipping inbound mapping '{}': {}", mn, e);
                            continue;
                        }
                    };

                    // Parse column_map JSON
                    let cm_json: serde_json::Value = match serde_json::from_str(&cm_str) {
                        Ok(v) => v,
                        Err(e) => {
                            log!("pgmqtt: skipping inbound mapping '{}': invalid column_map JSON: {}", mn, e);
                            continue;
                        }
                    };

                    let map_obj = match cm_json.as_object() {
                        Some(obj) => obj,
                        None => {
                            log!("pgmqtt: skipping inbound mapping '{}': column_map is not an object", mn);
                            continue;
                        }
                    };

                    let mut parsed_columns = Vec::new();
                    let mut parse_ok = true;
                    for (col_name, expr_val) in map_obj {
                        if let Some(expr) = expr_val.as_str() {
                            match inbound_map::parse_column_source(expr) {
                                Ok(s) => parsed_columns.push((col_name.clone(), s)),
                                Err(e) => {
                                    log!("pgmqtt: skipping inbound mapping '{}': column '{}': {}", mn, col_name, e);
                                    parse_ok = false;
                                    break;
                                }
                            }
                        }
                    }
                    if !parse_ok {
                        continue;
                    }

                    let col_names: Vec<String> = parsed_columns.iter().map(|(n, _)| n.clone()).collect();
                    let sql = inbound_map::generate_sql(
                        &ts,
                        &tt,
                        &col_names,
                        &inbound_op,
                        cc.as_deref(),
                    );

                    mappings.push(inbound_map::InboundMapping {
                        mapping_name: Arc::from(mn.as_str()),
                        pattern_segments: segments,
                        target_schema: ts,
                        target_table: tt,
                        column_map: parsed_columns,
                        op: inbound_op,
                        conflict_columns: cc,
                        sql: Arc::from(sql.as_str()),
                        topic_pattern: tp,
                        template_type: "jsonpath".to_string(),
                    });
                }
            }

            let count = mappings.len();
            inbound_map::set_mappings(mappings);
            if count > 0 {
                log!("pgmqtt: loaded {} inbound mappings", count);
            }
            Ok::<_, pgrx::spi::Error>(())
        });
    });
}

/// Execute a batch of inbound table writes in a single transaction.
/// Message tracking (pgmqtt_messages), PUBACKs, and subscriber delivery are
/// handled by the normal publish_messages_batch path — this function only
/// performs the downstream table writes.
fn execute_inbound_writes(writes: Vec<inbound_map::PendingInboundWrite>) {
    if writes.is_empty() {
        return;
    }

    let write_count = writes.len();

    BackgroundWorker::transaction(|| {
        let result = pgrx::spi::Spi::connect_mut(|client| {
            for write in &writes {
                let spi_args: Vec<pgrx::datum::DatumWithOid> = write.args
                    .iter()
                    .map(|a| {
                        let opt: Option<&str> = a.as_deref();
                        opt.into()
                    })
                    .collect();
                client.update(&*write.sql, None, &spi_args)?;
            }
            Ok::<_, pgrx::spi::Error>(())
        });
        match result {
            Ok(_) => {
                if write_count > 0 {
                    log!("pgmqtt inbound: committed {} writes", write_count);
                }
            }
            Err(e) => {
                log!(
                    "pgmqtt inbound: transaction failed for {} writes: {:?}",
                    write_count,
                    e
                );
            }
        }
    });
}

/// Virtual subscriber: process QoS 1 inbound-pending messages.
///
/// Reads from pgmqtt_inbound_pending (joined with pgmqtt_messages),
/// re-matches against inbound mappings to get SQL + args, executes the
/// table write, and on success removes the pending row and cleans up
/// the message if no other references remain.
///
/// Each pending row is processed in its own transaction so a single
/// failure doesn't block the rest of the batch.
fn process_inbound_pending() {
    const BATCH_SIZE: i64 = 50;
    const MAX_RETRIES: i32 = 10;

    // Step 1: read a batch of pending rows (read-only transaction)
    struct PendingRow {
        message_id: i64,
        mapping_name: String,
        retry_count: i32,
        topic: String,
        payload: Vec<u8>,
    }

    let rows: Vec<PendingRow> = BackgroundWorker::transaction(|| {
        pgrx::spi::Spi::connect(|client| {
            let mut out = Vec::new();
            if let Ok(table) = client.select(
                "SELECT p.message_id, p.mapping_name, p.retry_count, \
                        m.topic, m.payload \
                 FROM pgmqtt_inbound_pending p \
                 JOIN pgmqtt_messages m ON p.message_id = m.id \
                 WHERE p.next_retry_at <= now() \
                 ORDER BY p.next_retry_at ASC \
                 LIMIT $1",
                None,
                &[BATCH_SIZE.into()],
            ) {
                for row in table {
                    let message_id: i64 = row.get_by_name("message_id").ok().flatten().unwrap_or(0);
                    let mapping_name: String = row.get_by_name("mapping_name").ok().flatten().unwrap_or_default();
                    let retry_count: i32 = row.get_by_name("retry_count").ok().flatten().unwrap_or(0);
                    let topic: String = row.get_by_name("topic").ok().flatten().unwrap_or_default();
                    let payload: Option<Vec<u8>> = row.get_by_name("payload").ok().flatten();
                    out.push(PendingRow {
                        message_id,
                        mapping_name,
                        retry_count,
                        topic,
                        payload: payload.unwrap_or_default(),
                    });
                }
            }
            Ok::<_, pgrx::spi::Error>(out)
        })
    })
    .unwrap_or_default();

    if rows.is_empty() {
        return;
    }

    // Step 2: process each row in its own transaction
    for pending in &rows {
        let matches = inbound_map::try_match(&pending.topic, &pending.payload);
        let target_match = matches.into_iter()
            .find(|(_, m)| m.mapping_name.as_ref() == pending.mapping_name);

        match target_match {
            Some((_, match_result)) => {
                // Attempt the table write
                let write_ok = BackgroundWorker::transaction(|| {
                    pgrx::spi::Spi::connect_mut(|client| {
                        let spi_args: Vec<pgrx::datum::DatumWithOid> = match_result.values
                            .iter()
                            .map(|a| {
                                let opt: Option<&str> = a.as_deref();
                                opt.into()
                            })
                            .collect();
                        client.update(&*match_result.sql, None, &spi_args)?;

                        // Success: remove pending row and clean up message
                        client.update(
                            "DELETE FROM pgmqtt_inbound_pending \
                             WHERE message_id = $1 AND mapping_name = $2",
                            None,
                            &[pending.message_id.into(), pending.mapping_name.as_str().into()],
                        )?;
                        client.update(
                            "DELETE FROM pgmqtt_messages \
                             WHERE id = $1 AND retain = false \
                               AND NOT EXISTS (SELECT 1 FROM pgmqtt_session_messages WHERE message_id = $1) \
                               AND NOT EXISTS (SELECT 1 FROM pgmqtt_inbound_pending WHERE message_id = $1)",
                            None,
                            &[pending.message_id.into()],
                        )?;
                        Ok::<_, pgrx::spi::Error>(())
                    })
                });

                if write_ok.is_ok() {
                    log!(
                        "pgmqtt inbound: processed message {} for mapping '{}'",
                        pending.message_id,
                        pending.mapping_name,
                    );
                } else {
                    // Write failed — classify and handle
                    let err_msg = format!("{:?}", write_ok.err().unwrap());
                    handle_inbound_failure(pending.message_id, &pending.mapping_name,
                        pending.retry_count, &err_msg, MAX_RETRIES,
                        &pending.topic, &pending.payload);
                }
            }
            None => {
                // Mapping was removed since the message was published
                handle_inbound_failure(pending.message_id, &pending.mapping_name,
                    pending.retry_count, "mapping no longer exists", MAX_RETRIES,
                    &pending.topic, &pending.payload);
            }
        }
    }

    log!("pgmqtt inbound: processed {} pending rows", rows.len());
}

/// Classify an inbound write failure and either retry or dead-letter.
fn handle_inbound_failure(
    message_id: i64,
    mapping_name: &str,
    retry_count: i32,
    error_msg: &str,
    max_retries: i32,
    topic: &str,
    payload: &[u8],
) {
    let retryable = is_retryable_error(error_msg);
    let should_dead_letter = !retryable || retry_count >= max_retries;

    if should_dead_letter {
        log!(
            "pgmqtt inbound: dead-lettering message {} for mapping '{}': {}",
            message_id, mapping_name, error_msg
        );
        BackgroundWorker::transaction(|| {
            let _ = pgrx::spi::Spi::connect_mut(|client| {
                let payload_arg: Option<&[u8]> = if payload.is_empty() { None } else { Some(payload) };
                client.update(
                    "INSERT INTO pgmqtt_dead_letters \
                        (original_message_id, topic, payload, mapping_name, error_message, retry_count) \
                     VALUES ($1, $2, $3, $4, $5, $6)",
                    None,
                    &[
                        message_id.into(),
                        topic.into(),
                        payload_arg.into(),
                        mapping_name.into(),
                        error_msg.into(),
                        retry_count.into(),
                    ],
                )?;
                client.update(
                    "DELETE FROM pgmqtt_inbound_pending \
                     WHERE message_id = $1 AND mapping_name = $2",
                    None,
                    &[message_id.into(), mapping_name.into()],
                )?;
                client.update(
                    "DELETE FROM pgmqtt_messages \
                     WHERE id = $1 AND retain = false \
                       AND NOT EXISTS (SELECT 1 FROM pgmqtt_session_messages WHERE message_id = $1) \
                       AND NOT EXISTS (SELECT 1 FROM pgmqtt_inbound_pending WHERE message_id = $1)",
                    None,
                    &[message_id.into()],
                )?;
                Ok::<_, pgrx::spi::Error>(())
            });
        });
    } else {
        log!(
            "pgmqtt inbound: retry {}/{} for message {} mapping '{}': {}",
            retry_count + 1, max_retries, message_id, mapping_name, error_msg
        );
        BackgroundWorker::transaction(|| {
            let _ = pgrx::spi::Spi::connect_mut(|client| {
                // Exponential backoff: 1s, 2s, 4s, ... capped at 256s
                client.update(
                    "UPDATE pgmqtt_inbound_pending \
                     SET retry_count = retry_count + 1, \
                         last_error = $3, \
                         next_retry_at = now() + (interval '1 second' * power(2, LEAST($4, 8))) \
                     WHERE message_id = $1 AND mapping_name = $2",
                    None,
                    &[
                        message_id.into(),
                        mapping_name.into(),
                        error_msg.into(),
                        retry_count.into(),
                    ],
                )?;
                Ok::<_, pgrx::spi::Error>(())
            });
        });
    }
}

/// Classify whether an inbound write error is retryable.
fn is_retryable_error(err_msg: &str) -> bool {
    let non_retryable = [
        "does not exist",
        "permission denied",
        "invalid input syntax",
        "violates not-null constraint",
        "mapping no longer exists",
    ];
    !non_retryable.iter().any(|pat| err_msg.contains(pat))
}

struct MqttClient {
    transport: Transport,
    client_id: String,
    buf: Vec<u8>,
    will: Option<mqtt::Will>,
    keep_alive: u16,
    last_received_at: std::time::Instant,
    receive_maximum: u16,
    /// JWT claim-based topic filters for subscribe authorization.
    sub_claims: Vec<String>,
    /// JWT claim-based topic filters for publish authorization.
    pub_claims: Vec<String>,
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
            sub_claims: Vec::new(),
            pub_claims: Vec::new(),
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
            unsafe { pgrx::pg_sys::ProcessConfigFile(pgrx::pg_sys::GucContext::PGC_SIGHUP); }
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
pub fn run_mqtt_cdc(ports: crate::PortConfig, slot_name: &str) {
    db_mark_sessions_disconnected_on_startup();
    db_load_sessions_on_startup();
    load_inbound_mappings();

    // Bind MQTT TCP listener (optional)
    let mqtt_listener = if ports.mqtt_enabled {
        let addr = format!("0.0.0.0:{}", ports.mqtt_port);
        match TcpListener::bind(&addr) {
            Ok(l) => {
                if let Err(e) = l.set_nonblocking(true) {
                    log!("pgmqtt mqtt: failed to set non-blocking: {}", e);
                    return;
                }
                log!("pgmqtt mqtt: listening on {} (raw TCP)", addr);
                Some(l)
            }
            Err(e) => {
                log!("pgmqtt mqtt: failed to bind {}: {}", addr, e);
                return;
            }
        }
    } else {
        log!("pgmqtt mqtt: TCP listener disabled");
        None
    };

    // Bind WebSocket listener (optional)
    let ws_listener = if ports.ws_enabled {
        let addr = format!("0.0.0.0:{}", ports.ws_port);
        match TcpListener::bind(&addr) {
            Ok(l) => {
                if let Err(e) = l.set_nonblocking(true) {
                    log!("pgmqtt ws: failed to set non-blocking: {}", e);
                    return;
                }
                log!("pgmqtt ws: listening on {} (WebSocket)", addr);
                Some(l)
            }
            Err(e) => {
                log!("pgmqtt ws: failed to bind {}: {}", addr, e);
                return;
            }
        }
    } else {
        log!("pgmqtt ws: WebSocket listener disabled");
        None
    };

    // Bind MQTT TLS listener (optional)
    let mqtts_listener = if ports.mqtts_enabled {
        let addr = format!("0.0.0.0:{}", ports.mqtts_port);
        match TcpListener::bind(&addr) {
            Ok(l) => {
                if let Err(e) = l.set_nonblocking(true) {
                    log!("pgmqtt mqtts: failed to set non-blocking: {}", e);
                    return;
                }
                log!("pgmqtt mqtts: listening on {} (TLS)", addr);
                Some(l)
            }
            Err(e) => {
                log!("pgmqtt mqtts: failed to bind {}: {}", addr, e);
                return;
            }
        }
    } else {
        log!("pgmqtt mqtts: TLS listener disabled");
        None
    };

    // Bind WSS listener (optional)
    let wss_listener = if ports.wss_enabled {
        let addr = format!("0.0.0.0:{}", ports.wss_port);
        match TcpListener::bind(&addr) {
            Ok(l) => {
                if let Err(e) = l.set_nonblocking(true) {
                    log!("pgmqtt wss: failed to set non-blocking: {}", e);
                    return;
                }
                log!("pgmqtt wss: listening on {} (WSS)", addr);
                Some(l)
            }
            Err(e) => {
                log!("pgmqtt wss: failed to bind {}: {}", addr, e);
                return;
            }
        }
    } else {
        log!("pgmqtt wss: WSS listener disabled");
        None
    };

    // Load TLS configuration if any secure listener is enabled
    let tls_config = if ports.mqtts_enabled || ports.wss_enabled {
        match build_tls_config(&ports.tls_cert_file, &ports.tls_key_file) {
            Some(cfg) => Some(cfg),
            None => {
                log!(
                    "pgmqtt tls: failed to load TLS config (cert='{}', key='{}') — \
                     check that tls_cert_file and tls_key_file are set to valid PEM files \
                     readable by the postgres process; aborting",
                    ports.tls_cert_file,
                    ports.tls_key_file,
                );
                return;
            }
        }
    } else {
        None
    };

    // client_id → MqttClient
    let mut clients: HashMap<String, MqttClient> = HashMap::new();
    while BackgroundWorker::wait_latch(Some(LATCH_INTERVAL)) {
        if BackgroundWorker::sighup_received() {
            log!("pgmqtt mqtt+cdc: SIGHUP received");
            unsafe { pgrx::pg_sys::ProcessConfigFile(pgrx::pg_sys::GucContext::PGC_SIGHUP); }
        }

        // Reload inbound mappings every tick for synchronous pickup (~80ms)
        load_inbound_mappings();

        // ── MQTT: accept raw TCP, accept WebSocket, poll ──
        let mut session_db_actions = Vec::new();
        let mut pending_inbound_writes: Vec<inbound_map::PendingInboundWrite> = Vec::new();

        if let Some(ref listener) = mqtt_listener {
            accept_mqtt_connections(listener, &mut clients, &mut session_db_actions);
        }
        if let Some(ref listener) = ws_listener {
            accept_ws_connections(listener, &mut clients, &mut session_db_actions);
        }
        if let Some(ref listener) = mqtts_listener {
            if let Some(ref cfg) = tls_config {
                accept_mqtts_connections(listener, cfg, &mut clients, &mut session_db_actions);
            }
        }
        if let Some(ref listener) = wss_listener {
            if let Some(ref cfg) = tls_config {
                accept_wss_connections(listener, cfg, &mut clients, &mut session_db_actions);
            }
        }

        let mut publishes = Vec::new();
        poll_mqtt_clients(
            &mut clients,
            &mut publishes,
            &mut session_db_actions,
            &mut pending_inbound_writes,
        );

        // Execute inbound writes (MQTT → PostgreSQL) before CDC and message delivery
        execute_inbound_writes(pending_inbound_writes);

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

        // Virtual subscriber: process QoS 1 inbound-pending messages
        process_inbound_pending();

        // Sweep sessions whose Session Expiry Interval has elapsed
        sweep_expired_sessions(&mut session_db_actions);

        // Execute all collected session DB actions in one transaction
        execute_session_db_actions(session_db_actions);

    }

    // Graceful shutdown: send DISCONNECT to all clients, fire will messages,
    // and persist session state so reconnecting clients find correct disconnected_at.
    log!("pgmqtt mqtt+cdc: SIGTERM received, shutting down gracefully");
    let mut shutdown_db_actions = Vec::new();
    let mut will_publishes = Vec::new();

    for (id, mut client) in clients.drain() {
        // MQTT 5.0 §3.14: server MUST send DISCONNECT before closing the network connection.
        let _ = client.transport.write_all(
            &mqtt::build_disconnect(mqtt::reason::SERVER_SHUTTING_DOWN),
        );

        // Server-initiated disconnect triggers the will message (MQTT 5.0 §3.1.3.3).
        // The client did NOT send a normal DISCONNECT, so the will fires.
        if let Some(will) = client.will.take() {
            log!("pgmqtt mqtt: firing Will for '{}' on shutdown", id);
            will_publishes.push(PendingPublish {
                topic: will.topic,
                payload: will.payload,
                qos: will.qos,
                retain: will.retain,
                log_sender: format!("{} (Will/shutdown)", id),
                packet_id: None,
                inbound_mappings: Vec::new(),
            });
        }

        // Remove in-memory subscriptions and mark session state.
        subscriptions::remove_client(&id);
        let (should_remove, mark_disc) = with_sessions(|s| {
            let remove = s
                .get(&id)
                .map(|sess| sess.expiry_interval == 0)
                .unwrap_or(false);
            if remove {
                s.remove(&id);
            } else if let Some(sess) = s.get_mut(&id) {
                sess.disconnected_at = Some(std::time::Instant::now());
            }
            (remove, !remove)
        });
        if should_remove {
            shutdown_db_actions.push(SessionDbAction::DeleteSession { client_id: id });
        } else if mark_disc {
            shutdown_db_actions.push(SessionDbAction::MarkDisconnected { client_id: id });
        }
    }

    // Persist will messages (QoS ≥ 1 wills land in pgmqtt_messages so reconnecting
    // subscribers receive them; QoS 0 wills are fire-and-forget as per spec).
    if !will_publishes.is_empty() {
        let mut no_clients: HashMap<String, MqttClient> = HashMap::new();
        publish_messages_batch(will_publishes, &mut no_clients);
    }

    // Flush session disconnect state to DB.
    execute_session_db_actions(shutdown_db_actions);

    log!("pgmqtt mqtt+cdc: shutdown complete");
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

    // ── Startup: load mapping cache from pgmqtt_slot_mappings ────────────────
    //
    // pgmqtt_slot_mappings is the WAL-synchronized checkpoint: it is updated
    // atomically inside the same BackgroundWorker::transaction that advances
    // the slot LSN.  Loading from it on restart gives us the mapping state
    // exactly at confirmed_flush_lsn — never a "future" version.
    //
    // On a fresh slot (confirmed_flush_lsn IS NULL — nothing consumed yet) we
    // bootstrap by copying pgmqtt_topic_mappings → pgmqtt_slot_mappings once,
    // since pre-slot mapping rows will never appear as WAL events.
    if topic_map::get().is_none() {
        BackgroundWorker::transaction(|| {
            // Detect whether the slot has ever consumed data.
            let is_fresh = Spi::connect(|client| {
                let lsn: Option<String> = client
                    .select(
                        "SELECT confirmed_flush_lsn::text FROM pg_replication_slots \
                         WHERE slot_name = $1",
                        None,
                        &[slot_name.into()],
                    )?
                    .first()
                    .get_one::<String>()?;
                Ok::<bool, spi::Error>(lsn.is_none())
            })
            .unwrap_or(false);

            if is_fresh {
                // Bootstrap: copy current user-facing table into the slot checkpoint.
                let _ = Spi::run(
                    "INSERT INTO pgmqtt_slot_mappings \
                         SELECT schema_name, table_name, mapping_name, \
                                topic_template, payload_template, qos, template_type \
                         FROM pgmqtt_topic_mappings \
                         ON CONFLICT DO NOTHING",
                );
                log!("pgmqtt: fresh slot — bootstrapped slot mappings from pgmqtt_topic_mappings");
            }

            // Load from the checkpoint into the in-process cache.
            if let Ok(mappings) = Spi::connect(|client| {
                let mut rows = Vec::new();
                if let Ok(table) = client.select(
                    "SELECT schema_name, table_name, mapping_name, \
                            topic_template, payload_template, qos, template_type \
                     FROM pgmqtt_slot_mappings",
                    None,
                    &[],
                ) {
                    for row in table {
                        let s: String = row.get_by_name("schema_name").ok().flatten().unwrap_or_default();
                        let t: String = row.get_by_name("table_name").ok().flatten().unwrap_or_default();
                        let mn: String = row.get_by_name("mapping_name").ok().flatten().unwrap_or_else(|| "default".to_string());
                        let tt: String = row.get_by_name("topic_template").ok().flatten().unwrap_or_default();
                        let pt: String = row.get_by_name("payload_template").ok().flatten().unwrap_or_default();
                        let q: i32 = row.get_by_name("qos").ok().flatten().unwrap_or_default();
                        let tmpl: String = row.get_by_name("template_type").ok().flatten().unwrap_or_else(|| "jinja2".to_string());
                        rows.push(topic_map::TopicMapping {
                            name: mn, schema: s, table: t,
                            topic_template: tt, payload_template: pt, qos: q as u8,
                            template_type: tmpl,
                        });
                    }
                }
                Ok::<_, spi::Error>(rows)
            }) {
                let count = mappings.len();
                topic_map::set_mappings(mappings);
                log!("pgmqtt: loaded {} topic mappings from slot checkpoint", count);
            }
        });
    }

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

                // ── Step 2: drain ring_buffer; process events in WAL order ──────
                //
                // MappingUpdate events apply mapping deltas both to the in-process
                // cache and to pgmqtt_slot_mappings within this transaction, so the
                // checkpoint stays atomically consistent with confirmed_flush_lsn.
                let events = ring_buffer::drain();

                for event in &events {
                    match event {
                        ring_buffer::RingEvent::MappingUpdate { op, columns } => {
                            // Helper to pull a column value by name.
                            let col = |name: &str| -> String {
                                columns
                                    .iter()
                                    .find(|(k, _)| k == name)
                                    .map(|(_, v)| v.clone())
                                    .unwrap_or_default()
                            };
                            let schema = col("schema_name");
                            let table = col("table_name");
                            let name = col("mapping_name");

                            if *op == "DELETE" {
                                topic_map::wal_remove(&schema, &table, &name);
                                let _ = pgrx::spi::Spi::connect_mut(|client| {
                                    client.update(
                                        "DELETE FROM pgmqtt_slot_mappings \
                                         WHERE schema_name = $1 AND table_name = $2 AND mapping_name = $3",
                                        None,
                                        &[schema.as_str().into(), table.as_str().into(), name.as_str().into()],
                                    ).map(|_| ())
                                });
                                log!("pgmqtt: WAL mapping DELETE {}.{} ({})", schema, table, name);
                            } else {
                                // INSERT or UPDATE
                                let topic_template = col("topic_template");
                                let payload_template = col("payload_template");
                                let qos: u8 = col("qos").parse().unwrap_or(0);
                                let mapping = topic_map::TopicMapping {
                                    name: name.clone(),
                                    schema: schema.clone(),
                                    table: table.clone(),
                                    topic_template: topic_template.clone(),
                                    payload_template: payload_template.clone(),
                                    qos,
                                    template_type: col("template_type"),
                                };
                                topic_map::wal_upsert(mapping);
                                let tmpl_type = col("template_type");
                                let _ = pgrx::spi::Spi::connect_mut(|client| {
                                    client.update(
                                        "INSERT INTO pgmqtt_slot_mappings \
                                             (schema_name, table_name, mapping_name, \
                                              topic_template, payload_template, qos, template_type) \
                                         VALUES ($1, $2, $3, $4, $5, $6, $7) \
                                         ON CONFLICT (schema_name, table_name, mapping_name) DO UPDATE \
                                         SET topic_template = EXCLUDED.topic_template, \
                                             payload_template = EXCLUDED.payload_template, \
                                             qos = EXCLUDED.qos, \
                                             template_type = EXCLUDED.template_type",
                                        None,
                                        &[
                                            schema.as_str().into(),
                                            table.as_str().into(),
                                            name.as_str().into(),
                                            topic_template.as_str().into(),
                                            payload_template.as_str().into(),
                                            (qos as i32).into(),
                                            tmpl_type.as_str().into(),
                                        ],
                                    ).map(|_| ())
                                });
                                log!("pgmqtt: WAL mapping {} {}.{} ({})", op, schema, table, name);
                            }
                        }

                        ring_buffer::RingEvent::Data(change) => {
                            let rendered_messages = topic_map::render(
                                &change.schema, &change.table, change.op, &change.columns,
                            );

                            if rendered_messages.is_empty() {
                                log!(
                                    "pgmqtt: no mapping match for {}.{}",
                                    change.schema,
                                    change.table
                                );
                                continue;
                            }

                            for rendered in rendered_messages {
                                let topic_str = rendered.topic.clone();

                                // Skip if no subscribers (CDC events are never retained).
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
                                            return (Vec::new(), batch_count, false);
                                        }
                                    }
                                } else {
                                    // QOS 0 — fire-and-forget, pushed after commit.
                                    to_publish.push(topic_buffer::MqttMessage {
                                        id: None,
                                        topic: rendered.topic,
                                        payload: rendered.payload,
                                        qos: 0,
                                    });
                                }

                                log!(
                                    "pgmqtt: processed {} on {}.{} → topic='{}'",
                                    change.op,
                                    change.schema,
                                    change.table,
                                    topic_str
                                );
                            }
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
/// Accept new MQTTS (TCP + TLS) connections and perform the MQTT CONNECT handshake.
fn accept_mqtts_connections(
    listener: &TcpListener,
    tls_config: &Arc<rustls::ServerConfig>,
    clients: &mut HashMap<String, MqttClient>,
    session_db_actions: &mut Vec<SessionDbAction>,
) {
    loop {
        match listener.accept() {
            Ok((stream, addr)) => {
                log!("pgmqtt mqtts: new MQTTS connection from {}", addr);
                match Transport::new_tls(stream, tls_config.clone()) {
                    Ok(transport) => {
                        handle_new_connection(transport, None, clients, session_db_actions);
                    }
                    Err(e) => {
                        log!("pgmqtt mqtts: TLS handshake failed for {}: {}", addr, e);
                    }
                }
            }
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
            Err(e) => {
                log!("pgmqtt mqtts: accept error: {}", e);
                break;
            }
        }
    }
}

fn accept_wss_connections(
    listener: &TcpListener,
    tls_config: &Arc<rustls::ServerConfig>,
    clients: &mut HashMap<String, MqttClient>,
    session_db_actions: &mut Vec<SessionDbAction>,
) {
    loop {
        match listener.accept() {
            Ok((stream, addr)) => {
                log!("pgmqtt wss: new WSS connection from {}", addr);

                // Keep the stream in blocking mode with a timeout.
                // rustls::StreamOwned drives the TLS handshake lazily on the first
                // read/write inside websocket::handshake — identical to how MQTTS works.
                let _ = stream.set_read_timeout(Some(Duration::from_secs(5)));
                let _ = stream.set_write_timeout(Some(Duration::from_secs(5)));

                let conn = match rustls::ServerConnection::new(tls_config.clone()) {
                    Ok(c) => c,
                    Err(e) => {
                        log!("pgmqtt wss: failed to create TLS session for {}: {}", addr, e);
                        continue;
                    }
                };
                let mut tls_stream = rustls::StreamOwned::new(conn, stream);

                match websocket::handshake(&mut tls_stream) {
                    Ok((leftover, ws_jwt)) => {
                        log!("pgmqtt wss: WSS upgrade complete for {}", addr);
                        let ws = websocket::WsStream::new(tls_stream, leftover);
                        handle_new_connection(Transport::Wss(ws), ws_jwt, clients, session_db_actions);
                    }
                    Err(e) => {
                        log!("pgmqtt wss: upgrade failed for {}: {}", addr, e);
                    }
                }
            }
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
            Err(e) => {
                log!("pgmqtt wss: accept error: {}", e);
                break;
            }
        }
    }
}

fn accept_mqtt_connections(
    listener: &TcpListener,
    clients: &mut HashMap<String, MqttClient>,
    session_db_actions: &mut Vec<SessionDbAction>,
) {
    loop {
        match listener.accept() {
            Ok((stream, addr)) => {
                log!("pgmqtt mqtt: new TCP connection from {}", addr);
                handle_new_connection(Transport::Raw(stream), None, clients, session_db_actions);
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
                    Ok((leftover, ws_jwt)) => {
                        log!("pgmqtt ws: WebSocket upgrade complete for {}", addr);
                        let ws = websocket::WsStream::new(stream, leftover);
                        handle_new_connection(Transport::Ws(ws), ws_jwt, clients, session_db_actions);
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
    ws_jwt: Option<String>,
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
                finish_connect(transport, connect, ws_jwt, clients, session_db_actions);
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
    ws_jwt: Option<String>,
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

    // ── JWT authentication ────────────────────────────────────────────────────
    let jwt_key_str = crate::get_jwt_public_key_guc();
    let is_ws = matches!(transport, Transport::Ws(_) | Transport::Wss(_));
    let jwt_required = if is_ws && crate::get_jwt_required_ws_guc() {
        true
    } else {
        crate::get_jwt_required_guc()
    };
    let mut jwt_sub_claims: Vec<String> = Vec::new();
    let mut jwt_pub_claims: Vec<String> = Vec::new();

    if !jwt_key_str.is_empty() {
        // Try to parse the JWT: password field first, then WS token (query param / header).
        let token_opt = packet
            .password
            .as_deref()
            .and_then(|b| std::str::from_utf8(b).ok())
            .map(|s| s.trim().to_string())
            .or(ws_jwt);

        match (token_opt, parse_jwt_public_key(&jwt_key_str)) {
            (Some(token), Some(pubkey)) => match validate_jwt(&token, &pubkey) {
                Ok(claims) => {
                    // Enforce client_id claim: if JWT specifies a client_id, CONNECT must match
                    if let Some(ref jwt_cid) = claims.client_id {
                        if jwt_cid != &client_id {
                            log!(
                                "pgmqtt mqtt: JWT client_id '{}' does not match CONNECT client_id '{}'",
                                jwt_cid,
                                client_id
                            );
                            let _ = transport.write_all(&mqtt::build_connack(false, mqtt::reason::NOT_AUTHORIZED));
                            return;
                        }
                    }
                    jwt_sub_claims = claims.sub_claims;
                    jwt_pub_claims = claims.pub_claims;
                    log!("pgmqtt mqtt: JWT validated for '{}'", client_id);
                }
                Err(e) => {
                    log!("pgmqtt mqtt: JWT validation failed for '{}': {}", client_id, e);
                    if jwt_required {
                        let _ = transport.write_all(&mqtt::build_connack(false, mqtt::reason::NOT_AUTHORIZED));
                        return;
                    }
                }
            },
            (None, _) => {
                if jwt_required {
                    log!("pgmqtt mqtt: JWT required but no token provided for '{}'", client_id);
                    let _ = transport.write_all(&mqtt::build_connack(false, mqtt::reason::NOT_AUTHORIZED));
                    return;
                }
            }
            (_, None) => {
                log!("pgmqtt mqtt: failed to parse JWT public key GUC");
                if jwt_required {
                    let _ = transport.write_all(&mqtt::build_connack(false, mqtt::reason::NOT_AUTHORIZED));
                    return;
                }
            }
        }
    }

    // ── Connection limit enforcement ──────────────────────────────────────────
    // Session takeover (same client_id reconnecting) replaces an existing slot
    // and is always allowed.  A genuinely new client is rejected when the limit
    // is reached so operators can control memory usage.
    let limit = crate::license::max_connections();
    if !clients.contains_key(&client_id) && clients.len() >= limit {
        log!(
            "pgmqtt mqtt: connection limit ({}) reached, rejecting '{}'",
            limit,
            client_id
        );
        let _ = transport.write_all(&mqtt::build_connack(false, mqtt::reason::QUOTA_EXCEEDED));
        return;
    }

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
    let mut mqtt_client = MqttClient::new(transport, client_id.clone(), packet.will, packet.keep_alive, packet.receive_maximum);
    mqtt_client.sub_claims = jwt_sub_claims;
    mqtt_client.pub_claims = jwt_pub_claims;
    clients.insert(client_id.clone(), mqtt_client);

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
    /// Mapping names that matched this publish for inbound table writes.
    /// Only populated for QoS >= 1; used by publish_messages_batch to insert
    /// tracking rows into pgmqtt_inbound_pending atomically with message
    /// persistence.
    inbound_mappings: Vec<Arc<str>>,
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
                inbound_mappings: Vec::new(),
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
    pending_inbound_writes: &mut Vec<inbound_map::PendingInboundWrite>,
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
                    if !handle_mqtt_packet(client, packet, pending_publishes, session_db_actions, pending_inbound_writes) {
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
                    let mut msg_id_opt: Option<i64> = None;

                    // Issue 10: an empty-payload retain is a "clear retained" command.
                    // We must NOT insert a pgmqtt_messages row here — it would be an orphan
                    // with no pgmqtt_retained entry pointing to it after the DELETE.
                    // We still push to topic_buffer so live subscribers receive the message.
                    if p.retain && p.payload.is_empty() {
                        // Clear retained entry; errors are propagated so the transaction
                        // rolls back cleanly (issue 13).
                        let args: Vec<pgrx::datum::DatumWithOid> =
                            vec![p.topic.as_str().into()];
                        client.update(
                            "DELETE FROM pgmqtt_retained WHERE topic = $1",
                            None,
                            &args,
                        )?;
                    } else {
                        // Normal publish: persist the message and update retained index.
                        let payload_arg = Some(p.payload.as_slice());
                        let insert_msg = "INSERT INTO pgmqtt_messages (topic, payload, qos, retain) VALUES ($1, $2, $3, $4) RETURNING id";
                        let spi_args: Vec<pgrx::datum::DatumWithOid> = vec![
                            p.topic.as_str().into(),
                            payload_arg.into(),
                            (p.qos as i32).into(),
                            p.retain.into(),
                        ];
                        // Issue 13: use ? so any INSERT failure rolls back the whole
                        // transaction rather than leaving a partial write.
                        let table = client.update(insert_msg, None, &spi_args)?;
                        for row in table {
                            if let Ok(Some(id)) = row.get_by_name::<i64, _>("id") {
                                msg_id_opt = Some(id);
                            }
                            break;
                        }

                        if p.retain {
                            if let Some(msg_id) = msg_id_opt {
                                let args: Vec<pgrx::datum::DatumWithOid> =
                                    vec![p.topic.as_str().into(), msg_id.into()];
                                client.update(
                                    "INSERT INTO pgmqtt_retained (topic, message_id) VALUES ($1, $2) \
                                     ON CONFLICT (topic) DO UPDATE SET message_id = EXCLUDED.message_id",
                                    None,
                                    &args,
                                )?;
                            }
                        }
                    }

                    // Insert inbound-pending tracking rows (virtual subscriber).
                    // These are committed atomically with the message so the
                    // PUBACK reflects durable intent to process.
                    if let Some(msg_id) = msg_id_opt {
                        for mapping_name in &p.inbound_mappings {
                            let args: Vec<pgrx::datum::DatumWithOid> =
                                vec![msg_id.into(), mapping_name.as_ref().into()];
                            client.update(
                                "INSERT INTO pgmqtt_inbound_pending (message_id, mapping_name) \
                                 VALUES ($1, $2) ON CONFLICT DO NOTHING",
                                None,
                                &args,
                            )?;
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
    pending_inbound_writes: &mut Vec<inbound_map::PendingInboundWrite>,
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
                // JWT subscribe authorization
                let jwt_authorized = if client.sub_claims.is_empty() {
                    true
                } else {
                    client.sub_claims.iter().any(|claim| crate::mqtt::topic_matches_filter(topic_filter, claim))
                };

                if !jwt_authorized {
                    log!(
                        "pgmqtt mqtt: '{}' not authorized to subscribe to '{}'",
                        client_id,
                        topic_filter
                    );
                    reason_codes.push(mqtt::reason::NOT_AUTHORIZED);
                    continue;
                }

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

            // Deliver retained messages for successfully subscribed filters (MQTT §3.8.4)
            let subscribed_filters: Vec<&String> = sub
                .topics
                .iter()
                .zip(reason_codes.iter())
                .filter_map(|((filter, _), &rc)| if rc <= 0x02 { Some(filter) } else { None })
                .collect();

            if !subscribed_filters.is_empty() {
                let retained_msgs: Vec<(String, Vec<u8>)> = BackgroundWorker::transaction(|| {
                    let mut results = Vec::new();
                    let _ = pgrx::spi::Spi::connect(|spi| {
                        if let Ok(table) = spi.select(
                            "SELECT r.topic, m.payload \
                             FROM pgmqtt_retained r \
                             JOIN pgmqtt_messages m ON r.message_id = m.id",
                            None,
                            &[],
                        ) {
                            for row in table {
                                let topic: String =
                                    row.get_by_name("topic").ok().flatten().unwrap_or_default();
                                let payload: Option<Vec<u8>> =
                                    row.get_by_name("payload").ok().flatten();
                                if !topic.is_empty() {
                                    results.push((topic, payload.unwrap_or_default()));
                                }
                            }
                        }
                        Ok::<_, pgrx::spi::Error>(())
                    });
                    Ok::<Vec<(String, Vec<u8>)>, pgrx::spi::Error>(results)
                })
                .unwrap_or_default();

                for (topic, payload) in &retained_msgs {
                    for filter in &subscribed_filters {
                        if mqtt::topic_matches_filter(topic, filter) {
                            let pkt = mqtt::build_publish_qos0_retain(topic, payload);
                            let _ = transport.write_all(&pkt);
                            break; // deliver each retained message at most once per SUBSCRIBE
                        }
                    }
                }
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
            // JWT publish authorization
            if !client.pub_claims.is_empty() {
                let authorized = client.pub_claims.iter().any(|claim| crate::mqtt::topic_matches_filter(&pub_pkt.topic, claim));
                if !authorized {
                    log!(
                        "pgmqtt mqtt: '{}' not authorized to publish to '{}'",
                        client_id,
                        pub_pkt.topic
                    );
                    if pub_pkt.qos == 1 {
                        if let Some(pid) = pub_pkt.packet_id {
                            // Send PUBACK with NOT_AUTHORIZED reason code
                            let mut vh = Vec::with_capacity(4);
                            vh.extend_from_slice(&pid.to_be_bytes());
                            vh.push(mqtt::reason::NOT_AUTHORIZED);
                            vh.push(0x00); // no properties
                            use crate::mqtt::PacketType;
                            let first_byte = ((PacketType::Puback as u8) << 4) | 0x00;
                            let mut pkt = vec![first_byte, vh.len() as u8];
                            pkt.extend_from_slice(&vh);
                            let _ = transport.write_all(&pkt);
                        }
                    }
                    // QoS 0: silently drop
                    return true;
                }
            }

            // Validate topic name: MQTT §4.7.3 prohibits wildcards in PUBLISH; §1.5.3 prohibits null chars
            if pub_pkt.topic.contains('\0') || pub_pkt.topic.contains('+') || pub_pkt.topic.contains('#') {
                log!(
                    "pgmqtt mqtt: '{}' published to invalid topic '{}' — disconnecting",
                    client_id,
                    pub_pkt.topic
                );
                let _ = transport.write_all(&mqtt::build_disconnect(mqtt::reason::TOPIC_NAME_INVALID));
                return false;
            }

            // Check for inbound mapping matches (MQTT → PostgreSQL table writes)
            let inbound_matches = inbound_map::try_match(&pub_pkt.topic, &pub_pkt.payload);

            // QoS 0: collect for inline best-effort execution
            // QoS 1: collect mapping names to attach to PendingPublish for
            //         durable tracking via pgmqtt_inbound_pending
            let mut inbound_mapping_names: Vec<Arc<str>> = Vec::new();
            for (_idx, match_result) in inbound_matches {
                if pub_pkt.qos == 0 {
                    pending_inbound_writes.push(inbound_map::PendingInboundWrite {
                        sql: match_result.sql,
                        args: match_result.values,
                    });
                } else {
                    inbound_mapping_names.push(match_result.mapping_name);
                }
            }

            // Optimization: skip all processing if no one is listening, not
            // retained, and no QoS 1 inbound mappings need durable tracking.
            let subscriber_ids = subscriptions::match_topic(&pub_pkt.topic);
            if !pub_pkt.retain && subscriber_ids.is_empty() && inbound_mapping_names.is_empty() {
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
                inbound_mappings: inbound_mapping_names,
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
                Disconnect,
            }
            let (pkt_res, db_action) = with_sessions(|sessions| {
                if let Some(session) = sessions.get_mut(sub_id) {
                    let delivery_qos = std::cmp::min(msg.qos, *granted_qos);
                    if delivery_qos == 1 {
                        if session.inflight.len() >= inflight_limit {
                            if session.queue.len() >= MAX_QUEUE_SIZE {
                                // Client is hopelessly backed up; signal disconnection to
                                // reclaim memory.  The caller adds it to to_remove.
                                pgrx::log!(
                                    "pgmqtt: client '{}' queue hit hard limit ({} messages). Disconnecting.",
                                    sub_id,
                                    MAX_QUEUE_SIZE,
                                );
                                return (None, DbAction::Disconnect);
                            }
                            // Queue instead of drop to preserve at-least-once semantics.
                            session.queue.push_back(topic_buffer::MqttMessage {
                                id: msg.id,
                                topic: msg.topic.clone(),
                                payload: msg.payload.clone(),
                                qos: delivery_qos,
                            });
                            if session.queue.len() > QUEUE_WARNING_THRESHOLD {
                                pgrx::log!(
                                    "pgmqtt: client '{}' queue exceeded {} messages ({}). Consider investigating client health.",
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
                    if msg.id.is_some() {
                        batch_entries.push((sub_id.clone(), None));
                    }
                }
                DbAction::Inflight(pid) => {
                    if msg.id.is_some() {
                        batch_entries.push((sub_id.clone(), Some(pid)));
                    }
                }
                DbAction::Disconnect => {
                    to_remove.push(sub_id.clone());
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

// ---------------------------------------------------------------------------
// TLS helpers
// ---------------------------------------------------------------------------

/// Build a rustls ServerConfig from a PEM cert file and a PEM private key file.
/// Returns None on any error (missing files, parse errors, etc.).
pub fn build_tls_config(cert_path: &str, key_path: &str) -> Option<Arc<rustls::ServerConfig>> {
    use rustls::pki_types::{CertificateDer, PrivateKeyDer};
    use std::fs::File;
    use std::io::BufReader;

    let cert_file = File::open(cert_path).ok()?;
    let mut cert_reader = BufReader::new(cert_file);
    let certs: Vec<CertificateDer<'static>> = rustls_pemfile::certs(&mut cert_reader)
        .filter_map(|r| r.ok())
        .map(|c| c.into_owned())
        .collect();
    if certs.is_empty() {
        return None;
    }

    let key_file = File::open(key_path).ok()?;
    let mut key_reader = BufReader::new(key_file);
    let private_key: PrivateKeyDer<'static> = match rustls_pemfile::private_key(&mut key_reader) {
        Ok(Some(k)) => k.clone_key(),
        _ => return None,
    };

    let config = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, private_key)
        .ok()?;

    Some(Arc::new(config))
}

// ---------------------------------------------------------------------------
// JWT helpers
// ---------------------------------------------------------------------------

/// Parse a JWT public key from either PEM format or raw base64url-encoded bytes.
/// Returns 32-byte Ed25519 public key on success.
fn parse_jwt_public_key(key_str: &str) -> Option<[u8; 32]> {
    let key_str = key_str.trim();
    if key_str.starts_with("-----BEGIN") {
        // PEM format — extract the base64 body (standard base64, not URL-safe)
        use base64::Engine;
        let body: String = key_str
            .lines()
            .filter(|l| !l.starts_with("-----"))
            .collect::<Vec<_>>()
            .join("");
        let der = base64::engine::general_purpose::STANDARD.decode(&body).ok()?;
        // Ed25519 SubjectPublicKeyInfo: last 32 bytes are the raw key
        if der.len() < 32 {
            return None;
        }
        let raw = &der[der.len() - 32..];
        let mut arr = [0u8; 32];
        arr.copy_from_slice(raw);
        Some(arr)
    } else {
        // Raw base64url
        let raw = crate::license::base64_url_decode(key_str).ok()?;
        if raw.len() != 32 {
            return None;
        }
        let mut arr = [0u8; 32];
        arr.copy_from_slice(&raw);
        Some(arr)
    }
}

/// JWT claims extracted from a token.
struct JwtClaims {
    /// If present, the CONNECT client_id must match this value.
    client_id: Option<String>,
    sub_claims: Vec<String>,
    pub_claims: Vec<String>,
}

/// Validate a JWT token using an Ed25519 public key.
/// Returns Ok(JwtClaims) on success, Err on failure.
fn validate_jwt(token: &str, pubkey_bytes: &[u8; 32]) -> Result<JwtClaims, String> {
    use ed25519_dalek::{Signature, VerifyingKey, Verifier};

    let parts: Vec<&str> = token.splitn(3, '.').collect();
    if parts.len() != 3 {
        return Err("invalid JWT format".into());
    }

    let payload_b64 = parts[1];
    let sig_b64 = parts[2];

    // Decode payload
    let payload_bytes = crate::license::base64_url_decode(payload_b64)
        .map_err(|_| "bad payload base64".to_string())?;

    // Decode signature
    let sig_bytes = crate::license::base64_url_decode(sig_b64)
        .map_err(|_| "bad sig base64".to_string())?;

    if sig_bytes.len() != 64 {
        return Err("signature must be 64 bytes".into());
    }
    let sig_arr: [u8; 64] = sig_bytes.try_into()
        .expect("unreachable: length already checked to be exactly 64");
    let signature = Signature::from_bytes(&sig_arr);

    // Verify signature over "header.payload" (slice the original token to avoid allocation)
    let last_dot = token.rfind('.')
        .expect("unreachable: token already confirmed to have 3 dot-separated parts");
    let signed_data = &token[..last_dot];
    let verifying_key = VerifyingKey::from_bytes(pubkey_bytes)
        .map_err(|e| format!("bad public key: {}", e))?;
    verifying_key
        .verify(signed_data.as_bytes(), &signature)
        .map_err(|_| "signature verification failed".to_string())?;

    // Parse payload JSON
    let payload: serde_json::Value = serde_json::from_slice(&payload_bytes)
        .map_err(|e| format!("bad payload JSON: {}", e))?;

    // Check exp claim (mandatory)
    let now = crate::license::now_secs();
    let exp = payload.get("exp").and_then(|v| v.as_i64())
        .ok_or_else(|| "missing or invalid exp claim".to_string())?;
    if now > exp {
        return Err("token expired".into());
    }

    // Extract client_id claim
    let client_id = payload
        .get("client_id")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());

    // Extract sub_claims and pub_claims arrays
    let sub_claims = payload
        .get("sub_claims")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(|s| s.to_string()))
                .collect()
        })
        .unwrap_or_default();

    let pub_claims = payload
        .get("pub_claims")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(|s| s.to_string()))
                .collect()
        })
        .unwrap_or_default();

    Ok(JwtClaims { client_id, sub_claims, pub_claims })
}
#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn test_build_tls_config_valid() {
        // These files were generated in /tmp during verification step
        let cert_path = "/tmp/server.crt";
        let key_path = "/tmp/server.key";

        if fs::metadata(cert_path).is_ok() && fs::metadata(key_path).is_ok() {
            let config = build_tls_config(cert_path, key_path);
            assert!(config.is_some(), "Should be able to load valid TLS config");
        } else {
            pgrx::log!("Skipping test_build_tls_config_valid: certs not found in /tmp");
        }
    }

    #[test]
    fn test_build_tls_config_invalid() {
        let config = build_tls_config("/tmp/nonexistent.crt", "/tmp/nonexistent.key");
        assert!(config.is_none(), "Should return None for nonexistent files");

        let garbage_path = "/tmp/garbage.txt";
        fs::write(garbage_path, "not a certificate").unwrap();
        let config = build_tls_config(garbage_path, garbage_path);
        assert!(config.is_none(), "Should return None for garbage files");
        let _ = fs::remove_file(garbage_path);
    }
}
