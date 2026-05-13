//! MQTT broker main event loop and client polling.

pub mod db_action;
pub mod session;
pub mod transport;

use crate::inbound_map;
use crate::mqtt;
use crate::subscriptions;
pub use db_action::{execute_session_db_actions, SessionDbAction};
pub use session::{with_sessions, MqttMessage, MqttSession};
pub use transport::Transport;

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
fn latch_interval() -> Duration {
    Duration::from_millis(crate::get_tick_interval_ms_guc() as u64)
}

/// Timeout for HTTP client read/write operations.
const CLIENT_TIMEOUT: Duration = Duration::from_secs(2);

/// Pre-handshake DoS floor: peer must produce a parseable CONNECT before
/// accumulating more than this. Sized for chunky JWTs (~8 KiB) + headroom.
const MAX_PRE_CONNECT_BYTES: usize = 16_384;

const READ_CHUNK_BYTES: usize = 65_536;

/// MQTT-3.2.2.3.5 Maximum Packet Size advertised in CONNACK.
fn broker_max_packet_size() -> u32 {
    crate::get_max_client_buffer_bytes_guc().min(u32::MAX as usize) as u32
}

/// Maximum number of unacked QoS 1 messages per client.
const MAX_INFLIGHT_MESSAGES: usize = 800;

/// Threshold for warning when a client's message queue exceeds this size.
const QUEUE_WARNING_THRESHOLD: usize = 10_000;

/// Hard cap on the per-client pending queue.  Clients that exceed this are
/// disconnected to prevent unbounded memory growth inside the PostgreSQL process.
const MAX_QUEUE_SIZE: usize = 50_000;



// Individual db_* functions were refactored into execute_session_db_actions.

/// Run `f` inside a PostgreSQL subtransaction (savepoint).  On `Ok`, the
/// savepoint is released; on `Err` (whether a Rust error or a PG longjmp
/// caught by `catch_others`), the savepoint is rolled back and the error is
/// returned.  Callers can retry or surface the error without aborting the
/// enclosing `BackgroundWorker::transaction`.
fn with_subtransaction<T>(
    f: impl FnOnce() -> Result<T, pgrx::spi::Error> + std::panic::UnwindSafe,
) -> Result<T, pgrx::spi::Error> {
    use pgrx::pg_sys::pg_try::PgTryBuilder;
    use pgrx::spi;
    use std::sync::atomic::{AtomicBool, Ordering};

    unsafe {
        pgrx::pg_sys::BeginInternalSubTransaction(std::ptr::null_mut());
    }
    // Track whether catch_others already rolled back (PG longjmp path) so we
    // don't double-free the subtransaction on the Rust-Err path below.
    let rolled_back_by_pg = AtomicBool::new(false);
    let result = PgTryBuilder::new(f)
        .catch_others(|_caught| {
            unsafe {
                pgrx::pg_sys::RollbackAndReleaseCurrentSubTransaction();
            }
            rolled_back_by_pg.store(true, Ordering::Relaxed);
            // NoAttribute is used as a generic sentinel — callers only check
            // is_ok()/is_err(), not the specific code.
            Err(spi::Error::SpiError(spi::SpiErrorCodes::NoAttribute))
        })
        .execute();
    if result.is_ok() {
        unsafe { pgrx::pg_sys::ReleaseCurrentSubTransaction(); }
    } else if !rolled_back_by_pg.load(Ordering::Relaxed) {
        unsafe { pgrx::pg_sys::RollbackAndReleaseCurrentSubTransaction(); }
    }
    result
}

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
    let startup_overflow = std::sync::Mutex::new(Vec::<(String, i64)>::new());
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
            let queue_cap = crate::get_max_queue_bytes_per_client_guc();
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
                    let mut overflow_count = 0usize;
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
                                // Inflight — always load; cap only applies to the queue.
                                sess.inflight.insert(
                                    pid as u16,
                                    (
                                        Arc::from(topic.as_str()),
                                        Arc::from(payload.unwrap_or_default()),
                                        Some(message_id),
                                        std::time::Instant::now(),
                                    ),
                                );
                                msg_count += 1;
                            } else {
                                // Queued — enforce byte cap so a restart can't bypass it.
                                let payload_data: Vec<u8> = payload.unwrap_or_default();
                                if sess.queue_bytes.saturating_add(payload_data.len()) > queue_cap {
                                    startup_overflow.lock().unwrap().push((client_id.clone(), message_id));
                                    overflow_count += 1;
                                } else {
                                    sess.queue_push_back(MqttMessage {
                                        id: Some(message_id),
                                        topic: Arc::from(topic.as_str()),
                                        payload: Arc::from(payload_data),
                                        qos: qos as u8,
                                    });
                                    msg_count += 1;
                                }
                            }
                        }
                    }
                    if msg_count > 0 {
                        pgrx::log!(
                            "pgmqtt: loaded {} pending messages into sessions",
                            msg_count
                        );
                    }
                    if overflow_count > 0 {
                        pgrx::log!(
                            "pgmqtt: startup: dropped {} queued messages exceeding max_queue_bytes_per_client ({}B)",
                            overflow_count,
                            queue_cap,
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

    let startup_overflow = startup_overflow.into_inner().unwrap();
    if !startup_overflow.is_empty() {
        execute_session_db_actions(
            startup_overflow
                .into_iter()
                .map(|(client_id, message_id)| SessionDbAction::DeleteMessage {
                    client_id,
                    message_id,
                })
                .collect(),
        );
    }
}

/// Cached xmin fingerprint: changes whenever pgmqtt_inbound_mappings is modified.
/// A reload is only performed when this value changes, avoiding the full
/// mapping + column-type query overhead on every tick.
static INBOUND_XMIN_FINGERPRINT: std::sync::Mutex<Option<i64>> = std::sync::Mutex::new(None);

/// Load inbound mappings from pgmqtt_inbound_mappings into the in-memory cache.
///
/// Uses a lightweight change-detection query (`sum(xmin)`) to skip the
/// expensive full reload when nothing has changed.
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

            // Change detection: sum of xmin values changes on any INSERT/UPDATE/DELETE.
            let current_fingerprint: i64 = client
                .select(
                    "SELECT COALESCE(SUM(xmin::text::bigint), 0)::bigint FROM pgmqtt_inbound_mappings",
                    None,
                    &[],
                )?
                .first()
                .get_one::<i64>()?
                .unwrap_or(0);

            {
                let mut cached = INBOUND_XMIN_FINGERPRINT
                    .lock()
                    .unwrap_or_else(|e| e.into_inner());
                if let Some(prev) = *cached {
                    if prev == current_fingerprint {
                        return Ok::<_, pgrx::spi::Error>(()); // no change
                    }
                }
                *cached = Some(current_fingerprint);
            }

            let mut mappings = Vec::new();
            if let Ok(table) = client.select(
                "SELECT mapping_name, topic_pattern, target_schema, target_table, \
                        column_map::text, op, conflict_columns, template_type \
                 FROM pgmqtt_inbound_mappings",
                None,
                &[],
            ) {
                for row in table {
                    let mn: String = row
                        .get_by_name("mapping_name")
                        .ok()
                        .flatten()
                        .unwrap_or_default();
                    let tp: String = row
                        .get_by_name("topic_pattern")
                        .ok()
                        .flatten()
                        .unwrap_or_default();
                    let ts: String = row
                        .get_by_name("target_schema")
                        .ok()
                        .flatten()
                        .unwrap_or_else(|| "public".to_string());
                    let tt: String = row
                        .get_by_name("target_table")
                        .ok()
                        .flatten()
                        .unwrap_or_default();
                    let cm_str: String = row
                        .get_by_name("column_map")
                        .ok()
                        .flatten()
                        .unwrap_or_else(|| "{}".to_string());
                    let op_str: String = row
                        .get_by_name("op")
                        .ok()
                        .flatten()
                        .unwrap_or_else(|| "insert".to_string());
                    let cc: Option<Vec<String>> =
                        row.get_by_name("conflict_columns").ok().flatten();
                    let tmpl_type: String = row
                        .get_by_name("template_type")
                        .ok()
                        .flatten()
                        .unwrap_or_else(|| "jsonpath".to_string());

                    // Parse the mapping at load time
                    let segments = match inbound_map::parse_pattern(&tp) {
                        Ok(s) => s,
                        Err(e) => {
                            log!(
                                "pgmqtt: skipping inbound mapping '{}': invalid pattern: {}",
                                mn,
                                e
                            );
                            continue;
                        }
                    };

                    let inbound_op = match inbound_map::InboundOp::parse(&op_str) {
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
                                    log!(
                                        "pgmqtt: skipping inbound mapping '{}': column '{}': {}",
                                        mn,
                                        col_name,
                                        e
                                    );
                                    parse_ok = false;
                                    break;
                                }
                            }
                        }
                    }
                    if !parse_ok {
                        continue;
                    }

                    let col_names: Vec<String> =
                        parsed_columns.iter().map(|(n, _)| n.clone()).collect();

                    // Look up column types for typed placeholder casts (single query via JOIN)
                    let qualified_name = format!(
                        "{}.{}",
                        inbound_map::quote_ident(&ts),
                        inbound_map::quote_ident(&tt),
                    );
                    let mut col_types: Vec<String> = Vec::new();
                    let mut types_ok = true;
                    match client.select(
                        "SELECT a.attname::text AS col_name, \
                                format_type(a.atttypid, a.atttypmod) AS col_type \
                         FROM pg_attribute a \
                         WHERE a.attrelid = $1::regclass \
                           AND a.attnum > 0 AND NOT a.attisdropped",
                        None,
                        &[qualified_name.as_str().into()],
                    ) {
                        Ok(attr_table) => {
                            // Build a lookup map from column name → type
                            let mut type_map = std::collections::HashMap::new();
                            for attr_row in attr_table {
                                if let (Ok(Some(name)), Ok(Some(typ))) = (
                                    attr_row.get_by_name::<String, _>("col_name"),
                                    attr_row.get_by_name::<String, _>("col_type"),
                                ) {
                                    type_map.insert(name, typ);
                                }
                            }
                            for col_name in &col_names {
                                match type_map.get(col_name) {
                                    Some(typ) => col_types.push(typ.clone()),
                                    None => {
                                        log!("pgmqtt: skipping inbound mapping '{}': column '{}' type not found", mn, col_name);
                                        types_ok = false;
                                        break;
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            log!("pgmqtt: skipping inbound mapping '{}': failed to look up column types: {}", mn, e);
                            types_ok = false;
                        }
                    }
                    if !types_ok {
                        continue;
                    }

                    let sql = inbound_map::generate_sql(
                        &ts,
                        &tt,
                        &col_names,
                        &col_types,
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
                        template_type: tmpl_type,
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

/// Execute inbound table writes (QoS 0, best-effort).
///
/// Each write is executed in its own transaction so a single failure
/// (constraint violation, bad data) doesn't roll back the entire batch.
fn execute_inbound_writes(writes: Vec<inbound_map::PendingInboundWrite>) {
    if writes.is_empty() {
        return;
    }

    let total = writes.len();
    let mut err_count = 0usize;

    for write in &writes {
        let ok: bool = BackgroundWorker::transaction(|| {
            pgrx::spi::Spi::connect_mut(|client| {
                let spi_args: Vec<pgrx::datum::DatumWithOid> = write
                    .args
                    .iter()
                    .map(|a| {
                        let opt: Option<&str> = a.as_deref();
                        opt.into()
                    })
                    .collect();
                client.update(&*write.sql, None, &spi_args)?;
                Ok::<_, pgrx::spi::Error>(())
            })
            .is_ok()
        });
        if !ok {
            err_count += 1;
        }
    }

    let ok_count = total - err_count;
    if ok_count > 0 {
        log!("pgmqtt inbound: committed {} writes", ok_count);
        crate::metrics::add(&crate::metrics::get().inbound_writes_ok, ok_count as u64);
    }
    if err_count > 0 {
        log!("pgmqtt inbound: {} writes failed", err_count);
        crate::metrics::add(
            &crate::metrics::get().inbound_writes_failed,
            err_count as u64,
        );
    }
}

/// Virtual subscriber: process QoS 1 inbound-pending messages.
///
/// One transaction per row: SELECT FOR UPDATE SKIP LOCKED + target INSERT +
/// DELETE from pgmqtt_inbound_pending + orphan cleanup are all atomic.
/// This eliminates the race where a concurrent DROP TABLE on the target
/// could execute between a separate read transaction and the write transaction.
fn process_inbound_pending() {
    const BATCH_SIZE: usize = 50;
    const MAX_RETRIES: i32 = 10;

    enum RowOutcome {
        /// No pending rows ready.
        Empty,
        /// Row processed successfully.
        Ok { message_id: i64, mapping_name: String },
        /// Mapping no longer exists in the current config.
        MappingGone { message_id: i64, mapping_name: String, retry_count: i32, topic: String, payload: Vec<u8> },
        /// Target write failed; error is returned for classification outside the transaction.
        Failed { message_id: i64, mapping_name: String, retry_count: i32, topic: String, payload: Vec<u8>, error: pgrx::spi::Error },
    }

    let mut processed = 0;
    for _ in 0..BATCH_SIZE {
        let outcome = BackgroundWorker::transaction(|| {
            pgrx::spi::Spi::connect_mut(|client| {
                let table = client.select(
                    "SELECT p.message_id, p.mapping_name, p.retry_count, \
                            m.topic, m.payload \
                     FROM pgmqtt_inbound_pending p \
                     JOIN pgmqtt_messages m ON p.message_id = m.id \
                     WHERE p.next_retry_at <= now() \
                     ORDER BY p.next_retry_at ASC \
                     LIMIT 1",
                    None,
                    &[],
                )?;

                let row = match table.into_iter().next() {
                    None => return Ok::<RowOutcome, pgrx::spi::Error>(RowOutcome::Empty),
                    Some(r) => r,
                };

                let message_id: i64 = row.get_by_name("message_id").ok().flatten().unwrap_or(0);
                let mapping_name: String =
                    row.get_by_name("mapping_name").ok().flatten().unwrap_or_default();
                let retry_count: i32 =
                    row.get_by_name("retry_count").ok().flatten().unwrap_or(0);
                let topic: String =
                    row.get_by_name("topic").ok().flatten().unwrap_or_default();
                let payload: Vec<u8> =
                    row.get_by_name("payload").ok().flatten().unwrap_or_default();

                let matches = inbound_map::try_match(&topic, &payload);
                let target_match = matches
                    .into_iter()
                    .find(|(_, m)| m.mapping_name.as_ref() == mapping_name);

                let (_, match_result) = match target_match {
                    None => {
                        return Ok(RowOutcome::MappingGone {
                            message_id, mapping_name, retry_count, topic, payload,
                        });
                    }
                    Some(t) => t,
                };

                // Pre-check: to_regclass returns NULL for missing tables rather than
                // raising a C-level ERROR (which would longjmp past catch_unwind).
                let qualified = format!(
                    "{}.{}",
                    inbound_map::quote_ident(&match_result.target_schema),
                    inbound_map::quote_ident(&match_result.target_table),
                );
                let table_exists = client
                    .select(
                        "SELECT to_regclass($1) IS NOT NULL",
                        None,
                        &[qualified.as_str().into()],
                    )?
                    .into_iter()
                    .next()
                    .and_then(|r| r.get_by_name::<bool, _>("?column?").ok().flatten())
                    .unwrap_or(false);
                if !table_exists {
                    return Ok(RowOutcome::MappingGone {
                        message_id, mapping_name, retry_count, topic, payload,
                    });
                }

                let spi_args: Vec<pgrx::datum::DatumWithOid> = match_result
                    .values
                    .iter()
                    .map(|a| { let opt: Option<&str> = a.as_deref(); opt.into() })
                    .collect();

                if let Err(e) = client.update(&*match_result.sql, None, &spi_args) {
                    return Ok(RowOutcome::Failed {
                        message_id, mapping_name, retry_count, topic, payload, error: e,
                    });
                }

                client.update(
                    "DELETE FROM pgmqtt_inbound_pending \
                     WHERE message_id = $1 AND mapping_name = $2",
                    None,
                    &[message_id.into(), mapping_name.as_str().into()],
                )?;
                db_action::cleanup_orphaned_message(client, message_id)?;
                Ok(RowOutcome::Ok { message_id, mapping_name })
            })
        })
        .unwrap_or(RowOutcome::Empty);

        match outcome {
            RowOutcome::Empty => break,
            RowOutcome::Ok { message_id, mapping_name } => {
                crate::metrics::inc(&crate::metrics::get().inbound_writes_ok);
                log!(
                    "pgmqtt inbound: processed message {} for mapping '{}'",
                    message_id,
                    mapping_name,
                );
                processed += 1;
            }
            RowOutcome::MappingGone { message_id, mapping_name, retry_count, topic, payload } => {
                dead_letter_inbound(
                    message_id, &mapping_name, retry_count,
                    "mapping no longer exists", &topic, &payload,
                );
                processed += 1;
            }
            RowOutcome::Failed { message_id, mapping_name, retry_count, topic, payload, error } => {
                crate::metrics::inc(&crate::metrics::get().inbound_writes_failed);
                handle_inbound_failure(
                    message_id, &mapping_name, retry_count,
                    &error, MAX_RETRIES, &topic, &payload,
                );
                processed += 1;
            }
        }
    }

    if processed > 0 {
        log!("pgmqtt inbound: processed {} pending rows", processed);
    }
}

/// Classify a write failure and either retry or dead-letter.
fn handle_inbound_failure(
    message_id: i64,
    mapping_name: &str,
    retry_count: i32,
    error: &pgrx::spi::Error,
    max_retries: i32,
    topic: &str,
    payload: &[u8],
) {
    let retryable = is_retryable_error(error);
    let error_msg = format!("{error}");

    if !retryable || retry_count >= max_retries {
        dead_letter_inbound(message_id, mapping_name, retry_count, &error_msg, topic, payload);
    } else {
        crate::metrics::inc(&crate::metrics::get().inbound_retries);
        log!(
            "pgmqtt inbound: retry {}/{} for message {} mapping '{}': {}",
            retry_count + 1,
            max_retries,
            message_id,
            mapping_name,
            error_msg,
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
                        error_msg.as_str().into(),
                        retry_count.into(),
                    ],
                )?;
                Ok::<_, pgrx::spi::Error>(())
            });
        });
    }
}

/// Move a failed inbound message to the dead-letter table.
fn dead_letter_inbound(
    message_id: i64,
    mapping_name: &str,
    retry_count: i32,
    error_msg: &str,
    topic: &str,
    payload: &[u8],
) {
    crate::metrics::inc(&crate::metrics::get().inbound_dead_letters);
    log!(
        "pgmqtt inbound: dead-lettering message {} for mapping '{}': {}",
        message_id,
        mapping_name,
        error_msg
    );
    BackgroundWorker::transaction(|| {
        let _ = pgrx::spi::Spi::connect_mut(|client| {
            let payload_arg: Option<&[u8]> = if payload.is_empty() {
                None
            } else {
                Some(payload)
            };
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
            db_action::cleanup_orphaned_message(client, message_id)?;
            Ok::<_, pgrx::spi::Error>(())
        });
    });
}

/// Classify whether an SPI error is retryable.
///
/// Most inbound write failures are deterministic (missing table, type mismatch,
/// constraint violation) and will never succeed on retry.  Only SPI-level
/// connection or transaction errors are transient and worth retrying.
fn is_retryable_error(error: &pgrx::spi::Error) -> bool {
    matches!(
        error,
        pgrx::spi::Error::SpiError(
            pgrx::spi::SpiErrorCodes::Connect | pgrx::spi::SpiErrorCodes::Transaction
        )
    )
}

struct MqttClient {
    transport: Transport,
    client_id: String,
    buf: Vec<u8>,
    will: Option<mqtt::Will>,
    keep_alive: u16,
    last_received_at: std::time::Instant,
    receive_maximum: u16,
    /// MQTT-3.1.2.24-1: outbound packets larger than this MUST be dropped.
    /// None ⇒ no limit (always None for v3.1.1).
    max_packet_size: Option<u32>,
    /// MQTT protocol version (4 = v3.1.1, 5 = v5.0).
    protocol_version: u8,
    /// JWT claim-based topic filters for subscribe authorization.
    sub_claims: Vec<String>,
    /// JWT claim-based topic filters for publish authorization.
    pub_claims: Vec<String>,
    transport_label: &'static str,
    connected_at_unix: u64,
    msgs_received_count: u64,
    msgs_sent_count: u64,
    bytes_received_count: u64,
    bytes_sent_count: u64,
    /// Set to true when the client sends a normal DISCONNECT; affects disconnect metrics.
    clean_disconnect: bool,
    /// Inline session state — avoids global mutex lookup for connected clients.
    session: MqttSession,
    /// Outbound bytes that could not be written in a previous tick (WouldBlock).
    /// Drained at the start of each tick before new messages are delivered.
    write_buf: Vec<u8>,
}

impl MqttClient {
    fn new(
        transport: Transport,
        client_id: String,
        will: Option<mqtt::Will>,
        keep_alive: u16,
        receive_maximum: u16,
        max_packet_size: Option<u32>,
        protocol_version: u8,
        session: MqttSession,
        transport_label: &'static str,
    ) -> Self {
        Self {
            transport,
            client_id,
            buf: Vec::with_capacity(READ_CHUNK_BYTES),
            will,
            keep_alive,
            last_received_at: std::time::Instant::now(),
            receive_maximum,
            max_packet_size,
            protocol_version,
            sub_claims: Vec::new(),
            pub_claims: Vec::new(),
            session,
            transport_label,
            connected_at_unix: crate::license::now_secs() as u64,
            msgs_received_count: 0,
            msgs_sent_count: 0,
            bytes_received_count: 0,
            bytes_sent_count: 0,
            clean_disconnect: false,
            write_buf: Vec::new(),
        }
    }

    /// Returns true if this client is using MQTT 5.0.
    #[inline]
    fn v5(&self) -> bool {
        mqtt::is_v5(self.protocol_version)
    }

    /// MQTT-3.1.2.24-1: callers MUST drop the packet if this is true.
    #[inline]
    fn exceeds_max_packet(&self, pkt_len: usize) -> bool {
        matches!(self.max_packet_size, Some(m) if pkt_len > m as usize)
    }

    #[inline]
    fn record_msg_sent(&mut self, payload_len: usize) {
        let m = crate::metrics::get();
        crate::metrics::inc(&m.msgs_sent);
        crate::metrics::add(&m.bytes_sent, payload_len as u64);
        self.msgs_sent_count += 1;
        self.bytes_sent_count += payload_len as u64;
    }

    /// Drain `write_buf` into the transport. Returns `false` on a fatal write
    /// error (caller must disconnect the client); returns `true` if the buffer
    /// is empty or if writing stalled again (WouldBlock — retry next tick).
    fn flush_write_buf(&mut self) -> bool {
        use std::io::Write;
        let mut pos = 0;
        while pos < self.write_buf.len() {
            match self.transport.write(&self.write_buf[pos..]) {
                Ok(0) => return false,
                Ok(n) => pos += n,
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
                Err(_) => return false,
            }
        }
        self.write_buf.drain(..pos);
        true
    }

    /// Write `data` to the transport, buffering any bytes that could not be
    /// sent immediately (WouldBlock).  Returns `Err(())` on a fatal error.
    fn try_write(&mut self, data: &[u8]) -> Result<(), ()> {
        use std::io::Write;
        if !self.write_buf.is_empty() {
            self.write_buf.extend_from_slice(data);
            return Ok(());
        }
        let mut pos = 0;
        while pos < data.len() {
            match self.transport.write(&data[pos..]) {
                Ok(0) => return Err(()),
                Ok(n) => pos += n,
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    self.write_buf.extend_from_slice(&data[pos..]);
                    return Ok(());
                }
                Err(_) => return Err(()),
            }
        }
        Ok(())
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

    while BackgroundWorker::wait_latch(Some(latch_interval())) {
        if BackgroundWorker::sighup_received() {
            log!("pgmqtt http: SIGHUP received");
            unsafe {
                pgrx::pg_sys::ProcessConfigFile(pgrx::pg_sys::GucContext::PGC_SIGHUP);
            }
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
/// Both share the same `clients` map, so CDC events are delivered to all
/// connected clients regardless of transport.
pub fn run_mqtt_cdc(ports: crate::PortConfig, slot_name: &str) {
    db_mark_sessions_disconnected_on_startup();
    db_load_sessions_on_startup();
    load_inbound_mappings();

    BackgroundWorker::transaction(|| {
        crate::statements::prepare_hot_path_statements();
    });

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
    // Tick counter for throttling low-priority periodic work.
    let mut tick: u64 = 0;
    // Wall-clock timers for periodic tasks — their frequency must stay stable
    // regardless of tick rate (pgmqtt.tick_interval_ms).
    let mut last_inbound_reload = std::time::Instant::now();
    let mut last_session_sweep = std::time::Instant::now();
    let mut last_redeliver_check = std::time::Instant::now();
    // Enterprise metrics flush timers (only active when metrics feature is licensed).
    let mut last_metrics_flush = std::time::Instant::now();
    let mut last_connections_flush = std::time::Instant::now();
    while BackgroundWorker::wait_latch(Some(latch_interval())) {
        tick = tick.wrapping_add(1);

        if BackgroundWorker::sighup_received() {
            log!("pgmqtt mqtt+cdc: SIGHUP received");
            unsafe {
                pgrx::pg_sys::ProcessConfigFile(pgrx::pg_sys::GucContext::PGC_SIGHUP);
            }
        }

        // Reload inbound mappings every ~500 ms to reduce idle transaction
        // overhead while staying responsive to config changes.
        if last_inbound_reload.elapsed() >= Duration::from_millis(500) {
            load_inbound_mappings();
            last_inbound_reload = std::time::Instant::now();
        }

        // ── MQTT: accept raw TCP, accept WebSocket, poll ──
        let mut session_db_actions = Vec::new();
        let mut pending_inbound_writes: Vec<inbound_map::PendingInboundWrite> = Vec::new();
        let mut publishes = Vec::new();

        if let Some(ref listener) = mqtt_listener {
            accept_mqtt_connections(
                listener,
                &mut clients,
                &mut session_db_actions,
                &mut publishes,
            );
        }
        if let Some(ref listener) = ws_listener {
            accept_ws_connections(
                listener,
                &mut clients,
                &mut session_db_actions,
                &mut publishes,
            );
        }
        if let Some(ref listener) = mqtts_listener {
            if let Some(ref cfg) = tls_config {
                accept_mqtts_connections(
                    listener,
                    cfg,
                    &mut clients,
                    &mut session_db_actions,
                    &mut publishes,
                );
            }
        }
        if let Some(ref listener) = wss_listener {
            if let Some(ref cfg) = tls_config {
                accept_wss_connections(
                    listener,
                    cfg,
                    &mut clients,
                    &mut session_db_actions,
                    &mut publishes,
                );
            }
        }
        poll_mqtt_clients(
            &mut clients,
            &mut publishes,
            &mut session_db_actions,
            &mut pending_inbound_writes,
        );

        // Execute inbound writes (MQTT → PostgreSQL) before CDC and message delivery
        execute_inbound_writes(pending_inbound_writes);

        if tick % crate::get_cdc_every_n_ticks_guc() == 0 {
            let cdc_messages = cdc_tick(slot_name);
            if !cdc_messages.is_empty() {
                deliver_messages(
                    &cdc_messages,
                    &mut clients,
                    &mut publishes,
                    &mut session_db_actions,
                );
            }
        }

        // Periodically resend unacked QoS 1 messages (5s timeout; checking every 1s is sufficient)
        if last_redeliver_check.elapsed() >= Duration::from_secs(1) {
            redeliver_unacked_messages(&mut clients, &mut publishes, &mut session_db_actions);
            last_redeliver_check = std::time::Instant::now();
        }

        publish_messages_batch(publishes, &mut clients, &mut session_db_actions);

        // Virtual subscriber: drain QoS 1 inbound-pending rows every tick so
        // that callers receive durable delivery (row in target table) in the
        // same tick as the PUBACK.  The SELECT is a no-op when the table is
        // empty, so per-tick overhead is negligible.
        process_inbound_pending();

        if last_session_sweep.elapsed() >= Duration::from_millis(500) {
            sweep_expired_sessions(&mut session_db_actions);
            last_session_sweep = std::time::Instant::now();
        }

        // Execute all collected session DB actions in one transaction
        execute_session_db_actions(session_db_actions);

        // ── Enterprise metrics flush ──────────────────────────────────────────
        if crate::license::has_feature(crate::license::Feature::Metrics) {
            let snap_interval = crate::get_metrics_snapshot_interval_guc();
            if snap_interval > 0 && last_metrics_flush.elapsed().as_secs() >= snap_interval as u64 {
                let snap = crate::metrics::MetricsSnapshot::capture();
                flush_metrics_snapshot(&snap);
                last_metrics_flush = std::time::Instant::now();
            }
            let conn_interval = crate::get_metrics_connections_cache_interval_guc();
            if conn_interval > 0
                && last_connections_flush.elapsed().as_secs() >= conn_interval as u64
            {
                flush_connections_cache(&clients);
                last_connections_flush = std::time::Instant::now();
            }
        }
    }

    // Graceful shutdown: send DISCONNECT to all clients, fire will messages,
    // and persist session state so reconnecting clients find correct disconnected_at.
    log!("pgmqtt mqtt+cdc: SIGTERM received, shutting down gracefully");
    let mut shutdown_db_actions = Vec::new();
    let mut will_publishes = Vec::new();

    for (id, mut client) in clients.drain() {
        // MQTT 5.0 §3.14: server MUST send DISCONNECT before closing the network connection.
        let _ = client.transport.write_all(&mqtt::build_disconnect(
            mqtt::reason::SERVER_SHUTTING_DOWN,
            client.v5(),
        ));

        // Server-initiated disconnect triggers the will message (MQTT 5.0 §3.1.3.3).
        // The client did NOT send a normal DISCONNECT, so the will fires.
        if let Some(will) = client.will.take() {
            log!("pgmqtt mqtt: firing Will for '{}' on shutdown", id);
            crate::metrics::inc(&crate::metrics::get().wills_fired);
            will_publishes.push(PendingPublish {
                topic: Arc::from(will.topic.as_str()),
                payload: Arc::from(will.payload),
                qos: will.qos,
                retain: will.retain,
                log_sender: format!("{} (Will/shutdown)", id),
                packet_id: None,
                inbound_mappings: Vec::new(),
            });
        }

        // Remove in-memory subscriptions and mark session state.
        subscriptions::remove_client(&id);
        if client.session.expiry_interval == 0 {
            shutdown_db_actions.push(SessionDbAction::DeleteSession { client_id: id });
        } else {
            client.session.disconnected_at = Some(std::time::Instant::now());
            with_sessions(|s| {
                s.insert(id.clone(), client.session);
            });
            shutdown_db_actions.push(SessionDbAction::MarkDisconnected { client_id: id });
        }
    }

    // Persist will messages (QoS ≥ 1 wills land in pgmqtt_messages so reconnecting
    // subscribers receive them; QoS 0 wills are fire-and-forget as per spec).
    if !will_publishes.is_empty() {
        let mut no_clients: HashMap<String, MqttClient> = HashMap::new();
        publish_messages_batch(will_publishes, &mut no_clients, &mut shutdown_db_actions);
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
/// Returns all rendered messages ready for delivery. The caller is responsible
/// for passing them to `deliver_messages`.
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
/// and returned after commit (fire-and-forget).
fn cdc_tick(slot_name: &str) -> Vec<MqttMessage> {
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
                        let s: String = row
                            .get_by_name("schema_name")
                            .ok()
                            .flatten()
                            .unwrap_or_default();
                        let t: String = row
                            .get_by_name("table_name")
                            .ok()
                            .flatten()
                            .unwrap_or_default();
                        let mn: String = row
                            .get_by_name("mapping_name")
                            .ok()
                            .flatten()
                            .unwrap_or_else(|| "default".to_string());
                        let tt: String = row
                            .get_by_name("topic_template")
                            .ok()
                            .flatten()
                            .unwrap_or_default();
                        let pt: String = row
                            .get_by_name("payload_template")
                            .ok()
                            .flatten()
                            .unwrap_or_default();
                        let q: i32 = row.get_by_name("qos").ok().flatten().unwrap_or_default();
                        rows.push(topic_map::TopicMapping {
                            name: mn,
                            schema: s,
                            table: t,
                            topic_template: tt,
                            payload_template: pt,
                            qos: q as u8,
                        });
                    }
                }
                Ok::<_, spi::Error>(rows)
            }) {
                let count = mappings.len();
                let mapped_set = mappings
                    .iter()
                    .map(|m| (m.schema.clone(), m.table.clone()))
                    .collect::<std::collections::HashSet<_>>();
                topic_map::set_mappings(mappings);
                crate::ring_buffer::mapped_tables_init(mapped_set);
                log!(
                    "pgmqtt: loaded {} topic mappings from slot checkpoint",
                    count
                );
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
    // Messages are collected into `all_messages` and returned to the caller
    // only after the transaction commits, ensuring QOS ≥ 1 messages are
    // durably persisted before delivery.
    let mut all_messages: Vec<MqttMessage> = Vec::new();
    loop {
        let (to_publish, batch_count, batch_ok) = BackgroundWorker::transaction(
            || -> (Vec<MqttMessage>, usize, bool) {
                let mut to_publish: Vec<MqttMessage> = Vec::new();
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
                    // Suppress PostgreSQL's "starting logical decoding" LOG messages
                    // that fire on every slot read (every 80ms).  These are informational
                    // and extremely noisy in production.  In PostgreSQL's log_min_messages
                    // hierarchy, LOG sits above ERROR, so we need 'fatal' to suppress it.
                    // The 'true' flag makes this local to the current transaction only.
                    let _ = client.select(
                        "SELECT set_config('log_min_messages', 'fatal', true)",
                        None,
                        &[],
                    );
                    let mut n = 0usize;
                    let table = client.select(&advance_query, None, &[])?;
                    for _ in table {
                        n += 1;
                    }
                    Ok::<usize, spi::Error>(n)
                }) {
                    Ok(n) => {
                        batch_count = n;
                        if n > 0 {
                            log!("pgmqtt: slot batch fetched {} raw logical messages", n);
                            crate::metrics::add(
                                &crate::metrics::get().cdc_events_processed,
                                n as u64,
                            );
                        }
                    }
                    Err(e) => {
                        log!("pgmqtt: error advancing slot: {:?} — skipping batch", e);
                        crate::metrics::inc(&crate::metrics::get().cdc_slot_errors);
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
                                // Only remove from the fast-path set if no other
                                // mappings remain for this (schema, table) pair.
                                if !topic_map::has_any_mapping(&schema, &table) {
                                    crate::ring_buffer::mapped_table_remove(&schema, &table);
                                }
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
                                };
                                topic_map::wal_upsert(mapping);
                                crate::ring_buffer::mapped_table_add(&schema, &table);
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
                                &change.schema,
                                &change.table,
                                change.op,
                                &change.columns,
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
                                if !subscriptions::has_subscribers(&topic_str) {
                                    log!(
                                        "pgmqtt cdc: no subscribers for rendered topic '{}', skipping",
                                        topic_str
                                    );
                                    continue;
                                }

                                if rendered.qos > 0 {
                                    // Persist within this transaction — committed atomically
                                    // with the slot advance above.  The subtransaction ensures a
                                    // PostgreSQL error inside persist_message is caught and counted
                                    // without crashing the background worker; the outer batch still
                                    // rolls back so events are retried on the next tick.
                                    let result = with_subtransaction(|| {
                                        pgrx::spi::Spi::connect_mut(|client| {
                                            let msg_id = db_action::persist_message(
                                                client,
                                                &topic_str,
                                                &rendered.payload,
                                                rendered.qos,
                                                false,
                                            )?;
                                            Ok::<_, spi::Error>(Some(msg_id))
                                        })
                                    });

                                    match result {
                                        Ok(msg_id) => {
                                            log!(
                                                "pgmqtt cdc: persisted QOS {} to '{}' (msg_id={:?})",
                                                rendered.qos,
                                                rendered.topic,
                                                msg_id
                                            );
                                            to_publish.push(MqttMessage {
                                                id: msg_id,
                                                topic: rendered.topic,
                                                payload: rendered.payload,
                                                qos: rendered.qos,
                                            });
                                            crate::metrics::inc(
                                                &crate::metrics::get().cdc_msgs_published,
                                            );
                                        }
                                        Err(e) => {
                                            log!(
                                                "pgmqtt cdc: error persisting QOS {} to '{}': {:?} \
                                                 — batch rolls back, events retried next tick",
                                                rendered.qos,
                                                topic_str,
                                                e
                                            );
                                            crate::metrics::inc(&crate::metrics::get().cdc_persist_errors);
                                            return (Vec::new(), batch_count, false);
                                        }
                                    }
                                } else {
                                    // QOS 0 — fire-and-forget, pushed after commit.
                                    to_publish.push(MqttMessage {
                                        id: None,
                                        topic: rendered.topic,
                                        payload: rendered.payload,
                                        qos: 0,
                                    });
                                    crate::metrics::inc(&crate::metrics::get().cdc_msgs_published);
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
            },
        );
        // ↑ COMMIT: slot LSN advances IFF all QOS ≥ 1 inserts committed.
        //   batch_ok=false means the transaction rolled back; slot unchanged.

        if !batch_ok {
            log!("pgmqtt cdc: batch transaction failed or rolled back — events will be retried");
        }

        if batch_ok {
            all_messages.extend(to_publish);
        }

        // Stop when the batch was smaller than the limit — WAL fully drained.
        if batch_count < CDC_BATCH_SIZE {
            break;
        }
    }
    all_messages
}
/// Accept new MQTTS (TCP + TLS) connections and perform the MQTT CONNECT handshake.
fn accept_mqtts_connections(
    listener: &TcpListener,
    tls_config: &Arc<rustls::ServerConfig>,
    clients: &mut HashMap<String, MqttClient>,
    session_db_actions: &mut Vec<SessionDbAction>,
    pending_publishes: &mut Vec<PendingPublish>,
) {
    loop {
        match listener.accept() {
            Ok((stream, addr)) => {
                log!("pgmqtt mqtts: new MQTTS connection from {}", addr);
                match Transport::new_tls(stream, tls_config.clone()) {
                    Ok(transport) => {
                        handle_new_connection(
                            transport,
                            None,
                            clients,
                            session_db_actions,
                            pending_publishes,
                        );
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
    pending_publishes: &mut Vec<PendingPublish>,
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
                        log!(
                            "pgmqtt wss: failed to create TLS session for {}: {}",
                            addr,
                            e
                        );
                        continue;
                    }
                };
                let mut tls_stream = rustls::StreamOwned::new(conn, stream);

                match websocket::handshake(&mut tls_stream) {
                    Ok((leftover, ws_jwt)) => {
                        log!("pgmqtt wss: WSS upgrade complete for {}", addr);
                        let ws = websocket::WsStream::new(tls_stream, leftover);
                        handle_new_connection(
                            Transport::Wss(ws),
                            ws_jwt,
                            clients,
                            session_db_actions,
                            pending_publishes,
                        );
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
    pending_publishes: &mut Vec<PendingPublish>,
) {
    loop {
        match listener.accept() {
            Ok((stream, addr)) => {
                log!("pgmqtt mqtt: new TCP connection from {}", addr);
                handle_new_connection(
                    Transport::Raw(stream),
                    None,
                    clients,
                    session_db_actions,
                    pending_publishes,
                );
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
    pending_publishes: &mut Vec<PendingPublish>,
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
                        handle_new_connection(
                            Transport::Ws(ws),
                            ws_jwt,
                            clients,
                            session_db_actions,
                            pending_publishes,
                        );
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
    pending_publishes: &mut Vec<PendingPublish>,
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

        // Pass version=5 for initial CONNECT parse; parse_connect self-detects the real version.
        match mqtt::parse_packet(&connect_buf, 5) {
            Ok((mqtt::InboundPacket::Connect(connect), _consumed)) => {
                // Success! Proceed to register the client.
                finish_connect(
                    transport,
                    connect,
                    ws_jwt,
                    clients,
                    session_db_actions,
                    pending_publishes,
                );
                return;
            }
            Ok(_) => {
                log!("pgmqtt mqtt: expected CONNECT, got something else");
                return;
            }
            Err(mqtt::MqttError::Incomplete) => {
                // Keep reading
                if connect_buf.len() > MAX_PRE_CONNECT_BYTES {
                    log!("pgmqtt mqtt: CONNECT packet too large");
                    return;
                }
                continue;
            }
            Err(e) => {
                log!("pgmqtt mqtt: CONNECT parse error: {}", e);
                // We don't know the client's version yet — try to extract it from the raw
                // buffer. A well-formed CONNECT has: 2-byte fixed header + "MQTT" (2-byte
                // len + 4 bytes) = 8 bytes before the protocol version byte at offset 8.
                // For a malformed packet this may be wrong, so we default to false (v3.1.1)
                // rather than risk sending a v5-format CONNACK to a v3 client.
                let err_v5 = connect_buf.get(8).copied() == Some(5);
                let _ = transport.write_all(&mqtt::build_connack(
                    false,
                    mqtt::reason::MALFORMED_PACKET,
                    err_v5,
                ));
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
    pending_publishes: &mut Vec<PendingPublish>,
) {
    let v5 = mqtt::is_v5(packet.protocol_version);

    // MQTT 3.1.1 §3.1.3.1: an empty client ID with clean_session=0 MUST be rejected.
    // (v5 permits empty client IDs with auto-assignment regardless of clean_start.)
    if !v5 && packet.client_id.is_empty() && !packet.clean_start {
        log!(
            "pgmqtt mqtt: MQTT 3.1.1 client sent empty client ID with clean_session=0 — rejecting"
        );
        let _ = transport.write_all(&mqtt::build_connack(
            false,
            mqtt::reason::CLIENT_IDENTIFIER_NOT_VALID,
            false,
        ));
        return;
    }

    let client_id = if packet.client_id.is_empty() {
        let id = NEXT_AUTO_CLIENT_ID.fetch_add(1, Ordering::Relaxed);
        format!("pgmqtt-auto-{}", id)
    } else {
        packet.client_id.clone()
    };
    log!(
        "pgmqtt mqtt: CONNECT from client '{}' (MQTT {})",
        client_id,
        if v5 { "5.0" } else { "3.1.1" }
    );

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
                            let _ = transport.write_all(&mqtt::build_connack(
                                false,
                                mqtt::reason::NOT_AUTHORIZED,
                                v5,
                            ));
                            return;
                        }
                    }
                    jwt_sub_claims = claims.sub_claims;
                    jwt_pub_claims = claims.pub_claims;
                    log!("pgmqtt mqtt: JWT validated for '{}'", client_id);
                }
                Err(e) => {
                    log!(
                        "pgmqtt mqtt: JWT validation failed for '{}': {}",
                        client_id,
                        e
                    );
                    if jwt_required {
                        let _ = transport.write_all(&mqtt::build_connack(
                            false,
                            mqtt::reason::NOT_AUTHORIZED,
                            v5,
                        ));
                        crate::metrics::inc(&crate::metrics::get().connections_rejected);
                        return;
                    }
                }
            },
            (None, _) => {
                if jwt_required {
                    log!(
                        "pgmqtt mqtt: JWT required but no token provided for '{}'",
                        client_id
                    );
                    let _ = transport.write_all(&mqtt::build_connack(
                        false,
                        mqtt::reason::NOT_AUTHORIZED,
                        v5,
                    ));
                    crate::metrics::inc(&crate::metrics::get().connections_rejected);
                    return;
                }
            }
            (_, None) => {
                log!("pgmqtt mqtt: failed to parse JWT public key GUC");
                if jwt_required {
                    let _ = transport.write_all(&mqtt::build_connack(
                        false,
                        mqtt::reason::NOT_AUTHORIZED,
                        v5,
                    ));
                    crate::metrics::inc(&crate::metrics::get().connections_rejected);
                    return;
                }
            }
        }
    }

    // ── Session takeover (MQTT 5.0 §4.9) ──────────────────────────────────────
    // If the ClientID represents a Client already connected, send DISCONNECT
    // with reason code 0x8E (Session taken over), fire its Will, then close
    // the old connection before proceeding.
    if let Some(mut old_client) = clients.remove(&client_id) {
        log!(
            "pgmqtt mqtt: session takeover for '{}', disconnecting old connection",
            client_id
        );
        // Bypasses disconnect_client(), so do its metric bookkeeping inline.
        {
            let m = crate::metrics::get();
            crate::metrics::dec(&m.connections_current);
            crate::metrics::inc(&m.disconnections_unclean);
        }
        let _ = old_client.transport.write_all(&mqtt::build_disconnect(
            mqtt::reason::SESSION_TAKEN_OVER,
            old_client.v5(),
        ));
        // Fire the old client's Will (MQTT 5.0 §3.1.3.3: Will is published
        // when the server closes the connection for any reason other than a
        // normal DISCONNECT from the client).
        if let Some(will) = old_client.will.take() {
            crate::metrics::inc(&crate::metrics::get().wills_fired);
            pending_publishes.push(PendingPublish {
                topic: Arc::from(will.topic.as_str()),
                payload: Arc::from(will.payload),
                qos: will.qos,
                retain: will.retain,
                log_sender: format!("{} (Will/takeover)", client_id),
                packet_id: None,
                inbound_mappings: Vec::new(),
            });
        }

        // Session takeover acts as a disconnect for the old connection.
        // If the old session had expiry_interval == 0 (end at disconnect),
        // clean it up now so that the session_present check below is correct
        // (MQTT 5.0 §3.2.2.1.1).
        if old_client.session.expiry_interval == 0 {
            subscriptions::remove_client(&client_id);
            session_db_actions.push(SessionDbAction::DeleteSession {
                client_id: client_id.clone(),
            });
        } else {
            // Persist the old session for the new connection to resume.
            old_client.session.disconnected_at = Some(std::time::Instant::now());
            with_sessions(|s| {
                s.insert(client_id.clone(), old_client.session);
            });
        }
    }

    // ── Connection limit enforcement ──────────────────────────────────────────
    // A genuinely new client is rejected when the limit is reached so operators
    // can control memory usage.  Session takeovers (handled above) always proceed.
    let limit = crate::license::max_connections();
    if !clients.contains_key(&client_id) && clients.len() >= limit {
        log!(
            "pgmqtt mqtt: connection limit ({}) reached, rejecting '{}'",
            limit,
            client_id
        );
        let _ = transport.write_all(&mqtt::build_connack(
            false,
            mqtt::reason::QUOTA_EXCEEDED,
            v5,
        ));
        crate::metrics::inc(&crate::metrics::get().connections_rejected);
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

    // Check for an existing disconnected session to resume.
    let (session_present, mut session) = with_sessions(|s| {
        if let Some(sess) = s.remove(&client_id) {
            (true, sess)
        } else {
            (false, MqttSession::new())
        }
    });
    {
        let m = crate::metrics::get();
        if session_present {
            crate::metrics::inc(&m.sessions_resumed);
        } else {
            crate::metrics::inc(&m.sessions_created);
        }
    }

    // Stamp the new connection's properties onto the session.
    session.expiry_interval = packet.session_expiry_interval;
    session.receive_maximum = packet.receive_maximum;
    session.disconnected_at = None;

    let next_pid = session.next_packet_id;
    let expiry = session.expiry_interval;
    session_db_actions.push(SessionDbAction::UpsertSession {
        client_id: client_id.clone(),
        next_packet_id: next_pid,
        expiry_interval: expiry,
    });

    let connack = mqtt::build_connack_with_max_packet(
        session_present,
        mqtt::reason::SUCCESS,
        v5,
        if v5 { Some(broker_max_packet_size()) } else { None },
    );
    if transport.write_all(&connack).is_err() {
        log!("pgmqtt mqtt: failed to send CONNACK to '{}'", client_id);
        // Put session back so it isn't lost.
        if session_present {
            with_sessions(|s| {
                s.insert(client_id.clone(), session);
            });
        }
        return;
    }

    {
        let m = crate::metrics::get();
        crate::metrics::inc(&m.connections_accepted);
        crate::metrics::inc(&m.connections_current);
    }

    // Set non-blocking for ongoing reads
    let _ = transport.set_nonblocking(true);
    log!("pgmqtt mqtt: client '{}' ready for polling", client_id);

    // ── Session resumption: redeliver inflight + drain queue ─────────────────
    // MQTT 5.0 §4.4: The broker MUST retransmit all unacknowledged PUBLISH
    // packets (with DUP=1) and deliver any queued messages after a reconnect
    // with session_present=true.
    let mut to_send: Vec<Vec<u8>> = Vec::new();
    if session_present {
        // MQTT-3.3.4-7: cap concurrent on-wire unacked PUBLISHes at the new
        // receive_maximum. Excess stays inflight with stale sent_at, picked
        // up by redeliver_unacked_messages as ACKs free capacity.
        let resume_budget = session.receive_maximum as usize;
        let client_max_pkt = packet.max_packet_size;
        let mut pids: Vec<u16> = session.inflight.keys().copied().collect();
        pids.sort_unstable();
        let now = std::time::Instant::now();
        let mut sent = 0;

        // MQTT-3.1.2.24-2: discard inflight entries the client cannot receive,
        // treating them as delivered so packet IDs and DB rows are freed.
        let oversized: Vec<(u16, Option<i64>)> = pids
            .iter()
            .filter_map(|pid| {
                session.inflight.get(pid).and_then(|(topic, payload, msg_id, _)| {
                    let pkt =
                        mqtt::build_publish(topic, payload, 1, Some(*pid), true, false, v5);
                    if matches!(client_max_pkt, Some(m) if pkt.len() > m as usize) {
                        Some((*pid, *msg_id))
                    } else {
                        None
                    }
                })
            })
            .collect();
        for (pid, msg_id) in oversized {
            session.inflight.remove(&pid);
            if let Some(mid) = msg_id {
                session_db_actions.push(SessionDbAction::DeleteMessage {
                    client_id: client_id.clone(),
                    message_id: mid,
                });
            }
        }

        for pid in pids {
            if sent >= resume_budget {
                break;
            }
            if let Some(entry) = session.inflight.get_mut(&pid) {
                let (topic, payload, _msg_id, sent_at) = entry;
                let pkt = mqtt::build_publish(topic, payload, 1, Some(pid), true, false, v5);
                to_send.push(pkt);
                *sent_at = now;
                sent += 1;
            }
        }
        if session.inflight.len() > resume_budget {
            log!(
                "pgmqtt mqtt: '{}' resume: {} inflight exceeds receive_maximum={}, deferring {} until ACKs free quota",
                client_id,
                session.inflight.len(),
                resume_budget,
                session.inflight.len() - resume_budget,
            );
        }
        // 2. Drain the pending queue into inflight and add to send list.
        let inflight_limit = std::cmp::min(MAX_INFLIGHT_MESSAGES, session.receive_maximum as usize);
        while session.inflight.len() < inflight_limit {
            if let Some(queued) = session.queue_pop_front() {
                let Some(pid) = session.alloc_packet_id() else {
                    // All 65535 ids occupied — push back and stop draining.
                    session.queue_push_front(queued);
                    break;
                };
                let pkt = mqtt::build_publish(&queued.topic, &queued.payload, 1, Some(pid), false, false, v5);
                if matches!(client_max_pkt, Some(m) if pkt.len() > m as usize) {
                    // MQTT-3.1.2.24-2: behave as if delivered.
                    if let Some(mid) = queued.id {
                        session_db_actions.push(SessionDbAction::DeleteMessage {
                            client_id: client_id.clone(),
                            message_id: mid,
                        });
                    }
                    continue;
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
                to_send.push(pkt);
            } else {
                break;
            }
        }
    }

    let transport_label: &'static str = match &transport {
        Transport::Raw(_) => "mqtt",
        Transport::Ws(_) => "ws",
        Transport::Tls(_) => "mqtts",
        Transport::Wss(_) => "wss",
    };
    let mut mqtt_client = MqttClient::new(
        transport,
        client_id.clone(),
        packet.will,
        packet.keep_alive,
        packet.receive_maximum,
        packet.max_packet_size,
        packet.protocol_version,
        session,
        transport_label,
    );
    mqtt_client.sub_claims = jwt_sub_claims;
    mqtt_client.pub_claims = jwt_pub_claims;
    clients.insert(client_id.clone(), mqtt_client);

    if !to_send.is_empty() {
        log!(
            "pgmqtt mqtt: resuming session for '{}': redelivering {} packet(s)",
            client_id,
            to_send.len()
        );
        let max_buf = crate::get_max_client_buffer_bytes_guc();
        let mut overflow = false;
        if let Some(client) = clients.get_mut(&client_id) {
            for pkt in to_send {
                if client.write_buf.len() + pkt.len() > max_buf {
                    log!("pgmqtt mqtt: client '{}' write buffer full during session resume. Disconnecting.", client_id);
                    overflow = true;
                    break;
                }
                let _ = client.try_write(&pkt);
            }
        }
        if overflow {
            disconnect_client(&client_id, clients, pending_publishes, session_db_actions);
        }
    }
}

struct PendingPublish {
    topic: Arc<str>,
    payload: Arc<[u8]>,
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
        let m = crate::metrics::get();
        crate::metrics::dec(&m.connections_current);
        if client.clean_disconnect {
            crate::metrics::inc(&m.disconnections_clean);
        } else {
            crate::metrics::inc(&m.disconnections_unclean);
        }
        if let Some(will) = client.will.take() {
            log!(
                "pgmqtt mqtt: client '{}' disconnected unexpectedly, buffering Will",
                id
            );
            crate::metrics::inc(&m.wills_fired);
            pending_publishes.push(PendingPublish {
                topic: Arc::from(will.topic.as_str()),
                payload: Arc::from(will.payload),
                qos: will.qos,
                retain: will.retain,
                log_sender: format!("{} (Will)", id),
                packet_id: None,
                inbound_mappings: Vec::new(),
            });
        }

        // Mark session as disconnected so the sweeper can reap it later.
        // If expiry_interval == 0 the session should end immediately.
        if client.session.expiry_interval == 0 {
            subscriptions::remove_client(id);
            session_db_actions.push(SessionDbAction::DeleteSession {
                client_id: id.to_string(),
            });
        } else {
            // Move session to the disconnected-sessions store for later
            // reconnection or expiry sweep.
            client.session.disconnected_at = Some(std::time::Instant::now());
            with_sessions(|s| {
                s.insert(id.to_string(), client.session);
            });
            session_db_actions.push(SessionDbAction::MarkDisconnected {
                client_id: id.to_string(),
            });
        }
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
    let max_inbound_buf = crate::get_max_client_buffer_bytes_guc();

    for (client_id, client) in clients.iter_mut() {
        // Flush any bytes buffered from the previous tick before reading.
        if !client.flush_write_buf() {
            to_remove.push(client_id.clone());
            continue;
        }

        // Drain-loop read: pull all available bytes from the socket in 64 KiB
        // chunks until WouldBlock, bounded by max_client_buffer_bytes.
        // When the cap is reached we stop reading; excess data stays in the
        // kernel TCP buffer and is consumed on the next tick.
        let mut skip_processing = false;
        loop {
            // Stop reading if we've accumulated enough for this tick.
            if client.buf.len() >= max_inbound_buf {
                break;
            }
            let mut tmp = [0u8; READ_CHUNK_BYTES];
            match client.transport.read(&mut tmp) {
                Ok(0) => {
                    // EOF: client closed the connection. Mark for removal but
                    // still process any packets already buffered — the FIN
                    // arrives after the last data segment.
                    log!("pgmqtt mqtt: client '{}' disconnected (EOF)", client_id);
                    to_remove.push(client_id.clone());
                    break;
                }
                Ok(n) => {
                    client.buf.extend_from_slice(&tmp[..n]);
                    client.last_received_at = std::time::Instant::now();
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    break; // No more data available this tick
                }
                Err(e) => {
                    // Read error: data may be corrupt, skip packet processing.
                    log!("pgmqtt mqtt: read error from '{}': {}", client_id, e);
                    to_remove.push(client_id.clone());
                    skip_processing = true;
                    break;
                }
            }
        }
        if skip_processing {
            continue;
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
                    let _ = client.transport.write_all(&mqtt::build_disconnect(
                        mqtt::reason::MALFORMED_PACKET,
                        client.v5(),
                    ));
                    to_remove.push(client_id.clone());
                    break;
                }
            };

            let pkt_data: Vec<u8> = client.buf.drain(..pkt_len).collect();

            match mqtt::parse_packet(&pkt_data, client.protocol_version) {
                Ok((packet, _)) => {
                    if !handle_mqtt_packet(
                        client,
                        packet,
                        pending_publishes,
                        session_db_actions,
                        pending_inbound_writes,
                    ) {
                        to_remove.push(client_id.clone());
                        break;
                    }
                }
                Err(e) => {
                    log!("pgmqtt mqtt: parse error from '{}': {}", client_id, e);
                    let _ = client.transport.write_all(&mqtt::build_disconnect(
                        mqtt::reason::MALFORMED_PACKET,
                        client.v5(),
                    ));
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

    // Clean up disconnected clients (deduplicate to avoid double Will firing)
    to_remove.sort_unstable();
    to_remove.dedup();
    for id in to_remove {
        disconnect_client(&id, clients, pending_publishes, session_db_actions);
    }
}

fn publish_messages_batch(
    pending: Vec<PendingPublish>,
    clients: &mut HashMap<String, MqttClient>,
    session_db_actions: &mut Vec<SessionDbAction>,
) {
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

                    // MQTT-3.3.1-6/7/10: clear pgmqtt_retained, then persist a
                    // non-retained row at QoS 1 so the forwarded clear has DB
                    // backing for reconnect redelivery.
                    if p.retain && p.payload.is_empty() {
                        let topic_ref: &str = &p.topic;
                        let args: Vec<pgrx::datum::DatumWithOid> =
                            vec![topic_ref.into()];
                        let table = client.update(
                            "DELETE FROM pgmqtt_retained WHERE topic = $1 RETURNING message_id",
                            None,
                            &args,
                        )?;
                        for row in table {
                            if let Ok(Some(old_id)) = row.get_by_name::<i64, _>("message_id") {
                                db_action::cleanup_orphaned_message(client, old_id)?;
                            }
                        }
                        if p.qos > 0 {
                            let msg_id = db_action::persist_message(
                                client,
                                &p.topic,
                                &p.payload,
                                p.qos,
                                false,
                            )?;
                            msg_id_opt = Some(msg_id);
                        }
                    } else {
                        // Normal publish: persist the message and update retained index.
                        let msg_id = db_action::persist_message(
                            client,
                            &p.topic,
                            &p.payload,
                            p.qos,
                            p.retain,
                        )?;
                        msg_id_opt = Some(msg_id);

                        if p.retain {
                            let topic_ref: &str = &p.topic;
                            let args: Vec<pgrx::datum::DatumWithOid> =
                                vec![topic_ref.into(), msg_id.into()];
                            let table = client.update(
                                "WITH old AS ( \
                                     SELECT message_id AS old_id FROM pgmqtt_retained WHERE topic = $1 \
                                 ), upsert AS ( \
                                     INSERT INTO pgmqtt_retained (topic, message_id) VALUES ($1, $2) \
                                     ON CONFLICT (topic) DO UPDATE SET message_id = EXCLUDED.message_id \
                                     RETURNING 1 \
                                 ) \
                                 SELECT old_id FROM old, upsert",
                                None,
                                &args,
                            )?;
                            let mut old_msg_id: Option<i64> = None;
                            for row in table {
                                if let Ok(Some(id)) = row.get_by_name::<i64, _>("old_id") {
                                    old_msg_id = Some(id);
                                }
                            }
                            if let Some(old_id) = old_msg_id {
                                if old_id != msg_id {
                                    db_action::cleanup_orphaned_message(client, old_id)?;
                                }
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

                    if crate::get_debug_log_guc() {
                        log!(
                            "pgmqtt: pushing message from '{}' to topic '{}' with qos={}",
                            p.log_sender,
                            p.topic,
                            p.qos
                        );
                    }
                    to_publish.push(MqttMessage {
                        id: msg_id_opt,
                        topic: p.topic.clone(),
                        payload: p.payload.clone(),
                        qos: p.qos,
                    });
                }
                Ok::<_, pgrx::spi::Error>(())
            })?;
            Ok::<(Vec<MqttMessage>, bool), pgrx::spi::Error>((to_publish, true))
        })
        .unwrap_or((Vec::new(), false));

        if ok {
            // Deliver directly to subscribers.
            let mut cascade = Vec::new();
            deliver_messages(&to_publish, clients, &mut cascade, session_db_actions);

            // Send PUBACKs only after successful commit — MQTT at-least-once semantics.
            for p in persistent {
                if p.qos == 1 {
                    if let Some(pid) = p.packet_id {
                        if let Some(client) = clients.get_mut(&p.log_sender) {
                            let puback = mqtt::build_puback(pid);
                            let _ = client.transport.write_all(&puback);
                            crate::metrics::inc(&crate::metrics::get().pubacks_sent);
                        }
                    }
                }
            }

            // Handle will messages from cascading disconnects (rare: socket
            // write failure during delivery).  Bounded by number of clients.
            if !cascade.is_empty() {
                publish_messages_batch(cascade, clients, session_db_actions);
            }
        }
    }

    if !transient.is_empty() {
        let transient_msgs: Vec<MqttMessage> = transient
            .into_iter()
            .map(|p| {
                log!(
                    "pgmqtt: pushing transient message from '{}' to topic '{}' with qos=0",
                    p.log_sender,
                    p.topic
                );
                MqttMessage {
                    id: None,
                    topic: p.topic,
                    payload: p.payload,
                    qos: 0,
                }
            })
            .collect();
        let mut cascade = Vec::new();
        deliver_messages(&transient_msgs, clients, &mut cascade, session_db_actions);
        if !cascade.is_empty() {
            publish_messages_batch(cascade, clients, session_db_actions);
        }
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
    match packet {
        mqtt::InboundPacket::Puback(packet_id) => {
            crate::metrics::inc(&crate::metrics::get().pubacks_received);
            let res = client.session.inflight.remove(&packet_id);
            if let Some((_, _, msg_id, _)) = res {
                if crate::get_debug_log_guc() {
                    log!(
                        "pgmqtt mqtt: '{}' acked packet_id={} (msg_id={:?})",
                        client_id,
                        packet_id,
                        msg_id
                    );
                }
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
            let v5 = client.v5();
            let client_max_pkt = client.max_packet_size;
            let session = &mut client.session;
            let inflight_limit =
                std::cmp::min(MAX_INFLIGHT_MESSAGES, session.receive_maximum as usize);
            let (next_pkt, db_action) =
                if session.inflight.len() < inflight_limit && !session.queue.is_empty() {
                    if let Some(queued) = session.queue_pop_front() {
                        // MQTT-3.1.2.24-1: size-check before reserving an
                        // inflight slot, otherwise an oversize message wastes
                        // a slot every PUBACK forever.
                        let pkt = mqtt::build_publish(
                            &queued.topic,
                            &queued.payload,
                            1,
                            Some(0),
                            false,
                            false,
                            v5,
                        );
                        if matches!(client_max_pkt, Some(m) if pkt.len() > m as usize) {
                            (None, None)
                        } else if let Some(pid) = session.alloc_packet_id() {
                            session.inflight.insert(
                                pid,
                                (
                                    queued.topic.clone(),
                                    queued.payload.clone(),
                                    queued.id,
                                    std::time::Instant::now(),
                                ),
                            );
                            let pkt = mqtt::build_publish(
                                &queued.topic,
                                &queued.payload,
                                1,
                                Some(pid),
                                false,
                                false,
                                v5,
                            );
                            (Some(pkt), queued.id.map(|id| (id, pid)))
                        } else {
                            session.queue_push_front(queued);
                            (None, None)
                        }
                    } else {
                        (None, None)
                    }
                } else {
                    (None, None)
                };

            if let Some((msg_id, pid)) = db_action {
                session_db_actions.push(SessionDbAction::UpdateMessageInflight {
                    client_id: client_id.clone(),
                    message_id: msg_id,
                    packet_id: pid,
                });
            }

            if let Some(pkt) = next_pkt {
                let _ = client.transport.write_all(&pkt);
            }
            true
        }
        mqtt::InboundPacket::Subscribe(sub) => {
            crate::metrics::inc(&crate::metrics::get().subscribe_ops);
            let mut reason_codes = Vec::with_capacity(sub.topics.len());
            for (topic_filter, requested_qos) in &sub.topics {
                // Shared subscriptions are MQTT 5.0 only (§4.8.2).
                // Reject $share/ filters from v3.1.1 clients.
                if topic_filter.starts_with("$share/") && !client.v5() {
                    log!(
                        "pgmqtt mqtt: MQTT 3.1.1 client '{}' attempted shared subscription '{}' (v5 only)",
                        client_id,
                        topic_filter
                    );
                    reason_codes.push(mqtt::reason::TOPIC_FILTER_INVALID);
                    continue;
                }
                let shared = subscriptions::parse_shared_filter(topic_filter);
                if topic_filter.starts_with("$share/") && shared.is_none() {
                    log!(
                        "pgmqtt mqtt: '{}' malformed shared subscription '{}'",
                        client_id,
                        topic_filter
                    );
                    reason_codes.push(mqtt::reason::TOPIC_FILTER_INVALID);
                    continue;
                }

                // JWT subscribe authorization — use the real filter for shared subs.
                // We need filter_covers_filter (not topic_matches_filter) because
                // auth_filter may contain wildcards.  The check is: "is the set of
                // topics matched by auth_filter a subset of some claim?"
                let auth_filter = shared.map(|(_, f)| f).unwrap_or(topic_filter.as_str());
                let jwt_authorized = if client.sub_claims.is_empty() {
                    true
                } else {
                    client
                        .sub_claims
                        .iter()
                        .any(|claim| crate::mqtt::filter_covers_filter(auth_filter, claim))
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
            let suback = mqtt::build_suback(sub.packet_id, &reason_codes, client.v5());
            if client.transport.write_all(&suback).is_err() {
                return false;
            }

            // Deliver retained messages for successfully subscribed filters (MQTT §3.8.4).
            // Shared subscriptions are excluded per MQTT 5.0 §4.8.2.
            // Each entry is (filter, granted_qos).
            let subscribed_filters: Vec<(&String, u8)> = sub
                .topics
                .iter()
                .zip(reason_codes.iter())
                .filter_map(|((filter, _), &rc)| {
                    if rc <= 0x02 && subscriptions::parse_shared_filter(filter).is_none() {
                        Some((filter, rc))
                    } else {
                        None
                    }
                })
                .collect();

            if !subscribed_filters.is_empty() {
                // Fetch retained messages with their stored QoS and message_id
                // (needed to track QoS 1 inflight state).
                let retained_msgs: Vec<(String, Vec<u8>, u8, i64)> =
                    BackgroundWorker::transaction(|| {
                        let mut results = Vec::new();
                        let _ = pgrx::spi::Spi::connect(|spi| {
                            if let Ok(table) = spi.select(
                                "SELECT r.topic, m.payload, m.qos, m.id \
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
                                    let qos: i32 =
                                        row.get_by_name("qos").ok().flatten().unwrap_or(0);
                                    let msg_id: i64 =
                                        row.get_by_name("id").ok().flatten().unwrap_or(0);
                                    if !topic.is_empty() {
                                        results.push((
                                            topic,
                                            payload.unwrap_or_default(),
                                            qos as u8,
                                            msg_id,
                                        ));
                                    }
                                }
                            }
                            Ok::<_, pgrx::spi::Error>(())
                        });
                        Ok::<Vec<(String, Vec<u8>, u8, i64)>, pgrx::spi::Error>(results)
                    })
                    .unwrap_or_default();

                // MQTT 5.0 §3.3.1.2: retained messages are delivered at
                // min(message_qos, subscription_granted_qos).
                for (topic, payload, msg_qos, msg_id) in &retained_msgs {
                    for (filter, sub_qos) in &subscribed_filters {
                        if mqtt::topic_matches_filter(topic, filter) {
                            let effective_qos = (*msg_qos).min(*sub_qos);
                            if effective_qos >= 1 {
                                // QoS 1 retained delivery — assign a packet ID and
                                // track in session inflight so PUBACKs are handled
                                // and redelivery works on reconnect.
                                let arc_topic: Arc<str> = Arc::from(topic.as_str());
                                let arc_payload: Arc<[u8]> = Arc::from(payload.as_slice());
                                let sess = &mut client.session;
                                let Some(pid) = sess.alloc_packet_id() else {
                                    // No free packet_id; skip this retained delivery.
                                    // Subscriber will see it on next reconnect via
                                    // pgmqtt_session_messages — but here, with no row
                                    // inserted, it is dropped.  Acceptable: clients
                                    // hitting this limit are already in trouble.
                                    break;
                                };
                                sess.inflight.insert(
                                    pid,
                                    (
                                        Arc::clone(&arc_topic),
                                        Arc::clone(&arc_payload),
                                        Some(*msg_id),
                                        std::time::Instant::now(),
                                    ),
                                );
                                session_db_actions.push(SessionDbAction::InsertMessageBatch {
                                    message_id: *msg_id,
                                    entries: vec![(client_id.clone(), Some(pid))],
                                });
                                let pkt = mqtt::build_publish(
                                    topic,
                                    payload,
                                    1,
                                    Some(pid),
                                    false,
                                    true,
                                    client.v5(),
                                );
                                if !client.exceeds_max_packet(pkt.len()) {
                                    let _ = client.transport.write_all(&pkt);
                                }
                            } else {
                                let pkt = mqtt::build_publish(
                                    topic,
                                    payload,
                                    0,
                                    None,
                                    false,
                                    true,
                                    client.v5(),
                                );
                                if !client.exceeds_max_packet(pkt.len()) {
                                    let _ = client.transport.write_all(&pkt);
                                }
                            }
                            break; // deliver each retained message at most once per SUBSCRIBE
                        }
                    }
                }
            }

            true
        }
        mqtt::InboundPacket::Unsubscribe(unsub) => {
            crate::metrics::inc(&crate::metrics::get().unsubscribe_ops);
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
            let unsuback = mqtt::build_unsuback(unsub.packet_id, &reason_codes, client.v5());
            if client.transport.write_all(&unsuback).is_err() {
                return false;
            }
            true
        }
        mqtt::InboundPacket::Pingreq => {
            let resp = mqtt::build_pingresp();
            client.transport.write_all(&resp).is_ok()
        }
        mqtt::InboundPacket::Disconnect(reason_code, expiry_override) => {
            log!(
                "pgmqtt mqtt: '{}' sent DISCONNECT (reason_code=0x{:02x})",
                client_id,
                reason_code
            );
            if reason_code == mqtt::reason::NORMAL_DISCONNECT {
                client.will = None;
                client.clean_disconnect = true;
            }
            // MQTT 5.0 §3.14.2.2.2: client may override Session Expiry Interval at DISCONNECT.
            // However, §3.14.2.2.2 also says: "If the Session Expiry Interval in the CONNECT
            // packet was zero, then it is a Protocol Error to set a non-zero Session Expiry
            // Interval in the DISCONNECT packet."
            if let Some(new_expiry) = expiry_override {
                let original_expiry = client.session.expiry_interval;
                let current_pid = client.session.next_packet_id;
                if original_expiry == 0 && new_expiry != 0 {
                    log!(
                        "pgmqtt mqtt: '{}' protocol error: cannot set non-zero session expiry at DISCONNECT when CONNECT had expiry=0",
                        client_id
                    );
                    let _ = client.transport.write_all(&mqtt::build_disconnect(
                        mqtt::reason::PROTOCOL_ERROR,
                        client.v5(),
                    ));
                } else {
                    client.session.expiry_interval = new_expiry;
                    session_db_actions.push(SessionDbAction::UpsertSession {
                        client_id: client_id.clone(),
                        next_packet_id: current_pid,
                        expiry_interval: new_expiry,
                    });
                }
            }
            false // signal removal
        }
        mqtt::InboundPacket::Publish(pub_pkt) => {
            // Track inbound bytes/message counts.
            {
                let m = crate::metrics::get();
                crate::metrics::inc(&m.msgs_received);
                match pub_pkt.qos {
                    0 => crate::metrics::inc(&m.msgs_received_qos0),
                    _ => crate::metrics::inc(&m.msgs_received_qos1),
                }
                crate::metrics::add(&m.bytes_received, pub_pkt.payload.len() as u64);
            }
            client.msgs_received_count += 1;
            client.bytes_received_count += pub_pkt.payload.len() as u64;
            // JWT publish authorization
            if !client.pub_claims.is_empty() {
                let authorized = client
                    .pub_claims
                    .iter()
                    .any(|claim| crate::mqtt::topic_matches_filter(&pub_pkt.topic, claim));
                if !authorized {
                    log!(
                        "pgmqtt mqtt: '{}' not authorized to publish to '{}'",
                        client_id,
                        pub_pkt.topic
                    );
                    if pub_pkt.qos == 1 {
                        if let Some(pid) = pub_pkt.packet_id {
                            if client.v5() {
                                let _ =
                                    client.transport.write_all(&mqtt::build_puback_with_reason(
                                        pid,
                                        mqtt::reason::NOT_AUTHORIZED,
                                    ));
                            } else {
                                let _ = client.transport.write_all(&mqtt::build_puback(pid));
                            }
                        }
                    }
                    // QoS 0: silently drop
                    return true;
                }
            }

            // Validate topic name: MQTT §4.7.3 prohibits wildcards in PUBLISH; §1.5.3 prohibits null chars
            if pub_pkt.topic.contains('\0')
                || pub_pkt.topic.contains('+')
                || pub_pkt.topic.contains('#')
            {
                log!(
                    "pgmqtt mqtt: '{}' published to invalid topic '{}' — disconnecting",
                    client_id,
                    pub_pkt.topic
                );
                let _ = client.transport.write_all(&mqtt::build_disconnect(
                    mqtt::reason::TOPIC_NAME_INVALID,
                    client.v5(),
                ));
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
            let has_subs = subscriptions::has_subscribers(&pub_pkt.topic);
            if !pub_pkt.retain && !has_subs && inbound_mapping_names.is_empty() {
                if pub_pkt.qos == 1 {
                    if let Some(pid) = pub_pkt.packet_id {
                        log!(
                            "pgmqtt mqtt: '{}' published to '{}' (QoS 1) with no subscribers. Sending PUBACK and skipping persistence.",
                            client_id,
                            pub_pkt.topic
                        );
                        let puback = mqtt::build_puback(pid);
                        let _ = client.transport.write_all(&puback);
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
                topic: Arc::from(pub_pkt.topic.as_str()),
                payload: Arc::from(pub_pkt.payload),
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
            let _ = client.transport.write_all(&mqtt::build_disconnect(
                mqtt::reason::PROTOCOL_ERROR,
                client.v5(),
            ));
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
    if !expired_ids.is_empty() {
        crate::metrics::add(
            &crate::metrics::get().sessions_expired,
            expired_ids.len() as u64,
        );
    }
}

/// Deliver a batch of messages to matching subscribers.
///
/// Used by both the CDC path (via `cdc_tick` return value) and the client
/// PUBLISH path (via `publish_messages_batch`).
fn deliver_messages(
    messages: &[MqttMessage],
    clients: &mut HashMap<String, MqttClient>,
    pending_publishes: &mut Vec<PendingPublish>,
    session_db_actions: &mut Vec<SessionDbAction>,
) {
    if messages.is_empty() {
        return;
    }

    log!("pgmqtt: publishing {} messages to clients", messages.len());

    let connected: std::collections::HashSet<String> = clients.keys().cloned().collect();
    let mut to_remove = Vec::new();

    for msg in messages {
        let subscriber_ids = subscriptions::match_topic(&msg.topic, &connected);
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
            if let Some(client) = clients.get_mut(sub_id) {
                // ── Connected client: deliver or queue via inline session ──
                let session = &mut client.session;
                let inflight_limit =
                    std::cmp::min(MAX_INFLIGHT_MESSAGES, client.receive_maximum as usize);
                let delivery_qos = std::cmp::min(msg.qos, *granted_qos);

                if delivery_qos == 1 {
                    if session.inflight.len() >= inflight_limit {
                        let queue_byte_cap = crate::get_max_queue_bytes_per_client_guc();
                        if session.queue.len() >= MAX_QUEUE_SIZE
                            || session.queue_bytes.saturating_add(msg.payload.len()) > queue_byte_cap
                        {
                            if crate::get_debug_log_guc() {
                                pgrx::log!(
                                    "pgmqtt: client '{}' queue hit hard limit ({} msgs / {} bytes, cap {}). Disconnecting.",
                                    sub_id,
                                    session.queue.len(),
                                    session.queue_bytes,
                                    queue_byte_cap,
                                );
                            }
                            crate::metrics::inc(&crate::metrics::get().msgs_dropped_queue_full);
                            to_remove.push(sub_id.clone());
                            continue;
                        }
                        session.queue_push_back(MqttMessage {
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
                        if msg.id.is_some() {
                            batch_entries.push((sub_id.clone(), None));
                        }
                    } else {
                        // alloc_packet_id cannot return None here: inflight.len() < 800
                        // is far below the 65535 packet_id space.  expect() is fine.
                        let pid = session
                            .alloc_packet_id()
                            .expect("unreachable: inflight slot free implies free packet_id");
                        session.inflight.insert(
                            pid,
                            (
                                msg.topic.clone(),
                                msg.payload.clone(),
                                msg.id,
                                std::time::Instant::now(),
                            ),
                        );
                        if msg.id.is_some() {
                            batch_entries.push((sub_id.clone(), Some(pid)));
                        }
                        let pkt = mqtt::build_publish(
                            &msg.topic,
                            &msg.payload,
                            1,
                            Some(pid),
                            false,
                            false,
                            client.v5(),
                        );
                        if client.exceeds_max_packet(pkt.len()) {
                            pgrx::log!(
                                "pgmqtt: dropping {}-byte PUBLISH for '{}' (exceeds client max_packet_size={:?})",
                                pkt.len(), sub_id, client.max_packet_size,
                            );
                        } else if client.write_buf.len() + pkt.len() > crate::get_max_client_buffer_bytes_guc() {
                            pgrx::log!(
                                "pgmqtt: client '{}' write buffer full (QoS 1). Disconnecting.",
                                sub_id
                            );
                            to_remove.push(sub_id.clone());
                        } else {
                            match client.try_write(&pkt) {
                                Ok(()) => client.record_msg_sent(msg.payload.len()),
                                Err(()) => { to_remove.push(sub_id.clone()); }
                            }
                        }
                    }
                } else {
                    let pkt = mqtt::build_publish(
                        &msg.topic,
                        &msg.payload,
                        0,
                        None,
                        false,
                        false,
                        client.v5(),
                    );
                    if client.exceeds_max_packet(pkt.len()) {
                        crate::metrics::inc(&crate::metrics::get().msgs_dropped_queue_full);
                    } else if client.write_buf.len() + pkt.len() > crate::get_max_client_buffer_bytes_guc() {
                        // QoS 0 is at-most-once: drop rather than disconnect.
                        crate::metrics::inc(&crate::metrics::get().msgs_dropped_queue_full);
                    } else {
                        match client.try_write(&pkt) {
                            Ok(()) => client.record_msg_sent(msg.payload.len()),
                            Err(()) => { to_remove.push(sub_id.clone()); }
                        }
                    }
                }
            } else {
                // ── Disconnected client with persistent session: queue for later ──
                let delivery_qos = std::cmp::min(msg.qos, *granted_qos);
                if delivery_qos >= 1 {
                    let queue_byte_cap = crate::get_max_queue_bytes_per_client_guc();
                    with_sessions(|sessions| {
                        if let Some(session) = sessions.get_mut(sub_id) {
                            if session.queue.len() >= MAX_QUEUE_SIZE
                                || session.queue_bytes.saturating_add(msg.payload.len()) > queue_byte_cap
                            {
                                crate::metrics::inc(&crate::metrics::get().msgs_dropped_queue_full);
                                return;
                            }
                            session.queue_push_back(MqttMessage {
                                id: msg.id,
                                topic: msg.topic.clone(),
                                payload: msg.payload.clone(),
                                qos: delivery_qos,
                            });
                            if msg.id.is_some() {
                                batch_entries.push((sub_id.clone(), None));
                            }
                        }
                    });
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

    for (client_id, client) in clients.iter_mut() {
        // Cap per-tick redelivery at receive_maximum (MQTT-3.3.4-7), oldest first.
        for pid in client.session.select_redelivery_pids(now, timeout) {
            if let Some(entry) = client.session.inflight.get_mut(&pid) {
                to_resend.push((client_id.clone(), pid, entry.0.clone(), entry.1.clone()));
                entry.3 = now; // update timer for next redelivery
            }
        }
    }

    let max_buf = crate::get_max_client_buffer_bytes_guc();
    let mut to_remove = Vec::new();
    for (cid, pid, topic, payload) in to_resend {
        if let Some(client) = clients.get_mut(&cid) {
            log!("pgmqtt mqtt: redelivering packet_id={} to '{}'", pid, cid);
            let pkt = mqtt::build_publish(&topic, &payload, 1, Some(pid), true, false, client.v5());
            if client.exceeds_max_packet(pkt.len()) {
                continue; // MQTT-3.1.2.24-1
            }
            if client.write_buf.len() + pkt.len() > max_buf {
                log!("pgmqtt mqtt: client '{}' write buffer full during redelivery. Disconnecting.", cid);
                to_remove.push(cid);
            } else if client.try_write(&pkt).is_err() {
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
        let der = base64::engine::general_purpose::STANDARD
            .decode(&body)
            .ok()?;
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
    use ed25519_dalek::{Signature, Verifier, VerifyingKey};

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
    let sig_bytes =
        crate::license::base64_url_decode(sig_b64).map_err(|_| "bad sig base64".to_string())?;

    if sig_bytes.len() != 64 {
        return Err("signature must be 64 bytes".into());
    }
    let sig_arr: [u8; 64] = sig_bytes
        .try_into()
        .expect("unreachable: length already checked to be exactly 64");
    let signature = Signature::from_bytes(&sig_arr);

    // Verify signature over "header.payload" (slice the original token to avoid allocation)
    let last_dot = token
        .rfind('.')
        .expect("unreachable: token already confirmed to have 3 dot-separated parts");
    let signed_data = &token[..last_dot];
    let verifying_key =
        VerifyingKey::from_bytes(pubkey_bytes).map_err(|e| format!("bad public key: {}", e))?;
    verifying_key
        .verify(signed_data.as_bytes(), &signature)
        .map_err(|_| "signature verification failed".to_string())?;

    // Parse payload JSON
    let payload: serde_json::Value =
        serde_json::from_slice(&payload_bytes).map_err(|e| format!("bad payload JSON: {}", e))?;

    // Check exp claim (mandatory)
    let now = crate::license::now_secs();
    let exp = payload
        .get("exp")
        .and_then(|v| v.as_i64())
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

    Ok(JwtClaims {
        client_id,
        sub_claims,
        pub_claims,
    })
}
// ── Enterprise metrics flush functions ───────────────────────────────────────

/// Write the current metrics snapshot to `pgmqtt_metrics_current` and
/// `pgmqtt_metrics_snapshots`, purge history beyond the retention window,
/// and fire any configured hook function or NOTIFY channel.
fn flush_metrics_snapshot(snap: &crate::metrics::MetricsSnapshot) {
    use pgrx::datum::DatumWithOid;
    use std::sync::OnceLock;

    let retention_days = crate::get_metrics_retention_days_guc();
    let hook_fn_str = crate::get_metrics_hook_function_guc();
    let notify_chan_str = crate::get_metrics_notify_channel_guc();
    let json_payload = snap.to_json();
    let captured_at = snap.captured_at_unix;

    // CTE that upserts `pgmqtt_metrics_current` and appends a historical row to
    // `pgmqtt_metrics_snapshots` in a single statement. $1 = captured_at (aliased
    // to snapshot_at in the archive table); $2..=$N map to SNAPSHOT_COLUMNS.
    static UPSERT_AND_INSERT_SQL: OnceLock<String> = OnceLock::new();
    let sql = UPSERT_AND_INSERT_SQL.get_or_init(|| {
        use crate::metrics::MetricsSnapshot;
        let cols = MetricsSnapshot::SNAPSHOT_COLUMNS;
        let col_list = cols.join(",");
        let vals: Vec<String> = (2..=cols.len() + 1).map(|i| format!("${}", i)).collect();
        let vals_list = vals.join(",");
        let sets = cols
            .iter()
            .map(|c| format!("{c}=EXCLUDED.{c}"))
            .collect::<Vec<_>>()
            .join(",");
        format!(
            "WITH upsert AS (\
               INSERT INTO pgmqtt_metrics_current (id,captured_at,{cols}) \
               VALUES (1,$1,{vals}) \
               ON CONFLICT (id) DO UPDATE SET captured_at=EXCLUDED.captured_at,{sets} \
               RETURNING 1\
             ) \
             INSERT INTO pgmqtt_metrics_snapshots (snapshot_at,{cols}) \
             SELECT $1,{vals} FROM upsert",
            cols = col_list, vals = vals_list, sets = sets,
        )
    });

    let values = snap.as_args();
    let snap_args: Vec<DatumWithOid> = values.iter().map(|v| (*v).into()).collect();

    BackgroundWorker::transaction(move || {
        let _ = pgrx::spi::Spi::connect_mut(|spi| {
            if let Err(e) = spi.update(sql, None, &snap_args) {
                pgrx::log!("pgmqtt metrics: failed to persist metrics snapshot: {}", e);
            }
            // Purge snapshots older than the retention window (0 = keep forever)
            if retention_days > 0 {
                let cutoff = captured_at - (retention_days as i64 * 86400);
                let cutoff_args: Vec<DatumWithOid> = vec![cutoff.into()];
                if let Err(e) = spi.update(
                    "DELETE FROM pgmqtt_metrics_snapshots WHERE snapshot_at < $1",
                    None,
                    &cutoff_args,
                ) {
                    pgrx::log!("pgmqtt metrics: failed to purge old snapshots: {}", e);
                }
            }
            // Call hook function if configured.
            // The GUC is superuser-settable; we still validate that the name
            // looks like a valid SQL identifier or schema-qualified identifier
            // (e.g. "my_hook" or "public.my_hook").
            if !hook_fn_str.is_empty() {
                let is_valid_ident = |s: &str| -> bool {
                    !s.is_empty()
                        && s.starts_with(|c: char| c.is_ascii_alphabetic() || c == '_')
                        && s.chars()
                            .all(|c: char| c.is_ascii_alphanumeric() || c == '_')
                };
                let name_ok = hook_fn_str.split('.').all(|part| is_valid_ident(part));
                if name_ok {
                    let hook_sql = format!("SELECT {}($1::jsonb)", hook_fn_str);
                    let args: Vec<DatumWithOid> = vec![DatumWithOid::from(json_payload.as_str())];
                    if let Err(e) = spi.select(&hook_sql, None, &args) {
                        pgrx::log!("pgmqtt metrics: hook '{}' error: {}", hook_fn_str, e);
                    }
                } else {
                    pgrx::log!(
                        "pgmqtt metrics: ignoring hook function with unsafe name: '{}'",
                        hook_fn_str
                    );
                }
            }
            // Send NOTIFY if a channel name is configured
            if !notify_chan_str.is_empty() {
                let args: Vec<DatumWithOid> = vec![
                    DatumWithOid::from(notify_chan_str.as_str()),
                    DatumWithOid::from(json_payload.as_str()),
                ];
                if let Err(e) = spi.update("SELECT pg_notify($1, $2)", None, &args) {
                    pgrx::log!(
                        "pgmqtt metrics: NOTIFY error on channel '{}': {}",
                        notify_chan_str,
                        e
                    );
                }
            }
            Ok::<_, pgrx::spi::Error>(())
        });
    });
}

/// Refresh `pgmqtt_connections_cache` from the current in-memory client map.
///
/// Uses INSERT ... ON CONFLICT DO UPDATE to upsert live clients, then deletes
/// stale rows whose `cached_at_unix` wasn't touched this cycle.  This avoids
/// the dead-tuple churn of DELETE-all + re-INSERT on every flush interval.
fn flush_connections_cache(clients: &HashMap<String, MqttClient>) {
    use pgrx::datum::DatumWithOid;

    let now_unix = crate::license::now_secs();
    let now_instant = std::time::Instant::now();

    // Snapshot client data into owned values before the `move` closure.
    struct ConnRow {
        client_id: String,
        transport: &'static str,
        connected_at_unix: i64,
        last_activity_unix: i64,
        keep_alive_secs: i32,
        msgs_received: i64,
        msgs_sent: i64,
        bytes_received: i64,
        bytes_sent: i64,
        subscriptions: i32,
        queue_depth: i32,
        inflight_count: i32,
        will_set: bool,
    }

    let sub_counts = subscriptions::subscription_counts();
    let rows: Vec<ConnRow> = clients
        .iter()
        .map(|(id, client)| {
            let elapsed_secs = now_instant
                .checked_duration_since(client.last_received_at)
                .map(|d| d.as_secs() as i64)
                .unwrap_or(0);
            ConnRow {
                client_id: id.clone(),
                transport: client.transport_label,
                connected_at_unix: client.connected_at_unix as i64,
                last_activity_unix: now_unix - elapsed_secs,
                keep_alive_secs: client.keep_alive as i32,
                msgs_received: client.msgs_received_count as i64,
                msgs_sent: client.msgs_sent_count as i64,
                bytes_received: client.bytes_received_count as i64,
                bytes_sent: client.bytes_sent_count as i64,
                subscriptions: sub_counts.get(id.as_str()).copied().unwrap_or(0) as i32,
                queue_depth: client.session.queue.len() as i32,
                inflight_count: client.session.inflight.len() as i32,
                will_set: client.will.is_some(),
            }
        })
        .collect();

    // Pivot row-of-structs into struct-of-Vecs so each column travels as a single
    // PG array parameter to UNNEST().
    let mut client_ids: Vec<String> = Vec::with_capacity(rows.len());
    let mut transports: Vec<String> = Vec::with_capacity(rows.len());
    let mut connected_at: Vec<i64> = Vec::with_capacity(rows.len());
    let mut last_activity: Vec<i64> = Vec::with_capacity(rows.len());
    let mut keep_alive: Vec<i32> = Vec::with_capacity(rows.len());
    let mut msgs_received: Vec<i64> = Vec::with_capacity(rows.len());
    let mut msgs_sent: Vec<i64> = Vec::with_capacity(rows.len());
    let mut bytes_received: Vec<i64> = Vec::with_capacity(rows.len());
    let mut bytes_sent: Vec<i64> = Vec::with_capacity(rows.len());
    let mut subscriptions: Vec<i32> = Vec::with_capacity(rows.len());
    let mut queue_depth: Vec<i32> = Vec::with_capacity(rows.len());
    let mut inflight_count: Vec<i32> = Vec::with_capacity(rows.len());
    let mut will_set: Vec<bool> = Vec::with_capacity(rows.len());
    for row in rows {
        client_ids.push(row.client_id);
        transports.push(row.transport.to_string());
        connected_at.push(row.connected_at_unix);
        last_activity.push(row.last_activity_unix);
        keep_alive.push(row.keep_alive_secs);
        msgs_received.push(row.msgs_received);
        msgs_sent.push(row.msgs_sent);
        bytes_received.push(row.bytes_received);
        bytes_sent.push(row.bytes_sent);
        subscriptions.push(row.subscriptions);
        queue_depth.push(row.queue_depth);
        inflight_count.push(row.inflight_count);
        will_set.push(row.will_set);
    }

    BackgroundWorker::transaction(move || {
        let _ = pgrx::spi::Spi::connect_mut(|spi| {
            let upsert_args: Vec<DatumWithOid> = vec![
                client_ids.into(),
                transports.into(),
                connected_at.into(),
                last_activity.into(),
                keep_alive.into(),
                msgs_received.into(),
                msgs_sent.into(),
                bytes_received.into(),
                bytes_sent.into(),
                subscriptions.into(),
                queue_depth.into(),
                inflight_count.into(),
                will_set.into(),
                now_unix.into(),
            ];
            // One round-trip regardless of client count: every column is a PG array
            // unrolled by UNNEST.  $14 (cached_at_unix) is broadcast to every row.
            const UPSERT_SQL: &str = "\
                INSERT INTO pgmqtt_connections_cache \
                 (client_id,transport,connected_at_unix,last_activity_at_unix,\
                  keep_alive_secs,msgs_received,msgs_sent,bytes_received,bytes_sent,\
                  subscriptions,queue_depth,inflight_count,will_set,cached_at_unix) \
                 SELECT client_id,transport,connected_at_unix,last_activity_at_unix,\
                  keep_alive_secs,msgs_received,msgs_sent,bytes_received,bytes_sent,\
                  subscriptions,queue_depth,inflight_count,will_set,$14 \
                 FROM UNNEST($1::text[],$2::text[],$3::bigint[],$4::bigint[],\
                  $5::int[],$6::bigint[],$7::bigint[],$8::bigint[],$9::bigint[],\
                  $10::int[],$11::int[],$12::int[],$13::bool[]) \
                 AS u(client_id,transport,connected_at_unix,last_activity_at_unix,\
                      keep_alive_secs,msgs_received,msgs_sent,bytes_received,bytes_sent,\
                      subscriptions,queue_depth,inflight_count,will_set) \
                 ON CONFLICT (client_id) DO UPDATE SET \
                  transport=EXCLUDED.transport,\
                  connected_at_unix=EXCLUDED.connected_at_unix,\
                  last_activity_at_unix=EXCLUDED.last_activity_at_unix,\
                  keep_alive_secs=EXCLUDED.keep_alive_secs,\
                  msgs_received=EXCLUDED.msgs_received,\
                  msgs_sent=EXCLUDED.msgs_sent,\
                  bytes_received=EXCLUDED.bytes_received,\
                  bytes_sent=EXCLUDED.bytes_sent,\
                  subscriptions=EXCLUDED.subscriptions,\
                  queue_depth=EXCLUDED.queue_depth,\
                  inflight_count=EXCLUDED.inflight_count,\
                  will_set=EXCLUDED.will_set,\
                  cached_at_unix=EXCLUDED.cached_at_unix";
            if let Err(e) = spi.update(UPSERT_SQL, None, &upsert_args) {
                pgrx::log!("pgmqtt metrics: failed to upsert connections cache: {}", e);
            }
            // Remove rows for clients that disconnected since the last flush.
            let stale_args: Vec<DatumWithOid> = vec![now_unix.into()];
            if let Err(e) = spi.update(
                "DELETE FROM pgmqtt_connections_cache WHERE cached_at_unix < $1",
                None,
                &stale_args,
            ) {
                pgrx::log!("pgmqtt metrics: failed to prune stale connections: {}", e);
            }
            Ok::<_, pgrx::spi::Error>(())
        });
    });
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
