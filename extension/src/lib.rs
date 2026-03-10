use pgrx::bgworkers::*;
use pgrx::prelude::*;
use std::time::Duration;

mod mqtt;
mod ring_buffer;
mod server;
mod subscriptions;
mod topic_buffer;
mod topic_map;
mod websocket;

::pgrx::pg_module_magic!(name, version);

// ---------------------------------------------------------------------------
// SQL-callable functions
// ---------------------------------------------------------------------------

#[pg_extern]
fn hello_pgmqtt() -> &'static str {
    "Hello, pgmqtt"
}

fn ensure_tables_exist() {
    Spi::run(
        "CREATE TABLE IF NOT EXISTS pgmqtt_topic_mappings (
            schema_name text NOT NULL,
            table_name text NOT NULL,
            topic_template text NOT NULL,
            payload_template text NOT NULL,
            PRIMARY KEY (schema_name, table_name)
        )",
    )
    .unwrap_or_else(|e| pgrx::error!("pgmqtt: failed to create mappings table: {}", e));

    // Migration: add qos to mappings if missing
    let _ =
        Spi::run("ALTER TABLE pgmqtt_topic_mappings ADD COLUMN IF NOT EXISTS qos int DEFAULT 0");

    // TODO - if no session matching, drop message
    Spi::run(
        "CREATE TABLE IF NOT EXISTS pgmqtt_messages (
            id bigserial PRIMARY KEY,
            topic text NOT NULL,
            payload bytea,
            qos int DEFAULT 0,
            retain boolean DEFAULT false,
            created_at timestamptz DEFAULT NOW()
        )",
    )
    .unwrap_or_else(|e| pgrx::error!("pgmqtt: failed to create messages table: {}", e));

    // Migration: add qos to messages if missing
    let _ = Spi::run("ALTER TABLE pgmqtt_messages ADD COLUMN IF NOT EXISTS qos int DEFAULT 0");

    Spi::run(
        "CREATE TABLE IF NOT EXISTS pgmqtt_retained (
            topic text PRIMARY KEY,
            message_id bigint REFERENCES pgmqtt_messages(id) ON DELETE CASCADE
        )",
    )
    .unwrap_or_else(|e| pgrx::error!("pgmqtt: failed to create retained table: {}", e));

    Spi::run(
        "CREATE TABLE IF NOT EXISTS pgmqtt_sessions (
            client_id text PRIMARY KEY,
            next_packet_id integer NOT NULL DEFAULT 1,
            expiry_interval integer NOT NULL DEFAULT 0,
            disconnected_at timestamptz
        )",
    )
    .unwrap_or_else(|e| pgrx::error!("pgmqtt: failed to create sessions table: {}", e));

    Spi::run(
        "CREATE TABLE IF NOT EXISTS pgmqtt_session_messages (
            client_id text NOT NULL REFERENCES pgmqtt_sessions(client_id) ON DELETE CASCADE,
            message_id bigint NOT NULL REFERENCES pgmqtt_messages(id) ON DELETE CASCADE,
            
            packet_id integer,
            sent_at timestamptz,
            
            created_at timestamptz NOT NULL DEFAULT NOW(),

            PRIMARY KEY (client_id, message_id)
        )",
    )
    .unwrap_or_else(|e| pgrx::error!("pgmqtt: failed to create session messages table: {}", e));
}

/// Register a CDC → MQTT topic mapping (persisted to DB table).
///
/// Example:
/// ```sql
/// SELECT pgmqtt_add_mapping(
///     'public',
///     'events',
///     'events/{{ op | lower }}',
///     '{{ columns | tojson }}'
/// );
/// ```
#[pg_extern]
fn pgmqtt_add_mapping(
    schema_name: &str,
    table_name: &str,
    topic_template: &str,
    payload_template: &str,
    qos: default!(i32, 0),
) -> &'static str {
    ensure_tables_exist();

    // Upsert the mapping
    let query = "\
        INSERT INTO pgmqtt_topic_mappings (schema_name, table_name, topic_template, payload_template, qos) \
        VALUES ($1, $2, $3, $4, $5) \
        ON CONFLICT (schema_name, table_name) DO UPDATE \
        SET topic_template = EXCLUDED.topic_template, \
            payload_template = EXCLUDED.payload_template, \
            qos = EXCLUDED.qos";

    let args: Vec<pgrx::datum::DatumWithOid> = vec![
        schema_name.into(),
        table_name.into(),
        topic_template.into(),
        payload_template.into(),
        qos.into(),
    ];

    pgrx::spi::Spi::connect_mut(|client| client.update(query, None, &args).map(|_| ()))
        .unwrap_or_else(|e| pgrx::error!("pgmqtt: failed to upsert mapping: {}", e));

    pgrx::log!(
        "pgmqtt: added mapping {}.{} → topic='{}' payload='{}'",
        schema_name,
        table_name,
        topic_template,
        payload_template
    );
    "ok"
}

/// Remove a CDC → MQTT topic mapping.
#[pg_extern]
fn pgmqtt_remove_mapping(schema_name: &str, table_name: &str) -> bool {
    let query = "DELETE FROM pgmqtt_topic_mappings WHERE schema_name = $1 AND table_name = $2";
    let args: Vec<pgrx::datum::DatumWithOid> = vec![schema_name.into(), table_name.into()];

    // If the table doesn't exist, Spi::connect_mut will error — treat as "not found"
    let deleted =
        pgrx::spi::Spi::connect_mut(|client| client.update(query, None, &args).map(|_| ())).is_ok();

    pgrx::log!(
        "pgmqtt: remove mapping {}.{} → {}",
        schema_name,
        table_name,
        if deleted { "removed" } else { "not found" }
    );
    deleted
}

/// List all active topic mappings.
#[pg_extern]
fn pgmqtt_list_mappings() -> TableIterator<
    'static,
    (
        name!(schema_name, String),
        name!(table_name, String),
        name!(topic_template, String),
        name!(payload_template, String),
        name!(qos, i32),
    ),
> {
    let mappings = Spi::connect(|client| {
        // Check if table exists
        let table_exists = client
            .select("SELECT to_regclass('pgmqtt_topic_mappings')::text", None, &[])?
            .first()
            .get_one::<String>()?
            .is_some();

        if !table_exists {
            return Ok::<_, spi::Error>(Vec::new());
        }

        let mut rows = Vec::new();
        // Try to select from the table
        if let Ok(table) = client.select(
            "SELECT schema_name, table_name, topic_template, payload_template, qos FROM pgmqtt_topic_mappings",
            None, &[],
        ) {
            for row in table {
                let s: String = row.get_by_name("schema_name").ok().flatten().unwrap_or_default();
                let t: String = row.get_by_name("table_name").ok().flatten().unwrap_or_default();
                let tt: String = row.get_by_name("topic_template").ok().flatten().unwrap_or_default();
                let pt: String = row.get_by_name("payload_template").ok().flatten().unwrap_or_default();
                let q: i32 = row.get_by_name("qos").ok().flatten().unwrap_or_default();
                rows.push((s, t, tt, pt, q));
            }
        }
        Ok::<_, spi::Error>(rows)
    }).unwrap_or_default();

    TableIterator::new(mappings.into_iter())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use pgrx::prelude::*;

    #[pg_test]
    fn test_hello_pgmqtt() {
        assert_eq!("Hello, pgmqtt", crate::hello_pgmqtt());
    }
}

/// This module is required by `cargo pgrx test` invocations.
/// It must be visible at the root of your extension crate.
#[cfg(test)]
pub mod pg_test {
    pub fn setup(_options: Vec<&str>) {
        // perform one-off initialization when the pg_test framework starts
    }

    #[must_use]
    pub fn postgresql_conf_options() -> Vec<&'static str> {
        // return any postgresql.conf settings that are required for your tests
        vec![]
    }
}

// ---------------------------------------------------------------------------
// Background worker registration
// ---------------------------------------------------------------------------

#[no_mangle]
pub unsafe extern "C" fn _PG_init() {
    // Database name for the MQTT+CDC worker
    let db_name = "postgres";
    let db_name_cstr = std::ffi::CString::new(db_name)
        .unwrap_or_else(|e| pgrx::error!("CString::new failed: {}", e));
    let db_name_ptr = db_name_cstr.as_ptr();
    let db_name_datum = unsafe { std::mem::transmute::<*const std::os::raw::c_char, pg_sys::Datum>(db_name_ptr) };

    // HTTP healthcheck server (port 8080)
    BackgroundWorkerBuilder::new("pgmqtt_http")
        .set_function("pgmqtt_http_worker_main")
        .set_library("pgmqtt")
        .enable_shmem_access(None)
        .set_start_time(BgWorkerStartTime::RecoveryFinished)
        .set_restart_time(Some(Duration::from_secs(5)))
        .load();

    // MQTT broker + CDC consumer (port 1883) — single process for shared state
    BackgroundWorkerBuilder::new("pgmqtt_mqtt")
        .set_function("pgmqtt_mqtt_worker_main")
        .set_library("pgmqtt")
        .enable_spi_access()
        .set_start_time(BgWorkerStartTime::RecoveryFinished)
        .set_restart_time(Some(Duration::from_secs(5)))
        .set_argument(Some(db_name_datum))
        .load();

    // Keep the CString alive
    std::mem::forget(db_name_cstr);
}

// ---------------------------------------------------------------------------
// HTTP healthcheck worker
// ---------------------------------------------------------------------------

#[pg_guard]
#[no_mangle]
pub unsafe extern "C-unwind" fn pgmqtt_http_worker_main(_arg: pg_sys::Datum) {
    BackgroundWorker::attach_signal_handlers(SignalWakeFlags::SIGHUP | SignalWakeFlags::SIGTERM);
    pgrx::log!("pgmqtt_http: starting on port 8080");
    server::run_http(8080);
}

// ---------------------------------------------------------------------------
// MQTT broker + CDC consumer worker (single process)
// ---------------------------------------------------------------------------

#[pg_guard]
#[no_mangle]
pub unsafe extern "C-unwind" fn pgmqtt_mqtt_worker_main(arg: pg_sys::Datum) {
    let db_name = if arg.is_null() {
        "postgres"
    } else {
        let cstr = unsafe {
            std::ffi::CStr::from_ptr(std::mem::transmute::<pg_sys::Datum, *const std::os::raw::c_char>(arg))
        };
        cstr.to_str().unwrap_or("postgres")
    };

    BackgroundWorker::attach_signal_handlers(SignalWakeFlags::SIGHUP | SignalWakeFlags::SIGTERM);
    BackgroundWorker::connect_worker_to_spi(Some(db_name), None);

    pgrx::log!("pgmqtt mqtt+cdc: starting, connected to '{}'", db_name);

    let slot_name = "pgmqtt_slot";
    let output_plugin = "pgmqtt";

    // Ensure tables exist
    BackgroundWorker::transaction(|| {
        ensure_tables_exist();
    });

    // Ensure the replication slot exists
    BackgroundWorker::transaction(|| {
        let create_slot_query = format!(
            "SELECT pg_create_logical_replication_slot('{}', '{}') \
             WHERE NOT EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = '{}')",
            slot_name, output_plugin, slot_name
        );

        match Spi::connect(|client| {
            client.select(&create_slot_query, None, &[])?;
            Ok::<_, spi::Error>(())
        }) {
            Ok(_) => {
                pgrx::log!("pgmqtt: replication slot '{}' ready", slot_name);
            }
            Err(e) => {
                pgrx::log!("pgmqtt: error creating replication slot: {:?}", e);
            }
        }
    });

    // Run the combined MQTT + CDC server
    server::run_mqtt_cdc(1883, 9001, slot_name);
}

// ---------------------------------------------------------------------------
// Logical decoding output plugin
// ---------------------------------------------------------------------------

#[pg_guard]
#[no_mangle]
pub unsafe extern "C-unwind" fn _PG_output_plugin_init(cb: *mut pg_sys::OutputPluginCallbacks) {
    let cb = unsafe { &mut *cb };
    cb.startup_cb = Some(pg_decode_startup);
    cb.begin_cb = Some(pg_decode_begin_txn);
    cb.commit_cb = Some(pg_decode_commit_txn);
    cb.change_cb = Some(pg_decode_change);
}

#[pg_guard]
unsafe extern "C-unwind" fn pg_decode_startup(
    _ctx: *mut pg_sys::LogicalDecodingContext,
    options: *mut pg_sys::OutputPluginOptions,
    _is_init: bool,
) {
    let options = unsafe { &mut *options };
    options.output_type = pg_sys::OutputPluginOutputType::OUTPUT_PLUGIN_TEXTUAL_OUTPUT;
    options.receive_rewrites = false;
    pgrx::log!("pgmqtt: output plugin started");
}

#[pg_guard]
unsafe extern "C-unwind" fn pg_decode_begin_txn(
    _ctx: *mut pg_sys::LogicalDecodingContext,
    _txn: *mut pg_sys::ReorderBufferTXN,
) {
}

#[pg_guard]
unsafe extern "C-unwind" fn pg_decode_commit_txn(
    _ctx: *mut pg_sys::LogicalDecodingContext,
    _txn: *mut pg_sys::ReorderBufferTXN,
    _commit_lsn: pg_sys::XLogRecPtr,
) {
}

#[pg_guard]
unsafe extern "C-unwind" fn pg_decode_change(
    ctx: *mut pg_sys::LogicalDecodingContext,
    _txn: *mut pg_sys::ReorderBufferTXN,
    relation: pg_sys::Relation,
    change: *mut pg_sys::ReorderBufferChange,
) {
    let rel_data = unsafe { *relation };
    let rel_name = unsafe {
        std::ffi::CStr::from_ptr((*rel_data.rd_rel).relname.data.as_ptr()).to_string_lossy()
    };

    let schema_oid = unsafe { (*rel_data.rd_rel).relnamespace };
    let schema_name = unsafe {
        let ns_name = pg_sys::get_namespace_name(schema_oid);
        if ns_name.is_null() {
            std::borrow::Cow::Borrowed("unknown")
        } else {
            std::ffi::CStr::from_ptr(ns_name).to_string_lossy()
        }
    };

    let op = unsafe {
        match (*change).action {
            pg_sys::ReorderBufferChangeType::REORDER_BUFFER_CHANGE_INSERT => "INSERT",
            pg_sys::ReorderBufferChangeType::REORDER_BUFFER_CHANGE_UPDATE => "UPDATE",
            pg_sys::ReorderBufferChangeType::REORDER_BUFFER_CHANGE_DELETE => "DELETE",
            other => {
                pgrx::log!(
                    "pgmqtt: ignoring CDC event type {:?} on {}.{}",
                    other,
                    schema_name,
                    rel_name
                );
                return;
            }
        }
    };

    // Extract column data from the tuple
    let columns = unsafe { extract_columns(relation, change) };

    pgrx::log!("pgmqtt: CDC event: {} on {}.{}", op, schema_name, rel_name);

    // Ignore inner extension tables to avoid infinite pub sub loops
    if rel_name.starts_with("pgmqtt_") {
        return;
    }

    pgrx::log!("pgmqtt: CDC event: {} on {}.{}", op, schema_name, rel_name);

    ring_buffer::push(ring_buffer::ChangeEvent {
        op,
        schema: schema_name.into_owned(),
        table: rel_name.into_owned(),
        columns,
    });

    // The output plugin MUST produce output for pg_logical_slot_get_changes to advance.
    unsafe {
        pg_sys::OutputPluginPrepareWrite(ctx, true);
        let msg = std::ffi::CString::new(format!("{}", op))
            .unwrap_or_else(|e| pgrx::error!("CString::new failed: {}", e));
        pg_sys::appendStringInfoString((*ctx).out, msg.as_ptr());
        pg_sys::OutputPluginWrite(ctx, true);
    }
}

/// Extract column name/value pairs from a ReorderBufferChange.
///
/// For INSERT/UPDATE we read the new tuple; for DELETE we read the old tuple
/// (which requires REPLICA IDENTITY to be set on the table).
unsafe fn extract_columns(
    relation: pg_sys::Relation,
    change: *mut pg_sys::ReorderBufferChange,
) -> Vec<(String, String)> {
    let mut columns = Vec::new();

    let rel_data = unsafe { *relation };
    let tupdesc = rel_data.rd_att;
    if tupdesc.is_null() {
        return columns;
    }

    let tupdesc_ref = unsafe { &*tupdesc };
    let natts = tupdesc_ref.natts as usize;

    // Access the union field directly — it's a C union, not a method
    let change_ref = unsafe { &*change };
    let tp = unsafe { change_ref.data.tp };

    let tuple = {
        match change_ref.action {
            pg_sys::ReorderBufferChangeType::REORDER_BUFFER_CHANGE_INSERT => tp.newtuple,
            pg_sys::ReorderBufferChangeType::REORDER_BUFFER_CHANGE_UPDATE => {
                if !tp.newtuple.is_null() {
                    tp.newtuple
                } else {
                    tp.oldtuple
                }
            }
            pg_sys::ReorderBufferChangeType::REORDER_BUFFER_CHANGE_DELETE => tp.oldtuple,
            _ => return columns,
        }
    };

    if tuple.is_null() {
        return columns;
    }

    // ReorderBufferTupleBuf contains a HeapTupleData
    let heap_tuple = unsafe { &(*tuple).tuple };

    for i in 0..natts {
        let attr = unsafe {
            let attrs = (*tupdesc).attrs.as_slice(natts);
            &attrs[i]
        };

        // Skip dropped columns
        if attr.attisdropped {
            continue;
        }

        let col_name = unsafe {
            std::ffi::CStr::from_ptr(attr.attname.data.as_ptr())
                .to_string_lossy()
                .into_owned()
        };

        let mut is_null = false;
        let datum = unsafe {
            pg_sys::heap_getattr(
                heap_tuple as *const pg_sys::HeapTupleData as *mut pg_sys::HeapTupleData,
                (i + 1) as i32,
                tupdesc,
                &mut is_null,
            )
        };

        let col_value = if is_null {
            "NULL".to_string()
        } else {
            // Use the type's output function to get text representation
            unsafe {
                let typoid = attr.atttypid;
                let mut typoutput = typoid; // same Oid type, will be overwritten
                let mut typisvarlena: bool = false;
                pg_sys::getTypeOutputInfo(typoid, &mut typoutput, &mut typisvarlena);
                let cstr = pg_sys::OidOutputFunctionCall(typoutput, datum);
                if cstr.is_null() {
                    "NULL".to_string()
                } else {
                    std::ffi::CStr::from_ptr(cstr)
                        .to_string_lossy()
                        .into_owned()
                }
            }
        };

        columns.push((col_name, col_value));
    }

    columns
}
