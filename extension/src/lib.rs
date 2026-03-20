use pgrx::bgworkers::*;
use pgrx::prelude::*;
use std::time::Duration;

mod ffi_safe;
mod init010;
pub mod license;
mod mqtt;
mod ring_buffer;
mod server;
mod subscriptions;
mod topic_buffer;
mod topic_map;
mod websocket;

::pgrx::pg_module_magic!(name, version);

// Restrict topic-mapping administration to the extension owner by default.
// DBAs can later open access with:
//   GRANT EXECUTE ON FUNCTION pgmqtt_add_mapping(...) TO some_role;
extension_sql!(
    r#"
    REVOKE EXECUTE ON FUNCTION pgmqtt_add_mapping(text, text, text, text, int) FROM PUBLIC;
    REVOKE EXECUTE ON FUNCTION pgmqtt_remove_mapping(text, text) FROM PUBLIC;
    REVOKE EXECUTE ON FUNCTION pgmqtt_list_mappings() FROM PUBLIC;
    "#,
    name = "revoke_mapping_from_public",
    requires = [pgmqtt_add_mapping, pgmqtt_remove_mapping, pgmqtt_list_mappings],
);

// ---------------------------------------------------------------------------
// GUC definitions
// ---------------------------------------------------------------------------

use pgrx::guc::{GucContext, GucFlags, GucRegistry, GucSetting};
use std::ffi::CString;

static LICENSE_KEY: GucSetting<Option<CString>> =
    GucSetting::<Option<CString>>::new(None);

static MQTT_ENABLED: GucSetting<bool> = GucSetting::<bool>::new(true);
static WS_ENABLED: GucSetting<bool> = GucSetting::<bool>::new(true);
static MQTTS_ENABLED: GucSetting<bool> = GucSetting::<bool>::new(false);
static WSS_ENABLED: GucSetting<bool> = GucSetting::<bool>::new(false);

static MQTT_PORT: GucSetting<i32> = GucSetting::<i32>::new(1883);
static WS_PORT: GucSetting<i32> = GucSetting::<i32>::new(9001);
static MQTTS_PORT: GucSetting<i32> = GucSetting::<i32>::new(8883);
static WSS_PORT: GucSetting<i32> = GucSetting::<i32>::new(9002);

static TLS_CERT_FILE: GucSetting<Option<CString>> =
    GucSetting::<Option<CString>>::new(None);
static TLS_KEY_FILE: GucSetting<Option<CString>> =
    GucSetting::<Option<CString>>::new(None);

static JWT_PUBLIC_KEY: GucSetting<Option<CString>> =
    GucSetting::<Option<CString>>::new(None);
static JWT_REQUIRED: GucSetting<bool> = GucSetting::<bool>::new(false);
static JWT_REQUIRED_WS: GucSetting<bool> = GucSetting::<bool>::new(false);

// ---------------------------------------------------------------------------
// GUC accessors
// ---------------------------------------------------------------------------

pub fn get_license_key_guc() -> String {
    LICENSE_KEY
        .get()
        .map(|c| c.to_string_lossy().into_owned())
        .unwrap_or_default()
}

pub fn get_jwt_public_key_guc() -> String {
    JWT_PUBLIC_KEY
        .get()
        .map(|c| c.to_string_lossy().into_owned())
        .unwrap_or_default()
}

pub fn get_jwt_required_guc() -> bool {
    JWT_REQUIRED.get()
}

pub fn get_jwt_required_ws_guc() -> bool {
    JWT_REQUIRED_WS.get()
}

pub struct PortConfig {
    pub mqtt_port: u16,
    pub ws_port: u16,
    pub mqtts_port: u16,
    pub wss_port: u16,
    pub mqtt_enabled: bool,
    pub ws_enabled: bool,
    pub mqtts_enabled: bool,
    pub wss_enabled: bool,
    pub tls_cert_file: String,
    pub tls_key_file: String,
}

pub fn get_port_gucs() -> PortConfig {
    PortConfig {
        mqtt_port: MQTT_PORT.get() as u16,
        ws_port: WS_PORT.get() as u16,
        mqtts_port: MQTTS_PORT.get() as u16,
        wss_port: WSS_PORT.get() as u16,
        mqtt_enabled: MQTT_ENABLED.get(),
        ws_enabled: WS_ENABLED.get(),
        mqtts_enabled: MQTTS_ENABLED.get(),
        wss_enabled: WSS_ENABLED.get(),
        tls_cert_file: get_tls_cert_file_guc(),
        tls_key_file: get_tls_key_file_guc(),
    }
}

pub fn get_tls_cert_file_guc() -> String {
    TLS_CERT_FILE
        .get()
        .map(|c| c.to_string_lossy().into_owned())
        .unwrap_or_else(|| "server.crt".to_string())
}

pub fn get_tls_key_file_guc() -> String {
    TLS_KEY_FILE
        .get()
        .map(|c| c.to_string_lossy().into_owned())
        .unwrap_or_else(|| "server.key".to_string())
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Execute SQL, logging and ignoring errors (for DDL operations).
fn run_sql_or_error(sql: &str, operation: &str) {
    Spi::run(sql).unwrap_or_else(|e| pgrx::error!("pgmqtt: failed to {}: {}", operation, e));
}

// ---------------------------------------------------------------------------
// SQL-callable functions
// ---------------------------------------------------------------------------

fn ensure_tables_exist() {
    init010::init_010();
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

/// Return the current license status as a composite row.
#[pg_extern]
fn pgmqtt_license_status() -> TableIterator<
    'static,
    (
        name!(customer, String),
        name!(status, String),
        name!(expires_at, i64),
        name!(grace_expires_at, i64),
        name!(features, Vec<String>),
        name!(max_connections, i32),
    ),
> {
    let row = match license::current_status() {
        license::LicenseStatus::Community => (
            "".to_string(),
            "community".to_string(),
            0i64,
            0i64,
            vec![],
            1000i32,
        ),
        license::LicenseStatus::Active(p) => (
            p.customer,
            "active".to_string(),
            p.expires_at,
            p.grace_expires_at,
            p.features,
            p.max_connections as i32,
        ),
        license::LicenseStatus::Grace(p) => (
            p.customer,
            "grace".to_string(),
            p.expires_at,
            p.grace_expires_at,
            p.features,
            p.max_connections as i32,
        ),
        license::LicenseStatus::Expired => (
            "".to_string(),
            "expired".to_string(),
            0i64,
            0i64,
            vec![],
            0i32,
        ),
        license::LicenseStatus::Invalid(reason) => (
            "".to_string(),
            format!("invalid: {}", reason),
            0i64,
            0i64,
            vec![],
            1000i32,
        ),
    };
    TableIterator::new(std::iter::once(row))
}

/// Return operational status and metrics.
///
/// All table counts are taken from a single SQL statement so they reflect a
/// consistent snapshot under READ COMMITTED isolation.
#[pg_extern]
fn pgmqtt_status() -> TableIterator<
    'static,
    (
        name!(active_connections, i32),
        name!(total_subscriptions, i32),
        name!(total_retained_messages, i32),
        name!(pending_session_messages, i32),
        name!(cdc_mappings, i32),
        name!(cdc_slot_active, i64),
    ),
> {
    let row = Spi::connect(|client| -> Result<(i32, i32, i32, i32, i32, i64), spi::Error> {
        // Single statement: all counts share one snapshot, preventing torn reads.
        let q = "\
            SELECT \
              (SELECT COUNT(*)::int  FROM pgmqtt_sessions         WHERE disconnected_at IS NULL) AS active_connections, \
              (SELECT COUNT(*)::int  FROM pgmqtt_subscriptions)                                  AS total_subscriptions, \
              (SELECT COUNT(*)::int  FROM pgmqtt_retained)                                       AS total_retained_messages, \
              (SELECT COUNT(*)::int  FROM pgmqtt_session_messages)                               AS pending_session_messages, \
              (SELECT COUNT(*)::int  FROM pgmqtt_topic_mappings)                                 AS cdc_mappings, \
              (SELECT COUNT(*)::int8 FROM pg_replication_slots \
                 WHERE slot_name = 'pgmqtt_slot' AND slot_type = 'logical' AND active)           AS cdc_slot_active\
        ";

        if let Ok(mut rows) = client.select(q, None, &[]) {
            if let Some(row) = rows.next() {
                let ac  = row.get_by_name::<i32, _>("active_connections").ok().flatten().unwrap_or(0);
                let ts  = row.get_by_name::<i32, _>("total_subscriptions").ok().flatten().unwrap_or(0);
                let tr  = row.get_by_name::<i32, _>("total_retained_messages").ok().flatten().unwrap_or(0);
                let ps  = row.get_by_name::<i32, _>("pending_session_messages").ok().flatten().unwrap_or(0);
                let cm  = row.get_by_name::<i32, _>("cdc_mappings").ok().flatten().unwrap_or(0);
                let csa = row.get_by_name::<i64, _>("cdc_slot_active").ok().flatten().unwrap_or(0);
                return Ok((ac, ts, tr, ps, cm, csa));
            }
        }
        Ok((0, 0, 0, 0, 0, 0))
    }).unwrap_or((0, 0, 0, 0, 0, 0));

    TableIterator::new(std::iter::once(row))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use pgrx::prelude::*;
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
    // Register GUCs
    GucRegistry::define_string_guc(
        c"pgmqtt.license_key",
        c"pgmqtt enterprise license key",
        c"",
        &LICENSE_KEY,
        GucContext::Sighup,
        GucFlags::SUPERUSER_ONLY,
    );
    GucRegistry::define_int_guc(
        c"pgmqtt.mqtt_port",
        c"MQTT TCP port",
        c"",
        &MQTT_PORT,
        0,
        65535,
        GucContext::Sighup,
        GucFlags::SUPERUSER_ONLY,
    );
    GucRegistry::define_int_guc(
        c"pgmqtt.ws_port",
        c"MQTT WebSocket port",
        c"",
        &WS_PORT,
        0,
        65535,
        GucContext::Sighup,
        GucFlags::SUPERUSER_ONLY,
    );
    GucRegistry::define_bool_guc(
        c"pgmqtt.mqtt_enabled",
        c"Enable plain MQTT TCP listener",
        c"",
        &MQTT_ENABLED,
        GucContext::Sighup,
        GucFlags::SUPERUSER_ONLY,
    );
    GucRegistry::define_bool_guc(
        c"pgmqtt.ws_enabled",
        c"Enable plain MQTT WebSocket listener",
        c"",
        &WS_ENABLED,
        GucContext::Sighup,
        GucFlags::SUPERUSER_ONLY,
    );
    GucRegistry::define_bool_guc(
        c"pgmqtt.mqtts_enabled",
        c"Enable MQTT TLS listener",
        c"",
        &MQTTS_ENABLED,
        GucContext::Sighup,
        GucFlags::SUPERUSER_ONLY,
    );
    GucRegistry::define_bool_guc(
        c"pgmqtt.wss_enabled",
        c"Enable WebSocket TLS listener",
        c"",
        &WSS_ENABLED,
        GucContext::Sighup,
        GucFlags::SUPERUSER_ONLY,
    );
    GucRegistry::define_int_guc(
        c"pgmqtt.mqtts_port",
        c"MQTT TLS port",
        c"",
        &MQTTS_PORT,
        0,
        65535,
        GucContext::Sighup,
        GucFlags::SUPERUSER_ONLY,
    );
    GucRegistry::define_int_guc(
        c"pgmqtt.wss_port",
        c"WebSocket TLS port",
        c"",
        &WSS_PORT,
        0,
        65535,
        GucContext::Sighup,
        GucFlags::SUPERUSER_ONLY,
    );
    GucRegistry::define_string_guc(
        c"pgmqtt.tls_cert_file",
        c"TLS certificate path",
        c"",
        &TLS_CERT_FILE,
        GucContext::Sighup,
        GucFlags::SUPERUSER_ONLY,
    );
    GucRegistry::define_string_guc(
        c"pgmqtt.tls_key_file",
        c"TLS private key path",
        c"",
        &TLS_KEY_FILE,
        GucContext::Sighup,
        GucFlags::SUPERUSER_ONLY,
    );
    GucRegistry::define_string_guc(
        c"pgmqtt.jwt_public_key",
        c"Ed25519 public key for JWT verification (base64url or PEM)",
        c"",
        &JWT_PUBLIC_KEY,
        GucContext::Sighup,
        GucFlags::SUPERUSER_ONLY,
    );
    GucRegistry::define_bool_guc(
        c"pgmqtt.jwt_required",
        c"Require valid JWT for all MQTT connections",
        c"",
        &JWT_REQUIRED,
        GucContext::Sighup,
        GucFlags::SUPERUSER_ONLY,
    );
    GucRegistry::define_bool_guc(
        c"pgmqtt.jwt_required_ws",
        c"Require valid JWT for WebSocket connections (overrides jwt_required for WS)",
        c"",
        &JWT_REQUIRED_WS,
        GucContext::Sighup,
        GucFlags::SUPERUSER_ONLY,
    );
    // Database name for the MQTT+CDC worker
    let db_name = "postgres";
    let db_name_cstr = std::ffi::CString::new(db_name)
        .unwrap_or_else(|e| pgrx::error!("CString::new failed: {}", e));
    let db_name_datum = ffi_safe::cstr_to_datum(db_name_cstr.as_ptr());

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
    let db_name = ffi_safe::datum_to_str(arg);

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

    // Run the combined MQTT + CDC server (ports from GUCs)
    let ports = get_port_gucs();
    server::run_mqtt_cdc(ports, slot_name);
}

// ---------------------------------------------------------------------------
// Logical decoding output plugin
// ---------------------------------------------------------------------------

#[pg_guard]
#[no_mangle]
pub unsafe extern "C-unwind" fn _PG_output_plugin_init(cb: *mut pg_sys::OutputPluginCallbacks) {
    let cb = &mut *cb;
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
    let options = &mut *options;
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
    let rel_name = ffi_safe::relation_name(relation);
    let schema_oid = ffi_safe::relation_schema_oid(relation);
    let schema_name = ffi_safe::oid_to_schema_name(schema_oid);

    let op = match ffi_safe::change_operation(change) {
        Some(op) => op,
        None => {
            pgrx::log!(
                "pgmqtt: ignoring unsupported CDC event type on {}.{}",
                schema_name,
                rel_name
            );
            return;
        }
    };

    // Extract column data from the tuple
    let columns = extract_columns(relation, change);

    // Ignore inner extension tables to avoid infinite pub sub loops
    if rel_name.starts_with("pgmqtt_") {
        return;
    }

    pgrx::log!("pgmqtt: CDC event: {} on {}.{}", op, schema_name, rel_name);

    ring_buffer::push(ring_buffer::ChangeEvent {
        op,
        schema: schema_name,
        table: rel_name,
        columns,
    });

    // The output plugin MUST produce output for pg_logical_slot_get_changes to advance.
    let msg = std::ffi::CString::new(format!("{}", op))
        .unwrap_or_else(|e| pgrx::error!("CString::new failed: {}", e));
    unsafe {
        pg_sys::OutputPluginPrepareWrite(ctx, true);
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

    // Dereference all pointers at the top to minimize nested unsafe blocks
    let rel_data = *relation;
    let tupdesc = rel_data.rd_att;
    if tupdesc.is_null() {
        return columns;
    }

    let tupdesc_ref = &*tupdesc;
    let natts = tupdesc_ref.natts as usize;

    // Access the union field and determine which tuple to use
    let change_ref = &*change;
    let tp = change_ref.data.tp;

    let tuple = match change_ref.action {
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
    };

    if tuple.is_null() {
        return columns;
    }

    // ReorderBufferTupleBuf contains a HeapTupleData
    let heap_tuple = &(*tuple).tuple;

    for i in 0..natts {
        let attrs = (*tupdesc).attrs.as_slice(natts);
        let attr = &attrs[i];

        // Skip dropped columns
        if attr.attisdropped {
            continue;
        }

        let col_name = std::ffi::CStr::from_ptr(attr.attname.data.as_ptr())
            .to_string_lossy()
            .into_owned();

        let mut is_null = false;
        let datum = pg_sys::heap_getattr(
            heap_tuple as *const pg_sys::HeapTupleData as *mut pg_sys::HeapTupleData,
            (i + 1) as i32,
            tupdesc,
            &mut is_null,
        );

        let col_value = if is_null {
            "NULL".to_string()
        } else {
            // Use the type's output function to get text representation
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
        };

        columns.push((col_name, col_value));
    }

    columns
}
