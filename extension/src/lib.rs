use pgrx::bgworkers::*;
use pgrx::prelude::*;
use std::time::Duration;

mod ffi_safe;
pub mod inbound_map;
mod init010;
pub mod license;
pub mod metrics;
mod mqtt;
mod ring_buffer;
mod server;
mod subscriptions;
mod topic_map;
mod websocket;

::pgrx::pg_module_magic!(name, version);

// Restrict topic-mapping administration to the extension owner by default.
// DBAs can later open access with:
//   GRANT EXECUTE ON FUNCTION pgmqtt_add_outbound_mapping(...) TO some_role;
extension_sql!(
    r#"
    REVOKE EXECUTE ON FUNCTION pgmqtt_add_outbound_mapping(text, text, text, text, int, text, text) FROM PUBLIC;
    REVOKE EXECUTE ON FUNCTION pgmqtt_remove_outbound_mapping(text, text, text) FROM PUBLIC;
    REVOKE EXECUTE ON FUNCTION pgmqtt_list_outbound_mappings() FROM PUBLIC;
    "#,
    name = "revoke_mapping_from_public",
    requires = [pgmqtt_add_outbound_mapping, pgmqtt_remove_outbound_mapping, pgmqtt_list_outbound_mappings],
);

extension_sql!(
    r#"
    REVOKE EXECUTE ON FUNCTION pgmqtt_add_inbound_mapping(text, text, jsonb, text, text[], text, text, text) FROM PUBLIC;
    REVOKE EXECUTE ON FUNCTION pgmqtt_remove_inbound_mapping(text) FROM PUBLIC;
    REVOKE EXECUTE ON FUNCTION pgmqtt_list_inbound_mappings() FROM PUBLIC;
    "#,
    name = "revoke_inbound_mapping_from_public",
    requires = [pgmqtt_add_inbound_mapping, pgmqtt_remove_inbound_mapping, pgmqtt_list_inbound_mappings],
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

// Observability GUCs (enterprise: metrics feature)
/// How often (seconds) to flush metrics snapshot to DB. 0 = disabled.
static METRICS_SNAPSHOT_INTERVAL: GucSetting<i32> = GucSetting::<i32>::new(60);
/// Days to retain historical metric snapshots. 0 = keep forever.
static METRICS_RETENTION_DAYS: GucSetting<i32> = GucSetting::<i32>::new(3);
/// How often (seconds) to refresh pgmqtt_connections_cache. 0 = disabled.
static METRICS_CONNECTIONS_CACHE_INTERVAL: GucSetting<i32> = GucSetting::<i32>::new(10);
/// Fully-qualified SQL function called after each snapshot flush: fn(jsonb) → void.
static METRICS_HOOK_FUNCTION: GucSetting<Option<CString>> =
    GucSetting::<Option<CString>>::new(None);
/// PostgreSQL NOTIFY channel for streaming metric snapshots as JSON. Empty = disabled.
static METRICS_NOTIFY_CHANNEL: GucSetting<Option<CString>> =
    GucSetting::<Option<CString>>::new(None);

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

pub fn get_metrics_snapshot_interval_guc() -> i32 {
    METRICS_SNAPSHOT_INTERVAL.get()
}

pub fn get_metrics_retention_days_guc() -> i32 {
    METRICS_RETENTION_DAYS.get()
}

pub fn get_metrics_connections_cache_interval_guc() -> i32 {
    METRICS_CONNECTIONS_CACHE_INTERVAL.get()
}

pub fn get_metrics_hook_function_guc() -> String {
    METRICS_HOOK_FUNCTION
        .get()
        .map(|c| c.to_string_lossy().into_owned())
        .unwrap_or_default()
}

pub fn get_metrics_notify_channel_guc() -> String {
    METRICS_NOTIFY_CHANNEL
        .get()
        .map(|c| c.to_string_lossy().into_owned())
        .unwrap_or_default()
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
// SQL-callable functions
// ---------------------------------------------------------------------------

fn ensure_tables_exist() {
    init010::init_010();
}

/// Register a CDC → MQTT outbound topic mapping (persisted to DB table).
///
/// Multiple mappings per (schema, table) are supported via distinct `mapping_name` values,
/// enabling parallel publish to multiple topics (e.g. for gradual reader schema migration).
///
/// Example:
/// ```sql
/// SELECT pgmqtt_add_outbound_mapping(
///     'public',
///     'events',
///     'events/{{ op | lower }}',
///     '{{ columns | tojson }}'
/// );
/// -- Add a second mapping for the same table:
/// SELECT pgmqtt_add_outbound_mapping(
///     'public',
///     'events',
///     'events/v2/{{ op | lower }}',
///     '{"id": "{{ columns.id }}"}',
///     0,
///     'v2'
/// );
/// ```
#[pg_extern]
fn pgmqtt_add_outbound_mapping(
    schema_name: &str,
    table_name: &str,
    topic_template: &str,
    payload_template: &str,
    qos: default!(i32, 0),
    mapping_name: default!(Option<&str>, "NULL"),
    template_type: default!(&str, "'jinja2'"),
) -> &'static str {
    let mapping_name = mapping_name.unwrap_or("default");

    let query = "\
        INSERT INTO pgmqtt_topic_mappings \
            (schema_name, table_name, mapping_name, topic_template, payload_template, qos, template_type) \
        VALUES ($1, $2, $3, $4, $5, $6, $7) \
        ON CONFLICT (schema_name, table_name, mapping_name) DO UPDATE \
        SET topic_template = EXCLUDED.topic_template, \
            payload_template = EXCLUDED.payload_template, \
            qos = EXCLUDED.qos, \
            template_type = EXCLUDED.template_type";

    let args: Vec<pgrx::datum::DatumWithOid> = vec![
        schema_name.into(),
        table_name.into(),
        mapping_name.into(),
        topic_template.into(),
        payload_template.into(),
        qos.into(),
        template_type.into(),
    ];

    pgrx::spi::Spi::connect_mut(|client| client.update(query, None, &args).map(|_| ()))
        .unwrap_or_else(|e| pgrx::error!("pgmqtt: failed to upsert outbound mapping: {}", e));

    pgrx::log!(
        "pgmqtt: added outbound mapping {}.{} (name='{}') → topic='{}' payload='{}' template_type='{}'",
        schema_name,
        table_name,
        mapping_name,
        topic_template,
        payload_template,
        template_type
    );
    "ok"
}

/// Remove a CDC → MQTT outbound topic mapping by name.
///
/// Removes the mapping with the given `mapping_name` (default `'default'`).
/// To remove all mappings for a table, call this once per mapping name.
#[pg_extern]
fn pgmqtt_remove_outbound_mapping(
    schema_name: &str,
    table_name: &str,
    mapping_name: default!(Option<&str>, "NULL"),
) -> bool {
    let mapping_name = mapping_name.unwrap_or("default");

    let query = "DELETE FROM pgmqtt_topic_mappings \
                 WHERE schema_name = $1 AND table_name = $2 AND mapping_name = $3";
    let args: Vec<pgrx::datum::DatumWithOid> =
        vec![schema_name.into(), table_name.into(), mapping_name.into()];

    let deleted = pgrx::spi::Spi::connect_mut(|client| {
        let status = client.update(query, None, &args)?;
        Ok::<bool, spi::Error>(status.len() > 0)
    })
    .unwrap_or(false);

    pgrx::log!(
        "pgmqtt: remove outbound mapping {}.{} (name='{}') → {}",
        schema_name,
        table_name,
        mapping_name,
        if deleted { "removed" } else { "not found" }
    );
    deleted
}

/// List all active outbound topic mappings.
#[pg_extern]
fn pgmqtt_list_outbound_mappings() -> TableIterator<
    'static,
    (
        name!(schema_name, String),
        name!(table_name, String),
        name!(mapping_name, String),
        name!(topic_template, String),
        name!(payload_template, String),
        name!(qos, i32),
        name!(template_type, String),
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
        if let Ok(table) = client.select(
            "SELECT schema_name, table_name, mapping_name, topic_template, payload_template, qos, template_type \
             FROM pgmqtt_topic_mappings",
            None, &[],
        ) {
            for row in table {
                let s: String = row.get_by_name("schema_name").ok().flatten().unwrap_or_default();
                let t: String = row.get_by_name("table_name").ok().flatten().unwrap_or_default();
                let mn: String = row.get_by_name("mapping_name").ok().flatten().unwrap_or_else(|| "default".to_string());
                let tt: String = row.get_by_name("topic_template").ok().flatten().unwrap_or_default();
                let pt: String = row.get_by_name("payload_template").ok().flatten().unwrap_or_default();
                let q: i32 = row.get_by_name("qos").ok().flatten().unwrap_or_default();
                let tmpl: String = row.get_by_name("template_type").ok().flatten().unwrap_or_else(|| "jinja2".to_string());
                rows.push((s, t, mn, tt, pt, q, tmpl));
            }
        }
        Ok::<_, spi::Error>(rows)
    }).unwrap_or_default();

    TableIterator::new(mappings.into_iter())
}

// ---------------------------------------------------------------------------
// Inbound mapping SQL functions
// ---------------------------------------------------------------------------

/// Create or update an inbound MQTT → PostgreSQL table mapping.
///
/// Validates at creation time:
/// 1. Topic pattern syntax (no wildcards, no empty segments, no duplicate vars)
/// 2. Target table exists
/// 3. All column_map keys exist in target table
/// 4. For upsert: conflict_columns exist
/// 5. All `{var}` references in column_map appear in topic pattern
#[pg_extern]
fn pgmqtt_add_inbound_mapping(
    topic_pattern: &str,
    target_table: &str,
    column_map: pgrx::JsonB,
    op: default!(&str, "'insert'"),
    conflict_columns: default!(Option<Vec<String>>, "NULL"),
    target_schema: default!(&str, "'public'"),
    mapping_name: default!(&str, "'default'"),
    template_type: default!(&str, "'jsonpath'"),
) -> &'static str {
    use inbound_map::*;

    // 1. Parse and validate topic pattern
    let segments = match parse_pattern(topic_pattern) {
        Ok(s) => s,
        Err(e) => pgrx::error!("pgmqtt: invalid topic pattern: {}", e),
    };

    // 2. Validate op
    let inbound_op = match InboundOp::parse(op) {
        Ok(o) => o,
        Err(e) => pgrx::error!("pgmqtt: {}", e),
    };

    // 3. Parse column_map JSON object
    let map_obj = match column_map.0.as_object() {
        Some(obj) => obj,
        None => pgrx::error!("pgmqtt: column_map must be a JSON object"),
    };

    let mut parsed_columns: Vec<(String, ColumnSource)> = Vec::new();
    for (col_name, expr_val) in map_obj {
        let expr = match expr_val.as_str() {
            Some(s) => s,
            None => pgrx::error!("pgmqtt: column_map value for '{}' must be a string", col_name),
        };
        let source = match parse_column_source(expr) {
            Ok(s) => s,
            Err(e) => pgrx::error!("pgmqtt: column_map '{}': {}", col_name, e),
        };
        parsed_columns.push((col_name.clone(), source));
    }

    // 4. Validate {var} references
    if let Err(e) = validate_column_map_vars(&parsed_columns, &segments) {
        pgrx::error!("pgmqtt: {}", e);
    }

    // 5. For upsert/delete: require conflict_columns
    if (inbound_op == InboundOp::Upsert || inbound_op == InboundOp::Delete)
        && conflict_columns.is_none()
    {
        pgrx::error!(
            "pgmqtt: op '{}' requires conflict_columns",
            inbound_op.as_str()
        );
    }

    // 6. Verify target table exists and collect column names via pg_catalog
    let col_names: Vec<String> = parsed_columns.iter().map(|(n, _)| n.clone()).collect();

    // Check table existence using parameterized query (safe against injection)
    let qualified_name = format!("{}.{}", quote_ident(target_schema), quote_ident(target_table));
    let table_exists = pgrx::spi::Spi::connect(|client| {
        let result = client.select(
            "SELECT to_regclass($1)::text",
            None,
            &[qualified_name.as_str().into()],
        )?
            .first()
            .get_one::<String>()?;
        Ok::<bool, spi::Error>(result.is_some())
    })
    .unwrap_or(false);

    if !table_exists {
        pgrx::error!(
            "pgmqtt: target table {}.{} does not exist",
            target_schema,
            target_table
        );
    }

    let existing_cols: Vec<String> = pgrx::spi::Spi::connect(|client| {
        let mut cols = Vec::new();
        if let Ok(table) = client.select(
            "SELECT attname::text FROM pg_attribute \
             WHERE attrelid = $1::regclass \
               AND attnum > 0 AND NOT attisdropped",
            None,
            &[qualified_name.as_str().into()],
        ) {
            for row in table {
                if let Ok(Some(name)) = row.get_by_name::<String, _>("attname") {
                    cols.push(name);
                }
            }
        }
        Ok::<_, spi::Error>(cols)
    })
    .unwrap_or_default();

    // 7. Verify all column_map keys exist in target table
    for col_name in &col_names {
        if !existing_cols.contains(col_name) {
            pgrx::error!(
                "pgmqtt: column '{}' does not exist in {}.{}",
                col_name,
                target_schema,
                target_table
            );
        }
    }

    // 8. For upsert: verify conflict_columns exist in the column_map
    if let Some(ref cc) = conflict_columns {
        for c in cc {
            if !col_names.contains(c) {
                pgrx::error!(
                    "pgmqtt: conflict column '{}' is not in column_map",
                    c
                );
            }
        }
    }

    // Persist to database
    let column_map_json = serde_json::to_string(&column_map.0)
        .unwrap_or_else(|e| pgrx::error!("pgmqtt: failed to serialize column_map: {}", e));

    let query = "\
        INSERT INTO pgmqtt_inbound_mappings \
            (mapping_name, topic_pattern, target_schema, target_table, column_map, op, conflict_columns, template_type) \
         VALUES ($1, $2, $3, $4, $5::jsonb, $6, $7::text[], $8) \
         ON CONFLICT (mapping_name) DO UPDATE \
         SET topic_pattern = EXCLUDED.topic_pattern, \
             target_schema = EXCLUDED.target_schema, \
             target_table = EXCLUDED.target_table, \
             column_map = EXCLUDED.column_map, \
             op = EXCLUDED.op, \
             conflict_columns = EXCLUDED.conflict_columns, \
             template_type = EXCLUDED.template_type";

    pgrx::spi::Spi::connect_mut(|client| {
        let args: Vec<pgrx::datum::DatumWithOid> = vec![
            mapping_name.into(),
            topic_pattern.into(),
            target_schema.into(),
            target_table.into(),
            column_map_json.as_str().into(),
            op.into(),
            conflict_columns.into(),
            template_type.into(),
        ];
        client.update(query, None, &args).map(|_| ())
    })
    .unwrap_or_else(|e| pgrx::error!("pgmqtt: failed to upsert inbound mapping: {}", e));

    pgrx::log!(
        "pgmqtt: added inbound mapping '{}' → {}.{} (pattern='{}', op='{}', template_type='{}')",
        mapping_name,
        target_schema,
        target_table,
        topic_pattern,
        op,
        template_type
    );
    "ok"
}

/// Remove an inbound mapping by name.
#[pg_extern]
fn pgmqtt_remove_inbound_mapping(
    mapping_name: default!(&str, "'default'"),
) -> bool {
    let query = "DELETE FROM pgmqtt_inbound_mappings WHERE mapping_name = $1";
    let args: Vec<pgrx::datum::DatumWithOid> = vec![mapping_name.into()];

    let deleted = pgrx::spi::Spi::connect_mut(|client| {
        let status = client.update(query, None, &args)?;
        Ok::<bool, spi::Error>(status.len() > 0)
    })
    .unwrap_or(false);

    pgrx::log!(
        "pgmqtt: remove inbound mapping '{}' → {}",
        mapping_name,
        if deleted { "removed" } else { "not found" }
    );
    deleted
}

/// List all inbound mappings.
#[pg_extern]
fn pgmqtt_list_inbound_mappings() -> TableIterator<
    'static,
    (
        name!(mapping_name, String),
        name!(topic_pattern, String),
        name!(target_schema, String),
        name!(target_table, String),
        name!(column_map, pgrx::JsonB),
        name!(op, String),
        name!(conflict_columns, Option<Vec<String>>),
        name!(template_type, String),
    ),
> {
    let mappings = Spi::connect(|client| {
        let table_exists = client
            .select("SELECT to_regclass('pgmqtt_inbound_mappings')::text", None, &[])?
            .first()
            .get_one::<String>()?
            .is_some();

        if !table_exists {
            return Ok::<_, spi::Error>(Vec::new());
        }

        let mut rows = Vec::new();
        if let Ok(table) = client.select(
            "SELECT mapping_name, topic_pattern, target_schema, target_table, \
                    column_map::text, op, conflict_columns, template_type \
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
                let tmpl: String = row.get_by_name("template_type").ok().flatten().unwrap_or_else(|| "jsonpath".to_string());

                let cm_json: serde_json::Value = serde_json::from_str(&cm_str).unwrap_or(serde_json::json!({}));
                rows.push((mn, tp, ts, tt, pgrx::JsonB(cm_json), op_str, cc, tmpl));
            }
        }
        Ok::<_, spi::Error>(rows)
    })
    .unwrap_or_default();

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
        name!(inbound_mappings, i32),
        name!(inbound_pending, i32),
        name!(dead_letters, i32),
    ),
> {
    let row = Spi::connect(|client| -> Result<(i32, i32, i32, i32, i32, i64, i32, i32, i32), spi::Error> {
        // Single statement: all counts share one snapshot, preventing torn reads.
        let q = "\
            SELECT \
              (SELECT COUNT(*)::int  FROM pgmqtt_sessions         WHERE disconnected_at IS NULL) AS active_connections, \
              (SELECT COUNT(*)::int  FROM pgmqtt_subscriptions)                                  AS total_subscriptions, \
              (SELECT COUNT(*)::int  FROM pgmqtt_retained)                                       AS total_retained_messages, \
              (SELECT COUNT(*)::int  FROM pgmqtt_session_messages)                               AS pending_session_messages, \
              (SELECT COUNT(*)::int  FROM pgmqtt_topic_mappings)                                 AS cdc_mappings, \
              (SELECT COUNT(*)::int8 FROM pg_replication_slots \
                 WHERE slot_name = 'pgmqtt_slot' AND slot_type = 'logical' AND active)           AS cdc_slot_active, \
              (SELECT COUNT(*)::int  FROM pgmqtt_inbound_mappings)                               AS inbound_mappings, \
              (SELECT COUNT(*)::int  FROM pgmqtt_inbound_pending)                                AS inbound_pending, \
              (SELECT COUNT(*)::int  FROM pgmqtt_dead_letters)                                   AS dead_letters\
        ";

        if let Ok(mut rows) = client.select(q, None, &[]) {
            if let Some(row) = rows.next() {
                let ac  = row.get_by_name::<i32, _>("active_connections").ok().flatten().unwrap_or(0);
                let ts  = row.get_by_name::<i32, _>("total_subscriptions").ok().flatten().unwrap_or(0);
                let tr  = row.get_by_name::<i32, _>("total_retained_messages").ok().flatten().unwrap_or(0);
                let ps  = row.get_by_name::<i32, _>("pending_session_messages").ok().flatten().unwrap_or(0);
                let cm  = row.get_by_name::<i32, _>("cdc_mappings").ok().flatten().unwrap_or(0);
                let csa = row.get_by_name::<i64, _>("cdc_slot_active").ok().flatten().unwrap_or(0);
                let im  = row.get_by_name::<i32, _>("inbound_mappings").ok().flatten().unwrap_or(0);
                let ip  = row.get_by_name::<i32, _>("inbound_pending").ok().flatten().unwrap_or(0);
                let dl  = row.get_by_name::<i32, _>("dead_letters").ok().flatten().unwrap_or(0);
                return Ok((ac, ts, tr, ps, cm, csa, im, ip, dl));
            }
        }
        Ok((0, 0, 0, 0, 0, 0, 0, 0, 0))
    }).unwrap_or((0, 0, 0, 0, 0, 0, 0, 0, 0));

    TableIterator::new(std::iter::once(row))
}

// ---------------------------------------------------------------------------
// Enterprise observability SQL functions
// ---------------------------------------------------------------------------

/// Return all broker metrics as named (metric_name, value, unit, description) rows.
///
/// Values are read from `pgmqtt_metrics_current`, which the background worker
/// refreshes every `pgmqtt.metrics_snapshot_interval` seconds.  All timestamps
/// are Unix epoch seconds; use `to_timestamp(value)` to convert.
///
/// Requires an enterprise license with the `metrics` feature.
#[pg_extern]
fn pgmqtt_metrics() -> TableIterator<
    'static,
    (
        name!(metric_name, String),
        name!(value, i64),
        name!(unit, String),
        name!(description, String),
    ),
> {
    if !crate::license::has_feature(crate::license::Feature::Metrics) {
        pgrx::error!(
            "pgmqtt_metrics() requires an enterprise license with the 'metrics' feature"
        );
    }

    let rows = Spi::connect(|client| {
        let mut out: Vec<(String, i64, String, String)> = Vec::new();

        if let Ok(mut rows) = client.select(
            "SELECT captured_at, started_at, last_reset_at,
                    connections_accepted, connections_rejected, connections_current,
                    disconnections_clean, disconnections_unclean, wills_fired,
                    sessions_created, sessions_resumed, sessions_expired,
                    msgs_received, msgs_received_qos0, msgs_received_qos1, bytes_received,
                    msgs_sent, bytes_sent, msgs_dropped_queue_full,
                    pubacks_sent, pubacks_received,
                    subscribe_ops, unsubscribe_ops,
                    cdc_events_processed, cdc_msgs_published,
                    cdc_render_errors, cdc_slot_errors, cdc_persist_errors, cdc_lag_ms_last,
                    inbound_writes_ok, inbound_writes_failed, inbound_retries, inbound_dead_letters,
                    db_batches_committed, db_session_errors, db_message_errors, db_subscription_errors
             FROM pgmqtt_metrics_current
             LIMIT 1",
            None,
            &[],
        ) {
            if let Some(row) = rows.next() {
                macro_rules! m {
                    ($name:expr, $unit:expr, $desc:expr) => {
                        out.push((
                            $name.to_string(),
                            row.get_by_name::<i64, _>($name).ok().flatten().unwrap_or(0),
                            $unit.to_string(),
                            $desc.to_string(),
                        ));
                    };
                }
                m!("captured_at",            "unix_seconds", "Unix timestamp when this snapshot was captured");
                m!("started_at",             "unix_seconds", "Unix timestamp when the broker started");
                m!("last_reset_at",          "unix_seconds", "Unix timestamp of last counter reset");
                m!("connections_accepted",   "total",        "MQTT clients accepted (CONNACK success sent)");
                m!("connections_rejected",   "total",        "Connection attempts rejected (auth or license limit)");
                m!("connections_current",    "gauge",        "Currently active connections");
                m!("disconnections_clean",   "total",        "Clients that sent a normal DISCONNECT packet");
                m!("disconnections_unclean", "total",        "Clients disconnected unexpectedly (keepalive, error)");
                m!("wills_fired",            "total",        "Will messages fired");
                m!("sessions_created",       "total",        "Brand-new sessions created");
                m!("sessions_resumed",       "total",        "Sessions resumed from persistent state");
                m!("sessions_expired",       "total",        "Sessions reaped by the expiry sweeper");
                m!("msgs_received",          "total",        "PUBLISH packets received from clients");
                m!("msgs_received_qos0",     "total",        "QoS 0 PUBLISH packets received");
                m!("msgs_received_qos1",     "total",        "QoS 1 PUBLISH packets received");
                m!("bytes_received",         "bytes",        "Bytes received in PUBLISH payloads");
                m!("msgs_sent",              "total",        "PUBLISH packets delivered to subscribers");
                m!("bytes_sent",             "bytes",        "Bytes sent in PUBLISH payloads");
                m!("msgs_dropped_queue_full", "total",       "Messages dropped due to full client queue");
                m!("pubacks_sent",           "total",        "PUBACK packets sent");
                m!("pubacks_received",       "total",        "PUBACK packets received from clients");
                m!("subscribe_ops",          "total",        "SUBSCRIBE operations");
                m!("unsubscribe_ops",        "total",        "UNSUBSCRIBE operations");
                m!("cdc_events_processed",   "total",        "WAL events decoded from CDC slot");
                m!("cdc_msgs_published",     "total",        "Messages emitted from CDC pipeline");
                m!("cdc_render_errors",      "total",        "CDC template render errors");
                m!("cdc_slot_errors",        "total",        "CDC replication slot errors");
                m!("cdc_persist_errors",     "total",        "CDC message persist errors");
                m!("cdc_lag_ms_last",        "milliseconds", "Most recent CDC replication lag");
                m!("inbound_writes_ok",      "total",        "Successful inbound MQTT-to-DB writes");
                m!("inbound_writes_failed",  "total",        "Failed inbound MQTT-to-DB writes");
                m!("inbound_retries",        "total",        "Inbound write retries");
                m!("inbound_dead_letters",   "total",        "Messages moved to dead-letter table");
                m!("db_batches_committed",   "total",        "DB session action batches committed");
                m!("db_session_errors",      "total",        "DB session operation errors");
                m!("db_message_errors",      "total",        "DB message operation errors");
                m!("db_subscription_errors", "total",        "DB subscription operation errors");
            }
        }
        Ok::<_, spi::Error>(out)
    })
    .unwrap_or_default();

    TableIterator::new(rows.into_iter())
}

/// Return per-client connection detail from `pgmqtt_connections_cache`.
///
/// The cache is refreshed by the background worker every
/// `pgmqtt.metrics_connections_cache_interval` seconds (default 10 s).
/// `connected_at_unix` and `last_activity_at_unix` are Unix epoch seconds;
/// use `to_timestamp(connected_at_unix)` to convert.
///
/// Requires an enterprise license with the `metrics` feature.
#[pg_extern]
fn pgmqtt_connections() -> TableIterator<
    'static,
    (
        name!(client_id, String),
        name!(transport, String),
        name!(connected_at_unix, i64),
        name!(last_activity_at_unix, i64),
        name!(keep_alive_secs, i32),
        name!(msgs_received, i64),
        name!(msgs_sent, i64),
        name!(bytes_received, i64),
        name!(bytes_sent, i64),
        name!(subscriptions, i32),
        name!(queue_depth, i32),
        name!(inflight_count, i32),
        name!(will_set, bool),
        name!(cached_at_unix, i64),
    ),
> {
    if !crate::license::has_feature(crate::license::Feature::Metrics) {
        pgrx::error!(
            "pgmqtt_connections() requires an enterprise license with the 'metrics' feature"
        );
    }

    type Row = (String, String, i64, i64, i32, i64, i64, i64, i64, i32, i32, i32, bool, i64);
    let rows = Spi::connect(|client| {
        let mut out: Vec<Row> = Vec::new();
        if let Ok(table) = client.select(
            "SELECT client_id, transport, connected_at_unix, last_activity_at_unix,
                    keep_alive_secs, msgs_received, msgs_sent, bytes_received, bytes_sent,
                    subscriptions, queue_depth, inflight_count, will_set, cached_at_unix
             FROM pgmqtt_connections_cache
             ORDER BY client_id",
            None,
            &[],
        ) {
            for row in table {
                let g_str  = |n: &str| row.get_by_name::<String, _>(n).ok().flatten().unwrap_or_default();
                let g_i64  = |n: &str| row.get_by_name::<i64, _>(n).ok().flatten().unwrap_or(0);
                let g_i32  = |n: &str| row.get_by_name::<i32, _>(n).ok().flatten().unwrap_or(0);
                let g_bool = |n: &str| row.get_by_name::<bool, _>(n).ok().flatten().unwrap_or(false);
                out.push((
                    g_str("client_id"),
                    g_str("transport"),
                    g_i64("connected_at_unix"),
                    g_i64("last_activity_at_unix"),
                    g_i32("keep_alive_secs"),
                    g_i64("msgs_received"),
                    g_i64("msgs_sent"),
                    g_i64("bytes_received"),
                    g_i64("bytes_sent"),
                    g_i32("subscriptions"),
                    g_i32("queue_depth"),
                    g_i32("inflight_count"),
                    g_bool("will_set"),
                    g_i64("cached_at_unix"),
                ));
            }
        }
        Ok::<_, spi::Error>(out)
    })
    .unwrap_or_default();

    TableIterator::new(rows.into_iter())
}

/// Return all broker metrics in Prometheus text exposition format.
///
/// Reads from `pgmqtt_metrics_current`.  Compatible with `postgres_exporter`
/// custom query files and any Prometheus scraper that can run SQL.
///
/// Requires an enterprise license with the `metrics` feature.
#[pg_extern]
fn pgmqtt_prometheus_metrics() -> String {
    if !crate::license::has_feature(crate::license::Feature::Metrics) {
        pgrx::error!(
            "pgmqtt_prometheus_metrics() requires an enterprise license with the 'metrics' feature"
        );
    }

    // Re-use MetricsSnapshot to build the Prometheus text. We read from the
    // current table (same data that pgmqtt_metrics() returns) rather than the
    // live in-process atomics, since this function runs in a user backend.
    let snap = Spi::connect(|client| {
        let mut s = crate::metrics::MetricsSnapshot::default();
        if let Ok(mut rows) = client.select(
            "SELECT * FROM pgmqtt_metrics_current LIMIT 1", None, &[],
        ) {
            if let Some(row) = rows.next() {
                macro_rules! g {
                    ($field:ident) => {
                        s.$field = row.get_by_name::<i64, _>(stringify!($field))
                            .ok().flatten().unwrap_or(0);
                    };
                    ($field:ident, $col:expr) => {
                        s.$field = row.get_by_name::<i64, _>($col)
                            .ok().flatten().unwrap_or(0);
                    };
                }
                g!(captured_at_unix, "captured_at"); g!(started_at_unix, "started_at"); g!(last_reset_at_unix, "last_reset_at");
                g!(connections_accepted); g!(connections_rejected); g!(connections_current);
                g!(disconnections_clean); g!(disconnections_unclean); g!(wills_fired);
                g!(sessions_created); g!(sessions_resumed); g!(sessions_expired);
                g!(msgs_received); g!(msgs_received_qos0); g!(msgs_received_qos1);
                g!(bytes_received); g!(msgs_sent); g!(bytes_sent); g!(msgs_dropped_queue_full);
                g!(pubacks_sent); g!(pubacks_received);
                g!(subscribe_ops); g!(unsubscribe_ops);
                g!(cdc_events_processed); g!(cdc_msgs_published);
                g!(cdc_render_errors); g!(cdc_slot_errors); g!(cdc_persist_errors);
                g!(cdc_lag_ms_last);
                g!(inbound_writes_ok); g!(inbound_writes_failed); g!(inbound_retries);
                g!(inbound_dead_letters); g!(db_batches_committed);
                g!(db_session_errors); g!(db_message_errors); g!(db_subscription_errors);
            }
        }
        Ok::<_, spi::Error>(s)
    })
    .unwrap_or_default();

    snap.to_prometheus()
}

extension_sql!(
    r#"
    REVOKE EXECUTE ON FUNCTION pgmqtt_metrics() FROM PUBLIC;
    REVOKE EXECUTE ON FUNCTION pgmqtt_connections() FROM PUBLIC;
    REVOKE EXECUTE ON FUNCTION pgmqtt_prometheus_metrics() FROM PUBLIC;
    "#,
    name = "revoke_metrics_from_public",
    requires = [pgmqtt_metrics, pgmqtt_connections, pgmqtt_prometheus_metrics],
);

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
    GucRegistry::define_int_guc(
        c"pgmqtt.metrics_snapshot_interval",
        c"Seconds between metric snapshot flushes to pgmqtt_metrics_snapshots (0 = disabled)",
        c"",
        &METRICS_SNAPSHOT_INTERVAL,
        0, 86400,
        GucContext::Sighup,
        GucFlags::SUPERUSER_ONLY,
    );
    GucRegistry::define_int_guc(
        c"pgmqtt.metrics_retention_days",
        c"Days to retain rows in pgmqtt_metrics_snapshots (0 = keep forever, default 3)",
        c"",
        &METRICS_RETENTION_DAYS,
        0, 3650,
        GucContext::Sighup,
        GucFlags::SUPERUSER_ONLY,
    );
    GucRegistry::define_int_guc(
        c"pgmqtt.metrics_connections_cache_interval",
        c"Seconds between pgmqtt_connections_cache refreshes (0 = disabled)",
        c"",
        &METRICS_CONNECTIONS_CACHE_INTERVAL,
        0, 3600,
        GucContext::Sighup,
        GucFlags::SUPERUSER_ONLY,
    );
    GucRegistry::define_string_guc(
        c"pgmqtt.metrics_hook_function",
        c"SQL function called after each metric flush: schema.function(jsonb) -> void",
        c"",
        &METRICS_HOOK_FUNCTION,
        GucContext::Sighup,
        GucFlags::SUPERUSER_ONLY,
    );
    GucRegistry::define_string_guc(
        c"pgmqtt.metrics_notify_channel",
        c"PostgreSQL NOTIFY channel for streaming metric snapshots as JSON (empty = disabled)",
        c"",
        &METRICS_NOTIFY_CHANNEL,
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

    // Intercept changes to the mapping table and push them into the ring buffer as
    // MappingUpdate events so the consumer can apply deltas in WAL order, keeping the
    // in-memory cache consistent with the WAL position.
    if rel_name == "pgmqtt_topic_mappings" {
        let columns = extract_columns(relation, change);
        ring_buffer::push(ring_buffer::RingEvent::MappingUpdate { op, columns });
        let msg = std::ffi::CString::new("mapping_update")
            .unwrap_or_else(|e| pgrx::error!("CString::new failed: {}", e));
        unsafe {
            pg_sys::OutputPluginPrepareWrite(ctx, true);
            pg_sys::appendStringInfoString((*ctx).out, msg.as_ptr());
            pg_sys::OutputPluginWrite(ctx, true);
        }
        return;
    }

    // Ignore other internal extension tables to avoid infinite pub/sub loops.
    if rel_name.starts_with("pgmqtt_") {
        return;
    }

    // Extract column data from the tuple (only for user tables).
    let columns = extract_columns(relation, change);

    pgrx::log!("pgmqtt: CDC event: {} on {}.{}", op, schema_name, rel_name);

    ring_buffer::push(ring_buffer::RingEvent::Data(ring_buffer::ChangeEvent {
        op,
        schema: schema_name,
        table: rel_name,
        columns,
    }));

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

    // ReorderBufferTupleBuf layout changed in PG 17: the `tuple` field was removed
    // and the HeapTupleData fields are now directly on the struct.
    #[cfg(any(feature = "pg17", feature = "pg18"))]
    let heap_tuple_data = pg_sys::HeapTupleData {
        t_len: (*tuple).t_len,
        t_self: (*tuple).t_self,
        t_tableOid: (*tuple).t_tableOid,
        t_data: (*tuple).t_data,
    };
    #[cfg(not(any(feature = "pg17", feature = "pg18")))]
    let heap_tuple_data = (*tuple).tuple;

    let heap_tuple = &heap_tuple_data;

    for i in 0..natts {
        #[cfg(feature = "pg18")]
        let attr = &*pg_sys::TupleDescAttr(tupdesc, i as i32);
        #[cfg(not(feature = "pg18"))]
        let attr = &(*tupdesc).attrs.as_slice(natts)[i];

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
