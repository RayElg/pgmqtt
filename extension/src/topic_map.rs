//! Topic mapping: admin-configured `(schema, table) → (topic_template, payload_template)`.
//!
//! Mappings are persisted in the `pgmqtt.topic_mappings` table so they are visible
//! across all PostgreSQL processes (backend connections AND background workers).
//!
//! Templates are rendered via *minijinja* with context variables:
//! `schema`, `table`, `op`, `columns` (a map of column name → value string).

use minijinja::{context, Environment};
use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct TopicMapping {
    pub name: String,
    pub schema: String,
    pub table: String,
    pub topic_template: String,
    pub payload_template: String,
    pub qos: u8,
}

// NOTE: template_type is persisted in the DB for future extensibility (e.g. jsonpath)
// but is not read at runtime — the engine is always minijinja for outbound mappings.

/// A rendered MQTT message ready for the per-topic buffer.
#[derive(Debug, Clone)]
pub struct RenderedMessage {
    pub topic: Arc<str>,
    pub payload: Arc<[u8]>,
    pub qos: u8,
}

// ---------------------------------------------------------------------------
// In-process cache (loaded from DB by the consumer worker)
// ---------------------------------------------------------------------------

static MAPPINGS: Mutex<Option<Vec<TopicMapping>>> = Mutex::new(None);

/// Replace the in-process cache with a fresh set of mappings.
/// Called by the consumer worker after loading from the DB via SPI.
pub fn set_mappings(mappings: Vec<TopicMapping>) {
    let mut lock = MAPPINGS.lock().expect("topic_map: poisoned mutex");
    *lock = Some(mappings);
}

/// Get a clone of the current mappings.
pub fn get() -> Option<Vec<TopicMapping>> {
    let lock = MAPPINGS.lock().unwrap_or_else(|e| e.into_inner());
    lock.clone()
}

/// Apply a WAL-decoded INSERT or UPDATE to the in-process cache.
/// Upserts by (schema, table, name) — safe to call during WAL replay on restart.
pub fn wal_upsert(mapping: TopicMapping) {
    let mut lock = MAPPINGS.lock().expect("topic_map: poisoned mutex");
    let mappings = lock.get_or_insert_with(Vec::new);
    match mappings
        .iter_mut()
        .find(|m| m.schema == mapping.schema && m.table == mapping.table && m.name == mapping.name)
    {
        Some(existing) => *existing = mapping,
        None => mappings.push(mapping),
    }
}

/// Apply a WAL-decoded DELETE to the in-process cache.
/// No-op if the mapping is not found (safe for idempotent WAL replay).
pub fn wal_remove(schema: &str, table: &str, name: &str) {
    let mut lock = MAPPINGS.lock().expect("topic_map: poisoned mutex");
    if let Some(mappings) = lock.as_mut() {
        mappings.retain(|m| !(m.schema == schema && m.table == table && m.name == name));
    }
}

// ---------------------------------------------------------------------------
// Template rendering
// ---------------------------------------------------------------------------

/// Render a CDC event against the topic map.
///
/// Returns one `RenderedMessage` per matching mapping. Returns an empty vec if
/// no mapping matches or if mappings have not been loaded yet.
pub fn render(
    schema: &str,
    table: &str,
    op: &str,
    columns: &[(String, String)],
) -> Vec<RenderedMessage> {
    let lock = MAPPINGS.lock().unwrap_or_else(|e| e.into_inner());

    let mappings = match lock.as_ref() {
        Some(m) => m,
        None => return vec![],
    };

    // Build the column map for template context once, shared across all matching mappings.
    let col_map: HashMap<&str, &str> = columns
        .iter()
        .map(|(k, v)| (k.as_str(), v.as_str()))
        .collect();

    static ENV: OnceLock<Environment<'static>> = OnceLock::new();
    let env = ENV.get_or_init(Environment::new);

    let mut results = Vec::new();

    for mapping in mappings.iter().filter(|m| m.schema == schema && m.table == table) {

        let topic = match env.render_str(
            &mapping.topic_template,
            context! {
                schema => schema,
                table => table,
                op => op,
                columns => col_map,
            },
        ) {
            Ok(t) => t,
            Err(e) => {
                pgrx::log!(
                    "WARNING: pgmqtt topic template render error for mapping '{}': {}",
                    mapping.name,
                    e
                );
                crate::metrics::inc(&crate::metrics::get().cdc_render_errors);
                continue;
            }
        };

        let payload = match env.render_str(
            &mapping.payload_template,
            context! {
                schema => schema,
                table => table,
                op => op,
                columns => col_map,
            },
        ) {
            Ok(p) => p,
            Err(e) => {
                pgrx::log!(
                    "WARNING: pgmqtt payload template render error for mapping '{}': {}",
                    mapping.name,
                    e
                );
                crate::metrics::inc(&crate::metrics::get().cdc_render_errors);
                continue;
            }
        };

        results.push(RenderedMessage {
            topic: Arc::from(topic.as_str()),
            payload: Arc::from(payload.into_bytes()),
            qos: mapping.qos,
        });
    }

    results
}
