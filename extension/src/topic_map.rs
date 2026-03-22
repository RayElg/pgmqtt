//! Topic mapping: admin-configured `(schema, table) → (topic_template, payload_template)`.
//!
//! Mappings are persisted in the `pgmqtt.topic_mappings` table so they are visible
//! across all PostgreSQL processes (backend connections AND background workers).
//!
//! Templates are rendered via *minijinja* with context variables:
//! `schema`, `table`, `op`, `columns` (a map of column name → value string).

use minijinja::{context, Environment};
use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};

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

/// A rendered MQTT message ready for the per-topic buffer.
#[derive(Debug, Clone)]
pub struct RenderedMessage {
    pub topic: String,
    pub payload: Vec<u8>,
    pub qos: u8,
}

// ---------------------------------------------------------------------------
// In-process cache (loaded from DB by the consumer worker)
// ---------------------------------------------------------------------------

static MAPPINGS: Mutex<Option<Vec<TopicMapping>>> = Mutex::new(None);

#[allow(dead_code)]
fn with_mappings<R>(f: impl FnOnce(&Vec<TopicMapping>) -> R) -> Option<R> {
    let lock = MAPPINGS.lock().expect("topic_map: poisoned mutex");
    lock.as_ref().map(f)
}

/// Replace the in-process cache with a fresh set of mappings.
/// Called by the consumer worker after loading from the DB via SPI.
pub fn set_mappings(mappings: Vec<TopicMapping>) {
    let mut lock = MAPPINGS.lock().expect("topic_map: poisoned mutex");
    *lock = Some(mappings);
}

/// Get the current in-process mapping count (for diagnostics).
#[allow(dead_code)]
pub fn mapping_count() -> usize {
    with_mappings(|m| m.len()).unwrap_or(0)
}

/// Get a clone of the current mappings (for diagnostics).
#[allow(dead_code)]
pub fn get() -> Option<Vec<TopicMapping>> {
    let lock = MAPPINGS.lock().expect("topic_map: poisoned mutex");
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
    let lock = MAPPINGS.lock().expect("topic_map: poisoned mutex");

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
    let mut matched = false;

    for mapping in mappings.iter().filter(|m| m.schema == schema && m.table == table) {
        matched = true;

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
                continue;
            }
        };

        results.push(RenderedMessage {
            topic,
            payload: payload.into_bytes(),
            qos: mapping.qos,
        });
    }

    if !matched {
        pgrx::log!(
            "WARNING: pgmqtt.topic_map no mapping match for {}.{}. Active mappings: {:?}",
            schema,
            table,
            mappings
                .iter()
                .map(|m| format!("{}.{} ({})", m.schema, m.table, m.name))
                .collect::<Vec<_>>()
        );
    }

    results
}
