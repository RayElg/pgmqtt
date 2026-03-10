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

// ---------------------------------------------------------------------------
// Template rendering
// ---------------------------------------------------------------------------

/// Render a CDC event against the topic map.
///
/// Returns `None` if no mapping matches.
pub fn render(
    schema: &str,
    table: &str,
    op: &str,
    columns: &[(String, String)],
) -> Option<RenderedMessage> {
    let lock = MAPPINGS.lock().expect("topic_map: poisoned mutex");

    let mappings = lock.as_ref()?;

    let mapping = match mappings
        .iter()
        .find(|m| m.schema == schema && m.table == table)
    {
        Some(m) => m,
        None => {
            // We log this as a warning, but gracefully return None so the worker
            // does not crash.
            pgrx::log!(
                "WARNING: pgmqtt.topic_map no mapping match for {}.{}. Active mappings: {:?}",
                schema,
                table,
                mappings
                    .iter()
                    .map(|m| format!("{}.{}", m.schema, m.table))
                    .collect::<Vec<_>>()
            );
            return None;
        }
    };

    // Build the column map for template context
    let col_map: HashMap<&str, &str> = columns
        .iter()
        .map(|(k, v)| (k.as_str(), v.as_str()))
        .collect();

    static ENV: OnceLock<Environment<'static>> = OnceLock::new();
    let env = ENV.get_or_init(Environment::new);

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
            pgrx::log!("WARNING: pgmqtt topic template render error: {}", e);
            return None;
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
            pgrx::log!("WARNING: pgmqtt payload template render error: {}", e);
            return None;
        }
    };

    Some(RenderedMessage {
        topic,
        payload: payload.into_bytes(),
        qos: mapping.qos,
    })
}
