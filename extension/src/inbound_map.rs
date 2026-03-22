//! Inbound MQTT → PostgreSQL table mapping.
//!
//! Allows MQTT devices to publish messages that are automatically written
//! to PostgreSQL tables. Topic patterns with `{variable}` segments capture
//! values from the topic path; column maps reference those variables and
//! JSON payload fields.

use std::sync::{Arc, Mutex};

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/// A single segment in a parsed topic pattern.
#[derive(Debug, Clone)]
pub enum PatternSegment {
    Literal(String),
    Variable(String),
}

/// Operation type for the inbound mapping.
#[derive(Debug, Clone, PartialEq)]
pub enum InboundOp {
    Insert,
    Upsert,
    Delete,
}

impl InboundOp {
    pub fn from_str(s: &str) -> Result<Self, String> {
        match s {
            "insert" => Ok(InboundOp::Insert),
            "upsert" => Ok(InboundOp::Upsert),
            "delete" => Ok(InboundOp::Delete),
            _ => Err(format!("invalid op '{}': must be 'insert', 'upsert', or 'delete'", s)),
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            InboundOp::Insert => "insert",
            InboundOp::Upsert => "upsert",
            InboundOp::Delete => "delete",
        }
    }
}

/// A column-value source expression in the column map.
#[derive(Debug, Clone)]
pub enum ColumnSource {
    /// `{var_name}` — from a topic pattern variable
    TopicVariable(String),
    /// `$.path.to.field` — from JSON payload (dot-path)
    JsonPath(Vec<String>),
    /// `$payload` — entire raw payload as text
    RawPayload,
    /// `$topic` — raw topic string
    RawTopic,
}

/// A fully parsed and validated inbound mapping, ready for runtime use.
#[derive(Debug, Clone)]
pub struct InboundMapping {
    pub mapping_name: Arc<str>,
    pub pattern_segments: Vec<PatternSegment>,
    pub target_schema: String,
    pub target_table: String,
    /// (column_name, source_expression)
    pub column_map: Vec<(String, ColumnSource)>,
    pub op: InboundOp,
    pub conflict_columns: Option<Vec<String>>,
    /// Pre-computed SQL statement (Arc: cloned per match without allocation)
    pub sql: Arc<str>,
    /// Original topic_pattern string (for diagnostics / reload)
    pub topic_pattern: String,
    pub template_type: String,
}

/// A pending inbound write, accumulated during a tick.
pub struct PendingInboundWrite {
    pub sql: Arc<str>,
    pub args: Vec<Option<String>>,
    pub client_id: String,
    pub packet_id: Option<u16>,
    pub qos: u8,
    /// Pre-computed args for the pgmqtt_messages INSERT
    pub topic: String,
    pub payload: Vec<u8>,
}

// ---------------------------------------------------------------------------
// In-memory cache
// ---------------------------------------------------------------------------

static INBOUND_MAPPINGS: Mutex<Option<Vec<InboundMapping>>> = Mutex::new(None);

/// Replace the in-memory inbound mapping cache.
pub fn set_mappings(mappings: Vec<InboundMapping>) {
    let mut lock = INBOUND_MAPPINGS.lock().expect("inbound_map: poisoned mutex");
    *lock = Some(mappings);
}

/// Check if mappings have been loaded.
pub fn is_loaded() -> bool {
    let lock = INBOUND_MAPPINGS.lock().expect("inbound_map: poisoned mutex");
    lock.is_some()
}

/// Try to match a topic against all inbound mappings and extract values.
///
/// Returns a list of (mapping_index, extracted_values) for each match.
/// `extracted_values` maps column_name → resolved value string.
///
/// JSON parsing is deferred until at least one pattern matches on segment
/// count, avoiding the ~1-5μs `serde_json::from_slice` cost on the vast
/// majority of publishes that don't hit any inbound mapping.
pub fn try_match(
    topic: &str,
    payload: &[u8],
) -> Vec<(usize, InboundMatchResult)> {
    let lock = INBOUND_MAPPINGS.lock().expect("inbound_map: poisoned mutex");
    let mappings = match lock.as_ref() {
        Some(m) if !m.is_empty() => m,
        _ => return vec![],
    };

    let topic_parts: Vec<&str> = topic.split('/').collect();
    let mut results = Vec::new();

    // Lazy JSON parse: only deserialized on first pattern match.
    let mut json_parsed = false;
    let mut json_value: Option<serde_json::Value> = None;

    for (idx, mapping) in mappings.iter().enumerate() {
        // Fast reject: segment count mismatch (no alloc, no JSON parse)
        if mapping.pattern_segments.len() != topic_parts.len() {
            continue;
        }

        if let Some(topic_vars) = match_pattern(&mapping.pattern_segments, &topic_parts) {
            // Defer JSON parse until we actually need it
            if !json_parsed {
                json_value = serde_json::from_slice(payload).ok();
                json_parsed = true;
            }

            let values = resolve_column_values(
                &mapping.column_map,
                &topic_vars,
                json_value.as_ref(),
                topic,
                payload,
            );
            results.push((idx, InboundMatchResult {
                sql: mapping.sql.clone(),
                values,
                mapping_name: mapping.mapping_name.clone(),
            }));
        }
    }

    results
}

/// Result of matching an inbound topic against a mapping.
pub struct InboundMatchResult {
    pub sql: Arc<str>,
    pub values: Vec<Option<String>>,
    pub mapping_name: Arc<str>,
}

// ---------------------------------------------------------------------------
// Pattern parsing
// ---------------------------------------------------------------------------

/// Parse a topic pattern like `sensor/{site_id}/temperature/{sensor_id}`
/// into segments. Rejects MQTT wildcards (`+`, `#`), empty segments,
/// and duplicate variable names.
pub fn parse_pattern(pattern: &str) -> Result<Vec<PatternSegment>, String> {
    if pattern.is_empty() {
        return Err("topic pattern must not be empty".into());
    }
    if pattern.contains('+') || pattern.contains('#') {
        return Err("topic pattern must not contain MQTT wildcards (+ or #)".into());
    }

    let mut segments = Vec::new();
    let mut seen_vars = std::collections::HashSet::new();

    for (i, part) in pattern.split('/').enumerate() {
        if part.is_empty() {
            return Err(format!("topic pattern has empty segment at position {}", i));
        }

        if part.starts_with('{') && part.ends_with('}') {
            let var_name = &part[1..part.len() - 1];
            if var_name.is_empty() {
                return Err(format!("topic pattern has empty variable name at position {}", i));
            }
            if !seen_vars.insert(var_name.to_string()) {
                return Err(format!("duplicate variable '{}' in topic pattern", var_name));
            }
            segments.push(PatternSegment::Variable(var_name.to_string()));
        } else {
            segments.push(PatternSegment::Literal(part.to_string()));
        }
    }

    Ok(segments)
}

/// Parse a column source expression.
pub fn parse_column_source(expr: &str) -> Result<ColumnSource, String> {
    if expr == "$payload" {
        return Ok(ColumnSource::RawPayload);
    }
    if expr == "$topic" {
        return Ok(ColumnSource::RawTopic);
    }
    if expr.starts_with("$.") {
        let path: Vec<String> = expr[2..].split('.').map(|s| s.to_string()).collect();
        if path.is_empty() || path.iter().any(|s| s.is_empty()) {
            return Err(format!("invalid JSON path: '{}'", expr));
        }
        return Ok(ColumnSource::JsonPath(path));
    }
    if expr.starts_with('{') && expr.ends_with('}') {
        let var_name = &expr[1..expr.len() - 1];
        if var_name.is_empty() {
            return Err("empty variable reference in column map".into());
        }
        return Ok(ColumnSource::TopicVariable(var_name.to_string()));
    }
    Err(format!(
        "invalid column source '{}': expected {{var}}, $.json.path, $payload, or $topic",
        expr
    ))
}

// ---------------------------------------------------------------------------
// Topic matching
// ---------------------------------------------------------------------------

/// Match a topic (split into parts) against parsed pattern segments.
/// Returns captured variable values if matched, None otherwise.
fn match_pattern<'a, 'b>(
    segments: &'b [PatternSegment],
    topic_parts: &[&'a str],
) -> Option<Vec<(&'a str, &'b str)>> {
    if segments.len() != topic_parts.len() {
        return None;
    }

    let mut vars = Vec::new();
    for (seg, part) in segments.iter().zip(topic_parts.iter()) {
        match seg {
            PatternSegment::Literal(lit) => {
                if lit != part {
                    return None;
                }
            }
            PatternSegment::Variable(name) => {
                vars.push((*part, name.as_str()));
            }
        }
    }

    Some(vars)
}

/// Resolve all column values for a matched mapping.
fn resolve_column_values(
    column_map: &[(String, ColumnSource)],
    topic_vars: &[(&str, &str)],
    json_value: Option<&serde_json::Value>,
    topic: &str,
    payload: &[u8],
) -> Vec<Option<String>> {
    column_map
        .iter()
        .map(|(_, source)| resolve_single_value(source, topic_vars, json_value, topic, payload))
        .collect()
}

fn resolve_single_value(
    source: &ColumnSource,
    topic_vars: &[(&str, &str)],
    json_value: Option<&serde_json::Value>,
    topic: &str,
    payload: &[u8],
) -> Option<String> {
    match source {
        ColumnSource::TopicVariable(name) => {
            topic_vars
                .iter()
                .find(|(_, var_name)| var_name == name)
                .map(|(val, _)| val.to_string())
        }
        ColumnSource::JsonPath(path) => {
            let json = json_value?;
            let mut current = json;
            for key in path {
                current = current.get(key)?;
            }
            // Convert JSON value to string representation
            Some(json_value_to_string(current))
        }
        ColumnSource::RawPayload => {
            Some(String::from_utf8_lossy(payload).into_owned())
        }
        ColumnSource::RawTopic => {
            Some(topic.to_string())
        }
    }
}

/// Convert a serde_json::Value to a PostgreSQL-friendly string.
fn json_value_to_string(val: &serde_json::Value) -> String {
    match val {
        serde_json::Value::String(s) => s.clone(),
        serde_json::Value::Number(n) => n.to_string(),
        serde_json::Value::Bool(b) => b.to_string(),
        serde_json::Value::Null => return String::new(), // will be treated as NULL upstream
        // For objects/arrays, return JSON text
        other => other.to_string(),
    }
}

// ---------------------------------------------------------------------------
// SQL generation
// ---------------------------------------------------------------------------

/// Generate the pre-computed SQL for an inbound mapping.
pub fn generate_sql(
    schema: &str,
    table: &str,
    column_names: &[String],
    op: &InboundOp,
    conflict_columns: Option<&[String]>,
) -> String {
    let qualified = format!(
        "{}.{}",
        quote_ident(schema),
        quote_ident(table)
    );
    let cols: Vec<String> = column_names.iter().map(|c| quote_ident(c)).collect();
    let placeholders: Vec<String> = (1..=column_names.len()).map(|i| format!("${}", i)).collect();

    match op {
        InboundOp::Insert => {
            format!(
                "INSERT INTO {} ({}) VALUES ({})",
                qualified,
                cols.join(", "),
                placeholders.join(", ")
            )
        }
        InboundOp::Upsert => {
            let conflict_cols = conflict_columns
                .expect("upsert requires conflict_columns");
            let conflict_quoted: Vec<String> = conflict_cols.iter().map(|c| quote_ident(c)).collect();
            let update_set: Vec<String> = cols
                .iter()
                .filter(|c| !conflict_quoted.contains(c))
                .map(|c| format!("{} = EXCLUDED.{}", c, c))
                .collect();
            if update_set.is_empty() {
                // All columns are conflict columns — DO NOTHING
                format!(
                    "INSERT INTO {} ({}) VALUES ({}) ON CONFLICT ({}) DO NOTHING",
                    qualified,
                    cols.join(", "),
                    placeholders.join(", "),
                    conflict_quoted.join(", ")
                )
            } else {
                format!(
                    "INSERT INTO {} ({}) VALUES ({}) ON CONFLICT ({}) DO UPDATE SET {}",
                    qualified,
                    cols.join(", "),
                    placeholders.join(", "),
                    conflict_quoted.join(", "),
                    update_set.join(", ")
                )
            }
        }
        InboundOp::Delete => {
            let where_cols = conflict_columns
                .expect("delete requires conflict_columns");
            let where_clauses: Vec<String> = where_cols
                .iter()
                .enumerate()
                .map(|(i, c)| {
                    // Find the positional index of this column in column_names
                    let pos = column_names.iter().position(|n| n == c)
                        .expect("conflict column must be in column_map");
                    format!("{} = ${}", quote_ident(c), pos + 1)
                })
                .collect();
            format!(
                "DELETE FROM {} WHERE {}",
                qualified,
                where_clauses.join(" AND ")
            )
        }
    }
}

/// Simple identifier quoting (double-quote, escape internal double-quotes).
fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

// ---------------------------------------------------------------------------
// Validation helpers (used by SQL functions at creation time)
// ---------------------------------------------------------------------------

/// Validate that all `{var}` references in column_map exist in the topic pattern.
pub fn validate_column_map_vars(
    column_map: &[(String, ColumnSource)],
    pattern_segments: &[PatternSegment],
) -> Result<(), String> {
    let pattern_vars: std::collections::HashSet<&str> = pattern_segments
        .iter()
        .filter_map(|s| match s {
            PatternSegment::Variable(name) => Some(name.as_str()),
            _ => None,
        })
        .collect();

    for (col_name, source) in column_map {
        if let ColumnSource::TopicVariable(var_name) = source {
            if !pattern_vars.contains(var_name.as_str()) {
                return Err(format!(
                    "column '{}' references topic variable '{{{}}}' which is not in the topic pattern",
                    col_name, var_name
                ));
            }
        }
    }

    Ok(())
}
