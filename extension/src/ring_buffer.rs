use std::collections::{HashSet, VecDeque};
use std::sync::{Mutex, OnceLock, RwLock};

/// Maximum number of events the ring buffer will hold before dropping the oldest.
const DEFAULT_CAPACITY: usize = 8192;

/// A single CDC event extracted by the output plugin.
#[derive(Debug, Clone)]
pub struct ChangeEvent {
    /// INSERT, UPDATE, or DELETE
    pub op: &'static str,
    pub schema: String,
    pub table: String,
    /// Column name → value (as text). Empty for DELETE w/o REPLICA IDENTITY.
    pub columns: Vec<(String, String)>,
}

/// An event that can be pushed into the ring buffer.
#[derive(Debug, Clone)]
pub enum RingEvent {
    /// A CDC data change on a user table.
    Data(ChangeEvent),
    /// A change to `pgmqtt_topic_mappings` decoded directly from WAL.
    /// The consumer applies this delta in-place to the in-memory mapping cache,
    /// keeping the cache in sync at the exact WAL position of the change.
    MappingUpdate {
        /// INSERT, UPDATE, or DELETE
        op: &'static str,
        /// Column name → value from the WAL record (new tuple for INSERT/UPDATE,
        /// old/PK tuple for DELETE).
        columns: Vec<(String, String)>,
    },
}

/// Fixed-capacity ring buffer that drops the oldest entry on overflow.
struct RingBuffer {
    buf: VecDeque<RingEvent>,
    capacity: usize,
    dropped: u64,
}

impl RingBuffer {
    fn new(capacity: usize) -> Self {
        Self {
            buf: VecDeque::with_capacity(capacity),
            capacity,
            dropped: 0,
        }
    }

    fn push(&mut self, event: RingEvent) {
        if self.buf.len() >= self.capacity {
            self.buf.pop_front();
            self.dropped += 1;
            crate::metrics::inc(&crate::metrics::shared_cdc().ring_buffer_dropped);
            if self.dropped % 100 == 1 {
                pgrx::log!(
                    "WARNING: pgmqtt ring buffer overflow! Dropped {} CDC events so far.",
                    self.dropped
                );
            }
        }
        self.buf.push_back(event);
    }

    fn drain(&mut self) -> Vec<RingEvent> {
        self.buf.drain(..).collect()
    }

}

static RING: Mutex<Option<RingBuffer>> = Mutex::new(None);

/// Push a change event into the global ring buffer.
pub fn push(event: RingEvent) {
    let mut lock = RING.lock().unwrap_or_else(|e| e.into_inner());
    lock.get_or_insert_with(|| RingBuffer::new(DEFAULT_CAPACITY))
        .push(event);
}

/// Drain all buffered events, returning them in FIFO order.
pub fn drain() -> Vec<RingEvent> {
    let mut lock = RING.lock().unwrap_or_else(|e| e.into_inner());
    lock.get_or_insert_with(|| RingBuffer::new(DEFAULT_CAPACITY))
        .drain()
}

// ── Mapped-table fast-path filter ────────────────────────────────────────────
//
// Tracks which (schema, table) pairs have active topic mappings.  The output
// plugin checks this set in `pg_decode_change` before calling `extract_columns`
// so that WAL records for unmapped tables are silently consumed without any
// tuple deserialization cost.
//
// Updated by `cdc_tick`:
//   - on startup, seeded from the slot-checkpoint mapping load
//   - on each MappingUpdate DELETE / INSERT / UPDATE event from the ring buffer

static MAPPED_TABLES: OnceLock<RwLock<HashSet<String>>> = OnceLock::new();

fn mapped_tables() -> &'static RwLock<HashSet<String>> {
    MAPPED_TABLES.get_or_init(|| RwLock::new(HashSet::new()))
}

// Keys are stored as "{schema}\0{table}". The null byte is forbidden in
// PostgreSQL identifiers so it is a safe separator; using a single String per
// entry halves allocations on the hot-path read compared to (String, String).
fn make_key(schema: &str, table: &str) -> String {
    format!("{schema}\0{table}")
}

/// Replace the entire mapped-table set (called once at BGW startup).
pub fn mapped_tables_init(tables: impl IntoIterator<Item = (String, String)>) {
    *mapped_tables().write().unwrap_or_else(|e| e.into_inner()) =
        tables.into_iter().map(|(s, t)| make_key(&s, &t)).collect();
}

/// Add or refresh a single (schema, table) entry.
pub fn mapped_table_add(schema: &str, table: &str) {
    mapped_tables()
        .write()
        .unwrap_or_else(|e| e.into_inner())
        .insert(make_key(schema, table));
}

/// Remove a (schema, table) entry.  No-op if it was not present.
pub fn mapped_table_remove(schema: &str, table: &str) {
    mapped_tables()
        .write()
        .unwrap_or_else(|e| e.into_inner())
        .remove(&make_key(schema, table));
}

/// Returns true if the table has at least one active topic mapping.
pub fn is_table_mapped(schema: &str, table: &str) -> bool {
    mapped_tables()
        .read()
        .unwrap_or_else(|e| e.into_inner())
        .contains(&make_key(schema, table))
}

