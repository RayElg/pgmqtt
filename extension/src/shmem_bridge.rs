//! Cross-process handoff from the `pgmqtt_cdc` worker to the `pgmqtt_mqtt`
//! worker, backed by real PostgreSQL shared memory.
//!
//! Rust's ordinary statics (`Mutex<...>`, as used by `ring_buffer`) only work
//! within a single OS process. Once CDC decoding moves into its own
//! background worker, the resulting messages have to cross a process
//! boundary to reach the sockets owned by `pgmqtt_mqtt`. What crosses, and
//! how, is split by durability — not everything belongs in shared memory:
//!
//! - **QoS >= 1 messages are never carried in shared memory at all.** They
//!   are already durably persisted to `pgmqtt_messages` by the CDC worker,
//!   and their ids are queued to `pgmqtt_cdc_outbox` *inside the same
//!   transaction that advances the replication slot* (see
//!   `server::cdc_worker`). The database is the handoff medium: it is
//!   durable across crashes, preserves insertion order, and never "fills
//!   up" the way a fixed-size ring does — so at-least-once delivery holds
//!   with no drop-on-overflow path anywhere. The only thing this module
//!   contributes for that flow is the [`ring_outbox_doorbell`] counter, a
//!   wakeup hint that lets `pgmqtt_mqtt` skip polling the outbox table on
//!   idle ticks.
//! - **QoS 0 messages that fit the fixed byte caps travel inline** through a
//!   bounded ring. They were never persisted (fire-and-forget), so there is
//!   no id to pass; loss on overflow is acceptable for QoS 0 and the ring
//!   avoids paying a database write for every fire-and-forget message.
//!   QoS 0 messages that *don't* fit ([`fits_inline`]) take the outbox path
//!   above instead — persisted, delivered once, then reclaimed — so an
//!   oversize payload is delivered rather than dropped.

// `pg_shmem_init!`'s expansion refers to `pg_sys` and `#[pg_guard]` unqualified,
// so both must already be in scope at the call site — hence the prelude import.
use pgrx::prelude::*;
use pgrx::{pg_shmem_init, PgAtomic, PgLwLock};
use std::sync::atomic::{AtomicU64, Ordering};

/// Max buffered QoS 0 messages awaiting delivery.
///
/// A single large transaction (e.g. one bulk `INSERT ... SELECT
/// generate_series(...)`) is replayed to the output plugin as one atomic
/// unit — `pg_logical_slot_get_changes`'s `upto_nchanges` limit is a soft
/// cap that yields to transaction boundaries — so a single push burst can
/// exceed the CDC batch size. QoS 0 is best-effort (matches ring_buffer's
/// own DEFAULT_CAPACITY for the same reason), so this is sized generously
/// rather than exactly — it bounds worst-case loss, not eliminates it.
const INLINE_RING_CAPACITY: usize = 8192;

/// Max inline topic length. Longer topics take the outbox path (see module docs).
const INLINE_TOPIC_CAP: usize = 256;

/// Max inline payload length. Longer payloads take the outbox path (see module docs).
const INLINE_PAYLOAD_CAP: usize = 1024;

/// Whether a QoS 0 message fits the inline ring's fixed shared-memory slots.
/// The CDC worker checks this *inside* its batch transaction: a message that
/// doesn't fit is persisted and queued through `pgmqtt_cdc_outbox` like a
/// QoS >= 1 message instead, so it is delivered rather than dropped.
pub fn fits_inline(topic: &str, payload: &[u8]) -> bool {
    topic.len() <= INLINE_TOPIC_CAP && payload.len() <= INLINE_PAYLOAD_CAP
}

// ---------------------------------------------------------------------------
// Outbox doorbell (QoS >= 1 and oversize QoS 0, persisted + queued in the DB)
// ---------------------------------------------------------------------------

static OUTBOX_DOORBELL: PgAtomic<AtomicU64> =
    unsafe { PgAtomic::new(c"pgmqtt_bridge_outbox_doorbell") };

/// Signal `pgmqtt_mqtt` that new rows were committed to `pgmqtt_cdc_outbox`.
/// Called by the CDC worker *after* the batch transaction commits, so a
/// reader woken by the new value always sees the committed rows.
pub fn ring_outbox_doorbell() {
    OUTBOX_DOORBELL.get().fetch_add(1, Ordering::Relaxed);
}

/// Current doorbell value. `pgmqtt_mqtt` compares against the last value it
/// saw and queries the outbox only when it changed — a missed increment is
/// impossible (the counter only grows) and at worst costs one extra query.
/// This is purely an idle-tick optimization: correctness never depends on
/// it, because the delivery worker also does an unconditional first-tick
/// query (crash recovery) and keeps re-querying while a fetch comes back
/// full.
pub fn outbox_doorbell_seq() -> u64 {
    OUTBOX_DOORBELL.get().load(Ordering::Relaxed)
}

// ---------------------------------------------------------------------------
// WAL flush request (pgmqtt_mqtt -> pgmqtt_cdc)
// ---------------------------------------------------------------------------
//
// In the enterprise topology, pgmqtt_mqtt commits its write transactions
// with synchronous_commit = off and defers client-visible effects (PUBACKs,
// QoS >= 1 delivery) until pg_current_wal_flush_lsn() covers them, so the
// socket loop never blocks on an fsync. Under CDC load the flush advances
// for free (the CDC worker's batch commits are synchronous); when it
// doesn't, pgmqtt_mqtt raises this flag and the CDC worker issues one small
// synchronous commit, which group-flushes all earlier WAL — the fsync
// happens off the socket loop either way.

static FLUSH_REQUEST: PgAtomic<AtomicU64> =
    unsafe { PgAtomic::new(c"pgmqtt_bridge_flush_request") };

/// Ask the CDC worker to force a WAL flush soon (idempotent while pending).
pub fn request_wal_flush() {
    FLUSH_REQUEST.get().store(1, Ordering::Relaxed);
}

/// Consume a pending flush request (CDC worker side). Returns whether one
/// was pending. Swap semantics coalesce any number of requests raised since
/// the last beacon into a single synchronous commit.
pub fn take_wal_flush_request() -> bool {
    FLUSH_REQUEST.get().swap(0, Ordering::Relaxed) == 1
}

// ---------------------------------------------------------------------------
// Inline ring (QoS 0, never persisted)
// ---------------------------------------------------------------------------

#[derive(Copy, Clone)]
struct InlineMsg {
    topic_len: u16,
    payload_len: u16,
    topic: [u8; INLINE_TOPIC_CAP],
    payload: [u8; INLINE_PAYLOAD_CAP],
}

impl Default for InlineMsg {
    fn default() -> Self {
        Self {
            topic_len: 0,
            payload_len: 0,
            topic: [0u8; INLINE_TOPIC_CAP],
            payload: [0u8; INLINE_PAYLOAD_CAP],
        }
    }
}

unsafe impl pgrx::PGRXSharedMemory for InlineMsg {}

#[derive(Copy, Clone)]
struct InlineRingShared {
    slots: [InlineMsg; INLINE_RING_CAPACITY],
    head: u32,
    len: u32,
}

impl Default for InlineRingShared {
    fn default() -> Self {
        Self {
            slots: [InlineMsg::default(); INLINE_RING_CAPACITY],
            head: 0,
            len: 0,
        }
    }
}

unsafe impl pgrx::PGRXSharedMemory for InlineRingShared {}

static INLINE_RING: PgLwLock<InlineRingShared> =
    unsafe { PgLwLock::new(c"pgmqtt_bridge_inline_ring") };

/// Push a QoS 0 message inline. Drops the oldest buffered message on ring
/// overflow (counted in `cdc_bridge_dropped`).
///
/// Callers are expected to have checked [`fits_inline`] already and routed
/// oversize messages through the outbox; an oversize message reaching this
/// function is a caller bug, handled by dropping (counted, logged) rather
/// than corrupting the fixed-size slot.
pub fn push_inline(topic: &str, payload: &[u8]) -> bool {
    if !fits_inline(topic, payload) {
        crate::metrics::inc(&crate::metrics::shared_cdc().bridge_dropped);
        pgrx::log!(
            "pgmqtt cdc: BUG: oversize QoS 0 message for '{}' reached push_inline \
             ({} byte topic / {} byte payload, caps {}/{}) — dropped; \
             should have been routed through pgmqtt_cdc_outbox",
            topic,
            topic.len(),
            payload.len(),
            INLINE_TOPIC_CAP,
            INLINE_PAYLOAD_CAP,
        );
        return false;
    }

    let mut ring = INLINE_RING.exclusive();
    if ring.len as usize >= INLINE_RING_CAPACITY {
        ring.head = (ring.head + 1) % INLINE_RING_CAPACITY as u32;
        ring.len -= 1;
        crate::metrics::inc(&crate::metrics::shared_cdc().bridge_dropped);
    }
    let tail = (ring.head + ring.len) % INLINE_RING_CAPACITY as u32;
    let slot = &mut ring.slots[tail as usize];
    slot.topic_len = topic.len() as u16;
    slot.topic[..topic.len()].copy_from_slice(topic.as_bytes());
    slot.payload_len = payload.len() as u16;
    slot.payload[..payload.len()].copy_from_slice(payload);
    ring.len += 1;
    true
}

/// Drain every QoS 0 message currently buffered, in FIFO order.
pub fn drain_inline() -> Vec<(String, Vec<u8>)> {
    let mut ring = INLINE_RING.exclusive();
    let mut out = Vec::with_capacity(ring.len as usize);
    for i in 0..ring.len {
        let idx = (ring.head + i) % INLINE_RING_CAPACITY as u32;
        let slot = &ring.slots[idx as usize];
        let topic = String::from_utf8_lossy(&slot.topic[..slot.topic_len as usize]).into_owned();
        let payload = slot.payload[..slot.payload_len as usize].to_vec();
        out.push((topic, payload));
    }
    ring.head = 0;
    ring.len = 0;
    out
}

/// Register the bridge's shared memory with PostgreSQL. Must be called from
/// `_PG_init` — the extension must be loaded via `shared_preload_libraries`.
pub fn init() {
    pg_shmem_init!(OUTBOX_DOORBELL);
    pg_shmem_init!(FLUSH_REQUEST);
    pg_shmem_init!(INLINE_RING);
}
