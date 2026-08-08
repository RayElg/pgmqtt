//! Cross-process signals between `pgmqtt_mqtt` and `pgmqtt_cdc`.
//!
//! QoS >= 1 never rides shared memory — the durable handoff is
//! `pgmqtt_cdc_outbox`, and this module only adds its wakeup hint. QoS 0
//! within the byte caps rides the inline ring; oversize spills to the
//! outbox.

// `pg_shmem_init!`'s expansion refers to `pg_sys` and `#[pg_guard]` unqualified,
// so both must already be in scope at the call site — hence the prelude import.
use pgrx::prelude::*;
use pgrx::{pg_shmem_init, PgAtomic, PgLwLock};
use std::sync::atomic::{AtomicU64, Ordering};

/// One bulk transaction decodes as an atomic unit, so a burst can exceed
/// the CDC batch size.
const INLINE_RING_CAPACITY: usize = 8192;

const INLINE_TOPIC_CAP: usize = 256;

const INLINE_PAYLOAD_CAP: usize = 1024;

/// Checked inside the CDC batch transaction: what doesn't fit is persisted
/// and queued through the outbox rather than dropped.
pub fn fits_inline(topic: &str, payload: &[u8]) -> bool {
    topic.len() <= INLINE_TOPIC_CAP && payload.len() <= INLINE_PAYLOAD_CAP
}

// ---------------------------------------------------------------------------
// Outbox doorbell (QoS >= 1 and oversize QoS 0, persisted + queued in the DB)
// ---------------------------------------------------------------------------

static OUTBOX_DOORBELL: PgAtomic<AtomicU64> =
    unsafe { PgAtomic::new(c"pgmqtt_bridge_outbox_doorbell") };

/// Must be rung *after* the batch commits, so a woken reader sees the rows.
pub fn ring_outbox_doorbell() {
    OUTBOX_DOORBELL.get().fetch_add(1, Ordering::Relaxed);
}

/// An idle-tick optimization only: the first-tick query and full-fetch
/// re-query keep correctness independent of it.
pub fn outbox_doorbell_seq() -> u64 {
    OUTBOX_DOORBELL.get().load(Ordering::Relaxed)
}

// ---------------------------------------------------------------------------
// WAL flush request (pgmqtt_mqtt -> pgmqtt_cdc)
// ---------------------------------------------------------------------------
//
// pgmqtt_mqtt commits async and gates client-visible effects on the flush
// LSN. Under CDC load the flush advances for free; otherwise this asks for
// one sync commit that group-flushes all earlier WAL. Either way the fsync
// stays off the socket loop.

static FLUSH_REQUEST: PgAtomic<AtomicU64> =
    unsafe { PgAtomic::new(c"pgmqtt_bridge_flush_request") };

/// Idempotent while a request is pending.
pub fn request_wal_flush() {
    FLUSH_REQUEST.get().store(1, Ordering::Relaxed);
}

/// Coalesces every request since the last beacon into one sync commit.
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

/// Must be called from `_PG_init`, so pgmqtt must be in
/// `shared_preload_libraries`.
pub fn init() {
    pg_shmem_init!(OUTBOX_DOORBELL);
    pg_shmem_init!(FLUSH_REQUEST);
    pg_shmem_init!(INLINE_RING);
}
