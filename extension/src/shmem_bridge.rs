//! Cross-process signals between the pgmqtt workers, backed by PostgreSQL
//! shared memory. What crosses here, and what doesn't, is split by
//! durability — not everything belongs in shared memory:
//!
//! - **QoS >= 1 messages are never carried in shared memory at all.** They
//!   are already durably persisted to `pgmqtt_messages` by the CDC worker,
//!   and their ids are queued to `pgmqtt_cdc_outbox` *inside the same
//!   transaction that persists them*, with the replication slot advanced
//!   only after that commit (see `server::cdc_worker`). The database is
//!   the handoff medium: it is
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
// Outbox enqueue floors (socket_workers > 1)
// ---------------------------------------------------------------------------
//
// Multi-worker delivery cursors consume `pgmqtt_cdc_outbox` in id order, but
// ids (= pgmqtt_messages sequence values) are assigned in *allocation*
// order while transactions commit in any order: a worker's fetch can see id
// 100 committed while id 99's transaction is still in flight, advance its
// cursor to 100, and never deliver 99 — even though 99's publisher gets a
// PUBACK once that transaction commits. To make the consumed prefix stable,
// every process that enqueues outbox rows publishes a *floor* — a
// pgmqtt_messages sequence value read before any of its inserts, so every
// id it will enqueue is strictly greater — for the duration of its write
// transaction. Readers only trust ids at or below the minimum active floor
// ([`enqueue_barrier`]): any in-flight enqueuer that could still commit a
// smaller id is, by construction, holding a floor below that id.
//
// Only pgmqtt's own writers enqueue outbox rows, so the barrier can only be
// held down by a pgmqtt write transaction (one tick, bounded) — unlike a
// `pg_snapshot_xmin()` barrier, which any long-running user transaction
// would pin for its whole lifetime, stalling delivery cluster-wide.

/// One floor slot per socket worker plus one for the CDC worker.
const FLOOR_SLOTS: usize = crate::MAX_SOCKET_WORKERS as usize + 1;

/// Floor slot index for the CDC worker (socket workers use their own slot).
pub const CDC_FLOOR_SLOT: usize = crate::MAX_SOCKET_WORKERS as usize;

/// i64::MAX = no enqueue in flight from this slot.
#[derive(Copy, Clone)]
struct EnqueueFloors {
    floors: [i64; FLOOR_SLOTS],
}

impl Default for EnqueueFloors {
    fn default() -> Self {
        Self {
            floors: [i64::MAX; FLOOR_SLOTS],
        }
    }
}

unsafe impl pgrx::PGRXSharedMemory for EnqueueFloors {}

static ENQUEUE_FLOORS: PgLwLock<EnqueueFloors> =
    unsafe { PgLwLock::new(c"pgmqtt_bridge_enqueue_floors") };

/// Publish this process's enqueue floor. Must be called *before* the first
/// message/outbox insert of the write transaction, with a sequence value
/// read at that point; cleared (with [`clear_enqueue_floor`]) only after
/// the transaction commits or aborts.
pub fn publish_enqueue_floor(slot: usize, floor: i64) {
    if let Some(f) = ENQUEUE_FLOORS.exclusive().floors.get_mut(slot) {
        *f = floor;
    }
}

/// Clear this process's enqueue floor after its write transaction ends.
/// Also called once at worker startup so a floor orphaned by a crash (which
/// would freeze every cursor) never outlives the transaction it covered —
/// by restart time that transaction has certainly committed or aborted.
pub fn clear_enqueue_floor(slot: usize) {
    if let Some(f) = ENQUEUE_FLOORS.exclusive().floors.get_mut(slot) {
        *f = i64::MAX;
    }
}

/// Highest outbox id that delivery cursors may consume: every id above this
/// might still gain a smaller committed sibling from an in-flight enqueue.
/// `i64::MAX` when no enqueue is in flight.
pub fn enqueue_barrier() -> i64 {
    ENQUEUE_FLOORS
        .share()
        .floors
        .iter()
        .copied()
        .min()
        .unwrap_or(i64::MAX)
}

// ---------------------------------------------------------------------------
// Cross-worker command rings (socket_workers > 1)
// ---------------------------------------------------------------------------
//
// With several socket workers, a client's connection can live in any of
// them, so two things need a cross-worker control path: session takeover
// (a new CONNECT with an existing client_id must disconnect the old
// connection wherever it is) and admin commands (slot 0 drains the
// pgmqtt_admin_commands table and fans the command out — disconnects and
// ACL reloads must reach every worker's local clients). Commands are tiny
// and rare, so a small fixed ring per worker suffices; on overflow the
// oldest command is dropped with a log line.
//
// The two traffics get *separate* rings per worker: takeover kicks are
// best-effort CONNECT-rate traffic (a lost kick self-heals via keepalive
// timeout), while admin commands are one-shot security controls whose DB
// row is already consumed by the time they're queued here — CONNECT churn
// must not be able to evict a pending disconnect or ACL reload.

const CMD_RING_CAPACITY: usize = 256;
/// Also the broker's client-id admission bound: CONNECT enforces
/// `client_id.len() <= CMD_ARG_CAP` (see `finish_connect`), so every
/// admitted client can be addressed by cross-worker commands. Role names
/// fit for free (PostgreSQL caps them at NAMEDATALEN-1 = 63 bytes).
pub const CMD_ARG_CAP: usize = 128;
const MAX_RINGS: usize = crate::MAX_SOCKET_WORKERS as usize;

/// Which per-worker ring a command travels through (see above).
#[derive(Copy, Clone)]
pub enum RingClass {
    /// Best-effort session-takeover kicks (CONNECT-rate, self-healing).
    Takeover,
    /// One-shot admin/security commands (rare, must not be evicted by
    /// takeover churn).
    Admin,
}

/// Decoded cross-worker command (mirrors `admin_commands::Command`).
pub enum WorkerCommand {
    DisconnectClient { client_id: String, reason: u8 },
    DisconnectRole { role_name: String, reason: u8 },
    ReloadAcls { target: String },
}

impl From<&crate::admin_commands::Command> for WorkerCommand {
    fn from(cmd: &crate::admin_commands::Command) -> Self {
        use crate::admin_commands::Command;
        match cmd {
            Command::DisconnectClient { client_id, reason } => WorkerCommand::DisconnectClient {
                client_id: client_id.clone(),
                reason: *reason,
            },
            Command::DisconnectRole { role_name, reason } => WorkerCommand::DisconnectRole {
                role_name: role_name.clone(),
                reason: *reason,
            },
            Command::ReloadAcls { target } => WorkerCommand::ReloadAcls {
                target: target.clone(),
            },
        }
    }
}

impl From<WorkerCommand> for crate::admin_commands::Command {
    fn from(wc: WorkerCommand) -> Self {
        use crate::admin_commands::Command;
        match wc {
            WorkerCommand::DisconnectClient { client_id, reason } => {
                Command::DisconnectClient { client_id, reason }
            }
            WorkerCommand::DisconnectRole { role_name, reason } => {
                Command::DisconnectRole { role_name, reason }
            }
            WorkerCommand::ReloadAcls { target } => Command::ReloadAcls { target },
        }
    }
}

#[derive(Copy, Clone)]
struct CmdSlot {
    op: u8,
    reason: u8,
    arg_len: u16,
    arg: [u8; CMD_ARG_CAP],
}

impl Default for CmdSlot {
    fn default() -> Self {
        Self {
            op: 0,
            reason: 0,
            arg_len: 0,
            arg: [0u8; CMD_ARG_CAP],
        }
    }
}

unsafe impl pgrx::PGRXSharedMemory for CmdSlot {}

#[derive(Copy, Clone)]
struct CmdRing {
    slots: [CmdSlot; CMD_RING_CAPACITY],
    head: u32,
    len: u32,
}

impl Default for CmdRing {
    fn default() -> Self {
        Self {
            slots: [CmdSlot::default(); CMD_RING_CAPACITY],
            head: 0,
            len: 0,
        }
    }
}

unsafe impl pgrx::PGRXSharedMemory for CmdRing {}

#[derive(Copy, Clone)]
struct CmdRings {
    takeover: [CmdRing; MAX_RINGS],
    admin: [CmdRing; MAX_RINGS],
}

impl Default for CmdRings {
    fn default() -> Self {
        Self {
            takeover: [CmdRing::default(); MAX_RINGS],
            admin: [CmdRing::default(); MAX_RINGS],
        }
    }
}

impl CmdRings {
    fn class(&mut self, class: RingClass) -> &mut [CmdRing; MAX_RINGS] {
        match class {
            RingClass::Takeover => &mut self.takeover,
            RingClass::Admin => &mut self.admin,
        }
    }
}

unsafe impl pgrx::PGRXSharedMemory for CmdRings {}

static CMD_RINGS: PgLwLock<CmdRings> = unsafe { PgLwLock::new(c"pgmqtt_bridge_cmd_rings") };

fn encode(cmd: &WorkerCommand) -> Option<CmdSlot> {
    let (op, reason, arg): (u8, u8, &str) = match cmd {
        WorkerCommand::DisconnectClient { client_id, reason } => (0, *reason, client_id),
        WorkerCommand::DisconnectRole { role_name, reason } => (1, *reason, role_name),
        WorkerCommand::ReloadAcls { target } => (2, 0, target),
    };
    if arg.len() > CMD_ARG_CAP {
        pgrx::log!(
            "pgmqtt: cross-worker command argument too long ({} bytes, cap {}) — not forwarded",
            arg.len(),
            CMD_ARG_CAP
        );
        return None;
    }
    let mut slot = CmdSlot {
        op,
        reason,
        arg_len: arg.len() as u16,
        ..Default::default()
    };
    slot.arg[..arg.len()].copy_from_slice(arg.as_bytes());
    Some(slot)
}

fn decode(slot: &CmdSlot) -> Option<WorkerCommand> {
    let arg = String::from_utf8_lossy(&slot.arg[..slot.arg_len as usize]).into_owned();
    match slot.op {
        0 => Some(WorkerCommand::DisconnectClient {
            client_id: arg,
            reason: slot.reason,
        }),
        1 => Some(WorkerCommand::DisconnectRole {
            role_name: arg,
            reason: slot.reason,
        }),
        2 => Some(WorkerCommand::ReloadAcls { target: arg }),
        _ => None,
    }
}

/// Queue `cmd` on the given ring class for every socket worker except
/// `exclude_slot` (pass -1 to include all). Drops the oldest queued command
/// per ring on overflow. Returns whether the command was encodable — a
/// `false` means it reached **no** worker (argument over [`CMD_ARG_CAP`]),
/// which callers must treat as a delivery failure, not silently ignore.
#[must_use]
pub fn broadcast_command(
    exclude_slot: i32,
    workers: i32,
    class: RingClass,
    cmd: &WorkerCommand,
) -> bool {
    let Some(encoded) = encode(cmd) else {
        return false;
    };
    let workers = (workers.clamp(1, MAX_RINGS as i32)) as usize;
    let mut rings = CMD_RINGS.exclusive();
    let rings = rings.class(class);
    for (slot_idx, ring) in rings.iter_mut().enumerate().take(workers) {
        if slot_idx as i32 == exclude_slot {
            continue;
        }
        if ring.len as usize >= CMD_RING_CAPACITY {
            ring.head = (ring.head + 1) % CMD_RING_CAPACITY as u32;
            ring.len -= 1;
            pgrx::log!(
                "pgmqtt: cross-worker command ring for slot {} overflowed — oldest dropped",
                slot_idx
            );
        }
        let tail = (ring.head + ring.len) % CMD_RING_CAPACITY as u32;
        ring.slots[tail as usize] = encoded;
        ring.len += 1;
    }
    true
}

/// Drain every command queued for `slot`, in FIFO order. Admin commands
/// come first: they are rarer, security-relevant, and must not wait behind
/// takeover churn.
pub fn drain_commands(slot: i32) -> Vec<WorkerCommand> {
    if slot < 0 || slot as usize >= MAX_RINGS {
        return Vec::new();
    }
    let mut rings = CMD_RINGS.exclusive();
    let mut out = Vec::new();
    for class in [RingClass::Admin, RingClass::Takeover] {
        let ring = &mut rings.class(class)[slot as usize];
        if ring.len == 0 {
            continue;
        }
        out.reserve(ring.len as usize);
        for i in 0..ring.len {
            let idx = (ring.head + i) % CMD_RING_CAPACITY as u32;
            if let Some(cmd) = decode(&ring.slots[idx as usize]) {
                out.push(cmd);
            }
        }
        ring.head = 0;
        ring.len = 0;
    }
    out
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
    pg_shmem_init!(CMD_RINGS);
    pg_shmem_init!(ENQUEUE_FLOORS);
}
