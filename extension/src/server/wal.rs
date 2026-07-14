//! WAL bookkeeping for asynchronous commits: LSN reading and the
//! synchronous flush beacon.

use pgrx::bgworkers::BackgroundWorker;
use pgrx::log;

/// Parse PostgreSQL's textual LSN ("16/B374D848") into a comparable u64.
fn parse_lsn(s: &str) -> Option<u64> {
    let (hi, lo) = s.split_once('/')?;
    Some((u64::from_str_radix(hi, 16).ok()? << 32) | u64::from_str_radix(lo, 16).ok()?)
}

/// Evaluate a WAL LSN expression in a read-only transaction (no WAL write,
/// no flush wait) and return it as a comparable u64.
pub(super) fn read_lsn(expr: &str) -> Option<u64> {
    let query = format!("SELECT {}::text", expr);
    BackgroundWorker::transaction(|| {
        pgrx::spi::Spi::connect(|client| {
            client.select(&query, None, &[])?.first().get_one::<String>()
        })
        .ok()
        .flatten()
    })
    .and_then(|s| parse_lsn(&s))
}

/// Force a WAL flush with one small synchronous commit; returns whether it
/// succeeded — callers gating client-visible effects on durability must not
/// treat a failed flush as flushed.
/// `pg_logical_emit_message` decodes to nothing (the output plugin registers
/// no message callback), so it cannot feed back into the CDC pipeline.
pub(super) fn force_flush() -> bool {
    BackgroundWorker::transaction(|| {
        pgrx::spi::Spi::run("SELECT pg_logical_emit_message(true, 'pgmqtt_flush', '')").is_ok()
    })
}

/// Watermark meaning "durability unconfirmed": both the LSN read and the
/// fallback flush failed, so the batch may only be released after a later
/// *successful* flush (which covers all earlier commits). See
/// `DeferredQueue::release_due`.
pub(super) const WATERMARK_UNCONFIRMED: u64 = u64::MAX;

/// WAL position that covers everything committed so far — read *after* an
/// asynchronous commit, it bounds that transaction's commit record. If the
/// read fails (should not happen), force a synchronous flush on the spot:
/// success makes the batch releasable immediately (watermark 0, durability
/// already paid); failure returns [`WATERMARK_UNCONFIRMED`] so release
/// waits for a flush that actually succeeded rather than acknowledging
/// QoS 1 publishes whose WAL may not be on disk.
pub(super) fn capture_insert_watermark() -> u64 {
    match read_lsn("pg_current_wal_insert_lsn()") {
        Some(lsn) => lsn,
        None => {
            log!("pgmqtt: failed to read WAL insert LSN — forcing a synchronous flush instead");
            if force_flush() {
                0
            } else {
                log!("pgmqtt: WAL flush fallback failed too — deferring batch until a flush succeeds");
                WATERMARK_UNCONFIRMED
            }
        }
    }
}
