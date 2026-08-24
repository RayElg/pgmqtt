//! WAL bookkeeping for asynchronous commits.

use pgrx::bgworkers::BackgroundWorker;
use pgrx::log;

/// "16/B374D848" -> comparable u64.
fn parse_lsn(s: &str) -> Option<u64> {
    let (hi, lo) = s.split_once('/')?;
    Some((u64::from_str_radix(hi, 16).ok()? << 32) | u64::from_str_radix(lo, 16).ok()?)
}

/// Read-only: no WAL write, no flush wait.
pub(super) fn read_lsn(expr: &str) -> Option<u64> {
    let query = format!("SELECT {}::text", expr);
    BackgroundWorker::transaction(|| {
        pgrx::spi::Spi::connect(|client| {
            client
                .select(&query, None, &[])?
                .first()
                .get_one::<String>()
        })
        .ok()
        .flatten()
    })
    .and_then(|s| parse_lsn(&s))
}

/// Callers gating client-visible effects on durability must not treat a
/// `false` as flushed. `pg_logical_emit_message` decodes to nothing (no
/// message callback), so it cannot feed back into the CDC pipeline.
pub(super) fn force_flush() -> bool {
    BackgroundWorker::transaction(|| {
        pgrx::spi::Spi::run("SELECT pg_logical_emit_message(true, 'pgmqtt_flush', '')").is_ok()
    })
}

/// Durability unconfirmed: release only after a later *successful* flush,
/// which covers all earlier commits.
pub(super) const WATERMARK_UNCONFIRMED: u64 = u64::MAX;

/// Read *after* an async commit, this bounds that transaction's commit
/// record. On read failure, flush instead: success means already durable
/// (watermark 0); failure yields [`WATERMARK_UNCONFIRMED`] rather than
/// acknowledging QoS 1 publishes whose WAL may not be on disk.
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
