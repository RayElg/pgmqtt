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

/// Force a WAL flush with one small synchronous commit.
/// `pg_logical_emit_message` decodes to nothing (the output plugin registers
/// no message callback), so it cannot feed back into the CDC pipeline.
pub(super) fn force_flush() {
    BackgroundWorker::transaction(|| {
        let _ = pgrx::spi::Spi::run("SELECT pg_logical_emit_message(true, 'pgmqtt_flush', '')");
    });
}

/// WAL position that covers everything committed so far — read *after* an
/// asynchronous commit, it bounds that transaction's commit record. If the
/// read fails (should not happen), force a synchronous flush on the spot and
/// return 0 so the batch is releasable immediately with durability already
/// guaranteed.
pub(super) fn capture_insert_watermark() -> u64 {
    match read_lsn("pg_current_wal_insert_lsn()") {
        Some(lsn) => lsn,
        None => {
            log!("pgmqtt: failed to read WAL insert LSN — forcing a synchronous flush instead");
            force_flush();
            0
        }
    }
}
