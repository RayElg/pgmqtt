//! Listener binding and the replication-origin setup shared by the
//! `pgmqtt_mqtt` and `pgmqtt_cdc` workers.

use pgrx::bgworkers::BackgroundWorker;
use pgrx::log;
use std::net::TcpListener;

/// Bind one optional listener, logging the outcome. `Err` means the port was
/// enabled but could not be bound — the caller aborts the worker.
pub(super) fn bind(
    enabled: bool,
    port: u16,
    label: &str,
    kind: &str,
    desc: &str,
) -> Result<Option<TcpListener>, ()> {
    if !enabled {
        log!("{}: {} listener disabled", label, kind);
        return Ok(None);
    }
    let addr = format!("0.0.0.0:{}", port);
    match TcpListener::bind(&addr) {
        Ok(l) => {
            if let Err(e) = l.set_nonblocking(true) {
                log!("{}: failed to set non-blocking: {}", label, e);
                return Err(());
            }
            log!("{}: listening on {} ({})", label, addr, desc);
            Ok(Some(l))
        }
        Err(e) => {
            log!("{}: failed to bind {}: {}", label, addr, e);
            Err(())
        }
    }
}

/// Tag this session's commits with a named replication origin and register
/// its id in shared memory, so the output plugin skips the worker's own
/// WAL pre-reorder-buffer. Failure is tolerated — correct without it,
/// just slower to decode.
pub(crate) fn setup_replication_origin(name: &str) {
    let ident = BackgroundWorker::transaction(|| {
        super::with_subtransaction(|| {
            pgrx::spi::Spi::connect_mut(|client| {
                // Advisory-locked against the partner worker's slot
                // creation (see ensure_replication_slot in lib.rs).
                let _ = client.select(
                    &format!(
                        "SELECT pg_advisory_xact_lock({})",
                        crate::SCHEMA_MIGRATION_LOCK_KEY
                    ),
                    None,
                    &[],
                );
                let args: Vec<pgrx::datum::DatumWithOid> = vec![name.into()];
                client.update(
                    "SELECT pg_replication_origin_create($1) \
                     WHERE NOT EXISTS (SELECT 1 FROM pg_replication_origin WHERE roname = $1)",
                    None,
                    &args,
                )?;
                // Exclusive to this session; every commit from here on
                // carries the origin id in WAL.
                client.update(
                    "SELECT pg_replication_origin_session_setup($1)",
                    None,
                    &args,
                )?;
                let args: Vec<pgrx::datum::DatumWithOid> = vec![name.into()];
                client
                    .select(
                        "SELECT roident::int FROM pg_replication_origin WHERE roname = $1",
                        None,
                        &args,
                    )?
                    .first()
                    .get_one::<i32>()
            })
        })
    });
    match ident {
        Ok(Some(id)) if id > 0 => {
            crate::metrics::register_worker_origin(id as u16);
            let _ = SESSION_ORIGIN_NAME.set(name.to_string());
            log!("pgmqtt: replication origin '{}' attached (id {})", name, id);
        }
        other => {
            log!(
                "pgmqtt: could not set up replication origin '{}' ({:?}) — \
                 own WAL will be decoded and discarded by name instead of filtered",
                name,
                other
            );
        }
    }
}

// The session origin tags every WAL record — including inbound-mapped
// rows in user target tables, which must stay decodable (a doubly-mapped
// table echoes MQTT-written rows back out through CDC). Inbound write
// batches detach it for their duration. Origin state is not
// transactional; the flag tracks reality across aborts.
static SESSION_ORIGIN_NAME: std::sync::OnceLock<String> = std::sync::OnceLock::new();
static SESSION_ORIGIN_SUSPENDED: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);

/// Detach the session origin so the caller's writes decode as ordinary user
/// writes. Must run inside the caller's transaction *before* its first WAL
/// write; stays detached until [`resume_session_origin`]. No-op when no
/// origin is attached (or it is already suspended).
pub(crate) fn suspend_session_origin(
    client: &mut pgrx::spi::SpiClient<'_>,
) -> Result<(), pgrx::spi::Error> {
    use std::sync::atomic::Ordering;
    if SESSION_ORIGIN_NAME.get().is_none() || SESSION_ORIGIN_SUSPENDED.load(Ordering::Relaxed) {
        return Ok(());
    }
    client.select("SELECT pg_replication_origin_session_reset()", None, &[])?;
    SESSION_ORIGIN_SUSPENDED.store(true, Ordering::Relaxed);
    Ok(())
}

/// Re-attach the session origin after a run of inbound writes. On failure
/// the flag stays set so the next call retries; until then this process's
/// own WAL is decoded and discarded by name instead of filtered — correct,
/// just slower.
pub(crate) fn resume_session_origin() {
    use std::sync::atomic::Ordering;
    if !SESSION_ORIGIN_SUSPENDED.load(Ordering::Relaxed) {
        return;
    }
    let Some(name) = SESSION_ORIGIN_NAME.get() else {
        return;
    };
    let ok = BackgroundWorker::transaction(|| {
        pgrx::spi::Spi::connect_mut(|client| {
            let args: Vec<pgrx::datum::DatumWithOid> = vec![name.as_str().into()];
            client
                .update("SELECT pg_replication_origin_session_setup($1)", None, &args)
                .map(|_| ())
        })
        .is_ok()
    });
    if ok {
        SESSION_ORIGIN_SUSPENDED.store(false, Ordering::Relaxed);
    } else {
        log!("pgmqtt: failed to re-attach replication origin '{}' — will retry", name);
    }
}
