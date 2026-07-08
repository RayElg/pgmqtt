//! Which process this is: worker slot, boot-time worker count, and the
//! listener / replication-origin setup that depends on them.

use pgrx::bgworkers::BackgroundWorker;
use pgrx::log;
use std::net::TcpListener;

// Process-local; set once by the worker entry points before their loops start,
// so deep call paths can consult the topology without threading it through
// every signature.
static WORKER_SLOT: std::sync::atomic::AtomicI32 = std::sync::atomic::AtomicI32::new(0);
static SOCKET_WORKERS: std::sync::atomic::AtomicI32 = std::sync::atomic::AtomicI32::new(1);

/// `slot` -1 marks a non-socket worker (the CDC worker): it never drains a
/// command ring or owns an outbox cursor, but its QoS 0 routing still
/// consults [`multi_worker`].
pub(crate) fn set(slot: i32, workers: i32) {
    WORKER_SLOT.store(slot, std::sync::atomic::Ordering::Relaxed);
    SOCKET_WORKERS.store(workers.max(1), std::sync::atomic::Ordering::Relaxed);
}

pub(crate) fn worker_slot() -> i32 {
    WORKER_SLOT.load(std::sync::atomic::Ordering::Relaxed)
}

pub(super) fn socket_workers() -> i32 {
    SOCKET_WORKERS.load(std::sync::atomic::Ordering::Relaxed)
}

/// `> 1` switches the cross-worker paths on: outbox cursors instead of
/// delete-on-delivery, client publishes routed through the outbox, GC-owned
/// orphan cleanup, takeover/admin broadcast rings.
pub(crate) fn multi_worker() -> bool {
    socket_workers() > 1
}

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
    match bind_listener(&addr) {
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

/// Bind with `SO_REUSEPORT` when several socket workers share the same ports
/// (the kernel then load-balances incoming connections across the workers'
/// accept queues); plain bind otherwise.
fn bind_listener(addr: &str) -> std::io::Result<TcpListener> {
    if !multi_worker() {
        return TcpListener::bind(addr);
    }
    let parsed: std::net::SocketAddr = addr
        .parse()
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, format!("{e}")))?;
    let std::net::SocketAddr::V4(v4) = parsed else {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "only IPv4 listen addresses are supported",
        ));
    };
    unsafe {
        let fd = libc::socket(libc::AF_INET, libc::SOCK_STREAM | libc::SOCK_CLOEXEC, 0);
        if fd < 0 {
            return Err(std::io::Error::last_os_error());
        }
        let close_err = |fd: i32| -> std::io::Error {
            let err = std::io::Error::last_os_error();
            libc::close(fd);
            err
        };
        let one: libc::c_int = 1;
        for opt in [libc::SO_REUSEADDR, libc::SO_REUSEPORT] {
            if libc::setsockopt(
                fd,
                libc::SOL_SOCKET,
                opt,
                &one as *const _ as *const libc::c_void,
                std::mem::size_of::<libc::c_int>() as libc::socklen_t,
            ) != 0
            {
                return Err(close_err(fd));
            }
        }
        let sin = libc::sockaddr_in {
            sin_family: libc::AF_INET as libc::sa_family_t,
            sin_port: v4.port().to_be(),
            sin_addr: libc::in_addr {
                s_addr: u32::from_be_bytes(v4.ip().octets()).to_be(),
            },
            sin_zero: [0; 8],
        };
        if libc::bind(
            fd,
            &sin as *const _ as *const libc::sockaddr,
            std::mem::size_of::<libc::sockaddr_in>() as libc::socklen_t,
        ) != 0
        {
            return Err(close_err(fd));
        }
        if libc::listen(fd, 128) != 0 {
            return Err(close_err(fd));
        }
        use std::os::unix::io::FromRawFd;
        Ok(TcpListener::from_raw_fd(fd))
    }
}

/// Tag every transaction this worker session commits with a named
/// replication origin, and register the origin's id in shared memory so the
/// output plugin can skip the worker's own WAL *before* it enters the
/// reorder buffer (`pg_decode_filter_by_origin` in lib.rs). User writes (no
/// origin) and foreign replication origins are unaffected.
///
/// Failure is logged and tolerated — the broker is fully correct without
/// origin tagging, just slower to decode under write load.
pub(crate) fn setup_replication_origin(name: &str) {
    let ident = BackgroundWorker::transaction(|| {
        super::with_subtransaction(|| {
            pgrx::spi::Spi::connect_mut(|client| {
                // Serialized with the partner worker's slot creation: a write
                // transaction in flight while the slot searches for its
                // decoding start point can wedge that worker through a fast
                // shutdown (see ensure_replication_slot in lib.rs).
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
