//! Readiness-based client polling (Linux epoll).
//!
//! The tick loop used to attempt a nonblocking `read()` on every connected
//! client every tick — O(connections) syscalls at 200 ticks/s regardless of
//! activity, which is what capped connection counts in practice. An epoll
//! set makes each tick's read pass O(active clients): `epoll_wait` with a
//! zero timeout reports exactly which sockets have bytes (or errors /
//! hangups — those also wake a read, which then surfaces them through the
//! normal error paths), and everything else is skipped. Tick pacing stays
//! on the BGW latch; epoll is consulted, never waited on.
//!
//! Correctness notes:
//!
//! - **Level-triggered** (the epoll default): unconsumed kernel bytes keep
//!   the fd in every subsequent ready set, so a partial drain can never
//!   strand data.
//! - **Carry-over rule:** a client that actually read bytes this tick, or
//!   stopped early on the per-tick buffer cap, is re-polled next tick even
//!   if its fd shows nothing new — TLS and WebSocket transports can hold
//!   decrypted/decoded bytes internally after the kernel buffer drains,
//!   and a cap-limited client still has backlog by definition.
//! - **Reconnect takeover:** inserting a new connection under an existing
//!   client_id swaps the transport (new fd). `sync` compares fds, not just
//!   ids, so the replacement is re-registered.
//! - **Fallback:** on non-Linux builds, or if any epoll call fails, the
//!   ready set is `None` and the caller polls every client exactly as
//!   before. Readiness is an optimization, never a correctness dependency.

use std::collections::{HashMap, HashSet};

pub(crate) struct ReadinessPoller {
    /// epoll instance fd; `None` = fallback mode (poll everything).
    epfd: Option<i32>,
    /// fd -> client_id for currently registered fds.
    by_fd: HashMap<i32, String>,
    /// client_id -> fd mirror of `by_fd`.
    by_id: HashMap<String, i32>,
    /// Clients whose registration failed — always polled.
    always: HashSet<String>,
    /// Clients to force into the next ready set (see carry-over rule).
    pub(crate) carry: HashSet<String>,
}

impl ReadinessPoller {
    pub(crate) fn new() -> Self {
        #[cfg(target_os = "linux")]
        let epfd = {
            let fd = unsafe { libc::epoll_create1(libc::EPOLL_CLOEXEC) };
            if fd < 0 {
                pgrx::log!(
                    "pgmqtt mqtt: epoll_create1 failed ({}) — falling back to full client scans",
                    std::io::Error::last_os_error()
                );
                None
            } else {
                Some(fd)
            }
        };
        #[cfg(not(target_os = "linux"))]
        let epfd = None;

        Self {
            epfd,
            by_fd: HashMap::new(),
            by_id: HashMap::new(),
            always: HashSet::new(),
            carry: HashSet::new(),
        }
    }

    /// Reconcile epoll registrations with the live client map: register new
    /// or replaced connections, forget departed ones. Departed fds are
    /// already closed (dropping the transport closes the socket), which
    /// removes them from the epoll interest list automatically.
    pub(crate) fn sync(&mut self, clients: &HashMap<String, super::MqttClient>) {
        let Some(epfd) = self.epfd else { return };

        // Drop departed clients every sync, unconditionally. Closed fd
        // numbers are reused by the very next accept, so a stale entry that
        // lingers even one tick can alias a live client — and the fd-keyed
        // map must only be cleared by an entry that still owns the fd, or
        // the cleanup itself would unmap the newcomer (leaving it
        // permanently invisible to readiness and therefore never read).
        let by_fd = &mut self.by_fd;
        self.by_id.retain(|id, fd| {
            let live = clients.contains_key(id);
            if !live {
                if by_fd.get(fd).is_some_and(|owner| owner == id) {
                    by_fd.remove(fd);
                }
            }
            live
        });
        self.always.retain(|id| clients.contains_key(id));

        for (id, client) in clients {
            let fd = client.transport.raw_fd();
            match self.by_id.get(id) {
                Some(&known) if known == fd => continue,
                Some(&stale) => {
                    // Same client_id, new connection (session takeover).
                    // The old fd closed with the old transport.
                    if self.by_fd.get(&stale).is_some_and(|owner| owner == id) {
                        self.by_fd.remove(&stale);
                    }
                    self.by_id.remove(id);
                }
                None => {}
            }
            let registered = self.register(epfd, fd);
            if registered {
                // If this fd number aliased a different (stale) id, evict
                // that id entirely — its socket is long closed.
                if let Some(prev) = self.by_fd.insert(fd, id.clone()) {
                    if prev != *id {
                        self.by_id.remove(&prev);
                    }
                }
                self.by_id.insert(id.clone(), fd);
                self.always.remove(id);
            } else {
                self.always.insert(id.clone());
            }
        }
    }

    #[cfg(target_os = "linux")]
    fn register(&self, epfd: i32, fd: i32) -> bool {
        let mut ev = libc::epoll_event {
            events: (libc::EPOLLIN | libc::EPOLLRDHUP) as u32,
            u64: fd as u64,
        };
        let mut rc = unsafe { libc::epoll_ctl(epfd, libc::EPOLL_CTL_ADD, fd, &mut ev) };
        if rc != 0 && std::io::Error::last_os_error().raw_os_error() == Some(libc::EEXIST) {
            // The fd number is somehow still in the interest set (e.g. a
            // duplicated descriptor kept the file description alive past
            // the old transport). Take it over.
            rc = unsafe { libc::epoll_ctl(epfd, libc::EPOLL_CTL_MOD, fd, &mut ev) };
        }
        if rc != 0 {
            pgrx::log!(
                "pgmqtt mqtt: epoll_ctl(fd {}) failed ({}) — polling this client every tick",
                fd,
                std::io::Error::last_os_error()
            );
        }
        rc == 0
    }

    #[cfg(not(target_os = "linux"))]
    fn register(&self, _epfd: i32, _fd: i32) -> bool {
        false
    }

    /// The set of clients worth attempting a read on this tick, or `None`
    /// to poll everyone (fallback mode / epoll error).
    pub(crate) fn ready_set(&mut self) -> Option<HashSet<String>> {
        #[cfg(target_os = "linux")]
        {
            let epfd = self.epfd?;
            let mut ready = std::mem::take(&mut self.carry);
            ready.extend(self.always.iter().cloned());

            const MAX_EVENTS: usize = 256;
            loop {
                let mut events: [libc::epoll_event; MAX_EVENTS] =
                    unsafe { std::mem::zeroed() };
                let n = unsafe {
                    libc::epoll_wait(epfd, events.as_mut_ptr(), MAX_EVENTS as i32, 0)
                };
                if n < 0 {
                    let err = std::io::Error::last_os_error();
                    if err.kind() == std::io::ErrorKind::Interrupted {
                        continue;
                    }
                    pgrx::log!(
                        "pgmqtt mqtt: epoll_wait failed ({}) — polling all clients this tick",
                        err
                    );
                    return None;
                }
                for ev in events.iter().take(n as usize) {
                    if let Some(id) = self.by_fd.get(&(ev.u64 as i32)) {
                        ready.insert(id.clone());
                    }
                }
                if (n as usize) < MAX_EVENTS {
                    break;
                }
            }
            Some(ready)
        }
        #[cfg(not(target_os = "linux"))]
        {
            None
        }
    }
}

impl Drop for ReadinessPoller {
    fn drop(&mut self) {
        #[cfg(target_os = "linux")]
        if let Some(fd) = self.epfd {
            unsafe {
                libc::close(fd);
            }
        }
    }
}
