//! Readiness-based client polling (Linux epoll): the read pass is
//! O(active) instead of O(connections); epoll is consulted with a zero
//! timeout, never waited on. Correctness notes:
//!
//! - Level-triggered: unconsumed kernel bytes keep the fd in every ready
//!   set, so a partial drain can't strand data.
//! - Carry-over: a client that read bytes or hit the per-tick cap is
//!   re-polled next tick — TLS/WS transports buffer decrypted bytes
//!   internally.
//! - Takeover swaps the transport; `sync` compares fds, not just ids.
//! - Fallback: `None` (poll everyone) on non-Linux or any epoll failure —
//!   readiness is an optimization, never a correctness dependency.

use std::collections::{HashMap, HashSet};

pub(crate) struct ReadinessPoller {
    /// epoll instance fd; `None` = fallback mode (poll everything).
    epfd: Option<i32>,
    /// fd -> client_id for currently registered fds.
    by_fd: HashMap<i32, String>,
    /// client_id -> fd mirror of `by_fd`.
    by_id: HashMap<String, i32>,
    /// Fds whose registration failed — always polled. Rebuilt every sync.
    always: HashSet<i32>,
    /// Fds to force into the next ready set (see carry-over rule). Keyed by
    /// fd, not client_id: the per-event/per-chunk inserts and lookups stay
    /// integer-hashed and allocation-free on the hot path. A stale fd (its
    /// client departed, the number possibly reused) costs at most one
    /// spurious WouldBlock read next tick.
    pub(crate) carry: HashSet<i32>,
    /// Reusable `epoll_wait` output buffer, grown to the interest-set size.
    #[cfg(target_os = "linux")]
    events: Vec<libc::epoll_event>,
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
            #[cfg(target_os = "linux")]
            events: Vec::new(),
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
        // Repopulated below from this tick's registration failures, so
        // departed fds never linger to force-poll an unrelated newcomer.
        self.always.clear();

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
            } else {
                self.always.insert(fd);
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

    /// The set of fds worth attempting a read on this tick, or `None` to
    /// poll everyone (fallback mode / epoll error).
    pub(crate) fn ready_set(&mut self) -> Option<HashSet<i32>> {
        #[cfg(target_os = "linux")]
        {
            let epfd = self.epfd?;
            let mut ready = std::mem::take(&mut self.carry);
            ready.extend(self.always.iter().copied());

            // One wait, with the buffer sized to the interest set, reports
            // every currently-ready fd exactly once. Level-triggered epoll
            // re-reports the same fds on every call until their data is
            // consumed, so draining "until a short batch" with a small fixed
            // buffer never terminates once >= bufsize fds stay ready — a
            // remote-triggerable livelock. Anything the kernel would not fit
            // here (interest set raced larger between sync and wait) is
            // simply re-reported next tick.
            let want = self.by_fd.len().max(1);
            if self.events.len() < want {
                self.events.resize(want, unsafe { std::mem::zeroed() });
            }
            let n = loop {
                let n = unsafe {
                    libc::epoll_wait(
                        epfd,
                        self.events.as_mut_ptr(),
                        self.events.len().min(i32::MAX as usize) as i32,
                        0,
                    )
                };
                if n >= 0 {
                    break n;
                }
                let err = std::io::Error::last_os_error();
                if err.kind() == std::io::ErrorKind::Interrupted {
                    continue;
                }
                pgrx::log!(
                    "pgmqtt mqtt: epoll_wait failed ({}) — polling all clients this tick",
                    err
                );
                return None;
            };
            for ev in self.events.iter().take(n as usize) {
                ready.insert(ev.u64 as i32);
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
