use std::collections::VecDeque;
use std::sync::Mutex;

/// Maximum number of events the ring buffer will hold before dropping the oldest.
const DEFAULT_CAPACITY: usize = 8192;

/// A single CDC event extracted by the output plugin.
#[derive(Debug, Clone)]
pub struct ChangeEvent {
    /// INSERT, UPDATE, or DELETE
    pub op: &'static str,
    pub schema: String,
    pub table: String,
    /// Column name → value (as text). Empty for DELETE w/o REPLICA IDENTITY.
    pub columns: Vec<(String, String)>,
}

/// Fixed-capacity ring buffer that drops the oldest entry on overflow.
struct RingBuffer {
    buf: VecDeque<ChangeEvent>,
    capacity: usize,
    dropped: u64,
}

impl RingBuffer {
    fn new(capacity: usize) -> Self {
        Self {
            buf: VecDeque::with_capacity(capacity),
            capacity,
            dropped: 0,
        }
    }

    fn push(&mut self, event: ChangeEvent) {
        if self.buf.len() >= self.capacity {
            self.buf.pop_front();
            self.dropped += 1;
            if self.dropped % 100 == 1 {
                pgrx::log!(
                    "WARNING: pgmqtt ring buffer overflow! Dropped {} CDC events so far.",
                    self.dropped
                );
            }
        }
        self.buf.push_back(event);
    }

    fn drain(&mut self) -> Vec<ChangeEvent> {
        self.buf.drain(..).collect()
    }

    fn peek_all(&self) -> Vec<ChangeEvent> {
        self.buf.iter().cloned().collect()
    }

    #[allow(dead_code)]
    fn dropped_count(&self) -> u64 {
        self.dropped
    }
}

static RING: Mutex<Option<RingBuffer>> = Mutex::new(None);

/// Lazily initialise the global ring buffer (idempotent).
fn ensure_init(guard: &mut Option<RingBuffer>) {
    if guard.is_none() {
        *guard = Some(RingBuffer::new(DEFAULT_CAPACITY));
    }
}

/// Push a change event into the global ring buffer.
pub fn push(event: ChangeEvent) {
    let mut lock = RING.lock().expect("ring_buffer: poisoned mutex");
    ensure_init(&mut lock);
    lock.as_mut().unwrap().push(event);
}

/// Drain all buffered events, returning them in FIFO order.
pub fn drain() -> Vec<ChangeEvent> {
    let mut lock = RING.lock().expect("ring_buffer: poisoned mutex");
    ensure_init(&mut lock);
    lock.as_mut().unwrap().drain()
}

/// Peek at all buffered events without removing them.
pub fn peek_all() -> Vec<ChangeEvent> {
    let mut lock = RING.lock().expect("ring_buffer: poisoned mutex");
    ensure_init(&mut lock);
    lock.as_ref().unwrap().peek_all()
}

/// Return the number of events that were dropped due to overflow.
#[allow(dead_code)]
pub fn dropped_count() -> u64 {
    let lock = RING.lock().expect("ring_buffer: poisoned mutex");
    lock.as_ref().map_or(0, |rb| rb.dropped_count())
}
