//! Session state and management.
//!
//! Tracks MQTT sessions (queued messages, inflight messages, subscriptions, expiry).
//! All session state is lazily initialized and protected by a Mutex.

use crate::topic_buffer;
use std::collections::{HashMap, VecDeque};
use std::sync::Mutex;

/// An MQTT session: tracks queued/inflight messages, subscriptions, and expiry.
#[derive(Clone)]
pub struct MqttSession {
    /// Next packet ID to assign (wraps at 65536).
    pub next_packet_id: u16,
    /// Outgoing packet_id → (topic, payload, msg_id, sent_at)
    pub inflight: HashMap<u16, (String, Vec<u8>, Option<i64>, std::time::Instant)>,
    /// Messages waiting to be promoted into inflight once a slot opens up.
    pub queue: VecDeque<topic_buffer::MqttMessage>,
    /// MQTT 5.0 Session Expiry Interval in seconds. 0 = end at disconnect.
    /// 0xFFFFFFFF means never expire.
    pub expiry_interval: u32,
    /// Set when the client disconnects (used by the session sweeper).
    pub disconnected_at: Option<std::time::Instant>,
    /// MQTT 5.0 Receive Maximum: max unacknowledged QoS 1/2 messages allowed.
    /// Default: 65535 per spec. Set on CONNECT.
    pub receive_maximum: u16,
}

impl MqttSession {
    /// Create a new empty session.
    pub fn new() -> Self {
        Self {
            next_packet_id: 1,
            inflight: HashMap::new(),
            queue: VecDeque::new(),
            expiry_interval: 0,
            disconnected_at: None,
            receive_maximum: 65535,
        }
    }
}

/// Global session store: client_id → MqttSession.
/// Lazily initialized on first use via with_sessions().
static SESSIONS: Mutex<Option<HashMap<String, MqttSession>>> = Mutex::new(None);

/// Apply a function to the global session map, returning the result.
/// Lazily initializes the map on first call.
pub fn with_sessions<F, R>(f: F) -> R
where
    F: FnOnce(&mut HashMap<String, MqttSession>) -> R,
{
    let mut lock = SESSIONS.lock().expect("sessions: poisoned mutex");
    if lock.is_none() {
        *lock = Some(HashMap::new());
    }
    f(lock.as_mut().unwrap())
}
