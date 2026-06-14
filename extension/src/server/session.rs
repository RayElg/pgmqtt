//! Session state and management.
//!
//! Tracks MQTT sessions (queued messages, inflight messages, subscriptions, expiry).
//! All session state is lazily initialized and protected by a Mutex.

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};

/// An MQTT message ready to publish or queue.
#[derive(Debug, Clone)]
pub struct MqttMessage {
    pub id: Option<i64>,
    pub topic: Arc<str>,
    pub payload: Arc<[u8]>,
    pub qos: u8,
}

/// An MQTT session: tracks queued/inflight messages, subscriptions, and expiry.
#[derive(Clone)]
pub struct MqttSession {
    /// Next packet ID to assign (wraps at 65536).
    pub next_packet_id: u16,
    /// Outgoing packet_id → (topic, payload, msg_id, sent_at)
    pub inflight: HashMap<u16, (Arc<str>, Arc<[u8]>, Option<i64>, std::time::Instant)>,
    /// Messages waiting to be promoted into inflight once a slot opens up.
    pub queue: VecDeque<MqttMessage>,
    /// MQTT 5.0 Session Expiry Interval in seconds. 0 = end at disconnect.
    /// 0xFFFFFFFF means never expire.
    pub expiry_interval: u32,
    /// Set when the client disconnects (used by the session sweeper).
    pub disconnected_at: Option<std::time::Instant>,
    /// MQTT 5.0 Receive Maximum: max unacknowledged QoS 1/2 messages allowed.
    /// Default: 65535 per spec. Set on CONNECT.
    pub receive_maximum: u16,
    /// Sum of queued payload bytes; gated on `pgmqtt.max_queue_bytes_per_client`.
    pub queue_bytes: usize,
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
            queue_bytes: 0,
        }
    }

    pub fn queue_push_back(&mut self, msg: MqttMessage) {
        self.queue_bytes = self.queue_bytes.saturating_add(msg.payload.len());
        self.queue.push_back(msg);
    }

    pub fn queue_pop_front(&mut self) -> Option<MqttMessage> {
        let msg = self.queue.pop_front()?;
        self.queue_bytes = self.queue_bytes.saturating_sub(msg.payload.len());
        Some(msg)
    }

    pub fn queue_push_front(&mut self, msg: MqttMessage) {
        self.queue_bytes = self.queue_bytes.saturating_add(msg.payload.len());
        self.queue.push_front(msg);
    }

    /// MQTT-2.2.1 fixes packet_id at u16; linear-probe to skip occupied ids.
    /// None ⇒ all 65535 occupied (caller queues instead).
    pub fn alloc_packet_id(&mut self) -> Option<u16> {
        for _ in 0..u16::MAX as u32 {
            let pid = self.next_packet_id;
            self.next_packet_id = self.next_packet_id.wrapping_add(1);
            if self.next_packet_id == 0 {
                self.next_packet_id = 1;
            }
            if !self.inflight.contains_key(&pid) {
                return Some(pid);
            }
        }
        None
    }

    /// MQTT-3.3.4-7: cap per call at `receive_maximum`. Oldest sent_at first.
    pub fn select_redelivery_pids(
        &self,
        now: std::time::Instant,
        timeout: std::time::Duration,
    ) -> Vec<u16> {
        let mut entries: Vec<(std::time::Instant, u16)> = self
            .inflight
            .iter()
            .filter_map(|(pid, (_, _, _, sent_at))| {
                if now.duration_since(*sent_at) > timeout {
                    Some((*sent_at, *pid))
                } else {
                    None
                }
            })
            .collect();
        entries.sort_unstable_by_key(|(sent_at, _)| *sent_at);
        entries
            .into_iter()
            .take(self.receive_maximum as usize)
            .map(|(_, pid)| pid)
            .collect()
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
    let mut lock = SESSIONS.lock().unwrap_or_else(|e| e.into_inner());
    f(lock.get_or_insert_with(HashMap::new))
}
