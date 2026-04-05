//! Per-topic ring buffers for rendered MQTT messages.
//!
//! CDC events, after template rendering, are pushed here.
//! The MQTT server worker drains these buffers and delivers
//! PUBLISH packets to matching subscribers.

use std::collections::{HashMap, VecDeque};
use std::sync::Mutex;

/// Default per-topic capacity.
const DEFAULT_CAPACITY: usize = 4096;

/// An MQTT message ready to publish.
#[derive(Debug, Clone)]
pub struct MqttMessage {
    pub id: Option<i64>,
    pub topic: String,
    pub payload: Vec<u8>,
    pub qos: u8,
}

struct TopicBufferState {
    /// topic → bounded buffer of QoS 0 messages
    buffers_qos0: HashMap<String, VecDeque<MqttMessage>>,
    /// topic → unbounded buffer of QoS > 0 messages
    buffers_qos1: HashMap<String, VecDeque<MqttMessage>>,
    capacity: usize,
    /// Per-topic drop counters for overflow diagnostics.
    dropped: HashMap<String, u64>,
}

impl TopicBufferState {
    fn new(capacity: usize) -> Self {
        Self {
            buffers_qos0: HashMap::new(),
            buffers_qos1: HashMap::new(),
            capacity,
            dropped: HashMap::new(),
        }
    }

    fn push(&mut self, msg: MqttMessage) {
        if msg.qos == 0 {
            let topic_key = msg.topic.clone();
            let buf = self
                .buffers_qos0
                .entry(topic_key.clone())
                .or_insert_with(|| VecDeque::with_capacity(self.capacity));

            if buf.len() >= self.capacity {
                buf.pop_front();
                let count = self.dropped.entry(topic_key.clone()).or_insert(0);
                *count += 1;
                if *count % 100 == 1 {
                    pgrx::log!(
                        "WARNING: pgmqtt topic buffer overflow (QoS 0) for topic='{}'. Dropped {} messages for this topic.",
                        topic_key,
                        count
                    );
                }
            }
            buf.push_back(msg);
        } else {
            let topic_key = msg.topic.clone();
            let buf = self
                .buffers_qos1
                .entry(topic_key.clone())
                .or_insert_with(VecDeque::new);

            buf.push_back(msg);
            if buf.len() % 1000 == 0 {
                pgrx::log!(
                    "WARNING: pgmqtt topic buffer (QoS 1+) for topic='{}' is growing large (buf_qos1_len={}).",
                    topic_key,
                    buf.len()
                );
            }
        }
    }

    fn drain_all(&mut self) -> Vec<MqttMessage> {
        let mut out = Vec::new();
        // Drain and remove empty entries to prevent unbounded HashMap growth
        // from dynamic topics.
        self.buffers_qos0.retain(|_topic, buf| {
            out.extend(buf.drain(..));
            false // remove all entries; they'll be re-created on next push
        });
        self.buffers_qos1.retain(|_topic, buf| {
            out.extend(buf.drain(..));
            false
        });
        out
    }
}

static STATE: Mutex<Option<TopicBufferState>> = Mutex::new(None);

fn with_state<F, R>(f: F) -> R
where
    F: FnOnce(&mut TopicBufferState) -> R,
{
    let mut lock = STATE.lock().expect("topic_buffer: poisoned mutex");
    if lock.is_none() {
        *lock = Some(TopicBufferState::new(DEFAULT_CAPACITY));
    }
    f(lock.as_mut().unwrap())
}

/// Push a rendered message into the per-topic buffer.
pub fn push(msg: MqttMessage) {
    with_state(|state| state.push(msg));
}

/// Drain all pending messages across all topics.
pub fn drain_all() -> Vec<MqttMessage> {
    with_state(|state| state.drain_all())
}
