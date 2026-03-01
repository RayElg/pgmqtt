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
    dropped: u64,
}

impl TopicBufferState {
    fn new(capacity: usize) -> Self {
        Self {
            buffers_qos0: HashMap::new(),
            buffers_qos1: HashMap::new(),
            capacity,
            dropped: 0,
        }
    }

    fn push(&mut self, msg: MqttMessage) {
        if msg.qos == 0 {
            let buf = self
                .buffers_qos0
                .entry(msg.topic.clone())
                .or_insert_with(|| VecDeque::with_capacity(self.capacity));

            if buf.len() >= self.capacity {
                buf.pop_front();
                self.dropped += 1;
                if self.dropped % 100 == 1 {
                    pgrx::log!(
                        "WARNING: pgmqtt topic buffer overflow (QoS 0) for topic='{}'! Dropped {} messages so far.",
                        msg.topic,
                        self.dropped
                    );
                }
            }
            buf.push_back(msg.clone());
            pgrx::log!(
                "pgmqtt: queued QoS 0 message for topic='{}' (buf_qos0_len={})",
                msg.topic,
                buf.len()
            );
        } else {
            let buf = self
                .buffers_qos1
                .entry(msg.topic.clone())
                .or_insert_with(VecDeque::new);

            buf.push_back(msg.clone());
            if buf.len() % 1000 == 0 {
                pgrx::log!(
                    "WARNING: pgmqtt topic buffer (QoS 1+) for topic='{}' is growing large (buf_qos1_len={}).",
                    msg.topic,
                    buf.len()
                );
            } else {
                pgrx::log!(
                    "pgmqtt: queued QoS 1+ message for topic='{}' (buf_qos1_len={})",
                    msg.topic,
                    buf.len()
                );
            }
        }
    }

    fn drain_all(&mut self) -> Vec<MqttMessage> {
        let mut out = Vec::new();
        for (_topic, buf) in &mut self.buffers_qos0 {
            out.extend(buf.drain(..));
        }
        for (_topic, buf) in &mut self.buffers_qos1 {
            out.extend(buf.drain(..));
        }
        out
    }

    #[allow(dead_code)]
    fn dropped_count(&self) -> u64 {
        self.dropped
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

/// Number of messages dropped due to per-topic overflow.
#[allow(dead_code)]
pub fn dropped_count() -> u64 {
    with_state(|state| state.dropped_count())
}
