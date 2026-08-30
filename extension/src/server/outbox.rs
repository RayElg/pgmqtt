//! Durable handoff from the CDC worker through `pgmqtt_cdc_outbox`: ids
//! are queued in the transaction that persists the rows, so the handoff is
//! as durable as the messages themselves.

use super::{cdc_worker, MqttClient, MqttMessage, SessionDbAction};
use pgrx::bgworkers::BackgroundWorker;
use pgrx::log;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Must run inside the persisting transaction, so ids become visible to
/// the delivery fetch exactly when the message rows do.
pub(crate) fn enqueue(
    client: &mut pgrx::spi::SpiClient<'_>,
    ids: &[i64],
) -> Result<(), pgrx::spi::Error> {
    let args: Vec<pgrx::datum::DatumWithOid> = vec![ids.to_vec().into()];
    client
        .update(
            "INSERT INTO pgmqtt_cdc_outbox (id) SELECT unnest($1::bigint[]) \
             ON CONFLICT DO NOTHING",
            None,
            &args,
        )
        .map(|_| ())
}

/// `None` message = dangling id (LEFT JOIN NULLs); the caller still gets
/// the id so it can dequeue past it instead of re-scanning forever.
fn message_from_row(
    row: &pgrx::spi::SpiHeapTupleData,
) -> Result<(Option<i64>, Option<MqttMessage>), pgrx::spi::Error> {
    let Some(id) = row.get_by_name::<i64, _>("id")? else {
        return Ok((None, None));
    };
    let Some(topic) = row.get_by_name::<String, _>("topic")? else {
        return Ok((Some(id), None));
    };
    let payload: Vec<u8> = row.get_by_name("payload")?.unwrap_or_default();
    let qos: i32 = row.get_by_name("qos")?.unwrap_or(0);
    Ok((
        Some(id),
        Some(MqttMessage {
            id: Some(id),
            topic: Arc::from(topic.as_str()),
            payload: Arc::from(payload),
            qos: qos as u8,
        }),
    ))
}

fn fetch_batch() -> (Vec<i64>, Vec<MqttMessage>) {
    BackgroundWorker::transaction(|| {
        pgrx::spi::Spi::connect(|client| {
            let mut ids = Vec::new();
            let mut out = Vec::new();
            let query = format!(
                "SELECT o.id, m.topic, m.payload, m.qos \
                 FROM pgmqtt_cdc_outbox o \
                 LEFT JOIN pgmqtt_messages m ON m.id = o.id \
                 ORDER BY o.id \
                 LIMIT {}",
                cdc_worker::CDC_BATCH_SIZE
            );
            let table = client.select(&query, None, &[])?;
            for row in table {
                let (id, msg) = message_from_row(&row)?;
                if let Some(id) = id {
                    ids.push(id);
                }
                if let Some(msg) = msg {
                    out.push(msg);
                }
            }
            Ok::<_, pgrx::spi::Error>((ids, out))
        })
    })
    .unwrap_or_else(|e| {
        log!("pgmqtt: failed to fetch pending CDC outbox batch: {}", e);
        (Vec::new(), Vec::new())
    })
}

/// Per-tick outbox consumption, including the QoS 0 shared-memory ring.
pub(super) struct Drain {
    /// Starts true: the doorbell does not survive a postmaster crash.
    pending: bool,
    doorbell_seen: u64,
    last_safety_check: Instant,
}

impl Drain {
    pub(super) fn new() -> Self {
        Drain {
            pending: true,
            doorbell_seen: 0,
            last_safety_check: Instant::now(),
        }
    }

    pub(super) fn tick(
        &mut self,
        clients: &mut HashMap<String, MqttClient>,
        publishes: &mut Vec<super::PendingPublish>,
        session_db_actions: &mut Vec<SessionDbAction>,
    ) {
        // Wakeup hint only; the first-tick query and 30s re-check keep
        // correctness independent of it.
        let doorbell = crate::shmem_bridge::outbox_doorbell_seq();
        if self.last_safety_check.elapsed() >= Duration::from_secs(30) {
            self.last_safety_check = Instant::now();
            self.pending = true;
        }
        if self.pending || doorbell != self.doorbell_seen {
            self.doorbell_seen = doorbell;
            let (outbox_ids, outbox_messages) = fetch_batch();

            self.pending = outbox_ids.len() >= cdc_worker::CDC_BATCH_SIZE;
            // Must be queued BEFORE the cleanup actions: orphan-reclaim
            // refuses to delete a message whose outbox row still exists,
            // and both run in the one end-of-tick transaction.
            if !outbox_ids.is_empty() {
                session_db_actions.push(SessionDbAction::DrainCdcOutbox { ids: outbox_ids });
            }
            if !outbox_messages.is_empty() {
                super::deliver_messages(&outbox_messages, clients, publishes, session_db_actions);
                // Spilled QoS 0 gets no session_messages row, so nothing
                // else would reclaim it.
                for msg in &outbox_messages {
                    if msg.qos == 0 {
                        if let Some(message_id) = msg.id {
                            session_db_actions
                                .push(SessionDbAction::CleanupOrphanedMessage { message_id });
                        }
                    }
                }
            }
        }

        let bridged_inline = crate::shmem_bridge::drain_inline();
        if !bridged_inline.is_empty() {
            let inline_messages: Vec<MqttMessage> = bridged_inline
                .into_iter()
                .map(|(topic, payload)| MqttMessage {
                    id: None,
                    topic: Arc::from(topic.as_str()),
                    payload: Arc::from(payload),
                    qos: 0,
                })
                .collect();
            super::deliver_messages(&inline_messages, clients, publishes, session_db_actions);
        }
    }
}
