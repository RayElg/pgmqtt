use crate::mqtt;
use pgrx::bgworkers::BackgroundWorker;
use pgrx::spi::Spi;

use std::collections::{HashMap, HashSet};
use std::sync::Mutex;

// ---------------------------------------------------------------------------
// Global state
// ---------------------------------------------------------------------------

struct SubState {
    /// topic_filter → client_id → granted_qos
    filter_to_clients: HashMap<String, HashMap<String, u8>>,
    /// client_id → set of topic_filters (reverse index for cleanup)
    client_to_filters: HashMap<String, HashSet<String>>,
}

impl SubState {
    fn new() -> Self {
        Self {
            filter_to_clients: HashMap::new(),
            client_to_filters: HashMap::new(),
        }
    }
}

static SUBS: Mutex<Option<SubState>> = Mutex::new(None);

fn with_state<F, R>(f: F) -> R
where
    F: FnOnce(&mut SubState) -> R,
{
    let mut lock = SUBS.lock().expect("subscriptions: poisoned mutex");
    if lock.is_none() {
        *lock = Some(SubState::new());
    }
    f(lock.as_mut().unwrap())
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Maximum number of subscriptions allowed per client.
const MAX_SUBSCRIPTIONS_PER_CLIENT: usize = 100;

/// Load subscriptions from the database into memory.
pub fn load_from_db() {
    BackgroundWorker::transaction(|| {
        let _ = Spi::connect(|client| {
            // Check if table exists
            let table_exists = client
                .select(
                    "SELECT to_regclass('pgmqtt_subscriptions')::text",
                    None,
                    &[],
                )?
                .first()
                .get_one::<String>()?
                .is_some();

            if !table_exists {
                return Ok::<_, pgrx::spi::Error>(());
            }

            if let Ok(table) = client.select(
                "SELECT client_id, topic_filter, qos FROM pgmqtt_subscriptions",
                None,
                &[],
            ) {
                with_state(|state| {
                    for row in table {
                        if let (Ok(Some(client_id)), Ok(Some(filter)), Ok(Some(qos))) = (
                            row.get_by_name::<String, _>("client_id"),
                            row.get_by_name::<String, _>("topic_filter"),
                            row.get_by_name::<i32, _>("qos"),
                        ) {
                            state
                                .client_to_filters
                                .entry(client_id.clone())
                                .or_default()
                                .insert(filter.clone());
                            state
                                .filter_to_clients
                                .entry(filter)
                                .or_default()
                                .insert(client_id, qos as u8);
                        }
                    }
                });
            }
            Ok::<_, pgrx::spi::Error>(())
        });
    });
}

/// Add a subscription for a client. Returns the granted QoS.
pub fn subscribe(client_id: &str, topic_filter: &str, requested_qos: u8) -> u8 {
    let granted_qos = if requested_qos > 1 { 1 } else { requested_qos };

    // Persist to database
    let _ = BackgroundWorker::transaction(|| {
        Spi::connect_mut(|client| {
            let args: Vec<pgrx::datum::DatumWithOid> = vec![
                client_id.into(),
                topic_filter.into(),
                (granted_qos as i32).into(),
            ];
            client.update(
                "INSERT INTO pgmqtt_subscriptions (client_id, topic_filter, qos) \
                 VALUES ($1, $2, $3) \
                 ON CONFLICT (client_id, topic_filter) DO UPDATE \
                 SET qos = EXCLUDED.qos",
                None,
                &args,
            )?;
            Ok::<_, pgrx::spi::Error>(())
        })
    });

    with_state(|state| {
        let filters = state
            .client_to_filters
            .entry(client_id.to_string())
            .or_default();

        if filters.len() >= MAX_SUBSCRIPTIONS_PER_CLIENT && !filters.contains(topic_filter) {
            pgrx::log!(
                "pgmqtt: client '{}' hit max subscriptions ({})",
                client_id,
                MAX_SUBSCRIPTIONS_PER_CLIENT
            );
            return crate::mqtt::reason::UNSPECIFIED_ERROR;
        }

        filters.insert(topic_filter.to_string());

        state
            .filter_to_clients
            .entry(topic_filter.to_string())
            .or_default()
            .insert(client_id.to_string(), granted_qos);

        granted_qos
    })
}

/// Remove a single subscription.
pub fn unsubscribe(client_id: &str, topic_filter: &str) -> bool {
    // Delete from database
    let _ = BackgroundWorker::transaction(|| {
        Spi::connect_mut(|client| {
            let args: Vec<pgrx::datum::DatumWithOid> = vec![client_id.into(), topic_filter.into()];
            client.update(
                "DELETE FROM pgmqtt_subscriptions WHERE client_id = $1 AND topic_filter = $2",
                None,
                &args,
            )?;
            Ok::<_, pgrx::spi::Error>(())
        })
    });

    with_state(|state| {
        let mut removed = false;
        if let Some(clients) = state.filter_to_clients.get_mut(topic_filter) {
            removed = clients.remove(client_id).is_some();
            if clients.is_empty() {
                state.filter_to_clients.remove(topic_filter);
            }
        }
        if let Some(filters) = state.client_to_filters.get_mut(client_id) {
            filters.remove(topic_filter);
            if filters.is_empty() {
                state.client_to_filters.remove(client_id);
            }
        }
        removed
    })
}

/// Remove all subscriptions for a client (on disconnect).
pub fn remove_client(client_id: &str) {
    with_state(|state| {
        if let Some(filters) = state.client_to_filters.remove(client_id) {
            for filter in filters {
                if let Some(clients) = state.filter_to_clients.get_mut(&filter) {
                    clients.remove(client_id);
                    if clients.is_empty() {
                        state.filter_to_clients.remove(&filter);
                    }
                }
            }
        }
    })
}

/// Find all client IDs whose topic filters match a concrete topic, with their granted QoS.
pub fn match_topic(topic: &str) -> Vec<(String, u8)> {
    with_state(|state| {
        let mut matched = HashMap::new();
        for (filter, clients) in &state.filter_to_clients {
            if mqtt::topic_matches_filter(topic, filter) {
                for (client, qos) in clients {
                    let entry = matched.entry(client.clone()).or_insert(*qos);
                    if *qos > *entry {
                        *entry = *qos;
                    }
                }
            }
        }
        matched.into_iter().collect()
    })
}

/// Helper for diagnostics: return all active topic filters.
pub fn active_filters() -> Vec<String> {
    with_state(|state| state.filter_to_clients.keys().cloned().collect())
}
