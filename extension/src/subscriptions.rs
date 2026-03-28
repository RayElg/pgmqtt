//! In-memory subscription state: topic-filter → set of client IDs.
//!
//! Used by the MQTT server to know which connected clients should receive
//! a PUBLISH for a given concrete topic.
//!
//! Supports MQTT 5.0 shared subscriptions (`$share/{group}/{filter}`).
//! Within a shared group, each message is delivered to exactly one member
//! via round-robin, preferring connected clients.

use crate::mqtt;

use std::cell::Cell;
use std::collections::{HashMap, HashSet};
use std::sync::Mutex;

// ---------------------------------------------------------------------------
// Shared subscription group
// ---------------------------------------------------------------------------

struct SharedGroup {
    /// The real topic filter (without the `$share/{group}/` prefix).
    filter: String,
    /// Members: client_id → granted_qos.
    members: HashMap<String, u8>,
    /// Ordered member list for deterministic round-robin.
    member_order: Vec<String>,
    /// Round-robin counter (index into `member_order`).
    rr_index: Cell<usize>,
}

impl SharedGroup {
    fn new(filter: String) -> Self {
        Self {
            filter,
            members: HashMap::new(),
            member_order: Vec::new(),
            rr_index: Cell::new(0),
        }
    }

    fn add_member(&mut self, client_id: String, qos: u8) {
        if self.members.insert(client_id.clone(), qos).is_none() {
            self.member_order.push(client_id);
        }
    }

    fn remove_member(&mut self, client_id: &str) -> bool {
        if self.members.remove(client_id).is_some() {
            if let Some(pos) = self.member_order.iter().position(|c| c == client_id) {
                self.member_order.remove(pos);
                let len = self.member_order.len();
                if len == 0 {
                    self.rr_index.set(0);
                } else {
                    let idx = self.rr_index.get();
                    if pos < idx {
                        self.rr_index.set(idx - 1);
                    } else if idx >= len {
                        self.rr_index.set(0);
                    }
                }
            }
            true
        } else {
            false
        }
    }

    fn is_empty(&self) -> bool {
        self.members.is_empty()
    }

    /// Select the next member via round-robin, preferring connected clients.
    fn next_member<F>(&self, is_connected: F) -> Option<(String, u8)>
    where
        F: Fn(&str) -> bool,
    {
        if self.member_order.is_empty() {
            return None;
        }

        let len = self.member_order.len();
        let start = self.rr_index.get() % len;

        // First pass: try to find a connected client starting from rr_index.
        for i in 0..len {
            let idx = (start + i) % len;
            let cid = &self.member_order[idx];
            if is_connected(cid) {
                self.rr_index.set((idx + 1) % len);
                let qos = self.members[cid];
                return Some((cid.clone(), qos));
            }
        }

        // No connected member; fall back to any member (supports persistent sessions).
        let cid = &self.member_order[start];
        self.rr_index.set((start + 1) % len);
        let qos = self.members[cid];
        Some((cid.clone(), qos))
    }
}

// ---------------------------------------------------------------------------
// Global state
// ---------------------------------------------------------------------------

struct SubState {
    /// topic_filter → client_id → granted_qos (regular subscriptions)
    filter_to_clients: HashMap<String, HashMap<String, u8>>,
    /// client_id → set of topic_filters (reverse index for cleanup;
    /// stores the full wire string, including `$share/group/` prefix when applicable)
    client_to_filters: HashMap<String, HashSet<String>>,
    /// full wire filter (`$share/{group}/{filter}`) → SharedGroup
    shared_groups: HashMap<String, SharedGroup>,
}

impl SubState {
    fn new() -> Self {
        Self {
            filter_to_clients: HashMap::new(),
            client_to_filters: HashMap::new(),
            shared_groups: HashMap::new(),
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

/// Parse a topic filter that may be a shared subscription.
///
/// Returns `Some((group_name, real_filter))` for `$share/{group}/{filter}`,
/// or `None` for a regular subscription.
///
/// Per MQTT 5.0 §4.8.2, the group name must be at least 1 character and
/// must not contain `/`, `+`, or `#`.
pub fn parse_shared_filter(topic_filter: &str) -> Option<(&str, &str)> {
    let rest = topic_filter.strip_prefix("$share/")?;
    let slash_pos = rest.find('/')?;
    if slash_pos == 0 {
        return None; // empty group name
    }
    let group = &rest[..slash_pos];
    let filter = &rest[slash_pos + 1..];
    if filter.is_empty() {
        return None; // empty filter
    }
    if group.contains('+') || group.contains('#') || group.contains('/') {
        return None; // invalid characters in group name
    }
    Some((group, filter))
}

/// Add a subscription for a client. Returns the granted QoS.
pub fn subscribe(client_id: &str, topic_filter: &str, requested_qos: u8) -> u8 {
    let granted_qos = if requested_qos > 1 { 1 } else { requested_qos };

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

        if let Some((_, real_filter)) = parse_shared_filter(topic_filter) {
            let sg = state
                .shared_groups
                .entry(topic_filter.to_string())
                .or_insert_with(|| SharedGroup::new(real_filter.to_string()));
            sg.add_member(client_id.to_string(), granted_qos);
        } else {
            state
                .filter_to_clients
                .entry(topic_filter.to_string())
                .or_default()
                .insert(client_id.to_string(), granted_qos);
        }

        granted_qos
    })
}

/// Remove a single subscription.
pub fn unsubscribe(client_id: &str, topic_filter: &str) -> bool {
    with_state(|state| {
        let mut removed = false;

        if parse_shared_filter(topic_filter).is_some() {
            if let Some(sg) = state.shared_groups.get_mut(topic_filter) {
                removed = sg.remove_member(client_id);
                if sg.is_empty() {
                    state.shared_groups.remove(topic_filter);
                }
            }
        } else if let Some(clients) = state.filter_to_clients.get_mut(topic_filter) {
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
                if parse_shared_filter(&filter).is_some() {
                    if let Some(sg) = state.shared_groups.get_mut(filter.as_str()) {
                        sg.remove_member(client_id);
                        if sg.is_empty() {
                            state.shared_groups.remove(filter.as_str());
                        }
                    }
                } else if let Some(clients) = state.filter_to_clients.get_mut(&filter) {
                    clients.remove(client_id);
                    if clients.is_empty() {
                        state.filter_to_clients.remove(&filter);
                    }
                }
            }
        }
    })
}

/// Check whether any subscription (regular or shared) matches a concrete topic.
///
/// This does NOT advance shared-group round-robin state, so it is safe to call
/// from optimization-check paths that only need a yes/no answer.
pub fn has_subscribers(topic: &str) -> bool {
    with_state(|state| {
        for filter in state.filter_to_clients.keys() {
            if mqtt::topic_matches_filter(topic, filter) {
                return true;
            }
        }
        for sg in state.shared_groups.values() {
            if mqtt::topic_matches_filter(topic, &sg.filter) {
                return true;
            }
        }
        false
    })
}

/// Find all client IDs whose topic filters match a concrete topic, with their granted QoS.
///
/// For regular subscriptions: returns all matching clients (deduped, highest QoS wins).
/// For shared subscriptions: returns exactly one client per matching group (round-robin,
/// preferring connected clients).
///
/// `connected_clients` is the set of currently-connected client IDs, used to prefer
/// online members when selecting from a shared group.
pub fn match_topic(topic: &str, connected_clients: &HashSet<String>) -> Vec<(String, u8)> {
    with_state(|state| {
        let mut matched: HashMap<String, u8> = HashMap::new();

        // Regular subscriptions.
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

        // Shared subscriptions: one client per matching group.
        for sg in state.shared_groups.values() {
            if mqtt::topic_matches_filter(topic, &sg.filter) {
                if let Some((cid, qos)) = sg.next_member(|c| connected_clients.contains(c)) {
                    let entry = matched.entry(cid).or_insert(qos);
                    if qos > *entry {
                        *entry = qos;
                    }
                }
            }
        }

        matched.into_iter().collect()
    })
}

/// Helper for diagnostics: return all active topic filters.
pub fn active_filters() -> Vec<String> {
    with_state(|state| {
        let mut filters: Vec<String> = state.filter_to_clients.keys().cloned().collect();
        filters.extend(state.shared_groups.keys().cloned());
        filters
    })
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_shared_filter() {
        // Valid shared subscriptions.
        assert_eq!(
            parse_shared_filter("$share/group1/sensor/+/data"),
            Some(("group1", "sensor/+/data"))
        );
        assert_eq!(
            parse_shared_filter("$share/g/topic"),
            Some(("g", "topic"))
        );
        assert_eq!(
            parse_shared_filter("$share/mygroup/a/b/c"),
            Some(("mygroup", "a/b/c"))
        );
        assert_eq!(
            parse_shared_filter("$share/g/#"),
            Some(("g", "#"))
        );

        // Invalid: empty group name.
        assert_eq!(parse_shared_filter("$share//topic"), None);
        // Invalid: empty filter.
        assert_eq!(parse_shared_filter("$share/group/"), None);
        // Invalid: wildcards in group name.
        assert_eq!(parse_shared_filter("$share/a+b/topic"), None);
        assert_eq!(parse_shared_filter("$share/a#b/topic"), None);
        // Not a shared subscription.
        assert_eq!(parse_shared_filter("regular/topic"), None);
        assert_eq!(parse_shared_filter("$other/group/topic"), None);
        // Missing filter entirely.
        assert_eq!(parse_shared_filter("$share/group"), None);
    }

    #[test]
    fn test_shared_group_round_robin() {
        let mut sg = SharedGroup::new("test/+".to_string());
        sg.add_member("a".to_string(), 1);
        sg.add_member("b".to_string(), 1);
        sg.add_member("c".to_string(), 1);

        // All connected: round-robin through a, b, c.
        let all_connected = |_: &str| true;
        assert_eq!(sg.next_member(&all_connected), Some(("a".to_string(), 1)));
        assert_eq!(sg.next_member(&all_connected), Some(("b".to_string(), 1)));
        assert_eq!(sg.next_member(&all_connected), Some(("c".to_string(), 1)));
        assert_eq!(sg.next_member(&all_connected), Some(("a".to_string(), 1)));
    }

    #[test]
    fn test_shared_group_prefers_connected() {
        let mut sg = SharedGroup::new("test/+".to_string());
        sg.add_member("a".to_string(), 1);
        sg.add_member("b".to_string(), 0);
        sg.add_member("c".to_string(), 1);

        // Only b is connected.
        let only_b = |c: &str| c == "b";
        assert_eq!(sg.next_member(&only_b), Some(("b".to_string(), 0)));
        assert_eq!(sg.next_member(&only_b), Some(("b".to_string(), 0)));
    }

    #[test]
    fn test_shared_group_fallback_disconnected() {
        let mut sg = SharedGroup::new("test".to_string());
        sg.add_member("a".to_string(), 1);

        // No one connected: falls back to any member.
        let none_connected = |_: &str| false;
        assert_eq!(sg.next_member(&none_connected), Some(("a".to_string(), 1)));
    }

    #[test]
    fn test_shared_group_remove_adjusts_index() {
        let mut sg = SharedGroup::new("test".to_string());
        sg.add_member("a".to_string(), 1);
        sg.add_member("b".to_string(), 1);
        sg.add_member("c".to_string(), 1);

        let all = |_: &str| true;
        // Advance to b (rr_index = 1 after selecting a).
        assert_eq!(sg.next_member(&all), Some(("a".to_string(), 1)));
        // Remove a (pos 0 < rr_index 1). member_order = [b, c], rr_index decrements to 0.
        sg.remove_member("a");
        assert_eq!(sg.member_order, vec!["b", "c"]);
        // Should continue from the adjusted position (b, not skip to c).
        assert_eq!(sg.next_member(&all), Some(("b".to_string(), 1)));
    }
}
