//! In-broker metrics store: atomic counters maintained by the background worker.
//!
//! These counters live in the background worker process and cannot be read
//! directly from SQL functions (which run in separate user backend processes).
//! The background worker periodically flushes a `MetricsSnapshot` to the
//! `pgmqtt_metrics_current` and `pgmqtt_metrics_snapshots` tables, which
//! `pgmqtt_metrics()` and related SQL functions then query.

use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::OnceLock;

/// All broker-level metric counters, held as lock-free atomics.
///
/// The background worker is single-threaded, so all writes use `Relaxed`
/// ordering — there is no concurrent writer to synchronize with.
pub struct BrokerMetrics {
    // --- Connections ---
    /// Cumulative MQTT clients accepted (CONNACK success sent).
    pub connections_accepted: AtomicU64,
    /// Cumulative connection attempts rejected (auth failure, license limit).
    pub connections_rejected: AtomicU64,
    /// Gauge: currently connected clients.
    pub connections_current: AtomicU64,
    /// Cumulative clients that sent a normal DISCONNECT packet.
    pub disconnections_clean: AtomicU64,
    /// Cumulative clients disconnected unexpectedly (keepalive, error, shutdown).
    pub disconnections_unclean: AtomicU64,
    /// Cumulative Will messages fired.
    pub wills_fired: AtomicU64,

    // --- Sessions ---
    /// Cumulative brand-new sessions created (no prior state).
    pub sessions_created: AtomicU64,
    /// Cumulative sessions resumed from persistent state.
    pub sessions_resumed: AtomicU64,
    /// Cumulative sessions reaped by the expiry sweeper.
    pub sessions_expired: AtomicU64,

    // --- Messages received from MQTT clients ---
    /// Cumulative PUBLISH packets received from clients.
    pub msgs_received: AtomicU64,
    pub msgs_received_qos0: AtomicU64,
    pub msgs_received_qos1: AtomicU64,
    /// Cumulative bytes received in PUBLISH payloads.
    pub bytes_received: AtomicU64,

    // --- Messages sent to MQTT clients ---
    /// Cumulative PUBLISH packets sent to subscribing clients.
    pub msgs_sent: AtomicU64,
    /// Cumulative bytes sent in PUBLISH payloads.
    pub bytes_sent: AtomicU64,
    /// Cumulative messages dropped (queue-full disconnect, auth drop).
    pub msgs_dropped: AtomicU64,

    // --- QoS handshakes ---
    pub pubacks_sent: AtomicU64,
    pub pubacks_received: AtomicU64,

    // --- Subscriptions ---
    pub subscribe_ops: AtomicU64,
    pub unsubscribe_ops: AtomicU64,

    // --- CDC / outbound pipeline ---
    pub cdc_events_processed: AtomicU64,
    pub cdc_msgs_published: AtomicU64,
    pub cdc_errors: AtomicU64,
    /// Most-recent CDC lag measurement in milliseconds (not cumulative).
    pub cdc_lag_ms_last: AtomicI64,

    // --- Inbound pipeline (MQTT → DB table writes) ---
    pub inbound_writes_ok: AtomicU64,
    pub inbound_writes_failed: AtomicU64,
    pub inbound_retries: AtomicU64,
    pub inbound_dead_letters: AtomicU64,

    // --- DB batch operations ---
    pub db_batches_committed: AtomicU64,
    pub db_errors: AtomicU64,

    // --- Lifecycle timestamps (unix seconds) ---
    pub started_at_unix: AtomicU64,
    pub last_reset_at_unix: AtomicU64,
}

impl BrokerMetrics {
    fn new() -> Self {
        let now = crate::license::now_secs() as u64;
        Self {
            connections_accepted:   AtomicU64::new(0),
            connections_rejected:   AtomicU64::new(0),
            connections_current:    AtomicU64::new(0),
            disconnections_clean:   AtomicU64::new(0),
            disconnections_unclean: AtomicU64::new(0),
            wills_fired:            AtomicU64::new(0),
            sessions_created:       AtomicU64::new(0),
            sessions_resumed:       AtomicU64::new(0),
            sessions_expired:       AtomicU64::new(0),
            msgs_received:          AtomicU64::new(0),
            msgs_received_qos0:     AtomicU64::new(0),
            msgs_received_qos1:     AtomicU64::new(0),
            bytes_received:         AtomicU64::new(0),
            msgs_sent:              AtomicU64::new(0),
            bytes_sent:             AtomicU64::new(0),
            msgs_dropped:           AtomicU64::new(0),
            pubacks_sent:           AtomicU64::new(0),
            pubacks_received:       AtomicU64::new(0),
            subscribe_ops:          AtomicU64::new(0),
            unsubscribe_ops:        AtomicU64::new(0),
            cdc_events_processed:   AtomicU64::new(0),
            cdc_msgs_published:     AtomicU64::new(0),
            cdc_errors:             AtomicU64::new(0),
            cdc_lag_ms_last:        AtomicI64::new(0),
            inbound_writes_ok:      AtomicU64::new(0),
            inbound_writes_failed:  AtomicU64::new(0),
            inbound_retries:        AtomicU64::new(0),
            inbound_dead_letters:   AtomicU64::new(0),
            db_batches_committed:   AtomicU64::new(0),
            db_errors:              AtomicU64::new(0),
            started_at_unix:        AtomicU64::new(now),
            last_reset_at_unix:     AtomicU64::new(now),
        }
    }
}

static METRICS: OnceLock<BrokerMetrics> = OnceLock::new();

/// Return the global broker metrics store (lazily initialized).
pub fn get() -> &'static BrokerMetrics {
    METRICS.get_or_init(BrokerMetrics::new)
}

/// Increment a counter by 1 using `Relaxed` ordering.
#[inline(always)]
pub fn inc(counter: &AtomicU64) {
    counter.fetch_add(1, Ordering::Relaxed);
}

/// Add `n` to a counter using `Relaxed` ordering.
#[inline(always)]
pub fn add(counter: &AtomicU64, n: u64) {
    if n > 0 {
        counter.fetch_add(n, Ordering::Relaxed);
    }
}

/// Decrement a counter by 1, saturating at 0.
#[inline(always)]
pub fn dec(counter: &AtomicU64) {
    counter.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| {
        Some(v.saturating_sub(1))
    }).ok();
}

// ---------------------------------------------------------------------------
// Snapshot
// ---------------------------------------------------------------------------

/// A plain-data point-in-time snapshot of all broker metrics.
///
/// Captured by the background worker and written to `pgmqtt_metrics_current`
/// and `pgmqtt_metrics_snapshots`.
#[derive(Default)]
pub struct MetricsSnapshot {
    pub captured_at_unix:        i64,
    pub started_at_unix:         i64,
    pub last_reset_at_unix:      i64,
    pub connections_accepted:    i64,
    pub connections_rejected:    i64,
    pub connections_current:     i64,
    pub disconnections_clean:    i64,
    pub disconnections_unclean:  i64,
    pub wills_fired:             i64,
    pub sessions_created:        i64,
    pub sessions_resumed:        i64,
    pub sessions_expired:        i64,
    pub msgs_received:           i64,
    pub msgs_received_qos0:      i64,
    pub msgs_received_qos1:      i64,
    pub bytes_received:          i64,
    pub msgs_sent:               i64,
    pub bytes_sent:              i64,
    pub msgs_dropped:            i64,
    pub pubacks_sent:            i64,
    pub pubacks_received:        i64,
    pub subscribe_ops:           i64,
    pub unsubscribe_ops:         i64,
    pub cdc_events_processed:    i64,
    pub cdc_msgs_published:      i64,
    pub cdc_errors:              i64,
    pub cdc_lag_ms_last:         i64,
    pub inbound_writes_ok:       i64,
    pub inbound_writes_failed:   i64,
    pub inbound_retries:         i64,
    pub inbound_dead_letters:    i64,
    pub db_batches_committed:    i64,
    pub db_errors:               i64,
}

impl MetricsSnapshot {
    /// Capture the current values of all atomics. Uses `Relaxed` ordering
    /// throughout — no cross-counter consistency guarantees, but that is
    /// acceptable for monitoring data.
    pub fn capture() -> Self {
        let m = get();
        Self {
            captured_at_unix:       crate::license::now_secs(),
            started_at_unix:        m.started_at_unix.load(Ordering::Relaxed) as i64,
            last_reset_at_unix:     m.last_reset_at_unix.load(Ordering::Relaxed) as i64,
            connections_accepted:   m.connections_accepted.load(Ordering::Relaxed) as i64,
            connections_rejected:   m.connections_rejected.load(Ordering::Relaxed) as i64,
            connections_current:    m.connections_current.load(Ordering::Relaxed) as i64,
            disconnections_clean:   m.disconnections_clean.load(Ordering::Relaxed) as i64,
            disconnections_unclean: m.disconnections_unclean.load(Ordering::Relaxed) as i64,
            wills_fired:            m.wills_fired.load(Ordering::Relaxed) as i64,
            sessions_created:       m.sessions_created.load(Ordering::Relaxed) as i64,
            sessions_resumed:       m.sessions_resumed.load(Ordering::Relaxed) as i64,
            sessions_expired:       m.sessions_expired.load(Ordering::Relaxed) as i64,
            msgs_received:          m.msgs_received.load(Ordering::Relaxed) as i64,
            msgs_received_qos0:     m.msgs_received_qos0.load(Ordering::Relaxed) as i64,
            msgs_received_qos1:     m.msgs_received_qos1.load(Ordering::Relaxed) as i64,
            bytes_received:         m.bytes_received.load(Ordering::Relaxed) as i64,
            msgs_sent:              m.msgs_sent.load(Ordering::Relaxed) as i64,
            bytes_sent:             m.bytes_sent.load(Ordering::Relaxed) as i64,
            msgs_dropped:           m.msgs_dropped.load(Ordering::Relaxed) as i64,
            pubacks_sent:           m.pubacks_sent.load(Ordering::Relaxed) as i64,
            pubacks_received:       m.pubacks_received.load(Ordering::Relaxed) as i64,
            subscribe_ops:          m.subscribe_ops.load(Ordering::Relaxed) as i64,
            unsubscribe_ops:        m.unsubscribe_ops.load(Ordering::Relaxed) as i64,
            cdc_events_processed:   m.cdc_events_processed.load(Ordering::Relaxed) as i64,
            cdc_msgs_published:     m.cdc_msgs_published.load(Ordering::Relaxed) as i64,
            cdc_errors:             m.cdc_errors.load(Ordering::Relaxed) as i64,
            cdc_lag_ms_last:        m.cdc_lag_ms_last.load(Ordering::Relaxed),
            inbound_writes_ok:      m.inbound_writes_ok.load(Ordering::Relaxed) as i64,
            inbound_writes_failed:  m.inbound_writes_failed.load(Ordering::Relaxed) as i64,
            inbound_retries:        m.inbound_retries.load(Ordering::Relaxed) as i64,
            inbound_dead_letters:   m.inbound_dead_letters.load(Ordering::Relaxed) as i64,
            db_batches_committed:   m.db_batches_committed.load(Ordering::Relaxed) as i64,
            db_errors:              m.db_errors.load(Ordering::Relaxed) as i64,
        }
    }

    /// Serialize snapshot to a compact JSON string for hook/NOTIFY payloads.
    /// All values are numbers, so no escaping is required.
    pub fn to_json(&self) -> String {
        format!(
            concat!(
                r#"{{"captured_at_unix":{captured_at},"started_at_unix":{started_at},"last_reset_at_unix":{last_reset},"#,
                r#""connections_accepted":{ca},"connections_rejected":{cr},"connections_current":{cc},"#,
                r#""disconnections_clean":{dc},"disconnections_unclean":{du},"wills_fired":{wf},"#,
                r#""sessions_created":{sc},"sessions_resumed":{sr},"sessions_expired":{se},"#,
                r#""msgs_received":{mr},"msgs_received_qos0":{mr0},"msgs_received_qos1":{mr1},"bytes_received":{br},"#,
                r#""msgs_sent":{ms},"bytes_sent":{bs},"msgs_dropped":{md},"#,
                r#""pubacks_sent":{ps},"pubacks_received":{pr},"#,
                r#""subscribe_ops":{so},"unsubscribe_ops":{uo},"#,
                r#""cdc_events_processed":{cep},"cdc_msgs_published":{cmp},"cdc_errors":{ce},"cdc_lag_ms_last":{cl},"#,
                r#""inbound_writes_ok":{iwo},"inbound_writes_failed":{iwf},"inbound_retries":{ir},"inbound_dead_letters":{idl},"#,
                r#""db_batches_committed":{dbc},"db_errors":{de}}}"#,
            ),
            captured_at = self.captured_at_unix,
            started_at  = self.started_at_unix,
            last_reset  = self.last_reset_at_unix,
            ca  = self.connections_accepted,
            cr  = self.connections_rejected,
            cc  = self.connections_current,
            dc  = self.disconnections_clean,
            du  = self.disconnections_unclean,
            wf  = self.wills_fired,
            sc  = self.sessions_created,
            sr  = self.sessions_resumed,
            se  = self.sessions_expired,
            mr  = self.msgs_received,
            mr0 = self.msgs_received_qos0,
            mr1 = self.msgs_received_qos1,
            br  = self.bytes_received,
            ms  = self.msgs_sent,
            bs  = self.bytes_sent,
            md  = self.msgs_dropped,
            ps  = self.pubacks_sent,
            pr  = self.pubacks_received,
            so  = self.subscribe_ops,
            uo  = self.unsubscribe_ops,
            cep = self.cdc_events_processed,
            cmp = self.cdc_msgs_published,
            ce  = self.cdc_errors,
            cl  = self.cdc_lag_ms_last,
            iwo = self.inbound_writes_ok,
            iwf = self.inbound_writes_failed,
            ir  = self.inbound_retries,
            idl = self.inbound_dead_letters,
            dbc = self.db_batches_committed,
            de  = self.db_errors,
        )
    }

    /// Format all metrics in Prometheus text exposition format.
    pub fn to_prometheus(&self) -> String {
        let mut out = String::with_capacity(4096);
        let lines: &[(&str, &str, i64, &str)] = &[
            ("pgmqtt_connections_accepted_total",   "counter", self.connections_accepted,   "Total MQTT connections accepted"),
            ("pgmqtt_connections_rejected_total",   "counter", self.connections_rejected,   "Total connection attempts rejected"),
            ("pgmqtt_connections_current",          "gauge",   self.connections_current,    "Currently active connections"),
            ("pgmqtt_disconnections_clean_total",   "counter", self.disconnections_clean,   "Clients that sent a normal DISCONNECT"),
            ("pgmqtt_disconnections_unclean_total", "counter", self.disconnections_unclean, "Clients disconnected unexpectedly"),
            ("pgmqtt_wills_fired_total",            "counter", self.wills_fired,            "Will messages fired"),
            ("pgmqtt_sessions_created_total",       "counter", self.sessions_created,       "New sessions created"),
            ("pgmqtt_sessions_resumed_total",       "counter", self.sessions_resumed,       "Sessions resumed from persistent state"),
            ("pgmqtt_sessions_expired_total",       "counter", self.sessions_expired,       "Sessions reaped by expiry sweeper"),
            ("pgmqtt_messages_received_total",      "counter", self.msgs_received,          "PUBLISH packets received from clients"),
            ("pgmqtt_messages_received_qos0_total", "counter", self.msgs_received_qos0,     "QoS 0 PUBLISH packets received"),
            ("pgmqtt_messages_received_qos1_total", "counter", self.msgs_received_qos1,     "QoS 1 PUBLISH packets received"),
            ("pgmqtt_bytes_received_total",         "counter", self.bytes_received,         "Bytes received in PUBLISH payloads"),
            ("pgmqtt_messages_sent_total",          "counter", self.msgs_sent,              "PUBLISH packets delivered to subscribers"),
            ("pgmqtt_bytes_sent_total",             "counter", self.bytes_sent,             "Bytes sent in PUBLISH payloads"),
            ("pgmqtt_messages_dropped_total",       "counter", self.msgs_dropped,           "Messages dropped (queue full or auth)"),
            ("pgmqtt_pubacks_sent_total",           "counter", self.pubacks_sent,           "PUBACK packets sent"),
            ("pgmqtt_pubacks_received_total",       "counter", self.pubacks_received,       "PUBACK packets received"),
            ("pgmqtt_subscribe_ops_total",          "counter", self.subscribe_ops,          "SUBSCRIBE operations"),
            ("pgmqtt_unsubscribe_ops_total",        "counter", self.unsubscribe_ops,        "UNSUBSCRIBE operations"),
            ("pgmqtt_cdc_events_processed_total",   "counter", self.cdc_events_processed,   "WAL events decoded from CDC slot"),
            ("pgmqtt_cdc_messages_published_total", "counter", self.cdc_msgs_published,     "Messages emitted from CDC pipeline"),
            ("pgmqtt_cdc_errors_total",             "counter", self.cdc_errors,             "CDC pipeline errors"),
            ("pgmqtt_cdc_lag_milliseconds",         "gauge",   self.cdc_lag_ms_last,        "Most recent CDC replication lag in ms"),
            ("pgmqtt_inbound_writes_ok_total",      "counter", self.inbound_writes_ok,      "Successful inbound MQTT-to-DB writes"),
            ("pgmqtt_inbound_writes_failed_total",  "counter", self.inbound_writes_failed,  "Failed inbound MQTT-to-DB writes"),
            ("pgmqtt_inbound_retries_total",        "counter", self.inbound_retries,        "Inbound write retries"),
            ("pgmqtt_inbound_dead_letters_total",   "counter", self.inbound_dead_letters,   "Messages moved to dead-letter table"),
            ("pgmqtt_db_batches_committed_total",   "counter", self.db_batches_committed,   "DB session action batches committed"),
            ("pgmqtt_db_errors_total",              "counter", self.db_errors,              "DB batch errors"),
            ("pgmqtt_broker_started_at_unix",       "gauge",   self.started_at_unix,        "Unix timestamp when the broker started"),
        ];

        for (name, kind, value, help) in lines {
            out.push_str("# HELP ");
            out.push_str(name);
            out.push(' ');
            out.push_str(help);
            out.push('\n');
            out.push_str("# TYPE ");
            out.push_str(name);
            out.push(' ');
            out.push_str(kind);
            out.push('\n');
            out.push_str(name);
            out.push(' ');
            out.push_str(&value.to_string());
            out.push('\n');
        }

        out
    }
}
