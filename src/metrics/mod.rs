//! OpenTelemetry metrics instrumentation.
//!
//! Uses the official OpenTelemetry Rust SDK (`opentelemetry` + `opentelemetry_sdk`)
//! following the pattern from https://opentelemetry.io/docs/languages/rust/getting-started/
//!
//! Two layers:
//! 1. **OTel Counters** — global singletons via `global::meter()`, exported via OTLP
//! 2. **Atomic snapshots** — readable counters for the `/api/metrics` HTTP endpoint

use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};

use opentelemetry::KeyValue;
use opentelemetry::global;
use opentelemetry::metrics::Counter;
use opentelemetry_sdk::metrics::SdkMeterProvider;

const METER_NAME: &str = "rocketmq";

// ─── Atomic Snapshot Counters (readable for HTTP) ────

/// Broker-wide metric snapshots — readable by the management API.
pub struct Snapshot {
    pub messages_published: AtomicU64,
    pub messages_delivered: AtomicU64,
    pub messages_acked: AtomicU64,
    pub messages_nacked: AtomicU64,
    pub connections_opened: AtomicU64,
    pub connections_closed: AtomicU64,
    pub channels_opened: AtomicU64,
    pub channels_closed: AtomicU64,
}

static SNAPSHOT: OnceLock<Snapshot> = OnceLock::new();

fn snapshot() -> &'static Snapshot {
    SNAPSHOT.get_or_init(|| Snapshot {
        messages_published: AtomicU64::new(0),
        messages_delivered: AtomicU64::new(0),
        messages_acked: AtomicU64::new(0),
        messages_nacked: AtomicU64::new(0),
        connections_opened: AtomicU64::new(0),
        connections_closed: AtomicU64::new(0),
        channels_opened: AtomicU64::new(0),
        channels_closed: AtomicU64::new(0),
    })
}

/// Read-only access to metric snapshots for the management HTTP API.
pub fn get_snapshot() -> &'static Snapshot {
    snapshot()
}

// ─── OTel Counter Singletons ─────────────────────────

static MESSAGES_PUBLISHED: OnceLock<Counter<u64>> = OnceLock::new();
static MESSAGES_DELIVERED: OnceLock<Counter<u64>> = OnceLock::new();
static MESSAGES_ACKED: OnceLock<Counter<u64>> = OnceLock::new();
static MESSAGES_NACKED: OnceLock<Counter<u64>> = OnceLock::new();
static CONNECTIONS_OPENED: OnceLock<Counter<u64>> = OnceLock::new();
static CONNECTIONS_CLOSED: OnceLock<Counter<u64>> = OnceLock::new();
static CHANNELS_OPENED: OnceLock<Counter<u64>> = OnceLock::new();
static CHANNELS_CLOSED: OnceLock<Counter<u64>> = OnceLock::new();

// ─── Provider Initialization ─────────────────────────

/// Initialize the OpenTelemetry meter provider.
/// Must be called once at startup before any metrics are recorded.
/// Returns the provider handle for graceful shutdown.
pub fn init_meter_provider() -> SdkMeterProvider {
    let provider = SdkMeterProvider::builder().build();
    global::set_meter_provider(provider.clone());
    provider
}

// ─── Counter Accessors ───────────────────────────────

fn otel_published() -> &'static Counter<u64> {
    MESSAGES_PUBLISHED.get_or_init(|| {
        global::meter(METER_NAME)
            .u64_counter("amqp.messages.published")
            .with_description("Total messages published to queues")
            .build()
    })
}

fn otel_delivered() -> &'static Counter<u64> {
    MESSAGES_DELIVERED.get_or_init(|| {
        global::meter(METER_NAME)
            .u64_counter("amqp.messages.delivered")
            .with_description("Total messages delivered to consumers")
            .build()
    })
}

fn otel_acked() -> &'static Counter<u64> {
    MESSAGES_ACKED.get_or_init(|| {
        global::meter(METER_NAME)
            .u64_counter("amqp.messages.acked")
            .with_description("Total consumer acknowledgements")
            .build()
    })
}

fn otel_nacked() -> &'static Counter<u64> {
    MESSAGES_NACKED.get_or_init(|| {
        global::meter(METER_NAME)
            .u64_counter("amqp.messages.nacked")
            .with_description("Total consumer negative acknowledgements")
            .build()
    })
}

fn otel_conn_opened() -> &'static Counter<u64> {
    CONNECTIONS_OPENED.get_or_init(|| {
        global::meter(METER_NAME)
            .u64_counter("amqp.connections.opened")
            .with_description("Total AMQP connections accepted")
            .build()
    })
}

fn otel_conn_closed() -> &'static Counter<u64> {
    CONNECTIONS_CLOSED.get_or_init(|| {
        global::meter(METER_NAME)
            .u64_counter("amqp.connections.closed")
            .with_description("Total AMQP connections closed")
            .build()
    })
}

fn otel_chan_opened() -> &'static Counter<u64> {
    CHANNELS_OPENED.get_or_init(|| {
        global::meter(METER_NAME)
            .u64_counter("amqp.channels.opened")
            .with_description("Total AMQP channels opened")
            .build()
    })
}

fn otel_chan_closed() -> &'static Counter<u64> {
    CHANNELS_CLOSED.get_or_init(|| {
        global::meter(METER_NAME)
            .u64_counter("amqp.channels.closed")
            .with_description("Total AMQP channels closed")
            .build()
    })
}

// ─── Public Recording API ────────────────────────────
// Each function: (1) records to OTel counter, (2) increments atomic snapshot.

/// Record a message published to a queue.
#[inline]
pub fn record_published(queue: &str) {
    otel_published().add(1, &[KeyValue::new("queue", queue.to_string())]);
    snapshot()
        .messages_published
        .fetch_add(1, Ordering::Relaxed);
}

/// Record a message delivered to a consumer.
#[inline]
pub fn record_delivered(queue: &str) {
    otel_delivered().add(1, &[KeyValue::new("queue", queue.to_string())]);
    snapshot()
        .messages_delivered
        .fetch_add(1, Ordering::Relaxed);
}

/// Record a consumer acknowledgement.
#[inline]
pub fn record_acked() {
    otel_acked().add(1, &[]);
    snapshot().messages_acked.fetch_add(1, Ordering::Relaxed);
}

/// Record a consumer negative-ack / reject.
#[inline]
pub fn record_nacked() {
    otel_nacked().add(1, &[]);
    snapshot().messages_nacked.fetch_add(1, Ordering::Relaxed);
}

/// Record a new connection opened.
#[inline]
pub fn record_conn_opened() {
    otel_conn_opened().add(1, &[]);
    snapshot()
        .connections_opened
        .fetch_add(1, Ordering::Relaxed);
}

/// Record a connection closed.
#[inline]
pub fn record_conn_closed() {
    otel_conn_closed().add(1, &[]);
    snapshot()
        .connections_closed
        .fetch_add(1, Ordering::Relaxed);
}

/// Record a channel opened.
#[inline]
pub fn record_chan_opened() {
    otel_chan_opened().add(1, &[]);
    snapshot().channels_opened.fetch_add(1, Ordering::Relaxed);
}

/// Record a channel closed.
#[inline]
pub fn record_chan_closed() {
    otel_chan_closed().add(1, &[]);
    snapshot().channels_closed.fetch_add(1, Ordering::Relaxed);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counter_and_snapshot_sync() {
        let _provider = init_meter_provider();
        record_published("test-q");
        record_published("test-q");
        record_delivered("test-q");
        record_acked();
        record_conn_opened();
        record_chan_opened();

        let s = get_snapshot();
        assert!(s.messages_published.load(Ordering::Relaxed) >= 2);
        assert!(s.messages_delivered.load(Ordering::Relaxed) >= 1);
        assert!(s.messages_acked.load(Ordering::Relaxed) >= 1);
        assert!(s.connections_opened.load(Ordering::Relaxed) >= 1);
        assert!(s.channels_opened.load(Ordering::Relaxed) >= 1);
    }
}
