// Copyright (c) 2026 Edilson Pateguana
// Licensed under the Apache License, Version 2.0

//! AMQP operation counters — monotonic totals tracked via OTel.
//!
//! Every public `record_*` function increments a single OTel counter.
//! The Prometheus exporter renders them as `_total` suffixed metrics.

use std::sync::OnceLock;

use opentelemetry::KeyValue;
use opentelemetry::global;
use opentelemetry::metrics::Counter;

use super::METER_NAME;

// ─── Counter Singletons ──────────────────────────────

static MSGS_PUBLISHED: OnceLock<Counter<u64>> = OnceLock::new();
static MSGS_DELIVERED: OnceLock<Counter<u64>> = OnceLock::new();
static MSGS_ACKED: OnceLock<Counter<u64>> = OnceLock::new();
static MSGS_NACKED: OnceLock<Counter<u64>> = OnceLock::new();
static CONNS_OPENED: OnceLock<Counter<u64>> = OnceLock::new();
static CONNS_CLOSED: OnceLock<Counter<u64>> = OnceLock::new();
static CHANS_OPENED: OnceLock<Counter<u64>> = OnceLock::new();
static CHANS_CLOSED: OnceLock<Counter<u64>> = OnceLock::new();
static QUEUES_DECLARED: OnceLock<Counter<u64>> = OnceLock::new();
static QUEUES_CREATED: OnceLock<Counter<u64>> = OnceLock::new();
static QUEUES_DELETED: OnceLock<Counter<u64>> = OnceLock::new();
static SCHEMA_FAILURES: OnceLock<Counter<u64>> = OnceLock::new();
static SCHEMAS_REGISTERED: OnceLock<Counter<u64>> = OnceLock::new();
static SCHEMA_LOOKUPS: OnceLock<Counter<u64>> = OnceLock::new();
static SCHEMA_COMPAT_FAILURES: OnceLock<Counter<u64>> = OnceLock::new();

// ─── Lazy Accessors ──────────────────────────────────

fn msgs_published() -> &'static Counter<u64> {
    MSGS_PUBLISHED.get_or_init(|| {
        global::meter(METER_NAME)
            .u64_counter("amqp_messages_published")
            .with_description("Total messages published to queues")
            .build()
    })
}

fn msgs_delivered() -> &'static Counter<u64> {
    MSGS_DELIVERED.get_or_init(|| {
        global::meter(METER_NAME)
            .u64_counter("amqp_messages_delivered")
            .with_description("Total messages delivered to consumers")
            .build()
    })
}

fn msgs_acked() -> &'static Counter<u64> {
    MSGS_ACKED.get_or_init(|| {
        global::meter(METER_NAME)
            .u64_counter("amqp_messages_acked")
            .with_description("Total consumer acknowledgements")
            .build()
    })
}

fn msgs_nacked() -> &'static Counter<u64> {
    MSGS_NACKED.get_or_init(|| {
        global::meter(METER_NAME)
            .u64_counter("amqp_messages_nacked")
            .with_description("Total consumer negative acknowledgements")
            .build()
    })
}

fn conns_opened() -> &'static Counter<u64> {
    CONNS_OPENED.get_or_init(|| {
        global::meter(METER_NAME)
            .u64_counter("amqp_connections_opened")
            .with_description("Total AMQP connections accepted")
            .build()
    })
}

fn conns_closed() -> &'static Counter<u64> {
    CONNS_CLOSED.get_or_init(|| {
        global::meter(METER_NAME)
            .u64_counter("amqp_connections_closed")
            .with_description("Total AMQP connections closed")
            .build()
    })
}

fn chans_opened() -> &'static Counter<u64> {
    CHANS_OPENED.get_or_init(|| {
        global::meter(METER_NAME)
            .u64_counter("amqp_channels_opened")
            .with_description("Total AMQP channels opened")
            .build()
    })
}

fn chans_closed() -> &'static Counter<u64> {
    CHANS_CLOSED.get_or_init(|| {
        global::meter(METER_NAME)
            .u64_counter("amqp_channels_closed")
            .with_description("Total AMQP channels closed")
            .build()
    })
}

fn queues_declared() -> &'static Counter<u64> {
    QUEUES_DECLARED.get_or_init(|| {
        global::meter(METER_NAME)
            .u64_counter("amqp_queues_declared")
            .with_description("Total queue declarations")
            .build()
    })
}

fn queues_created() -> &'static Counter<u64> {
    QUEUES_CREATED.get_or_init(|| {
        global::meter(METER_NAME)
            .u64_counter("amqp_queues_created")
            .with_description("Total queues created")
            .build()
    })
}

fn queues_deleted() -> &'static Counter<u64> {
    QUEUES_DELETED.get_or_init(|| {
        global::meter(METER_NAME)
            .u64_counter("amqp_queues_deleted")
            .with_description("Total queues deleted")
            .build()
    })
}

fn schema_failures() -> &'static Counter<u64> {
    SCHEMA_FAILURES.get_or_init(|| {
        global::meter(METER_NAME)
            .u64_counter("amqp_schema_validation_failures")
            .with_description("Messages rejected by schema validation")
            .build()
    })
}

fn schemas_registered() -> &'static Counter<u64> {
    SCHEMAS_REGISTERED.get_or_init(|| {
        global::meter(METER_NAME)
            .u64_counter("schema_registry_registered")
            .with_description("Total schemas registered in the registry")
            .build()
    })
}

fn schema_lookups() -> &'static Counter<u64> {
    SCHEMA_LOOKUPS.get_or_init(|| {
        global::meter(METER_NAME)
            .u64_counter("schema_registry_lookups")
            .with_description("Total schema lookups by ID")
            .build()
    })
}

fn schema_compat_failures() -> &'static Counter<u64> {
    SCHEMA_COMPAT_FAILURES.get_or_init(|| {
        global::meter(METER_NAME)
            .u64_counter("schema_registry_compatibility_failures")
            .with_description("Schema registrations rejected by compatibility checks")
            .build()
    })
}

/// Force-initializes every counter so Prometheus shows them at zero.
pub fn register_all() {
    msgs_published();
    msgs_delivered();
    msgs_acked();
    msgs_nacked();
    conns_opened();
    conns_closed();
    chans_opened();
    chans_closed();
    queues_declared();
    queues_created();
    queues_deleted();
    schema_failures();
    schemas_registered();
    schema_lookups();
    schema_compat_failures();
}

// ─── Public Recording API ────────────────────────────

#[inline]
pub fn record_published(queue: &str) {
    msgs_published().add(1, &[KeyValue::new("queue", queue.to_string())]);
}

#[inline]
pub fn record_delivered(queue: &str) {
    msgs_delivered().add(1, &[KeyValue::new("queue", queue.to_string())]);
}

#[inline]
pub fn record_acked() {
    msgs_acked().add(1, &[]);
}

#[inline]
pub fn record_nacked() {
    msgs_nacked().add(1, &[]);
}

#[inline]
pub fn record_conn_opened() {
    conns_opened().add(1, &[]);
}

#[inline]
pub fn record_conn_closed() {
    conns_closed().add(1, &[]);
}

#[inline]
pub fn record_chan_opened() {
    chans_opened().add(1, &[]);
}

#[inline]
pub fn record_chan_closed() {
    chans_closed().add(1, &[]);
}

#[inline]
pub fn record_queue_declared() {
    queues_declared().add(1, &[]);
}

#[inline]
pub fn record_queue_created() {
    queues_created().add(1, &[]);
}

#[inline]
pub fn record_queue_deleted() {
    queues_deleted().add(1, &[]);
}

#[inline]
pub fn record_schema_validation_failed(queue: &str) {
    schema_failures().add(1, &[KeyValue::new("queue", queue.to_string())]);
}

#[inline]
pub fn record_schema_registered(subject: &str) {
    schemas_registered().add(1, &[KeyValue::new("subject", subject.to_string())]);
}

#[inline]
pub fn record_schema_lookup() {
    schema_lookups().add(1, &[]);
}

#[inline]
pub fn record_schema_compat_failure(subject: &str) {
    schema_compat_failures().add(1, &[KeyValue::new("subject", subject.to_string())]);
}

// ─── Counter Value Reader ────────────────────────────

/// Snapshot of all counter totals, read from the Prometheus registry.
pub struct CounterSnapshot {
    pub messages_published: u64,
    pub messages_delivered: u64,
    pub messages_acked: u64,
    pub messages_nacked: u64,
    pub connections_opened: u64,
    pub connections_closed: u64,
    pub channels_opened: u64,
    pub channels_closed: u64,
    pub queues_declared: u64,
    pub queues_created: u64,
    pub queues_deleted: u64,
    pub schema_validation_failures: u64,
}

/// Reads the current value of all AMQP counters from the Prometheus
/// registry. Returns zeros if the registry hasn't been initialized.
pub fn read_all() -> CounterSnapshot {
    let families = super::get_registry().gather();
    CounterSnapshot {
        messages_published: read_counter_value(&families, "amqp_messages_published"),
        messages_delivered: read_counter_value(&families, "amqp_messages_delivered"),
        messages_acked: read_counter_value(&families, "amqp_messages_acked"),
        messages_nacked: read_counter_value(&families, "amqp_messages_nacked"),
        connections_opened: read_counter_value(&families, "amqp_connections_opened"),
        connections_closed: read_counter_value(&families, "amqp_connections_closed"),
        channels_opened: read_counter_value(&families, "amqp_channels_opened"),
        channels_closed: read_counter_value(&families, "amqp_channels_closed"),
        queues_declared: read_counter_value(&families, "amqp_queues_declared"),
        queues_created: read_counter_value(&families, "amqp_queues_created"),
        queues_deleted: read_counter_value(&families, "amqp_queues_deleted"),
        schema_validation_failures: read_counter_value(
            &families,
            "amqp_schema_validation_failures",
        ),
    }
}

/// Looks up a counter metric family by name and sums all its samples.
fn read_counter_value(families: &[prometheus::proto::MetricFamily], name: &str) -> u64 {
    for fam in families {
        if fam.name() == name {
            return fam
                .get_metric()
                .iter()
                .map(|m| m.get_counter().value() as u64)
                .sum();
        }
    }
    0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn record_functions_do_not_panic() {
        let _p = crate::metrics::init_meter_provider();
        record_published("test-q");
        record_delivered("test-q");
        record_acked();
        record_nacked();
        record_conn_opened();
        record_conn_closed();
        record_chan_opened();
        record_chan_closed();
        record_queue_declared();
        record_queue_created();
        record_queue_deleted();
        record_schema_validation_failed("test-q");
    }
}
