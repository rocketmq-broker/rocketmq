// Copyright (c) 2026 Edilson Pateguana
// Licensed under the Apache License, Version 2.0

//! Dynamic broker-state gauges.
//!
//! Unlike counters (monotonic), these reflect live broker state
//! and are polled on each Prometheus scrape via OTel observable gauges.
//! Requires a `Broker` handle to be set after startup.

use std::sync::OnceLock;

use opentelemetry::global;
use opentelemetry::metrics::ObservableGauge;

use crate::state::Broker;

use super::METER_NAME;

static BROKER_REF: OnceLock<Broker> = OnceLock::new();

/// Injects the broker handle so observable gauges can read live state.
/// Must be called once after `BrokerState` is constructed.
pub fn set_broker(broker: Broker) {
    let _ = BROKER_REF.set(broker);
}

/// Registers all broker-state observable gauges.
/// Safe to call before `set_broker`; callbacks will be no-ops
/// until the broker handle is available.
pub fn register_all(broker: Broker) {
    set_broker(broker);
    let meter = global::meter(METER_NAME);

    let _conns: ObservableGauge<u64> = meter
        .u64_observable_gauge("amqp_connections_active")
        .with_description("Current open AMQP connections")
        .with_callback(|gauge| {
            if let Some(b) = BROKER_REF.get() {
                gauge.observe(b.connections.len() as u64, &[]);
            }
        })
        .build();

    let _queues: ObservableGauge<u64> = meter
        .u64_observable_gauge("amqp_queues_active")
        .with_description("Current queue count")
        .with_callback(|gauge| {
            if let Some(b) = BROKER_REF.get() {
                gauge.observe(b.queues.len() as u64, &[]);
            }
        })
        .build();

    let _msgs: ObservableGauge<u64> = meter
        .u64_observable_gauge("amqp_messages_ready")
        .with_description("Total messages ready across all queues")
        .with_callback(|gauge| {
            if let Some(b) = BROKER_REF.get() {
                let total: u64 = b.queues.iter().map(|e| e.messages.len() as u64).sum();
                gauge.observe(total, &[]);
            }
        })
        .build();

    let _inflight: ObservableGauge<u64> = meter
        .u64_observable_gauge("amqp_messages_inflight")
        .with_description("Total unacknowledged messages across all queues")
        .with_callback(|gauge| {
            if let Some(b) = BROKER_REF.get() {
                let total: u64 = b.queues.iter().map(|e| e.inflight.len() as u64).sum();
                gauge.observe(total, &[]);
            }
        })
        .build();

    let _consumers: ObservableGauge<u64> = meter
        .u64_observable_gauge("amqp_consumers_active")
        .with_description("Total active consumers across all queues")
        .with_callback(|gauge| {
            if let Some(b) = BROKER_REF.get() {
                let total: u64 = b.queues.iter().map(|e| e.consumer_tags.len() as u64).sum();
                gauge.observe(total, &[]);
            }
        })
        .build();
}
