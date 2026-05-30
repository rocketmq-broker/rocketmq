// Copyright (c) 2026 Edilson Pateguana
// Licensed under the Apache License, Version 2.0

//! OpenTelemetry-only metrics instrumentation.
//!
//! All broker telemetry flows through the OTel SDK. The Prometheus
//! exporter renders counters and gauges on `/api/metrics`.
//!
//! Module layout:
//! - `counters`      — AMQP operation counters (publish, deliver, ack …)
//! - `system`        — process-level gauges (CPU, memory, disk, FDs)
//! - `broker_gauges` — dynamic broker-state gauges (connections, queues)

pub mod broker_gauges;
pub mod counters;
pub mod system;

// Re-export counter recording functions for backward compatibility.
// Callers use `crate::metrics::record_published(…)` etc.
pub use counters::{
    record_acked, record_chan_closed, record_chan_opened, record_conn_closed, record_conn_opened,
    record_delivered, record_published, record_queue_created, record_queue_declared,
    record_queue_deleted, record_schema_compat_failure, record_schema_lookup,
    record_schema_registered, record_schema_validation_failed,
};

use std::sync::OnceLock;

use opentelemetry::global;
use opentelemetry_prometheus::exporter;
use opentelemetry_sdk::metrics::SdkMeterProvider;
use prometheus::Registry;

pub const METER_NAME: &str = "rocketmq";

static REGISTRY: OnceLock<Registry> = OnceLock::new();

/// Returns the shared Prometheus registry backing the OTel exporter.
///
/// ```ignore
/// let families = get_registry().gather();
/// ```
pub fn get_registry() -> &'static Registry {
    REGISTRY.get_or_init(Registry::new)
}

static PROVIDER: OnceLock<SdkMeterProvider> = OnceLock::new();

/// Bootstraps the OTel meter provider with a Prometheus reader.
/// Idempotent — safe to call multiple times (e.g., from tests).
/// The returned provider must be held alive for the process lifetime.
pub fn init_meter_provider() -> SdkMeterProvider {
    PROVIDER
        .get_or_init(|| {
            let registry = get_registry().clone();
            let prom_exporter = exporter().with_registry(registry).build().unwrap();
            let provider = SdkMeterProvider::builder()
                .with_reader(prom_exporter)
                .build();
            global::set_meter_provider(provider.clone());

            // Eagerly register all counter instruments so they appear
            // in the first `/api/metrics` scrape even at zero.
            counters::register_all();
            system::register_all();

            provider
        })
        .clone()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn provider_boots_without_panic() {
        let _p = init_meter_provider();
    }
}
