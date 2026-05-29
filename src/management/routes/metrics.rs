// Copyright (c) 2026 Edilson Pateguana
// Licensed under the Apache License, Version 2.0
use axum::extract::State;

use crate::management::routes::helpers::*;
use crate::state::Broker;

// ─── Prometheus Metricsexposition ───────────────────────

pub async fn prometheus_metrics(State(broker): State<Broker>) -> String {
    let s = crate::metrics::get_snapshot();
    let mut out = String::with_capacity(4096);

    write_counter(
        &mut out,
        "amqp_messages_published_total",
        "Total messages published",
        s.messages_published
            .load(std::sync::atomic::Ordering::Relaxed),
    );
    write_counter(
        &mut out,
        "amqp_messages_delivered_total",
        "Total messages delivered to consumers",
        s.messages_delivered
            .load(std::sync::atomic::Ordering::Relaxed),
    );
    write_counter(
        &mut out,
        "amqp_messages_acked_total",
        "Total consumer acknowledgements",
        s.messages_acked.load(std::sync::atomic::Ordering::Relaxed),
    );
    write_counter(
        &mut out,
        "amqp_messages_nacked_total",
        "Total consumer negative-acks",
        s.messages_nacked.load(std::sync::atomic::Ordering::Relaxed),
    );
    write_counter(
        &mut out,
        "amqp_schema_validation_failures_total",
        "Total messages rejected due to schema validation failure",
        s.schema_validation_failures
            .load(std::sync::atomic::Ordering::Relaxed),
    );
    write_counter(
        &mut out,
        "amqp_connections_opened_total",
        "Total connections accepted",
        s.connections_opened
            .load(std::sync::atomic::Ordering::Relaxed),
    );
    write_counter(
        &mut out,
        "amqp_connections_closed_total",
        "Total connections closed",
        s.connections_closed
            .load(std::sync::atomic::Ordering::Relaxed),
    );
    write_counter(
        &mut out,
        "amqp_channels_opened_total",
        "Total channels opened",
        s.channels_opened.load(std::sync::atomic::Ordering::Relaxed),
    );
    write_counter(
        &mut out,
        "amqp_channels_closed_total",
        "Total channels closed",
        s.channels_closed.load(std::sync::atomic::Ordering::Relaxed),
    );

    write_gauge(
        &mut out,
        "amqp_connections",
        "Current open connections",
        broker.connections.len() as u64,
    );
    write_gauge(
        &mut out,
        "amqp_queues",
        "Total queue count",
        broker.queues.len() as u64,
    );

    let exchange_count = broker.exchanges.read().await.len();
    write_gauge(
        &mut out,
        "amqp_exchanges",
        "Total exchange count",
        exchange_count as u64,
    );

    let mut total_messages = 0u64;
    let mut total_consumers = 0u64;
    out.push_str("# HELP amqp_queue_messages Messages ready in queue\n");
    out.push_str("# TYPE amqp_queue_messages gauge\n");
    for entry in broker.queues.iter() {
        let (name, q) = entry.pair();
        let msgs = q.messages.len() as u64;
        let consumers = q.consumer_tags.len() as u64;
        total_messages += msgs;
        total_consumers += consumers;
        out.push_str(&format!(
            "amqp_queue_messages{{queue=\"{}\"}} {}\n",
            name, msgs
        ));
    }
    out.push('\n');

    out.push_str("# HELP amqp_queue_consumers Consumers on queue\n");
    out.push_str("# TYPE amqp_queue_consumers gauge\n");
    for entry in broker.queues.iter() {
        let (name, q) = entry.pair();
        out.push_str(&format!(
            "amqp_queue_consumers{{queue=\"{}\"}} {}\n",
            name,
            q.consumer_tags.len()
        ));
    }
    out.push('\n');

    write_gauge(
        &mut out,
        "amqp_messages_total",
        "Total messages across all queues",
        total_messages,
    );
    write_gauge(
        &mut out,
        "amqp_consumers_total",
        "Total consumers across all queues",
        total_consumers,
    );

    out
}
