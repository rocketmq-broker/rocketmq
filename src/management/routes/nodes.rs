// Copyright (c) 2026 Edilson Pateguana
// Licensed under the Apache License, Version 2.0
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::Json;

use crate::management::routes::helpers::*;
use crate::management::types::*;
use crate::state::Broker;

// ─── Overview & Nodes ──────────────────────────────────

/// Provides an overview of the broker status, object counts, and message rates.
pub async fn overview(State(broker): State<Broker>) -> Json<OverviewResponse> {
    let queue_count = broker.queues.len();
    let connection_count = broker.connections.len();
    let exchange_count = broker.exchanges.read().await.len();

    let mut total_messages = 0usize;
    let mut total_inflight = 0usize;
    let mut total_consumers = 0usize;
    let mut total_channels = 0usize;

    for entry in broker.queues.iter() {
        let q = entry.value();
        total_messages += q.messages.len();
        total_inflight += q.inflight.len();
        total_consumers += q.consumer_tags.len();
    }

    for entry in broker.conn_state.iter() {
        total_channels += entry.value().channels.len();
    }

    let version = env!("CARGO_PKG_VERSION").to_string();

    let total_all = total_messages + total_inflight;

    let (pub_val, pub_rate, del_val, del_rate, ack_val, ack_rate) = get_rates();

    let amqp_addr = crate::config::get_amqp_listen_addr();
    let amqps_addr = crate::config::get_amqps_listen_addr();
    let mgmt_addr = crate::config::get_mgmt_listen_addr();
    let node_id = crate::config::get_node_id();
    let node_name = format!("rocketmq-node-{}@localhost", node_id);

    let parse_addr = |addr: &str| -> (String, u16) {
        if let Some(pos) = addr.rfind(':') {
            let ip = addr[..pos].to_string();
            let port = addr[pos + 1..].parse::<u16>().unwrap_or(0);
            (ip, port)
        } else {
            (addr.to_string(), 0)
        }
    };

    let (amqp_ip, amqp_port) = parse_addr(&amqp_addr);
    let (amqps_ip, amqps_port) = parse_addr(&amqps_addr);
    let (mgmt_ip, mgmt_port) = parse_addr(&mgmt_addr);

    let listeners = vec![
        ListenerInfo {
            node: node_name.clone(),
            protocol: "amqp".into(),
            ip_address: amqp_ip,
            port: amqp_port,
            tls: false,
        },
        ListenerInfo {
            node: node_name.clone(),
            protocol: "amqp/ssl".into(),
            ip_address: amqps_ip,
            port: amqps_port,
            tls: true,
        },
        ListenerInfo {
            node: node_name.clone(),
            protocol: "http".into(),
            ip_address: mgmt_ip.clone(),
            port: mgmt_port,
            tls: false,
        },
    ];

    Json(OverviewResponse {
        cluster_name: "rocketmq@localhost".into(),
        node: node_name.clone(),
        rabbitmq_version: version.clone(),
        management_version: version.clone(),
        erlang_version: "rust/tokio".into(),
        erlang_full_version: format!("Rust {} / Tokio", version),
        product_name: "RocketMQ".into(),
        product_version: version,
        rates_mode: "basic".into(),
        object_totals: ObjectTotals {
            queues: queue_count,
            exchanges: exchange_count,
            connections: connection_count,
            channels: total_channels,
            consumers: total_consumers,
        },
        queue_totals: {
            record_samples(
                pub_val,
                del_val,
                ack_val,
                total_all as u64,
                total_messages as u64,
                total_inflight as u64,
            );
            QueueTotals {
                messages: total_all,
                messages_ready: total_messages,
                messages_unacknowledged: total_inflight,
                messages_details: Some(RateDetails::from_history(
                    pub_rate - del_rate,
                    "msg_total",
                    total_all as u64,
                )),
                messages_ready_details: Some(RateDetails::from_history(
                    pub_rate - del_rate,
                    "msg_ready",
                    total_messages as u64,
                )),
                messages_unacknowledged_details: Some(RateDetails::from_history(
                    del_rate - ack_rate,
                    "msg_unacked",
                    total_inflight as u64,
                )),
            }
        },
        listeners,
        exchange_types: vec![
            ExchangeTypeInfo {
                name: "direct".into(),
                description: "Direct exchange".into(),
                enabled: true,
            },
            ExchangeTypeInfo {
                name: "fanout".into(),
                description: "Fanout exchange".into(),
                enabled: true,
            },
            ExchangeTypeInfo {
                name: "topic".into(),
                description: "Topic exchange".into(),
                enabled: true,
            },
            ExchangeTypeInfo {
                name: "headers".into(),
                description: "Headers exchange".into(),
                enabled: true,
            },
        ],
        message_stats: MessageStats {
            publish: Some(pub_val),
            publish_details: Some(RateDetails::from_history(pub_rate, "publish", pub_val)),
            deliver_get: Some(del_val),
            deliver_get_details: Some(RateDetails::from_history(del_rate, "deliver", del_val)),
            ack: Some(ack_val),
            ack_details: Some(RateDetails::from_history(ack_rate, "ack", ack_val)),
            deliver: Some(del_val),
            deliver_details: Some(RateDetails::from_history(del_rate, "deliver", del_val)),
            confirm: None,
            confirm_details: None,
        },
        sample_retention_policies: serde_json::json!({
            "global": [60, 600, 3600, 28800, 86400],
            "basic":  [60, 600]
        }),
        disable_stats: false,
        enable_queue_totals: false,
        is_op_policy_updating_enabled: true,
        contexts: vec![serde_json::json!({
            "node": node_name,
            "description": "RocketMQ Management",
            "path": "/",
            "ip": mgmt_ip,
            "port": mgmt_port,
            "ssl": false
        })],
        churn_rates: get_churn_rates(),
        statistics_db_event_queue: 0,
    })
}

/// Lists all nodes in the cluster with memory, disk, and socket statistics.
pub async fn list_nodes(State(broker): State<Broker>) -> Json<Vec<NodeInfo>> {
    let connection_count = broker.connections.len();
    let local_node_id = crate::config::get_node_id();

    let apps = vec![serde_json::json!({
        "name": "rabbit",
        "version": env!("CARGO_PKG_VERSION"),
        "description": "RabbitMQ compatibility layer"
    })];

    let mut nodes = Vec::new();

    if let Some(cluster_mgr) = broker.cluster() {
        let members = cluster_mgr.members.read().await;
        for (&node_id, member) in members.iter() {
            let is_self = node_id == local_node_id;
            nodes.push(build_node_info(
                node_id,
                &broker,
                connection_count,
                is_self,
                member.is_active,
                member.last_seen,
                &apps,
            ));
        }
    }

    if nodes.is_empty() {
        nodes.push(build_node_info(
            local_node_id,
            &broker,
            connection_count,
            true,
            true,
            0,
            &apps,
        ));
    }

    nodes.sort_by(|a, b| a.name.cmp(&b.name));
    Json(nodes)
}

/// Constructs a NodeInfo for a single node.
/// When `is_self` is true, metrics come from the local process; otherwise, stubs.
fn build_node_info(
    node_id: u64,
    broker: &Broker,
    connection_count: usize,
    is_self: bool,
    running: bool,
    last_seen: u64,
    apps: &[serde_json::Value],
) -> NodeInfo {
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;

    let (mem_used, disk_free, fd_used, sockets_used, uptime, os_pid) = if is_self {
        (
            get_process_memory(),
            get_disk_free(),
            connection_count as u64 + 10,
            connection_count as u64,
            now_ms.saturating_sub(broker.start_time_ms()),
            std::process::id().to_string(),
        )
    } else {
        (
            125 * 1024 * 1024,
            120 * 1024 * 1024 * 1024,
            10,
            0,
            now_ms.saturating_sub(last_seen),
            "-".into(),
        )
    };

    NodeInfo {
        name: format!("rocketmq-node-{}@localhost", node_id),
        running,
        node_type: "disc".into(),
        mem_used,
        mem_limit: 4 * 1024 * 1024 * 1024,
        mem_alarm: false,
        disk_free,
        disk_free_limit: 50 * 1024 * 1024,
        disk_free_alarm: false,
        fd_used,
        fd_total: 65536,
        sockets_used,
        sockets_total: 65536,
        uptime,
        processors: num_cpus(),
        os_pid,
        applications: apps.to_vec(),
        proc_used: 10,
        proc_total: 1048576,
        rates_mode: "basic".into(),
        config_files: vec![],
        enabled_plugins: vec!["rabbitmq_management".to_string()],
        mem_calculation_strategy: "rss".into(),
        being_drained: false,
        db_dir: "data/mnesia".into(),
        log_files: vec!["data/log/rocketmq.log".into()],
        log_file: "data/log/rocketmq.log".into(),
        cluster_links: vec![],
        net_ticktime: 60,
        run_queue: 1,
        metrics_gc_queue_length: serde_json::json!({}),
        ra_open_file_metrics: serde_json::json!({}),
        exchange_types: vec![
            serde_json::json!({"name": "direct", "description": "Direct exchange", "enabled": true}),
            serde_json::json!({"name": "fanout", "description": "Fanout exchange", "enabled": true}),
            serde_json::json!({"name": "topic", "description": "Topic exchange", "enabled": true}),
            serde_json::json!({"name": "headers", "description": "Headers exchange", "enabled": true}),
        ],
        auth_mechanisms: vec![
            serde_json::json!({"name": "PLAIN", "description": "SASL PLAIN authentication", "enabled": true}),
            serde_json::json!({"name": "AMQPLAIN", "description": "AMQPLAIN authentication", "enabled": true}),
        ],
    }
}

pub async fn get_node(
    State(broker): State<Broker>,
    Path(name): Path<String>,
) -> Result<Json<NodeInfo>, StatusCode> {
    let Json(nodes) = list_nodes(State(broker)).await;
    match nodes.into_iter().find(|n| n.name == name) {
        Some(node) => Ok(Json(node)),
        None => Err(StatusCode::NOT_FOUND),
    }
}

pub async fn get_cluster_name() -> Json<serde_json::Value> {
    let node_name = format!("rocketmq-node-{}@localhost", crate::config::get_node_id());
    Json(serde_json::json!({ "name": node_name }))
}

pub async fn set_cluster_name(Json(_req): Json<ClusterNameRequest>) -> StatusCode {
    StatusCode::NO_CONTENT
}
