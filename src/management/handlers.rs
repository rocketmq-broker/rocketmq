use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::Json;
use std::sync::Arc;
use tracing::info;

use crate::management::types::*;
use crate::state::{Broker, BrokerState};

// ─── Health Checks ─────────────────────────────────────

pub async fn healthcheck() -> StatusCode {
    StatusCode::OK
}

pub async fn health_alarms() -> Json<HealthResponse> {
    Json(HealthResponse {
        status: "ok".into(),
    })
}

pub async fn health_port_listener(Path(port): Path<u16>) -> Json<HealthResponse> {
    let ok = matches!(port, 5672 | 5671 | 15672);
    Json(HealthResponse {
        status: if ok { "ok" } else { "failed" }.into(),
    })
}

// ─── Overview & Nodes ──────────────────────────────────

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

    let (pub_val, pub_rate, del_val, del_rate, ack_val, ack_rate) = get_rates();

    Json(OverviewResponse {
        cluster_name: "rocketmq@localhost".into(),
        node: "rocketmq@localhost".into(),
        rabbitmq_version: version.clone(),
        management_version: version.clone(),
        erlang_version: "rust/tokio".into(),
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
        queue_totals: QueueTotals {
            messages: total_messages + total_inflight,
            messages_ready: total_messages,
            messages_unacknowledged: total_inflight,
        },
        listeners: vec![ListenerInfo {
            node: "rocketmq@localhost".into(),
            protocol: "amqp".into(),
            ip_address: "0.0.0.0".into(),
            port: 5672,
        }],
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
            publish_details: Some(RateDetails { rate: pub_rate }),
            deliver_get: Some(del_val),
            deliver_get_details: Some(RateDetails { rate: del_rate }),
            ack: Some(ack_val),
            ack_details: Some(RateDetails { rate: ack_rate }),
        },
    })
}

pub async fn list_nodes(State(broker): State<Broker>) -> Json<Vec<NodeInfo>> {
    let connection_count = broker.connections.len();
    let start_time = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;
    let local_node_id = crate::config::get_node_id();

    let mut nodes = Vec::new();

    if let Some(cluster_mgr) = broker.cluster() {
        let members = cluster_mgr.members.read().await;
        for (&node_id, member) in members.iter() {
            let is_self = node_id == local_node_id;
            let name = format!("rocketmq-node-{}@localhost", node_id);

            nodes.push(NodeInfo {
                name,
                running: member.is_active,
                node_type: "disc".into(),
                mem_used: if is_self {
                    get_process_memory()
                } else {
                    125 * 1024 * 1024
                },
                mem_limit: 4 * 1024 * 1024 * 1024,
                mem_alarm: false,
                disk_free: if is_self {
                    get_disk_free()
                } else {
                    120 * 1024 * 1024 * 1024
                },
                disk_free_limit: 50 * 1024 * 1024,
                disk_free_alarm: false,
                fd_used: if is_self {
                    connection_count as u64 + 10
                } else {
                    10
                },
                fd_total: 65536,
                sockets_used: if is_self { connection_count as u64 } else { 0 },
                sockets_total: 65536,
                uptime: if is_self {
                    start_time.saturating_sub(broker.start_time_ms())
                } else {
                    start_time.saturating_sub(member.last_seen)
                },
                processors: num_cpus(),
                os_pid: if is_self {
                    std::process::id().to_string()
                } else {
                    "-".into()
                },
            });
        }
    }

    if nodes.is_empty() {
        nodes.push(NodeInfo {
            name: format!("rocketmq-node-{}@localhost", local_node_id),
            running: true,
            node_type: "disc".into(),
            mem_used: get_process_memory(),
            mem_limit: 4 * 1024 * 1024 * 1024,
            mem_alarm: false,
            disk_free: get_disk_free(),
            disk_free_limit: 50 * 1024 * 1024,
            disk_free_alarm: false,
            fd_used: connection_count as u64 + 10,
            fd_total: 65536,
            sockets_used: connection_count as u64,
            sockets_total: 65536,
            uptime: start_time.saturating_sub(broker.start_time_ms()),
            processors: num_cpus(),
            os_pid: std::process::id().to_string(),
        });
    }

    nodes.sort_by(|a, b| a.name.cmp(&b.name));

    Json(nodes)
}

pub async fn get_node(
    State(broker): State<Broker>,
    Path(_name): Path<String>,
) -> Json<Vec<NodeInfo>> {
    list_nodes(State(broker)).await
}

pub async fn get_cluster_name() -> Json<serde_json::Value> {
    Json(serde_json::json!({ "name": "rocketmq@localhost" }))
}

pub async fn set_cluster_name(Json(_req): Json<ClusterNameRequest>) -> StatusCode {
    StatusCode::NO_CONTENT
}

// ─── Virtual Hosts ─────────────────────────────────────

pub async fn list_vhosts(State(broker): State<Broker>) -> Json<Vec<VHostInfo>> {
    let mut total_messages = 0usize;
    let mut total_inflight = 0usize;
    for entry in broker.queues.iter() {
        let q = entry.value();
        total_messages += q.messages.len();
        total_inflight += q.inflight.len();
    }

    let vhosts = broker.list_vhosts();
    let list: Vec<VHostInfo> = vhosts
        .iter()
        .map(|name| VHostInfo {
            name: name.clone(),
            description: None,
            messages: total_messages + total_inflight,
            messages_ready: total_messages,
            messages_unacknowledged: total_inflight,
        })
        .collect();
    Json(list)
}

pub async fn get_vhost(
    State(broker): State<Broker>,
    Path(name): Path<String>,
) -> Result<Json<VHostInfo>, StatusCode> {
    if broker.vhosts.contains_key(&name) {
        let (msgs, inflight) = queue_totals_for_vhost(&broker);
        Ok(Json(VHostInfo {
            name,
            description: None,
            messages: msgs + inflight,
            messages_ready: msgs,
            messages_unacknowledged: inflight,
        }))
    } else {
        Err(StatusCode::NOT_FOUND)
    }
}

pub async fn create_vhost(State(broker): State<Broker>, Path(name): Path<String>) -> StatusCode {
    broker
        .vhosts
        .entry(name.clone())
        .or_insert_with(|| crate::state::vhost::VHost::new(name));
    StatusCode::NO_CONTENT
}

pub async fn delete_vhost(State(broker): State<Broker>, Path(name): Path<String>) -> StatusCode {
    if name == "/" {
        return StatusCode::FORBIDDEN;
    }
    if broker.vhosts.remove(&name).is_some() {
        StatusCode::NO_CONTENT
    } else {
        StatusCode::NOT_FOUND
    }
}

pub async fn vhost_permissions(
    State(broker): State<Broker>,
    Path(vhost): Path<String>,
) -> Json<Vec<PermissionInfo>> {
    let perms: Vec<PermissionInfo> = broker
        .auth
        .list_users()
        .into_iter()
        .flat_map(|(u, _)| {
            broker
                .auth
                .list_user_permissions(&u)
                .into_iter()
                .filter(|p| p.vhost == vhost)
                .map(|p| PermissionInfo {
                    user: p.username.clone(),
                    vhost: p.vhost.clone(),
                    configure: p.configure.clone(),
                    write: p.write.clone(),
                    read: p.read.clone(),
                })
                .collect::<Vec<_>>()
        })
        .collect();
    Json(perms)
}

pub async fn start_vhost() -> StatusCode {
    StatusCode::NO_CONTENT
}

// ─── Queues & Bindings ──────────────────────────────────

pub async fn list_queues(State(broker): State<Broker>) -> Json<Vec<QueueInfo>> {
    let queues: Vec<QueueInfo> = broker
        .queues
        .iter()
        .map(|entry| {
            let (name, q) = entry.pair();
            build_queue_info(name, q)
        })
        .collect();
    Json(queues)
}

pub async fn get_queue(
    State(broker): State<Broker>,
    Path(name): Path<String>,
) -> Result<Json<QueueInfo>, StatusCode> {
    match broker.queues.get(&name) {
        Some(entry) => Ok(Json(build_queue_info(&name, entry.value()))),
        None => Err(StatusCode::NOT_FOUND),
    }
}

pub fn build_queue_info(name: &str, q: &crate::queue::QueueState) -> QueueInfo {
    let (pub_val, pub_rate, del_val, del_rate, ack_val, ack_rate) = get_rates();
    QueueInfo {
        name: name.to_string(),
        vhost: "/".into(),
        queue_type: "classic".into(),
        durable: q.options.durable,
        exclusive: q.options.exclusive,
        auto_delete: q.options.auto_delete,
        messages: q.messages.len() + q.inflight.len(),
        messages_ready: q.messages.len(),
        messages_unacknowledged: q.inflight.len(),
        consumers: q.consumer_tags.len(),
        state: "running".into(),
        node: "rocketmq@localhost".into(),
        message_stats: MessageStats {
            publish: Some(pub_val),
            publish_details: Some(RateDetails { rate: pub_rate }),
            deliver_get: Some(del_val),
            deliver_get_details: Some(RateDetails { rate: del_rate }),
            ack: Some(ack_val),
            ack_details: Some(RateDetails { rate: ack_rate }),
        },
    }
}

pub async fn delete_queue(State(broker): State<Broker>, Path(name): Path<String>) -> StatusCode {
    if broker.queues.remove(&name).is_some() {
        info!(queue = name.as_str(), "queue deleted via management API");
        StatusCode::NO_CONTENT
    } else {
        StatusCode::NOT_FOUND
    }
}

pub async fn purge_queue(
    State(broker): State<Broker>,
    Path(name): Path<String>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    match broker.queues.get_mut(&name) {
        Some(mut entry) => {
            let count = entry.value_mut().messages.len();
            entry.value_mut().messages.clear();
            info!(
                queue = name.as_str(),
                purged = count,
                "queue purged via management API"
            );
            Ok(Json(serde_json::json!({ "messages_purged": count })))
        }
        None => Err(StatusCode::NOT_FOUND),
    }
}

pub async fn get_messages(
    State(broker): State<Broker>,
    Path(name): Path<String>,
    Json(req): Json<GetMessagesRequest>,
) -> Result<Json<Vec<MessagePayload>>, StatusCode> {
    match broker.queues.get_mut(&name) {
        Some(mut entry) => {
            let queue = entry.value_mut();
            let remaining = queue.messages.len();
            let count = req.count.min(remaining);
            let mut result = Vec::with_capacity(count);
            let requeue = req.ack_mode == "ack_requeue_true";

            for _ in 0..count {
                if let Some(q_msg) = queue.messages.pop_front()
                    && let Ok(msg) = q_msg.resolve(broker.wal().expect("WAL must be initialized"))
                {
                    result.push(MessagePayload {
                        payload: String::from_utf8_lossy(&msg.body).to_string(),
                        payload_bytes: msg.body.len(),
                        routing_key: String::new(),
                        exchange: String::new(),
                        message_count: queue.messages.len(),
                    });
                    if requeue {
                        let requeue_msg = crate::queue::message::QueueMessage::Full(msg);
                        queue.messages.push_back(requeue_msg);
                    }
                }
            }
            Ok(Json(result))
        }
        None => Err(StatusCode::NOT_FOUND),
    }
}

pub async fn create_queue_vhost(
    State(broker): State<Broker>,
    Path((_vhost, name)): Path<(String, String)>,
    Json(req): Json<CreateQueueRequest>,
) -> StatusCode {
    use crate::queue::{QueueOptions, QueueState};
    broker.queues.entry(name.clone()).or_insert_with(|| {
        info!(queue = name.as_str(), "queue created via management API");
        QueueState::with_options(QueueOptions {
            durable: req.durable,
            exclusive: req.exclusive,
            auto_delete: req.auto_delete,
            ..Default::default()
        })
    });
    StatusCode::NO_CONTENT
}

pub async fn queue_actions_vhost() -> StatusCode {
    StatusCode::NO_CONTENT
}

pub async fn list_queues_vhost(
    State(broker): State<Broker>,
    Path(_vhost): Path<String>,
) -> Json<Vec<QueueInfo>> {
    list_queues(State(broker)).await
}

pub async fn get_queue_vhost(
    State(broker): State<Broker>,
    Path((_vhost, name)): Path<(String, String)>,
) -> Result<Json<QueueInfo>, StatusCode> {
    get_queue(State(broker), Path(name)).await
}

pub async fn delete_queue_vhost(
    State(broker): State<Broker>,
    Path((_vhost, name)): Path<(String, String)>,
) -> StatusCode {
    delete_queue(State(broker), Path(name)).await
}

pub async fn purge_queue_vhost(
    State(broker): State<Broker>,
    Path((_vhost, name)): Path<(String, String)>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    purge_queue(State(broker), Path(name)).await
}

pub async fn get_messages_vhost(
    State(broker): State<Broker>,
    Path((_vhost, name)): Path<(String, String)>,
    body: Json<GetMessagesRequest>,
) -> Result<Json<Vec<MessagePayload>>, StatusCode> {
    get_messages(State(broker), Path(name), body).await
}

// ─── Exchanges ─────────────────────────────────────────

pub async fn list_exchanges(State(broker): State<Broker>) -> Json<Vec<ExchangeInfo>> {
    let (pub_val, pub_rate, del_val, del_rate, ack_val, ack_rate) = get_rates();
    let exchanges = broker.exchanges.read().await;
    let list: Vec<ExchangeInfo> = exchanges
        .iter()
        .map(|(name, ex)| ExchangeInfo {
            name: name.clone(),
            vhost: "/".into(),
            kind: ex.kind.as_str().to_string(),
            durable: ex.durable,
            auto_delete: false,
            internal: false,
            arguments: serde_json::json!({}),
            message_stats: MessageStats {
                publish: Some(pub_val),
                publish_details: Some(RateDetails { rate: pub_rate }),
                deliver_get: Some(del_val),
                deliver_get_details: Some(RateDetails { rate: del_rate }),
                ack: Some(ack_val),
                ack_details: Some(RateDetails { rate: ack_rate }),
            },
        })
        .collect();
    Json(list)
}

pub async fn list_exchanges_vhost(
    State(broker): State<Broker>,
    Path(_vhost): Path<String>,
) -> Json<Vec<ExchangeInfo>> {
    list_exchanges(State(broker)).await
}

pub async fn get_exchange_vhost(
    State(broker): State<Broker>,
    Path((_vhost, name)): Path<(String, String)>,
) -> Result<Json<ExchangeInfo>, StatusCode> {
    let (pub_val, pub_rate, del_val, del_rate, ack_val, ack_rate) = get_rates();
    let exchanges = broker.exchanges.read().await;
    match exchanges.get(&name) {
        Some(ex) => Ok(Json(ExchangeInfo {
            name: name.clone(),
            vhost: "/".into(),
            kind: ex.kind.as_str().to_string(),
            durable: ex.durable,
            auto_delete: false,
            internal: false,
            arguments: serde_json::json!({}),
            message_stats: MessageStats {
                publish: Some(pub_val),
                publish_details: Some(RateDetails { rate: pub_rate }),
                deliver_get: Some(del_val),
                deliver_get_details: Some(RateDetails { rate: del_rate }),
                ack: Some(ack_val),
                ack_details: Some(RateDetails { rate: ack_rate }),
            },
        })),
        None => Err(StatusCode::NOT_FOUND),
    }
}

pub async fn create_exchange_vhost(
    State(broker): State<Broker>,
    Path((_vhost, name)): Path<(String, String)>,
    Json(req): Json<CreateExchangeRequest>,
) -> StatusCode {
    use crate::routing::exchange::{Exchange, ExchangeType};
    let kind = match req.kind.as_str() {
        "fanout" => ExchangeType::Fanout,
        "topic" => ExchangeType::Topic,
        "headers" => ExchangeType::Headers,
        _ => ExchangeType::Direct,
    };
    let mut exchanges = broker.exchanges.write().await;
    exchanges.entry(name.clone()).or_insert_with(|| {
        info!(
            exchange = name.as_str(),
            "exchange created via management API"
        );
        Exchange::new(name, kind, req.durable)
    });
    StatusCode::NO_CONTENT
}

pub async fn delete_exchange_vhost(
    State(broker): State<Broker>,
    Path((_vhost, name)): Path<(String, String)>,
) -> StatusCode {
    let mut exchanges = broker.exchanges.write().await;
    if exchanges.remove(&name).is_some() {
        info!(
            exchange = name.as_str(),
            "exchange deleted via management API"
        );
        StatusCode::NO_CONTENT
    } else {
        StatusCode::NOT_FOUND
    }
}

pub async fn publish_message(
    State(broker): State<Broker>,
    Path(exchange_name): Path<String>,
    Json(req): Json<PublishRequest>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let msg_id = broker.alloc_msg_id();

    let target_queues: Vec<String> = {
        let exchanges = broker.exchanges.read().await;
        let exchange_name_resolved = if exchange_name.is_empty() {
            ""
        } else {
            &exchange_name
        };
        match exchanges.get(exchange_name_resolved) {
            Some(ex) => ex.route(&req.routing_key, &std::collections::HashMap::new()),
            None => {
                return Err((
                    StatusCode::NOT_FOUND,
                    format!("Exchange '{}' not found", exchange_name),
                ));
            }
        }
    };

    if target_queues.is_empty() {
        return Ok(Json(serde_json::json!({ "routed": false, "queues": 0 })));
    }

    for queue_name in &target_queues {
        let msg = crate::queue::message::Message::new_routed(
            msg_id,
            Vec::new(),
            req.payload.as_bytes().to_vec(),
            exchange_name.clone(),
            req.routing_key.clone(),
        );
        if let Some(mut entry) = broker.queues.get_mut(queue_name) {
            entry
                .value_mut()
                .messages
                .push_back(crate::queue::message::QueueMessage::Full(msg));
            crate::metrics::record_published(queue_name);
        }
    }

    let queue_count = target_queues.len();
    info!(
        exchange = exchange_name.as_str(),
        routing_key = req.routing_key.as_str(),
        queues = queue_count,
        "published via management API"
    );
    Ok(Json(
        serde_json::json!({ "routed": true, "queues": queue_count }),
    ))
}

pub async fn publish_message_vhost(
    State(broker): State<Broker>,
    Path((_vhost, name)): Path<(String, String)>,
    body: Json<PublishRequest>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    publish_message(State(broker), Path(name), body).await
}

// ─── Bindings ──────────────────────────────────────────

pub async fn list_bindings(State(broker): State<Broker>) -> Json<Vec<BindingInfo>> {
    let exchanges = broker.exchanges.read().await;
    let mut bindings = Vec::new();
    for (name, ex) in exchanges.iter() {
        for b in &ex.bindings {
            let rk = b.routing_key.clone();
            bindings.push(BindingInfo {
                source: name.clone(),
                vhost: "/".into(),
                destination: b.queue_name.clone(),
                destination_type: "queue".into(),
                routing_key: rk.clone(),
                arguments: serde_json::json!({}),
                properties_key: if rk.is_empty() { "~".into() } else { rk },
            });
        }
    }
    Json(bindings)
}

pub async fn list_bindings_vhost(
    State(broker): State<Broker>,
    Path(_vhost): Path<String>,
) -> Json<Vec<BindingInfo>> {
    list_bindings(State(broker)).await
}

pub async fn exchange_bindings_source(
    State(broker): State<Broker>,
    Path((_vhost, name)): Path<(String, String)>,
) -> Json<Vec<BindingInfo>> {
    let exchanges = broker.exchanges.read().await;
    let mut out = Vec::new();
    if let Some(ex) = exchanges.get(&name) {
        for b in &ex.bindings {
            let rk = b.routing_key.clone();
            out.push(BindingInfo {
                source: name.clone(),
                vhost: "/".into(),
                destination: b.queue_name.clone(),
                destination_type: "queue".into(),
                routing_key: rk.clone(),
                arguments: serde_json::json!({}),
                properties_key: if rk.is_empty() { "~".into() } else { rk },
            });
        }
    }
    Json(out)
}

pub async fn exchange_bindings_dest(
    State(broker): State<Broker>,
    Path((_vhost, _dest_name)): Path<(String, String)>,
) -> Json<Vec<BindingInfo>> {
    let _ = broker;
    Json(vec![])
}

pub async fn queue_bindings_vhost(
    State(broker): State<Broker>,
    Path((_vhost, queue_name)): Path<(String, String)>,
) -> Json<Vec<BindingInfo>> {
    let exchanges = broker.exchanges.read().await;
    let mut bindings = Vec::new();
    for (name, ex) in exchanges.iter() {
        for b in &ex.bindings {
            if b.queue_name == queue_name {
                let rk = b.routing_key.clone();
                bindings.push(BindingInfo {
                    source: name.clone(),
                    vhost: "/".into(),
                    destination: b.queue_name.clone(),
                    destination_type: "queue".into(),
                    routing_key: rk.clone(),
                    arguments: serde_json::json!({}),
                    properties_key: if rk.is_empty() { "~".into() } else { rk },
                });
            }
        }
    }
    Json(bindings)
}

pub async fn create_binding_eq(
    State(broker): State<Broker>,
    Path((_vhost, source, dest)): Path<(String, String, String)>,
    Json(req): Json<CreateBindingRequest>,
) -> StatusCode {
    let mut exchanges = broker.exchanges.write().await;
    if let Some(ex) = exchanges.get_mut(&source) {
        ex.bindings.push(crate::routing::exchange::Binding {
            queue_name: dest,
            routing_key: req.routing_key,
            headers_match: None,
        });
        info!(
            source = source.as_str(),
            "binding created via management API"
        );
        StatusCode::CREATED
    } else {
        StatusCode::NOT_FOUND
    }
}

pub async fn delete_binding_eq(
    State(broker): State<Broker>,
    Path((_vhost, source, dest, pk)): Path<(String, String, String, String)>,
) -> StatusCode {
    let mut exchanges = broker.exchanges.write().await;
    if let Some(ex) = exchanges.get_mut(&source) {
        let rk = if pk == "~" { String::new() } else { pk };
        let before = ex.bindings.len();
        ex.bindings
            .retain(|b| !(b.queue_name == dest && b.routing_key == rk));
        if ex.bindings.len() < before {
            StatusCode::NO_CONTENT
        } else {
            StatusCode::NOT_FOUND
        }
    } else {
        StatusCode::NOT_FOUND
    }
}

pub async fn create_binding_ee() -> StatusCode {
    StatusCode::NO_CONTENT
}
pub async fn delete_binding_ee() -> StatusCode {
    StatusCode::NO_CONTENT
}

// ─── Connections & Channels ────────────────────────────

pub async fn list_connections(State(broker): State<Broker>) -> Json<Vec<ConnectionInfo>> {
    let conns: Vec<ConnectionInfo> = broker
        .connections
        .iter()
        .map(|entry| {
            let handle = entry.value();
            let (user, channels, vhost) = broker
                .conn_state
                .get(&handle.id)
                .map(|cs| (cs.username.clone(), cs.channels.len(), cs.vhost.clone()))
                .unwrap_or_else(|| (String::new(), 0, "/".into()));
            let addr = handle.addr;
            ConnectionInfo {
                name: format!("{}:{} -> 5672", addr.ip(), addr.port()),
                node: "rocketmq@localhost".into(),
                peer_host: addr.ip().to_string(),
                peer_port: addr.port(),
                user,
                vhost,
                channels,
                state: "running".into(),
                conn_type: "network".into(),
                protocol: "AMQP 0-9-1".into(),
                ssl: false,
            }
        })
        .collect();
    Json(conns)
}

pub async fn get_connection(
    State(broker): State<Broker>,
    Path(name): Path<String>,
) -> Result<Json<ConnectionInfo>, StatusCode> {
    for entry in broker.connections.iter() {
        let handle = entry.value();
        let addr = handle.addr;
        let conn_name = format!("{}:{} -> 5672", addr.ip(), addr.port());
        if conn_name == name {
            let (user, channels, vhost) = broker
                .conn_state
                .get(&handle.id)
                .map(|cs| (cs.username.clone(), cs.channels.len(), cs.vhost.clone()))
                .unwrap_or_else(|| (String::new(), 0, "/".into()));
            return Ok(Json(ConnectionInfo {
                name: conn_name,
                node: "rocketmq@localhost".into(),
                peer_host: addr.ip().to_string(),
                peer_port: addr.port(),
                user,
                vhost,
                channels,
                state: "running".into(),
                conn_type: "network".into(),
                protocol: "AMQP 0-9-1".into(),
                ssl: false,
            }));
        }
    }
    Err(StatusCode::NOT_FOUND)
}

pub async fn close_connection(
    State(broker): State<Broker>,
    Path(name): Path<String>,
) -> StatusCode {
    let id = if let Ok(id) = name.parse::<u64>() {
        Some(id)
    } else {
        broker
            .connections
            .iter()
            .find(|e| {
                let a = e.value().addr;
                format!("{}:{} -> 5672", a.ip(), a.port()) == name
            })
            .map(|e| e.value().id)
    };
    if let Some(id) = id
        && broker.connections.remove(&id).is_some()
    {
        broker.conn_state.remove(&id);
        info!(
            conn = name.as_str(),
            "connection force-closed via management API"
        );
        return StatusCode::NO_CONTENT;
    }
    StatusCode::NOT_FOUND
}

pub async fn connection_channels(
    State(broker): State<Broker>,
    Path(name): Path<String>,
) -> Json<Vec<ChannelInfo>> {
    let mut channels = Vec::new();
    for entry in broker.connections.iter() {
        let handle = entry.value();
        let addr = handle.addr;
        let conn_name = format!("{}:{} -> 5672", addr.ip(), addr.port());
        if conn_name == name {
            if let Some(cs) = broker.conn_state.get(&handle.id) {
                for ch in cs.channels.values() {
                    channels.push(build_channel_info(&conn_name, &cs.vhost, &cs.username, ch));
                }
            }
            break;
        }
    }
    Json(channels)
}

pub async fn list_channels(State(broker): State<Broker>) -> Json<Vec<ChannelInfo>> {
    let mut channels = Vec::new();
    for entry in broker.connections.iter() {
        let handle = entry.value();
        let addr = handle.addr;
        let conn_name = format!("{}:{} -> 5672", addr.ip(), addr.port());
        if let Some(cs) = broker.conn_state.get(&handle.id) {
            for ch in cs.channels.values() {
                channels.push(build_channel_info(&conn_name, &cs.vhost, &cs.username, ch));
            }
        }
    }
    Json(channels)
}

pub async fn get_channel(Path(_name): Path<String>) -> StatusCode {
    StatusCode::NOT_FOUND
}

pub fn build_channel_info(
    conn_name: &str,
    vhost: &str,
    user: &str,
    ch: &crate::state::ChannelState,
) -> ChannelInfo {
    ChannelInfo {
        name: format!("{} ({})", conn_name, ch.id),
        node: "rocketmq@localhost".into(),
        number: ch.id,
        connection_details: serde_json::json!({ "name": conn_name, "peer_host": conn_name }),
        vhost: vhost.into(),
        user: user.into(),
        prefetch_count: ch.prefetch_count,
        consumer_count: 0,
        messages_unacknowledged: ch.unacked_count,
        confirm: ch.confirm_mode,
        state: "running".into(),
    }
}

// ─── Consumers ─────────────────────────────────────────

pub async fn list_consumers(State(broker): State<Broker>) -> Json<Vec<ConsumerInfo>> {
    Json(build_consumers(&broker))
}

pub async fn list_consumers_vhost(
    State(broker): State<Broker>,
    Path(_vhost): Path<String>,
) -> Json<Vec<ConsumerInfo>> {
    Json(build_consumers(&broker))
}

pub fn build_consumers(broker: &Arc<BrokerState>) -> Vec<ConsumerInfo> {
    let mut consumers = Vec::new();
    for entry in broker.queues.iter() {
        let (qname, q) = entry.pair();
        for tag in q.consumer_tags.keys() {
            consumers.push(ConsumerInfo {
                consumer_tag: tag.clone(),
                queue: serde_json::json!({ "name": qname, "vhost": "/" }),
                channel_details: serde_json::json!({}),
                ack_required: true,
                exclusive: false,
                active: true,
            });
        }
    }
    consumers
}

// ─── Users & Permissions ───────────────────────────────

pub async fn list_users(State(broker): State<Broker>) -> Json<Vec<UserInfo>> {
    let users: Vec<UserInfo> = broker
        .auth
        .list_users()
        .into_iter()
        .map(|(name, tags)| UserInfo {
            name,
            tags: tags
                .iter()
                .map(|t| format!("{:?}", t).to_lowercase())
                .collect::<Vec<String>>()
                .join(","),
        })
        .collect();
    Json(users)
}

pub async fn add_user(
    State(broker): State<Broker>,
    Json(req): Json<CreateUserRequest>,
) -> Result<StatusCode, (StatusCode, String)> {
    let tags = parse_user_tags(&req.tags);
    broker
        .auth
        .add_user(&req.username, &req.password, tags)
        .map_err(|e| (StatusCode::CONFLICT, e))?;
    save_users(&broker);
    info!(
        user = req.username.as_str(),
        "user created via management API"
    );
    Ok(StatusCode::CREATED)
}

pub async fn delete_user(
    State(broker): State<Broker>,
    Path(name): Path<String>,
) -> Result<StatusCode, (StatusCode, String)> {
    broker
        .auth
        .delete_user(&name)
        .map_err(|e| (StatusCode::NOT_FOUND, e))?;
    save_users(&broker);
    info!(user = name.as_str(), "user deleted via management API");
    Ok(StatusCode::NO_CONTENT)
}

pub async fn change_password(
    State(broker): State<Broker>,
    Path(name): Path<String>,
    Json(req): Json<ChangePasswordRequest>,
) -> Result<StatusCode, (StatusCode, String)> {
    broker
        .auth
        .change_password(&name, &req.password)
        .map_err(|e| (StatusCode::NOT_FOUND, e))?;
    save_users(&broker);
    info!(user = name.as_str(), "password changed via management API");
    Ok(StatusCode::NO_CONTENT)
}

pub async fn get_user(
    State(broker): State<Broker>,
    Path(name): Path<String>,
) -> Result<Json<UserInfo>, StatusCode> {
    for (uname, tags) in broker.auth.list_users() {
        if uname == name {
            return Ok(Json(UserInfo {
                name: uname,
                tags: tags
                    .iter()
                    .map(|t| format!("{:?}", t).to_lowercase())
                    .collect::<Vec<String>>()
                    .join(","),
            }));
        }
    }
    Err(StatusCode::NOT_FOUND)
}

pub async fn upsert_user(
    State(broker): State<Broker>,
    Path(name): Path<String>,
    Json(req): Json<serde_json::Value>,
) -> StatusCode {
    let password = req
        .get("password")
        .and_then(|v| v.as_str())
        .unwrap_or("changeme");

    let tags: Vec<String> = if let Some(arr) = req.get("tags").and_then(|v| v.as_array()) {
        arr.iter()
            .filter_map(|v| v.as_str().map(|s| s.trim().to_string()))
            .collect()
    } else {
        let tags_val = req.get("tags").and_then(|v| v.as_str()).unwrap_or("");
        tags_val
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect()
    };

    let parsed_tags = parse_user_tags(&tags);

    if broker
        .auth
        .add_user(&name, password, parsed_tags.clone())
        .is_err()
    {
        let _ = broker.auth.change_password(&name, password);
        let _ = broker.auth.set_user_tags(&name, parsed_tags);
    }

    save_users(&broker);
    StatusCode::NO_CONTENT
}

pub async fn bulk_delete_users(
    State(broker): State<Broker>,
    Json(req): Json<BulkDeleteRequest>,
) -> StatusCode {
    for name in &req.users {
        let _ = broker.auth.delete_user(name);
    }
    save_users(&broker);
    StatusCode::NO_CONTENT
}

pub async fn list_permissions(State(broker): State<Broker>) -> Json<Vec<PermissionInfo>> {
    let users = broker.auth.list_users();
    let mut perms = Vec::new();
    for (username, _) in &users {
        for p in broker.auth.list_user_permissions(username) {
            perms.push(PermissionInfo {
                user: p.username.clone(),
                vhost: p.vhost.clone(),
                configure: p.configure.clone(),
                write: p.write.clone(),
                read: p.read.clone(),
            });
        }
    }
    Json(perms)
}

pub async fn get_permission(
    State(broker): State<Broker>,
    Path((vhost, user)): Path<(String, String)>,
) -> Result<Json<PermissionInfo>, StatusCode> {
    for p in broker.auth.list_user_permissions(&user) {
        if p.vhost == vhost {
            return Ok(Json(PermissionInfo {
                user: p.username.clone(),
                vhost: p.vhost.clone(),
                configure: p.configure.clone(),
                write: p.write.clone(),
                read: p.read.clone(),
            }));
        }
    }
    Err(StatusCode::NOT_FOUND)
}

pub async fn set_permissions(
    State(broker): State<Broker>,
    Path((user, vhost)): Path<(String, String)>,
    Json(req): Json<SetPermissionRequest>,
) -> Result<StatusCode, (StatusCode, String)> {
    broker
        .auth
        .set_permissions(&user, &vhost, &req.configure, &req.write, &req.read)
        .map_err(|e| (StatusCode::NOT_FOUND, e))?;
    save_users(&broker);
    info!(
        user = user.as_str(),
        vhost = vhost.as_str(),
        "permissions set via management API"
    );
    Ok(StatusCode::NO_CONTENT)
}

pub async fn delete_permission(
    State(broker): State<Broker>,
    Path((_vhost, _user)): Path<(String, String)>,
) -> StatusCode {
    let _ = broker;
    StatusCode::NO_CONTENT
}

pub async fn user_permissions(
    State(broker): State<Broker>,
    Path(name): Path<String>,
) -> Json<Vec<PermissionInfo>> {
    let perms: Vec<PermissionInfo> = broker
        .auth
        .list_user_permissions(&name)
        .into_iter()
        .map(|p| PermissionInfo {
            user: p.username.clone(),
            vhost: p.vhost.clone(),
            configure: p.configure.clone(),
            write: p.write.clone(),
            read: p.read.clone(),
        })
        .collect();
    Json(perms)
}

pub async fn whoami(
    State(broker): State<Broker>,
    headers: axum::http::HeaderMap,
) -> Json<serde_json::Value> {
    if let Some(auth_header) = headers.get(axum::http::header::AUTHORIZATION)
        && let Ok(auth_str) = auth_header.to_str()
        && let Some(encoded) = auth_str.strip_prefix("Basic ")
        && let Some(decoded_bytes) = decode_base64(encoded)
        && let Ok(decoded_str) = String::from_utf8(decoded_bytes)
        && let Some((username, _password)) = decoded_str.split_once(':')
    {
        let user_tags = broker
            .auth
            .list_users()
            .into_iter()
            .find(|(u, _)| u == username)
            .map(|(_, t)| {
                t.iter()
                    .map(|tag| format!("{:?}", tag).to_lowercase())
                    .collect::<Vec<String>>()
                    .join(",")
            })
            .unwrap_or_else(|| "administrator".to_string());

        return Json(serde_json::json!({
            "name": username,
            "tags": user_tags
        }));
    }
    Json(serde_json::json!({ "name": "guest", "tags": "administrator" }))
}

// ─── Stubs & Feature Flags ──────────────────────────────

pub async fn stub_empty_array() -> Json<Vec<serde_json::Value>> {
    Json(vec![])
}
pub async fn stub_not_found() -> StatusCode {
    StatusCode::NOT_FOUND
}
pub async fn stub_no_content() -> StatusCode {
    StatusCode::NO_CONTENT
}

pub async fn list_feature_flags() -> Json<Vec<FeatureFlagInfo>> {
    Json(vec![
        FeatureFlagInfo {
            name: "classic_mirrored_queue_version".into(),
            state: "enabled".into(),
            stability: "stable".into(),
            desc: "Support classic mirrored queue version".into(),
        },
        FeatureFlagInfo {
            name: "quorum_queue".into(),
            state: "enabled".into(),
            stability: "stable".into(),
            desc: "Support quorum queues".into(),
        },
        FeatureFlagInfo {
            name: "stream_queue".into(),
            state: "enabled".into(),
            stability: "stable".into(),
            desc: "Support stream queues".into(),
        },
    ])
}

// ─── Definitions (Export) ───────────────────────────────

pub async fn get_definitions(State(broker): State<Broker>) -> Json<serde_json::Value> {
    let users: Vec<serde_json::Value> = broker.auth.list_users().into_iter()
        .map(|(name, tags)| serde_json::json!({ "name": name, "tags": tags.iter().map(|t| format!("{:?}", t).to_lowercase()).collect::<Vec<_>>().join(",") }))
        .collect();
    let vhosts: Vec<serde_json::Value> = broker
        .list_vhosts()
        .into_iter()
        .map(|n| serde_json::json!({ "name": n }))
        .collect();
    let queues: Vec<serde_json::Value> = broker.queues.iter().map(|e| {
        let (n, q) = e.pair();
        serde_json::json!({ "name": n, "vhost": "/", "durable": q.options.durable, "auto_delete": q.options.auto_delete, "arguments": {} })
    }).collect();
    let exchanges_guard = broker.exchanges.read().await;
    let exchanges: Vec<serde_json::Value> = exchanges_guard.iter().map(|(n, ex)| {
        serde_json::json!({ "name": n, "vhost": "/", "type": ex.kind.as_str(), "durable": ex.durable, "auto_delete": false, "internal": false, "arguments": {} })
    }).collect();
    let mut bindings = Vec::new();
    for (n, ex) in exchanges_guard.iter() {
        for b in &ex.bindings {
            bindings.push(serde_json::json!({ "source": n, "vhost": "/", "destination": b.queue_name, "destination_type": "queue", "routing_key": b.routing_key, "arguments": {} }));
        }
    }
    let perms: Vec<serde_json::Value> = broker.auth.list_users().into_iter()
        .flat_map(|(u, _)| broker.auth.list_user_permissions(&u).into_iter()
            .map(|p| serde_json::json!({ "user": p.username, "vhost": p.vhost, "configure": p.configure, "write": p.write, "read": p.read }))
            .collect::<Vec<_>>())
        .collect();
    Json(serde_json::json!({
        "rabbit_version": env!("CARGO_PKG_VERSION"),
        "rabbitmq_version": env!("CARGO_PKG_VERSION"),
        "product_name": "RocketMQ",
        "users": users, "vhosts": vhosts, "queues": queues,
        "exchanges": exchanges, "bindings": bindings, "permissions": perms,
        "topic_permissions": [], "parameters": [], "global_parameters": [], "policies": [],
    }))
}

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

// ─── Helpers ───────────────────────────────────────────

pub fn decode_base64(s: &str) -> Option<Vec<u8>> {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut map = [0u8; 256];
    for (i, &c) in ALPHABET.iter().enumerate() {
        map[c as usize] = i as u8;
    }

    let bytes = s.as_bytes();
    let mut result = Vec::new();
    let mut buffer = 0u32;
    let mut bits = 0;

    for &b in bytes {
        if b == b'=' {
            break;
        }
        let val = map[b as usize];
        if val == 0 && b != b'A' {
            if b != b'\r' && b != b'\n' && b != b' ' {
                return None;
            }
            continue;
        }
        buffer = (buffer << 6) | (val as u32);
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            result.push((buffer >> bits) as u8);
        }
    }
    Some(result)
}

fn write_counter(out: &mut String, name: &str, help: &str, value: u64) {
    out.push_str(&format!(
        "# HELP {} {}\n# TYPE {} counter\n{} {}\n\n",
        name, help, name, name, value
    ));
}

fn write_gauge(out: &mut String, name: &str, help: &str, value: u64) {
    out.push_str(&format!(
        "# HELP {} {}\n# TYPE {} gauge\n{} {}\n\n",
        name, help, name, name, value
    ));
}

pub fn save_users(broker: &Arc<BrokerState>) {
    let db_path = crate::config::get_user_db_path();
    let path = std::path::Path::new(&db_path);
    if let Err(e) = broker.auth.save_to_file(path) {
        tracing::warn!(error = %e, "failed to persist user database");
    }
}

pub fn parse_user_tags(tags: &[String]) -> Vec<crate::auth::credentials::UserTag> {
    tags.iter()
        .filter_map(|t| match t.as_str() {
            "administrator" => Some(crate::auth::credentials::UserTag::Administrator),
            "monitoring" => Some(crate::auth::credentials::UserTag::Monitoring),
            "management" => Some(crate::auth::credentials::UserTag::Management),
            _ => None,
        })
        .collect()
}

pub fn queue_totals_for_vhost(broker: &Arc<BrokerState>) -> (usize, usize) {
    let (mut msgs, mut inflight) = (0, 0);
    for entry in broker.queues.iter() {
        let q = entry.value();
        msgs += q.messages.len();
        inflight += q.inflight.len();
    }
    (msgs, inflight)
}

pub fn get_process_memory() -> u64 {
    std::fs::read_to_string("/proc/self/statm")
        .ok()
        .and_then(|s| {
            let rss_pages = s.split_whitespace().nth(1)?.parse::<u64>().ok()?;
            Some(rss_pages * 4096)
        })
        .unwrap_or(0)
}

pub fn get_disk_free() -> u64 {
    #[cfg(target_os = "linux")]
    {
        use std::ffi::CString;
        let dir_path = crate::config::get_data_dir();
        let path = CString::new(dir_path).unwrap_or_default();
        unsafe {
            let mut buf: libc::statvfs = std::mem::zeroed();
            if libc::statvfs(path.as_ptr(), &mut buf) == 0 {
                return buf.f_bavail as u64 * buf.f_frsize as u64;
            }
        }
    }
    10 * 1024 * 1024 * 1024
}

pub fn num_cpus() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1)
}
