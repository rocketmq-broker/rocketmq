// Copyright (c) 2026 Edilson Pateguana
// Licensed under the Apache License, Version 2.0
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::Json;
use tracing::info;

use crate::management::types::*;
use crate::state::Broker;

// ─── Queues & Bindings ──────────────────────────────────

pub async fn list_queues(
    State(broker): State<Broker>,
    Query(params): Query<PaginationParams>,
) -> Json<PaginatedResponse<QueueInfo>> {
    let queues: Vec<QueueInfo> = broker
        .queues
        .iter()
        .map(|entry| {
            let (name, q) = entry.pair();
            build_queue_info(name, q, &broker)
        })
        .collect();
    Json(PaginatedResponse::from_vec(queues, &params))
}

pub async fn get_queue(
    State(broker): State<Broker>,
    Path(name): Path<String>,
) -> Result<Json<QueueInfo>, StatusCode> {
    match broker.queues.get(&name) {
        Some(entry) => Ok(Json(build_queue_info(&name, entry.value(), &broker))),
        None => Err(StatusCode::NOT_FOUND),
    }
}

/// Builds the queue info payload for management API responses.
/// Builds the queue info payload for management API responses.
pub fn build_queue_info(name: &str, q: &crate::queue::QueueState, broker: &Broker) -> QueueInfo {
    let node_name = format!("rocketmq-node-{}@localhost", crate::config::get_node_id());

    let mut args = serde_json::Map::new();
    if let Some(ttl) = q.options.message_ttl {
        args.insert("x-message-ttl".into(), serde_json::json!(ttl.as_millis()));
    }
    if let Some(ref dlx) = q.options.dead_letter_exchange {
        args.insert("x-dead-letter-exchange".into(), serde_json::json!(dlx));
    }
    if let Some(ref dlkey) = q.options.dead_letter_routing_key {
        args.insert("x-dead-letter-routing-key".into(), serde_json::json!(dlkey));
    }
    if let Some(exp) = q.options.expires {
        args.insert("x-expires".into(), serde_json::json!(exp.as_millis()));
    }
    if let Some(lim) = q.options.rate_limit {
        args.insert("x-rate-limit".into(), serde_json::json!(lim));
    }
    if q.options.stream_mode {
        args.insert("x-queue-type".into(), serde_json::json!("stream"));
    }
    let arguments = serde_json::Value::Object(args);

    let mut consumer_details = Vec::new();
    for (tag, &(conn_id, channel_id)) in &q.consumer_tags {
        let mut prefetch = 0;
        let mut active = true;
        let mut channel_name = String::new();
        let mut conn_name = String::new();
        let mut peer_ip = String::new();
        let mut peer_port = 0;
        let mut username = String::new();

        if let Some(conn) = broker.connections.get(&conn_id) {
            peer_ip = conn.addr.ip().to_string();
            peer_port = conn.addr.port();
            conn_name = format!("{}:{} -> 5672", peer_ip, peer_port);
            channel_name = format!("{}:{} -> 5672 ({})", peer_ip, peer_port, channel_id);
        }

        if let Some(cs) = broker.conn_state.get(&conn_id) {
            username = cs.username.clone();
            if let Some(ch) = cs.channels.get(&channel_id) {
                prefetch = ch.prefetch_count as usize;
                active = ch.can_deliver();
            }
        }

        consumer_details.push(serde_json::json!({
            "consumer_tag": tag,
            "ack_required": true,
            "exclusive": false,
            "prefetch_count": prefetch,
            "active": active,
            "activity_status": "idle",
            "consumer_timeout": 0,
            "arguments": {},
            "channel_details": {
                "name": channel_name,
                "number": channel_id,
                "connection_name": conn_name,
                "peer_host": peer_ip,
                "peer_port": peer_port,
                "user": username,
            }
        }));
    }

    let owner_pid_details = q.owner_conn_id.map(|conn_id| {
        let (name, peer_host, peer_port) = if let Some(conn) = broker.connections.get(&conn_id) {
            let ip = conn.addr.ip().to_string();
            let port = conn.addr.port();
            (format!("{}:{} -> 5672", ip, port), ip, port)
        } else {
            (format!("unknown_conn_{}", conn_id), String::new(), 0)
        };
        serde_json::json!({
            "name": name,
            "peer_host": peer_host,
            "peer_port": peer_port,
        })
    });

    let pub_rate = 0.0_f64;
    let del_rate = 0.0_f64;
    let ack_rate = 0.0_f64;

    let msg_total = (q.messages.len() + q.inflight.len()) as u64;
    let msg_ready = q.messages.len() as u64;
    let msg_unacked = q.inflight.len() as u64;

    QueueInfo {
        name: name.to_string(),
        vhost: "/".into(),
        queue_type: if q.options.stream_mode {
            "stream".into()
        } else {
            "classic".into()
        },
        durable: q.options.durable,
        exclusive: q.options.exclusive,
        auto_delete: q.options.auto_delete,
        messages: msg_total as usize,
        messages_ready: msg_ready as usize,
        messages_unacknowledged: msg_unacked as usize,

        messages_details: Some(RateDetails::from_current(0.0, msg_total)),
        messages_ready_details: Some(RateDetails::from_current(0.0, msg_ready)),
        messages_unacknowledged_details: Some(RateDetails::from_current(0.0, msg_unacked)),
        consumers: q.consumer_tags.len(),
        state: "running".into(),
        node: node_name,
        message_stats: MessageStats {
            publish: Some(q.stat_published),
            publish_details: Some(RateDetails::from_current(pub_rate, q.stat_published)),
            deliver_get: Some(q.stat_delivered),
            deliver_get_details: Some(RateDetails::from_current(del_rate, q.stat_delivered)),
            ack: Some(q.stat_acked),
            ack_details: Some(RateDetails::from_current(ack_rate, q.stat_acked)),
            deliver: Some(q.stat_delivered),
            deliver_details: Some(RateDetails::from_current(del_rate, q.stat_delivered)),
            confirm: None,
            confirm_details: None,
        },
        arguments,
        consumer_details,
        owner_pid_details,
        effective_policy_definition: serde_json::json!({}),
        incoming: Vec::new(),
        deliveries: Vec::new(),
        reductions: None,
        garbage_collection: None,
        policy: None,
        operator_policy: None,
    }
}

/// Deletes a queue from the broker.
/// Deletes a queue from the broker.
pub async fn delete_queue(State(broker): State<Broker>, Path(name): Path<String>) -> StatusCode {
    if broker.queues.remove(&name).is_some() {
        crate::metrics::record_queue_deleted();
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
    body_bytes: axum::body::Bytes,
) -> Result<Json<Vec<MessagePayload>>, StatusCode> {
    let req: GetMessagesRequest = if body_bytes.is_empty() {
        GetMessagesRequest {
            count: 1,
            ack_mode: "ack_requeue_false".into(),
        }
    } else {
        serde_json::from_slice(&body_bytes).map_err(|e| {
            tracing::warn!("Failed to deserialize get_messages request: {}", e);
            StatusCode::BAD_REQUEST
        })?
    };

    match broker.queues.get_mut(&name) {
        Some(mut entry) => {
            let queue = entry.value_mut();
            let remaining = queue.messages.len();
            let count = req.count.min(remaining);
            let mut result = Vec::with_capacity(count);
            let requeue = req.ack_mode == "ack_requeue_true";

            for _ in 0..count {
                if let Some(q_msg) = queue.messages.pop_front() {
                    if let Ok(msg) = q_msg.resolve(broker.wal()) {
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
    Query(params): Query<PaginationParams>,
) -> Json<PaginatedResponse<QueueInfo>> {
    list_queues(State(broker), Query(params)).await
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
    body_bytes: axum::body::Bytes,
) -> Result<Json<Vec<MessagePayload>>, StatusCode> {
    get_messages(State(broker), Path(name), body_bytes).await
}
