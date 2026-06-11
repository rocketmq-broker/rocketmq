// Copyright (c) 2026 Edilson Pateguana
// Licensed under the Apache License, Version 2.0
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::Json;
use tracing::info;

use crate::management::types::*;
use crate::state::Broker;

pub async fn list_connections(
    State(broker): State<Broker>,
    Query(params): Query<PaginationParams>,
) -> Json<PaginatedResponse<ConnectionInfo>> {
    let conns: Vec<ConnectionInfo> = broker
        .connections
        .iter()
        .map(|entry| {
            let handle = entry.value();
            let mut user = String::new();
            let mut channels = 0;
            let mut vhost = "/".to_string();
            let mut timeout = 60;
            let mut frame_max = 131_072;
            let mut channel_max = 2047;

            if let Some(cs_guard) = broker.conn_state.get(&handle.id) {
                let cs = cs_guard.value();
                user = cs.username();
                channels = cs.channels_count();
                vhost = cs.vhost();
                timeout = cs.heartbeat() as u32;
                frame_max = cs.frame_max();
                channel_max = cs.channel_max();
            }

            let addr = handle.addr;
            ConnectionInfo {
                name: format!("{}:{} -> 5672", addr.ip(), addr.port()),
                node: format!("rocketmq-node-{}@localhost", crate::config::get_node_id()),
                peer_host: addr.ip().to_string(),
                peer_port: addr.port(),
                user,
                vhost,
                channels,
                state: "running".into(),
                conn_type: "network".into(),
                protocol: "AMQP 0-9-1".into(),
                ssl: false,
                client_properties: serde_json::json!({}),
                connected_at: broker.start_time_ms(),
                timeout,
                frame_max,
                channel_max,
                auth_mechanism: "PLAIN".into(),
            }
        })
        .collect();
    Json(PaginatedResponse::from_vec(conns, &params))
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
            let mut user = String::new();
            let mut channels = 0;
            let mut vhost = "/".to_string();
            let mut timeout = 60;
            let mut frame_max = 131_072;
            let mut channel_max = 2047;

            if let Some(cs_guard) = broker.conn_state.get(&handle.id) {
                let cs = cs_guard.value();
                user = cs.username();
                channels = cs.channels_count();
                vhost = cs.vhost();
                timeout = cs.heartbeat() as u32;
                frame_max = cs.frame_max();
                channel_max = cs.channel_max();
            }

            return Ok(Json(ConnectionInfo {
                name: conn_name,
                node: format!("rocketmq-node-{}@localhost", crate::config::get_node_id()),
                peer_host: addr.ip().to_string(),
                peer_port: addr.port(),
                user,
                vhost,
                channels,
                state: "running".into(),
                conn_type: "network".into(),
                protocol: "AMQP 0-9-1".into(),
                ssl: false,
                client_properties: serde_json::json!({}),
                connected_at: broker.start_time_ms(),
                timeout,
                frame_max,
                channel_max,
                auth_mechanism: "PLAIN".into(),
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
            if let Some(cs_guard) = broker.conn_state.get(&handle.id) {
                let cs = cs_guard.value();
                for ch in cs.get_channels() {
                    channels.push(build_channel_info(
                        &conn_name,
                        handle.id,
                        &cs.vhost(),
                        &cs.username(),
                        &ch,
                        &broker,
                    ));
                }
            }
            break;
        }
    }
    Json(channels)
}

pub async fn list_channels(
    State(broker): State<Broker>,
    Query(params): Query<PaginationParams>,
) -> Json<PaginatedResponse<ChannelInfo>> {
    let mut channels = Vec::new();
    for entry in broker.connections.iter() {
        let handle = entry.value();
        let addr = handle.addr;
        let conn_name = format!("{}:{} -> 5672", addr.ip(), addr.port());
        if let Some(cs_guard) = broker.conn_state.get(&handle.id) {
            let cs = cs_guard.value();
            for ch in cs.get_channels() {
                channels.push(build_channel_info(
                    &conn_name,
                    handle.id,
                    &cs.vhost(),
                    &cs.username(),
                    &ch,
                    &broker,
                ));
            }
        }
    }
    Json(PaginatedResponse::from_vec(channels, &params))
}

pub async fn get_channel(
    State(broker): State<Broker>,
    Path(name): Path<String>,
) -> Result<Json<ChannelInfo>, StatusCode> {
    for entry in broker.connections.iter() {
        let handle = entry.value();
        let addr = handle.addr;
        let conn_name = format!("{}:{} -> 5672", addr.ip(), addr.port());
        if let Some(cs_guard) = broker.conn_state.get(&handle.id) {
            let cs = cs_guard.value();
            for ch in cs.get_channels() {
                let full_name = format!("{} ({})", conn_name, ch.id);
                if full_name == name {
                    return Ok(Json(build_channel_info(
                        &conn_name,
                        handle.id,
                        &cs.vhost(),
                        &cs.username(),
                        &ch,
                        &broker,
                    )));
                }
            }
        }
    }
    Err(StatusCode::NOT_FOUND)
}

pub fn build_channel_info(
    conn_name: &str,
    conn_id: u64,
    vhost: &str,
    user: &str,
    ch: &crate::protocol::ChannelMeta,
    broker: &Broker,
) -> ChannelInfo {
    let (pub_val, pub_rate, del_val, del_rate, ack_val, ack_rate) = get_rates();

    let mut consumer_details = Vec::new();
    for entry in broker.queues.iter() {
        let (q_name, queue) = entry.pair();
        for (tag, &(c_id, ch_id, _)) in &queue.consumer_tags {
            if c_id == conn_id && ch_id == ch.id {
                consumer_details.push(serde_json::json!({
                    "consumer_tag": tag,
                    "ack_required": true,
                    "exclusive": false,
                    "prefetch_count": ch.prefetch_count,
                    "active": ch.flow_active && (ch.prefetch_count == 0 || ch.unacked_count < ch.prefetch_count),
                    "activity_status": "idle",
                    "consumer_timeout": 0,
                    "arguments": {},
                    "queue": {
                        "name": q_name,
                        "vhost": vhost,
                    },
                    "channel_details": {
                        "name": format!("{} ({})", conn_name, ch.id),
                        "number": ch.id,
                        "connection_name": conn_name,
                        "peer_host": conn_name,
                        "peer_port": 0,
                        "user": user,
                    }
                }));
            }
        }
    }
    let consumer_count = consumer_details.len();

    ChannelInfo {
        name: format!("{} ({})", conn_name, ch.id),
        node: format!("rocketmq-node-{}@localhost", crate::config::get_node_id()),
        number: ch.id,
        connection_details: serde_json::json!({ "name": conn_name, "peer_host": conn_name }),
        vhost: vhost.into(),
        user: user.into(),
        prefetch_count: ch.prefetch_count,
        consumer_count,
        messages_unacknowledged: ch.unacked_count,
        messages_unconfirmed: 0,
        messages_uncommitted: 0,
        acks_uncommitted: 0,
        pending_raft_commands: 0,
        cached_segments: 0,
        confirm: ch.confirm_mode,
        state: "running".into(),
        consumer_details,
        message_stats: MessageStats {
            publish: Some(pub_val),
            publish_details: Some(RateDetails::from_current(pub_rate, pub_val)),
            deliver_get: Some(del_val),
            deliver_get_details: Some(RateDetails::from_current(del_rate, del_val)),
            ack: Some(ack_val),
            ack_details: Some(RateDetails::from_current(ack_rate, ack_val)),
            deliver: Some(del_val),
            deliver_details: Some(RateDetails::from_current(del_rate, del_val)),
            confirm: None,
            confirm_details: None,
        },
        publishes: Vec::new(),
        deliveries: Vec::new(),
        reductions: None,
        garbage_collection: None,
    }
}
