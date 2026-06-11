// Copyright (c) 2026 Edilson Pateguana
// Licensed under the Apache License, Version 2.0
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::Json;

use crate::management::routes::helpers::*;
use crate::management::types::*;
use crate::state::Broker;

/// Lists all configured virtual hosts in the broker.
/// Returns the list of all configured virtual host names.
/// Lists all configured virtual hosts in the broker.
/// Returns the list of all configured virtual host names.
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
            cluster_state: {
                let node_name = format!("rocketmq-node-{}@localhost", crate::config::get_node_id());
                serde_json::json!({ node_name: "running" })
            },
            tracing: false,
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
            cluster_state: {
                let node_name = format!("rocketmq-node-{}@localhost", crate::config::get_node_id());
                serde_json::json!({ node_name: "running" })
            },
            tracing: false,
        }))
    } else {
        Err(StatusCode::NOT_FOUND)
    }
}

/// Creates a new virtual host with the specified name.
/// Creates a new virtual host with the specified name.
pub async fn create_vhost(State(broker): State<Broker>, Path(name): Path<String>) -> StatusCode {
    broker
        .vhosts
        .entry(name.clone())
        .or_insert_with(|| crate::state::vhost::VHost::new(name));
    StatusCode::NO_CONTENT
}

/// Removes a virtual host and all of its associated queues and exchanges.
/// Removes a virtual host and all of its associated queues and exchanges.
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
