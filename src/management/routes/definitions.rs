// Copyright (c) 2026 Edilson Pateguana
// Licensed under the Apache License, Version 2.0
use axum::extract::State;
use axum::response::Json;

use crate::state::Broker;

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
