// Copyright (c) 2026 Edilson Pateguana
// Licensed under the Apache License, Version 2.0
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::Json;
use tracing::info;

use crate::management::routes::helpers::*;
use crate::management::types::*;
use crate::state::Broker;

pub async fn list_bindings(State(broker): State<Broker>) -> Json<Vec<BindingInfo>> {
    let exchanges = broker.exchanges.read().await;
    let mut bindings = Vec::new();
    for (name, ex) in exchanges.iter() {
        for b in &ex.bindings {
            let rk = b.routing_key.clone();
            bindings.push(BindingInfo {
                source: name.clone(),
                vhost: "/".into(),
                destination: b.queue_name.as_ref().to_string(),
                destination_type: "queue".into(),
                routing_key: rk.as_ref().to_string(),
                arguments: serde_json::json!({}),
                properties_key: if rk.is_empty() {
                    "~".into()
                } else {
                    rk.as_ref().to_string()
                },
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
    let lookup_name = resolve_exchange_name(&name);
    let exchanges = broker.exchanges.read().await;
    let mut out = Vec::new();
    if let Some(ex) = exchanges.get(lookup_name) {
        for b in &ex.bindings {
            let rk = b.routing_key.clone();
            out.push(BindingInfo {
                source: name.clone(),
                vhost: "/".into(),
                destination: b.queue_name.as_ref().to_string(),
                destination_type: "queue".into(),
                routing_key: rk.as_ref().to_string(),
                arguments: serde_json::json!({}),
                properties_key: if rk.is_empty() {
                    "~".into()
                } else {
                    rk.as_ref().to_string()
                },
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
            if b.queue_name.as_ref() == queue_name {
                let rk = b.routing_key.clone();
                bindings.push(BindingInfo {
                    source: name.clone(),
                    vhost: "/".into(),
                    destination: b.queue_name.as_ref().to_string(),
                    destination_type: "queue".into(),
                    routing_key: rk.as_ref().to_string(),
                    arguments: serde_json::json!({}),
                    properties_key: if rk.is_empty() {
                        "~".into()
                    } else {
                        rk.as_ref().to_string()
                    },
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
            queue_name: dest.into(),
            routing_key: req.routing_key.into(),
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
            .retain(|b| !(b.queue_name.as_ref() == dest && b.routing_key.as_ref() == rk));
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
