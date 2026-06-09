// Copyright (c) 2026 Edilson Pateguana
// Licensed under the Apache License, Version 2.0
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::Json;
use tracing::info;

use crate::management::routes::helpers::*;
use crate::management::types::*;
use crate::state::Broker;

// ─── Exchanges ─────────────────────────────────────────

pub async fn list_exchanges(
    State(broker): State<Broker>,
    Query(params): Query<PaginationParams>,
) -> Json<PaginatedResponse<ExchangeInfo>> {
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
        })
        .collect();
    Json(PaginatedResponse::from_vec(list, &params))
}

pub async fn list_exchanges_vhost(
    State(broker): State<Broker>,
    Path(_vhost): Path<String>,
    Query(params): Query<PaginationParams>,
) -> Json<PaginatedResponse<ExchangeInfo>> {
    list_exchanges(State(broker), Query(params)).await
}

pub async fn get_exchange_vhost(
    State(broker): State<Broker>,
    Path((_vhost, name)): Path<(String, String)>,
) -> Result<Json<ExchangeInfo>, StatusCode> {
    let (pub_val, pub_rate, del_val, del_rate, ack_val, ack_rate) = get_rates();
    let lookup_name = resolve_exchange_name(&name);
    let exchanges = broker.exchanges.read().await;
    match exchanges.get(lookup_name) {
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
    body_bytes: axum::body::Bytes,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let req: PublishRequest = serde_json::from_slice(&body_bytes).map_err(|e| {
        tracing::warn!("Failed to deserialize publish request: {}", e);
        (StatusCode::BAD_REQUEST, format!("Invalid JSON: {}", e))
    })?;

    let msg_id = broker.alloc_msg_id();

    let target_queues: Vec<String> = {
        let exchanges = broker.exchanges.read().await;
        let exchange_name_resolved = resolve_exchange_name(&exchange_name);
        match exchanges.get(exchange_name_resolved) {
            Some(ex) => {
                let mut qs = Vec::new();
                ex.route_each(&req.routing_key, &std::collections::HashMap::new(), |q| {
                    qs.push(q.as_ref().to_string())
                });
                qs
            }
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
            Vec::new().into(),
            req.payload.as_bytes().to_vec().into(),
            exchange_name.clone().into(),
            req.routing_key.clone().into(),
        );
        if let Some(mut entry) = broker.queues.get_mut(queue_name.as_str()) {
            entry.value_mut().stat_published += 1;
            entry
                .value_mut()
                .messages
                .push_back(crate::queue::message::QueueMessage::Full(msg));
            crate::metrics::record_published(queue_name.as_str());
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
    body_bytes: axum::body::Bytes,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    publish_message(State(broker), Path(name), body_bytes).await
}
