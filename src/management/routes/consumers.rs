// Copyright (c) 2026 Edilson Pateguana
// Licensed under the Apache License, Version 2.0
use axum::extract::{Path, State};
use axum::response::Json;
use std::sync::Arc;

use crate::management::types::*;
use crate::state::{Broker, BrokerState};

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
