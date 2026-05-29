// Copyright (c) 2026 Edilson Pateguana
// Licensed under the Apache License, Version 2.0
use axum::http::StatusCode;
use axum::response::Json;

use crate::management::types::*;

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
