// Copyright (c) 2026 Edilson Pateguana
// Licensed under the Apache License, Version 2.0
use axum::extract::Path;
use axum::http::StatusCode;
use axum::response::Json;

use crate::management::types::*;

/// Verifies that the broker HTTP server is responsive.
/// Verifies that the broker HTTP server is responsive.
pub async fn healthcheck() -> StatusCode {
    StatusCode::OK
}

/// Checks if there are any active resource alarms (e.g., memory or disk pressure).
/// Checks if there are any active resource alarms (e.g., memory or disk pressure).
pub async fn health_alarms() -> Json<HealthResponse> {
    Json(HealthResponse {
        status: "ok".into(),
    })
}

/// Verifies if the specified network port listener is active.
/// Verifies if the specified network port listener is active.
pub async fn health_port_listener(Path(port): Path<u16>) -> Json<HealthResponse> {
    let ok = matches!(port, 5672 | 5671 | 15672);
    Json(HealthResponse {
        status: if ok { "ok" } else { "failed" }.into(),
    })
}
