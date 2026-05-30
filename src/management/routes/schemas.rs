// Copyright (c) 2026 Edilson Pateguana
// Licensed under the Apache License, Version 2.0

//! Confluent-compatible REST API for the built-in schema registry.
//!
//! All endpoints live under `/api/schemas/` and follow the Confluent
//! Schema Registry v1 API contract for Protobuf schemas.

use axum::Json;
use axum::extract::{Path, State};
use axum::http::StatusCode;

use crate::schema::registry::{CompatibilityLevel, SchemaResponse};
use crate::state::Broker;

// ─── Request / Response Types ─────────────────────────

#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RegisterSchemaRequest {
    pub schema: String,
    pub message_name: String,
    #[serde(default = "default_schema_type")]
    pub schema_type: String,
}

fn default_schema_type() -> String {
    "PROTOBUF".to_string()
}

#[derive(serde::Serialize)]
pub struct RegisterSchemaResponse {
    pub id: u64,
}

#[derive(serde::Serialize)]
pub struct ErrorResponse {
    pub error_code: u16,
    pub message: String,
}

#[derive(serde::Deserialize, serde::Serialize)]
pub struct CompatibilityConfig {
    pub compatibility: CompatibilityLevel,
}

#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CompatibilityCheckRequest {
    pub schema: String,
    pub message_name: String,
}

#[derive(serde::Serialize)]
pub struct CompatibilityCheckResponse {
    pub is_compatible: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

// ─── List All Schemas ─────────────────────────────────

pub async fn list_schemas(State(broker): State<Broker>) -> Json<Vec<SchemaResponse>> {
    let entries = broker.schema_registry.list_all();
    Json(entries.iter().map(SchemaResponse::from).collect())
}

// ─── Get Schema by ID ─────────────────────────────────

pub async fn get_schema_by_id(
    State(broker): State<Broker>,
    Path(id): Path<u64>,
) -> Result<Json<SchemaResponse>, StatusCode> {
    broker
        .schema_registry
        .get_by_id(id)
        .map(|e| Json(SchemaResponse::from(&e)))
        .ok_or(StatusCode::NOT_FOUND)
}

// ─── List Subjects ────────────────────────────────────

pub async fn list_subjects(State(broker): State<Broker>) -> Json<Vec<String>> {
    Json(broker.schema_registry.list_subjects())
}

// ─── List Versions ────────────────────────────────────

pub async fn list_subject_versions(
    State(broker): State<Broker>,
    Path(subject): Path<String>,
) -> Result<Json<Vec<u32>>, StatusCode> {
    broker
        .schema_registry
        .list_versions(&subject)
        .map(Json)
        .ok_or(StatusCode::NOT_FOUND)
}

// ─── Register Schema ──────────────────────────────────

pub async fn register_schema(
    State(broker): State<Broker>,
    Path(subject): Path<String>,
    Json(req): Json<RegisterSchemaRequest>,
) -> Result<Json<RegisterSchemaResponse>, (StatusCode, Json<ErrorResponse>)> {
    if req.schema_type.to_uppercase() != "PROTOBUF" {
        return Err(schema_error(
            StatusCode::UNPROCESSABLE_ENTITY,
            &format!(
                "Unsupported schema type '{}'. Only PROTOBUF is supported.",
                req.schema_type
            ),
        ));
    }

    match broker
        .schema_registry
        .register(&subject, &req.schema, &req.message_name)
    {
        Ok(id) => Ok(Json(RegisterSchemaResponse { id })),
        Err(e) => Err(schema_error(
            StatusCode::UNPROCESSABLE_ENTITY,
            &e.to_string(),
        )),
    }
}

// ─── Get Version ──────────────────────────────────────

pub async fn get_subject_version(
    State(broker): State<Broker>,
    Path((subject, version)): Path<(String, String)>,
) -> Result<Json<SchemaResponse>, StatusCode> {
    let entry = if version == "latest" {
        broker.schema_registry.get_latest(&subject)
    } else {
        let v: u32 = version.parse().map_err(|_| StatusCode::BAD_REQUEST)?;
        broker.schema_registry.get_version(&subject, v)
    };

    entry
        .map(|e| Json(SchemaResponse::from(&e)))
        .ok_or(StatusCode::NOT_FOUND)
}

// ─── Delete Subject ───────────────────────────────────

pub async fn delete_subject(
    State(broker): State<Broker>,
    Path(subject): Path<String>,
) -> Result<Json<Vec<u32>>, StatusCode> {
    let deleted = broker.schema_registry.delete_subject(&subject);
    if deleted.is_empty() {
        return Err(StatusCode::NOT_FOUND);
    }
    Ok(Json(deleted))
}

// ─── Delete Version ───────────────────────────────────

pub async fn delete_subject_version(
    State(broker): State<Broker>,
    Path((subject, version)): Path<(String, u32)>,
) -> Result<StatusCode, StatusCode> {
    if broker.schema_registry.delete_version(&subject, version) {
        Ok(StatusCode::NO_CONTENT)
    } else {
        Err(StatusCode::NOT_FOUND)
    }
}

// ─── Compatibility Check ──────────────────────────────

pub async fn check_compatibility(
    State(broker): State<Broker>,
    Path(subject): Path<String>,
    Json(req): Json<CompatibilityCheckRequest>,
) -> Json<CompatibilityCheckResponse> {
    let result = broker
        .schema_registry
        .register(&subject, &req.schema, &req.message_name);

    match result {
        Ok(_) => Json(CompatibilityCheckResponse {
            is_compatible: true,
            message: None,
        }),
        Err(e) => Json(CompatibilityCheckResponse {
            is_compatible: false,
            message: Some(e.to_string()),
        }),
    }
}

// ─── Compatibility Config ─────────────────────────────

pub async fn get_global_config(State(broker): State<Broker>) -> Json<CompatibilityConfig> {
    Json(CompatibilityConfig {
        compatibility: broker.schema_registry.global_compat(),
    })
}

pub async fn set_global_config(
    State(broker): State<Broker>,
    Json(req): Json<CompatibilityConfig>,
) -> Json<CompatibilityConfig> {
    broker.schema_registry.set_global_compat(req.compatibility);
    Json(CompatibilityConfig {
        compatibility: req.compatibility,
    })
}

pub async fn get_subject_config(
    State(broker): State<Broker>,
    Path(subject): Path<String>,
) -> Json<CompatibilityConfig> {
    let level = broker
        .schema_registry
        .subject_compat(&subject)
        .unwrap_or_else(|| broker.schema_registry.global_compat());
    Json(CompatibilityConfig {
        compatibility: level,
    })
}

pub async fn set_subject_config(
    State(broker): State<Broker>,
    Path(subject): Path<String>,
    Json(req): Json<CompatibilityConfig>,
) -> Json<CompatibilityConfig> {
    broker
        .schema_registry
        .set_subject_compat(&subject, req.compatibility);
    Json(CompatibilityConfig {
        compatibility: req.compatibility,
    })
}

// ─── Helpers ──────────────────────────────────────────

fn schema_error(status: StatusCode, message: &str) -> (StatusCode, Json<ErrorResponse>) {
    (
        status,
        Json(ErrorResponse {
            error_code: status.as_u16(),
            message: message.to_string(),
        }),
    )
}
