// Copyright (c) 2026 Edilson Pateguana
// Licensed under the Apache License, Version 2.0
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::Json;
use tracing::info;

use crate::management::routes::helpers::*;
use crate::management::types::*;
use crate::state::Broker;

// ─── Users & Permissions ───────────────────────────────

pub async fn list_users(
    State(broker): State<Broker>,
    Query(params): Query<PaginationParams>,
) -> Json<PaginatedResponse<UserInfo>> {
    let users: Vec<UserInfo> = broker
        .auth
        .list_users()
        .into_iter()
        .map(|(name, tags)| UserInfo {
            name,
            tags: tags
                .iter()
                .map(|t| format!("{:?}", t).to_lowercase())
                .collect::<Vec<String>>()
                .join(","),
            password_hash: "********".to_string(),
            hashing_algorithm: "rabbit_password_hashing_sha256".to_string(),
        })
        .collect();
    Json(PaginatedResponse::from_vec(users, &params))
}

pub async fn add_user(
    State(broker): State<Broker>,
    Json(req): Json<CreateUserRequest>,
) -> Result<StatusCode, (StatusCode, String)> {
    let tags = parse_user_tags(&req.tags);
    broker
        .auth
        .add_user(&req.username, &req.password, tags)
        .map_err(|e| (StatusCode::CONFLICT, e))?;
    save_users(&broker);
    info!(
        user = req.username.as_str(),
        "user created via management API"
    );
    Ok(StatusCode::CREATED)
}

pub async fn delete_user(
    State(broker): State<Broker>,
    Path(name): Path<String>,
) -> Result<StatusCode, (StatusCode, String)> {
    broker
        .auth
        .delete_user(&name)
        .map_err(|e| (StatusCode::NOT_FOUND, e))?;
    save_users(&broker);
    info!(user = name.as_str(), "user deleted via management API");
    Ok(StatusCode::NO_CONTENT)
}

pub async fn change_password(
    State(broker): State<Broker>,
    Path(name): Path<String>,
    Json(req): Json<ChangePasswordRequest>,
) -> Result<StatusCode, (StatusCode, String)> {
    broker
        .auth
        .change_password(&name, &req.password)
        .map_err(|e| (StatusCode::NOT_FOUND, e))?;
    save_users(&broker);
    info!(user = name.as_str(), "password changed via management API");
    Ok(StatusCode::NO_CONTENT)
}

pub async fn get_user(
    State(broker): State<Broker>,
    Path(name): Path<String>,
) -> Result<Json<UserInfo>, StatusCode> {
    for (uname, tags) in broker.auth.list_users() {
        if uname == name {
            return Ok(Json(UserInfo {
                name: uname,
                tags: tags
                    .iter()
                    .map(|t| format!("{:?}", t).to_lowercase())
                    .collect::<Vec<String>>()
                    .join(","),
                password_hash: "********".to_string(),
                hashing_algorithm: "rabbit_password_hashing_sha256".to_string(),
            }));
        }
    }
    Err(StatusCode::NOT_FOUND)
}

pub async fn upsert_user(
    State(broker): State<Broker>,
    Path(name): Path<String>,
    Json(req): Json<serde_json::Value>,
) -> StatusCode {
    let password = req
        .get("password")
        .and_then(|v| v.as_str())
        .unwrap_or("changeme");

    let tags: Vec<String> = if let Some(arr) = req.get("tags").and_then(|v| v.as_array()) {
        arr.iter()
            .filter_map(|v| v.as_str().map(|s| s.trim().to_string()))
            .collect()
    } else {
        let tags_val = req.get("tags").and_then(|v| v.as_str()).unwrap_or("");
        tags_val
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect()
    };

    let parsed_tags = parse_user_tags(&tags);

    if broker
        .auth
        .add_user(&name, password, parsed_tags.clone())
        .is_err()
    {
        let _ = broker.auth.change_password(&name, password);
        let _ = broker.auth.set_user_tags(&name, parsed_tags);
    }

    save_users(&broker);
    StatusCode::NO_CONTENT
}

pub async fn bulk_delete_users(
    State(broker): State<Broker>,
    Json(req): Json<BulkDeleteRequest>,
) -> StatusCode {
    for name in &req.users {
        let _ = broker.auth.delete_user(name);
    }
    save_users(&broker);
    StatusCode::NO_CONTENT
}

pub async fn list_permissions(State(broker): State<Broker>) -> Json<Vec<PermissionInfo>> {
    let users = broker.auth.list_users();
    let mut perms = Vec::new();
    for (username, _) in &users {
        for p in broker.auth.list_user_permissions(username) {
            perms.push(PermissionInfo {
                user: p.username.clone(),
                vhost: p.vhost.clone(),
                configure: p.configure.clone(),
                write: p.write.clone(),
                read: p.read.clone(),
            });
        }
    }
    Json(perms)
}

pub async fn get_permission(
    State(broker): State<Broker>,
    Path((vhost, user)): Path<(String, String)>,
) -> Result<Json<PermissionInfo>, StatusCode> {
    for p in broker.auth.list_user_permissions(&user) {
        if p.vhost == vhost {
            return Ok(Json(PermissionInfo {
                user: p.username.clone(),
                vhost: p.vhost.clone(),
                configure: p.configure.clone(),
                write: p.write.clone(),
                read: p.read.clone(),
            }));
        }
    }
    Err(StatusCode::NOT_FOUND)
}

pub async fn set_permissions(
    State(broker): State<Broker>,
    Path((user, vhost)): Path<(String, String)>,
    Json(req): Json<SetPermissionRequest>,
) -> Result<StatusCode, (StatusCode, String)> {
    broker
        .auth
        .set_permissions(&user, &vhost, &req.configure, &req.write, &req.read)
        .map_err(|e| (StatusCode::NOT_FOUND, e))?;
    save_users(&broker);
    info!(
        user = user.as_str(),
        vhost = vhost.as_str(),
        "permissions set via management API"
    );
    Ok(StatusCode::NO_CONTENT)
}

pub async fn delete_permission(
    State(broker): State<Broker>,
    Path((_vhost, _user)): Path<(String, String)>,
) -> StatusCode {
    let _ = broker;
    StatusCode::NO_CONTENT
}

pub async fn user_permissions(
    State(broker): State<Broker>,
    Path(name): Path<String>,
) -> Json<Vec<PermissionInfo>> {
    let perms: Vec<PermissionInfo> = broker
        .auth
        .list_user_permissions(&name)
        .into_iter()
        .map(|p| PermissionInfo {
            user: p.username.clone(),
            vhost: p.vhost.clone(),
            configure: p.configure.clone(),
            write: p.write.clone(),
            read: p.read.clone(),
        })
        .collect();
    Json(perms)
}

pub async fn whoami(
    State(broker): State<Broker>,
    headers: axum::http::HeaderMap,
) -> Json<serde_json::Value> {
    if let Some(auth_header) = headers.get(axum::http::header::AUTHORIZATION)
        && let Ok(auth_str) = auth_header.to_str()
        && let Some(encoded) = auth_str.strip_prefix("Basic ")
        && let Some(decoded_bytes) = decode_base64(encoded)
        && let Ok(decoded_str) = String::from_utf8(decoded_bytes)
        && let Some((username, _password)) = decoded_str.split_once(':')
    {
        let user_tags: Vec<String> = broker
            .auth
            .list_users()
            .into_iter()
            .find(|(u, _)| u == username)
            .map(|(_, t)| {
                t.iter()
                    .map(|tag| format!("{:?}", tag).to_lowercase())
                    .collect::<Vec<String>>()
            })
            .unwrap_or_else(|| vec!["administrator".to_string()]);

        return Json(serde_json::json!({
            "name": username,
            "tags": user_tags,
            "is_internal_user": true,
            "login_session_timeout": null
        }));
    }
    Json(serde_json::json!({
        "name": "guest",
        "tags": ["administrator"],
        "is_internal_user": true,
        "login_session_timeout": null
    }))
}
