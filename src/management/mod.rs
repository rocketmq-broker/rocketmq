// Copyright (c) 2026 Edilson Pateguana
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// Author: Edilson Pateguana
// Year: 2026
// File: mod.rs
// Description: Management HTTP server module initialization and routing.

//! Management HTTP API — RabbitMQ-compatible REST endpoints on port 15672.
//!
//! Provides runtime introspection and administration.


use axum::Router;
use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::{delete, get, post, put};
use tower_http::services::ServeDir;
use tracing::info;

use crate::state::Broker;

pub mod handlers;
pub mod types;

use handlers::*;

/// Executes the standard serve lifecycle step.
///
/// Executes the required business logic for serve.
///
/// # Arguments
///
/// * `broker` - `Broker`: Thread-safe pointer to the global shared broker storage & state.
///
/// # Returns
///
/// * `Result<(), Box<dyn std::error::Error>>` - A standard rust Result wrapping the status payloads or server failure codes.
pub async fn serve(broker: Broker) -> Result<(), Box<dyn std::error::Error>> {
    let www_dir = crate::config::get_www_dir();

    let app = Router::new()
        // Health & Overview
        .route("/api/healthcheck", get(healthcheck))
        .route("/api/overview", get(overview))
        .route(
            "/api/cluster-name",
            get(get_cluster_name).put(set_cluster_name),
        )
        // Health checks
        .route("/api/health/checks/alarms", get(health_alarms))
        .route("/api/health/checks/local-alarms", get(health_alarms))
        .route("/api/health/checks/virtual-hosts", get(health_alarms))
        .route(
            "/api/health/checks/node-is-mirror-sync-critical",
            get(health_alarms),
        )
        .route(
            "/api/health/checks/node-is-quorum-critical",
            get(health_alarms),
        )
        .route(
            "/api/health/checks/port-listener/{port}",
            get(health_port_listener),
        )
        .route(
            "/api/health/checks/protocol-listener/{protocol}",
            get(health_alarms),
        )
        .route(
            "/api/health/checks/certificate-expiration/{within}/{unit}",
            get(health_alarms),
        )
        // Nodes
        .route("/api/nodes", get(list_nodes))
        .route("/api/nodes/{name}", get(get_node))
        // VHosts
        .route("/api/vhosts", get(list_vhosts))
        .route(
            "/api/vhosts/{name}",
            get(get_vhost).put(create_vhost).delete(delete_vhost),
        )
        .route("/api/vhosts/{vhost}/permissions", get(vhost_permissions))
        .route(
            "/api/vhosts/{vhost}/topic-permissions",
            get(stub_empty_array),
        )
        .route("/api/vhosts/{vhost}/start/{node}", post(start_vhost))
        // Queues — vhost-scoped (RabbitMQ-compatible)
        .route("/api/queues", get(list_queues))
        .route("/api/queues/{vhost}", get(list_queues_vhost))
        .route(
            "/api/queues/{vhost}/{name}",
            get(get_queue_vhost)
                .put(create_queue_vhost)
                .delete(delete_queue_vhost),
        )
        .route(
            "/api/queues/{vhost}/{name}/contents",
            delete(purge_queue_vhost),
        )
        .route("/api/queues/{vhost}/{name}/get", post(get_messages_vhost))
        .route(
            "/api/queues/{vhost}/{name}/bindings",
            get(queue_bindings_vhost),
        )
        .route(
            "/api/queues/{vhost}/{name}/actions",
            post(queue_actions_vhost),
        )
        // Exchanges — vhost-scoped (RabbitMQ-compatible)
        .route("/api/exchanges", get(list_exchanges))
        .route("/api/exchanges/{vhost}", get(list_exchanges_vhost))
        .route(
            "/api/exchanges/{vhost}/{name}",
            get(get_exchange_vhost)
                .put(create_exchange_vhost)
                .delete(delete_exchange_vhost),
        )
        .route(
            "/api/exchanges/{vhost}/{name}/publish",
            post(publish_message_vhost),
        )
        .route(
            "/api/exchanges/{vhost}/{name}/bindings/source",
            get(exchange_bindings_source),
        )
        .route(
            "/api/exchanges/{vhost}/{name}/bindings/destination",
            get(exchange_bindings_dest),
        )
        // Bindings — vhost-scoped (RabbitMQ-compatible)
        .route("/api/bindings", get(list_bindings))
        .route("/api/bindings/{vhost}", get(list_bindings_vhost))
        .route(
            "/api/bindings/{vhost}/e/{source}/q/{dest}",
            post(create_binding_eq),
        )
        .route(
            "/api/bindings/{vhost}/e/{source}/q/{dest}/{pk}",
            delete(delete_binding_eq),
        )
        .route(
            "/api/bindings/{vhost}/e/{source}/e/{dest}",
            post(create_binding_ee),
        )
        .route(
            "/api/bindings/{vhost}/e/{source}/e/{dest}/{pk}",
            delete(delete_binding_ee),
        )
        // Connections
        .route("/api/connections", get(list_connections))
        .route(
            "/api/connections/{name}",
            get(get_connection).delete(close_connection),
        )
        .route("/api/connections/{name}/channels", get(connection_channels))
        // Channels
        .route("/api/channels", get(list_channels))
        .route("/api/channels/{name}", get(get_channel))
        // Consumers
        .route("/api/consumers", get(list_consumers))
        .route("/api/consumers/{vhost}", get(list_consumers_vhost))
        // Users
        .route("/api/users", get(list_users).post(add_user))
        .route("/api/users/bulk-delete", post(bulk_delete_users))
        .route(
            "/api/users/{name}",
            get(get_user).put(upsert_user).delete(delete_user),
        )
        .route("/api/users/{name}/password", put(change_password))
        .route("/api/users/{name}/permissions", get(user_permissions))
        .route("/api/users/{name}/topic-permissions", get(stub_empty_array))
        // Whoami
        .route("/api/whoami", get(whoami))
        // Permissions
        .route("/api/permissions", get(list_permissions))
        .route(
            "/api/permissions/{vhost}/{user}",
            get(get_permission)
                .put(set_permissions)
                .delete(delete_permission),
        )
        // Topic Permissions
        .route("/api/topic-permissions", get(stub_empty_array))
        // Policies
        .route("/api/policies", get(stub_empty_array))
        .route("/api/policies/{vhost}", get(stub_empty_array))
        .route(
            "/api/policies/{vhost}/{name}",
            get(stub_not_found)
                .put(stub_no_content)
                .delete(stub_no_content),
        )
        // Operator Policies
        .route("/api/operator-policies", get(stub_empty_array))
        .route("/api/operator-policies/{vhost}", get(stub_empty_array))
        .route(
            "/api/operator-policies/{vhost}/{name}",
            put(stub_no_content).delete(stub_no_content),
        )
        // Parameters
        .route("/api/parameters", get(stub_empty_array))
        .route("/api/parameters/{component}", get(stub_empty_array))
        .route("/api/parameters/{component}/{vhost}", get(stub_empty_array))
        .route(
            "/api/parameters/{component}/{vhost}/{name}",
            get(stub_not_found)
                .put(stub_no_content)
                .delete(stub_no_content),
        )
        // Global Parameters
        .route("/api/global-parameters", get(stub_empty_array))
        .route(
            "/api/global-parameters/{name}",
            get(stub_not_found)
                .put(stub_no_content)
                .delete(stub_no_content),
        )
        // Federation
        .route("/api/federation-links", get(stub_empty_array))
        .route("/api/federation-links/{vhost}", get(stub_empty_array))
        // Shovels
        .route("/api/shovels", get(stub_empty_array))
        .route("/api/shovels/{vhost}", get(stub_empty_array))
        // Feature Flags
        .route("/api/feature-flags", get(list_feature_flags))
        .route("/api/feature-flags/{name}/enable", put(stub_no_content))
        // Limits
        .route("/api/vhost-limits", get(stub_empty_array))
        .route("/api/vhost-limits/{vhost}", get(stub_empty_array))
        .route("/api/user-limits", get(stub_empty_array))
        .route("/api/user-limits/{user}", get(stub_empty_array))
        // Definitions (import/export)
        .route(
            "/api/definitions",
            get(get_definitions).post(stub_no_content),
        )
        .route(
            "/api/definitions/{vhost}",
            get(get_definitions).post(stub_no_content),
        )
        // Extensions
        .route("/api/extensions", get(stub_empty_array))
        // Auth attempts
        .route("/api/auth/attempts/{node}", get(stub_empty_array))
        .route("/api/auth/attempts/{node}/source", get(stub_empty_array))
        // Rebalance
        .route("/api/rebalance/queues", post(stub_no_content))
        // Missing Compatibility Endpoints
        .route("/api/version", get(get_version))
        .route("/api/deprecated-features", get(stub_empty_array))
        .route("/api/deprecated-features/used", get(stub_empty_array))
        .route("/api/reset", delete(stub_no_content))
        .route("/api/reset/{node}", delete(stub_no_content))
        .route("/api/nodes/{name}/memory", get(get_node_memory))
        .route(
            "/api/topic-permissions/{vhost}/{user}",
            get(stub_empty_array)
                .put(stub_no_content)
                .delete(stub_no_content),
        )
        .route(
            "/api/vhost-limits/{vhost}/{name}",
            put(stub_no_content).delete(stub_no_content),
        )
        .route(
            "/api/user-limits/{user}/{name}",
            put(stub_no_content).delete(stub_no_content),
        )
        .route("/api/connections/{name}/sessions", get(stub_empty_array))
        // Metrics
        .route("/api/metrics", get(prometheus_metrics))
        .layer(axum::middleware::from_fn_with_state(
            broker.clone(),
            auth_middleware,
        ))
        .fallback_service(ServeDir::new(&www_dir).append_index_html_on_directories(true))
        .layer(axum::middleware::map_response(
            |mut response: axum::response::Response| async move {
                response.headers_mut().insert(
                    "Permissions-Policy",
                    "unload=(self)".parse().unwrap(),
                );
                response
            },
        ))
        .with_state(broker.clone());

    let addr = crate::config::get_mgmt_listen_addr();
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    info!("Management HTTP API on http://{}", addr);

    // Spawn a background task to record time-series samples every 5 seconds
    // This ensures chart data accumulates even when nobody is viewing the dashboard
    let sample_broker = broker;
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(5));
        loop {
            interval.tick().await;
            let (pub_val, _pub_rate, del_val, _del_rate, ack_val, _ack_rate) =
                types::get_rates();

            let mut total_messages = 0u64;
            let mut total_inflight = 0u64;
            for entry in sample_broker.queues.iter() {
                let q = entry.value();
                total_messages += q.messages.len() as u64;
                total_inflight += q.inflight.len() as u64;
            }
            let total_all = total_messages + total_inflight;
            types::record_samples(
                pub_val, del_val, ack_val,
                total_all, total_messages, total_inflight,
            );
        }
    });

    axum::serve(listener, app).await?;
    Ok(())
}

async fn auth_middleware(
    State(broker): State<Broker>,
    req: axum::http::Request<axum::body::Body>,
    next: axum::middleware::Next,
) -> Result<axum::response::Response, StatusCode> {
    let path = req.uri().path();
    // Bypass auth for metrics, health checks, version, deprecated-features, and static UI routes
    if path == "/api/health"
        || path == "/api/healthcheck"
        || path == "/api/metrics"
        || path == "/api/version"
        || path == "/api/deprecated-features"
        || path == "/api/deprecated-features/used"
        || !path.starts_with("/api/")
    {
        return Ok(next.run(req).await);
    }

    let auth_header = req
        .headers()
        .get(axum::http::header::AUTHORIZATION)
        .ok_or(StatusCode::UNAUTHORIZED)?;

    let auth_str = auth_header.to_str().map_err(|_| StatusCode::UNAUTHORIZED)?;

    if !auth_str.starts_with("Basic ") {
        return Err(StatusCode::UNAUTHORIZED);
    }

    let encoded = &auth_str[6..];
    let decoded_bytes = handlers::decode_base64(encoded).ok_or(StatusCode::UNAUTHORIZED)?;
    let decoded_str = String::from_utf8(decoded_bytes).map_err(|_| StatusCode::UNAUTHORIZED)?;
    let (username, password) = decoded_str
        .split_once(':')
        .ok_or(StatusCode::UNAUTHORIZED)?;

    let peer_addr = std::net::SocketAddr::from(([127, 0, 0, 1], 0));
    broker
        .auth
        .authenticate(username, password, peer_addr)
        .map_err(|_| StatusCode::UNAUTHORIZED)?;

    Ok(next.run(req).await)
}