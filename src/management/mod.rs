//! Management HTTP API — RabbitMQ-compatible REST endpoints on port 15672.
//!
//! Provides runtime introspection and administration.

use std::sync::Arc;

use axum::Router;
use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::{delete, get, post, put};
use tracing::info;

use crate::state::Broker;

pub mod handlers;
pub mod types;

use handlers::*;

/// Spawn the management HTTP server on the configured port.
pub async fn serve(broker: Broker) -> Result<(), Box<dyn std::error::Error>> {
    let app = Router::new()
        // Management UI (redirect or premium placeholder)
        .route("/", get(serve_ui))
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
        // Metrics
        .route("/api/metrics", get(prometheus_metrics))
        .layer(axum::middleware::from_fn_with_state(
            broker.clone(),
            auth_middleware,
        ))
        .with_state(broker);

    let addr = crate::config::get_mgmt_listen_addr();
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    info!("Management HTTP API on http://{}", addr);
    axum::serve(listener, app).await?;
    Ok(())
}

async fn serve_ui() -> axum::response::Html<&'static str> {
    axum::response::Html(
        r#"<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <title>RocketMQ Management API</title>
    <style>
        body {
            font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
            background-color: #0d1117;
            color: #c9d1d9;
            display: flex;
            justify-content: center;
            align-items: center;
            height: 100vh;
            margin: 0;
        }
        .container {
            text-align: center;
            padding: 2rem;
            background-color: #161b22;
            border: 1px solid #30363d;
            border-radius: 12px;
            box-shadow: 0 8px 24px rgba(0,0,0,0.5);
            max-width: 500px;
        }
        h1 {
            color: #58a6ff;
            margin-top: 0;
        }
        p {
            line-height: 1.6;
        }
        a {
            color: #58a6ff;
            text-decoration: none;
        }
        a:hover {
            text-decoration: underline;
        }
        .badge {
            background-color: #238636;
            color: white;
            padding: 0.25rem 0.5rem;
            border-radius: 4px;
            font-size: 0.85rem;
            font-weight: bold;
        }
    </style>
</head>
<body>
    <div class="container">
        <h1>🚀 RocketMQ Cluster</h1>
        <p>The backend management REST API is running successfully.</p>
        <p>Please access the <span class="badge">NextJS Dashboard</span> at:</p>
        <h2><a href="http://localhost:3000" target="_blank">http://localhost:3000</a></h2>
    </div>
</body>
</html>"#,
    )
}

async fn auth_middleware(
    State(broker): State<Broker>,
    req: axum::http::Request<axum::body::Body>,
    next: axum::middleware::Next,
) -> Result<axum::response::Response, StatusCode> {
    let path = req.uri().path();
    // Bypass auth for metrics, health checks, and static UI routes
    if path == "/api/health"
        || path == "/api/healthcheck"
        || path == "/api/metrics"
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
