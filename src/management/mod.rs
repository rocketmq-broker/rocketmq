//! Management HTTP API — RabbitMQ-compatible REST endpoints on port 15672.
//!
//! Provides runtime introspection and administration:
//! - `/api/overview` — broker summary
//! - `/api/queues` — list/manage queues
//! - `/api/exchanges` — list exchanges
//! - `/api/connections` — list/close connections
//! - `/api/users` — user CRUD
//! - `/api/healthcheck` — k8s liveness probe
//! - `/api/metrics` — OpenTelemetry metrics (Prometheus exposition format)

use std::sync::Arc;

use axum::Router;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::Json;
use axum::routing::{delete, get, post, put};
use serde::{Deserialize, Serialize};
use tracing::info;

use crate::state::{Broker, BrokerState};

/// Spawn the management HTTP server on the configured port.
pub async fn serve(broker: Broker) -> Result<(), Box<dyn std::error::Error>> {
    let app = Router::new()
        // Management UI
        .route("/", get(serve_ui))
        // Health & Overview
        .route("/api/healthcheck", get(healthcheck))
        .route("/api/overview", get(overview))
        // Queues
        .route("/api/queues", get(list_queues))
        .route("/api/queues/{name}", get(get_queue))
        .route("/api/queues/{name}", delete(delete_queue))
        .route("/api/queues/{name}/purge", delete(purge_queue))
        .route("/api/queues/{name}/get", post(get_messages))
        // Exchanges
        .route("/api/exchanges", get(list_exchanges))
        // Bindings
        .route("/api/bindings", get(list_bindings))
        // Publish
        .route("/api/exchanges/{name}/publish", post(publish_message))
        // Connections
        .route("/api/connections", get(list_connections))
        .route("/api/connections/{id}", delete(close_connection))
        // Users
        .route("/api/users", get(list_users))
        .route("/api/users", post(add_user))
        .route("/api/users/{name}", delete(delete_user))
        .route("/api/users/{name}/password", put(change_password))
        // Permissions
        .route("/api/permissions", get(list_permissions))
        .route("/api/permissions/{user}/{vhost}", put(set_permissions))
        // Metrics
        .route("/api/metrics", get(prometheus_metrics))
        .with_state(broker);

    let addr = crate::config::get_mgmt_listen_addr();
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    info!("Management HTTP API on http://{}", addr);
    axum::serve(listener, app).await?;
    Ok(())
}

// ─── DTOs ────────────────────────────────────────────

#[derive(Serialize)]
struct OverviewResponse {
    node: String,
    version: String,
    amqp_listeners: Vec<ListenerInfo>,
    queue_count: usize,
    connection_count: usize,
    exchange_count: usize,
    message_count: usize,
}

#[derive(Serialize)]
struct ListenerInfo {
    protocol: String,
    address: String,
}

#[derive(Serialize)]
struct QueueInfo {
    name: String,
    messages: usize,
    consumers: usize,
    durable: bool,
    exclusive: bool,
    auto_delete: bool,
    inflight: usize,
}

#[derive(Serialize)]
struct ExchangeInfo {
    name: String,
    #[serde(rename = "type")]
    kind: String,
    durable: bool,
    bindings: usize,
}

#[derive(Serialize)]
struct ConnectionInfo {
    id: u64,
    peer_address: String,
    user: String,
    channels: usize,
    connected: bool,
}

#[derive(Serialize)]
struct UserInfo {
    name: String,
    tags: Vec<String>,
}

#[derive(Deserialize)]
struct CreateUserRequest {
    username: String,
    password: String,
    #[serde(default)]
    tags: Vec<String>,
}

#[derive(Deserialize)]
struct ChangePasswordRequest {
    password: String,
}

#[derive(Serialize)]
struct PermissionInfo {
    user: String,
    vhost: String,
    configure: String,
    write: String,
    read: String,
}

#[derive(Deserialize)]
struct SetPermissionRequest {
    configure: String,
    write: String,
    read: String,
}

#[derive(Deserialize)]
struct PublishRequest {
    routing_key: String,
    payload: String,
    #[serde(default)]
    properties: PublishProperties,
}

#[derive(Deserialize, Default)]
struct PublishProperties {
    #[serde(default)]
    delivery_mode: Option<u8>,
    #[serde(default)]
    content_type: Option<String>,
}

#[derive(Deserialize)]
struct GetMessagesRequest {
    #[serde(default = "default_count")]
    count: usize,
    #[serde(default = "default_ack_mode")]
    ack_mode: String,
}

fn default_count() -> usize {
    1
}
fn default_ack_mode() -> String {
    "ack_requeue_false".into()
}

#[derive(Serialize)]
struct MessagePayload {
    payload: String,
    payload_bytes: usize,
    routing_key: String,
    exchange: String,
    message_count: usize,
}

#[derive(Serialize)]
struct BindingInfo {
    source: String,
    destination: String,
    routing_key: String,
    destination_type: String,
}

// ─── Handlers ────────────────────────────────────────

async fn healthcheck() -> StatusCode {
    StatusCode::OK
}

async fn overview(State(broker): State<Broker>) -> Json<OverviewResponse> {
    let queue_count = broker.queues.len();
    let connection_count = broker.connections.len();
    let exchange_count = broker.exchanges.read().await.len();
    let mut message_count = 0usize;
    for entry in broker.queues.iter() {
        message_count += entry.value().messages.len();
    }

    Json(OverviewResponse {
        node: "rocketmq@localhost".into(),
        version: env!("CARGO_PKG_VERSION").into(),
        amqp_listeners: vec![
            ListenerInfo {
                protocol: "amqp".into(),
                address: crate::config::AMQP_LISTEN_ADDR.into(),
            },
            ListenerInfo {
                protocol: "amqps".into(),
                address: crate::config::AMQPS_LISTEN_ADDR.into(),
            },
        ],
        queue_count,
        connection_count,
        exchange_count,
        message_count,
    })
}

async fn list_queues(State(broker): State<Broker>) -> Json<Vec<QueueInfo>> {
    let queues: Vec<QueueInfo> = broker
        .queues
        .iter()
        .map(|entry| {
            let (name, q) = entry.pair();
            QueueInfo {
                name: name.clone(),
                messages: q.messages.len(),
                consumers: q.consumer_tags.len(),
                durable: q.options.durable,
                exclusive: q.options.exclusive,
                auto_delete: q.options.auto_delete,
                inflight: q.inflight.len(),
            }
        })
        .collect();
    Json(queues)
}

async fn get_queue(
    State(broker): State<Broker>,
    Path(name): Path<String>,
) -> Result<Json<QueueInfo>, StatusCode> {
    match broker.queues.get(&name) {
        Some(entry) => {
            let q = entry.value();
            Ok(Json(QueueInfo {
                name: name.clone(),
                messages: q.messages.len(),
                consumers: q.consumer_tags.len(),
                durable: q.options.durable,
                exclusive: q.options.exclusive,
                auto_delete: q.options.auto_delete,
                inflight: q.inflight.len(),
            }))
        }
        None => Err(StatusCode::NOT_FOUND),
    }
}

async fn delete_queue(State(broker): State<Broker>, Path(name): Path<String>) -> StatusCode {
    if broker.queues.remove(&name).is_some() {
        info!(queue = name.as_str(), "queue deleted via management API");
        StatusCode::NO_CONTENT
    } else {
        StatusCode::NOT_FOUND
    }
}

async fn purge_queue(
    State(broker): State<Broker>,
    Path(name): Path<String>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    match broker.queues.get_mut(&name) {
        Some(mut entry) => {
            let count = entry.value_mut().messages.len();
            entry.value_mut().messages.clear();
            info!(
                queue = name.as_str(),
                purged = count,
                "queue purged via management API"
            );
            Ok(Json(serde_json::json!({ "messages_purged": count })))
        }
        None => Err(StatusCode::NOT_FOUND),
    }
}

async fn get_messages(
    State(broker): State<Broker>,
    Path(name): Path<String>,
    Json(req): Json<GetMessagesRequest>,
) -> Result<Json<Vec<MessagePayload>>, StatusCode> {
    match broker.queues.get_mut(&name) {
        Some(mut entry) => {
            let queue = entry.value_mut();
            let remaining = queue.messages.len();
            let count = req.count.min(remaining);
            let mut result = Vec::with_capacity(count);
            let requeue = req.ack_mode == "ack_requeue_true";

            for _ in 0..count {
                if let Some(q_msg) = queue.messages.pop_front()
                    && let Ok(msg) = q_msg.resolve(broker.wal().expect("WAL must be initialized"))
                {
                    result.push(MessagePayload {
                        payload: String::from_utf8_lossy(&msg.body).to_string(),
                        payload_bytes: msg.body.len(),
                        routing_key: String::new(),
                        exchange: String::new(),
                        message_count: queue.messages.len(),
                    });
                    if requeue {
                        let requeue_msg = crate::queue::message::QueueMessage::Full(msg);
                        queue.messages.push_back(requeue_msg);
                    }
                }
            }
            Ok(Json(result))
        }
        None => Err(StatusCode::NOT_FOUND),
    }
}

async fn publish_message(
    State(broker): State<Broker>,
    Path(exchange_name): Path<String>,
    Json(req): Json<PublishRequest>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let msg_id = broker.alloc_msg_id();

    // Route message
    let target_queues: Vec<String> = {
        let exchanges = broker.exchanges.read().await;
        let exchange_name_resolved = if exchange_name.is_empty() {
            ""
        } else {
            &exchange_name
        };
        match exchanges.get(exchange_name_resolved) {
            Some(ex) => ex.route(&req.routing_key, &std::collections::HashMap::new()),
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
            Vec::new(),
            req.payload.as_bytes().to_vec(),
            exchange_name.clone(),
            req.routing_key.clone(),
        );
        if let Some(mut entry) = broker.queues.get_mut(queue_name) {
            entry
                .value_mut()
                .messages
                .push_back(crate::queue::message::QueueMessage::Full(msg));
            crate::metrics::record_published(queue_name);
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

async fn list_exchanges(State(broker): State<Broker>) -> Json<Vec<ExchangeInfo>> {
    let exchanges = broker.exchanges.read().await;
    let list: Vec<ExchangeInfo> = exchanges
        .iter()
        .map(|(name, ex)| ExchangeInfo {
            name: name.clone(),
            kind: ex.kind.as_str().to_string(),
            durable: ex.durable,
            bindings: ex.bindings.len(),
        })
        .collect();
    Json(list)
}

async fn list_bindings(State(broker): State<Broker>) -> Json<Vec<BindingInfo>> {
    let exchanges = broker.exchanges.read().await;
    let mut bindings = Vec::new();
    for (name, ex) in exchanges.iter() {
        for b in &ex.bindings {
            bindings.push(BindingInfo {
                source: name.clone(),
                destination: b.queue_name.clone(),
                routing_key: b.routing_key.clone(),
                destination_type: "queue".into(),
            });
        }
    }
    Json(bindings)
}

async fn serve_ui() -> axum::response::Html<&'static str> {
    axum::response::Html(include_str!("ui.html"))
}

async fn list_connections(State(broker): State<Broker>) -> Json<Vec<ConnectionInfo>> {
    let conns: Vec<ConnectionInfo> = broker
        .connections
        .iter()
        .map(|entry| {
            let handle = entry.value();
            let (user, channels) = broker
                .conn_state
                .get(&handle.id)
                .map(|cs| (cs.username.clone(), cs.channels.len()))
                .unwrap_or_default();
            ConnectionInfo {
                id: handle.id,
                peer_address: handle.addr.to_string(),
                user,
                channels,
                connected: true,
            }
        })
        .collect();
    Json(conns)
}

async fn close_connection(State(broker): State<Broker>, Path(id): Path<u64>) -> StatusCode {
    if broker.connections.remove(&id).is_some() {
        broker.conn_state.remove(&id);
        info!(conn_id = id, "connection force-closed via management API");
        StatusCode::NO_CONTENT
    } else {
        StatusCode::NOT_FOUND
    }
}

async fn list_users(State(broker): State<Broker>) -> Json<Vec<UserInfo>> {
    let users: Vec<UserInfo> = broker
        .auth
        .list_users()
        .into_iter()
        .map(|(name, tags)| UserInfo {
            name,
            tags: tags
                .iter()
                .map(|t| format!("{:?}", t).to_lowercase())
                .collect(),
        })
        .collect();
    Json(users)
}

async fn add_user(
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

async fn delete_user(
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

async fn change_password(
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

async fn list_permissions(State(broker): State<Broker>) -> Json<Vec<PermissionInfo>> {
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

async fn set_permissions(
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

// ─── OpenTelemetry Metrics Endpoint ──────────────────

async fn prometheus_metrics(State(broker): State<Broker>) -> String {
    let s = crate::metrics::get_snapshot();
    let mut out = String::with_capacity(4096);

    // ── OTel Counters (monotonic) ────────────────────
    write_counter(
        &mut out,
        "amqp_messages_published_total",
        "Total messages published",
        s.messages_published
            .load(std::sync::atomic::Ordering::Relaxed),
    );
    write_counter(
        &mut out,
        "amqp_messages_delivered_total",
        "Total messages delivered to consumers",
        s.messages_delivered
            .load(std::sync::atomic::Ordering::Relaxed),
    );
    write_counter(
        &mut out,
        "amqp_messages_acked_total",
        "Total consumer acknowledgements",
        s.messages_acked.load(std::sync::atomic::Ordering::Relaxed),
    );
    write_counter(
        &mut out,
        "amqp_messages_nacked_total",
        "Total consumer negative-acks",
        s.messages_nacked.load(std::sync::atomic::Ordering::Relaxed),
    );
    write_counter(
        &mut out,
        "amqp_connections_opened_total",
        "Total connections accepted",
        s.connections_opened
            .load(std::sync::atomic::Ordering::Relaxed),
    );
    write_counter(
        &mut out,
        "amqp_connections_closed_total",
        "Total connections closed",
        s.connections_closed
            .load(std::sync::atomic::Ordering::Relaxed),
    );
    write_counter(
        &mut out,
        "amqp_channels_opened_total",
        "Total channels opened",
        s.channels_opened.load(std::sync::atomic::Ordering::Relaxed),
    );
    write_counter(
        &mut out,
        "amqp_channels_closed_total",
        "Total channels closed",
        s.channels_closed.load(std::sync::atomic::Ordering::Relaxed),
    );

    // ── Live Gauges (point-in-time) ──────────────────
    write_gauge(
        &mut out,
        "amqp_connections",
        "Current open connections",
        broker.connections.len() as u64,
    );
    write_gauge(
        &mut out,
        "amqp_queues",
        "Total queue count",
        broker.queues.len() as u64,
    );

    let exchange_count = broker.exchanges.read().await.len();
    write_gauge(
        &mut out,
        "amqp_exchanges",
        "Total exchange count",
        exchange_count as u64,
    );

    // Per-queue metrics
    let mut total_messages = 0u64;
    let mut total_consumers = 0u64;
    out.push_str("# HELP amqp_queue_messages Messages ready in queue\n");
    out.push_str("# TYPE amqp_queue_messages gauge\n");
    for entry in broker.queues.iter() {
        let (name, q) = entry.pair();
        let msgs = q.messages.len() as u64;
        let consumers = q.consumer_tags.len() as u64;
        total_messages += msgs;
        total_consumers += consumers;
        out.push_str(&format!(
            "amqp_queue_messages{{queue=\"{}\"}} {}\n",
            name, msgs
        ));
    }
    out.push('\n');

    out.push_str("# HELP amqp_queue_consumers Consumers on queue\n");
    out.push_str("# TYPE amqp_queue_consumers gauge\n");
    for entry in broker.queues.iter() {
        let (name, q) = entry.pair();
        out.push_str(&format!(
            "amqp_queue_consumers{{queue=\"{}\"}} {}\n",
            name,
            q.consumer_tags.len()
        ));
    }
    out.push('\n');

    write_gauge(
        &mut out,
        "amqp_messages_total",
        "Total messages across all queues",
        total_messages,
    );
    write_gauge(
        &mut out,
        "amqp_consumers_total",
        "Total consumers across all queues",
        total_consumers,
    );

    out
}

fn write_counter(out: &mut String, name: &str, help: &str, value: u64) {
    out.push_str(&format!(
        "# HELP {} {}\n# TYPE {} counter\n{} {}\n\n",
        name, help, name, name, value
    ));
}

fn write_gauge(out: &mut String, name: &str, help: &str, value: u64) {
    out.push_str(&format!(
        "# HELP {} {}\n# TYPE {} gauge\n{} {}\n\n",
        name, help, name, name, value
    ));
}

// ─── Helpers ─────────────────────────────────────────

fn save_users(broker: &Arc<BrokerState>) {
    let path = std::path::Path::new(crate::config::USER_DB_PATH);
    if let Err(e) = broker.auth.save_to_file(path) {
        tracing::warn!(error = %e, "failed to persist user database");
    }
}

fn parse_user_tags(tags: &[String]) -> Vec<crate::auth::credentials::UserTag> {
    tags.iter()
        .filter_map(|t| match t.as_str() {
            "administrator" => Some(crate::auth::credentials::UserTag::Administrator),
            "monitoring" => Some(crate::auth::credentials::UserTag::Monitoring),
            "management" => Some(crate::auth::credentials::UserTag::Management),
            _ => None,
        })
        .collect()
}
