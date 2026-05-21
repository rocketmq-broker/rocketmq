use serde::{Deserialize, Serialize};
use std::sync::Mutex;
use std::sync::OnceLock;
use std::time::Instant;

// ─── Telemetry Rates State ─────────────────────────────

pub struct RateState {
    pub last_time: Instant,
    pub last_publish: u64,
    pub last_deliver: u64,
    pub last_ack: u64,
    pub publish_rate: f64,
    pub deliver_rate: f64,
    pub ack_rate: f64,
}

pub static RATE_STATE: OnceLock<Mutex<RateState>> = OnceLock::new();

pub fn get_rates() -> (u64, f64, u64, f64, u64, f64) {
    let state_mutex = RATE_STATE.get_or_init(|| {
        Mutex::new(RateState {
            last_time: Instant::now(),
            last_publish: 0,
            last_deliver: 0,
            last_ack: 0,
            publish_rate: 0.0,
            deliver_rate: 0.0,
            ack_rate: 0.0,
        })
    });

    let mut state = state_mutex.lock().unwrap();
    let now = Instant::now();
    let elapsed = now.duration_since(state.last_time).as_secs_f64();

    let snapshot = crate::metrics::get_snapshot();
    let current_publish = snapshot
        .messages_published
        .load(std::sync::atomic::Ordering::Relaxed);
    let current_deliver = snapshot
        .messages_delivered
        .load(std::sync::atomic::Ordering::Relaxed);
    let current_ack = snapshot
        .messages_acked
        .load(std::sync::atomic::Ordering::Relaxed);

    if elapsed >= 0.5 {
        state.publish_rate = (current_publish.saturating_sub(state.last_publish) as f64) / elapsed;
        state.deliver_rate = (current_deliver.saturating_sub(state.last_deliver) as f64) / elapsed;
        state.ack_rate = (current_ack.saturating_sub(state.last_ack) as f64) / elapsed;

        state.last_time = now;
        state.last_publish = current_publish;
        state.last_deliver = current_deliver;
        state.last_ack = current_ack;
    }

    (
        current_publish,
        state.publish_rate,
        current_deliver,
        state.deliver_rate,
        current_ack,
        state.ack_rate,
    )
}

#[derive(Serialize, Clone)]
pub struct RateDetails {
    pub rate: f64,
}

#[derive(Serialize, Clone)]
pub struct MessageStats {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub publish: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub publish_details: Option<RateDetails>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub deliver_get: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub deliver_get_details: Option<RateDetails>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ack: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ack_details: Option<RateDetails>,
}

#[derive(Serialize)]
pub struct OverviewResponse {
    pub cluster_name: String,
    pub node: String,
    pub rabbitmq_version: String,
    pub management_version: String,
    pub erlang_version: String,
    pub product_name: String,
    pub product_version: String,
    pub rates_mode: String,
    pub object_totals: ObjectTotals,
    pub queue_totals: QueueTotals,
    pub listeners: Vec<ListenerInfo>,
    pub exchange_types: Vec<ExchangeTypeInfo>,
    pub message_stats: MessageStats,
}

#[derive(Serialize)]
pub struct ObjectTotals {
    pub queues: usize,
    pub exchanges: usize,
    pub connections: usize,
    pub channels: usize,
    pub consumers: usize,
}

#[derive(Serialize)]
pub struct QueueTotals {
    pub messages: usize,
    pub messages_ready: usize,
    pub messages_unacknowledged: usize,
}

#[derive(Serialize)]
pub struct ListenerInfo {
    pub node: String,
    pub protocol: String,
    pub ip_address: String,
    pub port: u16,
}

#[derive(Serialize)]
pub struct ExchangeTypeInfo {
    pub name: String,
    pub description: String,
    pub enabled: bool,
}

#[derive(Serialize)]
pub struct NodeInfo {
    pub name: String,
    pub running: bool,
    #[serde(rename = "type")]
    pub node_type: String,
    pub mem_used: u64,
    pub mem_limit: u64,
    pub mem_alarm: bool,
    pub disk_free: u64,
    pub disk_free_limit: u64,
    pub disk_free_alarm: bool,
    pub fd_used: u64,
    pub fd_total: u64,
    pub sockets_used: u64,
    pub sockets_total: u64,
    pub uptime: u64,
    pub processors: usize,
    pub os_pid: String,
}

#[derive(Serialize)]
pub struct VHostInfo {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    pub messages: usize,
    pub messages_ready: usize,
    pub messages_unacknowledged: usize,
}

#[derive(Serialize)]
pub struct HealthResponse {
    pub status: String,
}

#[derive(Serialize)]
pub struct QueueInfo {
    pub name: String,
    pub vhost: String,
    #[serde(rename = "type")]
    pub queue_type: String,
    pub durable: bool,
    pub exclusive: bool,
    pub auto_delete: bool,
    pub messages: usize,
    pub messages_ready: usize,
    pub messages_unacknowledged: usize,
    pub consumers: usize,
    pub state: String,
    pub node: String,
    pub message_stats: MessageStats,
}

#[derive(Serialize)]
pub struct ExchangeInfo {
    pub name: String,
    pub vhost: String,
    #[serde(rename = "type")]
    pub kind: String,
    pub durable: bool,
    pub auto_delete: bool,
    pub internal: bool,
    pub arguments: serde_json::Value,
    pub message_stats: MessageStats,
}

#[derive(Serialize)]
pub struct ConnectionInfo {
    pub name: String,
    pub node: String,
    pub peer_host: String,
    pub peer_port: u16,
    pub user: String,
    pub vhost: String,
    pub channels: usize,
    pub state: String,
    #[serde(rename = "type")]
    pub conn_type: String,
    pub protocol: String,
    pub ssl: bool,
}

#[derive(Serialize)]
pub struct UserInfo {
    pub name: String,
    pub tags: String,
}

#[derive(Deserialize)]
pub struct CreateUserRequest {
    pub username: String,
    pub password: String,
    #[serde(default)]
    pub tags: Vec<String>,
}

#[derive(Deserialize)]
pub struct ChangePasswordRequest {
    pub password: String,
}

#[derive(Serialize)]
pub struct PermissionInfo {
    pub user: String,
    pub vhost: String,
    pub configure: String,
    pub write: String,
    pub read: String,
}

#[derive(Deserialize)]
pub struct SetPermissionRequest {
    pub configure: String,
    pub write: String,
    pub read: String,
}

#[derive(Deserialize)]
pub struct PublishRequest {
    pub routing_key: String,
    pub payload: String,
    #[serde(default)]
    pub properties: PublishProperties,
}

#[derive(Deserialize, Default)]
pub struct PublishProperties {
    #[serde(default)]
    pub delivery_mode: Option<u8>,
    #[serde(default)]
    pub content_type: Option<String>,
}

#[derive(Deserialize)]
pub struct GetMessagesRequest {
    #[serde(default = "default_count")]
    pub count: usize,
    #[serde(default = "default_ack_mode")]
    pub ack_mode: String,
}

pub fn default_count() -> usize {
    1
}
pub fn default_ack_mode() -> String {
    "ack_requeue_false".into()
}

#[derive(Serialize)]
pub struct MessagePayload {
    pub payload: String,
    pub payload_bytes: usize,
    pub routing_key: String,
    pub exchange: String,
    pub message_count: usize,
}

#[derive(Serialize)]
pub struct BindingInfo {
    pub source: String,
    pub vhost: String,
    pub destination: String,
    pub destination_type: String,
    pub routing_key: String,
    pub arguments: serde_json::Value,
    pub properties_key: String,
}

#[derive(Serialize)]
pub struct ChannelInfo {
    pub name: String,
    pub node: String,
    pub number: u16,
    pub connection_details: serde_json::Value,
    pub vhost: String,
    pub user: String,
    pub prefetch_count: u16,
    pub consumer_count: usize,
    pub messages_unacknowledged: u16,
    pub confirm: bool,
    pub state: String,
}

#[derive(Serialize)]
pub struct ConsumerInfo {
    pub consumer_tag: String,
    pub queue: serde_json::Value,
    pub channel_details: serde_json::Value,
    pub ack_required: bool,
    pub exclusive: bool,
    pub active: bool,
}

#[derive(Serialize)]
pub struct FeatureFlagInfo {
    pub name: String,
    pub state: String,
    pub stability: String,
    pub desc: String,
}

#[derive(Deserialize)]
pub struct ClusterNameRequest {
    pub name: String,
}

#[derive(Deserialize)]
pub struct CreateQueueRequest {
    #[serde(default)]
    pub durable: bool,
    #[serde(default)]
    pub exclusive: bool,
    #[serde(default)]
    pub auto_delete: bool,
    #[serde(default)]
    pub arguments: serde_json::Value,
}

#[derive(Deserialize)]
pub struct CreateExchangeRequest {
    #[serde(rename = "type", default = "default_exchange_type")]
    pub kind: String,
    #[serde(default)]
    pub durable: bool,
    #[serde(default)]
    pub auto_delete: bool,
    #[serde(default)]
    pub internal: bool,
    #[serde(default)]
    pub arguments: serde_json::Value,
}

pub fn default_exchange_type() -> String {
    "direct".into()
}

#[derive(Deserialize)]
pub struct CreateBindingRequest {
    #[serde(default)]
    pub routing_key: String,
    #[serde(default)]
    pub arguments: serde_json::Value,
}

#[derive(Deserialize)]
pub struct BulkDeleteRequest {
    pub users: Vec<String>,
}
