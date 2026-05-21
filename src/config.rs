//! Centralized broker configuration constants.
//!
//! All tunable defaults live here to avoid magic numbers scattered
//! throughout the codebase. Changing a value here affects the entire broker.

use std::time::Duration;

// ─── Network ───────────────────────────────────────────

/// Address and port the AMQP listener binds to.
pub const AMQP_LISTEN_ADDR: &str = "127.0.0.1:5672";

/// Address and port the AMQPS (TLS) listener binds to.
pub const AMQPS_LISTEN_ADDR: &str = "127.0.0.1:5671";

// ─── TLS ───────────────────────────────────────────────

/// Path to the TLS certificate PEM file.
/// Auto-generated self-signed cert if not present.
pub const TLS_CERT_PATH: &str = "data/tls/server.pem";

/// Path to the TLS private key PEM file.
pub const TLS_KEY_PATH: &str = "data/tls/server.key";

// ─── Management HTTP API ──────────────────────────────

/// Address and port for the management HTTP API (RabbitMQ-compatible).
pub const MANAGEMENT_LISTEN_ADDR: &str = "127.0.0.1:15672";

// ─── AMQP Delivery Pipeline ───────────────────────────

/// Capacity of the per-connection AMQP delivery channel.
/// Controls how many outgoing frames can be buffered before backpressure.
pub const DELIVERY_CHANNEL_CAPACITY: usize = 256;

/// How often the delivery task polls for messages to push to consumers.
pub const DELIVERY_POLL_INTERVAL: Duration = Duration::from_millis(5);

// ─── Background Tasks ─────────────────────────────────

/// How often to check queue TTL expiration (x-expires).
pub const QUEUE_TTL_CHECK_INTERVAL: Duration = Duration::from_secs(1);

/// How often to run message TTL expiration sweeps.
pub const MESSAGE_TTL_CHECK_INTERVAL: Duration = Duration::from_millis(500);

/// How often to evict stale entries from the dedup cache.
pub const DEDUP_EVICTION_INTERVAL: Duration = Duration::from_secs(10);

/// How long a dedup entry is kept before eviction (5 minutes).
pub const DEDUP_WINDOW: Duration = Duration::from_secs(300);

/// How often to flush delayed messages that are ready for delivery.
pub const DELAY_FLUSH_INTERVAL: Duration = Duration::from_millis(100);

// ─── Persistence ───────────────────────────────────────

/// Root data directory.
pub const DATA_DIR: &str = "data";

/// Path to the WAL file for crash recovery.
pub const WAL_PATH: &str = "data/broker.wal";

/// Path to the user/permissions database.
pub const USER_DB_PATH: &str = "data/users.json";

/// How often to check if WAL compaction is needed.
pub const WAL_COMPACT_INTERVAL: Duration = Duration::from_secs(60);

/// Minimum number of WAL entries before compaction is triggered.
pub const WAL_COMPACT_THRESHOLD: u64 = 1000;

// ─── Authentication ────────────────────────────────────

/// bcrypt cost factor. 10 is the industry-standard default:
/// fast enough for login, slow enough for brute-force resistance.
pub const BCRYPT_COST: u32 = 10;

/// Name of the built-in guest user.
pub const DEFAULT_GUEST_USER: &str = "guest";

/// Default password for the guest user.
pub const DEFAULT_GUEST_PASS: &str = "guest";

/// Name of the built-in admin user.
pub const DEFAULT_ADMIN_USER: &str = "admin";

/// Default password for the admin user.
pub const DEFAULT_ADMIN_PASS: &str = "1234";

// ─── Logging ───────────────────────────────────────────

/// Default RUST_LOG filter when RUST_LOG env var is not set.
pub const DEFAULT_LOG_FILTER: &str = "rocketmq=info";

// ─── AMQP Connection ──────────────────────────────────

/// Fallback heartbeat timeout when client doesn't negotiate one.
pub const FALLBACK_HEARTBEAT_SECS: u64 = 60;

// ─── Clustering ────────────────────────────────────────

/// Get the Node ID from the environment (ROCKETMQ_NODE_ID), default to 1.
pub fn get_node_id() -> u64 {
    std::env::var("ROCKETMQ_NODE_ID")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(1)
}

/// Get the cluster internal bind address from the environment (ROCKETMQ_CLUSTER_ADDR), default to 127.0.0.1:5680.
pub fn get_cluster_addr() -> String {
    std::env::var("ROCKETMQ_CLUSTER_ADDR").unwrap_or_else(|_| "127.0.0.1:5680".to_string())
}

/// Get seed nodes from the environment (ROCKETMQ_CLUSTER_SEEDS), default to empty list.
pub fn get_cluster_seeds() -> Vec<String> {
    std::env::var("ROCKETMQ_CLUSTER_SEEDS")
        .map(|v| {
            v.split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect()
        })
        .unwrap_or_default()
}

/// Get the AMQP bind address, honoring the environment variable (ROCKETMQ_AMQP_PORT) if present.
pub fn get_amqp_listen_addr() -> String {
    if let Ok(port) = std::env::var("ROCKETMQ_AMQP_PORT") {
        format!("127.0.0.1:{}", port)
    } else {
        AMQP_LISTEN_ADDR.to_string()
    }
}

/// Get the AMQPS bind address, honoring the environment variable (ROCKETMQ_AMQPS_PORT) if present.
pub fn get_amqps_listen_addr() -> String {
    if let Ok(port) = std::env::var("ROCKETMQ_AMQPS_PORT") {
        format!("127.0.0.1:{}", port)
    } else {
        AMQPS_LISTEN_ADDR.to_string()
    }
}

/// Get the Management HTTP bind address, honoring the environment variable (ROCKETMQ_MGMT_PORT) if present.
pub fn get_mgmt_listen_addr() -> String {
    if let Ok(port) = std::env::var("ROCKETMQ_MGMT_PORT") {
        format!("127.0.0.1:{}", port)
    } else {
        MANAGEMENT_LISTEN_ADDR.to_string()
    }
}
