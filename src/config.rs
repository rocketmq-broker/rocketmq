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
// File: config.rs
// Description: Global configuration defaults, system environment variable parsing, and options.

//! Centralized broker configuration constants.
//!
//! All tunable defaults live here to avoid magic numbers scattered
//! throughout the codebase. Changing a value here affects the entire broker.

use std::time::Duration;

// ─── Network ───────────────────────────────────────────

pub const AMQP_LISTEN_ADDR: &str = "127.0.0.1:5672";

pub const AMQPS_LISTEN_ADDR: &str = "127.0.0.1:5671";

// ─── TLS ───────────────────────────────────────────────

/// Gets the file path for the SSL/TLS certificate.
///
/// Gets the file path for the SSL/TLS certificate.
///
/// # Returns
///
/// * `String` - The evaluated outcome or operation handle.
pub fn get_tls_cert_path() -> String {
    format!("{}/tls/server.pem", get_data_dir())
}

/// Gets the file path for the SSL/TLS private key.
///
/// Gets the file path for the SSL/TLS private key.
///
/// # Returns
///
/// * `String` - The evaluated outcome or operation handle.
pub fn get_tls_key_path() -> String {
    format!("{}/tls/server.key", get_data_dir())
}

// ─── Management HTTP API ──────────────────────────────

pub const MANAGEMENT_LISTEN_ADDR: &str = "127.0.0.1:15672";

// ─── AMQP Delivery Pipeline ───────────────────────────

pub const DELIVERY_CHANNEL_CAPACITY: usize = 256;

pub const DELIVERY_POLL_INTERVAL: Duration = Duration::from_millis(5);

// ─── Background Tasks ─────────────────────────────────

pub const QUEUE_TTL_CHECK_INTERVAL: Duration = Duration::from_secs(1);

pub const MESSAGE_TTL_CHECK_INTERVAL: Duration = Duration::from_millis(500);

pub const DEDUP_EVICTION_INTERVAL: Duration = Duration::from_secs(10);

pub const DEDUP_WINDOW: Duration = Duration::from_secs(300);

pub const DELAY_FLUSH_INTERVAL: Duration = Duration::from_millis(100);

// ─── Persistence ───────────────────────────────────────

/// Executes the standard get data dir lifecycle step.
///
/// Executes the required business logic for get data dir.
///
/// # Returns
///
/// * `String` - The evaluated outcome or operation handle.
pub fn get_data_dir() -> String {
    std::env::var("ROCKETMQ_DATA_DIR").unwrap_or_else(|_| "data".to_string())
}

/// Executes the standard get wal path lifecycle step.
///
/// Executes the required business logic for get wal path.
///
/// # Returns
///
/// * `String` - The evaluated outcome or operation handle.
pub fn get_wal_path() -> String {
    format!("{}/broker.wal", get_data_dir())
}

/// Executes the standard get user db path lifecycle step.
///
/// Executes the required business logic for get user db path.
///
/// # Returns
///
/// * `String` - The evaluated outcome or operation handle.
pub fn get_user_db_path() -> String {
    format!("{}/users.json", get_data_dir())
}

pub const WAL_COMPACT_INTERVAL: Duration = Duration::from_secs(60);

pub const WAL_COMPACT_THRESHOLD: u64 = 1000;

// ─── Authentication ────────────────────────────────────

pub const BCRYPT_COST: u32 = 10;

pub const DEFAULT_GUEST_USER: &str = "guest";

pub const DEFAULT_GUEST_PASS: &str = "guest";

pub const DEFAULT_ADMIN_USER: &str = "admin";

pub const DEFAULT_ADMIN_PASS: &str = "1234";

// ─── Logging ───────────────────────────────────────────

pub const DEFAULT_LOG_FILTER: &str = "rocketmq=info";

// ─── AMQP Connection ──────────────────────────────────

pub const FALLBACK_HEARTBEAT_SECS: u64 = 60;

// ─── Clustering ────────────────────────────────────────

/// Retrieves the unique identifier of this node from configuration or system state.
///
/// Retrieves the unique identifier of this node from configuration or system state.
///
/// # Returns
///
/// * `u64` - The evaluated outcome or operation handle.
pub fn get_node_id() -> u64 {
    std::env::var("ROCKETMQ_NODE_ID")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(1)
}

/// Retrieves the cluster communication address from the configuration.
///
/// Retrieves the cluster communication address from the configuration.
///
/// # Returns
///
/// * `String` - The evaluated outcome or operation handle.
pub fn get_cluster_addr() -> String {
    std::env::var("ROCKETMQ_CLUSTER_ADDR").unwrap_or_else(|_| "127.0.0.1:5680".to_string())
}

/// Retrieves the list of seed nodes for joining the cluster.
///
/// Retrieves the list of seed nodes for joining the cluster.
///
/// # Returns
///
/// * `Vec<String>` - The evaluated outcome or operation handle.
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

/// Gets the local TCP socket address to bind the AMQP listener.
///
/// Gets the local TCP socket address to bind the AMQP listener.
///
/// # Returns
///
/// * `String` - The evaluated outcome or operation handle.
pub fn get_amqp_listen_addr() -> String {
    let host = std::env::var("ROCKETMQ_BIND_HOST").unwrap_or_else(|_| "127.0.0.1".to_string());
    if let Ok(port) = std::env::var("ROCKETMQ_AMQP_PORT") {
        format!("{}:{}", host, port)
    } else {
        format!("{}:5672", host)
    }
}

/// Gets the local TCP socket address to bind the AMQPS (TLS) listener.
///
/// Gets the local TCP socket address to bind the AMQPS (TLS) listener.
///
/// # Returns
///
/// * `String` - The evaluated outcome or operation handle.
pub fn get_amqps_listen_addr() -> String {
    let host = std::env::var("ROCKETMQ_BIND_HOST").unwrap_or_else(|_| "127.0.0.1".to_string());
    if let Ok(port) = std::env::var("ROCKETMQ_AMQPS_PORT") {
        format!("{}:{}", host, port)
    } else {
        format!("{}:5671", host)
    }
}

/// Executes the standard get mgmt listen addr lifecycle step.
///
/// Executes the required business logic for get mgmt listen addr.
///
/// # Returns
///
/// * `String` - The evaluated outcome or operation handle.
pub fn get_mgmt_listen_addr() -> String {
    let host = std::env::var("ROCKETMQ_BIND_HOST").unwrap_or_else(|_| "127.0.0.1".to_string());
    if let Ok(port) = std::env::var("ROCKETMQ_MGMT_PORT") {
        format!("{}:{}", host, port)
    } else {
        format!("{}:15672", host)
    }
}
