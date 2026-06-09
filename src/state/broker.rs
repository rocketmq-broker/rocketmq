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
// File: broker.rs
// Description: Global broker state coordination, queue registry, and connection tracking.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;
use tokio::sync::RwLock;

use dashmap::DashMap;

use crate::auth::AuthBackend;
use crate::queue::{DelayQueue, QueueState};
use crate::routing::exchange::{Binding, Exchange, create_default_exchanges};
use crate::state::vhost::{DEFAULT_VHOST, VHost};

/// Handle to a live client connection, carrying its unique ID, remote address,
/// and an MPSC sender for writing AMQP frames back to the socket.
#[derive(Clone)]
pub struct ConnHandle {
    pub id: u64,
    pub addr: SocketAddr,
    pub sink: std::sync::Arc<dyn crate::protocol::DeliverySink>,
}

/// Per-channel state tracking within an AMQP connection.
///
/// Tracks prefetch limits, unacknowledged message counts, publisher-confirm
/// mode, and flow-control status. Delivery is gated by [`can_deliver`].
pub struct ChannelState {
    pub id: u16,
    pub prefetch_count: u16,
    pub unacked_count: u16,
    pub confirm_mode: bool,
    pub next_delivery_tag: u64,
    pub flow_active: bool,
}

impl ChannelState {
    /// Creates a new instance with the given id.
    pub fn new(id: u16) -> Self {
        Self {
            id,
            prefetch_count: 0,
            unacked_count: 0,
            confirm_mode: false,
            next_delivery_tag: 1,
            flow_active: true,
        }
    }

    /// Returns `true` if this channel is allowed to receive another
    /// delivery, considering both flow-control state and prefetch limits.
    pub fn can_deliver(&self) -> bool {
        if !self.flow_active {
            return false;
        }
        self.prefetch_count == 0 || self.unacked_count < self.prefetch_count
    }
}

/// A single operation buffered inside a transaction (`Tx.Select`).
///
/// Operations are accumulated until the client issues `Tx.Commit` (applied
/// atomically) or `Tx.Rollback` (discarded).
#[derive(Clone, Debug)]
pub enum PendingOp {
    Publish {
        exchange: String,
        routing_key: String,
        headers: Vec<u8>,
        body: Vec<u8>,
    },
    Ack {
        msg_id: u64,
    },
}

/// Tracks active channels and metadata associated with a single client connection.
/// Per-connection state shared across all channels on a single TCP link.
///
/// Holds the virtual-host binding, channel map, transaction buffer, and
/// exclusive queue ownership for cleanup on disconnect.
/// Tracks active channels and metadata associated with a single client connection.
pub struct ConnectionState {
    pub channels: HashMap<u16, ChannelState>,
    pub confirm_mode: bool,
    pub next_delivery_tag: u64,
    pub vhost: String,
    pub tx_mode: bool,
    pub tx_buffer: Vec<PendingOp>,
    pub frame_max: u32,
    pub channel_max: u16,
    pub heartbeat: u16,
    pub authenticated: bool,
    pub username: String,
}

impl Default for ConnectionState {
    fn default() -> Self {
        Self::new()
    }
}

impl ConnectionState {
    /// Creates a new instance with default values.
    pub fn new() -> Self {
        Self {
            channels: HashMap::new(),
            confirm_mode: false,
            next_delivery_tag: 1,
            vhost: DEFAULT_VHOST.to_string(),
            tx_mode: false,
            tx_buffer: Vec::new(),
            frame_max: 131_072,
            channel_max: 2047,
            heartbeat: 60,
            authenticated: false,
            username: String::new(),
        }
    }
}

/// Tracks the active connections, channel registries, queues, exchanges, and clusters.
/// Global shared broker state coordinating queues, exchanges, connections,
/// and background subsystems.
///
/// All fields are concurrent-safe (`DashMap`, `RwLock`, atomics) so that
/// connection tasks can operate without holding a global lock.
/// Tracks the active connections, channel registries, queues, exchanges, and clusters.
pub struct BrokerState {
    next_conn_id: AtomicU64,
    next_msg_id: AtomicU64,
    pub exchanges: RwLock<HashMap<String, Exchange>>,
    pub queues: DashMap<String, QueueState>,
    pub connections: DashMap<u64, ConnHandle>,
    pub conn_state: DashMap<u64, Box<dyn crate::protocol::ConnectionMeta>>,
    pub conn_consumers: DashMap<u64, Vec<(String, String, u16)>>, // conn_id -> Vec<(queue_name, consumer_tag, channel_id)>
    pub conn_exclusive_queues: DashMap<u64, Vec<String>>,         // conn_id -> Vec<queue_name>
    /// Maps delivery tag -> queue name.
    pub delivery_index: DashMap<u64, Arc<str>>,
    /// Tracks which deliveries were sent to each connection (for ack/nack).
    pub conn_deliveries: DashMap<u64, Vec<(Arc<str>, u64)>>, // conn_id -> Vec<(queue_name, delivery_tag)>
    wal: Arc<crate::storage::wal::Wal>,
    pub dedup_cache: DashMap<String, Instant>,
    pub delay_queue: DelayQueue,
    pub vhosts: DashMap<String, VHost>,
    pub auth: AuthBackend,
    cluster: OnceLock<Arc<crate::cluster::ClusterCoordinator>>,
    /// Epoch ms when the broker started.
    started_at_ms: u64,
    /// OPT-2: O(1) consumer_tag → queue_name index.
    /// Populated on consume, removed on cancel/disconnect.
    /// Eliminates the O(Q) full-queue scan in cancel handler.
    pub consumer_index: DashMap<String, String>,
}

impl BrokerState {
    /// Creates a new instance with default values.
    pub fn new(wal: Arc<crate::storage::wal::Wal>) -> Self {
        let auth = AuthBackend::new();
        let db_path = crate::config::get_user_db_path();
        let user_db = std::path::Path::new(&db_path);
        if let Err(e) = auth.load_from_file(user_db) {
            tracing::warn!(error = %e, "failed to load user database, using defaults");
        }

        if let Err(e) = auth.save_to_file(user_db) {
            tracing::warn!(error = %e, "failed to save user database");
        }

        Self {
            next_conn_id: AtomicU64::new(1),
            next_msg_id: AtomicU64::new(1),
            exchanges: RwLock::new(create_default_exchanges()),
            queues: DashMap::new(),
            connections: DashMap::new(),
            conn_state: DashMap::new(),
            conn_consumers: DashMap::new(),
            conn_exclusive_queues: DashMap::new(),
            delivery_index: DashMap::new(),
            conn_deliveries: DashMap::new(),
            wal,
            dedup_cache: DashMap::new(),
            delay_queue: DelayQueue::new(),
            vhosts: {
                let map = DashMap::new();
                map.insert(
                    DEFAULT_VHOST.to_string(),
                    VHost::new(DEFAULT_VHOST.to_string()),
                );
                map
            },
            auth,
            cluster: OnceLock::new(),
            started_at_ms: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64,
            consumer_index: DashMap::new(),
        }
    }

    /// Stores the cluster manager handle for cross-node coordination.
    pub fn set_cluster(&self, cluster: Arc<crate::cluster::ClusterCoordinator>) {
        let _ = self.cluster.set(cluster);
    }

    /// Returns the broker start time as a Unix timestamp in milliseconds.
    pub fn start_time_ms(&self) -> u64 {
        self.started_at_ms
    }

    /// Lists all configured virtual hosts in the broker.
    /// Returns the list of all configured virtual host names.
    /// Lists all configured virtual hosts in the broker.
    /// Returns the list of all configured virtual host names.
    pub fn list_vhosts(&self) -> Vec<String> {
        self.vhosts.iter().map(|e| e.key().clone()).collect()
    }

    /// Returns a reference to the cluster manager, if one has been set.
    pub fn cluster(&self) -> Option<&Arc<crate::cluster::ClusterCoordinator>> {
        self.cluster.get()
    }

    /// Returns a reference to the WAL.
    pub fn wal(&self) -> &Arc<crate::storage::wal::Wal> {
        &self.wal
    }

    /// Allocates a globally unique, monotonically increasing connection ID.
    #[inline(always)]
    pub fn alloc_conn_id(&self) -> u64 {
        self.next_conn_id.fetch_add(1, Ordering::Relaxed)
    }

    /// Allocates a globally unique, monotonically increasing message ID.
    #[inline(always)]
    pub fn alloc_msg_id(&self) -> u64 {
        self.next_msg_id.fetch_add(1, Ordering::Relaxed)
    }

    pub fn remove_connection(&self, conn_id: u64) {
        self.connections.remove(&conn_id);
        self.conn_state.remove(&conn_id);

        if let Some((_, excl_queues)) = self.conn_exclusive_queues.remove(&conn_id) {
            for queue_name in excl_queues {
                self.queues.remove(&queue_name);
            }
        }

        if let Some((_, deliveries)) = self.conn_deliveries.remove(&conn_id) {
            for (queue_name, delivery_tag) in deliveries {
                self.delivery_index.remove(&delivery_tag);
                if let Some(mut q) = self.queues.get_mut(queue_name.as_ref()) {
                    if let Some(msg) = q.inflight.remove(&delivery_tag) {
                        q.messages
                            .push_front(crate::queue::message::QueueMessage::Full(msg));
                    }
                }
            }
        }

        let mut queues_to_remove = Vec::new();
        for mut entry in self.queues.iter_mut() {
            let (name, queue) = entry.pair_mut();
            queue.listeners.retain(|&(id, _)| id != conn_id);

            // Clean up consumer_index for removed consumers
            let removed_tags: Vec<String> = queue
                .consumer_tags
                .iter()
                .filter(|(_, (cid, _, _))| *cid == conn_id)
                .map(|(tag, _)| tag.clone())
                .collect();
            for tag in &removed_tags {
                self.consumer_index.remove(tag);
            }

            queue
                .consumer_tags
                .retain(|_tag, &mut (cid, _, _)| cid != conn_id);
            queue.consumer_count = queue.listeners.len();

            if queue.options.exclusive && queue.owner_conn_id == Some(conn_id) {
                queues_to_remove.push(name.clone());
            }
        }
        for name in &queues_to_remove {
            self.queues.remove(name);
        }

        let auto_delete: Vec<String> = self
            .queues
            .iter()
            .filter(|e| e.value().options.auto_delete && e.value().listeners.is_empty())
            .map(|e| e.key().clone())
            .collect();
        for name in auto_delete {
            self.queues.remove(&name);
        }
    }

    /// Creates an implicit binding from the default exchange to the named
    /// queue (AMQP requires every queue to be reachable via its own name
    /// as the routing key on the default exchange).
    pub fn auto_bind_default_exchange(&self, queue_name: &str) {
        let Ok(mut exchanges) = self.exchanges.try_write() else {
            return;
        };
        let Some(default_ex) = exchanges.get_mut("") else {
            return;
        };

        default_ex.add_binding(Binding {
            queue_name: queue_name.to_string().into(),
            routing_key: queue_name.to_string().into(),
            headers_match: None,
        });
    }

    pub fn register_exclusive_queue(&self, conn_id: u64, queue_name: &str) {
        self.conn_exclusive_queues
            .entry(conn_id)
            .or_default()
            .push(queue_name.to_string());
    }

    pub fn deregister_exclusive_queue(&self, conn_id: u64, queue_name: &str) {
        if let Some(mut qs) = self.conn_exclusive_queues.get_mut(&conn_id) {
            qs.retain(|q| q != queue_name);
        }
    }

    pub fn register_consumer(
        &self,
        conn_id: u64,
        channel_id: u16,
        queue_name: &str,
        consumer_tag: &str,
    ) {
        self.conn_consumers.entry(conn_id).or_default().push((
            queue_name.to_string(),
            consumer_tag.to_string(),
            channel_id,
        ));
        self.consumer_index
            .insert(consumer_tag.to_string(), queue_name.to_string());
    }

    pub fn deregister_consumer(&self, conn_id: u64, consumer_tag: &str) {
        if let Some(mut cons) = self.conn_consumers.get_mut(&conn_id) {
            cons.retain(|(_, tag, _)| tag != consumer_tag);
        }
        self.consumer_index.remove(consumer_tag);
    }
}

pub type Broker = Arc<BrokerState>;

#[cfg(test)]
mod tests {
    #[allow(unused_imports)]
    use super::*;
    use crate::queue::QueueOptions;
    use tokio::sync::mpsc;

    struct MockConnectionMeta {
        vhost: String,
        username: String,
        tx_mode: bool,
    }

    impl crate::protocol::ConnectionMeta for MockConnectionMeta {
        fn username(&self) -> String {
            self.username.clone()
        }
        fn vhost(&self) -> String {
            self.vhost.clone()
        }
        fn channels_count(&self) -> usize {
            0
        }
        fn get_channels(&self) -> Vec<crate::protocol::ChannelMeta> {
            vec![]
        }
        fn heartbeat(&self) -> u16 {
            0
        }
        fn frame_max(&self) -> u32 {
            0
        }
        fn channel_max(&self) -> u16 {
            0
        }
        fn tx_mode(&self) -> bool {
            self.tx_mode
        }
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }
        fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
            self
        }
    }

    /// Creates a test BrokerState with a temporary WAL file.
    fn test_broker() -> BrokerState {
        use std::sync::atomic::AtomicU32;
        static COUNTER: AtomicU32 = AtomicU32::new(0);
        let id = COUNTER.fetch_add(1, Ordering::Relaxed);
        let dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("target")
            .join("test_broker_wal");
        let _ = std::fs::create_dir_all(&dir);
        let path = dir.join(format!("test_{}.wal", id));
        let wal = Arc::new(crate::storage::wal::Wal::open(&path).unwrap());
        BrokerState::new(wal)
    }

    #[test]
    fn channel_state_prefetch_gating() {
        let mut ch = ChannelState::new(1);
        ch.prefetch_count = 2;
        assert!(ch.can_deliver());
        ch.unacked_count = 2;
        assert!(!ch.can_deliver());
    }

    #[test]
    fn channel_state_flow_control() {
        let mut ch = ChannelState::new(1);
        assert!(ch.can_deliver());

        ch.flow_active = false;
        assert!(!ch.can_deliver());

        ch.flow_active = true;
        assert!(ch.can_deliver());
    }

    #[test]
    fn channel_state_flow_overrides_prefetch() {
        let mut ch = ChannelState::new(1);
        ch.prefetch_count = 10;
        ch.unacked_count = 0;
        ch.flow_active = false;
        assert!(!ch.can_deliver());
    }

    #[test]
    fn broker_state_alloc_ids_monotonic() {
        let bs = test_broker();
        assert_eq!(bs.alloc_conn_id(), 1);
        assert_eq!(bs.alloc_conn_id(), 2);
        assert_eq!(bs.alloc_msg_id(), 1);
        assert_eq!(bs.alloc_msg_id(), 2);
    }

    #[tokio::test]
    async fn broker_state_default_exchanges() {
        let bs = test_broker();
        let ex = bs.exchanges.read().await;
        assert_eq!(ex.len(), 5);
        assert!(ex.contains_key(""));
        assert!(ex.contains_key("amq.direct"));
        assert!(ex.contains_key("amq.fanout"));
        assert!(ex.contains_key("amq.topic"));
        assert!(ex.contains_key("amq.headers"));
    }

    #[test]
    fn broker_state_remove_connection() {
        let bs = test_broker();
        let (amqp_tx, _rx) = mpsc::channel(1);
        bs.connections.insert(
            1,
            ConnHandle {
                id: 1,
                addr: "127.0.0.1:1234".parse().unwrap(),
                sink: std::sync::Arc::new(crate::protocol::MpscDeliverySink::new(amqp_tx)),
            },
        );
        bs.queues.insert("q1".into(), QueueState::new());
        bs.queues.get_mut("q1").unwrap().listeners.push((1, 1));

        bs.remove_connection(1);
        assert!(!bs.connections.contains_key(&1));
        assert!(bs.queues.get("q1").unwrap().listeners.is_empty());
    }

    #[test]
    fn broker_state_exclusive_queue_removed() {
        let bs = test_broker();
        let (amqp_tx, _rx) = mpsc::channel(1);
        bs.connections.insert(
            1,
            ConnHandle {
                id: 1,
                addr: "127.0.0.1:1234".parse().unwrap(),
                sink: std::sync::Arc::new(crate::protocol::MpscDeliverySink::new(amqp_tx)),
            },
        );
        let mut opts = QueueOptions::default();
        opts.exclusive = true;
        let mut q = QueueState::with_options(opts);
        q.owner_conn_id = Some(1);
        bs.queues.insert("excl".into(), q);

        bs.remove_connection(1);
        assert!(!bs.queues.contains_key("excl"));
    }

    // ──────────────────────────────────────────────
    // Virtual Host tests
    // ──────────────────────────────────────────────

    #[test]
    fn broker_has_default_vhost() {
        let bs = test_broker();
        assert!(bs.vhosts.contains_key("/"));
        assert_eq!(bs.vhosts.len(), 1);
    }

    #[test]
    fn broker_create_vhost() {
        let bs = test_broker();
        bs.vhosts
            .insert("/staging".to_string(), VHost::new("/staging".to_string()));
        assert!(bs.vhosts.contains_key("/staging"));
        assert_eq!(bs.vhosts.len(), 2);
    }

    #[test]
    fn broker_delete_vhost() {
        let bs = test_broker();
        bs.vhosts
            .insert("/temp".to_string(), VHost::new("/temp".to_string()));
        assert_eq!(bs.vhosts.len(), 2);
        bs.vhosts.remove("/temp");
        assert_eq!(bs.vhosts.len(), 1);
        assert!(!bs.vhosts.contains_key("/temp"));
    }

    #[test]
    fn broker_cannot_delete_nonexistent_vhost() {
        let bs = test_broker();
        let removed = bs.vhosts.remove("nonexistent");
        assert!(removed.is_none());
    }

    #[tokio::test]
    async fn vhost_has_own_exchanges() {
        let bs = test_broker();
        bs.vhosts
            .insert("/prod".to_string(), VHost::new("/prod".to_string()));

        let vh = bs.vhosts.get("/prod").unwrap();
        let ex = vh.exchanges.read().await;

        assert_eq!(ex.len(), 5);
    }

    #[test]
    fn vhost_queues_are_isolated() {
        let bs = test_broker();
        bs.vhosts
            .insert("/a".to_string(), VHost::new("/a".to_string()));
        bs.vhosts
            .insert("/b".to_string(), VHost::new("/b".to_string()));

        bs.vhosts
            .get("/a")
            .unwrap()
            .queues
            .insert("q1".into(), QueueState::new());

        assert!(bs.vhosts.get("/a").unwrap().queues.contains_key("q1"));
        assert!(!bs.vhosts.get("/b").unwrap().queues.contains_key("q1"));
    }
}
