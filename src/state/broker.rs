use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;
use tokio::sync::{RwLock, mpsc};

use dashmap::DashMap;

use crate::core::protocol::Frame;
use crate::queue::{QueueOptions, QueueState};
use crate::routing::exchange::{Binding, Exchange, create_default_exchanges};

#[derive(Clone)]
pub struct ConnHandle {
    pub id: u64,
    pub tx: mpsc::Sender<Frame>,
    pub addr: SocketAddr,
}

pub struct ChannelState {
    pub id: u16,
    pub prefetch_count: u16,
    pub unacked_count: u16,
    pub confirm_mode: bool,
    pub next_delivery_tag: u64,
    pub flow_active: bool,
}

impl ChannelState {
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

    pub fn can_deliver(&self) -> bool {
        if !self.flow_active {
            return false;
        }
        self.prefetch_count == 0 || self.unacked_count < self.prefetch_count
    }
}

pub struct ConnectionState {
    pub channels: HashMap<u16, ChannelState>,
    pub confirm_mode: bool,
    pub next_delivery_tag: u64,
}

impl ConnectionState {
    pub fn new() -> Self {
        Self {
            channels: HashMap::new(),
            confirm_mode: false,
            next_delivery_tag: 1,
        }
    }
}

/// Broker state with per-collection locking for maximum concurrency.
/// - `queues` and `connections` use DashMap (sharded concurrent map)
/// - `exchanges` use RwLock (rarely written, frequently read)
/// - ID counters use AtomicU64 (lock-free)
pub struct BrokerState {
    next_conn_id: AtomicU64,
    next_msg_id: AtomicU64,
    pub exchanges: RwLock<HashMap<String, Exchange>>,
    pub queues: DashMap<String, QueueState>,
    pub connections: DashMap<u64, ConnHandle>,
    pub conn_state: DashMap<u64, ConnectionState>,
    wal: OnceLock<Arc<crate::storage::wal::Wal>>,
    /// Deduplication cache: message-id → timestamp of first seen.
    pub dedup_cache: DashMap<String, Instant>,
}

impl BrokerState {
    pub fn new() -> Self {
        Self {
            next_conn_id: AtomicU64::new(1),
            next_msg_id: AtomicU64::new(1),
            exchanges: RwLock::new(create_default_exchanges()),
            queues: DashMap::new(),
            connections: DashMap::new(),
            conn_state: DashMap::new(),
            wal: OnceLock::new(),
            dedup_cache: DashMap::new(),
        }
    }

    pub fn set_wal(&self, wal: Arc<crate::storage::wal::Wal>) {
        let _ = self.wal.set(wal);
    }

    pub fn wal(&self) -> Option<&Arc<crate::storage::wal::Wal>> {
        self.wal.get()
    }

    pub fn alloc_conn_id(&self) -> u64 {
        self.next_conn_id.fetch_add(1, Ordering::Relaxed)
    }

    pub fn alloc_msg_id(&self) -> u64 {
        self.next_msg_id.fetch_add(1, Ordering::Relaxed)
    }

    pub fn remove_connection(&self, conn_id: u64) {
        self.connections.remove(&conn_id);
        self.conn_state.remove(&conn_id);

        let mut queues_to_remove = Vec::new();
        for mut entry in self.queues.iter_mut() {
            let (name, queue) = entry.pair_mut();
            queue.listeners.retain(|&(id, _)| id != conn_id);
            if queue.options.exclusive && queue.owner_conn_id == Some(conn_id) {
                queues_to_remove.push(name.clone());
            }
        }
        for name in &queues_to_remove {
            self.queues.remove(name);
        }

        // Auto-delete queues with no listeners left
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

    pub fn auto_bind_default_exchange(&self, queue_name: &str) {
        if let Ok(mut exchanges) = self.exchanges.try_write() {
            if let Some(default_ex) = exchanges.get_mut("") {
                default_ex.add_binding(Binding {
                    queue_name: queue_name.to_string(),
                    routing_key: queue_name.to_string(),
                    headers_match: None,
                });
            }
        }
    }

    pub fn alloc_delivery_tag(&self, conn_id: u64) -> u64 {
        if let Some(mut cs) = self.conn_state.get_mut(&conn_id) {
            let tag = cs.next_delivery_tag;
            cs.next_delivery_tag += 1;
            tag
        } else {
            0
        }
    }
}

/// No more outer RwLock — each collection has its own lock.
pub type Broker = Arc<BrokerState>;

#[cfg(test)]
mod tests {
    use super::*;



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
        assert!(ch.can_deliver()); // flow_active defaults to true

        ch.flow_active = false;
        assert!(!ch.can_deliver()); // paused by flow control

        ch.flow_active = true;
        assert!(ch.can_deliver()); // resumed
    }

    #[test]
    fn channel_state_flow_overrides_prefetch() {
        let mut ch = ChannelState::new(1);
        ch.prefetch_count = 10; // plenty of room
        ch.unacked_count = 0;
        ch.flow_active = false;
        assert!(!ch.can_deliver()); // flow takes precedence
    }

    #[test]
    fn broker_state_alloc_ids_monotonic() {
        let bs = BrokerState::new();
        assert_eq!(bs.alloc_conn_id(), 1);
        assert_eq!(bs.alloc_conn_id(), 2);
        assert_eq!(bs.alloc_msg_id(), 1);
        assert_eq!(bs.alloc_msg_id(), 2);
    }

    #[tokio::test]
    async fn broker_state_default_exchanges() {
        let bs = BrokerState::new();
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
        let bs = BrokerState::new();
        let (tx, _rx) = mpsc::channel(1);
        bs.connections.insert(
            1,
            ConnHandle {
                id: 1,
                tx,
                addr: "127.0.0.1:1234".parse().unwrap(),
            },
        );
        bs.conn_state.insert(1, ConnectionState::new());
        bs.queues.insert("q1".into(), QueueState::new());
        bs.queues.get_mut("q1").unwrap().listeners.push((1, 1));

        bs.remove_connection(1);
        assert!(!bs.connections.contains_key(&1));
        assert!(bs.queues.get("q1").unwrap().listeners.is_empty());
    }

    #[test]
    fn broker_state_exclusive_queue_removed() {
        let bs = BrokerState::new();
        let (tx, _rx) = mpsc::channel(1);
        bs.connections.insert(
            1,
            ConnHandle {
                id: 1,
                tx,
                addr: "127.0.0.1:1234".parse().unwrap(),
            },
        );
        bs.conn_state.insert(1, ConnectionState::new());
        let mut opts = QueueOptions::default();
        opts.exclusive = true;
        let mut q = QueueState::with_options(opts);
        q.owner_conn_id = Some(1);
        bs.queues.insert("excl".into(), q);

        bs.remove_connection(1);
        assert!(!bs.queues.contains_key("excl"));
    }
}
