use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;
use tokio::sync::{RwLock, mpsc};

use dashmap::DashMap;

use crate::core::protocol::Frame;
use crate::queue::{DelayQueue, QueueOptions, QueueState};
use crate::routing::exchange::{Binding, Exchange, create_default_exchanges};
use crate::state::vhost::{VHost, DEFAULT_VHOST};

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

/// A pending operation buffered during a transaction.
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

pub struct ConnectionState {
    pub channels: HashMap<u16, ChannelState>,
    pub confirm_mode: bool,
    pub next_delivery_tag: u64,
    /// Which vhost this connection is on.
    pub vhost: String,
    /// Whether this connection is in transaction mode.
    pub tx_mode: bool,
    /// Buffered operations for the current transaction.
    pub tx_buffer: Vec<PendingOp>,
    /// Negotiated maximum frame size.
    pub frame_max: u32,
    /// Negotiated maximum channel number.
    pub channel_max: u16,
    /// Negotiated heartbeat interval (seconds).
    pub heartbeat: u16,
    /// Whether the connection has completed AMQP handshake.
    pub authenticated: bool,
    /// Username from SASL auth.
    pub username: String,
}

impl ConnectionState {
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
    /// Delayed message delivery buffer.
    pub delay_queue: DelayQueue,
    /// Virtual hosts for namespace isolation.
    pub vhosts: DashMap<String, VHost>,
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
            delay_queue: DelayQueue::new(),
            vhosts: {
                let map = DashMap::new();
                map.insert(DEFAULT_VHOST.to_string(), VHost::new(DEFAULT_VHOST.to_string()));
                map
            },
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

    // ──────────────────────────────────────────────
    // Virtual Host tests
    // ──────────────────────────────────────────────

    #[test]
    fn broker_has_default_vhost() {
        let bs = BrokerState::new();
        assert!(bs.vhosts.contains_key("/"));
        assert_eq!(bs.vhosts.len(), 1);
    }

    #[test]
    fn broker_create_vhost() {
        let bs = BrokerState::new();
        bs.vhosts
            .insert("/staging".to_string(), VHost::new("/staging".to_string()));
        assert!(bs.vhosts.contains_key("/staging"));
        assert_eq!(bs.vhosts.len(), 2);
    }

    #[test]
    fn broker_delete_vhost() {
        let bs = BrokerState::new();
        bs.vhosts
            .insert("/temp".to_string(), VHost::new("/temp".to_string()));
        assert_eq!(bs.vhosts.len(), 2);
        bs.vhosts.remove("/temp");
        assert_eq!(bs.vhosts.len(), 1);
        assert!(!bs.vhosts.contains_key("/temp"));
    }

    #[test]
    fn broker_cannot_delete_nonexistent_vhost() {
        let bs = BrokerState::new();
        let removed = bs.vhosts.remove("nonexistent");
        assert!(removed.is_none());
    }

    #[tokio::test]
    async fn vhost_has_own_exchanges() {
        let bs = BrokerState::new();
        bs.vhosts
            .insert("/prod".to_string(), VHost::new("/prod".to_string()));

        let vh = bs.vhosts.get("/prod").unwrap();
        let ex = vh.exchanges.read().await;
        // Each vhost gets its own set of default exchanges
        assert_eq!(ex.len(), 5);
    }

    #[test]
    fn vhost_queues_are_isolated() {
        let bs = BrokerState::new();
        bs.vhosts
            .insert("/a".to_string(), VHost::new("/a".to_string()));
        bs.vhosts
            .insert("/b".to_string(), VHost::new("/b".to_string()));

        // Add queue to vhost /a
        bs.vhosts
            .get("/a")
            .unwrap()
            .queues
            .insert("q1".into(), QueueState::new());

        // vhost /a has q1, vhost /b does not
        assert!(bs.vhosts.get("/a").unwrap().queues.contains_key("q1"));
        assert!(!bs.vhosts.get("/b").unwrap().queues.contains_key("q1"));
    }

    #[test]
    fn connection_state_defaults_to_root_vhost() {
        let cs = ConnectionState::new();
        assert_eq!(cs.vhost, "/");
    }

    #[test]
    fn connection_state_vhost_can_be_changed() {
        let mut cs = ConnectionState::new();
        cs.vhost = "/production".to_string();
        assert_eq!(cs.vhost, "/production");
    }

    #[test]
    fn connection_vhost_tracks_per_connection() {
        let bs = BrokerState::new();
        bs.conn_state.insert(1, ConnectionState::new());
        bs.conn_state.insert(2, ConnectionState::new());

        // Connection 1 uses default
        assert_eq!(bs.conn_state.get(&1).unwrap().vhost, "/");

        // Connection 2 switches to different vhost
        bs.conn_state.get_mut(&2).unwrap().vhost = "/staging".to_string();
        assert_eq!(bs.conn_state.get(&2).unwrap().vhost, "/staging");
        // Connection 1 unaffected
        assert_eq!(bs.conn_state.get(&1).unwrap().vhost, "/");
    }

    // ──────────────────────────────────────────────
    // Transaction tests
    // ──────────────────────────────────────────────

    #[test]
    fn connection_state_defaults_no_tx() {
        let cs = ConnectionState::new();
        assert!(!cs.tx_mode);
        assert!(cs.tx_buffer.is_empty());
    }

    #[test]
    fn tx_mode_enable() {
        let mut cs = ConnectionState::new();
        cs.tx_mode = true;
        assert!(cs.tx_mode);
    }

    #[test]
    fn tx_buffer_publish_op() {
        let mut cs = ConnectionState::new();
        cs.tx_mode = true;
        cs.tx_buffer.push(PendingOp::Publish {
            exchange: "".to_string(),
            routing_key: "q1".to_string(),
            headers: vec![],
            body: b"hello".to_vec(),
        });
        assert_eq!(cs.tx_buffer.len(), 1);
        match &cs.tx_buffer[0] {
            PendingOp::Publish { routing_key, body, .. } => {
                assert_eq!(routing_key, "q1");
                assert_eq!(body, b"hello");
            }
            _ => panic!("expected Publish"),
        }
    }

    #[test]
    fn tx_buffer_ack_op() {
        let mut cs = ConnectionState::new();
        cs.tx_mode = true;
        cs.tx_buffer.push(PendingOp::Ack { msg_id: 42 });
        assert_eq!(cs.tx_buffer.len(), 1);
        match &cs.tx_buffer[0] {
            PendingOp::Ack { msg_id } => assert_eq!(*msg_id, 42),
            _ => panic!("expected Ack"),
        }
    }

    #[test]
    fn tx_buffer_mixed_ops() {
        let mut cs = ConnectionState::new();
        cs.tx_mode = true;
        cs.tx_buffer.push(PendingOp::Publish {
            exchange: "ex1".to_string(),
            routing_key: "q1".to_string(),
            headers: b"h:v\r\n".to_vec(),
            body: b"msg1".to_vec(),
        });
        cs.tx_buffer.push(PendingOp::Ack { msg_id: 1 });
        cs.tx_buffer.push(PendingOp::Publish {
            exchange: "".to_string(),
            routing_key: "q2".to_string(),
            headers: vec![],
            body: b"msg2".to_vec(),
        });
        assert_eq!(cs.tx_buffer.len(), 3);
    }

    #[test]
    fn tx_rollback_clears_buffer() {
        let mut cs = ConnectionState::new();
        cs.tx_mode = true;
        cs.tx_buffer.push(PendingOp::Publish {
            exchange: "".to_string(),
            routing_key: "q1".to_string(),
            headers: vec![],
            body: b"data".to_vec(),
        });
        cs.tx_buffer.push(PendingOp::Ack { msg_id: 5 });

        // Simulate rollback
        cs.tx_buffer.clear();
        assert!(cs.tx_buffer.is_empty());
        // tx_mode stays on after rollback (per AMQP spec)
        assert!(cs.tx_mode);
    }

    #[test]
    fn tx_commit_drains_buffer() {
        let mut cs = ConnectionState::new();
        cs.tx_mode = true;
        cs.tx_buffer.push(PendingOp::Publish {
            exchange: "".to_string(),
            routing_key: "q1".to_string(),
            headers: vec![],
            body: b"data".to_vec(),
        });

        // Simulate commit: take buffer
        let ops = std::mem::take(&mut cs.tx_buffer);
        assert_eq!(ops.len(), 1);
        assert!(cs.tx_buffer.is_empty());
    }

    #[test]
    fn tx_commit_applies_publish_to_queue() {
        let bs = BrokerState::new();
        bs.queues.insert("q1".into(), QueueState::new());

        // Simulate a commit with a Publish op
        let op = PendingOp::Publish {
            exchange: "".to_string(),
            routing_key: "q1".to_string(),
            headers: vec![],
            body: b"committed".to_vec(),
        };

        // Apply the op
        match &op {
            PendingOp::Publish { routing_key, body, .. } => {
                let msg_id = bs.alloc_msg_id();
                if let Some(mut queue) = bs.queues.get_mut(routing_key.as_str()) {
                    let msg = crate::queue::Message::new(msg_id, Vec::new(), body.clone());
                    queue.messages.push_back(msg);
                }
            }
            _ => {}
        }

        let q = bs.queues.get("q1").unwrap();
        assert_eq!(q.messages.len(), 1);
    }

    #[test]
    fn tx_commit_applies_ack_removes_inflight() {
        let bs = BrokerState::new();
        bs.queues.insert("q1".into(), QueueState::new());

        // Put a message in inflight
        let msg = crate::queue::Message::new(42, vec![], b"test".to_vec());
        bs.queues.get_mut("q1").unwrap().inflight.insert(42, msg);

        // Simulate ack op
        let op = PendingOp::Ack { msg_id: 42 };
        match &op {
            PendingOp::Ack { msg_id } => {
                for mut entry in bs.queues.iter_mut() {
                    if entry.value_mut().inflight.remove(msg_id).is_some() {
                        break;
                    }
                }
            }
            _ => {}
        }

        let q = bs.queues.get("q1").unwrap();
        assert!(q.inflight.is_empty());
    }

    #[test]
    fn tx_multiple_commits_independent() {
        let mut cs = ConnectionState::new();
        cs.tx_mode = true;

        // First transaction
        cs.tx_buffer.push(PendingOp::Publish {
            exchange: "".to_string(),
            routing_key: "q1".to_string(),
            headers: vec![],
            body: b"tx1".to_vec(),
        });
        let ops1 = std::mem::take(&mut cs.tx_buffer);
        assert_eq!(ops1.len(), 1);

        // Second transaction (buffer was cleared after commit)
        cs.tx_buffer.push(PendingOp::Ack { msg_id: 10 });
        cs.tx_buffer.push(PendingOp::Ack { msg_id: 20 });
        let ops2 = std::mem::take(&mut cs.tx_buffer);
        assert_eq!(ops2.len(), 2);
        assert!(cs.tx_buffer.is_empty());
    }

    #[test]
    fn pending_op_clone() {
        let op = PendingOp::Publish {
            exchange: "ex".to_string(),
            routing_key: "rk".to_string(),
            headers: vec![1, 2],
            body: vec![3, 4],
        };
        let cloned = op.clone();
        match cloned {
            PendingOp::Publish { exchange, routing_key, headers, body } => {
                assert_eq!(exchange, "ex");
                assert_eq!(routing_key, "rk");
                assert_eq!(headers, vec![1, 2]);
                assert_eq!(body, vec![3, 4]);
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn pending_op_debug() {
        let op = PendingOp::Ack { msg_id: 99 };
        let debug_str = format!("{:?}", op);
        assert!(debug_str.contains("99"));
    }
}
