use std::collections::{BTreeMap, HashMap, VecDeque};
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::{RwLock, mpsc};

use dashmap::DashMap;

use crate::exchange::{Binding, Exchange, create_default_exchanges};
use crate::core::protocol::Frame;

#[derive(Clone)]
pub struct ConnHandle {
    pub id: u64,
    pub tx: mpsc::Sender<Frame>,
    pub addr: SocketAddr,
}

#[derive(Clone, Debug, Default)]
pub struct QueueOptions {
    pub durable: bool,
    pub exclusive: bool,
    pub auto_delete: bool,
    pub max_priority: u8,
    pub message_ttl: Option<Duration>,
    pub max_length: Option<usize>,
    pub dead_letter_exchange: Option<String>,
    pub dead_letter_routing_key: Option<String>,
}

impl QueueOptions {
    pub fn from_headers(headers: &str) -> (String, Self) {
        let mut name = String::new();
        let mut opts = Self::default();

        for line in headers.split("\r\n") {
            if line.is_empty() {
                continue;
            }
            if let Some((k, v)) = line.split_once(':') {
                match k {
                    "name" => name = v.to_string(),
                    "durable" => opts.durable = v == "true",
                    "exclusive" => opts.exclusive = v == "true",
                    "auto_delete" => opts.auto_delete = v == "true",
                    "max_priority" => opts.max_priority = v.parse().unwrap_or(0),
                    "message_ttl" => {
                        opts.message_ttl = v.parse::<u64>().ok().map(Duration::from_millis)
                    }
                    "max_length" => opts.max_length = v.parse().ok(),
                    "x-dead-letter-exchange" => opts.dead_letter_exchange = Some(v.to_string()),
                    "x-dead-letter-routing-key" => {
                        opts.dead_letter_routing_key = Some(v.to_string())
                    }
                    _ => {}
                }
            }
        }
        (name, opts)
    }
}

pub struct Message {
    pub id: u64,
    pub headers: Vec<u8>,
    pub body: Vec<u8>,
    pub priority: u8,
    pub expiration: Option<Instant>,
    pub redelivered: bool,
}

impl Message {
    pub fn new(id: u64, headers: Vec<u8>, body: Vec<u8>) -> Self {
        Self {
            id,
            headers,
            body,
            priority: 0,
            expiration: None,
            redelivered: false,
        }
    }

    pub fn is_expired(&self) -> bool {
        self.expiration.map_or(false, |exp| Instant::now() >= exp)
    }
}

pub struct PriorityQueue {
    buckets: BTreeMap<u8, VecDeque<Message>>,
}

impl PriorityQueue {
    pub fn new() -> Self {
        Self {
            buckets: BTreeMap::new(),
        }
    }

    pub fn push_back(&mut self, msg: Message) {
        self.buckets.entry(msg.priority).or_default().push_back(msg);
    }

    pub fn push_front(&mut self, msg: Message) {
        self.buckets
            .entry(msg.priority)
            .or_default()
            .push_front(msg);
    }

    pub fn pop_front(&mut self) -> Option<Message> {
        let key = *self.buckets.keys().next_back()?;
        let queue = self.buckets.get_mut(&key)?;
        let msg = queue.pop_front();
        if queue.is_empty() {
            self.buckets.remove(&key);
        }
        msg
    }

    pub fn pop_oldest(&mut self) -> Option<Message> {
        let key = *self.buckets.keys().next()?;
        let queue = self.buckets.get_mut(&key)?;
        let msg = queue.pop_front();
        if queue.is_empty() {
            self.buckets.remove(&key);
        }
        msg
    }

    pub fn len(&self) -> usize {
        self.buckets.values().map(|q| q.len()).sum()
    }
}

pub struct QueueState {
    pub options: QueueOptions,
    pub owner_conn_id: Option<u64>,
    pub listeners: Vec<u64>,
    pub messages: PriorityQueue,
    pub inflight: HashMap<u64, Message>,
    pub next_listener: usize,
    pub consumer_count: usize,
}

impl QueueState {
    pub fn new() -> Self {
        Self::with_options(QueueOptions::default())
    }

    pub fn with_options(options: QueueOptions) -> Self {
        Self {
            options,
            owner_conn_id: None,
            listeners: Vec::new(),
            messages: PriorityQueue::new(),
            inflight: HashMap::new(),
            next_listener: 0,
            consumer_count: 0,
        }
    }

    pub fn next_target(&mut self) -> Option<u64> {
        if self.listeners.is_empty() {
            return None;
        }
        let idx = self.next_listener % self.listeners.len();
        self.next_listener += 1;
        Some(self.listeners[idx])
    }
}

pub struct ChannelState {
    pub id: u16,
    pub prefetch_count: u16,
    pub unacked_count: u16,
    pub confirm_mode: bool,
    pub next_delivery_tag: u64,
}

impl ChannelState {
    pub fn new(id: u16) -> Self {
        Self {
            id,
            prefetch_count: 0,
            unacked_count: 0,
            confirm_mode: false,
            next_delivery_tag: 1,
        }
    }

    pub fn can_deliver(&self) -> bool {
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
            queue.listeners.retain(|&id| id != conn_id);
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
    fn queue_options_from_headers_full() {
        let input = "name:orders\r\ndurable:true\r\nexclusive:true\r\nauto_delete:true\r\nmax_priority:10\r\nmessage_ttl:60000\r\nmax_length:1000\r\nx-dead-letter-exchange:dlx\r\nx-dead-letter-routing-key:dead\r\n";
        let (name, opts) = QueueOptions::from_headers(input);
        assert_eq!(name, "orders");
        assert!(opts.durable);
        assert!(opts.exclusive);
        assert!(opts.auto_delete);
        assert_eq!(opts.max_priority, 10);
        assert_eq!(opts.message_ttl, Some(Duration::from_millis(60000)));
        assert_eq!(opts.max_length, Some(1000));
        assert_eq!(opts.dead_letter_exchange.as_deref(), Some("dlx"));
        assert_eq!(opts.dead_letter_routing_key.as_deref(), Some("dead"));
    }

    #[test]
    fn queue_options_from_headers_minimal() {
        let input = "name:q1\r\n";
        let (name, opts) = QueueOptions::from_headers(input);
        assert_eq!(name, "q1");
        assert!(!opts.durable);
        assert!(opts.message_ttl.is_none());
    }

    #[test]
    fn queue_options_default_values() {
        let opts = QueueOptions::default();
        assert!(!opts.durable);
        assert!(!opts.exclusive);
        assert_eq!(opts.max_priority, 0);
    }

    #[test]
    fn queue_options_ignores_unknown_keys() {
        let input = "name:q1\r\nunknown:value\r\n";
        let (name, _) = QueueOptions::from_headers(input);
        assert_eq!(name, "q1");
    }

    #[test]
    fn queue_options_invalid_ttl_ignored() {
        let input = "name:q1\r\nmessage_ttl:bad\r\n";
        let (_, opts) = QueueOptions::from_headers(input);
        assert!(opts.message_ttl.is_none());
    }

    #[test]
    fn message_new_defaults() {
        let msg = Message::new(1, vec![], vec![1, 2, 3]);
        assert_eq!(msg.id, 1);
        assert_eq!(msg.priority, 0);
        assert!(!msg.redelivered);
        assert!(!msg.is_expired());
    }

    #[test]
    fn message_expired() {
        let msg = Message {
            id: 1,
            headers: vec![],
            body: vec![],
            priority: 0,
            expiration: Some(Instant::now() - Duration::from_secs(1)),
            redelivered: false,
        };
        assert!(msg.is_expired());
    }

    #[test]
    fn message_not_expired() {
        let msg = Message {
            id: 1,
            headers: vec![],
            body: vec![],
            priority: 0,
            expiration: Some(Instant::now() + Duration::from_secs(60)),
            redelivered: false,
        };
        assert!(!msg.is_expired());
    }

    #[test]
    fn priority_queue_fifo_same_priority() {
        let mut pq = PriorityQueue::new();
        pq.push_back(Message::new(1, vec![], b"first".to_vec()));
        pq.push_back(Message::new(2, vec![], b"second".to_vec()));
        pq.push_back(Message::new(3, vec![], b"third".to_vec()));
        assert_eq!(pq.len(), 3);
        assert_eq!(pq.pop_front().unwrap().body, b"first");
        assert_eq!(pq.pop_front().unwrap().body, b"second");
        assert_eq!(pq.pop_front().unwrap().body, b"third");
        assert!(pq.pop_front().is_none());
    }

    #[test]
    fn priority_queue_higher_priority_first() {
        let mut pq = PriorityQueue::new();
        let mut low = Message::new(1, vec![], b"low".to_vec());
        low.priority = 1;
        let mut high = Message::new(2, vec![], b"high".to_vec());
        high.priority = 10;
        let mut mid = Message::new(3, vec![], b"mid".to_vec());
        mid.priority = 5;
        pq.push_back(low);
        pq.push_back(high);
        pq.push_back(mid);
        assert_eq!(pq.pop_front().unwrap().body, b"high");
        assert_eq!(pq.pop_front().unwrap().body, b"mid");
        assert_eq!(pq.pop_front().unwrap().body, b"low");
    }

    #[test]
    fn priority_queue_push_front_stays_at_front() {
        let mut pq = PriorityQueue::new();
        pq.push_back(Message::new(1, vec![], b"back".to_vec()));
        pq.push_front(Message::new(2, vec![], b"front".to_vec()));
        assert_eq!(pq.pop_front().unwrap().body, b"front");
    }

    #[test]
    fn priority_queue_pop_oldest_evicts_lowest() {
        let mut pq = PriorityQueue::new();
        let mut low = Message::new(1, vec![], b"low".to_vec());
        low.priority = 0;
        let mut high = Message::new(2, vec![], b"high".to_vec());
        high.priority = 10;
        pq.push_back(low);
        pq.push_back(high);
        assert_eq!(pq.pop_oldest().unwrap().body, b"low");
        assert_eq!(pq.len(), 1);
    }

    #[test]
    fn priority_queue_empty_operations() {
        let mut pq = PriorityQueue::new();
        assert_eq!(pq.len(), 0);
        assert!(pq.pop_front().is_none());
        assert!(pq.pop_oldest().is_none());
    }

    #[test]
    fn queue_state_round_robin() {
        let mut q = QueueState::new();
        q.listeners = vec![10, 20, 30];
        assert_eq!(q.next_target(), Some(10));
        assert_eq!(q.next_target(), Some(20));
        assert_eq!(q.next_target(), Some(30));
        assert_eq!(q.next_target(), Some(10));
    }

    #[test]
    fn queue_state_no_listeners() {
        let mut q = QueueState::new();
        assert_eq!(q.next_target(), None);
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
        bs.queues.get_mut("q1").unwrap().listeners.push(1);

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
