use std::collections::HashMap;
use std::time::Instant;
use super::options::QueueOptions;
use super::priority::PriorityQueue;
use super::message::Message;

pub struct QueueState {
    pub options: QueueOptions,
    pub owner_conn_id: Option<u64>,
    pub listeners: Vec<(u64, u16)>,
    pub messages: PriorityQueue,
    pub inflight: HashMap<u64, Message>,
    pub next_listener: usize,
    pub consumer_count: usize,
    /// Maps consumer_tag → (conn_id, channel_id)
    pub consumer_tags: HashMap<String, (u64, u16)>,
    /// Last time this queue had activity (publish, consume, listen).
    pub last_activity: Instant,
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
            consumer_tags: HashMap::new(),
            last_activity: Instant::now(),
        }
    }

    /// Add a consumer with an optional tag. Returns the assigned tag.
    pub fn add_consumer(&mut self, conn_id: u64, channel_id: u16, tag: Option<String>) -> String {
        let tag = tag.unwrap_or_else(|| format!("ctag-{}-{}", conn_id, channel_id));
        if !self.listeners.contains(&(conn_id, channel_id)) {
            self.listeners.push((conn_id, channel_id));
        }
        self.consumer_tags.insert(tag.clone(), (conn_id, channel_id));
        self.consumer_count = self.listeners.len();
        tag
    }

    /// Cancel a consumer by tag. Returns true if found.
    pub fn cancel_consumer(&mut self, tag: &str) -> bool {
        if let Some((conn_id, channel_id)) = self.consumer_tags.remove(tag) {
            self.listeners.retain(|&(c, ch)| !(c == conn_id && ch == channel_id));
            self.consumer_count = self.listeners.len();
            true
        } else {
            false
        }
    }

    pub fn next_target(&mut self, broker: &crate::state::Broker) -> Option<(u64, u16)> {
        if self.listeners.is_empty() {
            return None;
        }
        let len = self.listeners.len();
        for _ in 0..len {
            let idx = self.next_listener % len;
            self.next_listener += 1;
            let (target_id, channel_id) = self.listeners[idx];

            if let Some(cs) = broker.conn_state.get(&target_id) {
                if let Some(ch) = cs.channels.get(&channel_id) {
                    if ch.can_deliver() {
                        return Some((target_id, channel_id));
                    }
                }
            }
        }
        None
    }
}
