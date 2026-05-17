use std::collections::HashMap;
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
