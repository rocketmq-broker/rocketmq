use std::collections::HashMap;
use super::options::QueueOptions;
use super::priority::PriorityQueue;
use super::message::Message;

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
