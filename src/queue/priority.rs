use std::collections::{BTreeMap, VecDeque};
use super::message::Message;

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

    /// Peek at the highest-priority front message without removing it.
    pub fn peek_front(&self) -> Option<&Message> {
        let key = *self.buckets.keys().next_back()?;
        self.buckets.get(&key)?.front()
    }

    pub fn is_empty(&self) -> bool {
        self.buckets.is_empty()
    }

    pub fn clear(&mut self) {
        self.buckets.clear();
    }
}
