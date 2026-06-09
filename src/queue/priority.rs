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
// File: priority.rs
// Description: Priority queue priority handling and comparisons.

use super::message::QueueMessage;
use std::collections::{BTreeMap, VecDeque};

/// A queue structure that prioritizes messages based on priority field values.
/// Multi-level priority message store backed by one `VecDeque` per
/// priority level (0 through `max_priority`).
/// A queue structure that prioritizes messages based on priority field values.
pub struct PriorityQueue {
    buckets: BTreeMap<u8, VecDeque<QueueMessage>>,
    /// OPT-6: cached total count — makes len() O(1) instead of O(B).
    total_len: usize,
}

impl Default for PriorityQueue {
    fn default() -> Self {
        Self::new()
    }
}

impl PriorityQueue {
    /// Creates a new instance with default values.
    pub fn new() -> Self {
        Self {
            buckets: BTreeMap::new(),
            total_len: 0,
        }
    }

    #[inline]
    pub fn push_back(&mut self, msg: QueueMessage) {
        self.buckets
            .entry(msg.priority())
            .or_default()
            .push_back(msg);
        self.total_len += 1;
    }

    #[inline]
    pub fn push_front(&mut self, msg: QueueMessage) {
        self.buckets
            .entry(msg.priority())
            .or_default()
            .push_front(msg);
        self.total_len += 1;
    }

    #[inline]
    pub fn pop_front(&mut self) -> Option<QueueMessage> {
        let key = *self.buckets.keys().next_back()?;
        let queue = self.buckets.get_mut(&key)?;
        let msg = queue.pop_front();
        if queue.is_empty() {
            self.buckets.remove(&key);
        }
        if msg.is_some() {
            self.total_len -= 1;
        }
        msg
    }

    #[inline]
    pub fn pop_oldest(&mut self) -> Option<QueueMessage> {
        let key = *self.buckets.keys().next()?;
        let queue = self.buckets.get_mut(&key)?;
        let msg = queue.pop_front();
        if queue.is_empty() {
            self.buckets.remove(&key);
        }
        if msg.is_some() {
            self.total_len -= 1;
        }
        msg
    }

    /// Returns the total number of messages across all priority levels.
    /// O(1) — uses a cached counter maintained on push/pop.
    #[inline(always)]
    pub fn len(&self) -> usize {
        self.total_len
    }

    #[inline]
    pub fn peek_front(&self) -> Option<&QueueMessage> {
        let key = *self.buckets.keys().next_back()?;
        self.buckets.get(&key)?.front()
    }

    pub fn is_empty(&self) -> bool {
        self.total_len == 0
    }

    pub fn clear(&mut self) {
        self.buckets.clear();
        self.total_len = 0;
    }
}
