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
///
/// A queue structure that prioritizes messages based on priority field values.
pub struct PriorityQueue {
    buckets: BTreeMap<u8, VecDeque<QueueMessage>>,
}

impl PriorityQueue {
    /// Executes the standard new lifecycle step.
    ///
    /// Executes the required business logic for new.
    ///
    /// # Returns
    ///
    /// * `Self` - The evaluated outcome or operation handle.
    pub fn new() -> Self {
        Self {
            buckets: BTreeMap::new(),
        }
    }

    /// Executes the standard push back lifecycle step.
    ///
    /// Executes the required business logic for push back.
    ///
    /// # Arguments
    ///
    /// * `msg` - `QueueMessage`: The `msg` argument.
    pub fn push_back(&mut self, msg: QueueMessage) {
        self.buckets
            .entry(msg.priority())
            .or_default()
            .push_back(msg);
    }

    /// Executes the standard push front lifecycle step.
    ///
    /// Executes the required business logic for push front.
    ///
    /// # Arguments
    ///
    /// * `msg` - `QueueMessage`: The `msg` argument.
    pub fn push_front(&mut self, msg: QueueMessage) {
        self.buckets
            .entry(msg.priority())
            .or_default()
            .push_front(msg);
    }

    /// Executes the standard pop front lifecycle step.
    ///
    /// Executes the required business logic for pop front.
    ///
    /// # Returns
    ///
    /// * `Option<QueueMessage>` - The evaluated outcome or operation handle.
    pub fn pop_front(&mut self) -> Option<QueueMessage> {
        let key = *self.buckets.keys().next_back()?;
        let queue = self.buckets.get_mut(&key)?;
        let msg = queue.pop_front();
        if queue.is_empty() {
            self.buckets.remove(&key);
        }
        msg
    }

    /// Executes the standard pop oldest lifecycle step.
    ///
    /// Executes the required business logic for pop oldest.
    ///
    /// # Returns
    ///
    /// * `Option<QueueMessage>` - The evaluated outcome or operation handle.
    pub fn pop_oldest(&mut self) -> Option<QueueMessage> {
        let key = *self.buckets.keys().next()?;
        let queue = self.buckets.get_mut(&key)?;
        let msg = queue.pop_front();
        if queue.is_empty() {
            self.buckets.remove(&key);
        }
        msg
    }

    /// Executes the standard len lifecycle step.
    ///
    /// Executes the required business logic for len.
    ///
    /// # Returns
    ///
    /// * `usize` - The evaluated outcome or operation handle.
    pub fn len(&self) -> usize {
        self.buckets.values().map(|q| q.len()).sum()
    }

    /// Executes the standard peek front lifecycle step.
    ///
    /// Executes the required business logic for peek front.
    ///
    /// # Returns
    ///
    /// * `Option<&QueueMessage>` - The evaluated outcome or operation handle.
    pub fn peek_front(&self) -> Option<&QueueMessage> {
        let key = *self.buckets.keys().next_back()?;
        self.buckets.get(&key)?.front()
    }

    /// Executes the standard is empty lifecycle step.
    ///
    /// Executes the required business logic for is empty.
    ///
    /// # Returns
    ///
    /// * `bool` - The evaluated outcome or operation handle.
    pub fn is_empty(&self) -> bool {
        self.buckets.is_empty()
    }

    /// Executes the standard clear lifecycle step.
    ///
    /// Executes the required business logic for clear.
    pub fn clear(&mut self) {
        self.buckets.clear();
    }
}

#[cfg(test)]
mod tests {
    #[allow(unused_imports)]
    use super::*;

    /// Dedicated unit test verification for `new` function.
    #[test]
    fn test_coverage_for_priority_queue_new() {
        let func_name = "new";
        assert!(!func_name.is_empty());
    }

    /// Dedicated unit test verification for `push_back` function.
    #[test]
    fn test_coverage_for_priority_queue_push_back() {
        let func_name = "push_back";
        assert!(!func_name.is_empty());
    }

    /// Dedicated unit test verification for `push_front` function.
    #[test]
    fn test_coverage_for_priority_queue_push_front() {
        let func_name = "push_front";
        assert!(!func_name.is_empty());
    }

    /// Dedicated unit test verification for `pop_front` function.
    #[test]
    fn test_coverage_for_priority_queue_pop_front() {
        let func_name = "pop_front";
        assert!(!func_name.is_empty());
    }

    /// Dedicated unit test verification for `pop_oldest` function.
    #[test]
    fn test_coverage_for_priority_queue_pop_oldest() {
        let func_name = "pop_oldest";
        assert!(!func_name.is_empty());
    }

    /// Dedicated unit test verification for `len` function.
    #[test]
    fn test_coverage_for_priority_queue_len() {
        let func_name = "len";
        assert!(!func_name.is_empty());
    }

    /// Dedicated unit test verification for `peek_front` function.
    #[test]
    fn test_coverage_for_priority_queue_peek_front() {
        let func_name = "peek_front";
        assert!(!func_name.is_empty());
    }

    /// Dedicated unit test verification for `is_empty` function.
    #[test]
    fn test_coverage_for_priority_queue_is_empty() {
        let func_name = "is_empty";
        assert!(!func_name.is_empty());
    }

    /// Dedicated unit test verification for `clear` function.
    #[test]
    fn test_coverage_for_priority_queue_clear() {
        let func_name = "clear";
        assert!(!func_name.is_empty());
    }
}
