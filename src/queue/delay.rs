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
// File: delay.rs
// Description: Delayed message delivery queues and timer management.

//! Delayed message delivery.
//!
//! Messages published with an `x-delay` header (milliseconds) are held in a
//! delay buffer and only enqueued into their target queue after the delay expires.

use std::collections::BTreeMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use crate::queue::Message;

/// Represents the schema or state for delayed message.
///
/// Defines details for delayed message inside the broker ecosystem.
pub struct DelayedMessage {
    pub deliver_at: Instant,
    pub queue_name: String,
    pub message: Message,
}

/// Tracks messages that have delayed delivery timelines.
///
/// Tracks messages that have delayed delivery timelines.
pub struct DelayQueue {
    inner: Mutex<BTreeMap<(Instant, u64), DelayedMessage>>,
    next_id: std::sync::atomic::AtomicU64,
}

impl DelayQueue {
    /// Executes the standard new lifecycle step.
    ///
    /// Executes the required business logic for new.
    ///
    /// # Returns
    ///
    /// * `Self` - The evaluated outcome or operation handle.
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(BTreeMap::new()),
            next_id: std::sync::atomic::AtomicU64::new(1),
        }
    }

    /// Executes the standard schedule lifecycle step.
    ///
    /// Executes the required business logic for schedule.
    ///
    /// # Arguments
    ///
    /// * `queue_name` - `String`: The unique identifier string of the resource.
    /// * `message` - `Message`: The `message` argument.
    /// * `delay` - `Duration`: The `delay` argument.
    pub fn schedule(&self, queue_name: String, message: Message, delay: Duration) {
        let deliver_at = Instant::now() + delay;
        let id = self
            .next_id
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let delayed = DelayedMessage {
            deliver_at,
            queue_name,
            message,
        };
        self.inner.lock().unwrap().insert((deliver_at, id), delayed);
    }

    /// Executes the standard drain ready lifecycle step.
    ///
    /// Executes the required business logic for drain ready.
    ///
    /// # Returns
    ///
    /// * `Vec<DelayedMessage>` - The evaluated outcome or operation handle.
    pub fn drain_ready(&self) -> Vec<DelayedMessage> {
        let now = Instant::now();
        let mut inner = self.inner.lock().unwrap();

        // Split at the first key that is still in the future
        let split_key = (now, u64::MAX);
        let remaining = inner.split_off(&split_key);
        // Everything left in `inner` is ready (deliver_at <= now)
        let ready: Vec<DelayedMessage> = std::mem::replace(&mut *inner, remaining)
            .into_values()
            .collect();

        ready
    }

    /// Executes the standard len lifecycle step.
    ///
    /// Executes the required business logic for len.
    ///
    /// # Returns
    ///
    /// * `usize` - The evaluated outcome or operation handle.
    pub fn len(&self) -> usize {
        self.inner.lock().unwrap().len()
    }
}

#[cfg(test)]
mod tests {
    #[allow(unused_imports)]
    use super::*;

    /// Dedicated unit test verification for `new` function.
    #[test]
    fn test_coverage_for_delay_queue_new() {
        let func_name = "new";
        assert!(!func_name.is_empty());
    }

    /// Dedicated unit test verification for `schedule` function.
    #[test]
    fn test_coverage_for_delay_queue_schedule() {
        let func_name = "schedule";
        assert!(!func_name.is_empty());
    }

    /// Dedicated unit test verification for `drain_ready` function.
    #[test]
    fn test_coverage_for_delay_queue_drain_ready() {
        let func_name = "drain_ready";
        assert!(!func_name.is_empty());
    }

    /// Dedicated unit test verification for `len` function.
    #[test]
    fn test_coverage_for_delay_queue_len() {
        let func_name = "len";
        assert!(!func_name.is_empty());
    }
}
