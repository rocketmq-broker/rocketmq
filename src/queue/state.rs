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
// File: state.rs
// Description: Individual queue state management, delivery tracking, and consumer subscription.

use super::message::Message;
use super::options::{QueueOptions, QueueType};
use super::priority::PriorityQueue;
use crate::schema::CompiledSchema;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

/// Global atomic counter for unique consumer tag generation.
static CONSUMER_TAG_COUNTER: AtomicU64 = AtomicU64::new(1);

pub struct TokenBucket {
    pub rate: u32,
    pub tokens: f64,
    pub last_refill: Instant,
}

impl TokenBucket {
    /// Creates a new instance with the given rate.
    pub fn new(rate: u32) -> Self {
        Self {
            rate,
            tokens: rate as f64,
            last_refill: Instant::now(),
        }
    }

    pub fn refill(&mut self) {
        let now = Instant::now();
        let elapsed = now.duration_since(self.last_refill).as_secs_f64();
        self.tokens = (self.tokens + elapsed * self.rate as f64).min(self.rate as f64);
        self.last_refill = now;
    }

    pub fn try_consume(&mut self) -> bool {
        self.refill();
        if self.tokens >= 1.0 {
            self.tokens -= 1.0;
            true
        } else {
            false
        }
    }
}

pub struct ConsumerGroup {
    pub name: String,
    pub members: Vec<(u64, u16)>,
    next_member: usize,
}

impl ConsumerGroup {
    /// Creates a new instance with the given name.
    pub fn new(name: String) -> Self {
        Self {
            name,
            members: Vec::new(),
            next_member: 0,
        }
    }

    pub fn add_member(&mut self, conn_id: u64, channel_id: u16) -> bool {
        if self.members.contains(&(conn_id, channel_id)) {
            return false;
        }
        self.members.push((conn_id, channel_id));
        true
    }

    pub fn remove_member(&mut self, conn_id: u64, channel_id: u16) -> bool {
        let before = self.members.len();
        self.members
            .retain(|&(c, ch)| !(c == conn_id && ch == channel_id));
        self.members.len() != before
    }

    pub fn next_target(&mut self, broker: &crate::state::Broker) -> Option<(u64, u16)> {
        if self.members.is_empty() {
            return None;
        }
        let len = self.members.len();
        for _ in 0..len {
            let idx = self.next_member % len;
            self.next_member += 1;
            let (conn_id, channel_id) = self.members[idx];

            if broker.conn_state.contains_key(&conn_id) {
                return Some((conn_id, channel_id));
            }
        }
        None
    }
}

/// Manages message queues, tracking unacknowledged and ready messages, and active consumers.
/// Runtime state for a single AMQP queue.
///
/// Tracks messages, consumer subscriptions, delivery tags, queue
/// options (TTL, durability, auto-delete), and last-activity timestamps.
/// Manages message queues, tracking unacknowledged and ready messages, and active consumers.
pub struct QueueState {
    pub options: QueueOptions,
    pub name_arc: std::sync::Arc<str>,
    pub owner_conn_id: Option<u64>,
    pub listeners: Vec<(u64, u16)>,
    pub messages: PriorityQueue,
    pub inflight: HashMap<u64, Message>,
    pub next_listener: usize,
    pub consumer_count: usize,
    pub consumer_tags: HashMap<String, (u64, u16, bool)>,
    pub active_consumers: std::sync::Arc<[(String, u64, u16, bool)]>,
    pub last_activity: Instant,
    pub groups: HashMap<String, ConsumerGroup>,
    pub rate_limiter: Option<TokenBucket>,
    pub stream_mode: bool,
    pub stream_offset: u64,
    pub stat_published: u64,
    pub stat_delivered: u64,
    pub stat_acked: u64,
    pub schema: Option<Arc<CompiledSchema>>,

    // ── Cluster HA (Sprint 2 + 3) ─────────────────────
    /// Replication strategy for this queue.
    pub queue_type: QueueType,
    /// Node ID of the current leader (the node owning the queue).
    /// For classic queues this is always the declaring node.
    pub leader_node: Option<u64>,
    /// Node IDs hosting replicas (including leader) for quorum queues.
    pub replica_nodes: Vec<u64>,
}

impl Default for QueueState {
    fn default() -> Self {
        Self::new()
    }
}

impl QueueState {
    /// Creates a new instance with default values.
    pub fn new() -> Self {
        Self::with_options(QueueOptions::default())
    }

    pub fn with_options(options: QueueOptions) -> Self {
        let rate_limiter = options.rate_limit.map(TokenBucket::new);
        let stream_mode = options.stream_mode;
        let queue_type = options.queue_type.clone();
        Self {
            options,
            name_arc: std::sync::Arc::from(""),
            owner_conn_id: None,
            listeners: Vec::new(),
            messages: PriorityQueue::new(),
            inflight: HashMap::new(),
            next_listener: 0,
            consumer_count: 0,
            consumer_tags: HashMap::new(),
            active_consumers: std::sync::Arc::new([]),
            last_activity: Instant::now(),
            groups: HashMap::new(),
            rate_limiter,
            stream_mode,
            stream_offset: 0,
            stat_published: 0,
            stat_delivered: 0,
            stat_acked: 0,
            schema: None,
            queue_type,
            leader_node: None,
            replica_nodes: Vec::new(),
        }
    }

    pub fn check_rate_limit(&mut self) -> bool {
        match &mut self.rate_limiter {
            Some(bucket) => bucket.try_consume(),
            None => true,
        }
    }

    pub fn add_consumer(
        &mut self,
        conn_id: u64,
        channel_id: u16,
        tag: Option<String>,
        group: Option<String>,
        no_ack: bool,
    ) -> String {
        let tag = tag.unwrap_or_else(|| {
            let seq = CONSUMER_TAG_COUNTER.fetch_add(1, Ordering::Relaxed);
            format!("ctag-{}-{}", conn_id, seq)
        });
        if !self.listeners.contains(&(conn_id, channel_id)) {
            self.listeners.push((conn_id, channel_id));
        }
        self.consumer_tags
            .insert(tag.clone(), (conn_id, channel_id, no_ack));
        self.consumer_count = self.listeners.len();

        if let Some(group_name) = group {
            self.groups
                .entry(group_name.clone())
                .or_insert_with(|| ConsumerGroup::new(group_name))
                .add_member(conn_id, channel_id);
        }

        self.active_consumers = self.consumer_tags.iter().map(|(t, &(c, ch, na))| (t.clone(), c, ch, na)).collect::<Vec<_>>().into();

        tag
    }

    pub fn cancel_consumer(&mut self, tag: &str) -> bool {
        if let Some((conn_id, channel_id, _no_ack)) = self.consumer_tags.remove(tag) {
            self.listeners
                .retain(|&(c, ch)| !(c == conn_id && ch == channel_id));
            self.consumer_count = self.listeners.len();

            self.groups.values_mut().for_each(|g| {
                g.remove_member(conn_id, channel_id);
            });

            self.groups.retain(|_, g| !g.members.is_empty());
            self.active_consumers = self.consumer_tags.iter().map(|(t, &(c, ch, na))| (t.clone(), c, ch, na)).collect::<Vec<_>>().into();
            true
        } else {
            false
        }
    }

    pub fn next_target(&mut self, broker: &crate::state::Broker) -> Option<(u64, u16)> {
        if !self.groups.is_empty() {
            for group in self.groups.values_mut() {
                if let Some(target) = group.next_target(broker) {
                    return Some(target);
                }
            }
            return None;
        }

        if self.listeners.is_empty() {
            return None;
        }
        let len = self.listeners.len();
        for _ in 0..len {
            let idx = self.next_listener % len;
            self.next_listener += 1;
            let (target_id, channel_id) = self.listeners[idx];

            if broker.conn_state.contains_key(&target_id) {
                return Some((target_id, channel_id));
            }
        }
        None
    }
}
