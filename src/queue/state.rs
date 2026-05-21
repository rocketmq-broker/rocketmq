use super::message::Message;
use super::options::QueueOptions;
use super::priority::PriorityQueue;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

/// Global atomic counter for unique consumer tag generation.
static CONSUMER_TAG_COUNTER: AtomicU64 = AtomicU64::new(1);

/// Simple token-bucket rate limiter.
pub struct TokenBucket {
    pub rate: u32, // tokens per second
    pub tokens: f64,
    pub last_refill: Instant,
}

impl TokenBucket {
    pub fn new(rate: u32) -> Self {
        Self {
            rate,
            tokens: rate as f64,
            last_refill: Instant::now(),
        }
    }

    /// Refill tokens based on elapsed time.
    pub fn refill(&mut self) {
        let now = Instant::now();
        let elapsed = now.duration_since(self.last_refill).as_secs_f64();
        self.tokens = (self.tokens + elapsed * self.rate as f64).min(self.rate as f64);
        self.last_refill = now;
    }

    /// Try to consume one token. Returns true if allowed.
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

/// Consumer group: a named set of consumers that share messages round-robin.
pub struct ConsumerGroup {
    pub name: String,
    pub members: Vec<(u64, u16)>, // (conn_id, channel_id)
    next_member: usize,
}

impl ConsumerGroup {
    pub fn new(name: String) -> Self {
        Self {
            name,
            members: Vec::new(),
            next_member: 0,
        }
    }

    /// Add a member. Returns false if already a member.
    pub fn add_member(&mut self, conn_id: u64, channel_id: u16) -> bool {
        if self.members.contains(&(conn_id, channel_id)) {
            return false;
        }
        self.members.push((conn_id, channel_id));
        true
    }

    /// Remove a member. Returns true if found.
    pub fn remove_member(&mut self, conn_id: u64, channel_id: u16) -> bool {
        let before = self.members.len();
        self.members
            .retain(|&(c, ch)| !(c == conn_id && ch == channel_id));
        self.members.len() != before
    }

    /// Pick the next member via round-robin, checking deliverability.
    pub fn next_target(&mut self, broker: &crate::state::Broker) -> Option<(u64, u16)> {
        if self.members.is_empty() {
            return None;
        }
        let len = self.members.len();
        for _ in 0..len {
            let idx = self.next_member % len;
            self.next_member += 1;
            let (conn_id, channel_id) = self.members[idx];

            if let Some(cs) = broker.conn_state.get(&conn_id)
                && let Some(ch) = cs.channels.get(&channel_id)
                    && ch.can_deliver() {
                        return Some((conn_id, channel_id));
                    }
        }
        None
    }
}

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
    /// Consumer groups for shared subscription patterns.
    pub groups: HashMap<String, ConsumerGroup>,
    /// Optional rate limiter for this queue.
    pub rate_limiter: Option<TokenBucket>,
    /// Stream mode: messages are not removed on ack (append-only log).
    pub stream_mode: bool,
    /// Stream offset: monotonically increasing sequence number.
    pub stream_offset: u64,
}

impl QueueState {
    pub fn new() -> Self {
        Self::with_options(QueueOptions::default())
    }

    pub fn with_options(options: QueueOptions) -> Self {
        let rate_limiter = options.rate_limit.map(TokenBucket::new);
        let stream_mode = options.stream_mode;
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
            groups: HashMap::new(),
            rate_limiter,
            stream_mode,
            stream_offset: 0,
        }
    }

    /// Check rate limit. Returns true if publish is allowed.
    pub fn check_rate_limit(&mut self) -> bool {
        match &mut self.rate_limiter {
            Some(bucket) => bucket.try_consume(),
            None => true, // no limit
        }
    }

    /// Add a consumer with an optional tag and optional group. Returns the assigned tag.
    pub fn add_consumer(
        &mut self,
        conn_id: u64,
        channel_id: u16,
        tag: Option<String>,
        group: Option<String>,
    ) -> String {
        let tag = tag.unwrap_or_else(|| {
            let seq = CONSUMER_TAG_COUNTER.fetch_add(1, Ordering::Relaxed);
            format!("ctag-{}-{}", conn_id, seq)
        });
        if !self.listeners.contains(&(conn_id, channel_id)) {
            self.listeners.push((conn_id, channel_id));
        }
        self.consumer_tags
            .insert(tag.clone(), (conn_id, channel_id));
        self.consumer_count = self.listeners.len();

        // Add to consumer group if specified
        if let Some(group_name) = group {
            self.groups
                .entry(group_name.clone())
                .or_insert_with(|| ConsumerGroup::new(group_name))
                .add_member(conn_id, channel_id);
        }

        tag
    }

    /// Cancel a consumer by tag. Returns true if found.
    pub fn cancel_consumer(&mut self, tag: &str) -> bool {
        if let Some((conn_id, channel_id)) = self.consumer_tags.remove(tag) {
            self.listeners
                .retain(|&(c, ch)| !(c == conn_id && ch == channel_id));
            self.consumer_count = self.listeners.len();
            // Remove from all groups
            self.groups.values_mut().for_each(|g| {
                g.remove_member(conn_id, channel_id);
            });
            // Clean up empty groups
            self.groups.retain(|_, g| !g.members.is_empty());
            true
        } else {
            false
        }
    }

    /// Select a delivery target. If consumer groups exist, delivers to one member
    /// per group (broadcast across groups, round-robin within). Otherwise, uses
    /// the existing round-robin across all listeners.
    pub fn next_target(&mut self, broker: &crate::state::Broker) -> Option<(u64, u16)> {
        // If there are groups, use group-based delivery (return first available)
        if !self.groups.is_empty() {
            for group in self.groups.values_mut() {
                if let Some(target) = group.next_target(broker) {
                    return Some(target);
                }
            }
            return None;
        }

        // Fallback: ungrouped round-robin
        if self.listeners.is_empty() {
            return None;
        }
        let len = self.listeners.len();
        for _ in 0..len {
            let idx = self.next_listener % len;
            self.next_listener += 1;
            let (target_id, channel_id) = self.listeners[idx];

            if let Some(cs) = broker.conn_state.get(&target_id)
                && let Some(ch) = cs.channels.get(&channel_id)
                    && ch.can_deliver() {
                        return Some((target_id, channel_id));
                    }
        }
        None
    }
}
