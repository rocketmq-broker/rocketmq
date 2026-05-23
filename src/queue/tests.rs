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
// File: tests.rs
// Description: Unit and integration tests for queue state and behavior.

use super::priority::PriorityQueue;
use super::{DelayQueue, Message, QueueOptions, QueueState};
use std::time::{Duration, Instant};

/// Executes the standard queue options from headers full lifecycle step.
///
/// Executes the required business logic for queue options from headers full.
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

/// Executes the standard queue options from headers minimal lifecycle step.
///
/// Executes the required business logic for queue options from headers minimal.
#[test]
fn queue_options_from_headers_minimal() {
    let input = "name:q1\r\n";
    let (name, opts) = QueueOptions::from_headers(input);
    assert_eq!(name, "q1");
    assert!(!opts.durable);
    assert!(opts.message_ttl.is_none());
}

/// Executes the standard queue options default values lifecycle step.
///
/// Executes the required business logic for queue options default values.
#[test]
fn queue_options_default_values() {
    let opts = QueueOptions::default();
    assert!(!opts.durable);
    assert!(!opts.exclusive);
    assert_eq!(opts.max_priority, 0);
}

/// Executes the standard queue options ignores unknown keys lifecycle step.
///
/// Executes the required business logic for queue options ignores unknown keys.
#[test]
fn queue_options_ignores_unknown_keys() {
    let input = "name:q1\r\nunknown:value\r\n";
    let (name, _) = QueueOptions::from_headers(input);
    assert_eq!(name, "q1");
}

/// Executes the standard queue options invalid ttl ignored lifecycle step.
///
/// Executes the required business logic for queue options invalid ttl ignored.
#[test]
fn queue_options_invalid_ttl_ignored() {
    let input = "name:q1\r\nmessage_ttl:bad\r\n";
    let (_, opts) = QueueOptions::from_headers(input);
    assert!(opts.message_ttl.is_none());
}

/// Executes the standard message new defaults lifecycle step.
///
/// Executes the required business logic for message new defaults.
#[test]
fn message_new_defaults() {
    let msg = Message::new(1, vec![], vec![1, 2, 3]);
    assert_eq!(msg.id, 1);
    assert_eq!(msg.priority, 0);
    assert!(!msg.redelivered);
    assert!(!msg.is_expired());
}

/// Executes the standard message expired lifecycle step.
///
/// Executes the required business logic for message expired.
#[test]
fn message_expired() {
    let msg = Message {
        id: 1,
        headers: vec![],
        body: vec![],
        priority: 0,
        expiration: Some(Instant::now() - Duration::from_secs(1)),
        redelivered: false,
        delivery_count: 0,
        exchange: "".to_string(),
        routing_key: "".to_string(),
    };
    assert!(msg.is_expired());
}

/// Executes the standard message not expired lifecycle step.
///
/// Executes the required business logic for message not expired.
#[test]
fn message_not_expired() {
    let msg = Message {
        id: 1,
        headers: vec![],
        body: vec![],
        priority: 0,
        expiration: Some(Instant::now() + Duration::from_secs(60)),
        redelivered: false,
        delivery_count: 0,
        exchange: "".to_string(),
        routing_key: "".to_string(),
    };
    assert!(!msg.is_expired());
}

/// Executes the standard priority queue fifo same priority lifecycle step.
///
/// Executes the required business logic for priority queue fifo same priority.
#[test]
fn priority_queue_fifo_same_priority() {
    let mut pq = PriorityQueue::new();
    pq.push_back(crate::queue::message::QueueMessage::Full(Message::new(
        1,
        vec![],
        b"first".to_vec(),
    )));
    pq.push_back(crate::queue::message::QueueMessage::Full(Message::new(
        2,
        vec![],
        b"second".to_vec(),
    )));
    pq.push_back(crate::queue::message::QueueMessage::Full(Message::new(
        3,
        vec![],
        b"third".to_vec(),
    )));
    assert_eq!(pq.len(), 3);
    assert_eq!(pq.pop_front().unwrap().unwrap_full().body, b"first");
    assert_eq!(pq.pop_front().unwrap().unwrap_full().body, b"second");
    assert_eq!(pq.pop_front().unwrap().unwrap_full().body, b"third");
    assert!(pq.pop_front().is_none());
}

/// Executes the standard priority queue higher priority first lifecycle step.
///
/// Executes the required business logic for priority queue higher priority first.
#[test]
fn priority_queue_higher_priority_first() {
    let mut pq = PriorityQueue::new();
    let mut low = Message::new(1, vec![], b"low".to_vec());
    low.priority = 1;
    let mut high = Message::new(2, vec![], b"high".to_vec());
    high.priority = 10;
    let mut mid = Message::new(3, vec![], b"mid".to_vec());
    mid.priority = 5;
    pq.push_back(crate::queue::message::QueueMessage::Full(low));
    pq.push_back(crate::queue::message::QueueMessage::Full(high));
    pq.push_back(crate::queue::message::QueueMessage::Full(mid));
    assert_eq!(pq.pop_front().unwrap().unwrap_full().body, b"high");
    assert_eq!(pq.pop_front().unwrap().unwrap_full().body, b"mid");
    assert_eq!(pq.pop_front().unwrap().unwrap_full().body, b"low");
}

/// Executes the standard priority queue push front stays at front lifecycle step.
///
/// Executes the required business logic for priority queue push front stays at front.
#[test]
fn priority_queue_push_front_stays_at_front() {
    let mut pq = PriorityQueue::new();
    pq.push_back(crate::queue::message::QueueMessage::Full(Message::new(
        1,
        vec![],
        b"back".to_vec(),
    )));
    pq.push_front(crate::queue::message::QueueMessage::Full(Message::new(
        2,
        vec![],
        b"front".to_vec(),
    )));
    assert_eq!(pq.pop_front().unwrap().unwrap_full().body, b"front");
}

/// Executes the standard priority queue pop oldest evicts lowest lifecycle step.
///
/// Executes the required business logic for priority queue pop oldest evicts lowest.
#[test]
fn priority_queue_pop_oldest_evicts_lowest() {
    let mut pq = PriorityQueue::new();
    let mut low = Message::new(1, vec![], b"low".to_vec());
    low.priority = 0;
    let mut high = Message::new(2, vec![], b"high".to_vec());
    high.priority = 10;
    pq.push_back(crate::queue::message::QueueMessage::Full(low));
    pq.push_back(crate::queue::message::QueueMessage::Full(high));
    assert_eq!(pq.pop_oldest().unwrap().unwrap_full().body, b"low");
    assert_eq!(pq.len(), 1);
}

/// Executes the standard priority queue empty operations lifecycle step.
///
/// Executes the required business logic for priority queue empty operations.
#[test]
fn priority_queue_empty_operations() {
    let mut pq = PriorityQueue::new();
    assert_eq!(pq.len(), 0);
    assert!(pq.pop_front().is_none());
    assert!(pq.pop_oldest().is_none());
}

/// Executes the standard queue state round robin lifecycle step.
///
/// Executes the required business logic for queue state round robin.
#[test]
fn queue_state_round_robin() {
    let mut q = QueueState::new();
    let bs = crate::state::BrokerState::new();
    // Since cs.channels.get returns none for nonexistent, we need to populate connection state for tests to pass prefetch check
    bs.conn_state
        .insert(10, crate::state::ConnectionState::new());
    bs.conn_state
        .insert(20, crate::state::ConnectionState::new());
    bs.conn_state
        .insert(30, crate::state::ConnectionState::new());
    bs.conn_state
        .get_mut(&10)
        .unwrap()
        .channels
        .insert(1, crate::state::ChannelState::new(1));
    bs.conn_state
        .get_mut(&20)
        .unwrap()
        .channels
        .insert(1, crate::state::ChannelState::new(1));
    bs.conn_state
        .get_mut(&30)
        .unwrap()
        .channels
        .insert(1, crate::state::ChannelState::new(1));

    q.listeners = vec![(10, 1), (20, 1), (30, 1)];
    assert_eq!(q.next_target(&bs.into()), Some((10, 1)));

    let bs2 = crate::state::BrokerState::new();
    bs2.conn_state
        .insert(10, crate::state::ConnectionState::new());
    bs2.conn_state
        .insert(20, crate::state::ConnectionState::new());
    bs2.conn_state
        .insert(30, crate::state::ConnectionState::new());
    bs2.conn_state
        .get_mut(&10)
        .unwrap()
        .channels
        .insert(1, crate::state::ChannelState::new(1));
    bs2.conn_state
        .get_mut(&20)
        .unwrap()
        .channels
        .insert(1, crate::state::ChannelState::new(1));
    bs2.conn_state
        .get_mut(&30)
        .unwrap()
        .channels
        .insert(1, crate::state::ChannelState::new(1));
    assert_eq!(q.next_target(&bs2.into()), Some((20, 1)));

    let bs3 = crate::state::BrokerState::new();
    bs3.conn_state
        .insert(10, crate::state::ConnectionState::new());
    bs3.conn_state
        .insert(20, crate::state::ConnectionState::new());
    bs3.conn_state
        .insert(30, crate::state::ConnectionState::new());
    bs3.conn_state
        .get_mut(&10)
        .unwrap()
        .channels
        .insert(1, crate::state::ChannelState::new(1));
    bs3.conn_state
        .get_mut(&20)
        .unwrap()
        .channels
        .insert(1, crate::state::ChannelState::new(1));
    bs3.conn_state
        .get_mut(&30)
        .unwrap()
        .channels
        .insert(1, crate::state::ChannelState::new(1));
    assert_eq!(q.next_target(&bs3.into()), Some((30, 1)));

    let bs4 = crate::state::BrokerState::new();
    bs4.conn_state
        .insert(10, crate::state::ConnectionState::new());
    bs4.conn_state
        .insert(20, crate::state::ConnectionState::new());
    bs4.conn_state
        .insert(30, crate::state::ConnectionState::new());
    bs4.conn_state
        .get_mut(&10)
        .unwrap()
        .channels
        .insert(1, crate::state::ChannelState::new(1));
    bs4.conn_state
        .get_mut(&20)
        .unwrap()
        .channels
        .insert(1, crate::state::ChannelState::new(1));
    bs4.conn_state
        .get_mut(&30)
        .unwrap()
        .channels
        .insert(1, crate::state::ChannelState::new(1));
    assert_eq!(q.next_target(&bs4.into()), Some((10, 1)));
}

/// Executes the standard queue state no listeners lifecycle step.
///
/// Executes the required business logic for queue state no listeners.
#[test]
fn queue_state_no_listeners() {
    let mut q = QueueState::new();
    let bs = crate::state::BrokerState::new();
    assert_eq!(q.next_target(&bs.into()), None);
}

/// Executes the standard consumer tag auto generated lifecycle step.
///
/// Executes the required business logic for consumer tag auto generated.
#[test]
fn consumer_tag_auto_generated() {
    let mut q = QueueState::new();
    let tag = q.add_consumer(10, 1, None, None);
    assert_eq!(tag, "ctag-10-1");
    assert_eq!(q.listeners.len(), 1);
    assert_eq!(q.consumer_count, 1);
}

/// Executes the standard consumer tag custom lifecycle step.
///
/// Executes the required business logic for consumer tag custom.
#[test]
fn consumer_tag_custom() {
    let mut q = QueueState::new();
    let tag = q.add_consumer(10, 1, Some("my-worker".to_string()), None);
    assert_eq!(tag, "my-worker");
    assert!(q.consumer_tags.contains_key("my-worker"));
}

/// Executes the standard consumer cancel by tag lifecycle step.
///
/// Executes the required business logic for consumer cancel by tag.
#[test]
fn consumer_cancel_by_tag() {
    let mut q = QueueState::new();
    q.add_consumer(10, 1, Some("worker-1".to_string()), None);
    q.add_consumer(20, 1, Some("worker-2".to_string()), None);
    assert_eq!(q.listeners.len(), 2);

    assert!(q.cancel_consumer("worker-1"));
    assert_eq!(q.listeners.len(), 1);
    assert_eq!(q.listeners[0], (20, 1));
    assert_eq!(q.consumer_count, 1);

    // Cancel unknown tag returns false
    assert!(!q.cancel_consumer("nonexistent"));
}

/// Executes the standard consumer add idempotent lifecycle step.
///
/// Executes the required business logic for consumer add idempotent.
#[test]
fn consumer_add_idempotent() {
    let mut q = QueueState::new();
    q.add_consumer(10, 1, Some("tag-a".to_string()), None);
    q.add_consumer(10, 1, Some("tag-b".to_string()), None);
    // Same conn_id+channel_id should not duplicate in listeners
    assert_eq!(q.listeners.len(), 1);
    // But both tags should be tracked
    assert!(q.consumer_tags.contains_key("tag-a"));
    assert!(q.consumer_tags.contains_key("tag-b"));
}

/// Executes the standard delay queue schedule and drain lifecycle step.
///
/// Executes the required business logic for delay queue schedule and drain.
#[test]
fn delay_queue_schedule_and_drain() {
    let dq = DelayQueue::new();
    let msg = Message::new(1, vec![], b"delayed".to_vec());
    dq.schedule("q1".to_string(), msg, Duration::from_millis(1));
    assert_eq!(dq.len(), 1);

    // Not ready yet (might be, but let's test drain_ready logic)
    std::thread::sleep(Duration::from_millis(5));
    let ready = dq.drain_ready();
    assert_eq!(ready.len(), 1);
    assert_eq!(ready[0].queue_name, "q1");
    assert_eq!(ready[0].message.body, b"delayed");
    assert_eq!(dq.len(), 0);
}

/// Executes the standard delay queue future not drained lifecycle step.
///
/// Executes the required business logic for delay queue future not drained.
#[test]
fn delay_queue_future_not_drained() {
    let dq = DelayQueue::new();
    let msg = Message::new(1, vec![], b"future".to_vec());
    dq.schedule("q1".to_string(), msg, Duration::from_secs(60));
    let ready = dq.drain_ready();
    assert!(ready.is_empty());
    assert_eq!(dq.len(), 1);
}

/// Executes the standard queue options parses new fields lifecycle step.
///
/// Executes the required business logic for queue options parses new fields.
#[test]
fn queue_options_parses_new_fields() {
    let input = "name:q1\r\nx-expires:30000\r\nx-max-retries:5\r\nx-retry-delay:1000\r\nx-retry-multiplier:2.0\r\n";
    let (name, opts) = QueueOptions::from_headers(input);
    assert_eq!(name, "q1");
    assert_eq!(opts.expires, Some(Duration::from_millis(30000)));
    assert_eq!(opts.max_retries, Some(5));
    assert_eq!(opts.retry_delay_ms, Some(1000));
    assert_eq!(opts.retry_multiplier, Some(2.0));
}

/// Executes the standard message delivery count default lifecycle step.
///
/// Executes the required business logic for message delivery count default.
#[test]
fn message_delivery_count_default() {
    let msg = Message::new(1, vec![], vec![]);
    assert_eq!(msg.delivery_count, 0);
}

/// Executes the standard priority queue peek front lifecycle step.
///
/// Executes the required business logic for priority queue peek front.
#[test]
fn priority_queue_peek_front() {
    let mut pq = PriorityQueue::new();
    assert!(pq.peek_front().is_none());
    pq.push_back(crate::queue::message::QueueMessage::Full(Message::new(
        1,
        vec![],
        b"a".to_vec(),
    )));
    assert_eq!(pq.peek_front().unwrap().clone().unwrap_full().body, b"a");
    assert_eq!(pq.len(), 1); // peek doesn't remove
}

// ──────────────────────────────────────────────
// Consumer Group tests (3.2)
// ──────────────────────────────────────────────

use super::state::ConsumerGroup;

/// Executes the standard consumer group add member lifecycle step.
///
/// Executes the required business logic for consumer group add member.
#[test]
fn consumer_group_add_member() {
    let mut g = ConsumerGroup::new("workers".to_string());
    assert!(g.add_member(1, 1));
    assert_eq!(g.members.len(), 1);
}

/// Executes the standard consumer group add duplicate rejected lifecycle step.
///
/// Executes the required business logic for consumer group add duplicate rejected.
#[test]
fn consumer_group_add_duplicate_rejected() {
    let mut g = ConsumerGroup::new("workers".to_string());
    assert!(g.add_member(1, 1));
    assert!(!g.add_member(1, 1)); // duplicate
    assert_eq!(g.members.len(), 1);
}

/// Executes the standard consumer group remove member lifecycle step.
///
/// Executes the required business logic for consumer group remove member.
#[test]
fn consumer_group_remove_member() {
    let mut g = ConsumerGroup::new("workers".to_string());
    g.add_member(1, 1);
    g.add_member(2, 1);
    assert!(g.remove_member(1, 1));
    assert_eq!(g.members.len(), 1);
    assert_eq!(g.members[0], (2, 1));
}

/// Executes the standard consumer group remove nonexistent lifecycle step.
///
/// Executes the required business logic for consumer group remove nonexistent.
#[test]
fn consumer_group_remove_nonexistent() {
    let mut g = ConsumerGroup::new("workers".to_string());
    assert!(!g.remove_member(99, 1));
}

/// Executes the standard queue add consumer with group lifecycle step.
///
/// Executes the required business logic for queue add consumer with group.
#[test]
fn queue_add_consumer_with_group() {
    let mut q = QueueState::new();
    let tag = q.add_consumer(1, 1, Some("w1".to_string()), Some("workers".to_string()));
    assert_eq!(tag, "w1");
    assert!(q.groups.contains_key("workers"));
    assert_eq!(q.groups["workers"].members.len(), 1);
}

/// Executes the standard queue multiple consumers same group lifecycle step.
///
/// Executes the required business logic for queue multiple consumers same group.
#[test]
fn queue_multiple_consumers_same_group() {
    let mut q = QueueState::new();
    q.add_consumer(1, 1, Some("w1".to_string()), Some("g1".to_string()));
    q.add_consumer(2, 1, Some("w2".to_string()), Some("g1".to_string()));
    assert_eq!(q.groups.len(), 1);
    assert_eq!(q.groups["g1"].members.len(), 2);
}

/// Executes the standard queue multiple groups lifecycle step.
///
/// Executes the required business logic for queue multiple groups.
#[test]
fn queue_multiple_groups() {
    let mut q = QueueState::new();
    q.add_consumer(1, 1, Some("a".to_string()), Some("g1".to_string()));
    q.add_consumer(2, 1, Some("b".to_string()), Some("g2".to_string()));
    assert_eq!(q.groups.len(), 2);
}

/// Executes the standard queue cancel consumer removes from group lifecycle step.
///
/// Executes the required business logic for queue cancel consumer removes from group.
#[test]
fn queue_cancel_consumer_removes_from_group() {
    let mut q = QueueState::new();
    q.add_consumer(1, 1, Some("w1".to_string()), Some("workers".to_string()));
    q.add_consumer(2, 1, Some("w2".to_string()), Some("workers".to_string()));

    q.cancel_consumer("w1");
    assert_eq!(q.groups["workers"].members.len(), 1);
}

/// Executes the standard queue cancel last consumer removes group lifecycle step.
///
/// Executes the required business logic for queue cancel last consumer removes group.
#[test]
fn queue_cancel_last_consumer_removes_group() {
    let mut q = QueueState::new();
    q.add_consumer(1, 1, Some("w1".to_string()), Some("workers".to_string()));

    q.cancel_consumer("w1");
    // Empty group should be cleaned up
    assert!(q.groups.is_empty());
}

/// Executes the standard queue consumer no group lifecycle step.
///
/// Executes the required business logic for queue consumer no group.
#[test]
fn queue_consumer_no_group() {
    let mut q = QueueState::new();
    q.add_consumer(1, 1, Some("solo".to_string()), None);
    assert!(q.groups.is_empty());
}

// ──────────────────────────────────────────────
// Token Bucket / Rate Limiting tests (3.5)
// ──────────────────────────────────────────────

use super::state::TokenBucket;

/// Executes the standard token bucket initial full lifecycle step.
///
/// Executes the required business logic for token bucket initial full.
#[test]
fn token_bucket_initial_full() {
    let tb = TokenBucket::new(100);
    assert_eq!(tb.rate, 100);
    assert_eq!(tb.tokens, 100.0);
}

/// Executes the standard token bucket consume lifecycle step.
///
/// Executes the required business logic for token bucket consume.
#[test]
fn token_bucket_consume() {
    let mut tb = TokenBucket::new(10);
    assert!(tb.try_consume());
    assert!(tb.tokens < 10.0);
}

/// Executes the standard token bucket exhaustion lifecycle step.
///
/// Executes the required business logic for token bucket exhaustion.
#[test]
fn token_bucket_exhaustion() {
    let mut tb = TokenBucket::new(2);
    tb.tokens = 0.5; // Simulate exhausted
    tb.last_refill = std::time::Instant::now(); // No time to refill
    assert!(!tb.try_consume());
}

/// Executes the standard token bucket refill lifecycle step.
///
/// Executes the required business logic for token bucket refill.
#[test]
fn token_bucket_refill() {
    let mut tb = TokenBucket::new(1000);
    tb.tokens = 0.0;
    tb.last_refill = std::time::Instant::now() - Duration::from_secs(1);
    tb.refill();
    assert!(tb.tokens >= 999.0); // ~1000 tokens refilled in 1 second
}

/// Executes the standard token bucket caps at rate lifecycle step.
///
/// Executes the required business logic for token bucket caps at rate.
#[test]
fn token_bucket_caps_at_rate() {
    let mut tb = TokenBucket::new(10);
    tb.tokens = 10.0;
    tb.last_refill = std::time::Instant::now() - Duration::from_secs(100);
    tb.refill();
    assert!(tb.tokens <= 10.0); // Capped
}

/// Executes the standard queue rate limit from options lifecycle step.
///
/// Executes the required business logic for queue rate limit from options.
#[test]
fn queue_rate_limit_from_options() {
    let mut opts = QueueOptions::default();
    opts.rate_limit = Some(100);
    let mut q = QueueState::with_options(opts);
    assert!(q.rate_limiter.is_some());
    assert!(q.check_rate_limit()); // Should pass (full bucket)
}

/// Executes the standard queue no rate limit lifecycle step.
///
/// Executes the required business logic for queue no rate limit.
#[test]
fn queue_no_rate_limit() {
    let q = QueueState::new();
    assert!(q.rate_limiter.is_none());
}

/// Executes the standard queue check rate limit no limiter lifecycle step.
///
/// Executes the required business logic for queue check rate limit no limiter.
#[test]
fn queue_check_rate_limit_no_limiter() {
    let mut q = QueueState::new();
    assert!(q.check_rate_limit()); // Always true when no limiter
}

// ──────────────────────────────────────────────
// Stream Mode tests (3.6)
// ──────────────────────────────────────────────

/// Executes the standard stream mode default off lifecycle step.
///
/// Executes the required business logic for stream mode default off.
#[test]
fn stream_mode_default_off() {
    let q = QueueState::new();
    assert!(!q.stream_mode);
    assert_eq!(q.stream_offset, 0);
}

/// Executes the standard stream mode from options lifecycle step.
///
/// Executes the required business logic for stream mode from options.
#[test]
fn stream_mode_from_options() {
    let mut opts = QueueOptions::default();
    opts.stream_mode = true;
    let q = QueueState::with_options(opts);
    assert!(q.stream_mode);
}

/// Executes the standard stream mode from headers lifecycle step.
///
/// Executes the required business logic for stream mode from headers.
#[test]
fn stream_mode_from_headers() {
    let input = "name:events\r\nx-queue-type:stream\r\n";
    let (name, opts) = QueueOptions::from_headers(input);
    assert_eq!(name, "events");
    assert!(opts.stream_mode);
}

/// Executes the standard stream offset tracking lifecycle step.
///
/// Executes the required business logic for stream offset tracking.
#[test]
fn stream_offset_tracking() {
    let mut q = QueueState::new();
    q.stream_mode = true;
    q.stream_offset = 42;
    assert_eq!(q.stream_offset, 42);
    q.stream_offset += 1;
    assert_eq!(q.stream_offset, 43);
}

// ──────────────────────────────────────────────
// Options parsing for new fields
// ──────────────────────────────────────────────

/// Executes the standard options rate limit from headers lifecycle step.
///
/// Executes the required business logic for options rate limit from headers.
#[test]
fn options_rate_limit_from_headers() {
    let input = "name:q1\r\nx-rate-limit:500\r\n";
    let (name, opts) = QueueOptions::from_headers(input);
    assert_eq!(name, "q1");
    assert_eq!(opts.rate_limit, Some(500));
}

/// Executes the standard options stream type non stream lifecycle step.
///
/// Executes the required business logic for options stream type non stream.
#[test]
fn options_stream_type_non_stream() {
    let input = "name:q1\r\nx-queue-type:classic\r\n";
    let (_, opts) = QueueOptions::from_headers(input);
    assert!(!opts.stream_mode);
}
