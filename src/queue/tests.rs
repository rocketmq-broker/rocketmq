use std::time::{Duration, Instant};
use super::{Message, QueueOptions, PriorityQueue, QueueState};

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

#[test]
fn queue_options_from_headers_minimal() {
    let input = "name:q1\r\n";
    let (name, opts) = QueueOptions::from_headers(input);
    assert_eq!(name, "q1");
    assert!(!opts.durable);
    assert!(opts.message_ttl.is_none());
}

#[test]
fn queue_options_default_values() {
    let opts = QueueOptions::default();
    assert!(!opts.durable);
    assert!(!opts.exclusive);
    assert_eq!(opts.max_priority, 0);
}

#[test]
fn queue_options_ignores_unknown_keys() {
    let input = "name:q1\r\nunknown:value\r\n";
    let (name, _) = QueueOptions::from_headers(input);
    assert_eq!(name, "q1");
}

#[test]
fn queue_options_invalid_ttl_ignored() {
    let input = "name:q1\r\nmessage_ttl:bad\r\n";
    let (_, opts) = QueueOptions::from_headers(input);
    assert!(opts.message_ttl.is_none());
}

#[test]
fn message_new_defaults() {
    let msg = Message::new(1, vec![], vec![1, 2, 3]);
    assert_eq!(msg.id, 1);
    assert_eq!(msg.priority, 0);
    assert!(!msg.redelivered);
    assert!(!msg.is_expired());
}

#[test]
fn message_expired() {
    let msg = Message {
        id: 1,
        headers: vec![],
        body: vec![],
        priority: 0,
        expiration: Some(Instant::now() - Duration::from_secs(1)),
        redelivered: false,
    };
    assert!(msg.is_expired());
}

#[test]
fn message_not_expired() {
    let msg = Message {
        id: 1,
        headers: vec![],
        body: vec![],
        priority: 0,
        expiration: Some(Instant::now() + Duration::from_secs(60)),
        redelivered: false,
    };
    assert!(!msg.is_expired());
}

#[test]
fn priority_queue_fifo_same_priority() {
    let mut pq = PriorityQueue::new();
    pq.push_back(Message::new(1, vec![], b"first".to_vec()));
    pq.push_back(Message::new(2, vec![], b"second".to_vec()));
    pq.push_back(Message::new(3, vec![], b"third".to_vec()));
    assert_eq!(pq.len(), 3);
    assert_eq!(pq.pop_front().unwrap().body, b"first");
    assert_eq!(pq.pop_front().unwrap().body, b"second");
    assert_eq!(pq.pop_front().unwrap().body, b"third");
    assert!(pq.pop_front().is_none());
}

#[test]
fn priority_queue_higher_priority_first() {
    let mut pq = PriorityQueue::new();
    let mut low = Message::new(1, vec![], b"low".to_vec());
    low.priority = 1;
    let mut high = Message::new(2, vec![], b"high".to_vec());
    high.priority = 10;
    let mut mid = Message::new(3, vec![], b"mid".to_vec());
    mid.priority = 5;
    pq.push_back(low);
    pq.push_back(high);
    pq.push_back(mid);
    assert_eq!(pq.pop_front().unwrap().body, b"high");
    assert_eq!(pq.pop_front().unwrap().body, b"mid");
    assert_eq!(pq.pop_front().unwrap().body, b"low");
}

#[test]
fn priority_queue_push_front_stays_at_front() {
    let mut pq = PriorityQueue::new();
    pq.push_back(Message::new(1, vec![], b"back".to_vec()));
    pq.push_front(Message::new(2, vec![], b"front".to_vec()));
    assert_eq!(pq.pop_front().unwrap().body, b"front");
}

#[test]
fn priority_queue_pop_oldest_evicts_lowest() {
    let mut pq = PriorityQueue::new();
    let mut low = Message::new(1, vec![], b"low".to_vec());
    low.priority = 0;
    let mut high = Message::new(2, vec![], b"high".to_vec());
    high.priority = 10;
    pq.push_back(low);
    pq.push_back(high);
    assert_eq!(pq.pop_oldest().unwrap().body, b"low");
    assert_eq!(pq.len(), 1);
}

#[test]
fn priority_queue_empty_operations() {
    let mut pq = PriorityQueue::new();
    assert_eq!(pq.len(), 0);
    assert!(pq.pop_front().is_none());
    assert!(pq.pop_oldest().is_none());
}

#[test]
fn queue_state_round_robin() {
    let mut q = QueueState::new();
    q.listeners = vec![10, 20, 30];
    assert_eq!(q.next_target(), Some(10));
    assert_eq!(q.next_target(), Some(20));
    assert_eq!(q.next_target(), Some(30));
    assert_eq!(q.next_target(), Some(10));
}

#[test]
fn queue_state_no_listeners() {
    let mut q = QueueState::new();
    assert_eq!(q.next_target(), None);
}
