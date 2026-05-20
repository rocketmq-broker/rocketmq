//! Virtual host support for namespace isolation.
//!
//! Each VHost has its own set of exchanges, queues, and bindings.
//! The default vhost "/" is always present.

use dashmap::DashMap;
use std::collections::HashMap;
use tokio::sync::RwLock;

use crate::queue::QueueState;
use crate::routing::exchange::{Exchange, create_default_exchanges};

/// A virtual host providing namespace isolation for exchanges and queues.
pub struct VHost {
    pub name: String,
    pub exchanges: RwLock<HashMap<String, Exchange>>,
    pub queues: DashMap<String, QueueState>,
}

impl VHost {
    /// Create a new vhost with the default AMQP exchanges.
    pub fn new(name: String) -> Self {
        Self {
            name,
            exchanges: RwLock::new(create_default_exchanges()),
            queues: DashMap::new(),
        }
    }

    /// Create a new vhost with no default exchanges (for testing).
    pub fn empty(name: String) -> Self {
        Self {
            name,
            exchanges: RwLock::new(HashMap::new()),
            queues: DashMap::new(),
        }
    }
}

/// Default vhost name.
pub const DEFAULT_VHOST: &str = "/";

#[cfg(test)]
mod tests {
    use super::*;
    use crate::queue::QueueOptions;

    #[tokio::test]
    async fn vhost_new_has_default_exchanges() {
        let vh = VHost::new("/".to_string());
        let ex = vh.exchanges.read().await;
        assert_eq!(ex.len(), 5);
        assert!(ex.contains_key(""));
        assert!(ex.contains_key("amq.direct"));
        assert!(ex.contains_key("amq.fanout"));
        assert!(ex.contains_key("amq.topic"));
        assert!(ex.contains_key("amq.headers"));
    }

    #[test]
    fn vhost_empty_has_no_exchanges() {
        let vh = VHost::empty("test".to_string());
        // Can't await in sync test, so use try_read
        let ex = vh.exchanges.try_read().unwrap();
        assert_eq!(ex.len(), 0);
    }

    #[test]
    fn vhost_queues_isolated() {
        let vh1 = VHost::new("vh1".to_string());
        let vh2 = VHost::new("vh2".to_string());

        vh1.queues.insert("shared-name".into(), QueueState::new());

        assert!(vh1.queues.contains_key("shared-name"));
        assert!(!vh2.queues.contains_key("shared-name"));
    }

    #[test]
    fn vhost_queue_operations() {
        let vh = VHost::new("/".to_string());

        // Declare queue
        let opts = QueueOptions::default();
        vh.queues
            .insert("q1".into(), QueueState::with_options(opts));
        assert!(vh.queues.contains_key("q1"));

        // Delete queue
        vh.queues.remove("q1");
        assert!(!vh.queues.contains_key("q1"));
    }

    #[test]
    fn vhost_name_stored() {
        let vh = VHost::new("/production".to_string());
        assert_eq!(vh.name, "/production");
    }

    #[tokio::test]
    async fn vhost_exchange_declare() {
        use crate::routing::exchange::ExchangeType;

        let vh = VHost::new("/".to_string());
        {
            let mut ex = vh.exchanges.write().await;
            ex.insert(
                "custom.direct".to_string(),
                Exchange::new("custom.direct".to_string(), ExchangeType::Direct, false),
            );
        }
        let ex = vh.exchanges.read().await;
        assert_eq!(ex.len(), 6); // 5 default + 1 custom
        assert!(ex.contains_key("custom.direct"));
    }

    #[tokio::test]
    async fn vhost_exchange_delete() {
        let vh = VHost::new("/".to_string());
        {
            let mut ex = vh.exchanges.write().await;
            ex.remove("amq.headers");
        }
        let ex = vh.exchanges.read().await;
        assert_eq!(ex.len(), 4);
        assert!(!ex.contains_key("amq.headers"));
    }
}
