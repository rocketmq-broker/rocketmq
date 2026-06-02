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
// File: vhost.rs
// Description: Virtual host management and state representation.

//! Virtual host support for namespace isolation.
//!
//! Each VHost has its own set of exchanges, queues, and bindings.
//! The default vhost "/" is always present.

use dashmap::DashMap;
use std::collections::HashMap;
use tokio::sync::RwLock;

use crate::queue::QueueState;
use crate::routing::exchange::{Exchange, create_default_exchanges};

/// Container for virtual host routing, isolated queues, and user permissions.
/// Container for virtual host routing, isolated queues, and user permissions.
pub struct VHost {
    pub name: String,
    pub exchanges: RwLock<HashMap<String, Exchange>>,
    pub queues: DashMap<String, QueueState>,
}

impl VHost {
    /// Creates a new instance with the given name.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            exchanges: RwLock::new(create_default_exchanges()),
            queues: DashMap::new(),
        }
    }

    pub fn empty(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            exchanges: RwLock::new(HashMap::new()),
            queues: DashMap::new(),
        }
    }
}

pub const DEFAULT_VHOST: &str = "/";

#[cfg(test)]
mod tests {
    #[allow(unused_imports)]
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

        let opts = QueueOptions::default();
        vh.queues
            .insert("q1".into(), QueueState::with_options(opts));
        assert!(vh.queues.contains_key("q1"));

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
        assert_eq!(ex.len(), 6);
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
