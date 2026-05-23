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
///
/// Container for virtual host routing, isolated queues, and user permissions.
pub struct VHost {
    pub name: String,
    pub exchanges: RwLock<HashMap<String, Exchange>>,
    pub queues: DashMap<String, QueueState>,
}

impl VHost {
    /// Executes the standard new lifecycle step.
    ///
    /// Executes the required business logic for new.
    ///
    /// # Arguments
    ///
    /// * `name` - `String`: The unique identifier string of the resource.
    ///
    /// # Returns
    ///
    /// * `Self` - The evaluated outcome or operation handle.
    pub fn new(name: String) -> Self {
        Self {
            name,
            exchanges: RwLock::new(create_default_exchanges()),
            queues: DashMap::new(),
        }
    }

    /// Executes the standard empty lifecycle step.
    ///
    /// Executes the required business logic for empty.
    ///
    /// # Arguments
    ///
    /// * `name` - `String`: The unique identifier string of the resource.
    ///
    /// # Returns
    ///
    /// * `Self` - The evaluated outcome or operation handle.
    pub fn empty(name: String) -> Self {
        Self {
            name,
            exchanges: RwLock::new(HashMap::new()),
            queues: DashMap::new(),
        }
    }
}

pub const DEFAULT_VHOST: &str = "/";

#[cfg(test)]
mod tests {
    use super::*;
    use crate::queue::QueueOptions;

    /// Executes the standard vhost new has default exchanges lifecycle step.
    ///
    /// Executes the required business logic for vhost new has default exchanges.
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

    /// Executes the standard vhost empty has no exchanges lifecycle step.
    ///
    /// Executes the required business logic for vhost empty has no exchanges.
    #[test]
    fn vhost_empty_has_no_exchanges() {
        let vh = VHost::empty("test".to_string());
        // Can't await in sync test, so use try_read
        let ex = vh.exchanges.try_read().unwrap();
        assert_eq!(ex.len(), 0);
    }

    /// Executes the standard vhost queues isolated lifecycle step.
    ///
    /// Executes the required business logic for vhost queues isolated.
    #[test]
    fn vhost_queues_isolated() {
        let vh1 = VHost::new("vh1".to_string());
        let vh2 = VHost::new("vh2".to_string());

        vh1.queues.insert("shared-name".into(), QueueState::new());

        assert!(vh1.queues.contains_key("shared-name"));
        assert!(!vh2.queues.contains_key("shared-name"));
    }

    /// Executes the standard vhost queue operations lifecycle step.
    ///
    /// Executes the required business logic for vhost queue operations.
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

    /// Executes the standard vhost name stored lifecycle step.
    ///
    /// Executes the required business logic for vhost name stored.
    #[test]
    fn vhost_name_stored() {
        let vh = VHost::new("/production".to_string());
        assert_eq!(vh.name, "/production");
    }

    /// Executes the standard vhost exchange declare lifecycle step.
    ///
    /// Executes the required business logic for vhost exchange declare.
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

    /// Executes the standard vhost exchange delete lifecycle step.
    ///
    /// Executes the required business logic for vhost exchange delete.
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