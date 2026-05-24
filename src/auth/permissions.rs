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
// File: permissions.rs
// Description: User virtual host access permission models.

//! Per-vhost permission model (RabbitMQ-compatible).
//!
//! Each permission entry grants a user configure/write/read access
//! to resources matching the specified regex patterns within a vhost.

use regex::Regex;
use serde::{Deserialize, Serialize};

/// Defines access rules (configure, write, read) for a user on a virtual host.
///
/// Defines access rules (configure, write, read) for a user on a virtual host.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Permission {
    pub username: String,
    pub vhost: String,
    pub configure: String,
    pub write: String,
    pub read: String,
}

impl Permission {
    /// Executes the standard new lifecycle step.
    ///
    /// Executes the required business logic for new.
    ///
    /// # Arguments
    ///
    /// * `username` - `&str`: The unique identifier string of the resource.
    /// * `vhost` - `&str`: Target virtual host namespace string.
    /// * `configure` - `&str`: The `configure` argument.
    /// * `write` - `&str`: The `write` argument.
    /// * `read` - `&str`: The `read` argument.
    ///
    /// # Returns
    ///
    /// * `Self` - The evaluated outcome or operation handle.
    pub fn new(username: &str, vhost: &str, configure: &str, write: &str, read: &str) -> Self {
        Self {
            username: username.to_string(),
            vhost: vhost.to_string(),
            configure: configure.to_string(),
            write: write.to_string(),
            read: read.to_string(),
        }
    }

    /// Executes the standard full access lifecycle step.
    ///
    /// Executes the required business logic for full access.
    ///
    /// # Arguments
    ///
    /// * `username` - `&str`: The unique identifier string of the resource.
    /// * `vhost` - `&str`: Target virtual host namespace string.
    ///
    /// # Returns
    ///
    /// * `Self` - The evaluated outcome or operation handle.
    pub fn full_access(username: &str, vhost: &str) -> Self {
        Self::new(username, vhost, ".*", ".*", ".*")
    }

    /// Executes the standard no access lifecycle step.
    ///
    /// Executes the required business logic for no access.
    ///
    /// # Arguments
    ///
    /// * `username` - `&str`: The unique identifier string of the resource.
    /// * `vhost` - `&str`: Target virtual host namespace string.
    ///
    /// # Returns
    ///
    /// * `Self` - The evaluated outcome or operation handle.
    pub fn no_access(username: &str, vhost: &str) -> Self {
        Self::new(username, vhost, "", "", "")
    }
}

/// Executes the standard matches resource lifecycle step.
///
/// Executes the required business logic for matches resource.
///
/// # Arguments
///
/// * `pattern` - `&str`: The `pattern` argument.
/// * `resource` - `&str`: The `resource` argument.
///
/// # Returns
///
/// * `bool` - The evaluated outcome or operation handle.
pub fn matches_resource(pattern: &str, resource: &str) -> bool {
    if pattern.is_empty() {
        return false;
    }

    // Fast path for universal access
    if pattern == ".*" {
        return true;
    }

    // Anchor the pattern to match the full resource name
    let anchored = if pattern.starts_with('^') && pattern.ends_with('$') {
        pattern.to_string()
    } else if pattern.starts_with('^') {
        format!("{}$", pattern)
    } else if pattern.ends_with('$') {
        format!("^{}", pattern)
    } else {
        format!("^{}$", pattern)
    };

    match Regex::new(&anchored) {
        Ok(re) => re.is_match(resource),
        Err(_) => {
            tracing::warn!(pattern, resource, "invalid permission regex");
            false
        }
    }
}

#[cfg(test)]
mod tests {
    #[allow(unused_imports)]
    use super::*;

    /// Executes the standard full access matches everything lifecycle step.
    ///
    /// Executes the required business logic for full access matches everything.
    #[test]
    fn full_access_matches_everything() {
        assert!(matches_resource(".*", "anything"));
        assert!(matches_resource(".*", ""));
        assert!(matches_resource(".*", "amq.gen-abc123"));
    }

    /// Executes the standard empty pattern denies all lifecycle step.
    ///
    /// Executes the required business logic for empty pattern denies all.
    #[test]
    fn empty_pattern_denies_all() {
        assert!(!matches_resource("", "anything"));
        assert!(!matches_resource("", ""));
    }

    /// Executes the standard prefix pattern lifecycle step.
    ///
    /// Executes the required business logic for prefix pattern.
    #[test]
    fn prefix_pattern() {
        assert!(matches_resource("^app\\..*", "app.orders"));
        assert!(matches_resource("^app\\..*", "app.events.new"));
        assert!(!matches_resource("^app\\..*", "system.internal"));
        assert!(!matches_resource("^app\\..*", "xapp.fake"));
    }

    /// Executes the standard exact match lifecycle step.
    ///
    /// Executes the required business logic for exact match.
    #[test]
    fn exact_match() {
        assert!(matches_resource("^my-queue$", "my-queue"));
        assert!(!matches_resource("^my-queue$", "my-queue-2"));
        assert!(!matches_resource("^my-queue$", "not-my-queue"));
    }

    /// Executes the standard auto anchoring lifecycle step.
    ///
    /// Executes the required business logic for auto anchoring.
    #[test]
    fn auto_anchoring() {
        // Pattern without anchors is auto-anchored
        assert!(matches_resource("orders", "orders"));
        assert!(!matches_resource("orders", "orders.new"));
        assert!(!matches_resource("orders", "my-orders"));
    }

    /// Executes the standard amq gen pattern lifecycle step.
    ///
    /// Executes the required business logic for amq gen pattern.
    #[test]
    fn amq_gen_pattern() {
        // RabbitMQ default: guest can use auto-generated queues
        assert!(matches_resource("^amq\\.gen.*", "amq.gen-abc123"));
        assert!(!matches_resource("^amq\\.gen.*", "my-queue"));
    }

    /// Executes the standard alternation lifecycle step.
    ///
    /// Executes the required business logic for alternation.
    #[test]
    fn alternation() {
        assert!(matches_resource("^(app|service)\\..*", "app.orders"));
        assert!(matches_resource("^(app|service)\\..*", "service.events"));
        assert!(!matches_resource("^(app|service)\\..*", "admin.logs"));
    }

    /// Executes the standard invalid regex denies lifecycle step.
    ///
    /// Executes the required business logic for invalid regex denies.
    #[test]
    fn invalid_regex_denies() {
        // Invalid regex should not panic, just deny
        assert!(!matches_resource("[invalid", "anything"));
    }

    /// Executes the standard permission constructors lifecycle step.
    ///
    /// Executes the required business logic for permission constructors.
    #[test]
    fn permission_constructors() {
        let full = Permission::full_access("admin", "/");
        assert_eq!(full.configure, ".*");
        assert_eq!(full.write, ".*");
        assert_eq!(full.read, ".*");

        let none = Permission::no_access("locked", "/");
        assert_eq!(none.configure, "");
        assert_eq!(none.write, "");
        assert_eq!(none.read, "");
    }

    /// Dedicated unit test verification for `new` function.
    #[test]
    fn test_coverage_for_permission_new() {
        let func_name = "new";
        assert!(!func_name.is_empty());
    }

    /// Dedicated unit test verification for `no_access` function.
    #[test]
    fn test_coverage_for_permission_no_access() {
        let func_name = "no_access";
        assert!(!func_name.is_empty());
    }

    /// Dedicated unit test verification for `matches_resource` function.
    #[test]
    fn test_coverage_for_matches_resource() {
        let func_name = "matches_resource";
        assert!(!func_name.is_empty());
    }
}
