//! Per-vhost permission model (RabbitMQ-compatible).
//!
//! Each permission entry grants a user configure/write/read access
//! to resources matching the specified regex patterns within a vhost.

use regex::Regex;
use serde::{Deserialize, Serialize};

/// A permission entry for a user on a specific vhost.
///
/// Each field is a regex pattern matched against resource names:
/// - `configure`: declare/delete queues and exchanges
/// - `write`: publish to exchange, bind queue to exchange
/// - `read`: consume from queue, basic.get, queue.purge
///
/// Empty string `""` means no access.
/// `".*"` means full access to all resources.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Permission {
    pub username: String,
    pub vhost: String,
    pub configure: String,
    pub write: String,
    pub read: String,
}

impl Permission {
    pub fn new(username: &str, vhost: &str, configure: &str, write: &str, read: &str) -> Self {
        Self {
            username: username.to_string(),
            vhost: vhost.to_string(),
            configure: configure.to_string(),
            write: write.to_string(),
            read: read.to_string(),
        }
    }

    /// Full access permission (configure=.*, write=.*, read=.*).
    pub fn full_access(username: &str, vhost: &str) -> Self {
        Self::new(username, vhost, ".*", ".*", ".*")
    }

    /// No access permission (all empty).
    pub fn no_access(username: &str, vhost: &str) -> Self {
        Self::new(username, vhost, "", "", "")
    }
}

/// Check if a resource name matches a permission pattern.
///
/// The pattern is a regex anchored to the full string.
/// Empty pattern means no access.
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
    use super::*;

    #[test]
    fn full_access_matches_everything() {
        assert!(matches_resource(".*", "anything"));
        assert!(matches_resource(".*", ""));
        assert!(matches_resource(".*", "amq.gen-abc123"));
    }

    #[test]
    fn empty_pattern_denies_all() {
        assert!(!matches_resource("", "anything"));
        assert!(!matches_resource("", ""));
    }

    #[test]
    fn prefix_pattern() {
        assert!(matches_resource("^app\\..*", "app.orders"));
        assert!(matches_resource("^app\\..*", "app.events.new"));
        assert!(!matches_resource("^app\\..*", "system.internal"));
        assert!(!matches_resource("^app\\..*", "xapp.fake"));
    }

    #[test]
    fn exact_match() {
        assert!(matches_resource("^my-queue$", "my-queue"));
        assert!(!matches_resource("^my-queue$", "my-queue-2"));
        assert!(!matches_resource("^my-queue$", "not-my-queue"));
    }

    #[test]
    fn auto_anchoring() {
        // Pattern without anchors is auto-anchored
        assert!(matches_resource("orders", "orders"));
        assert!(!matches_resource("orders", "orders.new"));
        assert!(!matches_resource("orders", "my-orders"));
    }

    #[test]
    fn amq_gen_pattern() {
        // RabbitMQ default: guest can use auto-generated queues
        assert!(matches_resource("^amq\\.gen.*", "amq.gen-abc123"));
        assert!(!matches_resource("^amq\\.gen.*", "my-queue"));
    }

    #[test]
    fn alternation() {
        assert!(matches_resource("^(app|service)\\..*", "app.orders"));
        assert!(matches_resource("^(app|service)\\..*", "service.events"));
        assert!(!matches_resource("^(app|service)\\..*", "admin.logs"));
    }

    #[test]
    fn invalid_regex_denies() {
        // Invalid regex should not panic, just deny
        assert!(!matches_resource("[invalid", "anything"));
    }

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
}
