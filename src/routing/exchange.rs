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
// File: exchange.rs
// Description: Exchange types (direct, fanout, topic, headers) and routing logic.

use std::collections::HashMap;

/// Defines the various states or variants of exchange type.
///
/// Defines details for exchange type inside the broker ecosystem.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ExchangeType {
    Direct,
    Fanout,
    Topic,
    Headers,
}

impl ExchangeType {
    /// Executes the standard from str lifecycle step.
    ///
    /// Executes the required business logic for from str.
    ///
    /// # Arguments
    ///
    /// * `s` - `&str`: The `s` argument.
    ///
    /// # Returns
    ///
    /// * `Option<Self>` - The evaluated outcome or operation handle.
    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "direct" => Some(Self::Direct),
            "fanout" => Some(Self::Fanout),
            "topic" => Some(Self::Topic),
            "headers" => Some(Self::Headers),
            _ => None,
        }
    }

    /// Executes the standard as str lifecycle step.
    ///
    /// Executes the required business logic for as str.
    ///
    /// # Returns
    ///
    /// * `&'static str` - The evaluated outcome or operation handle.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Direct => "direct",
            Self::Fanout => "fanout",
            Self::Topic => "topic",
            Self::Headers => "headers",
        }
    }

    /// Executes the standard to byte lifecycle step.
    ///
    /// Executes the required business logic for to byte.
    ///
    /// # Returns
    ///
    /// * `u8` - The evaluated outcome or operation handle.
    pub fn to_byte(&self) -> u8 {
        match self {
            Self::Direct => 0x00,
            Self::Fanout => 0x01,
            Self::Topic => 0x02,
            Self::Headers => 0x03,
        }
    }
}

/// Defines the various states or variants of headers match.
///
/// Defines details for headers match inside the broker ecosystem.
#[derive(Clone, Debug)]
pub enum HeadersMatch {
    All(HashMap<String, String>),
    Any(HashMap<String, String>),
}

/// Represents the schema or state for binding.
///
/// Defines details for binding inside the broker ecosystem.
#[derive(Clone, Debug)]
pub struct Binding {
    pub queue_name: String,
    pub routing_key: String,
    pub headers_match: Option<HeadersMatch>,
}

/// Represents the schema or state for exchange.
///
/// Defines details for exchange inside the broker ecosystem.
pub struct Exchange {
    pub name: String,
    pub kind: ExchangeType,
    pub durable: bool,
    pub auto_delete: bool,
    pub bindings: Vec<Binding>,
}

impl Exchange {
    /// Executes the standard new lifecycle step.
    ///
    /// Executes the required business logic for new.
    ///
    /// # Arguments
    ///
    /// * `name` - `String`: The unique identifier string of the resource.
    /// * `kind` - `ExchangeType`: The `kind` argument.
    /// * `durable` - `bool`: The `durable` argument.
    ///
    /// # Returns
    ///
    /// * `Self` - The evaluated outcome or operation handle.
    pub fn new(name: String, kind: ExchangeType, durable: bool) -> Self {
        Self {
            name,
            kind,
            durable,
            auto_delete: false,
            bindings: Vec::new(),
        }
    }

    /// Executes the standard add binding lifecycle step.
    ///
    /// Executes the required business logic for add binding.
    ///
    /// # Arguments
    ///
    /// * `binding` - `Binding`: The `binding` argument.
    pub fn add_binding(&mut self, binding: Binding) {
        // Avoid duplicate bindings (same queue + routing key)
        let exists = self
            .bindings
            .iter()
            .any(|b| b.queue_name == binding.queue_name && b.routing_key == binding.routing_key);
        if !exists {
            self.bindings.push(binding);
        }
    }

    /// Executes the standard remove binding lifecycle step.
    ///
    /// Executes the required business logic for remove binding.
    ///
    /// # Arguments
    ///
    /// * `queue_name` - `&str`: The unique identifier string of the resource.
    /// * `routing_key` - `&str`: The `routing_key` argument.
    pub fn remove_binding(&mut self, queue_name: &str, routing_key: &str) {
        self.bindings
            .retain(|b| !(b.queue_name == queue_name && b.routing_key == routing_key));
    }

    /// Executes the standard route lifecycle step.
    ///
    /// Executes the required business logic for route.
    ///
    /// # Arguments
    ///
    /// * `routing_key` - `&str`: The `routing_key` argument.
    /// * `headers` - `&HashMap<String, String>`: The `headers` argument.
    ///
    /// # Returns
    ///
    /// * `Vec<String>` - The evaluated outcome or operation handle.
    pub fn route(&self, routing_key: &str, headers: &HashMap<String, String>) -> Vec<String> {
        match self.kind {
            ExchangeType::Direct => self.route_direct(routing_key),
            ExchangeType::Fanout => self.route_fanout(),
            ExchangeType::Topic => self.route_topic(routing_key),
            ExchangeType::Headers => self.route_headers(headers),
        }
    }

    /// Executes the standard route direct lifecycle step.
    ///
    /// Executes the required business logic for route direct.
    ///
    /// # Arguments
    ///
    /// * `routing_key` - `&str`: The `routing_key` argument.
    ///
    /// # Returns
    ///
    /// * `Vec<String>` - The evaluated outcome or operation handle.
    fn route_direct(&self, routing_key: &str) -> Vec<String> {
        self.bindings
            .iter()
            .filter(|b| b.routing_key == routing_key)
            .map(|b| b.queue_name.clone())
            .collect()
    }

    /// Executes the standard route fanout lifecycle step.
    ///
    /// Executes the required business logic for route fanout.
    ///
    /// # Returns
    ///
    /// * `Vec<String>` - The evaluated outcome or operation handle.
    fn route_fanout(&self) -> Vec<String> {
        self.bindings.iter().map(|b| b.queue_name.clone()).collect()
    }

    /// Executes the standard route topic lifecycle step.
    ///
    /// Executes the required business logic for route topic.
    ///
    /// # Arguments
    ///
    /// * `routing_key` - `&str`: The `routing_key` argument.
    ///
    /// # Returns
    ///
    /// * `Vec<String>` - The evaluated outcome or operation handle.
    fn route_topic(&self, routing_key: &str) -> Vec<String> {
        self.bindings
            .iter()
            .filter(|b| topic_matches(&b.routing_key, routing_key))
            .map(|b| b.queue_name.clone())
            .collect()
    }

    /// Executes the standard route headers lifecycle step.
    ///
    /// Executes the required business logic for route headers.
    ///
    /// # Arguments
    ///
    /// * `msg_headers` - `&HashMap<String, String>`: The `msg_headers` argument.
    ///
    /// # Returns
    ///
    /// * `Vec<String>` - The evaluated outcome or operation handle.
    fn route_headers(&self, msg_headers: &HashMap<String, String>) -> Vec<String> {
        self.bindings
            .iter()
            .filter(|b| match &b.headers_match {
                Some(HeadersMatch::All(required)) => {
                    required.iter().all(|(k, v)| msg_headers.get(k) == Some(v))
                }
                Some(HeadersMatch::Any(required)) => {
                    required.iter().any(|(k, v)| msg_headers.get(k) == Some(v))
                }
                None => false,
            })
            .map(|b| b.queue_name.clone())
            .collect()
    }
}

/// Executes the standard topic matches lifecycle step.
///
/// Executes the required business logic for topic matches.
///
/// # Arguments
///
/// * `pattern` - `&str`: The `pattern` argument.
/// * `routing_key` - `&str`: The `routing_key` argument.
///
/// # Returns
///
/// * `bool` - The evaluated outcome or operation handle.
fn topic_matches(pattern: &str, routing_key: &str) -> bool {
    let pattern_parts: Vec<&str> = pattern.split('.').collect();
    let key_parts: Vec<&str> = routing_key.split('.').collect();
    topic_match_recursive(&pattern_parts, &key_parts)
}

/// Executes the standard topic match recursive lifecycle step.
///
/// Executes the required business logic for topic match recursive.
///
/// # Arguments
///
/// * `pattern` - `&[&str]`: The `pattern` argument.
/// * `key` - `&[&str]`: The `key` argument.
///
/// # Returns
///
/// * `bool` - The evaluated outcome or operation handle.
fn topic_match_recursive(pattern: &[&str], key: &[&str]) -> bool {
    match (pattern.first(), key.first()) {
        (None, None) => true,
        (Some(&"#"), _) => {
            // # matches zero or more words
            if pattern.len() == 1 {
                return true; // trailing # matches everything
            }
            // Try matching # as zero words, one word, two words, etc.
            for skip in 0..=key.len() {
                if topic_match_recursive(&pattern[1..], &key[skip..]) {
                    return true;
                }
            }
            false
        }
        (Some(&"*"), Some(_)) => {
            // * matches exactly one word
            topic_match_recursive(&pattern[1..], &key[1..])
        }
        (Some(p), Some(k)) if p == k => topic_match_recursive(&pattern[1..], &key[1..]),
        _ => false,
    }
}

/// Executes the standard create default exchanges lifecycle step.
///
/// Executes the required business logic for create default exchanges.
///
/// # Returns
///
/// * `HashMap<String, Exchange>` - The evaluated outcome or operation handle.
pub fn create_default_exchanges() -> HashMap<String, Exchange> {
    let mut exchanges = HashMap::new();

    // Default exchange: direct, auto-binds queues by name
    exchanges.insert(
        String::new(),
        Exchange::new(String::new(), ExchangeType::Direct, true),
    );
    exchanges.insert(
        "amq.direct".into(),
        Exchange::new("amq.direct".into(), ExchangeType::Direct, true),
    );
    exchanges.insert(
        "amq.fanout".into(),
        Exchange::new("amq.fanout".into(), ExchangeType::Fanout, true),
    );
    exchanges.insert(
        "amq.topic".into(),
        Exchange::new("amq.topic".into(), ExchangeType::Topic, true),
    );
    exchanges.insert(
        "amq.headers".into(),
        Exchange::new("amq.headers".into(), ExchangeType::Headers, true),
    );

    exchanges
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Executes the standard direct routing lifecycle step.
    ///
    /// Executes the required business logic for direct routing.
    #[test]
    fn direct_routing() {
        let mut ex = Exchange::new("test".into(), ExchangeType::Direct, false);
        ex.add_binding(Binding {
            queue_name: "q1".into(),
            routing_key: "orders".into(),
            headers_match: None,
        });
        ex.add_binding(Binding {
            queue_name: "q2".into(),
            routing_key: "payments".into(),
            headers_match: None,
        });

        let empty = HashMap::new();
        assert_eq!(ex.route("orders", &empty), vec!["q1"]);
        assert_eq!(ex.route("payments", &empty), vec!["q2"]);
        assert!(ex.route("unknown", &empty).is_empty());
    }

    /// Executes the standard fanout routing lifecycle step.
    ///
    /// Executes the required business logic for fanout routing.
    #[test]
    fn fanout_routing() {
        let mut ex = Exchange::new("test".into(), ExchangeType::Fanout, false);
        ex.add_binding(Binding {
            queue_name: "q1".into(),
            routing_key: String::new(),
            headers_match: None,
        });
        ex.add_binding(Binding {
            queue_name: "q2".into(),
            routing_key: String::new(),
            headers_match: None,
        });

        let empty = HashMap::new();
        let routed = ex.route("anything", &empty);
        assert_eq!(routed.len(), 2);
        assert!(routed.contains(&"q1".to_string()));
        assert!(routed.contains(&"q2".to_string()));
    }

    /// Executes the standard topic routing lifecycle step.
    ///
    /// Executes the required business logic for topic routing.
    #[test]
    fn topic_routing() {
        let mut ex = Exchange::new("test".into(), ExchangeType::Topic, false);
        ex.add_binding(Binding {
            queue_name: "all_logs".into(),
            routing_key: "logs.#".into(),
            headers_match: None,
        });
        ex.add_binding(Binding {
            queue_name: "app_errors".into(),
            routing_key: "logs.app.error".into(),
            headers_match: None,
        });
        ex.add_binding(Binding {
            queue_name: "any_error".into(),
            routing_key: "logs.*.error".into(),
            headers_match: None,
        });

        let empty = HashMap::new();

        // logs.app.error → matches all three
        let routed = ex.route("logs.app.error", &empty);
        assert!(routed.contains(&"all_logs".to_string()));
        assert!(routed.contains(&"app_errors".to_string()));
        assert!(routed.contains(&"any_error".to_string()));

        // logs.db.error → matches all_logs + any_error
        let routed = ex.route("logs.db.error", &empty);
        assert!(routed.contains(&"all_logs".to_string()));
        assert!(routed.contains(&"any_error".to_string()));
        assert!(!routed.contains(&"app_errors".to_string()));

        // logs.app.info → matches only all_logs
        let routed = ex.route("logs.app.info", &empty);
        assert_eq!(routed, vec!["all_logs"]);
    }

    /// Executes the standard headers routing lifecycle step.
    ///
    /// Executes the required business logic for headers routing.
    #[test]
    fn headers_routing() {
        let mut ex = Exchange::new("test".into(), ExchangeType::Headers, false);
        let mut required = HashMap::new();
        required.insert("format".into(), "pdf".into());
        required.insert("type".into(), "report".into());

        ex.add_binding(Binding {
            queue_name: "pdf_reports".into(),
            routing_key: String::new(),
            headers_match: Some(HeadersMatch::All(required)),
        });

        let mut msg_headers = HashMap::new();
        msg_headers.insert("format".into(), "pdf".into());
        msg_headers.insert("type".into(), "report".into());
        assert_eq!(ex.route("", &msg_headers), vec!["pdf_reports"]);

        // Missing one header → no match
        let mut partial = HashMap::new();
        partial.insert("format".into(), "pdf".into());
        assert!(ex.route("", &partial).is_empty());
    }

    /// Executes the standard no duplicate bindings lifecycle step.
    ///
    /// Executes the required business logic for no duplicate bindings.
    #[test]
    fn no_duplicate_bindings() {
        let mut ex = Exchange::new("test".into(), ExchangeType::Direct, false);
        let binding = Binding {
            queue_name: "q1".into(),
            routing_key: "key".into(),
            headers_match: None,
        };
        ex.add_binding(binding.clone());
        ex.add_binding(binding);
        assert_eq!(ex.bindings.len(), 1);
    }

    // ── ExchangeType parsing ────────────────────────────────────────────

    /// Executes the standard exchange type from str lifecycle step.
    ///
    /// Executes the required business logic for exchange type from str.
    #[test]
    fn exchange_type_from_str() {
        assert_eq!(ExchangeType::from_str("direct"), Some(ExchangeType::Direct));
        assert_eq!(ExchangeType::from_str("fanout"), Some(ExchangeType::Fanout));
        assert_eq!(ExchangeType::from_str("topic"), Some(ExchangeType::Topic));
        assert_eq!(
            ExchangeType::from_str("headers"),
            Some(ExchangeType::Headers)
        );
        assert_eq!(ExchangeType::from_str("unknown"), None);
        assert_eq!(ExchangeType::from_str(""), None);
    }

    /// Executes the standard exchange type as str roundtrip lifecycle step.
    ///
    /// Executes the required business logic for exchange type as str roundtrip.
    #[test]
    fn exchange_type_as_str_roundtrip() {
        for kind in [
            ExchangeType::Direct,
            ExchangeType::Fanout,
            ExchangeType::Topic,
            ExchangeType::Headers,
        ] {
            let s = kind.as_str();
            assert_eq!(ExchangeType::from_str(s), Some(kind));
        }
    }

    // ── Binding removal ─────────────────────────────────────────────────

    /// Executes the standard remove binding by queue and key lifecycle step.
    ///
    /// Executes the required business logic for remove binding by queue and key.
    #[test]
    fn remove_binding_by_queue_and_key() {
        let mut ex = Exchange::new("test".into(), ExchangeType::Direct, false);
        ex.add_binding(Binding {
            queue_name: "q1".into(),
            routing_key: "a".into(),
            headers_match: None,
        });
        ex.add_binding(Binding {
            queue_name: "q2".into(),
            routing_key: "b".into(),
            headers_match: None,
        });

        ex.remove_binding("q1", "a");
        assert_eq!(ex.bindings.len(), 1);
        assert_eq!(ex.bindings[0].queue_name, "q2");
    }

    /// Executes the standard remove nonexistent binding is noop lifecycle step.
    ///
    /// Executes the required business logic for remove nonexistent binding is noop.
    #[test]
    fn remove_nonexistent_binding_is_noop() {
        let mut ex = Exchange::new("test".into(), ExchangeType::Direct, false);
        ex.add_binding(Binding {
            queue_name: "q1".into(),
            routing_key: "a".into(),
            headers_match: None,
        });

        ex.remove_binding("q1", "wrong_key");
        assert_eq!(ex.bindings.len(), 1); // still there
    }

    // ── Topic edge cases ────────────────────────────────────────────────

    /// Executes the standard topic hash matches everything lifecycle step.
    ///
    /// Executes the required business logic for topic hash matches everything.
    #[test]
    fn topic_hash_matches_everything() {
        assert!(topic_matches("#", "a.b.c"));
        assert!(topic_matches("#", "a"));
        assert!(topic_matches("#", ""));
    }

    /// Executes the standard topic hash in middle lifecycle step.
    ///
    /// Executes the required business logic for topic hash in middle.
    #[test]
    fn topic_hash_in_middle() {
        assert!(topic_matches("a.#.z", "a.b.c.z"));
        assert!(topic_matches("a.#.z", "a.z")); // # matches zero words
    }

    /// Executes the standard topic star requires exactly one word lifecycle step.
    ///
    /// Executes the required business logic for topic star requires exactly one word.
    #[test]
    fn topic_star_requires_exactly_one_word() {
        assert!(topic_matches("a.*.c", "a.b.c"));
        assert!(!topic_matches("a.*.c", "a.b.d.c")); // * can't match two words
        assert!(!topic_matches("*", "a.b")); // * is one word only
    }

    /// Executes the standard topic exact match lifecycle step.
    ///
    /// Executes the required business logic for topic exact match.
    #[test]
    fn topic_exact_match() {
        assert!(topic_matches("a.b.c", "a.b.c"));
        assert!(!topic_matches("a.b.c", "a.b.d"));
        assert!(!topic_matches("a.b", "a.b.c"));
    }

    /// Executes the standard topic empty pattern matches empty key lifecycle step.
    ///
    /// Executes the required business logic for topic empty pattern matches empty key.
    #[test]
    fn topic_empty_pattern_matches_empty_key() {
        assert!(topic_matches("", ""));
    }

    /// Executes the standard topic star does not match empty lifecycle step.
    ///
    /// Executes the required business logic for topic star does not match empty.
    #[test]
    fn topic_star_does_not_match_empty() {
        assert!(!topic_matches("a.*", "a"));
    }

    // ── Headers Any ─────────────────────────────────────────────────────

    /// Executes the standard headers any routing lifecycle step.
    ///
    /// Executes the required business logic for headers any routing.
    #[test]
    fn headers_any_routing() {
        let mut ex = Exchange::new("test".into(), ExchangeType::Headers, false);
        let mut required = HashMap::new();
        required.insert("color".into(), "red".into());
        required.insert("size".into(), "large".into());

        ex.add_binding(Binding {
            queue_name: "q1".into(),
            routing_key: String::new(),
            headers_match: Some(HeadersMatch::Any(required)),
        });

        // Match on just one header
        let mut h = HashMap::new();
        h.insert("color".into(), "red".into());
        assert_eq!(ex.route("", &h), vec!["q1"]);

        // No matching headers
        let mut h2 = HashMap::new();
        h2.insert("color".into(), "blue".into());
        assert!(ex.route("", &h2).is_empty());
    }

    // ── Edge cases ──────────────────────────────────────────────────────

    /// Executes the standard exchange no bindings routes nothing lifecycle step.
    ///
    /// Executes the required business logic for exchange no bindings routes nothing.
    #[test]
    fn exchange_no_bindings_routes_nothing() {
        let ex = Exchange::new("empty".into(), ExchangeType::Direct, false);
        let empty = HashMap::new();
        assert!(ex.route("anything", &empty).is_empty());
    }

    /// Executes the standard fanout ignores routing key lifecycle step.
    ///
    /// Executes the required business logic for fanout ignores routing key.
    #[test]
    fn fanout_ignores_routing_key() {
        let mut ex = Exchange::new("test".into(), ExchangeType::Fanout, false);
        ex.add_binding(Binding {
            queue_name: "q1".into(),
            routing_key: "specific".into(),
            headers_match: None,
        });

        let empty = HashMap::new();
        // Fanout ignores routing key entirely
        let routed = ex.route("totally_different", &empty);
        assert_eq!(routed, vec!["q1"]);
    }

    /// Executes the standard direct multiple queues same key lifecycle step.
    ///
    /// Executes the required business logic for direct multiple queues same key.
    #[test]
    fn direct_multiple_queues_same_key() {
        let mut ex = Exchange::new("test".into(), ExchangeType::Direct, false);
        ex.add_binding(Binding {
            queue_name: "q1".into(),
            routing_key: "events".into(),
            headers_match: None,
        });
        ex.add_binding(Binding {
            queue_name: "q2".into(),
            routing_key: "events".into(),
            headers_match: None,
        });

        let empty = HashMap::new();
        let routed = ex.route("events", &empty);
        assert_eq!(routed.len(), 2);
    }

    // ── Default exchanges ───────────────────────────────────────────────

    /// Executes the standard default exchanges correct types lifecycle step.
    ///
    /// Executes the required business logic for default exchanges correct types.
    #[test]
    fn default_exchanges_correct_types() {
        let exchanges = create_default_exchanges();
        assert_eq!(exchanges.get("").unwrap().kind, ExchangeType::Direct);
        assert_eq!(
            exchanges.get("amq.direct").unwrap().kind,
            ExchangeType::Direct
        );
        assert_eq!(
            exchanges.get("amq.fanout").unwrap().kind,
            ExchangeType::Fanout
        );
        assert_eq!(
            exchanges.get("amq.topic").unwrap().kind,
            ExchangeType::Topic
        );
        assert_eq!(
            exchanges.get("amq.headers").unwrap().kind,
            ExchangeType::Headers
        );
    }

    /// Executes the standard default exchanges are durable lifecycle step.
    ///
    /// Executes the required business logic for default exchanges are durable.
    #[test]
    fn default_exchanges_are_durable() {
        let exchanges = create_default_exchanges();
        for ex in exchanges.values() {
            assert!(ex.durable, "{} should be durable", ex.name);
        }
    }
}
