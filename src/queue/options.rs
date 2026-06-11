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
// File: options.rs
// Description: Configuration options for queues (TTL, DLX, max-length, etc.).

use std::time::Duration;

/// Discriminator for the queue's replication strategy.
///
/// Drives how a queue stores and replicates messages.
///
/// ```ignore
/// let qt = QueueType::from_amqp_arg(Some("quorum"));
/// assert_eq!(qt, QueueType::Quorum);
/// ```
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub enum QueueType {
    /// Single-node queue — messages reside on the declaring node only.
    #[default]
    Classic,
    /// Raft-replicated queue — writes require majority quorum.
    Quorum,
    /// Append-only log — messages are never removed on ACK.
    Stream,
}

impl QueueType {
    /// Parses the AMQP `x-queue-type` argument value.
    ///
    /// Returns `Classic` for `None` or unrecognised values, matching
    /// RabbitMQ's default behavior.
    pub fn from_amqp_arg(value: Option<&str>) -> Self {
        match value {
            Some("quorum") => Self::Quorum,
            Some("stream") => Self::Stream,
            _ => Self::Classic,
        }
    }

    /// Returns the wire-level string representation.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Classic => "classic",
            Self::Quorum => "quorum",
            Self::Stream => "stream",
        }
    }
}

/// Configurable parameters for queue creation (durable, exclusive, TTL, DLX, etc.).
/// Parsed queue configuration derived from AMQP `x-*` headers.
///
/// Includes message TTL, queue expiry, max length, dead-letter
/// exchange/routing-key, and priority level settings.
#[derive(Clone, Debug, Default)]
pub struct QueueOptions {
    pub durable: bool,
    pub exclusive: bool,
    pub auto_delete: bool,
    pub max_priority: u8,
    pub message_ttl: Option<Duration>,
    pub max_length: Option<usize>,
    pub dead_letter_exchange: Option<String>,
    pub dead_letter_routing_key: Option<String>,
    pub expires: Option<Duration>,
    pub max_retries: Option<u32>,
    pub retry_delay_ms: Option<u64>,
    pub retry_multiplier: Option<f64>,
    pub rate_limit: Option<u32>,
    pub stream_mode: bool,
    pub schema: Option<Vec<u8>>,
    pub schema_type: Option<String>,
    pub schema_message: Option<String>,

    /// Replication strategy chosen at declare time.
    pub queue_type: QueueType,
    /// Number of replicas for quorum queues (including the leader).
    /// Defaults to `default_quorum_group_size` from config.
    pub quorum_group_size: u32,
}

impl QueueOptions {
    pub fn from_headers(headers: &str) -> (String, Self) {
        let mut name = String::new();
        let mut opts = Self::default();

        for line in headers.split("\r\n") {
            if line.is_empty() {
                continue;
            }
            if let Some((k, v)) = line.split_once(':') {
                match k {
                    "name" => name = v.to_string(),
                    "durable" => opts.durable = v == "true",
                    "exclusive" => opts.exclusive = v == "true",
                    "auto_delete" => opts.auto_delete = v == "true",
                    "max_priority" => opts.max_priority = v.parse().unwrap_or(0),
                    "message_ttl" => {
                        opts.message_ttl = v.parse::<u64>().ok().map(Duration::from_millis)
                    }
                    "max_length" => opts.max_length = v.parse().ok(),
                    "x-dead-letter-exchange" => opts.dead_letter_exchange = Some(v.to_string()),
                    "x-dead-letter-routing-key" => {
                        opts.dead_letter_routing_key = Some(v.to_string())
                    }
                    "x-expires" => opts.expires = v.parse::<u64>().ok().map(Duration::from_millis),
                    "x-max-retries" => opts.max_retries = v.parse().ok(),
                    "x-retry-delay" => opts.retry_delay_ms = v.parse().ok(),
                    "x-retry-multiplier" => opts.retry_multiplier = v.parse().ok(),
                    "x-rate-limit" => opts.rate_limit = v.parse().ok(),
                    "x-queue-type" => {
                        opts.queue_type = QueueType::from_amqp_arg(Some(v));
                        if v == "stream" {
                            opts.stream_mode = true;
                        }
                    }
                    "x-quorum-initial-group-size" => {
                        opts.quorum_group_size = v.parse().unwrap_or(3);
                    }
                    "x-schema" => opts.schema = Some(v.as_bytes().to_vec()),
                    "x-schema-type" => opts.schema_type = Some(v.to_string()),
                    "x-schema-message" => opts.schema_message = Some(v.to_string()),

                    _ => {}
                }
            }
        }
        (name, opts)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn queue_type_from_amqp_arg_variants() {
        assert_eq!(QueueType::from_amqp_arg(None), QueueType::Classic);
        assert_eq!(
            QueueType::from_amqp_arg(Some("classic")),
            QueueType::Classic
        );
        assert_eq!(QueueType::from_amqp_arg(Some("quorum")), QueueType::Quorum);
        assert_eq!(QueueType::from_amqp_arg(Some("stream")), QueueType::Stream);
        assert_eq!(
            QueueType::from_amqp_arg(Some("unknown")),
            QueueType::Classic
        );
    }

    #[test]
    fn queue_type_as_str_roundtrip() {
        assert_eq!(QueueType::Classic.as_str(), "classic");
        assert_eq!(QueueType::Quorum.as_str(), "quorum");
        assert_eq!(QueueType::Stream.as_str(), "stream");
    }

    #[test]
    fn from_headers_parses_queue_type() {
        let headers = "name:my-q\r\nx-queue-type:quorum\r\nx-quorum-initial-group-size:5";
        let (name, opts) = QueueOptions::from_headers(headers);
        assert_eq!(name, "my-q");
        assert_eq!(opts.queue_type, QueueType::Quorum);
        assert_eq!(opts.quorum_group_size, 5);
    }

    #[test]
    fn from_headers_defaults_to_classic() {
        let headers = "name:classic-q\r\ndurable:true";
        let (_, opts) = QueueOptions::from_headers(headers);
        assert_eq!(opts.queue_type, QueueType::Classic);
        assert!(opts.durable);
    }

    #[test]
    fn from_headers_stream_sets_stream_mode() {
        let headers = "name:stream-q\r\nx-queue-type:stream";
        let (_, opts) = QueueOptions::from_headers(headers);
        assert_eq!(opts.queue_type, QueueType::Stream);
        assert!(opts.stream_mode);
    }
}
