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

/// Configurable parameters for queue creation (durable, exclusive, TTL, DLX, etc.).
///
/// Configurable parameters for queue creation (durable, exclusive, TTL, DLX, etc.).
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
}

impl QueueOptions {
    /// Executes the standard from headers lifecycle step.
    ///
    /// Executes the required business logic for from headers.
    ///
    /// # Arguments
    ///
    /// * `headers` - `&str`: The `headers` argument.
    ///
    /// # Returns
    ///
    /// * `(String, Self)` - The evaluated outcome or operation handle.
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
                    "x-queue-type" if v == "stream" => {
                        opts.stream_mode = true;
                    }
                    _ => {}
                }
            }
        }
        (name, opts)
    }
}
