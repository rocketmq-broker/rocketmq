use std::time::Duration;

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
    /// Queue auto-expires after being idle for this duration (no consumers, no activity).
    pub expires: Option<Duration>,
    /// Maximum delivery attempts before routing to DLX.
    pub max_retries: Option<u32>,
    /// Base retry delay in milliseconds.
    pub retry_delay_ms: Option<u64>,
    /// Retry delay multiplier for exponential backoff.
    pub retry_multiplier: Option<f64>,
    /// Rate limit: max messages per second accepted into this queue.
    pub rate_limit: Option<u32>,
    /// Stream mode: append-only log, messages not removed on ack.
    pub stream_mode: bool,
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
                        if v == "stream" {
                            opts.stream_mode = true;
                        }
                    }
                    _ => {}
                }
            }
        }
        (name, opts)
    }
}
