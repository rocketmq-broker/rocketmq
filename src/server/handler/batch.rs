//! Batch publish handler.
//!
//! BatchPublish payload format:
//! [count:u16 BE][msg1_len:u32 BE][msg1_data][msg2_len:u32 BE][msg2_data]...
//!
//! Each message data block is the same format as a normal Publish payload
//! (inline headers + body separated by bodyoff).

use tracing::{info, warn};

use crate::core::protocol::Frame;
use crate::state::Broker;

/// Parse a BatchPublish frame and process each message through the normal
/// publish path.
pub async fn batch_publish(conn_id: u64, broker: &Broker, body: &[u8]) {
    if body.len() < 2 {
        warn!(conn_id, "batch_publish: payload too short");
        return;
    }

    let count = u16::from_be_bytes([body[0], body[1]]) as usize;
    let mut offset = 2usize;
    let mut processed = 0usize;

    for _ in 0..count {
        if offset + 4 > body.len() {
            warn!(conn_id, "batch_publish: truncated at message length");
            break;
        }
        let msg_len = u32::from_be_bytes([
            body[offset],
            body[offset + 1],
            body[offset + 2],
            body[offset + 3],
        ]) as usize;
        offset += 4;

        if offset + msg_len > body.len() {
            warn!(conn_id, "batch_publish: truncated at message data");
            break;
        }

        let msg_data = &body[offset..offset + msg_len];
        offset += msg_len;

        // Each message is treated as a publish payload
        // For simplicity, route directly to the queue named in the first header line
        if let Ok(s) = std::str::from_utf8(msg_data) {
            let mut queue_name = None;
            for line in s.split("\r\n") {
                if let Some((k, v)) = line.split_once(':') {
                    if k == "queue" || k == "routing_key" {
                        queue_name = Some(v.to_string());
                        break;
                    }
                }
            }

            if let Some(qn) = queue_name {
                let msg_id = broker.alloc_msg_id();
                if let Some(mut queue) = broker.queues.get_mut(qn.as_str()) {
                    let msg = crate::queue::Message::new(msg_id, Vec::new(), msg_data.to_vec());
                    queue.messages.push_back(msg);
                    processed += 1;
                }
            }
        }
    }

    info!(conn_id, count, processed, "batch_publish completed");
}
