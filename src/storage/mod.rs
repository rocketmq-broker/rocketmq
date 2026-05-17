pub mod wal;

use std::collections::HashMap;
use std::sync::Arc;
use tracing::{info, warn};

use crate::broker::{BrokerState, Message, QueueOptions, QueueState};
use crate::exchange::{Binding, Exchange, ExchangeType};
use crate::storage::wal::{EntryType, Wal, WalEntry};

const WAL_PATH: &str = "data/broker.wal";

/// Initialize the WAL and replay any existing entries into the broker.
pub fn init_with_recovery(broker: &Arc<BrokerState>) -> std::io::Result<Arc<Wal>> {
    std::fs::create_dir_all("data")?;
    let wal = Arc::new(Wal::open(WAL_PATH)?);

    let entries = wal.read_all()?;
    if !entries.is_empty() {
        info!(entries = entries.len(), "replaying WAL");
        replay(broker, &entries);
    }

    Ok(wal)
}

/// Replay WAL entries to rebuild broker state after crash.
fn replay(broker: &Arc<BrokerState>, entries: &[WalEntry]) {
    // Track which messages have been acked so we can skip them
    let mut acked_ids: std::collections::HashSet<u64> = std::collections::HashSet::new();

    // First pass: collect all acked message IDs
    for entry in entries {
        if entry.entry_type == EntryType::Ack {
            if entry.data.len() >= 8 {
                let msg_id = u64::from_be_bytes(entry.data[..8].try_into().unwrap());
                acked_ids.insert(msg_id);
            }
        }
    }

    // Second pass: replay state
    for entry in entries {
        match entry.entry_type {
            EntryType::DeclareQueue => replay_declare_queue(broker, &entry.data),
            EntryType::Enqueue => replay_enqueue(broker, &entry.data, &acked_ids),
            EntryType::Ack => {} // Already processed in first pass
            EntryType::DeclareExchange => replay_declare_exchange(broker, &entry.data),
            EntryType::Bind => replay_bind(broker, &entry.data),
        }
    }

    // Log recovery summary
    let queue_count = broker.queues.len();
    let mut msg_count = 0usize;
    for entry in broker.queues.iter() {
        msg_count += entry.value().messages.len();
    }
    info!(
        queues = queue_count,
        messages = msg_count,
        "WAL replay complete"
    );
}

fn replay_declare_queue(broker: &Arc<BrokerState>, data: &[u8]) {
    if data.len() < 3 {
        return;
    }
    let name_len = u16::from_be_bytes([data[0], data[1]]) as usize;
    if data.len() < 2 + name_len + 1 {
        return;
    }
    let name = match std::str::from_utf8(&data[2..2 + name_len]) {
        Ok(s) => s.to_string(),
        Err(_) => return,
    };
    let durable = data[2 + name_len] == 1;

    // Only restore durable queues
    if durable {
        let mut opts = QueueOptions::default();
        opts.durable = true;
        broker
            .queues
            .entry(name.clone())
            .or_insert_with(|| QueueState::with_options(opts));
        broker.auto_bind_default_exchange(&name);
        info!(queue = name.as_str(), "restored durable queue");
    }
}

fn replay_enqueue(
    broker: &Arc<BrokerState>,
    data: &[u8],
    acked_ids: &std::collections::HashSet<u64>,
) {
    let mut offset = 0;

    // queue name
    if data.len() < offset + 2 {
        return;
    }
    let queue_len = u16::from_be_bytes([data[offset], data[offset + 1]]) as usize;
    offset += 2;
    if data.len() < offset + queue_len {
        return;
    }
    let queue_name = match std::str::from_utf8(&data[offset..offset + queue_len]) {
        Ok(s) => s.to_string(),
        Err(_) => return,
    };
    offset += queue_len;

    // msg_id
    if data.len() < offset + 8 {
        return;
    }
    let msg_id = u64::from_be_bytes(data[offset..offset + 8].try_into().unwrap());
    offset += 8;

    // Skip if already acked
    if acked_ids.contains(&msg_id) {
        return;
    }

    // headers
    if data.len() < offset + 4 {
        return;
    }
    let headers_len = u32::from_be_bytes(data[offset..offset + 4].try_into().unwrap()) as usize;
    offset += 4;
    if data.len() < offset + headers_len {
        return;
    }
    let headers = data[offset..offset + headers_len].to_vec();
    offset += headers_len;

    // body
    if data.len() < offset + 4 {
        return;
    }
    let body_len = u32::from_be_bytes(data[offset..offset + 4].try_into().unwrap()) as usize;
    offset += 4;
    if data.len() < offset + body_len {
        return;
    }
    let body = data[offset..offset + body_len].to_vec();

    // Re-enqueue the message (only if queue exists)
    if let Some(mut queue) = broker.queues.get_mut(&queue_name) {
        let msg = Message::new(msg_id, headers, body);
        queue.messages.push_back(msg);
        info!(queue = queue_name.as_str(), msg_id, "restored message");
    }
}

fn replay_declare_exchange(broker: &Arc<BrokerState>, data: &[u8]) {
    if data.len() < 4 {
        return;
    }
    let name_len = u16::from_be_bytes([data[0], data[1]]) as usize;
    if data.len() < 2 + name_len + 2 {
        return;
    }
    let name = match std::str::from_utf8(&data[2..2 + name_len]) {
        Ok(s) => s.to_string(),
        Err(_) => return,
    };
    let kind_byte = data[2 + name_len];
    let durable = data[2 + name_len + 1] == 1;

    if !durable {
        return; // Only restore durable exchanges
    }

    let kind = match kind_byte {
        0x00 => ExchangeType::Direct,
        0x01 => ExchangeType::Fanout,
        0x02 => ExchangeType::Topic,
        0x03 => ExchangeType::Headers,
        _ => return,
    };

    if let Ok(mut exchanges) = broker.exchanges.try_write() {
        exchanges
            .entry(name.clone())
            .or_insert_with(|| Exchange::new(name.clone(), kind, true));
        info!(exchange = name.as_str(), "restored durable exchange");
    }
}

fn replay_bind(broker: &Arc<BrokerState>, data: &[u8]) {
    let mut offset = 0;

    // exchange
    if data.len() < offset + 2 {
        return;
    }
    let ex_len = u16::from_be_bytes([data[offset], data[offset + 1]]) as usize;
    offset += 2;
    if data.len() < offset + ex_len {
        return;
    }
    let exchange = match std::str::from_utf8(&data[offset..offset + ex_len]) {
        Ok(s) => s.to_string(),
        Err(_) => return,
    };
    offset += ex_len;

    // queue
    if data.len() < offset + 2 {
        return;
    }
    let q_len = u16::from_be_bytes([data[offset], data[offset + 1]]) as usize;
    offset += 2;
    if data.len() < offset + q_len {
        return;
    }
    let queue = match std::str::from_utf8(&data[offset..offset + q_len]) {
        Ok(s) => s.to_string(),
        Err(_) => return,
    };
    offset += q_len;

    // routing_key
    if data.len() < offset + 2 {
        return;
    }
    let rk_len = u16::from_be_bytes([data[offset], data[offset + 1]]) as usize;
    offset += 2;
    if data.len() < offset + rk_len {
        return;
    }
    let routing_key = match std::str::from_utf8(&data[offset..offset + rk_len]) {
        Ok(s) => s.to_string(),
        Err(_) => return,
    };

    if let Ok(mut exchanges) = broker.exchanges.try_write() {
        if let Some(ex) = exchanges.get_mut(&exchange) {
            ex.add_binding(Binding {
                queue_name: queue,
                routing_key,
                headers_match: None,
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::path::PathBuf;

    fn tmp_wal(name: &str) -> PathBuf {
        let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("target")
            .join("test_recovery");
        fs::create_dir_all(&dir).unwrap();
        let path = dir.join(name);
        let _ = fs::remove_file(&path);
        path
    }

    #[tokio::test]
    async fn recovery_restores_durable_queues() {
        let path = tmp_wal("recovery_queues.wal");
        let wal = Wal::open(&path).unwrap();

        // Write WAL entries
        wal.log_declare_queue("durable_q", true).unwrap();
        wal.log_declare_queue("transient_q", false).unwrap();
        wal.log_enqueue("durable_q", 1, b"h:v\r\n", b"msg1")
            .unwrap();
        wal.log_enqueue("durable_q", 2, b"", b"msg2").unwrap();
        wal.log_ack(1).unwrap(); // ack msg 1

        // Simulate restart: create fresh broker and replay
        let broker = Arc::new(BrokerState::new());
        let entries = wal.read_all().unwrap();
        replay(&broker, &entries);

        // Only durable queue should exist
        assert!(broker.queues.contains_key("durable_q"));
        assert!(!broker.queues.contains_key("transient_q"));

        // Only un-acked message should be restored
        let q = broker.queues.get("durable_q").unwrap();
        assert_eq!(q.messages.len(), 1); // msg2 only (msg1 was acked)

        let _ = fs::remove_file(&path);
    }

    #[tokio::test]
    async fn recovery_restores_exchanges_and_bindings() {
        let path = tmp_wal("recovery_exchanges.wal");
        let wal = Wal::open(&path).unwrap();

        wal.log_declare_exchange("my.fanout", 0x01, true).unwrap();
        wal.log_declare_queue("q1", true).unwrap();
        wal.log_bind("my.fanout", "q1", "").unwrap();

        let broker = Arc::new(BrokerState::new());
        let entries = wal.read_all().unwrap();
        replay(&broker, &entries);

        let exchanges = broker.exchanges.read().await;
        assert!(exchanges.contains_key("my.fanout"));
        let ex = exchanges.get("my.fanout").unwrap();
        assert_eq!(ex.bindings.len(), 1);
        assert_eq!(ex.bindings[0].queue_name, "q1");

        let _ = fs::remove_file(&path);
    }
}
