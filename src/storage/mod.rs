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
// File: mod.rs
// Description: Storage engine and persistence layer interfaces.

pub mod stream;
pub mod wal;

use std::sync::Arc;
use tracing::info;

use crate::queue::{Message, QueueOptions, QueueState};
use crate::routing::exchange::{Binding, Exchange, ExchangeType};
use crate::state::BrokerState;
use crate::storage::wal::{EntryType, Wal, WalEntry};

/// Opens (or creates) the WAL file, returning a shared handle.
pub fn open_wal() -> std::io::Result<Arc<Wal>> {
    std::fs::create_dir_all(crate::config::get_data_dir())?;
    Ok(Arc::new(Wal::open(crate::config::get_wal_path())?))
}

/// Replays WAL entries into the broker to restore durable state after a restart.
pub fn recover(broker: &Arc<BrokerState>) -> std::io::Result<()> {
    let entries = broker.wal().read_all()?;
    if !entries.is_empty() {
        info!(entries = entries.len(), "replaying WAL");
        replay(broker, &entries);
    }
    Ok(())
}

fn replay(broker: &Arc<BrokerState>, entries: &[WalEntry]) {
    let mut acked_ids: std::collections::HashSet<u64> = std::collections::HashSet::new();

    for entry in entries {
        if entry.entry_type == EntryType::Ack && entry.data.len() >= 8 {
            let msg_id = u64::from_be_bytes(entry.data[..8].try_into().unwrap());
            acked_ids.insert(msg_id);
        }
    }

    for entry in entries {
        let res = match entry.entry_type {
            EntryType::DeclareQueue => replay_declare_queue(broker, &entry.data),
            EntryType::Enqueue => replay_enqueue(broker, &entry.data, &acked_ids),
            EntryType::Ack => Ok(()),
            EntryType::DeclareExchange => replay_declare_exchange(broker, &entry.data),
            EntryType::Bind => replay_bind(broker, &entry.data),
            EntryType::SetQueueSchema => replay_set_queue_schema(broker, &entry.data),
            // Raft entries are replayed by the cluster module, not here.
            EntryType::RaftEntry | EntryType::RaftVote => Ok(()),
        };
        if let Err(err) = res {
            tracing::warn!(?entry.entry_type, %err, "Failed to replay WAL entry");
        }
    }

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

#[derive(Debug)]
enum ReplayError {
    UnexpectedEof,
    InvalidUtf8,
    InvalidExchangeType(u8),
}

impl std::fmt::Display for ReplayError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnexpectedEof => write!(f, "unexpected end of file"),
            Self::InvalidUtf8 => write!(f, "invalid UTF-8 string"),
            Self::InvalidExchangeType(b) => write!(f, "invalid exchange type byte: {:#04x}", b),
        }
    }
}

impl std::error::Error for ReplayError {}

type Result<T> = std::result::Result<T, ReplayError>;

struct ReplayReader<'a> {
    data: &'a [u8],
    offset: usize,
}

impl<'a> ReplayReader<'a> {
    /// Creates a new instance with the given data.
    fn new(data: &'a [u8]) -> Self {
        Self { data, offset: 0 }
    }

    fn read_u8(&mut self) -> Result<u8> {
        if self.offset + 1 > self.data.len() {
            return Err(ReplayError::UnexpectedEof);
        }
        let val = self.data[self.offset];
        self.offset += 1;
        Ok(val)
    }

    fn read_u16(&mut self) -> Result<u16> {
        if self.offset + 2 > self.data.len() {
            return Err(ReplayError::UnexpectedEof);
        }
        let val = u16::from_be_bytes([self.data[self.offset], self.data[self.offset + 1]]);
        self.offset += 2;
        Ok(val)
    }

    fn read_u32(&mut self) -> Result<u32> {
        if self.offset + 4 > self.data.len() {
            return Err(ReplayError::UnexpectedEof);
        }
        let val = u32::from_be_bytes([
            self.data[self.offset],
            self.data[self.offset + 1],
            self.data[self.offset + 2],
            self.data[self.offset + 3],
        ]);
        self.offset += 4;
        Ok(val)
    }

    fn read_u64(&mut self) -> Result<u64> {
        if self.offset + 8 > self.data.len() {
            return Err(ReplayError::UnexpectedEof);
        }
        let val = u64::from_be_bytes([
            self.data[self.offset],
            self.data[self.offset + 1],
            self.data[self.offset + 2],
            self.data[self.offset + 3],
            self.data[self.offset + 4],
            self.data[self.offset + 5],
            self.data[self.offset + 6],
            self.data[self.offset + 7],
        ]);
        self.offset += 8;
        Ok(val)
    }

    fn read_slice(&mut self, len: usize) -> Result<&'a [u8]> {
        if self.offset + len > self.data.len() {
            return Err(ReplayError::UnexpectedEof);
        }
        let slice = &self.data[self.offset..self.offset + len];
        self.offset += len;
        Ok(slice)
    }

    fn read_string_u16(&mut self) -> Result<String> {
        let len = self.read_u16()? as usize;
        let bytes = self.read_slice(len)?;
        std::str::from_utf8(bytes)
            .map(|s| s.to_string())
            .map_err(|_| ReplayError::InvalidUtf8)
    }
}

fn replay_declare_queue(broker: &Arc<BrokerState>, data: &[u8]) -> Result<()> {
    let mut reader = ReplayReader::new(data);
    let name = reader.read_string_u16()?;
    let durable = reader.read_u8()? == 1;

    // Only restore durable queues
    if durable {
        let mut opts = QueueOptions::default();
        opts.durable = true;
        broker.queues.entry(name.clone()).or_insert_with(|| {
            let mut q = QueueState::with_options(opts);
            q.name_arc = std::sync::Arc::from(name.as_str());
            q
        });
        broker.auto_bind_default_exchange(&name);
        info!(queue = name.as_str(), "restored durable queue");
    }
    Ok(())
}

fn replay_set_queue_schema(broker: &Arc<BrokerState>, data: &[u8]) -> Result<()> {
    let mut reader = ReplayReader::new(data);
    let schema_id = reader.read_u64()?;
    let queue_name = reader.read_string_u16()?;
    let raw_proto_len = reader.read_u32()? as usize;
    let raw_proto = reader.read_slice(raw_proto_len)?.to_vec();
    let descriptor_set_bytes_len = reader.read_u32()? as usize;
    let descriptor_set_bytes = reader.read_slice(descriptor_set_bytes_len)?.to_vec();
    let message_name = reader.read_string_u16()?;

    if let Some(mut queue) = broker.queues.get_mut(&queue_name) {
        match crate::schema::reconstruct_schema(
            schema_id,
            raw_proto.clone(),
            descriptor_set_bytes,
            &message_name,
        ) {
            Ok(compiled) => {
                queue.schema = Some(std::sync::Arc::new(compiled));
                queue.options.schema = Some(raw_proto);
                queue.options.schema_type = Some("protobuf".to_string());
                queue.options.schema_message = Some(message_name);
                info!(queue = queue_name.as_str(), "restored queue schema");
            }
            Err(err) => {
                tracing::warn!(queue = queue_name.as_str(), %err, "Failed to reconstruct schema during replay");
            }
        }
    }
    Ok(())
}

fn replay_enqueue(
    broker: &Arc<BrokerState>,
    data: &[u8],
    acked_ids: &std::collections::HashSet<u64>,
) -> Result<()> {
    let mut reader = ReplayReader::new(data);

    let queue_name = reader.read_string_u16()?;
    let msg_id = reader.read_u64()?;

    // Skip if already acked
    if acked_ids.contains(&msg_id) {
        return Ok(());
    }

    let exchange = reader.read_string_u16()?;
    let routing_key = reader.read_string_u16()?;

    let headers_len = reader.read_u32()? as usize;
    let headers = reader.read_slice(headers_len)?.to_vec();

    let body_len = reader.read_u32()? as usize;
    let body = reader.read_slice(body_len)?.to_vec();

    // Re-enqueue the message (only if queue exists)
    if let Some(mut queue) = broker.queues.get_mut(&queue_name) {
        let mut msg = Message::new_routed(
            msg_id,
            headers.into(),
            body.into(),
            exchange.into(),
            routing_key.into(),
        );
        msg.redelivered = true; // recovered messages are marked as redelivered
        queue
            .messages
            .push_back(crate::queue::message::QueueMessage::Full(msg));
        info!(queue = queue_name.as_str(), msg_id, "restored message");
    }
    Ok(())
}

fn replay_declare_exchange(broker: &Arc<BrokerState>, data: &[u8]) -> Result<()> {
    let mut reader = ReplayReader::new(data);
    let name = reader.read_string_u16()?;
    let kind_byte = reader.read_u8()?;
    let durable = reader.read_u8()? == 1;

    if !durable {
        return Ok(()); // Only restore durable exchanges
    }

    let kind = match kind_byte {
        0x00 => ExchangeType::Direct,
        0x01 => ExchangeType::Fanout,
        0x02 => ExchangeType::Topic,
        0x03 => ExchangeType::Headers,
        _ => return Err(ReplayError::InvalidExchangeType(kind_byte)),
    };

    if let Ok(mut exchanges) = broker.exchanges.try_write() {
        exchanges
            .entry(name.clone())
            .or_insert_with(|| Exchange::new(name.clone(), kind, true));
        info!(exchange = name.as_str(), "restored durable exchange");
    }
    Ok(())
}

fn replay_bind(broker: &Arc<BrokerState>, data: &[u8]) -> Result<()> {
    let mut reader = ReplayReader::new(data);
    let exchange = reader.read_string_u16()?;
    let queue = reader.read_string_u16()?;
    let routing_key = reader.read_string_u16()?;

    if let Ok(mut exchanges) = broker.exchanges.try_write() {
        if let Some(ex) = exchanges.get_mut(&exchange) {
            ex.add_binding(Binding {
                queue_name: queue.into(),
                routing_key: routing_key.into(),
                headers_match: None,
            });
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    #[allow(unused_imports)]
    use super::*;
    use std::fs;
    use std::path::PathBuf;

    /// Creates a temporary WAL directory for testing and returns the
    /// path to the WAL file inside it.
    fn tmp_wal(name: &str) -> PathBuf {
        let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("target")
            .join("test_recovery")
            .join(name.replace(".wal", ""));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        dir.join("broker.wal")
    }

    #[tokio::test]
    async fn recovery_restores_durable_queues() {
        let path = tmp_wal("recovery_queues.wal");
        let wal = Wal::open(&path).unwrap();

        // Write WAL entries
        wal.log_declare_queue("durable_q", true).unwrap();
        wal.log_declare_queue("transient_q", false).unwrap();
        wal.log_enqueue("durable_q", 1, "", "", b"h:v\r\n", b"msg1")
            .unwrap();
        wal.log_enqueue("durable_q", 2, "", "", b"", b"msg2")
            .unwrap();
        wal.log_ack(1).unwrap(); // ack msg 1

        // Simulate restart: create fresh broker and replay
        let wal = Arc::new(wal);
        let broker = Arc::new(BrokerState::new(wal.clone()));
        let entries = wal.read_all().unwrap();
        replay(&broker, &entries);

        // Only durable queue should exist
        assert!(broker.queues.contains_key("durable_q"));
        assert!(!broker.queues.contains_key("transient_q"));

        // Only un-acked message should be restored
        let q = broker.queues.get("durable_q").unwrap();
        assert_eq!(q.messages.len(), 1); // msg2 only (msg1 was acked)

        let _ = fs::remove_dir_all(path.parent().unwrap());
    }

    #[tokio::test]
    async fn recovery_restores_durable_queues_with_schema() {
        let path = tmp_wal("recovery_schema_queues.wal");
        let wal = Wal::open(&path).unwrap();

        // Compile a dummy schema to get valid descriptor_set_bytes
        const TEST_PROTO: &str = r#"
            syntax = "proto3";
            package test;
            message Point {
                int32 x = 1;
                int32 y = 2;
            }
        "#;
        let compiled = crate::schema::compile_proto(TEST_PROTO.as_bytes(), "test.Point").unwrap();

        // Write WAL entries
        wal.log_declare_queue("schema_q", true).unwrap();
        wal.log_set_queue_schema(
            compiled.id,
            "schema_q",
            &compiled.raw,
            &compiled.descriptor_set_bytes,
            "test.Point",
        )
        .unwrap();

        // Simulate restart: create fresh broker and replay
        let wal = Arc::new(wal);
        let broker = Arc::new(BrokerState::new(wal.clone()));
        let entries = wal.read_all().unwrap();
        replay(&broker, &entries);

        // Verify queue and schema are restored
        assert!(broker.queues.contains_key("schema_q"));
        let q = broker.queues.get("schema_q").unwrap();
        assert!(q.schema.is_some());

        let schema = q.schema.as_ref().unwrap();
        assert_eq!(schema.id, compiled.id);
        assert_eq!(schema.raw, compiled.raw);
        assert_eq!(
            q.options.schema.as_ref().unwrap().as_slice(),
            compiled.raw.as_slice()
        );
        assert_eq!(q.options.schema_type.as_ref().unwrap().as_str(), "protobuf");
        assert_eq!(
            q.options.schema_message.as_ref().unwrap().as_str(),
            "test.Point"
        );

        // Let's verify we can validate payloads using the recovered schema

        let valid_payload = vec![0x08, 0x0a, 0x10, 0x14];
        let val_res = crate::schema::validate::validate_message(schema, &valid_payload);
        assert!(val_res.is_ok());

        let invalid_payload = vec![0x18, 0x05];
        let val_res_invalid = crate::schema::validate::validate_message(schema, &invalid_payload);
        assert!(val_res_invalid.is_err());

        let _ = fs::remove_dir_all(path.parent().unwrap());
    }

    #[tokio::test]
    async fn recovery_restores_exchanges_and_bindings() {
        let path = tmp_wal("recovery_exchanges.wal");
        let wal = Wal::open(&path).unwrap();

        wal.log_declare_exchange("my.fanout", 0x01, true).unwrap();
        wal.log_declare_queue("q1", true).unwrap();
        wal.log_bind("my.fanout", "q1", "").unwrap();

        let wal = Arc::new(wal);
        let broker = Arc::new(BrokerState::new(wal.clone()));
        let entries = wal.read_all().unwrap();
        replay(&broker, &entries);

        let exchanges = broker.exchanges.read().await;
        assert!(exchanges.contains_key("my.fanout"));
        let ex = exchanges.get("my.fanout").unwrap();
        assert_eq!(ex.bindings.len(), 1);
        assert_eq!(ex.bindings[0].queue_name.as_ref(), "q1");

        let _ = fs::remove_dir_all(path.parent().unwrap());
    }
}
