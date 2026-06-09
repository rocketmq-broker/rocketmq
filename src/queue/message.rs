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
// File: message.rs
// Description: Message structures, envelope representation, and persistence adapters.

use bytes::Bytes;
use std::sync::Arc;
use std::time::Instant;

#[derive(Clone, Debug)]
pub struct Message {
    pub id: u64,
    pub headers: Bytes,
    pub body: Bytes,
    pub priority: u8,
    pub expiration: Option<Instant>,
    pub redelivered: bool,
    pub delivery_count: u32,
    pub exchange: Arc<str>,
    pub routing_key: Arc<str>,
}

impl Message {
    /// Creates a new message with the given ID, serialized headers, and body payload.
    pub fn new(id: u64, headers: Bytes, body: Bytes) -> Self {
        Self {
            id,
            headers,
            body,
            priority: 0,
            expiration: None,
            redelivered: false,
            delivery_count: 0,
            exchange: Arc::from(""),
            routing_key: Arc::from(""),
        }
    }

    pub fn new_routed(
        id: u64,
        headers: Bytes,
        body: Bytes,
        exchange: Arc<str>,
        routing_key: Arc<str>,
    ) -> Self {
        Self {
            id,
            headers,
            body,
            priority: 0,
            expiration: None,
            redelivered: false,
            delivery_count: 0,
            exchange,
            routing_key,
        }
    }

    pub fn is_expired(&self) -> bool {
        self.expiration.is_some_and(|exp| Instant::now() >= exp)
    }
}

#[derive(Clone, Debug)]
pub struct MessageRef {
    pub id: u64,
    pub segment_id: u64,
    pub offset: u64,
    pub length: u32,
    pub priority: u8,
    pub expiration: Option<Instant>,
    pub redelivered: bool,
    pub delivery_count: u32,
    pub exchange: Arc<str>,
    pub routing_key: Arc<str>,
}

/// A message stored in a queue, either as a fully materialized payload
/// or as a reference into a WAL segment for lazy loading.
#[derive(Clone, Debug)]
pub enum QueueMessage {
    Ref(MessageRef),
    Full(Message),
}

impl QueueMessage {
    pub fn id(&self) -> u64 {
        match self {
            QueueMessage::Ref(r) => r.id,
            QueueMessage::Full(m) => m.id,
        }
    }

    pub fn priority(&self) -> u8 {
        match self {
            QueueMessage::Ref(r) => r.priority,
            QueueMessage::Full(m) => m.priority,
        }
    }

    pub fn expiration(&self) -> Option<Instant> {
        match self {
            QueueMessage::Ref(r) => r.expiration,
            QueueMessage::Full(m) => m.expiration,
        }
    }

    pub fn is_expired(&self) -> bool {
        self.expiration().is_some_and(|exp| Instant::now() >= exp)
    }

    pub fn redelivered(&self) -> bool {
        match self {
            QueueMessage::Ref(r) => r.redelivered,
            QueueMessage::Full(m) => m.redelivered,
        }
    }

    pub fn set_redelivered(&mut self, val: bool) {
        match self {
            QueueMessage::Ref(r) => r.redelivered = val,
            QueueMessage::Full(m) => m.redelivered = val,
        }
    }

    pub fn delivery_count(&self) -> u32 {
        match self {
            QueueMessage::Ref(r) => r.delivery_count,
            QueueMessage::Full(m) => m.delivery_count,
        }
    }

    pub fn set_delivery_count(&mut self, val: u32) {
        match self {
            QueueMessage::Ref(r) => r.delivery_count = val,
            QueueMessage::Full(m) => m.delivery_count = val,
        }
    }

    pub fn resolve(self, wal: &crate::storage::wal::Wal) -> std::io::Result<Message> {
        match self {
            QueueMessage::Full(m) => Ok(m),
            QueueMessage::Ref(r) => {
                let (headers, body) =
                    wal.read_message_payload(r.segment_id, r.offset, r.length as usize)?;
                let mut msg = Message::new_routed(
                    r.id,
                    headers.into(),
                    body.into(),
                    r.exchange.clone(),
                    r.routing_key.clone(),
                );
                msg.priority = r.priority;
                msg.expiration = r.expiration;
                msg.redelivered = r.redelivered;
                msg.delivery_count = r.delivery_count;
                Ok(msg)
            }
        }
    }

    pub fn unwrap_full(self) -> Message {
        match self {
            QueueMessage::Full(m) => m,
            QueueMessage::Ref(_) => panic!("expected QueueMessage::Full"),
        }
    }
}
