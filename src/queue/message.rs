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

use std::time::Instant;

/// Represents the schema or state for message.
///
/// Defines details for message inside the broker ecosystem.
#[derive(Clone, Debug)]
pub struct Message {
    pub id: u64,
    pub headers: Vec<u8>,
    pub body: Vec<u8>,
    pub priority: u8,
    pub expiration: Option<Instant>,
    pub redelivered: bool,
    pub delivery_count: u32,
    pub exchange: String,
    pub routing_key: String,
}

impl Message {
    /// Executes the standard new lifecycle step.
    ///
    /// Executes the required business logic for new.
    ///
    /// # Arguments
    ///
    /// * `id` - `u64`: The `id` argument.
    /// * `headers` - `Vec<u8>`: The `headers` argument.
    /// * `body` - `Vec<u8>`: Deserialized JSON payload representation containing request parameters.
    ///
    /// # Returns
    ///
    /// * `Self` - The evaluated outcome or operation handle.
    pub fn new(id: u64, headers: Vec<u8>, body: Vec<u8>) -> Self {
        Self {
            id,
            headers,
            body,
            priority: 0,
            expiration: None,
            redelivered: false,
            delivery_count: 0,
            exchange: String::new(),
            routing_key: String::new(),
        }
    }

    pub fn new_routed(
        id: u64,
        headers: Vec<u8>,
        body: Vec<u8>,
        exchange: String,
        routing_key: String,
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

    /// Executes the standard is expired lifecycle step.
    ///
    /// Executes the required business logic for is expired.
    ///
    /// # Returns
    ///
    /// * `bool` - The evaluated outcome or operation handle.
    pub fn is_expired(&self) -> bool {
        self.expiration.is_some_and(|exp| Instant::now() >= exp)
    }
}

/// Represents the schema or state for message ref.
///
/// Defines details for message ref inside the broker ecosystem.
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
    pub exchange: String,
    pub routing_key: String,
}

/// Defines the various states or variants of queue message.
///
/// Defines details for queue message inside the broker ecosystem.
#[derive(Clone, Debug)]
pub enum QueueMessage {
    Ref(MessageRef),
    Full(Message),
}

impl QueueMessage {
    /// Executes the standard id lifecycle step.
    ///
    /// Executes the required business logic for id.
    ///
    /// # Returns
    ///
    /// * `u64` - The evaluated outcome or operation handle.
    pub fn id(&self) -> u64 {
        match self {
            QueueMessage::Ref(r) => r.id,
            QueueMessage::Full(m) => m.id,
        }
    }

    /// Executes the standard priority lifecycle step.
    ///
    /// Executes the required business logic for priority.
    ///
    /// # Returns
    ///
    /// * `u8` - The evaluated outcome or operation handle.
    pub fn priority(&self) -> u8 {
        match self {
            QueueMessage::Ref(r) => r.priority,
            QueueMessage::Full(m) => m.priority,
        }
    }

    /// Executes the standard expiration lifecycle step.
    ///
    /// Executes the required business logic for expiration.
    ///
    /// # Returns
    ///
    /// * `Option<Instant>` - The evaluated outcome or operation handle.
    pub fn expiration(&self) -> Option<Instant> {
        match self {
            QueueMessage::Ref(r) => r.expiration,
            QueueMessage::Full(m) => m.expiration,
        }
    }

    /// Executes the standard is expired lifecycle step.
    ///
    /// Executes the required business logic for is expired.
    ///
    /// # Returns
    ///
    /// * `bool` - The evaluated outcome or operation handle.
    pub fn is_expired(&self) -> bool {
        self.expiration().is_some_and(|exp| Instant::now() >= exp)
    }

    /// Executes the standard redelivered lifecycle step.
    ///
    /// Executes the required business logic for redelivered.
    ///
    /// # Returns
    ///
    /// * `bool` - The evaluated outcome or operation handle.
    pub fn redelivered(&self) -> bool {
        match self {
            QueueMessage::Ref(r) => r.redelivered,
            QueueMessage::Full(m) => m.redelivered,
        }
    }

    /// Executes the standard set redelivered lifecycle step.
    ///
    /// Executes the required business logic for set redelivered.
    ///
    /// # Arguments
    ///
    /// * `val` - `bool`: The `val` argument.
    pub fn set_redelivered(&mut self, val: bool) {
        match self {
            QueueMessage::Ref(r) => r.redelivered = val,
            QueueMessage::Full(m) => m.redelivered = val,
        }
    }

    /// Executes the standard delivery count lifecycle step.
    ///
    /// Executes the required business logic for delivery count.
    ///
    /// # Returns
    ///
    /// * `u32` - The evaluated outcome or operation handle.
    pub fn delivery_count(&self) -> u32 {
        match self {
            QueueMessage::Ref(r) => r.delivery_count,
            QueueMessage::Full(m) => m.delivery_count,
        }
    }

    /// Executes the standard set delivery count lifecycle step.
    ///
    /// Executes the required business logic for set delivery count.
    ///
    /// # Arguments
    ///
    /// * `val` - `u32`: The `val` argument.
    pub fn set_delivery_count(&mut self, val: u32) {
        match self {
            QueueMessage::Ref(r) => r.delivery_count = val,
            QueueMessage::Full(m) => m.delivery_count = val,
        }
    }

    /// Executes the standard resolve lifecycle step.
    ///
    /// Executes the required business logic for resolve.
    ///
    /// # Arguments
    ///
    /// * `wal` - `&crate::storage::wal::Wal`: The `wal` argument.
    ///
    /// # Returns
    ///
    /// * `std::io::Result<Message>` - A standard rust Result wrapping the status payloads or server failure codes.
    pub fn resolve(self, wal: &crate::storage::wal::Wal) -> std::io::Result<Message> {
        match self {
            QueueMessage::Full(m) => Ok(m),
            QueueMessage::Ref(r) => {
                let (headers, body) =
                    wal.read_message_payload(r.segment_id, r.offset, r.length as usize)?;
                let mut msg = Message::new_routed(
                    r.id,
                    headers,
                    body,
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

    /// Executes the standard unwrap full lifecycle step.
    ///
    /// Executes the required business logic for unwrap full.
    ///
    /// # Returns
    ///
    /// * `Message` - The evaluated outcome or operation handle.
    pub fn unwrap_full(self) -> Message {
        match self {
            QueueMessage::Full(m) => m,
            QueueMessage::Ref(_) => panic!("expected QueueMessage::Full"),
        }
    }
}