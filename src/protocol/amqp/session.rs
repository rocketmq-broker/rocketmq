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

//! AMQP Connection/Channel session state models.

use bytes::Bytes;
use std::collections::HashMap;
use std::sync::Arc;

/// Per-channel state tracking within an AMQP connection.
///
/// Tracks prefetch limits, unacknowledged message counts, publisher-confirm
/// mode, and flow-control status. Delivery is gated by [`can_deliver`].
pub struct ChannelState {
    pub id: u16,
    pub prefetch_count: u16,
    pub unacked_count: u16,
    pub confirm_mode: bool,
    pub next_delivery_tag: u64,
    pub flow_active: bool,
}

impl ChannelState {
    /// Creates a new instance with the given id.
    pub fn new(id: u16) -> Self {
        Self {
            id,
            prefetch_count: 0,
            unacked_count: 0,
            confirm_mode: false,
            next_delivery_tag: 1,
            flow_active: true,
        }
    }

    /// Returns `true` if this channel is allowed to receive another
    /// delivery, considering both flow-control state and prefetch limits.
    pub fn can_deliver(&self) -> bool {
        if !self.flow_active {
            return false;
        }
        self.prefetch_count == 0 || self.unacked_count < self.prefetch_count
    }
}

/// A single operation buffered inside a transaction (`Tx.Select`).
///
/// Operations are accumulated until the client issues `Tx.Commit` (applied
/// atomically) or `Tx.Rollback` (discarded).
#[derive(Clone, Debug)]
pub enum PendingOp {
    Publish {
        exchange: Arc<str>,
        routing_key: Arc<str>,
        headers: Bytes,
        body: Bytes,
    },
    Ack {
        msg_id: u64,
    },
}

/// Tracks active channels and metadata associated with a single client connection.
/// Per-connection state shared across all channels on a single TCP link.
///
/// Holds the virtual-host binding, channel map, transaction buffer, and
/// exclusive queue ownership for cleanup on disconnect.
pub struct ConnectionState {
    pub channels: HashMap<u16, ChannelState>,
    pub confirm_mode: bool,
    pub next_delivery_tag: u64,
    pub vhost: String,
    pub tx_mode: bool,
    pub tx_buffer: Vec<PendingOp>,
    pub frame_max: u32,
    pub channel_max: u16,
    pub heartbeat: u16,
    pub authenticated: bool,
    pub username: String,
}

impl Default for ConnectionState {
    fn default() -> Self {
        Self::new()
    }
}

impl ConnectionState {
    /// Creates a new instance with default values.
    pub fn new() -> Self {
        Self {
            channels: HashMap::new(),
            confirm_mode: false,
            next_delivery_tag: 1,
            vhost: crate::state::vhost::DEFAULT_VHOST.to_string(),
            tx_mode: false,
            tx_buffer: Vec::new(),
            frame_max: 131_072,
            channel_max: 2047,
            heartbeat: 60,
            authenticated: false,
            username: String::new(),
        }
    }
}

impl crate::protocol::ConnectionMeta for ConnectionState {
    fn username(&self) -> String {
        self.username.clone()
    }

    fn vhost(&self) -> String {
        self.vhost.clone()
    }

    fn channels_count(&self) -> usize {
        self.channels.len()
    }

    fn get_channels(&self) -> Vec<crate::protocol::ChannelMeta> {
        self.channels
            .values()
            .map(|ch| crate::protocol::ChannelMeta {
                id: ch.id,
                prefetch_count: ch.prefetch_count,
                unacked_count: ch.unacked_count,
                confirm_mode: ch.confirm_mode,
                flow_active: ch.flow_active,
            })
            .collect()
    }

    fn heartbeat(&self) -> u16 {
        self.heartbeat
    }

    fn frame_max(&self) -> u32 {
        self.frame_max
    }

    fn channel_max(&self) -> u16 {
        self.channel_max
    }

    fn tx_mode(&self) -> bool {
        self.tx_mode
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}
