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

//! AMQP 0-9-1 protocol implementation.

use std::sync::Arc;

pub mod codec;
pub mod connection;
pub mod connection_loop;
pub mod delivery;
pub mod handlers;
pub mod method;
pub mod properties;
pub mod session;
pub mod types;
pub mod validation;

/// The AMQP writer type.
pub type AmqpWriter =
    tokio::io::BufWriter<tokio::io::WriteHalf<Box<dyn crate::protocol::AsyncStream>>>;

/// AMQP 0-9-1 implementation of the ProtocolAdapter trait.
#[derive(Clone, Copy)]
pub struct AmqpProtocol;

impl Default for AmqpProtocol {
    fn default() -> Self {
        Self::new()
    }
}

impl AmqpProtocol {
    /// Create a new instance of AmqpProtocol.
    pub fn new() -> Self {
        Self
    }
    pub fn name(&self) -> &'static str {
        "AMQP-0-9-1"
    }

    pub async fn start(
        self,
        broker: Arc<crate::state::BrokerState>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        crate::protocol::amqp::delivery::spawn_delivery_task(broker.clone());
        Ok(())
    }

    pub fn handle_stream(
        self,
        stream: Box<dyn crate::protocol::AsyncStream>,
        addr: std::net::SocketAddr,
        broker: Arc<crate::state::BrokerState>,
    ) {
        crate::protocol::amqp::connection_loop::spawn_amqp_on_stream(stream, addr, broker);
    }
}
