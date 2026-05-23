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
// Description: AMQP network server module declarations.

pub mod amqp_connection;
pub mod amqp_delivery;
pub mod amqp_loop;
pub mod handler;
pub mod tasks;
pub mod tls;

use tokio::io::{BufWriter, WriteHalf};

pub type AmqpWriter = BufWriter<WriteHalf<Box<dyn crate::server::AsyncStream>>>;

/// Defines behavioral capabilities for async stream.
///
/// Defines details for async stream inside the broker ecosystem.
pub trait AsyncStream: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send {}
impl<T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send> AsyncStream for T {}
