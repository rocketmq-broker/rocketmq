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
// File: main.rs
// Description: Main entry point for the RocketMQ AMQP broker application.

#![allow(
    clippy::too_many_arguments,
    clippy::field_reassign_with_default,
    clippy::collapsible_if
)]

#[allow(dead_code)]
mod auth;
#[allow(dead_code)]
mod cluster;
#[allow(dead_code)]
mod config;
#[allow(dead_code)]
mod management;
#[allow(dead_code)]
mod metrics;
#[allow(dead_code)]
pub mod protocol;
#[allow(dead_code)]
mod queue;
#[allow(dead_code)]
mod routing;
#[allow(dead_code)]
pub mod schema;
#[allow(dead_code)]
mod server;
#[allow(dead_code)]
mod state;
#[allow(dead_code)]
mod storage;

use std::sync::Arc;

use config::*;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| default_log_filter().parse().unwrap()),
        )
        .init();

    let _meter_provider = metrics::init_meter_provider();

    let wal = storage::open_wal()?;

    let broker: state::Broker = Arc::new(state::BrokerState::new(wal));

    // Register broker-state observable gauges
    metrics::broker_gauges::register_all(broker.clone());

    storage::recover(&broker)?;

    cluster::init_if_enabled(&broker).await?;

    server::tasks::spawn_all(broker.clone());

    let mgmt_broker = broker.clone();
    tokio::spawn(async move {
        if let Err(e) = management::serve(mgmt_broker).await {
            tracing::error!(error = %e, "Management HTTP API failed");
        }
    });

    protocol::start_adapters(broker.clone()).await?;

    std::future::pending::<()>().await;
    Ok(())
}
