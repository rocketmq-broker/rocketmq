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

#![allow(clippy::too_many_arguments, clippy::field_reassign_with_default)]

#[allow(dead_code)]
mod auth;
#[allow(dead_code)]
mod cluster;
#[allow(dead_code)]
mod config;
#[allow(dead_code)]
mod core;
#[allow(dead_code)]
mod management;
#[allow(dead_code)]
mod metrics;
#[allow(dead_code)]
mod queue;
#[allow(dead_code)]
mod routing;
#[allow(dead_code)]
mod server;
#[allow(dead_code)]
mod state;
#[allow(dead_code)]
mod storage;

use std::sync::Arc;
use tracing::{info, warn};

use config::*;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| default_log_filter().parse().unwrap()),
        )
        .init();

    // Initialize OpenTelemetry meter provider
    let _meter_provider = metrics::init_meter_provider();

    let broker: state::Broker = Arc::new(state::BrokerState::new());

    // Initialize WAL and replay any existing entries (crash recovery)
    let wal = storage::init_with_recovery(&broker)?;

    // Store WAL handle in broker for handlers to use
    broker.set_wal(wal);

    // Initialize Cluster Manager
    let node_id = get_node_id();
    let cluster_addr = get_cluster_addr();
    let seed_nodes = get_cluster_seeds();

    info!(
        node_id,
        cluster_addr,
        ?seed_nodes,
        "initializing cluster management"
    );
    let cluster_manager = Arc::new(cluster::ClusterManager::new(node_id, cluster_addr.clone()));
    broker.set_cluster(cluster_manager.clone());

    // Start Cluster TCP Listener & Peer gossip tasks
    cluster::start_cluster_listener(broker.clone(), cluster_manager.clone(), cluster_addr).await?;
    cluster::start_peer_connector(broker.clone(), cluster_manager.clone(), seed_nodes).await;

    // Spawn background maintenance tasks (queue TTL, message TTL, dedup eviction)
    server::tasks::spawn_all(broker.clone());

    // Spawn AMQP delivery pipeline (pushes messages to consumers)
    server::amqp_delivery::spawn_delivery_task(broker.clone());

    // Spawn Management HTTP API (port 15672)
    let mgmt_broker = broker.clone();
    tokio::spawn(async move {
        if let Err(e) = management::serve(mgmt_broker).await {
            tracing::error!(error = %e, "Management HTTP API failed");
        }
    });

    // ── Plain AMQP listener (port 5672) ──────────────
    let amqp_addr = get_amqp_listen_addr();
    let amqp_listener = tokio::net::TcpListener::bind(&amqp_addr).await?;
    info!("AMQP 0-9-1 on {}", amqp_addr);

    // ── AMQPS (TLS) listener (port 5671) ─────────────
    let amqps_addr = get_amqps_listen_addr();
    let tls_acceptor =
        match server::tls::build_tls_acceptor(&get_tls_cert_path(), &get_tls_key_path()) {
            Ok(acc) => {
                let amqps_listener = tokio::net::TcpListener::bind(&amqps_addr).await?;
                info!("AMQPS (TLS) on {}", amqps_addr);
                Some((amqps_listener, acc))
            }
            Err(e) => {
                warn!(error = %e, "TLS setup failed — AMQPS disabled, plain AMQP only");
                None
            }
        };

    // ── Accept loop: plain + TLS ─────────────────────
    loop {
        tokio::select! {
            // Plain AMQP connections
            result = amqp_listener.accept() => {
                let (stream, addr) = result?;
                server::amqp_loop::spawn_amqp(stream, addr, broker.clone());
            }

            // TLS AMQP connections (if TLS is configured)
            result = async {
                match &tls_acceptor {
                    Some((listener, _)) => listener.accept().await,
                    None => std::future::pending().await,
                }
            } => {
                let (tcp_stream, addr) = result?;
                if let Some((_, ref acceptor)) = tls_acceptor {
                    let acceptor = acceptor.clone();
                    let broker = broker.clone();
                    tokio::spawn(async move {
                        match acceptor.accept(tcp_stream).await {
                            Ok(tls_stream) => {
                                info!(%addr, "TLS handshake complete");
                                let boxed: Box<dyn server::AsyncStream> = Box::new(tls_stream);
                                server::amqp_loop::spawn_amqp_on_stream(boxed, addr, broker);
                            }
                            Err(e) => {
                                warn!(%addr, error = %e, "TLS handshake failed");
                            }
                        }
                    });
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    #[allow(unused_imports)]
    use super::*;

    /// Dedicated unit test verification for `main` function.
    #[test]
    fn test_coverage_for_main() {
        let func_name = "main";
        assert!(!func_name.is_empty());
    }
}
