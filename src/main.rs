mod core;
mod queue;
mod routing;
mod server;
mod state;
mod storage;

use std::sync::Arc;
use tracing::info;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "rocketmq=info".parse().unwrap()),
        )
        .init();

    let broker: state::Broker = Arc::new(state::BrokerState::new());

    // Initialize WAL and replay any existing entries (crash recovery)
    let wal = storage::init_with_recovery(&broker)?;

    // Store WAL handle in broker for handlers to use
    broker.set_wal(wal);

    // Spawn background maintenance tasks (queue TTL, message TTL, dedup eviction)
    server::tasks::spawn_all(broker.clone());

    let listener = tokio::net::TcpListener::bind("127.0.0.1:8080").await?;
    info!("listening on 127.0.0.1:8080");

    loop {
        let (stream, addr) = listener.accept().await?;
        server::connection::spawn(stream, addr, broker.clone());
    }
}
