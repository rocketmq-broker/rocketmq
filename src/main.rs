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

    // Legacy RQ protocol listener (will be removed after full migration)
    let legacy_broker = broker.clone();
    tokio::spawn(async move {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:8080").await.unwrap();
        info!("legacy RQ protocol on 127.0.0.1:8080");
        loop {
            let (stream, addr) = listener.accept().await.unwrap();
            server::connection::spawn(stream, addr, legacy_broker.clone());
        }
    });

    // AMQP 0-9-1 listener on standard port
    let amqp_listener = tokio::net::TcpListener::bind("127.0.0.1:5672").await?;
    info!("AMQP 0-9-1 on 127.0.0.1:5672");

    loop {
        let (stream, addr) = amqp_listener.accept().await?;
        server::amqp_loop::spawn_amqp(stream, addr, broker.clone());
    }
}
