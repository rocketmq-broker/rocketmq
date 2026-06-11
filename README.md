# RocketMQ

A high-performance message broker with **built-in schema enforcement**, written in Rust. One binary, no external services.

## Why RocketMQ

**Messages are validated before they're accepted.** Attach a schema (Protobuf or JSON) to any queue and the broker rejects malformed payloads at publish time. Bad data never reaches your consumers. No schema registry to deploy, no sidecar to maintain — it's part of the broker.

**Drop-in RabbitMQ alternative.** Wire-compatible with AMQP 0-9-1. Point any standard client (`amqplib`, `pika`, `lapin`) at it and it works. More protocols planned.

**Single binary, zero dependencies.** No JVM, no Zookeeper, no Erlang runtime. `cargo build` and you're running.

## Features

| Area | Details |
|------|---------|
| **Exchanges** | Direct, Fanout, Topic (`*` / `#` wildcards), Headers |
| **Queues** | Durable, exclusive, priority levels, per-message and per-queue TTL |
| **Delivery** | Publisher confirms, consumer prefetch (QoS), full `Tx` commit/rollback |
| **Type safety** | Built-in schema validation (Protobuf / JSON) enforced at publish time — no external registry needed |
| **Storage** | Segmented WAL with CRC32 integrity, crash recovery, compaction |
| **Security** | TLS via `tokio-rustls`, bcrypt user authentication, per-vhost permissions |
| **Clustering** | Multi-node Raft-based replication with automatic peer discovery |
| **Management** | RabbitMQ-compatible HTTP API and dashboard on port `15672` |
| **Observability** | OpenTelemetry metrics with Prometheus exporter |

## Quickstart

### Build from source

1. Clone the repository:
   ```bash
   git clone https://github.com/rocketmq-broker/rocketmq.git
   cd rocketmq
   ```

2. Run the broker:
   ```bash
   cargo run --release
   ```

### Run with Docker Compose

Alternatively, use this `docker-compose.yml` to run RocketMQ from the Docker Hub image:

```yaml
services:
  rocketmq:
    image: rocketbroker/rocketmq:latest
    ports:
      - "5672:5672"
      - "15672:15672"
    environment:
      - ROCKETMQ_DEFAULT_USER=guest
      - ROCKETMQ_DEFAULT_PASS=guest
    volumes:
      - rocketmq-data:/data

volumes:
  rocketmq-data:
```

Default ports:

| Port | Protocol |
|------|----------|
| `5672` | AMQP |
| `5671` | AMQPS (TLS) |
| `15672` | Management HTTP (credentials: `guest` / `guest`) |

## Configuration

Environment variables or `rocketmq.conf`:

| Variable | Default | Description |
|----------|---------|-------------|
| `ROCKETMQ_BIND_HOST` | `127.0.0.1` | Bind address |
| `ROCKETMQ_AMQP_PORT` | `5672` | AMQP port |
| `ROCKETMQ_AMQPS_PORT` | `5671` | AMQPS port |
| `ROCKETMQ_MGMT_PORT` | `15672` | Management UI port |
| `ROCKETMQ_DATA_DIR` | `data` | WAL and user database path |
| `ROCKETMQ_CLUSTER_ENABLED` | `false` | Enable clustering |
| `ROCKETMQ_CLUSTER_SEEDS` | — | Comma-separated peer addresses |

## Testing

```bash
cargo test
cargo clippy --all-targets --all-features
cargo fmt --check
```


## License

Apache-2.0. See [LICENSE](LICENSE).
