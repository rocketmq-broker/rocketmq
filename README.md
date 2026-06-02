# RocketMQ

An AMQP 0-9-1 message broker written from scratch in Rust. Wire-compatible with RabbitMQ — any standard AMQP client (`amqplib`, `pika`, `lapin`, etc.) works out of the box.

## Why

RocketMQ is a ground-up implementation of the AMQP 0-9-1 protocol focused on performance, correctness, and reliability. It offers built-in type-checking by validating published message payloads against defined schemas before they reach consumers. This validation is language-agnostic (using Protobuf under the hood), ensuring schema enforcement at publish time so malformed data is rejected at the broker level and never propagates downstream.

## Features

| Area | Details |
|------|---------|
| **Exchanges** | Direct, Fanout, Topic (`*` / `#` wildcards), Headers |
| **Queues** | Durable, exclusive, priority levels, per-message and per-queue TTL |
| **Delivery** | Publisher confirms, consumer prefetch (QoS), full `Tx` commit/rollback |
| **Type safety** | Language-agnostic schema validation (Protobuf) enforced at publish time |
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
version: '3.8'

services:
  rocketmq:
    image: dockerusername/dockerhunrepo:latest
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

All CI checks (format, clippy with `-Dwarnings`, full test suite) must pass before merge. See the [CI workflow](.github/workflows/ci.yml) for details.

## License

Apache-2.0. See [LICENSE](LICENSE).
