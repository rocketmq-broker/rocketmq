# RocketMQ

A message broker with built-in schema validation, written from scratch in Rust. Currently speaks AMQP 0-9-1 (wire-compatible with RabbitMQ), with more protocols on the roadmap.

## Why

Most brokers treat messages as opaque blobs and push schema validation to external services — a schema registry you have to deploy, a sidecar you have to maintain, or client-side logic you have to trust. RocketMQ takes a different approach: **schema validation lives inside the broker itself**.

When a queue declares a schema (Protobuf or JSON), every message published to that queue is validated at publish time by the broker core. Malformed payloads are rejected before they ever reach a consumer. No sidecar, no gateway, no external service — just the broker doing what a broker should do.

The validation is language-agnostic: define your schema once and every producer, regardless of language or client library, gets the same enforcement. This makes RocketMQ particularly well-suited for polyglot architectures where you can't rely on every team using the same serialization library.

Beyond schema validation, RocketMQ is a ground-up Rust implementation focused on performance, correctness, and operational simplicity. It currently implements AMQP 0-9-1 (any standard AMQP client — `amqplib`, `pika`, `lapin`, etc. — works out of the box), with additional protocol support planned.

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
