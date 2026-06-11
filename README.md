# RocketMQ

A high-performance message broker written in Rust, designed with built-in schema validation and minimal operational overhead. Deploy a single binary with no external dependencies.

## Why RocketMQ?

**Schema validation at the broker layer**

Define a schema (Protobuf or JSON) for any queue, and RocketMQ validates messages before they are accepted. Invalid payloads are rejected at publish time, ensuring consumers only receive data that conforms to the expected contract. Schema management is integrated directly into the broker, eliminating the need for a separate schema registry or additional infrastructure.

**AMQP 0-9-1 compatibility**

RocketMQ is compatible with the AMQP 0-9-1 protocol, making migration from RabbitMQ straightforward. Existing clients such as `amqplib`, `pika`, and `lapin` can connect without modification. Support for additional protocols is planned.

**Simple deployment**

RocketMQ runs as a single executable with no JVM, Erlang runtime, ZooKeeper, or other external services required. Build and run with standard Rust tooling, reducing operational complexity and resource requirements.

**Built for performance**

Implemented in Rust, RocketMQ combines predictable performance, memory safety, and efficient resource utilization, making it suitable for both small deployments and high-throughput production workloads.

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
