# RocketMQ

An AMQP 0-9-1 message broker written from scratch in Rust. Wire-compatible with RabbitMQ — any standard AMQP client (`amqplib`, `pika`, `lapin`, etc.) works out of the box.

~21k lines of Rust across 66 source files, backed by 657 tests.

## Why

Most AMQP brokers are either legacy Java/Erlang codebases or thin wrappers around existing libraries. RocketMQ is a ground-up implementation focused on correctness, low resource usage, and a single static binary with zero runtime dependencies.

## Features

| Area | Details |
|------|---------|
| **Exchanges** | Direct, Fanout, Topic (`*` / `#` wildcards), Headers |
| **Queues** | Durable, exclusive, priority levels, per-message and per-queue TTL |
| **Delivery** | Publisher confirms, consumer prefetch (QoS), full `Tx` commit/rollback |
| **Schema validation** | Protobuf schemas attached to queues via `x-schema-subject`, validated at publish time |
| **Storage** | Segmented WAL with CRC32 integrity, crash recovery, compaction |
| **Security** | TLS via `tokio-rustls`, bcrypt user authentication, per-vhost permissions |
| **Clustering** | Multi-node Raft-based replication with automatic peer discovery |
| **Management** | RabbitMQ-compatible HTTP API and dashboard on port `15672` |
| **Observability** | OpenTelemetry metrics with Prometheus exporter |

## Quickstart

```bash
# build and run
cargo run --release

# or via docker
docker compose up --build
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
# unit + integration tests (657 tests, ~4s)
cargo test

# lint
cargo clippy --all-targets --all-features

# format check
cargo fmt --check
```

All CI checks (format, clippy with `-Dwarnings`, full test suite) must pass before merge. See the [CI workflow](.github/workflows/ci.yml) for details.

## Project Structure

```
src/
├── auth/          # user credentials, bcrypt, vhost permissions
├── cluster/       # raft consensus, peer networking
├── core/          # AMQP frame codec, protocol constants
├── management/    # HTTP API routes (RabbitMQ-compatible)
├── metrics/       # OpenTelemetry + Prometheus counters
├── queue/         # queue state, priority heap, delay queue, TTL
├── routing/       # exchange types and binding logic
├── schema/        # protobuf schema registry and validation
├── server/        # TCP acceptor, AMQP processors, TLS
├── state/         # broker state, vhosts, channel/connection tracking
└── storage/       # segmented WAL, crash recovery, compaction
```

## License

Apache-2.0. See [LICENSE](LICENSE).
