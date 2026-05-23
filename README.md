# RocketMQ

A message broker written from scratch in Rust, implementing the AMQP 0-9-1 protocol. It speaks the same wire protocol as RabbitMQ, which means existing AMQP clients (pika, amqplib, etc.) connect to it without any modification. The project started as a way to deeply understand how message brokers work under the hood, and grew into something that actually handles real workloads.

## Why this exists

Most developers treat message brokers as black boxes. You `docker run rabbitmq`, publish messages, consume them, and never think about what happens between those two operations. I wanted to know what happens. How does a broker parse AMQP frames off the wire? How does exchange routing actually work? What does a write-ahead log look like when you build one yourself? This project is the answer to those questions, written in roughly 18,000 lines of Rust.

## What it does

RocketMQ is a fully functional AMQP 0-9-1 broker. Clients connect over TCP on port 5672, authenticate via SASL PLAIN, open channels, declare queues and exchanges, publish messages, and consume them. The protocol implementation covers the core AMQP lifecycle:

- **Connection handshake** -- full `Connection.Start` / `Start-Ok` / `Tune` / `Tune-Ok` / `Open` / `Open-Ok` negotiation
- **Channel multiplexing** -- multiple logical channels over a single TCP connection, each with independent state
- **Exchange routing** -- direct, fanout, topic (with `*` and `#` wildcards), and headers exchanges, all with proper binding management
- **Queue operations** -- declare, bind, unbind, purge, delete, with support for durable, exclusive, and auto-delete queues
- **Message delivery** -- `Basic.Publish`, `Basic.Deliver`, `Basic.Get`, `Basic.Ack`, `Basic.Nack`, `Basic.Reject`
- **QoS / prefetch** -- per-channel prefetch limits that gate delivery until the consumer acknowledges
- **Publisher confirms** -- opt-in confirm mode where the broker acknowledges each published message
- **Transactions** -- `Tx.Select`, `Tx.Commit`, `Tx.Rollback` for batching publishes and acks atomically
- **Flow control** -- `Channel.Flow` / `Channel.FlowOk` for pausing and resuming delivery on a channel
- **TLS** -- AMQPS on port 5671 via `tokio-rustls`, with automatic certificate generation for local development

The broker enforces AMQP compliance. Reserved `amq.*` prefixes are protected. Invalid exchange types return `503 COMMAND_INVALID`. Missing queues return `404 NOT_FOUND`. The integration test suite (using Python's `pika` library) validates all of this against the running broker.

## Architecture

The broker runs on Tokio. Each client connection spawns its own async task that reads AMQP frames off the socket, dispatches them to the appropriate handler, and writes response frames back. There is no thread-per-connection overhead.

The major internal modules:

**`core/`** -- AMQP frame codec, method definitions, content header properties, and frame validation. This is the parser that turns raw TCP bytes into structured AMQP frames and back.

**`server/`** -- Connection lifecycle, the main AMQP read loop, delivery pipeline, TLS termination, and background maintenance tasks. The `handler/` subdirectory contains individual handlers for each AMQP method class (basic, queue, exchange, tx).

**`routing/`** -- Exchange types and routing logic. Direct exchanges match routing keys exactly. Fanout broadcasts to all bound queues. Topic matching uses recursive pattern evaluation for `*` (one word) and `#` (zero or more words). Headers exchanges support both `all` and `any` match modes.

**`queue/`** -- Queue state, message storage, priority queue support, delayed message scheduling, and message TTL enforcement.

**`storage/`** -- Write-ahead log built on a segmented file architecture. Each WAL entry is checksummed with CRC32 for integrity. Segments rotate at 64MB by default. On startup, the broker replays the WAL to recover durable queues, exchanges, bindings, and in-flight messages.

**`auth/`** -- SASL PLAIN authentication, bcrypt-hashed credential storage persisted to disk, and per-user permission enforcement (configure/write/read regex patterns on resource names).

**`cluster/`** -- Multi-node clustering over a separate TCP protocol, with gossip-based peer discovery and a Raft consensus implementation for replicated state. The cluster layer handles leader election, log replication, and split-brain protection.

**`management/`** -- HTTP API on port 15672 that serves the RabbitMQ Management UI. The API implements enough of the RabbitMQ management interface (`/api/overview`, `/api/queues`, `/api/connections`, `/api/exchanges`, `/api/nodes`, etc.) to drive the stock management dashboard with live telemetry data.

**`metrics/`** -- OpenTelemetry instrumentation with periodic metric sampling for message rates, queue depths, and connection churn.

**`state/`** -- Centralized broker state coordination. Virtual host isolation, connection tracking, queue registry, and the glue that ties everything together. Uses `DashMap` for concurrent access and `RwLock` for exchange metadata.

## Getting started

You need Rust (2024 edition) and Cargo.

Build and run a single node:

```
cargo build
cargo run
```

The broker binds to `127.0.0.1:5672` (AMQP), `127.0.0.1:5671` (AMQPS), and `127.0.0.1:15672` (Management UI) by default.

Connect with any AMQP 0-9-1 client. Default credentials are `guest` / `guest`.

### Running a local cluster

The `run.sh` script spins up a 3-node cluster locally, generates TLS certificates, and launches a NestJS integration client that exercises the broker under load:

```
./run.sh
```

This starts:
- Node 1 on ports 5672 / 15672
- Node 2 on ports 5673 / 15673
- Node 3 on ports 5674 / 15674

All logs go to `./logs/`. Press `Ctrl+C` to tear everything down cleanly.

### Docker

Build and run a 3-node cluster with Docker Compose:

```
docker compose up --build
```

Each node gets its own data volume. TLS certificates are generated automatically by an init container before the broker nodes start. The compose file also includes a NestJS client container that connects to the cluster on startup.

## Configuration

All configuration is done through environment variables. There are no config files to manage.

- `ROCKETMQ_NODE_ID` -- numeric node identifier (default: 1)
- `ROCKETMQ_BIND_HOST` -- listen address (default: `127.0.0.1`)
- `ROCKETMQ_AMQP_PORT` -- AMQP port (default: 5672)
- `ROCKETMQ_AMQPS_PORT` -- AMQPS port (default: 5671)
- `ROCKETMQ_MGMT_PORT` -- Management API port (default: 15672)
- `ROCKETMQ_DATA_DIR` -- data directory for WAL, segments, and user database (default: `data`)
- `ROCKETMQ_CLUSTER_ADDR` -- this node's cluster communication address (default: `127.0.0.1:5680`)
- `ROCKETMQ_CLUSTER_SEEDS` -- comma-separated list of seed node addresses for cluster join
- `ROCKETMQ_MAX_SEGMENT_SIZE` -- WAL segment size in bytes before rotation (default: 67108864 / 64MB)
- `RUST_LOG` -- log level filter (default: `rocketmq=info`)

## Testing

Integration tests use Python's `pika` library against the live broker:

```
pip install pika
cargo run &
python3 tests/amqp_integration.py
```

The test suite covers connection auth, channel lifecycle, queue declare (named and server-generated), exchange declare and binding, publish/get, consume/deliver/ack, QoS, queue purge, confirm mode, transactions, and AMQP compliance error codes.

There are also cluster-level integration tests and a JavaScript test using `amqplib`:

```
node tests/amqp_integration.js
python3 tests/cluster_integration.py
```

Unit tests cover exchange routing logic, topic pattern matching, WAL serialization/deserialization, broker state management, priority queues, and Raft consensus:

```
cargo test
```

## Project structure

```
src/
  main.rs              -- entry point, listener setup, task orchestration
  config.rs            -- all tunable constants and env var parsing
  auth/                -- SASL PLAIN, credential storage, permissions
  cluster/             -- peer discovery, gossip, Raft consensus
  core/                -- AMQP frame codec, method types, validation
  management/          -- HTTP API + static dashboard assets
  metrics/             -- OpenTelemetry meter provider
  queue/               -- queue state, priority queues, delayed delivery
  routing/             -- exchange types and message routing
  server/              -- connection loop, delivery pipeline, TLS, handlers
  state/               -- broker state, virtual hosts, connection tracking
  storage/             -- segmented WAL, CRC integrity, crash recovery
tests/                 -- integration tests (Python, JavaScript)
client/                -- NestJS microservice client for load testing
docs/                  -- internal design documents and implementation plans
```

## Current status

The core broker is production-grade for single-node deployments. AMQP 0-9-1 wire compatibility has been validated against both `pika` (Python) and `amqplib` (Node.js). The management dashboard serves live telemetry. Clustering works for basic multi-node setups with Raft-based leader election.

Areas still under active development are documented in the `docs/` directory, including plans for segment-backed disk queues, tiered storage, stream/replay support, and advanced cluster features like federation and geo-replication.

## License

Apache License, Version 2.0
