#!/usr/bin/env bash

# Exit immediately if a command exits with a non-zero status
set -e

# Setup colors for console output
GREEN='\033[0;32m'
CYAN='\033[0;36m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo -e "${CYAN}=======================================================${NC}"
echo -e "${CYAN}    RocketMQ Local Cluster Simulator & Test Harness    ${NC}"
echo -e "${CYAN}=======================================================${NC}"

# Function to aggressively free up our ports
clean_ports() {
    echo -e "${YELLOW}Aggressively clearing cluster ports (3000, 3001, 5672-5674, 15672-15674, 5680-5682)...${NC}"
    for port in 3000 3001 5672 5673 5674 15672 15673 15674 5680 5681 5682; do
        if command -v lsof >/dev/null 2>&1; then
            pids=$(lsof -t -i tcp:$port || true)
            if [ -n "$pids" ]; then
                echo "Evicting process on port $port (PIDs: $pids)"
                kill -9 $pids >/dev/null 2>&1 || true
            fi
        fi
        if command -v fuser >/dev/null 2>&1; then
            fuser -k -n tcp $port >/dev/null 2>&1 || true
        fi
    done
}

# Function to clean up background processes on exit
cleanup() {
    echo -e "\n${YELLOW}Stopping all services gracefully...${NC}"
    
    # Kill NestJS client
    pkill -f "node dist/main.js" || true
    pkill -f "nest start" || true

    # Kill NextJS management console
    pkill -f "next dev" || true

    # Kill Rust broker nodes
    pkill -f "target/debug/rocketmq" || true
    pkill -f "cargo run" || true

    # Clean ports aggressively
    clean_ports

    echo -e "${GREEN}All services successfully terminated.${NC}"
}

# Clear any active ports before launching new instances
clean_ports

# Trap SIGINT, SIGTERM, and EXIT to trigger cleanup
trap cleanup INT TERM EXIT

# Make sure logs directory exists
mkdir -p logs
rm -rf logs/*

# Build Rust broker first to minimize startup latency in loop
echo -e "${YELLOW}Pre-building Rust Broker...${NC}"
cargo build

# Build NestJS client first to run production bundle directly
echo -e "${YELLOW}Pre-building NestJS Client...${NC}"
(cd client && npm run build)

# ── Launching Rust Broker Nodes ─────────────────────────────────────────

# Create separate data directories to avoid WAL/DB lock conflicts
mkdir -p data/node1 data/node2 data/node3
rm -rf data/node1/* data/node2/* data/node3/*

echo -e "${GREEN}Launching Node 1 (Ports: AMQP 5672, AMQPS 5675, Mgmt 15672)...${NC}"
ROCKETMQ_NODE_ID=1 \
ROCKETMQ_BIND_HOST=127.0.0.1 \
ROCKETMQ_CLUSTER_ADDR=127.0.0.1:5680 \
ROCKETMQ_CLUSTER_SEEDS="" \
ROCKETMQ_AMQP_PORT=5672 \
ROCKETMQ_AMQPS_PORT=5675 \
ROCKETMQ_MGMT_PORT=15672 \
ROCKETMQ_DATA_DIR=data/node1 \
RUST_LOG=info \
./target/debug/rocketmq > logs/node1.log 2>&1 &

echo -e "${GREEN}Launching Node 2 (Ports: AMQP 5673, AMQPS 5676, Mgmt 15673)...${NC}"
ROCKETMQ_NODE_ID=2 \
ROCKETMQ_BIND_HOST=127.0.0.1 \
ROCKETMQ_CLUSTER_ADDR=127.0.0.1:5681 \
ROCKETMQ_CLUSTER_SEEDS=127.0.0.1:5680 \
ROCKETMQ_AMQP_PORT=5673 \
ROCKETMQ_AMQPS_PORT=5676 \
ROCKETMQ_MGMT_PORT=15673 \
ROCKETMQ_DATA_DIR=data/node2 \
RUST_LOG=info \
./target/debug/rocketmq > logs/node2.log 2>&1 &

echo -e "${GREEN}Launching Node 3 (Ports: AMQP 5674, AMQPS 5677, Mgmt 15674)...${NC}"
ROCKETMQ_NODE_ID=3 \
ROCKETMQ_BIND_HOST=127.0.0.1 \
ROCKETMQ_CLUSTER_ADDR=127.0.0.1:5682 \
ROCKETMQ_CLUSTER_SEEDS=127.0.0.1:5680 \
ROCKETMQ_AMQP_PORT=5674 \
ROCKETMQ_AMQPS_PORT=5677 \
ROCKETMQ_MGMT_PORT=15674 \
ROCKETMQ_DATA_DIR=data/node3 \
RUST_LOG=info \
./target/debug/rocketmq > logs/node3.log 2>&1 &

# Wait for Node 1 to be fully up and running before connecting
echo -e "${YELLOW}Waiting for Broker Node 1 to bind AMQP port 5672...${NC}"
while ! nc -z 127.0.0.1 5672; do   
  sleep 0.5
done
echo -e "${GREEN}Broker Node 1 is online!${NC}"

# ── Launching NestJS Client Microservices ─────────────────────────────────

echo -e "${GREEN}Launching 8 Clustered NestJS Client Microservices...${NC}"
AMQP_URL=amqp://guest:guest@127.0.0.1:5672/ \
PORT=3001 \
node client/dist/main.js > logs/client.log 2>&1 &

# Wait for NestJS to set up the topology
sleep 2

# ── Launching NextJS Management Dashboard ──────────────────────────────────

echo -e "${GREEN}Launching NextJS Management Dashboard (Port 3000)...${NC}"
PORT=3000 \
HOSTNAME=0.0.0.0 \
npm --prefix managment run dev > logs/management.log 2>&1 &

echo -e "${CYAN}=======================================================${NC}"
echo -e "${GREEN}🚀 ALL SERVICES ARE FULLY OPERATIONAL IN THE CLUSTER!${NC}"
echo -e "   - Node 1: AMQP 127.0.0.1:5672 | Mgmt 127.0.0.1:15672"
echo -e "   - Node 2: AMQP 127.0.0.1:5673 | Mgmt 127.0.0.1:15673"
echo -e "   - Node 3: AMQP 127.0.0.1:5674 | Mgmt 127.0.0.1:15674"
echo -e "   - Management Web UI: http://localhost:3000"
echo -e "   - Log files saved to: ./logs/"
echo -e "${CYAN}=======================================================${NC}"
echo -e "${YELLOW}Tailing NestJS live cluster test telemetry... (Press Ctrl+C to stop all)${NC}\n"

# Tail NestJS client events to show continuous active AMQP queue actions, bans, ticks
tail -f logs/client.log
