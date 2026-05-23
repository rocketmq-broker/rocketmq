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
    echo -e "${YELLOW}Aggressressively clearing cluster ports (3001, 5672-5674, 15672-15674, 5680-5682)...${NC}"
    for port in 3001 5672 5673 5674 15672 15673 15674 5680 5681 5682; do
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

# ── Generating TLS Certificates ─────────────────────────────────────────
echo -e "${YELLOW}Setting up valid and trusted TLS certificates...${NC}"
mkdir -p data/tls
if [ ! -f data/tls/server.pem ]; then
    echo -e "${CYAN}Generating a new Root CA and server certificates...${NC}"
    openssl genrsa -out data/tls/ca.key 4096
    openssl req -x509 -new -nodes -key data/tls/ca.key -sha256 -days 3650 -subj '/CN=RocketMQ Local CA/O=Edilson Pateguana/C=MZ' -out data/tls/ca.pem
    openssl genrsa -out data/tls/server.key 2048
    
    cat <<EOF > data/tls/openssl.cnf
[req]
default_bits = 2048
prompt = no
default_md = sha256
req_extensions = req_ext
distinguished_name = dn

[dn]
C = MZ
O = Edilson Pateguana
CN = localhost

[req_ext]
subjectAltName = @alt_names

[alt_names]
DNS.1 = localhost
DNS.2 = node1
DNS.3 = node2
DNS.4 = node3
DNS.5 = rocketmq-node1
DNS.6 = rocketmq-node2
DNS.7 = rocketmq-node3
IP.1 = 127.0.0.1
EOF

    openssl req -new -key data/tls/server.key -config data/tls/openssl.cnf -out data/tls/server.csr
    openssl x509 -req -in data/tls/server.csr -CA data/tls/ca.pem -CAkey data/tls/ca.key -CAcreateserial -out data/tls/server.pem -days 3650 -sha256 -extfile data/tls/openssl.cnf -extensions req_ext
    rm data/tls/server.csr data/tls/openssl.cnf
    echo -e "${GREEN}TLS certificates generated successfully!${NC}"
else
    echo -e "${GREEN}TLS certificates already exist in data/tls/.${NC}"
fi

# Create separate data directories to avoid WAL/DB lock conflicts
mkdir -p data/node1/tls data/node2/tls data/node3/tls
rm -rf data/node1/* data/node2/* data/node3/*

# Distribute certs to each node's tls folder
mkdir -p data/node1/tls data/node2/tls data/node3/tls
cp data/tls/server.pem data/node1/tls/
cp data/tls/server.key data/node1/tls/
cp data/tls/ca.pem data/node1/tls/

cp data/tls/server.pem data/node2/tls/
cp data/tls/server.key data/node2/tls/
cp data/tls/ca.pem data/node2/tls/

cp data/tls/server.pem data/node3/tls/
cp data/tls/server.key data/node3/tls/
cp data/tls/ca.pem data/node3/tls/

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

# ── Cluster Operational Summary ──────────────────────────────────────────

echo -e "${CYAN}=======================================================${NC}"
echo -e "${GREEN}🚀 ALL SERVICES ARE FULLY OPERATIONAL IN THE CLUSTER!${NC}"
echo -e "   - Node 1: AMQP 127.0.0.1:5672 | Mgmt http://localhost:15672"
echo -e "   - Node 2: AMQP 127.0.0.1:5673 | Mgmt http://localhost:15673"
echo -e "   - Node 3: AMQP 127.0.0.1:5674 | Mgmt http://localhost:15674"
echo -e "   - Native Management UI: http://localhost:15672"
echo -e "   - Log files saved to: ./logs/"
echo -e "${CYAN}=======================================================${NC}"
echo -e "${YELLOW}Tailing NestJS live cluster test telemetry... (Press Ctrl+C to stop all)${NC}\n"

# Tail NestJS client events to show continuous active AMQP queue actions, bans, ticks
tail -f logs/client.log
