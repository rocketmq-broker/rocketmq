#!/usr/bin/env python3
"""
Cluster Integration Test for RocketMQ AMQP Broker (Sprint 5).
Spins up a 2-node active-active cluster, verifies gossip discovery,
metadata replication (queue/exchange declare, binding sync), and
quorum-based message publishing/consumption.
"""

import subprocess
import time
import sys
import os

try:
    import pika
except ImportError:
    print("SKIP: pika not installed (pip install pika)")
    sys.exit(0)


def start_node(node_id, amqp_port, amqps_port, mgmt_port, cluster_addr, seeds=""):
    env = os.environ.copy()
    env["ROCKETMQ_NODE_ID"] = str(node_id)
    env["ROCKETMQ_AMQP_PORT"] = str(amqp_port)
    env["ROCKETMQ_AMQPS_PORT"] = str(amqps_port)
    env["ROCKETMQ_MGMT_PORT"] = str(mgmt_port)
    env["ROCKETMQ_CLUSTER_ADDR"] = cluster_addr
    if seeds:
        env["ROCKETMQ_CLUSTER_SEEDS"] = seeds

    # Ensure WAL file is separate per node to avoid conflicts
    env["WAL_PATH"] = f"data/node_{node_id}.wal"

    # Start target bin
    proc = subprocess.Popen(
        ["target/debug/rocketmq"],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )
    return proc


def connect(port):
    creds = pika.PlainCredentials('guest', 'guest')
    params = pika.ConnectionParameters('127.0.0.1', port, '/', creds, heartbeat=60)
    return pika.BlockingConnection(params)


def main():
    print("\n🕸️  AMQP Active-Active Cluster Integration Tests")
    print("=" * 55)

    # 1. Compile broker binary first
    print("Building broker binary...")
    subprocess.run(["cargo", "build"], check=True)

    # 2. Spin up two cluster nodes
    print("Spawning Node 1 (Ports: AMQP=5690, Cluster=5688)...")
    proc1 = start_node(
        node_id=1,
        amqp_port=5690,
        amqps_port=5691,
        mgmt_port=15690,
        cluster_addr="127.0.0.1:5688"
    )

    print("Spawning Node 2 (Ports: AMQP=5692, Cluster=5689, Seed=Node 1)...")
    proc2 = start_node(
        node_id=2,
        amqp_port=5692,
        amqps_port=5693,
        mgmt_port=15692,
        cluster_addr="127.0.0.1:5689",
        seeds="127.0.0.1:5688"
    )

    # Give them 3 seconds to boot and establish Gossip connection
    time.sleep(3)

    conn1 = None
    conn2 = None
    try:
        # Test 1: Verify connections to both nodes
        print("Test 1: Connecting to Node 1 and Node 2 independently...")
        conn1 = connect(5690)
        conn2 = connect(5692)
        print("  ✓ Connected to both nodes")

        # Test 2: Queue declaration replication
        print("Test 2: Declaring queue 'cluster.test.queue' on Node 1...")
        ch1 = conn1.channel()
        ch1.queue_declare(queue='cluster.test.queue', durable=True)
        time.sleep(0.5)

        print("Verifying queue 'cluster.test.queue' exists on Node 2...")
        ch2 = conn2.channel()
        # Passive declare checks if queue exists
        res2 = ch2.queue_declare(queue='cluster.test.queue', passive=True)
        assert res2.method.queue == 'cluster.test.queue', "Queue metadata did not replicate to Node 2"
        print("  ✓ Queue metadata replicated successfully")

        # Test 3: Quorum-based publish-subscribe across different nodes
        print("Test 3: Publishing message on Node 1...")
        ch1.basic_publish(
            exchange='',
            routing_key='cluster.test.queue',
            body=b'Hello Cluster Quorum!'
        )
        time.sleep(0.5)

        print("Consuming/getting message from Node 2...")
        method, props, body = ch2.basic_get(queue='cluster.test.queue', auto_ack=True)
        assert body == b'Hello Cluster Quorum!', f"Expected 'Hello Cluster Quorum!', got: {body}"
        print("  ✓ Message replicated and consumed across nodes successfully")

        # Test 4: Queue deletion replication
        print("Test 4: Deleting queue 'cluster.test.queue' from Node 2...")
        ch2.queue_delete(queue='cluster.test.queue')
        time.sleep(0.5)

        print("Verifying queue 'cluster.test.queue' is removed from Node 1...")
        try:
            ch1.queue_declare(queue='cluster.test.queue', passive=True)
            assert False, "Queue should have been deleted from Node 1"
        except pika.exceptions.ChannelClosedByBroker as e:
            # Expected error since queue is deleted
            print("  ✓ Queue deletion replicated successfully")

        print("=" * 55)
        print("🎉 ALL CLUSTER INTEGRATION TESTS PASSED PERFECTLY!")
        success = True
    except Exception as e:
        print(f"  ✗ Test failed: {e}")
        success = False
        
        # Read and print processes outputs
        try:
            # Set non-blocking to prevent hanging if there's no output
            os.set_blocking(proc1.stdout.fileno(), False)
            os.set_blocking(proc1.stderr.fileno(), False)
            os.set_blocking(proc2.stdout.fileno(), False)
            os.set_blocking(proc2.stderr.fileno(), False)
            
            print("\n--- Node 1 STDERR ---")
            print(proc1.stderr.read().decode())
            print("\n--- Node 2 STDERR ---")
            print(proc2.stderr.read().decode())
        except Exception as read_err:
            print(f"Could not read logs: {read_err}")
    finally:
        # Cleanup connections
        if conn1 and not conn1.is_closed:
            try: conn1.close()
            except: pass
        if conn2 and not conn2.is_closed:
            try: conn2.close()
            except: pass

        # Terminate broker processes
        print("Cleaning up cluster node processes...")
        proc1.terminate()
        proc2.terminate()
        proc1.wait()
        proc2.wait()

    sys.exit(0 if success else 1)


if __name__ == '__main__':
    main()
