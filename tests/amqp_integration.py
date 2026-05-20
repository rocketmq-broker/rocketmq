#!/usr/bin/env python3
"""
AMQP 0-9-1 Integration Test — verifies our broker is wire-compatible
with standard pika client. Requires: pip install pika

Usage:
  1. Start the broker: cargo run
  2. Run this: python3 tests/amqp_integration.py

Tests:
  1. Connect with PLAIN auth (guest/guest)
  2. Channel open/close
  3. Queue declare (named + server-generated)
  4. Exchange declare + bind
  5. Basic.Publish + Basic.Get (synchronous pull)
  6. Basic.Consume + deliver
  7. Basic.Qos
  8. Queue delete + purge
  9. Confirm mode
  10. Tx Select/Commit/Rollback
"""

import sys
import time

try:
    import pika
except ImportError:
    print("SKIP: pika not installed (pip install pika)")
    sys.exit(0)


def test_connection():
    """Test 1: Basic connection with SASL PLAIN."""
    creds = pika.PlainCredentials('guest', 'guest')
    params = pika.ConnectionParameters('localhost', 5672, '/', creds)
    conn = pika.BlockingConnection(params)
    assert conn.is_open, "Connection should be open"
    conn.close()
    print("  ✓ Connection + auth")


def test_channel():
    """Test 2: Channel open/close."""
    conn = connect()
    ch = conn.channel()
    assert ch.is_open, "Channel should be open"
    ch.close()
    conn.close()
    print("  ✓ Channel open/close")


def test_queue_declare():
    """Test 3: Queue declare (named + server-generated)."""
    conn = connect()
    ch = conn.channel()

    # Named queue
    result = ch.queue_declare(queue='test.queue.1', durable=False)
    assert result.method.queue == 'test.queue.1'

    # Server-generated name
    result = ch.queue_declare(queue='', exclusive=True)
    assert result.method.queue.startswith('amq.gen-')

    ch.queue_delete(queue='test.queue.1')
    conn.close()
    print("  ✓ Queue declare (named + generated)")


def test_exchange_and_bind():
    """Test 4: Exchange declare + queue bind."""
    conn = connect()
    ch = conn.channel()

    ch.exchange_declare(exchange='test.exchange', exchange_type='direct')
    ch.queue_declare(queue='test.bind.queue')
    ch.queue_bind(queue='test.bind.queue', exchange='test.exchange', routing_key='test.key')
    ch.queue_unbind(queue='test.bind.queue', exchange='test.exchange', routing_key='test.key')
    ch.queue_delete(queue='test.bind.queue')
    ch.exchange_delete(exchange='test.exchange')

    conn.close()
    print("  ✓ Exchange declare + bind/unbind")


def test_publish_get():
    """Test 5: Basic.Publish + Basic.Get (synchronous pull)."""
    conn = connect()
    ch = conn.channel()

    ch.queue_declare(queue='test.get.queue')
    ch.basic_publish(exchange='', routing_key='test.get.queue', body=b'hello world')

    method, props, body = ch.basic_get(queue='test.get.queue', auto_ack=True)
    assert body == b'hello world', f"Expected 'hello world', got {body}"

    ch.queue_delete(queue='test.get.queue')
    conn.close()
    print("  ✓ Publish + Get")


def test_consume():
    """Test 6: Basic.Consume + deliver."""
    conn = connect()
    ch = conn.channel()
    ch.queue_declare(queue='test.consume.queue')

    received = []

    def on_message(ch, method, properties, body):
        received.append(body)
        ch.basic_ack(delivery_tag=method.delivery_tag)

    ch.basic_consume(queue='test.consume.queue', on_message_callback=on_message)
    ch.basic_publish(exchange='', routing_key='test.consume.queue', body=b'consumed!')

    # Process for up to 2 seconds
    start = time.time()
    while len(received) == 0 and time.time() - start < 2:
        conn.process_data_events(time_limit=0.1)

    assert len(received) == 1, f"Expected 1 message, got {len(received)}"
    assert received[0] == b'consumed!'

    ch.queue_delete(queue='test.consume.queue')
    conn.close()
    print("  ✓ Consume + deliver + ack")


def test_qos():
    """Test 7: Basic.Qos."""
    conn = connect()
    ch = conn.channel()
    ch.basic_qos(prefetch_count=10)
    conn.close()
    print("  ✓ QoS")


def test_queue_purge():
    """Test 8: Queue purge + delete."""
    conn = connect()
    ch = conn.channel()
    ch.queue_declare(queue='test.purge.queue')
    ch.basic_publish(exchange='', routing_key='test.purge.queue', body=b'msg1')
    ch.basic_publish(exchange='', routing_key='test.purge.queue', body=b'msg2')
    result = ch.queue_purge(queue='test.purge.queue')
    ch.queue_delete(queue='test.purge.queue')
    conn.close()
    print("  ✓ Queue purge + delete")


def test_confirm():
    """Test 9: Confirm mode."""
    conn = connect()
    ch = conn.channel()
    ch.confirm_delivery()
    ch.queue_declare(queue='test.confirm.queue')
    ch.basic_publish(exchange='', routing_key='test.confirm.queue', body=b'confirmed!')
    ch.queue_delete(queue='test.confirm.queue')
    conn.close()
    print("  ✓ Confirm mode")


def test_tx():
    """Test 10: Transactions."""
    conn = connect()
    ch = conn.channel()
    ch.queue_declare(queue='test.tx.queue')
    ch.tx_select()
    ch.basic_publish(exchange='', routing_key='test.tx.queue', body=b'tx-msg')
    ch.tx_commit()
    ch.tx_select()
    ch.basic_publish(exchange='', routing_key='test.tx.queue', body=b'rollback-msg')
    ch.tx_rollback()
    ch.queue_delete(queue='test.tx.queue')
    conn.close()
    print("  ✓ Transactions (select/commit/rollback)")


def connect():
    creds = pika.PlainCredentials('guest', 'guest')
    params = pika.ConnectionParameters('localhost', 5672, '/', creds)
    return pika.BlockingConnection(params)


def main():
    print("\n🐇 AMQP 0-9-1 Integration Tests")
    print("=" * 40)

    tests = [
        test_connection,
        test_channel,
        test_queue_declare,
        test_exchange_and_bind,
        test_publish_get,
        test_consume,
        test_qos,
        test_queue_purge,
        test_confirm,
        test_tx,
    ]

    passed = 0
    failed = 0
    for test in tests:
        try:
            test()
            passed += 1
        except Exception as e:
            print(f"  ✗ {test.__doc__}: {e}")
            failed += 1

    print("=" * 40)
    print(f"Results: {passed} passed, {failed} failed")
    sys.exit(1 if failed > 0 else 0)


if __name__ == '__main__':
    main()
