#!/usr/bin/env node
/**
 * AMQP 0-9-1 Integration Test — verifies wire compatibility with amqplib.
 * Requires: npm install amqplib
 *
 * Usage:
 *   1. Start the broker: cargo run
 *   2. Run this: node tests/amqp_integration.js
 */

const amqp = require('amqplib');

async function main() {
  console.log('\n🐇 AMQP 0-9-1 Integration Tests (amqplib)');
  console.log('='.repeat(45));

  let passed = 0;
  let failed = 0;

  async function run(name, fn) {
    try {
      await fn();
      console.log(`  ✓ ${name}`);
      passed++;
    } catch (e) {
      console.log(`  ✗ ${name}: ${e.message}`);
      failed++;
    }
  }

  // Test 1: Connect
  await run('Connection + auth', async () => {
    const conn = await amqp.connect('amqp://guest:guest@localhost:5672/');
    await conn.close();
  });

  // Test 2: Channel
  await run('Channel open/close', async () => {
    const conn = await amqp.connect('amqp://guest:guest@localhost:5672/');
    const ch = await conn.createChannel();
    await ch.close();
    await conn.close();
  });

  // Test 3: Queue declare
  await run('Queue declare', async () => {
    const conn = await amqp.connect('amqp://guest:guest@localhost:5672/');
    const ch = await conn.createChannel();
    const q = await ch.assertQueue('test.node.queue', { durable: false });
    if (q.queue !== 'test.node.queue') throw new Error('wrong queue name');
    await ch.deleteQueue('test.node.queue');
    await conn.close();
  });

  // Test 4: Publish + Get
  await run('Publish + Get', async () => {
    const conn = await amqp.connect('amqp://guest:guest@localhost:5672/');
    const ch = await conn.createChannel();
    await ch.assertQueue('test.node.get', { durable: false });
    ch.sendToQueue('test.node.get', Buffer.from('hello from node'));
    // Small delay for message to be queued
    await new Promise(r => setTimeout(r, 100));
    const msg = await ch.get('test.node.get', { noAck: true });
    if (!msg || msg.content.toString() !== 'hello from node') {
      throw new Error('message mismatch');
    }
    await ch.deleteQueue('test.node.get');
    await conn.close();
  });

  // Test 5: Consume
  await run('Consume + deliver', async () => {
    const conn = await amqp.connect('amqp://guest:guest@localhost:5672/');
    const ch = await conn.createChannel();
    await ch.assertQueue('test.node.consume', { durable: false });

    const received = [];
    await ch.consume('test.node.consume', (msg) => {
      received.push(msg.content.toString());
      ch.ack(msg);
    });

    ch.sendToQueue('test.node.consume', Buffer.from('consumed!'));
    await new Promise(r => setTimeout(r, 500));

    if (received.length !== 1 || received[0] !== 'consumed!') {
      throw new Error(`expected 1 msg, got ${received.length}`);
    }
    await ch.deleteQueue('test.node.consume');
    await conn.close();
  });

  // Test 6: QoS
  await run('QoS prefetch', async () => {
    const conn = await amqp.connect('amqp://guest:guest@localhost:5672/');
    const ch = await conn.createChannel();
    await ch.prefetch(10);
    await conn.close();
  });

  console.log('='.repeat(45));
  console.log(`Results: ${passed} passed, ${failed} failed`);
  process.exit(failed > 0 ? 1 : 0);
}

main().catch(e => {
  console.error('Fatal:', e.message);
  process.exit(1);
});
