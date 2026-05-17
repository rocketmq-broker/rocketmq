/**
 * Payment Service — listens to "orders" queue, processes payments,
 * publishes results to "notifications" queue.
 *
 * GET /health → 200 OK
 *
 * Run: deno run --allow-net client/payment_service.ts
 */

import { Logger } from "./logger.ts";
import { connect } from "./rocketmq.ts";

const log = new Logger("payment-service");

const HTTP_PORT = 3002;
const BROKER_HOST = "localhost";
const BROKER_PORT = 8080;

const encoder = new TextEncoder();

interface OrderMessage {
  orderId: string;
}

async function main() {
  log.info("connecting to broker...");
  const conn = await connect(BROKER_HOST, BROKER_PORT);
  const channel = await conn.createChannel();

  await channel.assertQueue("orders");
  await channel.assertQueue("notifications");

  log.info("listening on 'orders' queue");

  // Handle delivered messages
  await channel.consume<OrderMessage>("orders", async (order, headers, msg) => {
    const traceId = headers.trace_id || "unknown";
    log.info(`processing order ${order.orderId} (trace: ${traceId})`);

    // Simulate payment processing
    const result = {
      orderId: order.orderId,
      status: "paid",
      processedAt: new Date().toISOString(),
    };

    // Publish notification
    await channel.sendToQueue(
      "notifications",
      encoder.encode(JSON.stringify(result)),
      { headers: { trace_id: traceId } },
    );
    log.info(`payment processed, notification sent (trace: ${traceId})`);

    // ACK the message
    await channel.ack(msg);
  });

  // HTTP server (health check)
  Deno.serve({ port: HTTP_PORT }, (req) => {
    const url = new URL(req.url);
    if (url.pathname === "/health") return new Response("ok");
    return new Response("not found", { status: 404 });
  });

  log.info(`HTTP server on http://localhost:${HTTP_PORT}`);
}

main();
