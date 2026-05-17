/**
 * Order Service — HTTP server that accepts orders and publishes them to the broker.
 *
 * POST /orders  { "item": "...", "qty": N }  → publishes to "orders" queue
 * GET  /health                                → 200 OK
 *
 * Run: deno run --allow-net client/order_service.ts
 */

import { Logger } from "./logger.ts";
import { connect } from "./rocketmq.ts";

const log = new Logger("order-service");

const HTTP_PORT = 3001;
const BROKER_HOST = "localhost";
const BROKER_PORT = 8080;

const encoder = new TextEncoder();

async function main() {
  log.info("connecting to broker...");
  const conn = await connect(BROKER_HOST, BROKER_PORT);
  const channel = await conn.createChannel();

  // Ensure queue exists
  await channel.assertQueue("orders");
  log.info("queue 'orders' ready");

  // HTTP server
  Deno.serve({ port: HTTP_PORT }, async (req) => {
    const url = new URL(req.url);

    if (url.pathname === "/health") {
      return new Response("ok");
    }

    if (req.method === "POST" && url.pathname === "/orders") {
      try {
        const body = await req.json();
        const orderId = crypto.randomUUID();
        const message = JSON.stringify({
          orderId,
          ...body,
          createdAt: new Date().toISOString(),
        });

        const traceId = crypto.randomUUID();
        await channel.sendToQueue("orders", encoder.encode(message), {
          headers: { trace_id: traceId },
        });
        log.info(`published order ${orderId} (trace: ${traceId})`);

        return Response.json({ orderId, status: "published" }, { status: 201 });
      } catch (err) {
        log.error(`failed to process order: ${err}`);
        return Response.json({ error: String(err) }, { status: 400 });
      }
    }

    return new Response("not found", { status: 404 });
  });

  log.info(`HTTP server on http://localhost:${HTTP_PORT}`);
}

main();
