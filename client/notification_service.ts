/**
 * Notification Service — listens to "notifications" queue, logs notifications.
 *
 * GET /health        → 200 OK
 * GET /notifications → list of received notifications
 *
 * Run: deno run --allow-net client/notification_service.ts
 */

import { Logger } from "./logger.ts";
import { connect } from "./rocketmq.ts";

const log = new Logger("notification-service");

// In-memory store for demo purposes
const notifications: unknown[] = [];

interface NotificationMessage {
  orderId: string;
  status: string;
}

async function main() {
  log.info("connecting to broker...");
  const conn = await connect(BROKER_HOST, BROKER_PORT);
  const channel = await conn.createChannel();

  await channel.assertQueue("notifications");
  log.info("listening on 'notifications' queue");

  await channel.consume<NotificationMessage>(
    "notifications",
    async (notification, headers, msg) => {
      const traceId = headers.trace_id || "unknown";
      notifications.push(notification);
      log.info(
        `received: order ${notification.orderId} → ${notification.status} (trace: ${traceId})`,
      );

      await channel.ack(msg);
    },
  );

  // HTTP server
  Deno.serve({ port: HTTP_PORT }, (req) => {
    const url = new URL(req.url);
    if (url.pathname === "/health") return new Response("ok");
    if (url.pathname === "/notifications") {
      return Response.json(notifications);
    }
    return new Response("not found", { status: 404 });
  });

  log.info(`HTTP server on http://localhost:${HTTP_PORT}`);
}

const HTTP_PORT = 3003;
const BROKER_HOST = "localhost";
const BROKER_PORT = 8080;

main();
