import amqp from "amqplib";

export const AMQP_URL = "amqp://guest:guest@localhost:5672";
export const QUEUE = "demo.messages";
export const EXCHANGE = "demo.exchange";
export const ROUTING_KEY = "demo.key";

/** Opens a connection + channel and declares the shared exchange/queue/binding. */
export async function setupChannel(): Promise<{
  conn: amqp.Connection;
  ch: amqp.Channel;
}> {
  const conn = await amqp.connect(AMQP_URL);
  const ch = await conn.createChannel();

  await ch.assertExchange(EXCHANGE, "direct", { durable: true });
  await ch.assertQueue(QUEUE, { durable: true });
  await ch.bindQueue(QUEUE, EXCHANGE, ROUTING_KEY);

  return { conn, ch };
}
