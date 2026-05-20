// ─── AMQP Topology ─────────────────────────────────────
// All queue, exchange, and routing key names for the order pipeline.

export const AMQP_URL =
  process.env.AMQP_URL || 'amqp://guest:guest@127.0.0.1:5672/';

// Exchanges
export const EXCHANGE_ORDERS = 'orders.exchange';
export const EXCHANGE_PAYMENTS = 'payments.exchange';
export const EXCHANGE_INVENTORY = 'inventory.exchange';
export const EXCHANGE_NOTIFICATIONS = 'notifications.exchange';
export const EXCHANGE_DLX = 'dlx.exchange';

// Queues
export const QUEUE_ORDERS_CREATED = 'orders.created';
export const QUEUE_ORDERS_VALIDATED = 'orders.validated';
export const QUEUE_PAYMENTS_PROCESS = 'payments.process';
export const QUEUE_PAYMENTS_RESULT = 'payments.result';
export const QUEUE_INVENTORY_RESERVE = 'inventory.reserve';
export const QUEUE_INVENTORY_RESULT = 'inventory.result';
export const QUEUE_NOTIFICATIONS_SEND = 'notifications.send';
export const QUEUE_DLQ = 'dead-letter-queue';

// Routing keys
export const RK_ORDER_CREATED = 'order.created';
export const RK_ORDER_VALIDATED = 'order.validated';
export const RK_PAYMENT_PROCESS = 'payment.process';
export const RK_PAYMENT_SUCCESS = 'payment.success';
export const RK_PAYMENT_FAILED = 'payment.failed';
export const RK_INVENTORY_RESERVE = 'inventory.reserve';
export const RK_INVENTORY_OK = 'inventory.ok';
export const RK_NOTIFY_SEND = 'notify.send';
