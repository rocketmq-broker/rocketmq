import { Global, Logger, Module, OnModuleDestroy } from '@nestjs/common';
import * as amqp from 'amqplib';
import {
  AMQP_URL,
  EXCHANGE_DLX,
  EXCHANGE_INVENTORY,
  EXCHANGE_NOTIFICATIONS,
  EXCHANGE_ORDERS,
  EXCHANGE_PAYMENTS,
  QUEUE_DLQ,
  QUEUE_INVENTORY_RESERVE,
  QUEUE_INVENTORY_RESULT,
  QUEUE_NOTIFICATIONS_SEND,
  QUEUE_ORDERS_CREATED,
  QUEUE_ORDERS_VALIDATED,
  QUEUE_PAYMENTS_PROCESS,
  QUEUE_PAYMENTS_RESULT,
  RK_INVENTORY_OK,
  RK_INVENTORY_RESERVE,
  RK_ORDER_CREATED,
  RK_ORDER_VALIDATED,
  RK_PAYMENT_FAILED,
  RK_PAYMENT_PROCESS,
  RK_PAYMENT_SUCCESS,
} from './constants';

export const AMQP_CONNECTION = 'AMQP_CONNECTION';
export const AMQP_CHANNEL = 'AMQP_CHANNEL';

@Global()
@Module({
  providers: [
    {
      provide: AMQP_CONNECTION,
      useFactory: async () => {
        const logger = new Logger('AmqpModule');
        const conn = await amqp.connect(AMQP_URL);
        logger.log(`Connected to AMQP at ${AMQP_URL}`);
        return conn;
      },
    },
    {
      provide: AMQP_CHANNEL,
      useFactory: async (conn: amqp.ChannelModel) => {
        const logger = new Logger('AmqpModule');
        const ch = await conn.createChannel();
        await ch.prefetch(10);

        // ── Dead Letter Exchange ──────────────────────
        await ch.assertExchange(EXCHANGE_DLX, 'direct', { durable: true });
        await ch.assertQueue(QUEUE_DLQ, { durable: true });
        await ch.bindQueue(QUEUE_DLQ, EXCHANGE_DLX, 'dead');

        // ── Orders ───────────────────────────────────
        await ch.assertExchange(EXCHANGE_ORDERS, 'direct', { durable: true });
        await ch.assertQueue(QUEUE_ORDERS_CREATED, {
          durable: true,
          arguments: {
            'x-dead-letter-exchange': EXCHANGE_DLX,
            'x-dead-letter-routing-key': 'dead',
          },
        });
        await ch.assertQueue(QUEUE_ORDERS_VALIDATED, {
          durable: true,
          arguments: {
            'x-dead-letter-exchange': EXCHANGE_DLX,
            'x-dead-letter-routing-key': 'dead',
          },
        });
        await ch.bindQueue(
          QUEUE_ORDERS_CREATED,
          EXCHANGE_ORDERS,
          RK_ORDER_CREATED,
        );
        await ch.bindQueue(
          QUEUE_ORDERS_VALIDATED,
          EXCHANGE_ORDERS,
          RK_ORDER_VALIDATED,
        );

        // ── Payments ─────────────────────────────────
        await ch.assertExchange(EXCHANGE_PAYMENTS, 'direct', { durable: true });
        await ch.assertQueue(QUEUE_PAYMENTS_PROCESS, {
          durable: true,
          arguments: {
            'x-dead-letter-exchange': EXCHANGE_DLX,
            'x-dead-letter-routing-key': 'dead',
          },
        });
        await ch.assertQueue(QUEUE_PAYMENTS_RESULT, {
          durable: true,
          arguments: {
            'x-dead-letter-exchange': EXCHANGE_DLX,
            'x-dead-letter-routing-key': 'dead',
          },
        });
        await ch.bindQueue(
          QUEUE_PAYMENTS_PROCESS,
          EXCHANGE_PAYMENTS,
          RK_PAYMENT_PROCESS,
        );
        await ch.bindQueue(
          QUEUE_PAYMENTS_RESULT,
          EXCHANGE_PAYMENTS,
          RK_PAYMENT_SUCCESS,
        );
        await ch.bindQueue(
          QUEUE_PAYMENTS_RESULT,
          EXCHANGE_PAYMENTS,
          RK_PAYMENT_FAILED,
        );

        // ── Inventory ────────────────────────────────
        await ch.assertExchange(EXCHANGE_INVENTORY, 'direct', {
          durable: true,
        });
        await ch.assertQueue(QUEUE_INVENTORY_RESERVE, {
          durable: true,
          arguments: {
            'x-dead-letter-exchange': EXCHANGE_DLX,
            'x-dead-letter-routing-key': 'dead',
          },
        });
        await ch.assertQueue(QUEUE_INVENTORY_RESULT, {
          durable: true,
          arguments: {
            'x-dead-letter-exchange': EXCHANGE_DLX,
            'x-dead-letter-routing-key': 'dead',
          },
        });
        await ch.bindQueue(
          QUEUE_INVENTORY_RESERVE,
          EXCHANGE_INVENTORY,
          RK_INVENTORY_RESERVE,
        );
        await ch.bindQueue(
          QUEUE_INVENTORY_RESULT,
          EXCHANGE_INVENTORY,
          RK_INVENTORY_OK,
        );

        // ── Notifications ────────────────────────────
        await ch.assertExchange(EXCHANGE_NOTIFICATIONS, 'fanout', {
          durable: true,
        });
        await ch.assertQueue(QUEUE_NOTIFICATIONS_SEND, {
          durable: true,
          arguments: {
            'x-dead-letter-exchange': EXCHANGE_DLX,
            'x-dead-letter-routing-key': 'dead',
          },
        });
        await ch.bindQueue(
          QUEUE_NOTIFICATIONS_SEND,
          EXCHANGE_NOTIFICATIONS,
          '',
        );

        logger.log('AMQP topology declared (5 exchanges, 9 queues, DLX)');
        return ch;
      },
      inject: [AMQP_CONNECTION],
    },
  ],
  exports: [AMQP_CONNECTION, AMQP_CHANNEL],
})
export class AmqpModule implements OnModuleDestroy {
  constructor() {}

  async onModuleDestroy() {
    // Connection cleanup handled by NestJS lifecycle
  }
}
