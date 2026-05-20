import { Inject, Injectable, Logger, OnModuleInit } from '@nestjs/common';
import * as amqp from 'amqplib';
import {
  AMQP_CHANNEL,
  EXCHANGE_PAYMENTS,
  QUEUE_ORDERS_CREATED,
  RK_PAYMENT_PROCESS,
} from '../amqp';
import { OrderMessage, PaymentRequest } from '../amqp/interfaces';

@Injectable()
export class OrderService implements OnModuleInit {
  private readonly logger = new Logger('OrderService');

  constructor(@Inject(AMQP_CHANNEL) private readonly channel: amqp.Channel) {}

  async onModuleInit() {
    this.logger.log(`👂 Consuming from ${QUEUE_ORDERS_CREATED}`);

    await this.channel.consume(QUEUE_ORDERS_CREATED, (msg) => {
      if (!msg) return;
      try {
        this.handleOrder(msg);
      } catch (err: unknown) {
        const message = err instanceof Error ? err.message : String(err);
        this.logger.error(`💥 Failed to process order: ${message}`);
        this.channel.nack(msg, false, false);
      }
    });
  }

  private handleOrder(msg: amqp.ConsumeMessage): void {
    const order = JSON.parse(msg.content.toString()) as OrderMessage;
    this.logger.log(
      `📋 Validating order ${order.orderId} ($${order.total?.toFixed(2)})`,
    );

    // ── Business validation ───────────────────
    const errors: string[] = [];
    if (!order.customerId) errors.push('missing customerId');
    if (!order.items?.length) errors.push('empty items');
    if (order.total <= 0) errors.push('invalid total');
    if (order.total > 10000) errors.push('exceeds max order value ($10,000)');

    if (errors.length > 0) {
      this.logger.warn(
        `❌ Order ${order.orderId} validation failed: ${errors.join(', ')}`,
      );
      this.channel.nack(msg, false, false);
      return;
    }

    // ── Validation passed → forward to payment ─
    const paymentRequest: PaymentRequest = {
      orderId: order.orderId,
      customerId: order.customerId,
      amount: order.total,
      currency: 'USD',
      items: order.items,
      shippingAddress: order.shippingAddress,
      validatedAt: new Date().toISOString(),
    };

    this.channel.publish(
      EXCHANGE_PAYMENTS,
      RK_PAYMENT_PROCESS,
      Buffer.from(JSON.stringify(paymentRequest)),
      { persistent: true, contentType: 'application/json' },
    );

    this.logger.log(
      `✅ Order ${order.orderId} validated → ${EXCHANGE_PAYMENTS}/${RK_PAYMENT_PROCESS}`,
    );
    this.channel.ack(msg);
  }
}
