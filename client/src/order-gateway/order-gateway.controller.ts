import {
  Body,
  Controller,
  Get,
  HttpCode,
  Inject,
  Logger,
  Post,
} from '@nestjs/common';
import * as amqp from 'amqplib';
import { AMQP_CHANNEL, EXCHANGE_ORDERS, RK_ORDER_CREATED } from '../amqp';

interface CreateOrderDto {
  customerId: string;
  items: { productId: string; quantity: number; price: number }[];
  shippingAddress: string;
}

@Controller('orders')
export class OrderGatewayController {
  private readonly logger = new Logger('OrderGateway');
  private orderCounter = 0;

  constructor(@Inject(AMQP_CHANNEL) private readonly channel: amqp.Channel) {}

  @Post()
  @HttpCode(202)
  createOrder(@Body() dto: CreateOrderDto) {
    const orderId = `ORD-${Date.now()}-${++this.orderCounter}`;
    const total = dto.items.reduce((sum, i) => sum + i.price * i.quantity, 0);

    const order = {
      orderId,
      customerId: dto.customerId,
      items: dto.items,
      total,
      shippingAddress: dto.shippingAddress,
      status: 'CREATED',
      createdAt: new Date().toISOString(),
    };

    this.channel.publish(
      EXCHANGE_ORDERS,
      RK_ORDER_CREATED,
      Buffer.from(JSON.stringify(order)),
      { persistent: true, contentType: 'application/json' },
    );

    this.logger.log(
      `📦 Order ${orderId} published → ${EXCHANGE_ORDERS}/${RK_ORDER_CREATED} (total: $${total.toFixed(2)})`,
    );

    return {
      orderId,
      status: 'ACCEPTED',
      message: 'Order queued for processing',
    };
  }

  @Post('batch')
  @HttpCode(202)
  createBatch(@Body() orders: CreateOrderDto[]) {
    const results: { orderId: string; status: string; message: string }[] = [];
    for (const dto of orders) {
      const result = this.createOrder(dto);
      results.push(result);
    }
    this.logger.log(`📦 Batch of ${orders.length} orders published`);
    return { count: results.length, orders: results };
  }

  @Get('health')
  health() {
    return {
      service: 'order-gateway',
      status: 'UP',
      timestamp: new Date().toISOString(),
    };
  }
}
