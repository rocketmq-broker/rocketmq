import { Module } from '@nestjs/common';
import { OrderGatewayController } from './order-gateway.controller';

@Module({
  controllers: [OrderGatewayController],
})
export class OrderGatewayModule {}
