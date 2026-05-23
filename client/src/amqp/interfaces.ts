/**
 * Copyright (c) 2026 Edilson Pateguana
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Author: Edilson Pateguana
 * Year: 2026
 * File: interfaces.ts
 * Description: NestJS AMQP client integration and message broker gateway.
 */

/**
 * Structural definition for order item.
 *
 * Defines schemas, types, or services for order item inside the NestJS client.
 */
export interface OrderItem {
  productId: string;
  quantity: number;
  price: number;
}

/**
 * Structural definition for order message.
 *
 * Defines schemas, types, or services for order message inside the NestJS client.
 */
export interface OrderMessage {
  orderId: string;
  customerId: string;
  items: OrderItem[];
  total: number;
  shippingAddress: string;
  status: string;
  createdAt: string;
}

/**
 * Structural definition for payment request.
 *
 * Defines schemas, types, or services for payment request inside the NestJS client.
 */
export interface PaymentRequest {
  orderId: string;
  customerId: string;
  amount: number;
  currency: string;
  items: OrderItem[];
  shippingAddress: string;
  validatedAt: string;
}

/**
 * Structural definition for inventory request.
 *
 * Defines schemas, types, or services for inventory request inside the NestJS client.
 */
export interface InventoryRequest {
  orderId: string;
  customerId: string;
  transactionId: string;
  items: OrderItem[];
  amount: number;
  shippingAddress: string;
  paidAt: string;
}

/**
 * Structural definition for notification event.
 *
 * Defines schemas, types, or services for notification event inside the NestJS client.
 */
export interface NotificationEvent {
  type: string;
  orderId: string;
  customerId: string;
  transactionId?: string;
  items?: { productId: string; quantity: number; price?: number }[];
  amount?: number;
  shippingAddress?: string;
  reason?: string;
  errors?: string[];
  timestamp?: string;
  completedAt?: string;
}
