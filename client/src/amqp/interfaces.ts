/** Shared message interfaces for the order processing pipeline. */

export interface OrderItem {
  productId: string;
  quantity: number;
  price: number;
}

export interface OrderMessage {
  orderId: string;
  customerId: string;
  items: OrderItem[];
  total: number;
  shippingAddress: string;
  status: string;
  createdAt: string;
}

export interface PaymentRequest {
  orderId: string;
  customerId: string;
  amount: number;
  currency: string;
  items: OrderItem[];
  shippingAddress: string;
  validatedAt: string;
}

export interface InventoryRequest {
  orderId: string;
  customerId: string;
  transactionId: string;
  items: OrderItem[];
  amount: number;
  shippingAddress: string;
  paidAt: string;
}

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
