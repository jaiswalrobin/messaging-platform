export interface SendMessagePayload {
  conversationId: string;
  content: string;
  clientMessageId: string;
}

export interface MarkReadPayload {
  conversationId: string;
  lastReadMessageId: string;
}

export interface MessageDeliveredPayload {
  conversationId: string;
  messageId: string;
  clientMessageId?: string;
}

export type MessageStatus = 'sending' | 'sent' | 'delivered' | 'read';

// ── Kafka event payloads ──────────────────────────────────────────────────────

export type KafkaEventType = 'MESSAGE_SENT' | 'MESSAGE_DELIVERED' | 'MESSAGE_READ';

export interface KafkaMessageSentPayload {
  type: 'MESSAGE_SENT';
  messageId: string;
  conversationId: string;
  senderId: string;
  content: string;
  clientMessageId: string;
  createdAt: string;
}

export interface KafkaMessageDeliveredPayload {
  type: 'MESSAGE_DELIVERED';
  messageId: string;
  conversationId: string;
  recipientId: string;
  deliveredAt: string;
}

export interface KafkaMessageReadPayload {
  type: 'MESSAGE_READ';
  conversationId: string;
  lastReadMessageId: string;
  readerId: string;
  readAt: string;
}

export type KafkaChatEvent =
  | KafkaMessageSentPayload
  | KafkaMessageDeliveredPayload
  | KafkaMessageReadPayload;

export interface UserAuthResponse {
  userId: string;
  token: string;
}