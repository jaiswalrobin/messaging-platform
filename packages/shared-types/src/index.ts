/**
 * Shared runtime configuration helpers used by both apps.
 * Centralizes secrets and magic numbers so api and chat-gateway can't drift.
 *
 * NOTE: this package is consumed as raw TypeScript (no build step, `main` points
 * at src/index.ts and Node runs it via native type-stripping), so everything must
 * live in this single file — relative imports need on-disk extensions that tsc
 * rejects. Keep config here rather than splitting into modules.
 */

/** Options controlling the shared TypeORM postgres connection config. */
export type DbConfig = { synchronize: boolean };

/**
 * TypeORM postgres connection config shared by both apps so the connection
 * options can't drift. `synchronize` is decided per-app at the call site.
 * No explicit return type on purpose: the inferred object is structurally
 * assignable to TypeOrmModuleOptions at the call site, and importing the type
 * here would fail tsc under pnpm's strict node_modules (this package has no
 * @nestjs/typeorm dependency).
 */
export function getTypeOrmConfig({ synchronize }: DbConfig) {
  return {
    type: 'postgres' as const,
    host: process.env.DB_HOST ?? 'localhost',
    port: parseInt(process.env.DB_PORT ?? '5432'),
    username: process.env.DB_USERNAME ?? 'admin',
    password: process.env.DB_PASSWORD ?? 'admin',
    database: process.env.DB_NAME ?? 'chat_db',
    autoLoadEntities: true,
    synchronize,
  };
}

/** JWT signing/verification secret. MUST be overridden via env in any real deployment. */
export const getJwtSecret = (): string =>
  process.env.JWT_SECRET ?? 'super-secret-key-for-local-dev-only';

export const JWT_EXPIRES_IN = '7d';

/** Cap on how many history messages a single request may fetch (protects memory). */
export const MAX_HISTORY_LIMIT = 100;

/** Max length of a message body accepted from clients. */
export const MAX_MESSAGE_LENGTH = 4000;

/** Shared secret for internal gateway endpoints (api → gateway invalidation). */
export const getInternalApiKey = (): string =>
  process.env.INTERNAL_API_KEY ?? 'dev-internal-key';

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
