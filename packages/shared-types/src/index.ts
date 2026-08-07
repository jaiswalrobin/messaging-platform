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
type DbConfig = { synchronize: boolean };

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
export const getJwtSecret = (): string => {
  if (process.env.NODE_ENV === 'production' && !process.env.JWT_SECRET) {
    throw new Error('JWT_SECRET environment variable is required in production');
  }
  return process.env.JWT_SECRET ?? 'super-secret-key-for-local-dev-only';
};

export const JWT_EXPIRES_IN = '7d';

/** Cap on how many history messages a single request may fetch (protects memory). */
export const MAX_HISTORY_LIMIT = 100;

/** Max length of a message body accepted from clients. */
export const MAX_MESSAGE_LENGTH = 4000;

/** Shared secret for internal gateway endpoints (api → gateway invalidation). */
export const getInternalApiKey = (): string => {
  if (process.env.NODE_ENV === 'production' && !process.env.INTERNAL_API_KEY) {
    throw new Error('INTERNAL_API_KEY environment variable is required in production');
  }
  return process.env.INTERNAL_API_KEY ?? 'dev-internal-key';
};

/**
 * CORS config shared by both apps so the origin/credentials policy can't drift.
 * `CORS_ORIGIN` is a comma-separated allowlist; unset means allow all origins.
 */
export function getCorsConfig() {
  return {
    origin: process.env.CORS_ORIGIN ? process.env.CORS_ORIGIN.split(',') : true,
    credentials: true,
  };
}

// ── Kafka event payloads ──────────────────────────────────────────────────────

export interface KafkaMessageSentPayload {
  type: 'MESSAGE_SENT';
  messageId: string;
  conversationId: string;
  senderId: string;
  content: string;
  clientMessageId: string;
  createdAt: string;
  /**
   * Per-connection id of the originating WS socket. Lets the MSS consumer fan
   * out `message_received` to the sender's OTHER devices without re-sending to
   * the origin socket (which already got `message_sent` — a second frame would
   * duplicate the optimistic row if frames reorder). Optional: legacy/queued
   * events may lack it; the consumer then sends to all the sender's sockets
   * and relies on the FE's messageId dedupe.
   */
  senderSocketId?: string;
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

// ── SRP split: connection registry & delivery frames ─────────────────────────

export interface DeliveryFrame {
  /** WS event name the target node should emit ('message_received' | 'message_delivered' | 'message_read' | 'error'). */
  event: string;
  /** The frame payload exactly as the client expects it (same shape as today's direct sends). */
  data: unknown;
  /** Whose sockets the target node should write to. */
  userId: string;
  /** Origin-socket exclusion: the target node must skip the socket with this id. */
  excludeSocketId?: string;
}

/** Redis key prefix for the shared connection registry: registry:user:{userId} is a HASH {socketId → nodeId}. */
export const REGISTRY_KEY_PREFIX = 'registry:user:';
/** Redis channel prefix for delivery frames: delivery:{nodeId} — each gateway subscribes ONLY to its own. */
export const DELIVERY_CHANNEL_PREFIX = 'delivery:';
/** TTL for registry entries, refreshed by the gateway's 30s heartbeat (WhatsApp's 90s-TTL pattern). */
export const REGISTRY_TTL_SECONDS = 90;
