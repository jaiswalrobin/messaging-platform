# CLAUDE.md — project instructions

## ⚠️ Never commit architecture notes

`ARCHITECTURE.md`, `arch.md`, `arch2.md` (and any `arch*.md` variants) are **private design notes**. They are in `.gitignore` — never `git add` them, never include them in a commit, and never suggest committing them. If a commit accidentally stages them, unstage them before committing.

## What this is

A pnpm + Turborepo monorepo for a WhatsApp-style messaging platform: two NestJS services, one React frontend (`messaging-web`, separate repo at `~/Documents/messaging-web`).

| App | Port | Role |
|---|---|---|
| `apps/api` | 3000 | REST: auth, users, conversations (owns Postgres) |
| `apps/chat-gateway` | 8080 | HTTP + WebSocket hot path (owns Cassandra + Redis), raw `ws` via `WsAdapter` |
| `messaging-web` | 5173 | React FE (separate repo) |

## Schema ownership contract (the core rule)

Each store has exactly one owner; the other service never writes it directly:

- **PostgreSQL** (`chat_db`, dev: `admin`/`admin`) — **owned by `api`**. Gateway reads it only through minimal mirror entities with `synchronize: false`. Never let the gateway write Postgres — cross-service state goes through internal HTTP endpoints.
- **Cassandra 4.1** (`chat_ks.messages`) — **owned by `chat-gateway`** (append-only, partitioned by `conversation_id`, clustering `(created_at DESC, id DESC)`).
- **Redis 7** — gateway's best-effort cache (participant lists, 5-min TTL), fail-open: any Redis failure falls through to Postgres.
- **Internal endpoints** are guarded by `x-internal-key` (`getInternalApiKey()` from `@chat/shared-types`): api → gateway `POST /internal/participants/:id/invalidate`; gateway → api `POST /internal/conversations/:id/read` (read watermarks). Mirror these patterns for new cross-service calls.

## WS protocol (client ↔ gateway) — Kafka-first

- Client→server: `message {conversationId, content, clientMessageId}`, `fetch_messages {conversationId, limit?}` (DTO-capped 1–100), `mark_read {conversationId, lastReadMessageId}`, `message_delivered {conversationId, messageId}` (delivery ack — renamed from the old `ack_delivered`).
- Server→client: `message_sent {messageId, conversationId, clientMessageId, status:'sent'}` (to sender), `message_received {messageId, conversationId, senderId, content, createdAt}` (to recipients), `message_delivered {messageId, conversationId, recipientId, deliveredAt, status:'delivered'}` (to everyone but the acker — the FE matches its **own** messages by `messageId`, or `clientMessageId` in direct-fallback mode), `message_read {conversationId, readerId, lastReadMessageId, readAt, status:'read'}` (to everyone but the reader), `messages_history`, `error {code: FORBIDDEN|PERSIST_FAILED|FETCH_FAILED|INTERNAL, ...}` (`PERSIST_FAILED` is direct-fallback mode only).
- Auth: JWT in `?token=` query param, verified in `handleConnection` (close 1008 on failure); `WsAuthGuard` checks `client.user` exists.
- Every handler: **membership check first** (`participantCache.isMember`) — IDOR prevention.
- **Kafka-first pipeline**: `message` → gateway publishes `MESSAGE_SENT` to `chat-events` (idempotent producer) → **broker ACK ⇒ `message_sent` to sender (one tick)** → the gateway's own consumer (MSS role, `chat-gateway-group`) persists to Cassandra and fans out `message_received`. Broker down → direct persist-and-deliver fallback (sends work; receipts are dead).
- Delivery receipts (gray ✓✓): the recipient's client sends `message_delivered {conversationId, messageId}` only after actually processing the frame (app-level ack — a throttled/DevTools-offline browser never acks) → consumer persists to `message_receipts` (deduped read-before-write) → broadcasts `message_delivered` to everyone but the acker. Sender-side upgrade is monotonic (never downgrades read).
- Read receipts (blue ✓✓): `mark_read` → consumer persists to Cassandra `message_receipts` **and** advances the api's Postgres watermark best-effort (`POST /internal/conversations/:id/read`, monotonic CAS by v1 timeuuid compare) → broadcasts `message_read` only when the watermark advanced (`advanced: false` never fans out). Watermarks on `GET /conversations` hydrate blue ticks on reload.
- Exactly-once is **at-least-once + idempotency**: Cassandra PK-INSERTs are idempotent, the producer is idempotent, the FE dedupes `message_received` by messageId, and the consumer runs a bounded blocking retry (3×) that **never rethrows** (rethrow would drive kafkajs's batch retrier and crash the consumer). After 3 failures an event is skipped loudly (no DLQ yet).

## Dev workflow

```bash
docker compose up -d          # postgres 15 + redis 7 + cassandra 4.1 + kafka 3.9 (KRaft)
pnpm dev                      # runs api + chat-gateway via turbo
pnpm --filter api build       # type-check one service (nest build)
```

- api runs `synchronize: NODE_ENV !== 'production'` — schema auto-syncs in dev, needs real migrations in prod. Gateway is always `synchronize: false`.
- Full architecture write-up lives in the (uncommitted) `ARCHITECTURE.md`.

## Conventions

- DTOs: class-validator, validated with global `ValidationPipe({ whitelist: true, transform: true })` on both services.
- Cross-service failure of internal calls: log a warning, never fail the caller's request.
- Message content cap: `MAX_MESSAGE_LENGTH = 4000` (shared-types); FE enforces it client-side too.
