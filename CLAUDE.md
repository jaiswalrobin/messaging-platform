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

## WS protocol (client ↔ gateway)

- Client→server: `message {conversationId, content, clientMessageId}`, `fetch_messages {conversationId, limit}`, `mark_read {conversationId, lastReadMessageId}`, `ack_delivered {conversationId, messageId}`.
- Server→client: `message_sent`, `message_received`, `message_delivered {messageId, conversationId, clientMessageId, deliveredCount}` (to sender), `message_read {conversationId, readerId, lastReadMessageId, readAt}`, `messages_history`, `error {code: FORBIDDEN|PERSIST_FAILED, ...}`.
- Auth: JWT in `?token=` query param, verified in `handleConnection` (close 1008 on failure); `WsAuthGuard` checks `client.user` exists.
- Every message handler: **membership check first** (`participantCache.isMember`) — IDOR prevention.
- Read receipts: `mark_read` → gateway persists via api internal endpoint (monotonic watermark on `conversation_participants.last_read_message_id`, advanced by comparing v1 timeuuid timestamps) → fans out `message_read` to other participants. Persistence is best-effort; stale receipts (`advanced: false`) never fan out. Watermarks are exposed per participant on `GET /conversations` so clients can hydrate blue ticks for messages read while they were offline; live `message_read` keeps them current.
- Delivery receipts: **ack-driven, app-level** — "delivered" means the recipient's app processed the frame, not merely that its socket was open (a throttled/DevTools-offline browser keeps its socket OPEN, so fan-out is NOT delivery). The recipient's client sends `ack_delivered {conversationId, messageId}` after processing `message_received`; the gateway routes `message_delivered {messageId, conversationId, clientMessageId, deliveredCount}` to the original sender from a short-lived in-memory tracker (messageId → sender + clientMessageId, 60s TTL, per-recipient dedup). Sender-side tick upgrade is monotonic (never downgrades read). No queue for offline senders yet — history re-fetch re-derives 'delivered'.

## Dev workflow

```bash
docker compose up -d          # postgres 15 + redis 7 + cassandra 4.1
pnpm dev                      # runs api + chat-gateway via turbo
pnpm --filter api build       # type-check one service (nest build)
```

- api runs `synchronize: NODE_ENV !== 'production'` — schema auto-syncs in dev, needs real migrations in prod. Gateway is always `synchronize: false`.
- Full architecture write-up lives in the (uncommitted) `ARCHITECTURE.md`.

## Conventions

- DTOs: class-validator, validated with global `ValidationPipe({ whitelist: true, transform: true })` on both services.
- Cross-service failure of internal calls: log a warning, never fail the caller's request.
- Message content cap: `MAX_MESSAGE_LENGTH = 4000` (shared-types); FE enforces it client-side too.
