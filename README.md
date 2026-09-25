# Messaging Platform

![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)
![NestJS](https://img.shields.io/badge/NestJS-E0234E?logo=nestjs&logoColor=white)
![Kafka](https://img.shields.io/badge/Kafka-231F20?logo=apache-kafka&logoColor=white)
![Cassandra](https://img.shields.io/badge/Cassandra-1287B1?logo=apache-cassandra&logoColor=white)

A WhatsApp/Discord-style real-time chat backend — a pnpm + Turborepo monorepo with three NestJS services.

| Service | Port | Protocol | Responsibility |
|---|---|---|---|
| `apps/api` | 3000 | HTTP (REST) | Auth, JWT, user search, conversation & group management |
| `apps/chat-gateway` | 8080 | HTTP + WebSocket | Producer + connection holder: WS accept/auth/membership, publish events to Kafka, sender tick, shared-registry registration, delivery-frame subscriber |
| `apps/mss` | 8081 | HTTP + Kafka consumer | Consumer (MSS role): consumes `chat-events`, owns Cassandra (messages + receipts), registry-lookup + targeted Redis pub/sub delivery, api watermark calls, DLQ |

**Data stores:** PostgreSQL 15 (metadata, owner: api) · Cassandra 4.1 (messages + receipts, owner: **mss**) · Redis 7 (participant cache + shared connection registry — written by gateway, read by mss) · Kafka 3.9 / KRaft (event log).

## Architecture

```
React client (:5173, separate repo)
   │  REST /api ───────────────────▶  api :3000 ──▶ PostgreSQL (users, conversations)
   │  REST /gateway ───────────────▶  chat-gateway :8080
   │  WS /ws (?token=<JWT>) ───────▶  chat-gateway :8080 (raw ws, Nest WsAdapter)
                                                              │ publish MESSAGE_SENT
                                                              ▼
                                                       Kafka (chat-events)
                                                              │ consume (mss-group)
                                                              ▼
                                                       mss :8081 ──▶ Cassandra (messages, receipts)
                                                              │
                        ┌───────────────────┬─────────────────┼───────────────┐
                        │ registry lookup   │ read watermarks │ failures      │
                        ▼                   ▼                 ▼               ▼
                 Redis (registry)   api :3000 internal   DLQ topic    Redis pub/sub
                 registry:user:*    (Postgres writes)    (3 retries)  delivery:{nodeId}
                                                                          │
                                                                          ▼
                                                              gateway ──▶ WS frame to recipient
```

Send path: client `message` event → gateway publishes to Kafka → broker ACK → `message_sent` (one tick) back to the sender → mss persists to Cassandra, resolves recipient nodes via the Redis registry, and routes `message_received` over targeted per-node Redis pub/sub. Delivery/read receipts flow back the same way (see [Testing the services](#testing-the-services) for a live walkthrough).

## Prerequisites

- Node.js ≥ 23.6
- [pnpm](https://pnpm.io/installation) 9
- A Docker engine with **Compose v2** (`docker compose` — e.g. [colima](https://github.com/abiosoft/colima) or Docker Desktop). The deprecated `docker-compose` v1 standalone is **not** supported (the compose file uses the modern Compose Specification).

## Quickstart

```bash
# 1. Install dependencies
pnpm install

# 2. Start the infrastructure (postgres, redis, cassandra, kafka)
docker compose up -d

# 3. Start all apps (api, chat-gateway, mss)
pnpm dev
```

Environment is optional in dev (defaults match `docker-compose.yml`); see [.env.example](.env.example) for all variables. **In any real deployment, set `JWT_SECRET`** — all services must share the same value.

## Full install & run, step by step (fresh machine)

**1. Prerequisites** — git, Node.js ≥ 23.6, pnpm 9 (`corepack enable` or `npm i -g pnpm`), and Docker with **Compose v2** (`docker compose`, not the deprecated `docker-compose`).

**2. Clone + install dependencies**
```bash
git clone https://github.com/jaiswalrobin/messaging-platform.git
cd messaging-platform
pnpm install
```

**3. Start the infrastructure** (Postgres, Redis, Cassandra, Kafka — all bound to `127.0.0.1`)
```bash
docker compose up -d
docker compose ps          # wait until all 4 show "healthy"
```
*(If using colima, run `colima start` first.)*

**4. Start the backend services** (api :3000, chat-gateway :8080, mss :8081)
```bash
pnpm dev                   # turbo runs all three together (watch mode)
```
…or run them separately in three terminals:
```bash
pnpm --filter api start:dev
pnpm --filter chat-gateway start:dev
pnpm --filter mss start:dev
```

**5. Verify everything is running**
```bash
curl -s localhost:3000/health   # {"status":"ok",...,"postgres":true}
curl -s localhost:8080/health   # {"status":"ok","kafkaAvailable":true,"redis":true,"postgres":true}
curl -s localhost:8081/health   # {"status":"ok","service":"mss","cassandra":true,"redis":true,"kafka":true}
```
On boot the gateway auto-creates the Kafka topics (`chat-events`, `chat-events-dlq`); **mss** owns the Cassandra schema (`chat_ks` + `messages`/`message_receipts`), created on its boot.

**6. Run the frontend** (separate repo, `messaging-web`)
```bash
git clone <messaging-web-url> messaging-web
cd messaging-web
pnpm install
pnpm dev                   # Vite on :5173; proxies /api → :3000, /gateway & /ws → :8080
```

## Docker services control

Run from the repo root (where `docker-compose.yml` lives):

```bash
# START the databases (pulls images on first run, then starts + waits for health)
docker compose up -d

# STOP the containers but keep them (fast restart later)
docker compose stop
docker compose start          # start them again

# FULL STOP + remove containers (volumes kept → data survives)
docker compose down
# FULL STOP + remove containers AND wipe all data (postgres/cassandra/kafka volumes)
docker compose down -v

# status / health
docker compose ps
```

**The engine is separate from the stack** — with colima:

```bash
colima start     # boots the Linux VM the containers run in
colima stop      # shuts the VM down (frees RAM/CPU)
colima status
```

Full sequence: `colima start` → `docker compose up -d` → `pnpm dev`.

These commands control **infrastructure only** — the apps run separately via `pnpm dev`.

## Testing the services

```bash
# Verify infra is up
docker compose ps
curl -s localhost:3000/health   # api
curl -s localhost:8080/health   # chat-gateway
```

End-to-end walkthrough — register two users, chat over WebSocket, read history:

```bash
# 1. Register two users (api :3000) — save each userId + token
curl -s -X POST localhost:3000/auth/register \
  -H 'Content-Type: application/json' \
  -d '{"email":"a@test.com","password":"password123"}'

curl -s -X POST localhost:3000/auth/register \
  -H 'Content-Type: application/json' \
  -d '{"email":"b@test.com","password":"password123"}'

# 2. Create a direct conversation as user A
curl -s -X POST localhost:3000/conversations/direct \
  -H "Authorization: Bearer $TOKEN_A" -H 'Content-Type: application/json' \
  -d '{"targetUserId":"<USER_B_ID>"}'
# (or a group: POST /conversations/group {"title":"...","participantIds":["..."]})

# 3. Open a WebSocket as each user (raw ws, JWT in ?token=) and send a message.
# With wscat (npm i -g wscat):
wscat -c "ws://localhost:8080?token=$TOKEN_A"
# > {"event":"message","data":{"conversationId":"<CONV_ID>","content":"hello","clientMessageId":"m1"}}
# sender gets {"message_sent"...} (one tick); the other socket gets {"message_received"...}

# 4. Read history back (gateway :8080 proxies to mss; default 20 messages)
curl -s "localhost:8080/messages/<CONV_ID>?limit=20" \
  -H "Authorization: Bearer $TOKEN_A"
```

## Repo layout

```
apps/
  api/            REST service (port 3000)
  chat-gateway/   WebSocket + HTTP gateway (port 8080)
packages/
  shared-types/   @chat/shared-types — types + shared runtime config
docker-compose.yml   postgres 15, redis 7, cassandra 4.1, kafka 3.9 (KRaft)
```

## Useful commands

```bash
pnpm build        # build all apps
pnpm dev          # run all apps in watch mode
pnpm lint         # lint
```
