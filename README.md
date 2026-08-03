# Messaging Platform

A WhatsApp/Discord-style real-time chat backend — a pnpm + Turborepo monorepo with two NestJS services.

| Service | Port | Protocol | Responsibility |
|---|---|---|---|
| `apps/api` | 3000 | HTTP (REST) | Auth, JWT, user search, conversation & group management |
| `apps/chat-gateway` | 8080 | HTTP + WebSocket | Live connections, message persistence (Cassandra), routing, fan-out, delivery receipts, history |

**Data stores:** PostgreSQL 15 (metadata) · Cassandra 4.1 (messages + receipts) · Redis 7 (participant cache) · Kafka 3.9 / KRaft (event log — the Kafka-first send + receipts pipeline).

> Full technical reference: [ARCHITECTURE.md](ARCHITECTURE.md)

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

# 3. Start both apps
pnpm dev
```

Environment is optional in dev (defaults match `docker-compose.yml`); see [.env.example](.env.example) for all variables. **In any real deployment, set `JWT_SECRET`** — both apps must share the same value.

## Full install & run, step by step (fresh machine)

**1. Prerequisites** — git, Node.js ≥ 23.6, pnpm 9 (`corepack enable` or `npm i -g pnpm`), and Docker with **Compose v2** (`docker compose`, not the deprecated `docker-compose`).

**2. Clone + install dependencies**
```bash
git clone <your-repo-url> messaging-platform
cd messaging-platform
pnpm install
```

**3. Start the infrastructure** (Postgres, Redis, Cassandra, Kafka — all bound to `127.0.0.1`)
```bash
docker compose up -d
docker compose ps          # wait until all 4 show "healthy"
```
*(If using colima, run `colima start` first.)*

**4. Start the backend services** (api :3000 + chat-gateway :8080)
```bash
pnpm dev                   # turbo runs both together (watch mode)
```
…or run them separately in two terminals:
```bash
pnpm --filter api start:dev
pnpm --filter chat-gateway start:dev
```

**5. Verify everything is running**
```bash
curl -s localhost:3000/health   # {"status":"ok",...,"postgres":true}
curl -s localhost:8080/health   # {"status":"ok","kafkaAvailable":true,"cassandra":true,"redis":true,"postgres":true}
```
On boot the gateway auto-creates the Cassandra schema (`chat_ks` + `messages`/`message_receipts`) and the Kafka topics (`chat-events`, `chat-events-dlq`).

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

A full end-to-end walkthrough (register users → create group → WebSocket chat → history) lives in [ARCHITECTURE.md §18](ARCHITECTURE.md#18-testing-guide).

## Repo layout

```
apps/
  api/            REST service (port 3000)
  chat-gateway/   WebSocket + HTTP gateway (port 8080)
packages/
  shared-types/   @chat/shared-types — types + shared runtime config
docker-compose.yml   postgres 15, redis 7, cassandra 4.1, kafka 3.9 (KRaft)
ARCHITECTURE.md      exhaustive architecture reference
```

## Useful commands

```bash
pnpm build        # build all apps
pnpm dev          # run all apps in watch mode
pnpm lint         # lint
```
