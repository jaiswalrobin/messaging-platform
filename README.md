# Messaging Platform

A WhatsApp/Discord-style real-time chat backend — a pnpm + Turborepo monorepo with two NestJS services.

| Service | Port | Protocol | Responsibility |
|---|---|---|---|
| `apps/api` | 3000 | HTTP (REST) | Auth, JWT, user search, conversation & group management |
| `apps/chat-gateway` | 8080 | HTTP + WebSocket | Live connections, message persistence (Cassandra), routing, fan-out, delivery receipts, history |

**Data stores:** PostgreSQL 15 (metadata) · Cassandra 4.1 (messages) · Redis 7 (participant cache).

> Full technical reference: [ARCHITECTURE.md](ARCHITECTURE.md)

## Prerequisites

- Node.js ≥ 18
- [pnpm](https://pnpm.io/installation) 9
- A Docker engine + compose (e.g. [colima](https://github.com/abiosoft/colima) + `docker-compose`, or Docker Desktop)

## Quickstart

```bash
# 1. Install dependencies
pnpm install

# 2. Start the databases (postgres, redis, cassandra)
docker compose up -d

# 3. Start both apps
pnpm dev
```

Environment is optional in dev (defaults match `docker-compose.yml`); see [.env.example](.env.example) for all variables. **In any real deployment, set `JWT_SECRET`** — both apps must share the same value.

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
# FULL STOP + remove containers AND wipe all data (postgres/cassandra volumes)
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
  eslint-config/  shared ESLint config
  typescript-config/  shared TS config
docker-compose.yml   postgres 15, redis 7, cassandra 4.1
ARCHITECTURE.md      exhaustive architecture reference
```

## Useful commands

```bash
pnpm build        # build all apps
pnpm dev          # run all apps in watch mode
pnpm lint         # lint
```
