import { Injectable, Logger, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import Redis from 'ioredis';
import { DELIVERY_CHANNEL_PREFIX, REGISTRY_KEY_PREFIX } from '@chat/shared-types';
import type { DeliveryFrame } from '@chat/shared-types';

/**
 * In-memory TTL for the registry lookup cache. Routing changes slowly (the
 * gateway refreshes registry keys on a 15s heartbeat against a 90s TTL), so a
 * ~1.5s stale window is harmless: a stale entry routes to a node that may have
 * dropped the socket, that node drops the frame, and the FE's history backfill
 * reconciles on reload.
 */
const REGISTRY_LOOKUP_CACHE_TTL_MS = 1500;

/** Opportunistic cleanup bounds the lookup cache's growth for users that never come back online. */
const REGISTRY_CACHE_PRUNE_THRESHOLD = 1024;
const REGISTRY_CACHE_PRUNE_INTERVAL_MS = 10_000;

/** A cached registry lookup for one userId. */
interface RegistryCacheEntry {
  /** DISTINCT nodeIds owning the user's online sockets. */
  nodes: string[];
  /** Epoch ms after which the entry must be re-resolved from Redis. */
  expiresAt: number;
}

/**
 * MSS-side delivery bus (Phase 3 of the SRP split): routes event frames to the
 * online sockets of a userId via the shared Redis connection registry.
 *
 * At-most-once bus: every frame is a best-effort PUBLISH — frames can be lost
 * (Redis down, a node dying between registry lookup and publish). That is by
 * design: Cassandra is the durable source of truth for messages and receipts,
 * and the FE's history backfill reconciles whatever this bus drops. For the
 * same reason these methods NEVER throw: a routing failure must not drive the
 * consumer's retry → DLQ → spurious PERSIST_FAILED to a sender whose message
 * was actually saved.
 *
 * Delivery is TARGETED, never broadcast: each frame goes to `delivery:{nodeId}`
 * for exactly the nodes owning the target user's sockets (each gateway
 * subscribes only to its own channel). This avoids the P×N² wall of
 * broadcasting every frame to every node and letting each node filter.
 */
@Injectable()
export class DeliveryPublisherService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(DeliveryPublisherService.name);
  private redis: Redis;
  private readonly registryCache = new Map<string, RegistryCacheEntry>();
  private lastRegistryCachePruneAt = 0;

  onModuleInit(): void {
    this.redis = new Redis({
      host: process.env.REDIS_HOST ?? 'localhost',
      port: parseInt(process.env.REDIS_PORT ?? '6379'),
      lazyConnect: true,
      maxRetriesPerRequest: 1,        // fail a command fast when Redis is truly down
      // ⚠️ do NOT set enableOfflineQueue:false — with lazyConnect it makes the
      //    first command throw before the socket is writeable. Leave it default (true)
      //    so the first command buffers until connect, and outages still reject via maxRetriesPerRequest.
      retryStrategy: (times) => Math.min(times * 200, 2000),
    });

    this.redis.on('connect', () => this.logger.log('✅ Redis connected'));
    this.redis.on('error', (err) => this.logger.warn(`⚠️ Redis: ${err.message}`)); // warn, don't spam error
  }

  async onModuleDestroy(): Promise<void> {
    try {
      await this.redis.quit();
    } catch {
      /* ignore — shutting down anyway */
    }
  }

  /**
   * Route an event frame to every online socket of `userId`, optionally
   * excluding the origin socket (`excludeSocketId` — it already received the
   * direct response and a second frame would duplicate it).
   *
   * Best-effort: any Redis failure is logged loudly and the method resolves —
   * a routing failure must never drive the consumer's retry → DLQ → spurious
   * PERSIST_FAILED. The message is already durable in Cassandra; the FE's
   * history backfill reconciles whatever this bus drops.
   */
  async publishToUser(
    userId: string,
    event: string,
    data: unknown,
    excludeSocketId?: string,
  ): Promise<void> {
    try {
      const nodes = await this.lookupNodes(userId);
      if (nodes.length === 0) {
        // No online sockets — the user is offline. Nothing to route; the FE
        // reconciles on reconnect/reload. (Empty results are deliberately NOT
        // cached, so the first publish after the user comes online sees them.)
        this.logger.debug(`📡 ${userId} offline — '${event}' not routed`);
        return;
      }

      const frame: DeliveryFrame = { event, data, userId, excludeSocketId };
      const payload = JSON.stringify(frame);
      for (const nodeId of nodes) {
        const channel = `${DELIVERY_CHANNEL_PREFIX}${nodeId}`;
        await this.redis.publish(channel, payload);
        this.logger.debug(`📡 Published '${event}' for ${userId} → ${channel}`);
      }
    } catch (err) {
      this.logger.error(
        `❌ Delivery routing failed for user=${userId} event=${event}: ${(err as Error).message} — frame not routed (FE backfill reconciles)`,
      );
    }
  }

  /**
   * Route an event frame to every online socket of each userId in `userIds`
   * (duplicates published once). publishToUser never throws, so this loop is
   * safe to run bare — it too can never drive the consumer into retry/DLQ.
   */
  async publishToUsers(
    userIds: string[],
    event: string,
    data: unknown,
    excludeSocketId?: string,
  ): Promise<void> {
    for (const userId of [...new Set(userIds)]) {
      await this.publishToUser(userId, event, data, excludeSocketId);
    }
  }

  /**
   * Resolve the DISTINCT nodeIds owning userId's online sockets:
   * HGETALL `registry:user:{userId}` returns {socketId → nodeId}; the values
   * are the node ids. Results are memoized for REGISTRY_LOOKUP_CACHE_TTL_MS so
   * a conversation fan-out (one HGETALL per participant) doesn't hammer Redis;
   * an empty result is never cached so an offline→online transition is visible
   * immediately. Throws only on Redis failure — the caller handles that.
   */
  private async lookupNodes(userId: string): Promise<string[]> {
    const now = Date.now();
    const cached = this.registryCache.get(userId);
    if (cached && now < cached.expiresAt) {
      return cached.nodes;
    }

    const sockets = await this.redis.hgetall(`${REGISTRY_KEY_PREFIX}${userId}`);
    const nodes = [...new Set(Object.values(sockets))];

    if (nodes.length > 0) {
      this.registryCache.set(userId, { nodes, expiresAt: now + REGISTRY_LOOKUP_CACHE_TTL_MS });
    } else {
      // Do not cache empty: the next publish after the user reconnects must
      // re-check the registry instead of serving a stale "offline" verdict.
      this.registryCache.delete(userId);
    }

    this.pruneRegistryCache(now);
    return nodes;
  }

  /**
   * Bounded cleanup: entries for users that never come back online would
   * otherwise linger in the Map forever. Only scans when the cache is large,
   * and at most once per interval, so the hot path stays O(1).
   */
  private pruneRegistryCache(now: number): void {
    if (this.registryCache.size < REGISTRY_CACHE_PRUNE_THRESHOLD) return;
    if (now - this.lastRegistryCachePruneAt < REGISTRY_CACHE_PRUNE_INTERVAL_MS) return;
    this.lastRegistryCachePruneAt = now;
    for (const [userId, entry] of this.registryCache) {
      if (entry.expiresAt <= now) {
        this.registryCache.delete(userId);
      }
    }
  }
}
