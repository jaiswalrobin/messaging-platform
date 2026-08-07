import { Injectable, Logger, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import Redis from 'ioredis';
import { REGISTRY_KEY_PREFIX, REGISTRY_TTL_SECONDS } from '@chat/shared-types';

/**
 * This node's stable id in the shared connection registry. Every socket
 * registered by this gateway instance is recorded with this value, so other
 * nodes can route cross-node delivery to this node. Override via NODE_ID;
 * defaults to 'node-1' for single-node dev.
 */
export const NODE_ID: string = process.env.NODE_ID ?? 'node-1';

/** Redis key for a user's shared registry hash: userId → { socketId → nodeId }. */
const registryKey = (userId: string): string => `${REGISTRY_KEY_PREFIX}${userId}`;

/**
 * Shared-registry WRITE path: records which sockets of which users are served
 * by which node, in Redis. The local in-memory map in ConnectionRegistryService
 * stays authoritative for delivery within this node; this hash is what lets a
 * multi-node fleet route a frame to a socket hosted on a different node.
 *
 * Every method is best-effort and never throws — a Redis failure must never
 * break a handshake, a disconnect, or a heartbeat tick.
 */
@Injectable()
export class RegistryService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(RegistryService.name);
  private redis: Redis;

  onModuleInit() {
    this.redis = new Redis({
      host: process.env.REDIS_HOST ?? 'localhost',
      port: parseInt(process.env.REDIS_PORT ?? '6379'),
      lazyConnect: true,
      maxRetriesPerRequest: 1, // fail a command fast when Redis is truly down
      // ⚠️ do NOT set enableOfflineQueue:false — with lazyConnect it makes the
      //    first command throw before the socket is writeable. Leave it default (true)
      //    so the first command buffers until connect, and outages still reject via maxRetriesPerRequest.
      retryStrategy: (times) => Math.min(times * 200, 2000),
    });

    this.redis.on('connect', () => this.logger.log('✅ Redis connected'));
    this.redis.on('error', (err) => this.logger.warn(`⚠️ Redis: ${err.message}`)); // warn, don't spam error
  }

  async onModuleDestroy() {
    try {
      await this.redis.quit();
    } catch {
      /* ignore — shutting down anyway */
    }
  }

  /**
   * Record that `socketId` (hosted by THIS node) is an online device of
   * `userId`: HSET the field, then EXPIRE the whole key so stale entries from
   * crashed nodes can't live forever.
   */
  async register(userId: string, socketId: string): Promise<void> {
    try {
      const key = registryKey(userId);
      await this.redis.hset(key, socketId, NODE_ID);
      await this.redis.expire(key, REGISTRY_TTL_SECONDS);
      this.logger.debug(`📡 Registry: ${userId} socket ${socketId} → ${NODE_ID}`);
    } catch (err) {
      this.logger.error(
        `❌ Registry register failed for ${userId} (socket ${socketId}): ${(err as Error).message}`,
      );
    }
  }

  /**
   * Remove a socket's entry on disconnect. Best-effort; a stale entry is
   * harmless (it expires via the TTL, and cross-node routing just finds no
   * socket on this node).
   */
  async unregister(userId: string, socketId: string): Promise<void> {
    try {
      await this.redis.hdel(registryKey(userId), socketId);
      this.logger.debug(`📡 Registry: ${userId} socket ${socketId} removed`);
    } catch (err) {
      this.logger.error(
        `❌ Registry unregister failed for ${userId} (socket ${socketId}): ${(err as Error).message}`,
      );
    }
  }

  /**
   * Keep the user's entry alive. Called on every heartbeat while the user has
   * at least one live socket, so a healthy session never expires mid-flight.
   */
  async refresh(userId: string): Promise<void> {
    try {
      await this.redis.expire(registryKey(userId), REGISTRY_TTL_SECONDS);
    } catch (err) {
      // warn (not error) — this runs every heartbeat per socket, and a Redis
      // outage is already being logged by the on('error') handler.
      this.logger.warn(`⚠️ Registry refresh failed for ${userId} (non-fatal): ${(err as Error).message}`);
    }
  }
}
