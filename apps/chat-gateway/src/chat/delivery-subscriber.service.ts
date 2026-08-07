import { Injectable, Logger, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import Redis from 'ioredis';
import { DELIVERY_CHANNEL_PREFIX, type DeliveryFrame } from '@chat/shared-types';
import { ConnectionRegistryService } from './connection-registry.service';

/** This gateway node's identity — must match the publisher's view of the cluster. */
const NODE_ID = process.env.NODE_ID ?? 'node-1';

/** The channel THIS node subscribes to: other nodes publish frames addressed to us here. */
const DELIVERY_CHANNEL = `${DELIVERY_CHANNEL_PREFIX}${NODE_ID}`;

/**
 * Node-side half of the Redis delivery bus (multi-node routing).
 *
 * When a gateway node needs to reach a user whose sockets live on ANOTHER node,
 * it publishes a DeliveryFrame to `delivery:{nodeId}`; this subscriber consumes
 * the frames addressed to THIS node and routes them to the user's local sockets
 * via ConnectionRegistryService.sendToUserSockets (skipping the origin socket
 * when the frame carries excludeSocketId).
 *
 * Delivery semantics are at-most-once by design: while Redis is down the
 * subscriber sits inert and frames are simply missed — the FE backfills
 * (message_received dedupes by messageId, receipts re-hydrate on reload), so a
 * dropped delivery frame degrades gracefully to "the client finds out later".
 *
 * Single-node deployments subscribe but no other node publishes — harmless.
 */
@Injectable()
export class DeliverySubscriberService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(DeliverySubscriberService.name);
  private redis: Redis;

  constructor(private readonly registry: ConnectionRegistryService) {}

  onModuleInit(): void {
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

    this.redis.on('connect', () =>
      this.logger.log(`✅ Redis connected — delivery subscriber on ${DELIVERY_CHANNEL}`),
    );
    // warn, don't spam error — an 'error' event with no listener would crash the process
    this.redis.on('error', (err) =>
      this.logger.warn(`⚠️ Redis (delivery subscriber): ${err.message}`),
    );

    this.redis.on('message', this.handleMessage);

    // Both calls are fire-and-forget on purpose: while Redis is down, connect()
    // retries in the background (retryStrategy never gives up) and subscribe
    // stays queued until the connection comes up — module init must NOT block on
    // the bus being unavailable (frames missed during an outage are backfilled
    // by the FE; that's the designed at-most-once semantics).
    this.redis.connect().catch((err) =>
      this.logger.warn(`⚠️ Delivery subscriber connect failed: ${(err as Error).message}`),
    );
    this.redis.subscribe(DELIVERY_CHANNEL).catch((err) =>
      this.logger.warn(`⚠️ Delivery subscriber subscribe failed on ${DELIVERY_CHANNEL}: ${(err as Error).message}`),
    );
    // ⚠️ Do NOT re-subscribe from an on('connect') handler: ioredis re-subscribes
    // to previously subscribed channels automatically on reconnect, and a manual
    // re-subscribe would double-subscribe (duplicate deliveries of every frame).
  }

  async onModuleDestroy(): Promise<void> {
    try {
      await this.redis.unsubscribe(DELIVERY_CHANNEL);
    } catch {
      /* connection already down — nothing to unsubscribe */
    }
    try {
      await this.redis.quit();
    } catch {
      /* ignore — shutting down anyway */
    }
  }

  /**
   * A frame addressed to this node: route it to the user's local sockets,
   * skipping the origin socket when the frame says so. Malformed or structurally
   * invalid frames are logged and dropped — never allowed to throw into the
   * ioredis event emitter (an uncaught throw would crash the process).
   */
  private readonly handleMessage = (channel: string, message: string): void => {
    let frame: DeliveryFrame;
    try {
      frame = JSON.parse(message) as DeliveryFrame;
    } catch (err) {
      this.logger.warn(
        `⚠️ Dropped malformed delivery frame on ${channel}: ${(err as Error).message}`,
      );
      return;
    }

    if (typeof frame?.userId !== 'string' || typeof frame.event !== 'string') {
      this.logger.warn(`⚠️ Dropped delivery frame missing required fields on ${channel}`);
      return;
    }

    try {
      // Synchronous and defensive (skips closed sockets, catches write errors),
      // but never let an unexpected failure propagate into the event emitter.
      this.registry.sendToUserSockets(frame.userId, frame.event, frame.data, frame.excludeSocketId);
    } catch (err) {
      this.logger.error(
        `❌ Delivery frame dispatch failed for ${frame.userId}: ${(err as Error).message}`,
      );
    }
  };
}
