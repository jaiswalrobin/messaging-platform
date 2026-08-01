import { Injectable, Logger, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository } from 'typeorm';
import Redis from 'ioredis';
import { ConversationParticipant } from './conversation-participant.entity';

const CACHE_TTL_SECONDS = 300; // 5 minutes

/** Redis key for a conversation's cached participant list. */
const participantsCacheKey = (conversationId: string): string =>
  `conversation:${conversationId}:participants`;

@Injectable()
export class ParticipantCacheService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(ParticipantCacheService.name);
  private redis: Redis;

  constructor(
    @InjectRepository(ConversationParticipant)
    private readonly participantRepo: Repository<ConversationParticipant>,
  ) {}

  onModuleInit() {
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

  async onModuleDestroy() {
    try {
      await this.redis.quit();
    } catch {
      /* ignore — shutting down anyway */
    }
  }

  /**
   * Returns all userIds in a conversation.
   * Redis is a best-effort cache: any Redis failure falls through to Postgres.
   */
  async getParticipants(conversationId: string): Promise<string[]> {
    const cacheKey = participantsCacheKey(conversationId);

    // 1. Try cache — never let a cache failure break the hot path
    try {
      const cached = await this.redis.get(cacheKey);
      if (cached) {
        this.logger.debug(`🎯 Cache hit for ${conversationId}`);
        return JSON.parse(cached) as string[];
      }
    } catch (err) {
      this.logger.warn(`⚠️ Redis read failed, using DB: ${(err as Error).message}`);
    }

    // 2. DB is the source of truth — always runs
    const participants = await this.participantRepo.find({
      where: { conversationId },
      select: { userId: true },
    });
    const userIds = participants.map((p) => p.userId);

    // 3. Best-effort cache write
    if (userIds.length > 0) {
      try {
        await this.redis.setex(cacheKey, CACHE_TTL_SECONDS, JSON.stringify(userIds));
      } catch (err) {
        this.logger.warn(`⚠️ Redis write failed (non-fatal): ${(err as Error).message}`);
      }
    }

    return userIds;
  }

  /** Returns whether the user is a participant in the conversation. */
  async isMember(conversationId: string, userId: string): Promise<boolean> {
    const count = await this.participantRepo.count({ where: { conversationId, userId } });
    return count > 0;
  }

  /** Call when a participant is added/removed (from the api side). */
  async invalidate(conversationId: string): Promise<void> {
    try {
      await this.redis.del(participantsCacheKey(conversationId));
      this.logger.log(`🗑️  Cache invalidated for ${conversationId}`);
    } catch (err) {
      this.logger.warn(`⚠️ Cache invalidate failed (non-fatal): ${(err as Error).message}`);
    }
  }
}