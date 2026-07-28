import { Injectable, Logger, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository } from 'typeorm';
import Redis from 'ioredis';
import { ConversationParticipant } from './conversation-participant.entity';

const CACHE_TTL_SECONDS = 300; // 5 minutes

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
    });

    this.redis.on('connect', () =>
      this.logger.log('✅ Redis connected'),
    );
    this.redis.on('error', (err) =>
      this.logger.error('❌ Redis error', err.message),
    );
  }

  async onModuleDestroy() {
    await this.redis.quit();
  }

  /**
   * Returns all userIds in a conversation.
   * Checks Redis first (TTL = 5 min). On miss, queries Postgres and caches.
   */
  async getParticipants(conversationId: string): Promise<string[]> {
    const cacheKey = `conversation:${conversationId}:participants`;

    // 1. Redis cache hit
    const cached = await this.redis.get(cacheKey);
    if (cached) {
      this.logger.debug(`🎯 Cache hit for ${conversationId}`);
      return JSON.parse(cached) as string[];
    }

    // 2. Cache miss — query DB
    this.logger.debug(`💾 Cache miss for ${conversationId} — querying DB`);
    const participants = await this.participantRepo.find({
      where: { conversationId },
      select: { userId: true },
    });

    const userIds = participants.map((p) => p.userId);

    // 3. Store in Redis with TTL
    if (userIds.length > 0) {
      await this.redis.setex(cacheKey, CACHE_TTL_SECONDS, JSON.stringify(userIds));
    }

    return userIds;
  }

  /**
   * Explicitly invalidate a conversation's participant cache.
   * Call this when a participant is added or removed (from api side).
   */
  async invalidate(conversationId: string): Promise<void> {
    await this.redis.del(`conversation:${conversationId}:participants`);
    this.logger.log(`🗑️  Cache invalidated for ${conversationId}`);
  }
}
