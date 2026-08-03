import {
  Injectable,
  ForbiddenException,
  BadRequestException,
  Logger,
} from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { In, Repository, EntityManager } from 'typeorm';
import { getInternalApiKey, MAX_HISTORY_LIMIT } from '@chat/shared-types';
import { ConversationParticipant } from './conversation-participant.entity';
import { Conversation } from './conversation.entity';
import { User } from '../users/user.entity';

/**
 * Extract the 60-bit timestamp (100-ns intervals since the UUID epoch) from a
 * v1 time-based UUID as a single Number, so two such ids compare chronologically.
 *
 * Needed because Postgres `uuid` comparison orders by time_low first — the
 * LEAST significant 32 bits of the timestamp — so `uuid_a < uuid_b` does NOT
 * mean a was generated before b. Two messages more than ~7 min apart can
 * compare backwards, silently dropping read receipts if we relied on
 * `last_read_message_id < :newId` in SQL. Cassandra message ids are v1
 * timeuuids, so this recovers true time order.
 */
function uuidV1Timestamp(uuid: string): number {
  // Canonical layout: time_low(8)-time_mid(4)-ver+time_hi(4)-clock(4)-node(12)
  const timeLow = parseInt(uuid.slice(0, 8), 16);
  const timeMid = parseInt(uuid.slice(9, 13), 16);
  // char 14 is the version nibble; time_hi is the remaining 3 nibbles (12 bits)
  const timeHi = parseInt(uuid.slice(15, 18), 16);
  // Assemble the 60-bit value. The high terms exceed 2^32, so multiply rather
  // than use bitwise OR (which truncates to 32 bits). For a current-epoch v1
  // uuid the result is ~1.4e17 ≈ 2^57, which EXCEEDS 2^53 — JS Numbers only
  // represent integers exactly up to 2^53, so this is a close approximation
  // with a ULP of ~30 (≈ 3µs of 100-ns ticks). Two ids generated within ~3µs
  // of each other can therefore compare equal. That's fine here because read
  // watermarks are soft state, but the value is NOT exact — don't rely on
  // byte-perfect ordering at sub-3µs granularity.
  return timeHi * 2 ** 48 + timeMid * 2 ** 32 + timeLow;
}

@Injectable()
export class ConversationsService {
  private readonly logger = new Logger(ConversationsService.name);

  constructor(
    @InjectRepository(Conversation)
    private conversationRepo: Repository<Conversation>,
    @InjectRepository(ConversationParticipant)
    private participantRepo: Repository<ConversationParticipant>,
    @InjectRepository(User)
    private readonly userRepo: Repository<User>,
  ) {}

  private mapParticipants(participants: ConversationParticipant[]) {
    return participants.map((participant) => ({
      userId: participant.userId,
      email: participant.user.email,
      role: participant.role,
      joinedAt: participant.joinedAt,
      // Read-receipt watermark — exposed so clients can hydrate blue ticks for
      // messages read while they were offline. Soft state; may lag the live
      // signal, which the gateway's `message_read` fan-out covers in real time.
      lastReadMessageId: participant.lastReadMessageId,
    }));
  }

  private toConversationDto(
    conversation: Conversation,
    participants: ConversationParticipant[],
  ) {
    return {
      id: conversation.id,
      title: conversation.title,
      type: conversation.type,
      createdAt: conversation.createdAt,
      updatedAt: conversation.updatedAt,
      participants: this.mapParticipants(participants),
    };
  }

  private async loadConversationWithParticipants(conversationId: string) {
    const conversation = await this.conversationRepo.findOne({
      where: { id: conversationId },
    });
    if (!conversation) {
      return null;
    }

    const participants = await this.participantRepo.find({
      where: { conversationId },
      relations: { user: true },
    });

    return this.toConversationDto(conversation, participants);
  }

  private async findGroupAdmin(
    userId: string,
    conversationId: string,
  ): Promise<ConversationParticipant | null> {
    const participant = await this.participantRepo.findOne({
      where: { userId, conversationId },
      relations: { conversation: true },
    });

    if (
      !participant ||
      participant.role !== 'admin' ||
      participant.conversation.type !== 'group'
    ) {
      return null;
    }

    return participant;
  }

  private async findMissingUserIds(userIds: string[]): Promise<string[]> {
    // Chunk the IN(...) so a huge participant list can't exceed Postgres's
    // bound-parameter limit or bloat a single query.
    const CHUNK_SIZE = 1000;
    const uniqueIds = [...new Set(userIds)];
    const existingIds = new Set<string>();
    for (let i = 0; i < uniqueIds.length; i += CHUNK_SIZE) {
      const chunk = uniqueIds.slice(i, i + CHUNK_SIZE);
      const existing = await this.userRepo.find({
        where: { id: In(chunk) },
        select: { id: true },
      });
      for (const user of existing) existingIds.add(user.id);
    }
    return uniqueIds.filter((id) => !existingIds.has(id));
  }

  async getConversationsForUser(
    userId: string,
    limit?: number,
    offset?: number,
  ) {
    // Distinguish an explicit 0 from "not provided": a caller passing
    // `limit: 0` means it, so clamp it up to 1 rather than silently treating it
    // as "no limit" (which `limit && ...` did). Valid limits clamp to [1, MAX].
    const hasLimit = limit !== undefined && !isNaN(limit);
    const safeLimit = hasLimit
      ? Math.max(1, Math.min(limit, MAX_HISTORY_LIMIT))
      : undefined;
    const hasOffset = offset !== undefined && !isNaN(offset);
    const safeOffset = hasOffset ? Math.max(0, offset) : undefined;

    // Find all conversation IDs where this user is a participant. The ORDER BY
    // makes offset pagination deterministic: rows can't shift between pages
    // when conversation order is stable (updatedAt can tie, so id breaks it).
    const participations = await this.participantRepo.find({
      where: { userId },
      relations: { conversation: true },
      order: { conversation: { updatedAt: 'DESC', id: 'ASC' } },
      ...(safeLimit !== undefined ? { take: safeLimit } : {}),
      ...(safeOffset !== undefined ? { skip: safeOffset } : {}),
    });

    // Fetch participants for all conversations in one query
    const conversationIds = participations.map(
      (participation) => participation.conversation.id,
    );
    const allParticipants =
      conversationIds.length > 0
        ? await this.participantRepo.find({
            where: { conversationId: In(conversationIds) },
            relations: { user: true },
          })
        : [];

    const participantsByConversation = new Map<
      string,
      ConversationParticipant[]
    >();
    for (const participant of allParticipants) {
      const participantList =
        participantsByConversation.get(participant.conversationId) ?? [];
      participantList.push(participant);
      participantsByConversation.set(participant.conversationId, participantList);
    }

    return participations.map((participation) =>
      this.toConversationDto(
        participation.conversation,
        participantsByConversation.get(participation.conversation.id) ?? [],
      ),
    );
  }

  /**
   * Run a unit of work in its own transaction: create a query runner, begin,
   * commit on success, roll back and rethrow on failure, always release.
   */
  private async withTransaction<T>(
    fn: (manager: EntityManager) => Promise<T>,
  ): Promise<T> {
    const queryRunner =
      this.conversationRepo.manager.connection.createQueryRunner();
    await queryRunner.connect();
    await queryRunner.startTransaction();

    try {
      const result = await fn(queryRunner.manager);
      await queryRunner.commitTransaction();
      return result;
    } catch (err) {
      await queryRunner.rollbackTransaction();
      throw err;
    } finally {
      await queryRunner.release();
    }
  }

  /**
   * Best-effort invalidation of the gateway's participant cache — never fail the
   * caller. A non-2xx response (e.g. gateway 401/500) is logged as a warning
   * just like a network error; both are non-fatal soft-state misses.
   */
  private async invalidateGatewayParticipantCache(conversationId: string) {
    try {
      const res = await fetch(
        `http://${process.env.GATEWAY_URL ?? 'localhost:8080'}/internal/participants/${conversationId}/invalidate`,
        {
          method: 'POST',
          headers: { 'x-internal-key': getInternalApiKey() },
        },
      );
      if (!res.ok) {
        this.logger.warn(
          `⚠️ Gateway invalidation failed (non-fatal): HTTP ${res.status}`,
        );
      }
    } catch (err) {
      this.logger.warn(
        `⚠️ Gateway invalidation failed (non-fatal): ${(err as Error).message}`,
      );
    }
  }

  async createGroup(
    creatorId: string,
    title: string,
    participantIds: string[],
  ) {
    const savedConversation = await this.withTransaction(async (manager) => {
      // 1. Create the conversation
      const conversation = this.conversationRepo.create({
        title,
        type: 'group',
      });
      const saved = await manager.save(conversation);

      // 2. Create the creator as admin
      const adminParticipant = this.participantRepo.create({
        conversationId: saved.id,
        userId: creatorId,
        role: 'admin',
      });
      await manager.save(adminParticipant);

      // 3. Verify all participant IDs exist before inserting them
      const missingIds = await this.findMissingUserIds(participantIds);
      if (missingIds.length > 0) {
        throw new BadRequestException(
          'One or more participant IDs do not exist',
        );
      }

      // 4. Create other participants as members (deduped so duplicate
      // participantIds can't violate the (conversation_id, user_id) PK).
      // Build all member entities first, then insert them in one bulk save
      // instead of a per-participant round trip.
      const memberParticipants = [...new Set(participantIds)]
        .filter((participantId) => participantId !== creatorId) // Don't add creator twice
        .map((participantId) =>
          this.participantRepo.create({
            conversationId: saved.id,
            userId: participantId,
            role: 'member',
          }),
        );
      if (memberParticipants.length > 0) {
        await manager.save(memberParticipants);
      }

      return saved;
    });

    // Return fully loaded conversation matching getConversationsForUser structure
    return this.loadConversationWithParticipants(savedConversation.id);
  }

  async updateGroupTitle(
    userId: string,
    conversationId: string,
    title: string,
  ) {
    const participant = await this.findGroupAdmin(userId, conversationId);

    if (!participant) {
      throw new ForbiddenException('Unauthorized or invalid conversation');
    }

    participant.conversation.title = title;
    await this.conversationRepo.save(participant.conversation);

    // Return the same DTO shape as every other conversation endpoint — the
    // conversation is guaranteed to exist (findGroupAdmin just loaded it).
    return (await this.loadConversationWithParticipants(conversationId))!;
  }

  async addGroupParticipants(
    userId: string,
    conversationId: string,
    participantIds: string[],
  ) {
    const adminParticipant = await this.findGroupAdmin(userId, conversationId);

    if (!adminParticipant) {
      throw new ForbiddenException('Unauthorized or invalid conversation');
    }

    // Dedupe before filtering so the 'new participants' list can't contain duplicates
    const uniqueParticipantIds = [...new Set(participantIds)];

    // Filter out participants already in the group
    const existingParticipants = await this.participantRepo.find({
      where: { conversationId },
    });
    const existingIds = new Set(
      existingParticipants.map((participant) => participant.userId),
    );

    // Filter once to the id list; entities are only wrapped after validation
    // survives, so we never map to entities and immediately re-unwrap them.
    const newParticipantIds = uniqueParticipantIds.filter(
      (id) => !existingIds.has(id),
    );

    // Verify the new participant IDs actually exist
    const invalidIds =
      newParticipantIds.length > 0
        ? await this.findMissingUserIds(newParticipantIds)
        : [];
    if (invalidIds.length > 0) {
      throw new BadRequestException(
        `One or more participant IDs do not exist: ${invalidIds.join(', ')}`,
      );
    }

    if (newParticipantIds.length > 0) {
      const newParticipants = newParticipantIds.map((id) =>
        this.participantRepo.create({
          conversationId,
          userId: id,
          role: 'member',
        }),
      );
      await this.withTransaction(async (manager) => {
        await manager.save(newParticipants);
      });

      await this.invalidateGatewayParticipantCache(conversationId);
    }

    // Return the same DTO shape as every other conversation endpoint — the
    // conversation is guaranteed to exist (findGroupAdmin just loaded it).
    return (await this.loadConversationWithParticipants(conversationId))!;
  }

  /**
   * Advance a participant's read watermark (idempotent, monotonic).
   * Called by the chat-gateway through the internal endpoint — never directly.
   * Returns whether the watermark actually moved forward, so the gateway can
   * decide whether to broadcast a `message_read` receipt.
   *
   * Monotonicity is enforced by comparing the v1 timeuuid timestamps, NOT by
   * `last_read_message_id < :newId` in SQL — Postgres `uuid` byte order is not
   * chronological (see uuidV1Timestamp). We load the current watermark, compare
   * timestamps in JS, and compare-and-swap on the UPDATE so a concurrent
   * advance can't be clobbered by a stale one. Read receipts are soft state,
   * so the rare lost forward-advance in a concurrent race is acceptable.
   */
  async markRead(
    conversationId: string,
    userId: string,
    lastReadMessageId: string,
  ): Promise<{ advanced: boolean }> {
    // The watermark must be a canonical v1 timeuuid — reject anything else up
    // front (garbage would 22P02 on Postgres's uuid cast and NaN the compares).
    // The version nibble is pinned to '1' (not '[1-6]'): a well-formed v2–v6 uuid
    // with a past timestamp field would otherwise be written as the watermark,
    // forging blue ticks. (The gateway enforces the same gate before publishing.)
    if (
      !/^[0-9a-f]{8}-[0-9a-f]{4}-1[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(
        lastReadMessageId,
      )
    ) {
      throw new BadRequestException('Invalid lastReadMessageId');
    }

    // Select the PK columns alongside the watermark. TypeORM hydrates a row as
    // non-existent when EVERY projected column comes back NULL — so a `select`
    // of only the nullable `lastReadMessageId` returns null on the first read
    // (watermark still NULL), making markRead no-op forever and blue ticks never
    // fire. Projecting the non-null PK columns guarantees the row hydrates.
    const existing = await this.participantRepo.findOne({
      where: { conversationId, userId },
      select: { conversationId: true, userId: true, lastReadMessageId: true },
    });

    // Not a participant — nothing to advance.
    if (!existing) {
      return { advanced: false };
    }

    // The NULL (first read) and non-NULL branches below were identical except
    // for the CAS predicate (`last_read_message_id IS NULL` vs `= :current`).
    // Collapse them into one path: `currentValue` is the watermark we CAS
    // against, and `casWhere` renders the predicate — a bound `current` value,
    // or `IS NULL` when the watermark is NULL.
    const currentValue = existing.lastReadMessageId;

    // Only advance if the new receipt is genuinely newer (chronological, not
    // byte-order). When the watermark is NULL there is nothing to compare
    // against — any id is newer than NULL, so skip the stale check.
    if (
      currentValue !== null &&
      uuidV1Timestamp(lastReadMessageId) <= uuidV1Timestamp(currentValue)
    ) {
      this.logger.debug(
        `mark_read no-op for ${userId} in ${conversationId} (stale receipt)`,
      );
      return { advanced: false };
    }

    // Render the parameterized CAS predicate for the given stored watermark.
    const casWhere = (current: string | null) =>
      current !== null
        ? { sql: 'last_read_message_id = :current', params: { current } }
        : { sql: 'last_read_message_id IS NULL', params: {} };

    // CAS-update the watermark against the given stored value. A single code
    // path covers both the first read (CAS on NULL) and later advances (CAS on
    // the current value).
    const runCas = (current: string | null) =>
      this.participantRepo
        .createQueryBuilder()
        .update(ConversationParticipant)
        .set({ lastReadMessageId })
        .where('conversation_id = :conversationId', { conversationId })
        .andWhere('user_id = :userId', { userId })
        .andWhere(casWhere(current).sql, casWhere(current).params)
        .execute();

    const result = await runCas(currentValue);
    const advanced = (result.affected ?? 0) > 0;
    if (!advanced) {
      // CAS lost to a concurrent advance: re-query current watermark.
      const current = await this.participantRepo.findOne({
        where: { conversationId, userId },
        select: { lastReadMessageId: true },
      });

      // A concurrent advance that is already at-or-newer than the requested id
      // means the watermark did move forward — report advanced.
      if (
        current?.lastReadMessageId &&
        uuidV1Timestamp(current.lastReadMessageId) >=
          uuidV1Timestamp(lastReadMessageId)
      ) {
        this.logger.debug(
          `mark_read for ${userId} in ${conversationId}: race lost, but current watermark ${current.lastReadMessageId} >= ${lastReadMessageId}`,
        );
        return { advanced: true };
      }

      // The concurrent advance was itself stale (still older than the requested
      // id) — retry once against the fresh value so the newer receipt isn't
      // stranded. The retry can only fail if another writer moved the watermark
      // to >= requested in the meantime, keeping advanced=false only when the
      // stored value is already at-or-newer. (A re-queried NULL — e.g. a vanished
      // row — CASes on `IS NULL` and simply matches nothing.)
      const retry = await runCas(current?.lastReadMessageId ?? null);
      return { advanced: (retry.affected ?? 0) > 0 };
    }
    return { advanced };
  }

  async createDirectConversation(userId: string, targetUserId: string) {
    if (userId === targetUserId) {
      throw new BadRequestException(
        'Cannot create a direct conversation with yourself',
      );
    }

    // Verify the target user exists before creating anything
    const targetUser = await this.userRepo.findOne({
      where: { id: targetUserId },
    });
    if (!targetUser) {
      throw new BadRequestException('Target user does not exist');
    }

    // Check if a direct conversation already exists. The deterministic
    // directKey (sorted `a|b`) is a unique-indexed column, so a single indexed
    // lookup replaces the old two-table JOIN — concurrent creations still
    // collide on that unique index (handled below via the 23505 fallback).
    const directKey = [userId, targetUserId].sort().join('|');
    const existing = await this.conversationRepo.findOne({
      where: { directKey },
    });

    if (existing) {
      return this.loadConversationWithParticipants(existing.id);
    }

    try {
      const savedConversation = await this.withTransaction(async (manager) => {
        const conversation = this.conversationRepo.create({
          type: 'direct',
          directKey,
        });
        const saved = await manager.save(conversation);

        const participant1 = this.participantRepo.create({
          conversationId: saved.id,
          userId,
          role: 'member',
        });

        const participant2 = this.participantRepo.create({
          conversationId: saved.id,
          userId: targetUserId,
          role: 'member',
        });

        await manager.save([participant1, participant2]);
        return saved;
      });

      // Return fully loaded conversation matching getConversationsForUser structure
      return this.loadConversationWithParticipants(savedConversation.id);
    } catch (err) {
      // Unique violation on direct_key (Postgres error code 23505) means a
      // concurrent request already created this conversation — return it instead.
      // (withTransaction already rolled the transaction back and rethrew; we
      // only inspect the error here to pick the fallback path.)
      if ((err.driverError?.code ?? err.code) === '23505') {
        const existingConversation = await this.conversationRepo.findOne({
          where: { directKey },
        });
        if (existingConversation) {
          return this.loadConversationWithParticipants(existingConversation.id);
        }
      }
      throw err;
    }
  }
}
