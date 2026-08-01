import { Injectable, ForbiddenException, BadRequestException, Logger } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { In, Repository, EntityManager } from 'typeorm';
import { getInternalApiKey } from '@chat/shared-types';
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
  // than use bitwise OR (which truncates to 32 bits). The result stays well
  // below 2^53, so it is represented exactly as a JS Number.
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
    return participants.map((p) => ({
      userId: p.userId,
      email: p.user.email,
      role: p.role,
      joinedAt: p.joinedAt,
      // Read-receipt watermark — exposed so clients can hydrate blue ticks for
      // messages read while they were offline. Soft state; may lag the live
      // signal, which the gateway's `message_read` fan-out covers in real time.
      lastReadMessageId: p.lastReadMessageId,
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

    if (!participant || participant.role !== 'admin' || participant.conversation.type !== 'group') {
      return null;
    }

    return participant;
  }

  private async findMissingUserIds(userIds: string[]): Promise<string[]> {
    const existing = await this.userRepo.find({
      where: { id: In(userIds) },
      select: { id: true },
    });
    const existingIds = new Set(existing.map((u) => u.id));
    return [...new Set(userIds)].filter((id) => !existingIds.has(id));
  }

  async getConversationsForUser(userId: string) {
    // Find all conversation IDs where this user is a participant
    const participations = await this.participantRepo.find({
      where: { userId },
      relations: { conversation: true },
    });

    // Fetch participants for all conversations in one query
    const conversationIds = participations.map((p) => p.conversation.id);
    const allParticipants = conversationIds.length > 0
      ? await this.participantRepo.find({
          where: { conversationId: In(conversationIds) },
          relations: { user: true },
        })
      : [];

    const participantsByConversation = new Map<string, ConversationParticipant[]>();
    for (const participant of allParticipants) {
      const list = participantsByConversation.get(participant.conversationId) ?? [];
      list.push(participant);
      participantsByConversation.set(participant.conversationId, list);
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
    const queryRunner = this.conversationRepo.manager.connection.createQueryRunner();
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

  async createGroup(creatorId: string, title: string, participantIds: string[]) {
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
        throw new BadRequestException('One or more participant IDs do not exist');
      }

      // 4. Create other participants as members (deduped so duplicate
      // participantIds can't violate the (conversation_id, user_id) PK)
      for (const participantId of [...new Set(participantIds)]) {
        if (participantId === creatorId) continue; // Don't add creator twice

        const memberParticipant = this.participantRepo.create({
          conversationId: saved.id,
          userId: participantId,
          role: 'member',
        });
        await manager.save(memberParticipant);
      }

      return saved;
    });

    // Return fully loaded conversation matching getConversationsForUser structure
    return this.loadConversationWithParticipants(savedConversation.id);
  }

  async updateGroupTitle(userId: string, conversationId: string, title: string) {
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

  async addGroupParticipants(userId: string, conversationId: string, participantIds: string[]) {
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
    const existingIds = new Set(existingParticipants.map((p) => p.userId));

    const newParticipants = uniqueParticipantIds
      .filter((id) => !existingIds.has(id))
      .map((id) =>
        this.participantRepo.create({
          conversationId,
          userId: id,
          role: 'member',
        }),
      );

    // Verify the new participant IDs actually exist
    const newParticipantIds = newParticipants.map((p) => p.userId);
    const invalidIds = newParticipantIds.length > 0
      ? await this.findMissingUserIds(newParticipantIds)
      : [];
    if (invalidIds.length > 0) {
      throw new BadRequestException(`One or more participant IDs do not exist: ${invalidIds.join(', ')}`);
    }

    if (newParticipants.length > 0) {
      await this.participantRepo.save(newParticipants);

      // Best-effort invalidation of the gateway's participant cache — never fail the request.
      try {
        await fetch(`http://${process.env.GATEWAY_URL ?? 'localhost:8080'}/internal/participants/${conversationId}/invalidate`, {
          method: 'POST',
          headers: { 'x-internal-key': getInternalApiKey() },
        });
      } catch (err) {
        this.logger.warn(`⚠️ Gateway invalidation failed (non-fatal): ${(err as Error).message}`);
      }
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

    // First read in this conversation — advance from NULL.
    if (existing.lastReadMessageId === null) {
      const result = await this.participantRepo
        .createQueryBuilder()
        .update(ConversationParticipant)
        .set({ lastReadMessageId })
        .where('conversation_id = :conversationId', { conversationId })
        .andWhere('user_id = :userId', { userId })
        .andWhere('last_read_message_id IS NULL')
        .execute();
      return { advanced: (result.affected ?? 0) > 0 };
    }

    // Only advance if the new receipt is genuinely newer (chronological, not
    // byte-order). Compare-and-swap on the current value so a concurrent
    // advance wins cleanly instead of being clobbered by a stale one.
    if (uuidV1Timestamp(lastReadMessageId) <= uuidV1Timestamp(existing.lastReadMessageId)) {
      this.logger.debug(
        `mark_read no-op for ${userId} in ${conversationId} (stale receipt)`,
      );
      return { advanced: false };
    }

    const result = await this.participantRepo
      .createQueryBuilder()
      .update(ConversationParticipant)
      .set({ lastReadMessageId })
      .where('conversation_id = :conversationId', { conversationId })
      .andWhere('user_id = :userId', { userId })
      .andWhere('last_read_message_id = :current', { current: existing.lastReadMessageId })
      .execute();

    const advanced = (result.affected ?? 0) > 0;
    if (!advanced) {
      this.logger.debug(
        `mark_read no-op for ${userId} in ${conversationId} (race lost to concurrent advance)`,
      );
    }
    return { advanced };
  }

  async createDirectConversation(userId: string, targetUserId: string) {
    if (userId === targetUserId) {
      throw new BadRequestException('Cannot create a direct conversation with yourself');
    }

    // Verify the target user exists before creating anything
    const targetUser = await this.userRepo.findOne({ where: { id: targetUserId } });
    if (!targetUser) {
      throw new BadRequestException('Target user does not exist');
    }

    // Check if direct conversation already exists.
    // Table names mirror the entities: `conversations` = Conversation,
    // `conversation_participants` = ConversationParticipant. We look for a
    // conversation of type 'direct' where both users are participants.
    // Note: this assumes direct conversations always have exactly 2 participants
    const query = `
      SELECT c.id
      FROM conversations c
      JOIN conversation_participants cp1 ON c.id = cp1.conversation_id
      JOIN conversation_participants cp2 ON c.id = cp2.conversation_id
      WHERE c.type = 'direct'
        AND cp1.user_id = $1
        AND cp2.user_id = $2
      LIMIT 1
    `;
    const existing = await this.conversationRepo.query(query, [userId, targetUserId]);

    if (existing && existing.length > 0) {
      return this.loadConversationWithParticipants(existing[0].id);
    }

    // Deterministic key so concurrent creations collide on the unique index
    const directKey = [userId, targetUserId].sort().join('|');

    try {
      const savedConversation = await this.withTransaction(async (manager) => {
        const conversation = this.conversationRepo.create({
          type: 'direct',
          directKey,
        });
        const saved = await manager.save(conversation);

        const p1 = this.participantRepo.create({
          conversationId: saved.id,
          userId,
          role: 'member',
        });

        const p2 = this.participantRepo.create({
          conversationId: saved.id,
          userId: targetUserId,
          role: 'member',
        });

        await manager.save([p1, p2]);
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
