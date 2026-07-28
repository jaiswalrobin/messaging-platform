import { Injectable, ForbiddenException, BadRequestException } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository } from 'typeorm';
import { ConversationParticipant } from './conversation-participant.entity';
import { Conversation } from './conversation.entity';

@Injectable()
export class ConversationsService {
  constructor(
    @InjectRepository(Conversation)
    private conversationRepo: Repository<Conversation>,
    @InjectRepository(ConversationParticipant)
    private participantRepo: Repository<ConversationParticipant>,
  ) {}

  async getConversationsForUser(userId: string) {
    // Find all conversation IDs where this user is a participant
    const participations = await this.participantRepo.find({
      where: { userId },
      relations: { conversation: true },
    });

    const conversations: any[] = [];

    for (const participation of participations) {
      const conversation = participation.conversation;

      // Get all participants for this conversation
      const participants = await this.participantRepo.find({
        where: { conversationId: conversation.id },
        relations: { user: true },
      });

      const participantList = participants.map((p) => ({
        userId: p.userId,
        email: p.user.email,
        role: p.role,
        joinedAt: p.joinedAt,
      }));

      conversations.push({
        id: conversation.id,
        title: conversation.title,
        type: conversation.type,
        createdAt: conversation.createdAt,
        updatedAt: conversation.updatedAt,
        participants: participantList,
      });
    }

    return conversations;
  }

  async createGroup(creatorId: string, title: string, participantIds: string[]) {
    // Start transaction manually since we're using repositories
    const queryRunner = this.conversationRepo.manager.connection.createQueryRunner();
    await queryRunner.connect();
    await queryRunner.startTransaction();

    try {
      // 1. Create the conversation
      const conversation = this.conversationRepo.create({
        title,
        type: 'group',
      });
      const savedConversation = await queryRunner.manager.save(conversation);

      // 2. Create the creator as admin
      const adminParticipant = this.participantRepo.create({
        conversationId: savedConversation.id,
        userId: creatorId,
        role: 'admin',
      });
      await queryRunner.manager.save(adminParticipant);

      // 3. Create other participants as members
      for (const participantId of participantIds) {
        if (participantId === creatorId) continue; // Don't add creator twice

        const memberParticipant = this.participantRepo.create({
          conversationId: savedConversation.id,
          userId: participantId,
          role: 'member',
        });
        await queryRunner.manager.save(memberParticipant);
      }

      await queryRunner.commitTransaction();

      // Return fully loaded conversation matching getConversationsForUser structure
      const participants = await this.participantRepo.find({
        where: { conversationId: savedConversation.id },
        relations: { user: true },
      });

      return {
        id: savedConversation.id,
        title: savedConversation.title,
        type: savedConversation.type,
        createdAt: savedConversation.createdAt,
        updatedAt: savedConversation.updatedAt,
        participants: participants.map((p) => ({
          userId: p.userId,
          email: p.user.email,
          role: p.role,
          joinedAt: p.joinedAt,
        })),
      };
    } catch (err) {
      await queryRunner.rollbackTransaction();
      throw err;
    } finally {
      await queryRunner.release();
    }
  }

  async updateGroupTitle(userId: string, conversationId: string, title: string) {
    const participant = await this.participantRepo.findOne({
      where: { userId, conversationId },
      relations: { conversation: true },
    });

    if (!participant || participant.role !== 'admin' || participant.conversation.type !== 'group') {
      throw new ForbiddenException('Unauthorized or invalid conversation');
    }

    participant.conversation.title = title;
    return this.conversationRepo.save(participant.conversation);
  }

  async addGroupParticipants(userId: string, conversationId: string, participantIds: string[]) {
    const adminParticipant = await this.participantRepo.findOne({
      where: { userId, conversationId },
      relations: { conversation: true },
    });

    if (!adminParticipant || adminParticipant.role !== 'admin' || adminParticipant.conversation.type !== 'group') {
      throw new ForbiddenException('Unauthorized or invalid conversation');
    }

    // Filter out participants already in the group
    const existingParticipants = await this.participantRepo.find({
      where: { conversationId },
    });
    const existingIds = new Set(existingParticipants.map((p) => p.userId));
    
    const newParticipants = participantIds
      .filter((id) => !existingIds.has(id))
      .map((id) =>
        this.participantRepo.create({
          conversationId,
          userId: id,
          role: 'member',
        }),
      );

    if (newParticipants.length > 0) {
      await this.participantRepo.save(newParticipants);
    }
    
    return adminParticipant.conversation;
  }

  async createDirectConversation(userId: string, targetUserId: string) {
    if (userId === targetUserId) {
      throw new BadRequestException('Cannot create a direct conversation with yourself');
    }

    // Check if direct conversation already exists
    // We look for a conversation of type 'direct' where both users are participants
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
      return this.conversationRepo.findOne({ where: { id: existing[0].id } });
    }

    // Start transaction manually
    const queryRunner = this.conversationRepo.manager.connection.createQueryRunner();
    await queryRunner.connect();
    await queryRunner.startTransaction();

    try {
      const conversation = this.conversationRepo.create({
        type: 'direct',
      });
      const savedConversation = await queryRunner.manager.save(conversation);

      const p1 = this.participantRepo.create({
        conversationId: savedConversation.id,
        userId: userId,
        role: 'member',
      });
      
      const p2 = this.participantRepo.create({
        conversationId: savedConversation.id,
        userId: targetUserId,
        role: 'member',
      });

      await queryRunner.manager.save([p1, p2]);
      await queryRunner.commitTransaction();

      // Return fully loaded conversation matching getConversationsForUser structure
      const participants = await this.participantRepo.find({
        where: { conversationId: savedConversation.id },
        relations: { user: true },
      });

      return {
        id: savedConversation.id,
        title: savedConversation.title,
        type: savedConversation.type,
        createdAt: savedConversation.createdAt,
        updatedAt: savedConversation.updatedAt,
        participants: participants.map((p) => ({
          userId: p.userId,
          email: p.user.email,
          role: p.role,
          joinedAt: p.joinedAt,
        })),
      };
    } catch (err) {
      await queryRunner.rollbackTransaction();
      throw err;
    } finally {
      await queryRunner.release();
    }
  }
}
