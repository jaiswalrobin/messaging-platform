import { Injectable } from '@nestjs/common';
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
      return savedConversation;
    } catch (err) {
      await queryRunner.rollbackTransaction();
      throw err;
    } finally {
      await queryRunner.release();
    }
  }
}
