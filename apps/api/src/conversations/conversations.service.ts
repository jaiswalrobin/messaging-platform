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
}
