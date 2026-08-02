import { Module } from '@nestjs/common';
import { TypeOrmModule } from '@nestjs/typeorm';
import { Conversation } from './conversation.entity';
import { ConversationParticipant } from './conversation-participant.entity';
import { ConversationsService } from './conversations.service';
import { ConversationsController } from './conversations.controller';
import { InternalReadsController } from './internal-reads.controller';
import { User } from '../users/user.entity';

@Module({
  imports: [
    TypeOrmModule.forFeature([Conversation, ConversationParticipant, User]),
  ],
  controllers: [ConversationsController, InternalReadsController],
  providers: [ConversationsService],
})
export class ConversationsModule {}
