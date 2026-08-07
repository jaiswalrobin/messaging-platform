import { Module } from '@nestjs/common';
import { TypeOrmModule } from '@nestjs/typeorm';
import { ConversationParticipant } from './conversation-participant.entity';
import { ParticipantCacheService } from './participant-cache.service';

@Module({
  imports: [TypeOrmModule.forFeature([ConversationParticipant])],
  providers: [ParticipantCacheService],
  exports: [ParticipantCacheService],
})
export class ParticipantsModule {}
