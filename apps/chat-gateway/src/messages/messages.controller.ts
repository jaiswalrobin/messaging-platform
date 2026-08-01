import {
  Controller,
  Get,
  Param,
  Query,
  ParseIntPipe,
  DefaultValuePipe,
  UseGuards,
  Req,
  ForbiddenException,
} from '@nestjs/common';
import { CassandraService, MessageRecord } from './cassandra.service';
import { ParticipantCacheService } from '../participants/participant-cache.service';
import { HttpJwtGuard } from '../auth/http-jwt.guard';
import { MAX_HISTORY_LIMIT } from '@chat/shared-types';

@Controller('messages')
export class MessagesController {
  constructor(
    private readonly cassandraService: CassandraService,
    private readonly participantCache: ParticipantCacheService,
  ) {}

  @UseGuards(HttpJwtGuard)
  @Get(':conversationId')
  async getMessages(
    @Param('conversationId') conversationId: string,
    @Query('limit', new DefaultValuePipe(20), ParseIntPipe) limit: number,
    @Req() request: { user: { userId: string } },
  ): Promise<{ conversationId: string; messages: MessageRecord[] }> {
    // Reject non-members before exposing any conversation history (IDOR prevention)
    const member = await this.participantCache.isMember(conversationId, request.user.userId);
    if (!member) {
      throw new ForbiddenException('Not a member of this conversation');
    }

    const safeLimit = Math.min(limit, MAX_HISTORY_LIMIT);
    const messages = await this.cassandraService.getMessages(conversationId, safeLimit);
    return { conversationId, messages };
  }
}
