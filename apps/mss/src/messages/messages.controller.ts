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
  Logger,
} from '@nestjs/common';
import { CassandraService, MessageRecord } from './cassandra.service';
import { ParticipantCacheService } from '../participants/participant-cache.service';
import { HttpJwtGuard } from '../auth/http-jwt.guard';
import { MAX_HISTORY_LIMIT } from '@chat/shared-types';

/**
 * REST message history — the Cassandra owner's read path (moved from
 * chat-gateway by the SRP split). The gateway's /messages/:conversationId is
 * now a thin proxy to this endpoint.
 */
@Controller('messages')
export class MessagesController {
  private readonly logger = new Logger(MessagesController.name);

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
  ): Promise<{
    conversationId: string;
    messages: Array<MessageRecord & { receipts: { userId: string; status: 'delivered' | 'read' }[] }>;
  }> {
    // Reject non-members before exposing any conversation history (IDOR prevention)
    const isMember = await this.participantCache.isMember(conversationId, request.user.userId);
    if (!isMember) {
      throw new ForbiddenException('Not a member of this conversation');
    }

    const safeLimit = Math.max(1, Math.min(limit, MAX_HISTORY_LIMIT));
    const messages = await this.cassandraService.getMessages(conversationId, safeLimit);

    // Attach per-message receipt state so reloads hydrate accurate sent/delivered
    // ticks (the FE's history source is this REST endpoint). Best-effort: a
    // receipt-query failure falls back to empty arrays rather than failing history.
    let receiptsByMessage: Record<string, Array<{ userId: string; status: 'delivered' | 'read' }>> = {};
    try {
      const ids = messages.map((message) => message.id);
      receiptsByMessage = await this.cassandraService.getReceiptsForMessages(conversationId, ids);
    } catch (err) {
      this.logger.warn(
        `⚠️  Failed to fetch receipts for ${conversationId}: ${(err as Error).message} — returning history without receipt state`,
      );
    }
    const messagesWithReceipts = messages.map((message) => ({
      ...message,
      receipts: receiptsByMessage[message.id] ?? [],
    }));
    return { conversationId, messages: messagesWithReceipts };
  }
}
