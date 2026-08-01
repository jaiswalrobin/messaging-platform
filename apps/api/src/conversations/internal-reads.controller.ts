import {
  Controller,
  Post,
  Param,
  Body,
  Headers,
  UnauthorizedException,
} from '@nestjs/common';
import { getInternalApiKey } from '@chat/shared-types';
import { ConversationsService } from './conversations.service';
import { MarkReadDto } from './dto/mark-read.dto';

/**
 * Internal endpoints — called by the chat-gateway over HTTP, guarded by the
 * shared x-internal-key. Never exposed to browsers (no JWT here on purpose;
 * the gateway already verified membership before calling us).
 */
@Controller('internal/conversations')
export class InternalReadsController {
  constructor(private readonly conversationsService: ConversationsService) {}

  @Post(':conversationId/read')
  async markRead(
    @Param('conversationId') conversationId: string,
    @Body() body: MarkReadDto,
    @Headers('x-internal-key') key?: string,
  ): Promise<{ advanced: boolean }> {
    if (!key || key !== getInternalApiKey()) {
      throw new UnauthorizedException('Invalid internal key');
    }
    return this.conversationsService.markRead(
      conversationId,
      body.userId,
      body.lastReadMessageId,
    );
  }
}
