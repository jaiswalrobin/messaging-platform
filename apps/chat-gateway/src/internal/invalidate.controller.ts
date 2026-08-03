import {
  Controller,
  Post,
  Param,
  Headers,
  UnauthorizedException,
} from '@nestjs/common';
import { getInternalApiKey } from '@chat/shared-types';
import { ParticipantCacheService } from '../participants/participant-cache.service';

@Controller('internal/participants')
export class InvalidateController {
  constructor(private readonly participantCache: ParticipantCacheService) {}

  @Post(':conversationId/invalidate')
  async invalidate(
    @Param('conversationId') conversationId: string,
    @Headers('x-internal-key') key?: string,
  ): Promise<{ status: string }> {
    if (!key || key !== getInternalApiKey()) {
      throw new UnauthorizedException('Invalid internal key');
    }
    const ok = await this.participantCache.invalidate(conversationId);
    return { status: ok ? 'ok' : 'failed' }; // 200 — the caller inspects status
  }
}
