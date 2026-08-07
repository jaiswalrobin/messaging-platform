import {
  BadGatewayException,
  Controller,
  DefaultValuePipe,
  Get,
  Headers,
  Logger,
  Param,
  ParseIntPipe,
  Query,
} from '@nestjs/common';
import { MAX_HISTORY_LIMIT } from '@chat/shared-types';

/**
 * THIN PROXY to the mss service (the Cassandra owner under the SRP split). The
 * gateway no longer reads message storage: JWT auth, membership checks and
 * receipt hydration all happen in mss. The incoming Authorization header is
 * forwarded untouched so mss can authenticate the caller.
 */
@Controller('messages')
export class MessagesController {
  private readonly logger = new Logger(MessagesController.name);

  @Get(':conversationId')
  async getMessages(
    @Param('conversationId') conversationId: string,
    @Query('limit', new DefaultValuePipe(20), ParseIntPipe) limit: number,
    @Headers('authorization') authorization?: string,
  ): Promise<{ conversationId: string; messages: Array<Record<string, unknown>> }> {
    const mssUrl = process.env.MSS_URL ?? 'localhost:8081';
    const safeLimit = Math.max(1, Math.min(limit, MAX_HISTORY_LIMIT));

    const headers: Record<string, string> = {};
    if (authorization) {
      headers.authorization = authorization;
    }

    try {
      const response = await fetch(
        `http://${mssUrl}/messages/${conversationId}?limit=${safeLimit}`,
        { headers, signal: AbortSignal.timeout(5000) },
      );
      if (!response.ok) {
        throw new Error(`mss responded with ${response.status}`);
      }
      return (await response.json()) as {
        conversationId: string;
        messages: Array<Record<string, unknown>>;
      };
    } catch (err) {
      this.logger.error(
        `❌ mss history proxy failed for ${conversationId}: ${(err as Error).message}`,
      );
      throw new BadGatewayException('Message history service unavailable');
    }
  }
}
