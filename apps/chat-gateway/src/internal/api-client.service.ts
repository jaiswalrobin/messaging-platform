import { Injectable, Logger } from '@nestjs/common';
import { getInternalApiKey } from '@chat/shared-types';

/**
 * HTTP client for the api service's internal endpoints (x-internal-key guard).
 * The gateway never writes api-owned tables directly — every cross-service
 * write goes through these endpoints. Mirrors the reverse direction, where the
 * api calls the gateway's /internal/participants/:id/invalidate.
 */
@Injectable()
export class ApiClientService {
  private readonly logger = new Logger(ApiClientService.name);

  /**
   * Persist a read watermark on the api's conversation_participants table.
   * Returns the api's verdict, or null when the api is unreachable / rejects
   * the call — the caller decides how to degrade (read receipts are soft state,
   * so a null verdict is not an error).
   */
  async markRead(
    conversationId: string,
    userId: string,
    lastReadMessageId: string,
  ): Promise<{ advanced: boolean } | null> {
    try {
      const res = await fetch(
        `http://${process.env.API_URL ?? 'localhost:3000'}/internal/conversations/${conversationId}/read`,
        {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            'x-internal-key': getInternalApiKey(),
          },
          body: JSON.stringify({ userId, lastReadMessageId }),
          signal: AbortSignal.timeout(2000),
        },
      );
      if (!res.ok) {
        this.logger.warn(`⚠️ api rejected mark_read (${res.status})`);
        return null;
      }
      return (await res.json()) as { advanced: boolean };
    } catch (err) {
      this.logger.warn(`⚠️ api mark_read failed (non-fatal): ${(err as Error).message}`);
      return null;
    }
  }
}
