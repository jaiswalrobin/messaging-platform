import { Controller, Get, Param, Query, ParseIntPipe, DefaultValuePipe } from '@nestjs/common';
import { CassandraService, MessageRecord } from './cassandra.service';

@Controller('messages')
export class MessagesController {
  constructor(private readonly cassandraService: CassandraService) {}

  @Get(':conversationId')
  async getMessages(
    @Param('conversationId') conversationId: string,
    @Query('limit', new DefaultValuePipe(20), ParseIntPipe) limit: number,
  ): Promise<{ conversationId: string; messages: MessageRecord[] }> {
    const messages = await this.cassandraService.getMessages(conversationId, limit);
    return { conversationId, messages };
  }
}
