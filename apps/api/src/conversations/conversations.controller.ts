import { Controller, Get, UseGuards, Request } from '@nestjs/common';
import { ConversationsService } from './conversations.service';
import { JwtAuthGuard } from '../auth/jwt-auth.guard';

@Controller('conversations')
export class ConversationsController {
  constructor(private conversationsService: ConversationsService) {}

  @UseGuards(JwtAuthGuard)
  @Get()
  getMyConversations(@Request() req: any) {
    return this.conversationsService.getConversationsForUser(req.user.userId);
  }
}
