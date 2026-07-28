import { Controller, Get, Post, Body, UseGuards, Request } from '@nestjs/common';
import { ConversationsService } from './conversations.service';
import { JwtAuthGuard } from '../auth/jwt-auth.guard';
import { CreateGroupDto } from './dto/create-group.dto';

@Controller('conversations')
export class ConversationsController {
  constructor(private conversationsService: ConversationsService) {}

  @UseGuards(JwtAuthGuard)
  @Get()
  getMyConversations(@Request() req: any) {
    return this.conversationsService.getConversationsForUser(req.user.userId);
  }

  @UseGuards(JwtAuthGuard)
  @Post('group')
  createGroup(@Body() body: CreateGroupDto, @Request() req: any) {
    return this.conversationsService.createGroup(
      req.user.userId,
      body.title,
      body.participantIds,
    );
  }
}
