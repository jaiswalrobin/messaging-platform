import { Controller, Get, Post, Patch, Body, Param, UseGuards, Request } from '@nestjs/common';
import { ConversationsService } from './conversations.service';
import { JwtAuthGuard } from '../auth/jwt-auth.guard';
import type { AuthenticatedRequest } from '../auth/request-user';
import { CreateGroupDto } from './dto/create-group.dto';
import { UpdateGroupDto } from './dto/update-group.dto';
import { AddParticipantsDto } from './dto/add-participants.dto';
import { CreateDirectDto } from './dto/create-direct.dto';

@Controller('conversations')
export class ConversationsController {
  constructor(private conversationsService: ConversationsService) {}

  @UseGuards(JwtAuthGuard)
  @Get()
  getMyConversations(@Request() req: AuthenticatedRequest) {
    return this.conversationsService.getConversationsForUser(req.user.userId);
  }

  @UseGuards(JwtAuthGuard)
  @Post('group')
  createGroup(@Body() body: CreateGroupDto, @Request() req: AuthenticatedRequest) {
    return this.conversationsService.createGroup(
      req.user.userId,
      body.title,
      body.participantIds,
    );
  }

  @UseGuards(JwtAuthGuard)
  @Patch(':id/title')
  updateGroupTitle(@Param('id') id: string, @Body() body: UpdateGroupDto, @Request() req: AuthenticatedRequest) {
    return this.conversationsService.updateGroupTitle(req.user.userId, id, body.title);
  }

  @UseGuards(JwtAuthGuard)
  @Post(':id/participants')
  addGroupParticipants(@Param('id') id: string, @Body() body: AddParticipantsDto, @Request() req: AuthenticatedRequest) {
    return this.conversationsService.addGroupParticipants(req.user.userId, id, body.participantIds);
  }

  @UseGuards(JwtAuthGuard)
  @Post('direct')
  createDirectConversation(@Body() body: CreateDirectDto, @Request() req: AuthenticatedRequest) {
    return this.conversationsService.createDirectConversation(req.user.userId, body.targetUserId);
  }
}
