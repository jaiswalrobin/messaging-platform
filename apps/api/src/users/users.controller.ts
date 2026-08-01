import { Controller, Get, Query, UseGuards, Request } from '@nestjs/common';
import { UsersService } from './users.service';
import { JwtAuthGuard } from '../auth/jwt-auth.guard';
import type { AuthenticatedRequest } from '../auth/request-user';

@Controller('users')
export class UsersController {
  constructor(private usersService: UsersService) {}

  @UseGuards(JwtAuthGuard)
  @Get('search')
  searchUsers(@Query('q') query: string, @Request() req: AuthenticatedRequest) {
    return this.usersService.searchUsers(query, req.user.userId);
  }
}
