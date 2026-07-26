import { Controller, Get, Post, Body } from '@nestjs/common';
// Notice the "type" keyword here. This tells TypeScript it's only for compile-time checking.
import type { SendMessagePayload, UserAuthResponse } from '@chat/shared-types';

@Controller()
export class AppController {
  @Post('message')
  handleMessage(@Body() payload: SendMessagePayload): UserAuthResponse {
    return {
      userId: 'user-123',
      token: 'mock-jwt-token',
    };
  }

  @Get()
  getHello(): string {
    return 'API is running!';
  }
}
