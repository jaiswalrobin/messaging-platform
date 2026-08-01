import { Controller, Get } from '@nestjs/common';

@Controller('health')
export class HealthController {
  @Get()
  check(): { status: string; uptime: number; service: string } {
    return { status: 'ok', uptime: process.uptime(), service: 'chat-gateway' };
  }
}
