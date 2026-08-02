import { Controller, Get } from '@nestjs/common';
import { InjectDataSource } from '@nestjs/typeorm';
import { DataSource } from 'typeorm';

@Controller('health')
export class HealthController {
  constructor(
    @InjectDataSource()
    private readonly dataSource: DataSource,
  ) {}

  @Get()
  async check() {
    let postgres = false;
    try {
      // Probe Postgres with a ~2s budget so a hung DB can't wedge the check.
      await Promise.race([
        this.dataSource.query('SELECT 1'),
        new Promise((_, reject) =>
          setTimeout(() => reject(new Error('timeout')), 2000),
        ),
      ]);
      postgres = true;
    } catch {
      // probe failed — `postgres` already initialized to false
    }

    return {
      status: postgres ? 'ok' : 'degraded',
      service: 'api',
      uptime: process.uptime(),
      postgres,
    };
  }
}
