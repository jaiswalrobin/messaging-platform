import { Controller, Get } from '@nestjs/common';
import { InjectDataSource } from '@nestjs/typeorm';
import { DataSource } from 'typeorm';
import { KafkaService } from '../kafka/kafka.service';
import { CassandraService } from '../messages/cassandra.service';
import { ParticipantCacheService } from '../participants/participant-cache.service';

// Per-dependency probe timeout; a hung dependency must not hang /health.
const PROBE_TIMEOUT_MS = 2000;
// Overall budget so the four concurrent probes can't stall past ~2.5s.
const OVERALL_TIMEOUT_MS = 2500;

@Controller('health')
export class HealthController {
  constructor(
    private readonly kafkaService: KafkaService,
    private readonly cassandraService: CassandraService,
    private readonly participantCacheService: ParticipantCacheService,
    @InjectDataSource() private readonly dataSource: DataSource,
  ) {}

  @Get()
  async check(): Promise<{
    status: 'ok' | 'degraded';
    service: string;
    uptime: number;
    kafkaAvailable: boolean;
    cassandra: boolean;
    redis: boolean;
    postgres: boolean;
  }> {
    // Each probe resolves to a boolean; a 2s timeout turns a hung dependency into false.
    const probe = (check: Promise<boolean>): Promise<boolean> =>
      Promise.race([
        check,
        new Promise<boolean>((_, reject) =>
          setTimeout(() => reject(new Error('probe timed out')), PROBE_TIMEOUT_MS),
        ),
      ]);

    const allProbes = Promise.allSettled([
      probe(Promise.resolve(this.kafkaService.isAvailable)),
      probe(this.cassandraService.isHealthy()),
      probe(this.participantCacheService.isHealthy()),
      probe(this.dataSource.query('SELECT 1').then(() => true)),
    ]).then((results) => results.map((r) => r.status === 'fulfilled' && r.value === true));

    // If the overall budget elapses, report every dependency down rather than hang the request.
    const [kafka, cassandra, redis, postgres] = await Promise.race([
      allProbes,
      new Promise<boolean[]>((resolve) =>
        setTimeout(() => resolve([false, false, false, false]), OVERALL_TIMEOUT_MS),
      ),
    ]);

    return {
      status: kafka && cassandra && redis && postgres ? 'ok' : 'degraded',
      service: 'chat-gateway',
      uptime: process.uptime(),
      kafkaAvailable: kafka,
      cassandra,
      redis,
      postgres,
    };
  }
}
