import { Controller, Get } from '@nestjs/common';
import { MssKafkaService } from '../kafka/mss-kafka.service';
import { CassandraService } from '../messages/cassandra.service';
import { ParticipantCacheService } from '../participants/participant-cache.service';

// Per-dependency probe timeout; a hung dependency must not hang /health.
const PROBE_TIMEOUT_MS = 2000;
// Overall budget so the concurrent probes can't stall past ~2.5s.
const OVERALL_TIMEOUT_MS = 2500;

@Controller('health')
export class HealthController {
  constructor(
    private readonly kafkaService: MssKafkaService,
    private readonly cassandraService: CassandraService,
    private readonly participantCacheService: ParticipantCacheService,
  ) {}

  @Get()
  async check(): Promise<{
    status: 'ok' | 'degraded';
    service: string;
    cassandra: boolean;
    redis: boolean;
    kafka: boolean;
  }> {
    // Each probe resolves to a boolean; a 2s timeout turns a hung dependency into
    // false. The timeout timer is cleared once the race settles so it can't leak.
    const probe = (probePromise: Promise<boolean>): Promise<boolean> => {
      let timer: NodeJS.Timeout;
      return Promise.race([
        probePromise,
        new Promise<boolean>((_, reject) => {
          timer = setTimeout(() => reject(new Error('probe timed out')), PROBE_TIMEOUT_MS);
        }),
      ]).finally(() => clearTimeout(timer));
    };

    // kafkaService.isAvailable is a synchronous boolean read — it can't hang, so
    // it doesn't need (or benefit from) the timeout race above.
    const allProbes = Promise.allSettled([
      Promise.resolve(this.kafkaService.isAvailable),
      probe(this.cassandraService.isHealthy()),
      probe(this.participantCacheService.isHealthy()),
    ]).then((results) =>
      results.map((settlement) => settlement.status === 'fulfilled' && settlement.value === true),
    );

    // If the overall budget elapses, report every dependency down rather than hang
    // the request. Clear the budget timer once the race settles.
    let overallTimer: NodeJS.Timeout;
    const settled = await Promise.race([
      allProbes,
      new Promise<boolean[]>((resolve) => {
        overallTimer = setTimeout(() => resolve([false, false, false]), OVERALL_TIMEOUT_MS);
      }),
    ]).finally(() => clearTimeout(overallTimer));
    const [kafka, cassandra, redis] = settled;

    return {
      status: kafka && cassandra && redis ? 'ok' : 'degraded',
      service: 'mss',
      cassandra,
      redis,
      kafka,
    };
  }
}
