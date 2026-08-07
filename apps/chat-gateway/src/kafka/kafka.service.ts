import { Injectable, Logger, OnModuleInit, OnModuleDestroy } from '@nestjs/common';
import { Kafka, Producer, Admin, logLevel } from 'kafkajs';
import type { KafkaChatEvent } from '@chat/shared-types';

export const KAFKA_CHAT_TOPIC = 'chat-events';
export const KAFKA_CHAT_DLQ_TOPIC = 'chat-events-dlq';

// Reconnect cadence after a failed boot connect (Issue 19)
const RETRY_INTERVAL_MS = 15000;

/**
 * Producer-only Kafka service. The gateway publishes chat events; the consumer
 * (MSS) role lives in the separate `mss` service. Topic creation stays here —
 * mss's consumer needs chat-events / chat-events-dlq to exist, and the gateway
 * creates them at boot.
 */
@Injectable()
export class KafkaService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(KafkaService.name);
  private kafka: Kafka;
  private producer: Producer;
  private admin: Admin;
  private available = false;

  // Set true once on shutdown so a pending reconnect can't resurrect the producer.
  private destroyed = false;

  // Broker list retained for reconnect logging
  private brokers: string[];

  // Issue 19: bounded boot-time reconnect state. The step flags gate each
  // connectKafka run so a retry never re-connects an already-initialized
  // producer or admin (kafkajs throws on connect() twice).
  private attempts = 0;
  private retryTimer?: NodeJS.Timeout;
  private adminConnected = false;
  private producerConnected = false;

  async onModuleInit(): Promise<void> {
    this.brokers = (process.env.KAFKA_BROKERS ?? 'localhost:9092')
      .split(',')
      .map((b) => b.trim())
      .filter(Boolean);

    this.kafka = new Kafka({
      clientId: 'chat-gateway',
      brokers: this.brokers,
      logLevel: logLevel.WARN,
      // Retry / timeout tuning so startup does not block server if Kafka is down
      retry: { retries: 3, initialRetryTime: 300 },
    });

    this.admin = this.kafka.admin();
    // Idempotent producer: broker-level dedup (PID + sequence) so producer
    // retries can't duplicate events. kafkajs forces acks=all internally when
    // idempotent; maxInFlightRequests=1 is required by idempotence. The
    // Kafka-level retry config above still applies (bounded — kafkajs warns
    // "Limiting retries for the idempotent producer", which is intentional so
    // boot stays fail-soft).
    this.producer = this.kafka.producer({
      idempotent: true,
      maxInFlightRequests: 1,
    });

    // Runtime resilience: if the broker dies, flip `available` false and re-arm
    // the reconnect loop so /health stops lying and the producer can come back
    // without a restart. With the consumer removed (SRP split), the producer's
    // network events are the only broker-death signal (kafkajs Producer has no
    // `producer.error` event).
    this.producer.on('producer.connect', () => this.logger.log('✅ Kafka producer connected'));
    this.producer.on('producer.disconnect', () => {
      this.logger.warn('⚠️ Kafka producer disconnected');
      this.handleRuntimeDisconnect();
    });

    // Issue 19: reconnect on boot failure. connectKafkaWithRetry re-runs the
    // full sequence every 15s until it succeeds, then flips available and stops.
    await this.connectKafkaWithRetry();
  }

  /**
   * Bounded boot-time reconnect. Never throws — on failure the gateway keeps
   * serving in direct-delivery mode while a 15s timer retries the connect.
   */
  private async connectKafkaWithRetry(): Promise<void> {
    // Shutdown safety: never resurrect the producer after destroy.
    if (this.destroyed) return;
    try {
      await this.connectKafka();
      if (this.retryTimer) {
        clearTimeout(this.retryTimer);
        this.retryTimer = undefined;
      }
      this.available = true;
      this.logger.log(`✅ Kafka connected — broker(s): ${this.brokers.join(', ')}`);
    } catch (err) {
      this.attempts++;
      this.logger.error(
        `❌ Kafka connect attempt ${this.attempts} failed (${(err as Error).message}) — retrying in ${RETRY_INTERVAL_MS / 1000}s`,
      );
      this.armReconnect();
    }
  }

  /**
   * Called when a runtime producer disconnect flips the broker to unavailable.
   * Resets the runtime connection flags so a reconnect re-establishes the live
   * producer instead of skipping to a no-op "available", then re-arms the
   * bounded reconnect loop (guarded so it never stacks).
   */
  private handleRuntimeDisconnect(): void {
    if (this.destroyed || this.available === false) {
      // Already down / a reconnect is in flight — don't stack.
      return;
    }
    this.available = false;
    this.logger.warn('⚠️ Kafka lost connection — reconnecting…');

    // Reset the connect-sequence gates so a retry actually reconnects. Topics
    // already exist, so admin stays connected (recreate is idempotent anyway).
    this.producerConnected = false;

    this.armReconnect();
  }

  /** (Re)arm the 15s reconnect timer once; a pending timer is never doubled up. */
  private armReconnect(): void {
    if (this.destroyed || this.retryTimer) return;
    this.retryTimer = setTimeout(() => {
      this.retryTimer = undefined;
      void this.connectKafkaWithRetry();
    }, RETRY_INTERVAL_MS);
  }

  /**
   * Connect + init topics + connect the producer. Each step is gated by a flag
   * so the sequence is idempotent across retries: a retry after a partial
   * success skips already-completed steps (kafkajs throws if connect() is
   * called twice on the same instance).
   */
  private async connectKafka(): Promise<void> {
    if (!this.adminConnected) {
      // Ensure topics exist
      await this.admin.connect();
      await this.admin.createTopics({
        waitForLeaders: true,
        topics: [
          {
            topic: KAFKA_CHAT_TOPIC,
            numPartitions: 4,
            replicationFactor: 1,
          },
          {
            topic: KAFKA_CHAT_DLQ_TOPIC,
            numPartitions: 4,
            replicationFactor: 1,
          },
        ],
      });
      await this.admin.disconnect();
      this.adminConnected = true;
    }

    if (!this.producerConnected) {
      await this.producer.connect();
      this.producerConnected = true;
    }
  }

  async onModuleDestroy(): Promise<void> {
    this.destroyed = true;
    if (this.retryTimer) {
      clearTimeout(this.retryTimer);
      this.retryTimer = undefined;
    }
    try {
      await this.producer?.disconnect();
      await this.admin?.disconnect();
    } catch {
      // ignore errors on shutdown
    }
  }

  /**
   * Public read of the available flag (used by the health controller).
   */
  get isAvailable(): boolean {
    return this.available;
  }

  /**
   * Publish a typed event to the chat-events topic.
   * Returns false if Kafka is unavailable (fallback mode).
   */
  async publish(event: KafkaChatEvent): Promise<boolean> {
    if (!this.available) return false;
    try {
      await this.producer.send({
        topic: KAFKA_CHAT_TOPIC,
        messages: [
          {
            key: event.conversationId,
            value: JSON.stringify(event),
          },
        ],
      });
      return true;
    } catch (err) {
      this.logger.error('❌ Kafka publish failed', err);
      return false;
    }
  }
}
