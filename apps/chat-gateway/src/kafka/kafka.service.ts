import { Injectable, Logger, OnModuleInit, OnModuleDestroy } from '@nestjs/common';
import {
  Kafka,
  Producer,
  Consumer,
  Admin,
  logLevel,
  KafkaMessage,
} from 'kafkajs';
import type { KafkaChatEvent } from '@chat/shared-types';

export const KAFKA_CHAT_TOPIC = 'chat-events';

@Injectable()
export class KafkaService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(KafkaService.name);
  private kafka: Kafka;
  private producer: Producer;
  private consumer: Consumer;
  private admin: Admin;
  private available = false;

  // Callbacks registered by other services to consume events
  private handlers: Array<(event: KafkaChatEvent) => Promise<void>> = [];

  async onModuleInit(): Promise<void> {
    const brokers = (process.env.KAFKA_BROKERS ?? 'localhost:9092').split(',');

    this.kafka = new Kafka({
      clientId: 'chat-gateway',
      brokers,
      logLevel: logLevel.WARN,
      // Retry / timeout tuning so startup does not block server if Kafka is down
      retry: { retries: 3, initialRetryTime: 300 },
    });

    this.admin = this.kafka.admin();
    this.producer = this.kafka.producer();
    this.consumer = this.kafka.consumer({ groupId: 'chat-gateway-group' });

    try {
      // Ensure topic exists
      await this.admin.connect();
      await this.admin.createTopics({
        waitForLeaders: true,
        topics: [
          {
            topic: KAFKA_CHAT_TOPIC,
            numPartitions: 4,
            replicationFactor: 1,
          },
        ],
      });
      await this.admin.disconnect();

      await this.producer.connect();
      await this.consumer.connect();
      await this.consumer.subscribe({ topic: KAFKA_CHAT_TOPIC, fromBeginning: false });

      // Start consuming in background
      await this.consumer.run({
        eachMessage: async ({ message }: { message: KafkaMessage }) => {
          if (!message.value) return;
          try {
            const event: KafkaChatEvent = JSON.parse(message.value.toString());
            await Promise.all(this.handlers.map((h) => h(event)));
          } catch (err) {
            this.logger.error('❌ Failed to process Kafka event', err);
          }
        },
      });

      this.available = true;
      this.logger.log(`✅ Kafka connected — broker(s): ${brokers.join(', ')}`);
    } catch (err) {
      // Non-fatal: chat gateway continues in direct-delivery mode
      this.logger.warn(`⚠️  Kafka unavailable (${(err as Error).message}) — falling back to direct delivery`);
    }
  }

  async onModuleDestroy(): Promise<void> {
    try {
      await this.producer?.disconnect();
      await this.consumer?.disconnect();
    } catch {
      // ignore errors on shutdown
    }
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

  /**
   * Register a handler to be called for every consumed Kafka event.
   */
  onEvent(handler: (event: KafkaChatEvent) => Promise<void>): void {
    this.handlers.push(handler);
  }

  get isAvailable(): boolean {
    return this.available;
  }
}
