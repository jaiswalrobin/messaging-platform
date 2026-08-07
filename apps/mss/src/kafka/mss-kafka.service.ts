import { Injectable, Logger, OnModuleInit, OnModuleDestroy } from '@nestjs/common';
import { Kafka, Consumer, Producer, logLevel, KafkaMessage } from 'kafkajs';
import type { KafkaChatEvent } from '@chat/shared-types';

export const KAFKA_CHAT_TOPIC = 'chat-events';
export const KAFKA_CHAT_DLQ_TOPIC = 'chat-events-dlq';

// Bounded processing attempts per event before it is skipped loudly and published to DLQ; see the
// "never rethrow" comment block in connectKafka below.
const MAX_ATTEMPTS = 3;

// Reconnect cadence after a failed boot connect (Issue 19)
const RETRY_INTERVAL_MS = 15000;

/**
 * Consumer-only Kafka service for MSS (the gateway's producer-side KafkaService
 * was trimmed: no producer, no admin/topic creation — chat-events and
 * chat-events-dlq are created by the producing gateway). The only write this
 * service ever makes to Kafka is the DLQ, via a lazily-created producer on the
 * skip path.
 */
@Injectable()
export class MssKafkaService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(MssKafkaService.name);
  private kafka: Kafka;
  private consumer: Consumer;
  private dlqProducer?: Producer;
  private available = false;

  // Set true once on shutdown so a pending reconnect can't resurrect the consumer.
  private destroyed = false;

  // Broker list retained for reconnect logging
  private brokers: string[];

  // Issue 19: bounded boot-time reconnect state. The step flags gate each
  // connectKafka run so a retry never re-subscribes or re-runs an
  // already-initialized consumer (kafkajs throws on subscribe()/run() twice).
  private attempts = 0;
  private retryTimer?: NodeJS.Timeout;
  private consumerConnected = false;
  private subscribed = false;
  private running = false;

  // Callbacks registered by other services to consume events
  private handlers: Array<(event: KafkaChatEvent) => Promise<void>> = [];

  // Callbacks invoked on the final-skip path (after MAX_ATTEMPTS + DLQ publish)
  private skipHandlers: Array<(event: KafkaChatEvent) => Promise<void>> = [];

  async onModuleInit(): Promise<void> {
    this.brokers = (process.env.KAFKA_BROKERS ?? 'localhost:9092')
      .split(',')
      .map((b) => b.trim())
      .filter(Boolean);

    this.kafka = new Kafka({
      clientId: 'mss',
      brokers: this.brokers,
      logLevel: logLevel.WARN,
      // Retry / timeout tuning so startup does not block server if Kafka is down
      retry: { retries: 3, initialRetryTime: 300 },
    });

    this.consumer = this.kafka.consumer({ groupId: 'mss-group' });

    // Runtime resilience: `available` tracks kafkajs's own connection lifecycle
    // so /health stops lying. Recovery is owned by kafkajs's internal restart —
    // after a retriable crash it auto-restarts the consumer and re-emits
    // 'consumer.connect' on reconnect, which flips `available` back on. We must
    // NOT re-arm the external boot-time reconnect loop from the runtime-crash
    // path: it would call subscribe()/run() on the internally-restarted
    // consumer and throw KafkaJSNonRetriableError('Cannot subscribe to topic
    // while consumer is running') forever.
    this.consumer.on('consumer.connect', () => {
      this.available = true;
      this.logger.log('✅ Kafka consumer connected');
    });
    this.consumer.on('consumer.disconnect', () => {
      this.available = false;
      this.logger.warn('⚠️ Kafka consumer disconnected');
    });
    this.consumer.on('consumer.crash', (event) => {
      this.logger.error(
        `❌ Kafka consumer crashed: ${(event as { error?: Error })?.error?.message ?? JSON.stringify(event)}`,
      );
      this.handleRuntimeDisconnect();
    });

    // Issue 19: reconnect on boot failure. connectKafkaWithRetry re-runs the
    // full sequence every 15s until it succeeds, then flips available and stops.
    await this.connectKafkaWithRetry();
  }

  /**
   * Bounded boot-time reconnect. Never throws — on failure MSS keeps running
   * (persist/receipt logic idles) while a 15s timer retries the connect.
   */
  private async connectKafkaWithRetry(): Promise<void> {
    // Shutdown safety: never resurrect the consumer after destroy.
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
   * Called when a runtime consumer crash flips the broker to unavailable.
   * Marks the service down and lets kafkajs's internal restart own recovery:
   * after a retriable crash the consumer restarts itself and re-emits
   * 'consumer.connect' on reconnect, which flips `available` back on. Nothing
   * is re-armed here on purpose — re-running connectKafka would call
   * subscribe()/run() on the internally-restarted consumer and throw
   * KafkaJSNonRetriableError('Cannot subscribe to topic while consumer is
   * running'). If the crash is non-retriable the consumer stays down and
   * /health honestly reports kafka: false until the service is restarted.
   */
  private handleRuntimeDisconnect(): void {
    if (this.destroyed) return;
    this.available = false;
    this.logger.warn('⚠️ Kafka lost connection — waiting for internal consumer restart…');
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
   * Connect + subscribe + start consuming. Each step is gated by a flag so the
   * sequence is idempotent across retries: a retry after a partial success
   * skips already-completed steps (kafkajs throws if consumer.subscribe() or
   * run() is called twice on the same instance).
   */
  private async connectKafka(): Promise<void> {
    if (!this.consumerConnected) {
      await this.consumer.connect();
      this.consumerConnected = true;
    }

    if (!this.subscribed) {
      await this.consumer.subscribe({ topic: KAFKA_CHAT_TOPIC, fromBeginning: false });
      this.subscribed = true;
    }

    if (!this.running) {
      // Start consuming in background.
      //
      // Bounded blocking retry, NEVER rethrow: rethrowing drives kafkajs's
      // batch retrier → KafkaJSNumberOfRetriesExceeded → the consumer crashes
      // (the retry counter never resets, kafkajs #1592). Blocking sleeps
      // preserve per-partition ordering (the same partition stalls while we
      // retry) and stay well under the 30s session timeout. After 3 attempts
      // the event is published to DLQ (chat-events-dlq) before skipping — never
      // a silent drop.
      await this.consumer.run({
        eachMessage: async ({ message }: { message: KafkaMessage }) => {
          if (!message.value) return;

          let event: KafkaChatEvent;
          try {
            event = JSON.parse(message.value.toString()) as KafkaChatEvent;
          } catch (err) {
            this.logger.error('❌ Malformed Kafka message — skipping', err);
            return;
          }

          for (let attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
            // Run handlers independently: a throw in one must not re-run the
            // already-succeeded handlers nor re-throw into the batch retrier.
            let failed = false;
            for (const handler of this.handlers) {
              try {
                await handler(event);
              } catch (err) {
                failed = true;
                this.logger.error(
                  `❌ Kafka event ${event.type} handler failed (attempt ${attempt}/${MAX_ATTEMPTS}): ${(err as Error).message}`,
                );
              }
            }
            if (!failed) return;
            if (attempt < MAX_ATTEMPTS) {
              await new Promise((r) => setTimeout(r, 500 * attempt));
            }
          }

          this.logger.error(
            `⚠️  Giving up on ${event.type} after ${MAX_ATTEMPTS} attempts — publishing to DLQ (${KAFKA_CHAT_DLQ_TOPIC})`,
          );
          try {
            await this.publishToDlq(event);
            this.logger.log(`📥 Event ${event.type} published to DLQ (${KAFKA_CHAT_DLQ_TOPIC})`);
          } catch (dlqErr) {
            this.logger.error(
              `❌ Failed to publish event ${event.type} to DLQ (${KAFKA_CHAT_DLQ_TOPIC}): ${(dlqErr as Error).message}`,
            );
          }

          // Notify registered skip handlers (e.g. surface PERSIST_FAILED to the
          // affected sender). A throwing handler must never break the consumer.
          for (const skipHandler of this.skipHandlers) {
            try {
              await skipHandler(event);
            } catch (skipErr) {
              this.logger.error(
                `❌ onEventSkipped handler failed for ${event.type}: ${(skipErr as Error).message}`,
              );
            }
          }
        },
      });
      this.running = true;
    }
  }

  /**
   * Publish an event to the DLQ. The DLQ producer is created and connected
   * lazily on first use so boot stays fail-soft — this service is consumer-only
   * and never produces to chat-events. kafkajs 2.2.4 throws
   * KafkaJSError('The producer is disconnected') from send() on a producer that
   * was never connected, so connect() is awaited before the first send; the
   * connected instance is cached and later publishes just send() (kafkajs
   * auto-reconnects on send after a previous connection). A failed connect
   * leaves the cache empty so the next publish tries a fresh connect; the
   * caller wraps this call so a failure is logged loudly and the skip-handlers
   * still run.
   */
  private async publishToDlq(event: KafkaChatEvent): Promise<void> {
    if (!this.dlqProducer) {
      const producer = this.kafka.producer();
      await producer.connect();
      this.dlqProducer = producer;
    }
    await this.dlqProducer.send({
      topic: KAFKA_CHAT_DLQ_TOPIC,
      messages: [
        {
          key: event.conversationId,
          value: JSON.stringify(event),
        },
      ],
    });
  }

  async onModuleDestroy(): Promise<void> {
    this.destroyed = true;
    this.handlers = [];
    this.skipHandlers = [];
    if (this.retryTimer) {
      clearTimeout(this.retryTimer);
      this.retryTimer = undefined;
    }
    try {
      await this.dlqProducer?.disconnect();
      await this.consumer?.disconnect();
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
   * Register a handler to be called for every consumed Kafka event.
   */
  onEvent(handler: (event: KafkaChatEvent) => Promise<void>): void {
    this.handlers.push(handler);
  }

  /**
   * Register a handler invoked on the final-skip path — an event that exhausted
   * MAX_ATTEMPTS and was published to the DLQ. Invoked in a try/catch so a
   * throwing handler can never break the consumer.
   */
  onEventSkipped(handler: (event: KafkaChatEvent) => Promise<void>): void {
    this.skipHandlers.push(handler);
  }
}
