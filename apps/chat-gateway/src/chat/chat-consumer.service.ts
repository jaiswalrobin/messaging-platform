import { Injectable, Logger } from '@nestjs/common';
import type {
  KafkaChatEvent,
  KafkaMessageSentPayload,
  KafkaMessageDeliveredPayload,
  KafkaMessageReadPayload,
} from '@chat/shared-types';
import { CassandraService } from '../messages/cassandra.service';
import { ApiClientService } from '../internal/api-client.service';
import { ConnectionRegistryService } from './connection-registry.service';

/**
 * MSS — Message Storage Service (the gateway's Kafka consumer role).
 *
 * In this single-node deployment the gateway is both producer and consumer:
 * ChatGateway publishes events to Kafka, this service consumes them — persisting
 * to Cassandra (durable source of truth) and routing frames to online clients
 * through the connection registry. The KafkaService's "never rethrow" bounded
 * retry wraps handleKafkaEvent, so failures here never crash the consumer.
 */
@Injectable()
export class ChatConsumerService {
  private readonly logger = new Logger(ChatConsumerService.name);

  constructor(
    private readonly cassandraService: CassandraService,
    private readonly apiClient: ApiClientService,
    private readonly registry: ConnectionRegistryService,
  ) {}

  async handleKafkaEvent(event: KafkaChatEvent): Promise<void> {
    switch (event.type) {
      case 'MESSAGE_SENT':
        await this.onKafkaMessageSent(event);
        break;
      case 'MESSAGE_DELIVERED':
        await this.onKafkaMessageDelivered(event);
        break;
      case 'MESSAGE_READ':
        await this.onKafkaMessageRead(event);
        break;
      default:
        // Unknown event type (e.g. a future event produced by a newer node).
        // Log loudly but never rethrow — the consumer must not crash on events
        // it doesn't understand.
        this.logger.error(`❌ Unknown Kafka event type: ${(event as { type: string }).type}`);
    }
  }

  /** MSS: Persist message to Cassandra, route to online recipients; offline recipients are dropped (no queue yet). */
  private async onKafkaMessageSent(event: KafkaMessageSentPayload): Promise<void> {
    // 1. Persist to Cassandra (durable source of truth)
    await this.cassandraService.saveMessage(
      event.conversationId,
      event.senderId,
      event.content,
      event.messageId,
      new Date(event.createdAt),
    );
    this.logger.log(`💾 Persisted message ${event.messageId} to Cassandra`);

    // 2. Route to online recipients (all devices of each)
    await this.registry.broadcastToParticipants(
      event.conversationId,
      event.senderId,
      'message_received',
      {
        messageId: event.messageId,
        conversationId: event.conversationId,
        senderId: event.senderId,
        content: event.content,
        createdAt: event.createdAt,
      },
    );
  }

  /** MSS: Persist delivery receipt, route double grey tick to sender. */
  private async onKafkaMessageDelivered(event: KafkaMessageDeliveredPayload): Promise<void> {
    // Dedupe: a recipient's second device (or a re-delivered event) must not
    // re-broadcast the receipt. Read-before-write; the worst-case race
    // re-broadcasts once, which the FE's upgrade-only handler dedupes anyway.
    // Skip on ANY existing receipt row for this (message, user) — the row's
    // status may already have been overwritten to 'read' by markConversationRead
    // (monotone statuses, one row per message per user), so a delivered-only
    // check would let a late duplicate ack through.
    const existing = await this.cassandraService.getReceipts(
      event.conversationId,
      event.messageId,
    );
    if (existing.some((r) => r.userId === event.recipientId)) {
      this.logger.log(`📬 Duplicate delivery ACK from ${event.recipientId} — skipped`);
      return;
    }

    // Persist delivery receipt to Cassandra
    await this.cassandraService.upsertReceipt(
      event.conversationId,
      event.messageId,
      event.recipientId,
      'delivered',
    );

    // Route status update to all participants except the acker (sender included).
    // The FE upgrades only its own outgoing messages, matched by messageId.
    await this.registry.broadcastToParticipants(
      event.conversationId,
      event.recipientId,
      'message_delivered',
      {
        messageId: event.messageId,
        conversationId: event.conversationId,
        recipientId: event.recipientId,
        deliveredAt: event.deliveredAt,
        status: 'delivered',
      },
    );
  }

  /** MSS: Persist read receipt, route double blue tick to sender. */
  private async onKafkaMessageRead(event: KafkaMessageReadPayload): Promise<void> {
    // Persist read receipt to Cassandra (gateway-owned durable copy)
    await this.cassandraService.markConversationRead(
      event.conversationId,
      event.readerId,
      event.lastReadMessageId,
    );
    this.logger.log(`👁️  Read receipt stored — ${event.readerId} read up to ${event.lastReadMessageId}`);

    // Also advance the api's Postgres watermark (best-effort, dormant machinery).
    // This is what powers reload hydration: `GET /conversations` exposes
    // `lastReadMessageId` per participant, and the FE recomputes blue ticks from
    // it. A null verdict (api unreachable) still relays the live signal below.
    const verdict = await this.apiClient.markRead(
      event.conversationId,
      event.readerId,
      event.lastReadMessageId,
    );

    // Only broadcast when the watermark actually moved forward — a stale or
    // replayed receipt (advanced: false) must not flip ticks backward.
    if (verdict?.advanced === false) {
      return;
    }

    // Route blue tick to all OTHER participants (senders)
    await this.registry.broadcastToParticipants(
      event.conversationId,
      event.readerId,
      'message_read',
      {
        conversationId: event.conversationId,
        lastReadMessageId: event.lastReadMessageId,
        readerId: event.readerId,
        readAt: event.readAt,
        status: 'read',
      },
    );
  }
}
