import { Injectable, Logger, OnModuleInit } from '@nestjs/common';
import type {
  KafkaChatEvent,
  KafkaMessageSentPayload,
  KafkaMessageDeliveredPayload,
  KafkaMessageReadPayload,
} from '@chat/shared-types';
import { CassandraService } from '../messages/cassandra.service';
import { ApiClientService } from '../internal/api-client.service';
import { ParticipantCacheService } from '../participants/participant-cache.service';
import { DeliveryPublisherService } from './delivery-publisher.service';
import { MssKafkaService } from '../kafka/mss-kafka.service';

/**
 * MSS — Message Storage Service (the gateway's Kafka consumer role, extracted
 * into its own service by the SRP split).
 *
 * Consumes chat-events from Kafka: persists to Cassandra (durable source of
 * truth), resolves recipients via the participant cache, and routes frames to
 * online clients through the DeliveryPublisher (shared-registry pub/sub:
 * registry lookup + targeted per-node channels — at-most-once, the FE backfill
 * reconciles missed frames). The MssKafkaService's "never rethrow" bounded
 * retry wraps handleKafkaEvent, so failures here never crash the consumer.
 */
@Injectable()
export class ChatConsumerService implements OnModuleInit {
  private readonly logger = new Logger(ChatConsumerService.name);

  constructor(
    private readonly cassandraService: CassandraService,
    private readonly apiClient: ApiClientService,
    private readonly participantCache: ParticipantCacheService,
    private readonly deliveryPublisher: DeliveryPublisherService,
    private readonly kafkaService: MssKafkaService,
  ) {}

  onModuleInit(): void {
    // Register the consume handler for every Kafka event (the gateway's
    // producer-side ChatGateway used to do this; in the split this service is
    // the sole consumer, so it registers its own onEvent handler). NOTE: a
    // second onEvent registration would double-persist every message.
    this.kafkaService.onEvent((event) => this.handleKafkaEvent(event));
    // Register the final-skip handler: fires only for events the consumer gave
    // up on (after MAX_ATTEMPTS + DLQ publish).
    this.kafkaService.onEventSkipped((event) => this.onEventSkipped(event));
  }

  /**
   * Final-skip callback: an event exhausted the consumer's retries and was
   * published to the DLQ. For MESSAGE_SENT, surface PERSIST_FAILED to the
   * sender so the FE fails exactly the optimistic row it inserted (matched by
   * clientMessageId). Read/delivered receipts are soft state — warn only.
   */
  private async onEventSkipped(event: KafkaChatEvent): Promise<void> {
    if (event.type === 'MESSAGE_SENT') {
      await this.deliveryPublisher.publishToUser(
        event.senderId,
        'error',
        {
          code: 'PERSIST_FAILED',
          message: 'Message could not be saved',
          clientMessageId: event.clientMessageId,
          conversationId: event.conversationId,
        },
      );
      return;
    }
    this.logger.warn(
      `⚠️  Kafka event ${event.type} skipped after exhausting retries — soft state, not surfaced to clients`,
    );
  }

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

  /**
   * All conversation participants except `excludeUserId` — mirrors the
   * filtering chat-gateway's broadcastToParticipants applied before fan-out.
   */
  private async participantsExcept(
    conversationId: string,
    excludeUserId: string,
  ): Promise<string[]> {
    const participantIds = await this.participantCache.getParticipants(conversationId);
    return participantIds.filter((id) => id !== excludeUserId);
  }

  /** MSS: Persist message to Cassandra, route to online recipients; offline recipients are dropped (no queue yet). */
  private async onKafkaMessageSent(event: KafkaMessageSentPayload): Promise<void> {
    // 1. Persist to Cassandra (durable source of truth). This is the ONLY stage
    //    whose failure means "the message was not saved", so it is left uncaught:
    //    a throw here drives the consumer retry → DLQ → PERSIST_FAILED to the
    //    sender, which is the correct signal (the FE fails the optimistic row).
    await this.cassandraService.saveMessage(
      event.conversationId,
      event.senderId,
      event.content,
      event.messageId,
      new Date(event.createdAt),
      event.clientMessageId,
    );
    this.logger.log(`💾 Persisted message ${event.messageId} to Cassandra`);

    // 2. Route to online recipients (all devices of each). Best-effort: a failure
    //    here does NOT mean the message was lost — it is durable in Cassandra and
    //    history, so undelivered recipients recover it on reload. Never rethrow:
    //    rethrowing would drive the retry → DLQ → PERSIST_FAILED path and tell the
    //    sender "could not be saved" for a message that WAS saved (→ user resends
    //    → duplicate).
    try {
      const recipientIds = await this.participantsExcept(event.conversationId, event.senderId);
      await this.deliveryPublisher.publishToUsers(recipientIds, 'message_received', {
        messageId: event.messageId,
        conversationId: event.conversationId,
        senderId: event.senderId,
        content: event.content,
        createdAt: event.createdAt,
      });
    } catch (err) {
      this.logger.error(
        `⚠️  Recipient fan-out failed for ${event.messageId} (message already persisted): ${(err as Error).message}`,
      );
    }

    // 3. Also route to the sender's OTHER devices, excluding the origin socket
    // — it already got `message_sent`, and a second frame would duplicate the
    // optimistic row the FE inserted. Best-effort for the same reason as the
    // recipient fan-out: the message is already durable, so a routing failure
    // must not rethrow into the retry → DLQ → PERSIST_FAILED path (the message
    // WAS saved; surfacing PERSIST_FAILED would make the user resend → duplicate).
    try {
      await this.deliveryPublisher.publishToUser(
        event.senderId,
        'message_received',
        {
          messageId: event.messageId,
          conversationId: event.conversationId,
          senderId: event.senderId,
          content: event.content,
          createdAt: event.createdAt,
        },
        event.senderSocketId, // may be undefined for legacy events → sends to all sender sockets; FE dedupes by messageId
      );
    } catch (err) {
      this.logger.error(
        `⚠️  Sender fan-out failed for ${event.messageId} (message already persisted): ${(err as Error).message}`,
      );
    }
  }

  /** MSS: Persist delivery receipt, route double grey tick to sender. */
  private async onKafkaMessageDelivered(event: KafkaMessageDeliveredPayload): Promise<void> {
    // senderId is required for the all-delivered verdict (recipients = everyone
    // except the sender). Events lacking it come from a pre-feature producer or
    // a buggy client — reject loudly so they don't silently get first-wins
    // behaviour. The broadcast is the cheap part; the verdict is the reason
    // we're here.
    if (!event.senderId) {
      this.logger.warn(
        `⚠️  MESSAGE_DELIVERED missing senderId for ${event.messageId} from ${event.recipientId} — skipped (all-delivered verdict requires senderId)`,
      );
      return;
    }

    // Dedupe: a recipient's second device (or a re-delivered event) must not
    // re-broadcast the receipt. Read-before-write; the worst-case race
    // re-broadcasts once, which the FE's upgrade-only handler dedupes anyway.
    // Skip on ANY existing receipt row for this (message, user) — the row's
    // status may already have been overwritten to 'read' by markConversationRead
    // (monotone statuses, one row per message per user), so a delivered-only
    // check would let a late duplicate ack through.
    const existingReceipts = await this.cassandraService.getReceipts(
      event.conversationId,
      event.messageId,
    );
    if (existingReceipts.some((receipt) => receipt.userId === event.recipientId)) {
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

    // Re-read AFTER the insert so the count includes this ack AND any
    // concurrent commits that landed between our dedup read and this re-read.
    // Reading BEFORE the insert would race with concurrent inserts — both
    // callers would see the same count, both would compute the same wrong
    // verdict, and the last ack would never be observed. The re-read is
    // sub-millisecond (single partition, narrow clustering slice) and the
    // price of correctness for concurrent acks.
    const receiptsAfter = await this.cassandraService.getReceipts(
      event.conversationId,
      event.messageId,
    );
    const deliveredBy = new Set(
      receiptsAfter
        .filter((receipt) => receipt.status === 'delivered' || receipt.status === 'read')
        .map((receipt) => receipt.userId),
    );

    // recipients = everyone except the original sender (sender's own devices
    // are not recipients of the message). For single-sender/zero-recipient
    // conversations the verdict is trivially true — compute against real
    // length so 'all delivered' is meaningful only when there's actually
    // someone to wait for.
    const recipients = await this.participantsExcept(event.conversationId, event.senderId);
    const allDelivered = recipients.length > 0 && recipients.every((id) => deliveredBy.has(id));

    // Route the frame ONLY to the sender's user node — its sole job is
    // upgrading the sender's outgoing tick, and the sender's other devices
    // share that node too (e.g. Alice's MacBook receives it so applyRead can
    // later flip its row to 'read'). Other participants hold incoming rows
    // and have no tick to upgrade — fanning out to them would broadcast
    // receipt state nobody can act on, so we don't.
    // Best-effort: the receipt row is already persisted above, so a transient
    // fan-out failure must not rethrow — retrying would hit the dedup at the top
    // (row already written) and the gray-tick broadcast would be permanently
    // lost. Mirror the message_received fan-out handling in onKafkaMessageSent:
    // log the error, never rethrow.
    try {
      await this.deliveryPublisher.publishToUsers(
        [event.senderId],
        'message_delivered',
        {
          messageId: event.messageId,
          conversationId: event.conversationId,
          recipientId: event.recipientId,
          deliveredAt: event.deliveredAt,
          status: 'delivered',
          allDelivered,
        },
      );
    } catch (err) {
      this.logger.error(
        `⚠️  Delivery receipt fan-out failed for ${event.messageId} (receipt already persisted): ${(err as Error).message}`,
      );
    }
  }

  /** MSS: Persist read receipt, route double blue tick to sender. */
  private async onKafkaMessageRead(event: KafkaMessageReadPayload): Promise<void> {
    // The api's Postgres watermark is the read source of truth (and what powers
    // reload hydration). Decide whether this read actually advanced BEFORE
    // persisting anything, so a stale or replayed receipt (advanced: false)
    // neither writes a Cassandra row nor broadcasts a backward-moving blue tick.
    const verdict = await this.apiClient.markRead(
      event.conversationId,
      event.readerId,
      event.lastReadMessageId,
    );
    if (verdict?.advanced === false) {
      this.logger.debug(
        `👁️  mark_read no-op for ${event.readerId} in ${event.conversationId} (stale) — nothing persisted or broadcast`,
      );
      return;
    }

    // The watermark advanced, or the api is unreachable (verdict null — the
    // gateway's Cassandra copy is then the fallback truth). Persist the
    // gateway-owned durable copy, wrapped so a Cassandra failure doesn't block
    // the live signal below (the api is the other store owner and still healthy).
    try {
      await this.cassandraService.markConversationRead(
        event.conversationId,
        event.readerId,
        event.lastReadMessageId,
      );
      this.logger.log(`👁️  Read receipt stored — ${event.readerId} read up to ${event.lastReadMessageId}`);
    } catch (err) {
      this.logger.warn(
        `⚠️  Cassandra read-receipt persist failed for ${event.conversationId}: ${(err as Error).message} — continuing with broadcast`,
      );
    }

    // Route blue tick to all OTHER participants (senders). Best-effort: the
    // watermark (api Postgres) and gateway Cassandra copy are both persisted by
    // now, so a transient fan-out failure must not rethrow — retrying would hit
    // the api's markRead returning advanced:false (watermark already advanced)
    // and the blue-tick broadcast would be permanently lost. Log, never rethrow.
    try {
      const recipientIdsExcludingReader = await this.participantsExcept(
        event.conversationId,
        event.readerId,
      );
      await this.deliveryPublisher.publishToUsers(
        recipientIdsExcludingReader,
        'message_read',
        {
          conversationId: event.conversationId,
          lastReadMessageId: event.lastReadMessageId,
          readerId: event.readerId,
          readAt: event.readAt,
          status: 'read',
        },
      );
    } catch (err) {
      this.logger.error(
        `⚠️  Read receipt fan-out failed for ${event.readerId} in ${event.conversationId} (watermark already advanced): ${(err as Error).message}`,
      );
    }
  }
}
