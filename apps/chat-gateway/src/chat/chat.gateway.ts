import {
  WebSocketGateway,
  WebSocketServer,
  SubscribeMessage,
  OnGatewayConnection,
  OnGatewayDisconnect,
  OnGatewayInit,
  ConnectedSocket,
  MessageBody,
} from '@nestjs/websockets';
import { UseGuards, Logger } from '@nestjs/common';
import { WebSocket, Server } from 'ws';
import { WsAuthGuard } from '../auth/ws-auth.guard';
import type {
  KafkaMessageSentPayload,
  KafkaMessageDeliveredPayload,
  KafkaMessageReadPayload,
} from '@chat/shared-types';
import { CassandraService } from '../messages/cassandra.service';
import { KafkaService } from '../kafka/kafka.service';
import { ConnectionRegistryService } from './connection-registry.service';
import { ChatConsumerService } from './chat-consumer.service';
import { SendMessageDto } from './dto/send-message.dto';
import { FetchMessagesDto } from './dto/fetch-messages.dto';
import { MarkReadDto } from './dto/mark-read.dto';
import { MessageDeliveredDto } from './dto/message-delivered.dto';
import { types } from 'cassandra-driver';

/**
 * WS producer handlers. Connection lifecycle, heartbeat and fan-out live in
 * ConnectionRegistryService; the Kafka consumer (MSS) role lives in
 * ChatConsumerService. DTO validation is handled by the global ValidationPipe
 * in main.ts (per-handler @UsePipes removed as redundant).
 */
@WebSocketGateway({ cors: true })
export class ChatGateway implements OnGatewayConnection, OnGatewayDisconnect, OnGatewayInit {
  @WebSocketServer()
  server: Server;

  private readonly logger = new Logger(ChatGateway.name);

  constructor(
    private readonly cassandraService: CassandraService,
    private readonly kafkaService: KafkaService,
    private readonly registry: ConnectionRegistryService,
    private readonly consumer: ChatConsumerService,
  ) {}

  // ─── Module Init & Heartbeat ────────────────────────────────────────────────

  onModuleInit() {
    // Register the MSS consumer (ChatConsumerService) as the Kafka event handler
    this.kafkaService.onEvent((event) => this.consumer.handleKafkaEvent(event));
  }

  afterInit() {
    // Heartbeat wiring lives in the registry — it owns the socket lifecycle
    this.registry.attachServer(this.server);
  }

  // ─── Connection lifecycle (delegated to the registry) ───────────────────────

  handleConnection(client: any, request: any): void {
    this.registry.handleConnection(client, request);
  }

  handleDisconnect(client: WebSocket): void {
    this.registry.handleDisconnect(client);
  }

  // ─── WS: Send Message (Kafka-first) ────────────────────────────────────────

  @UseGuards(WsAuthGuard)
  @SubscribeMessage('message')
  async handleMessage(
    @ConnectedSocket() client: WebSocket,
    @MessageBody() data: SendMessageDto,
  ): Promise<void> {
    const senderId: string = (client as any).user.userId;

    // ── Step 0: Membership check (IDOR prevention) ─────────────────────────
    const member = await this.registry.requireMember(data.conversationId, senderId, client, {
      clientMessageId: data.clientMessageId,
    });
    if (!member) {
      return;
    }

    const messageId = types.TimeUuid.now().toString();
    const createdAt = new Date().toISOString();

    this.logger.log(`📨 Message from ${senderId} → conversation ${data.conversationId}`);

    // ── Step 1: Publish MESSAGE_SENT to Kafka ───────────────────────────────
    // One payload, reused for both the Kafka publish and the direct fallback.
    const kafkaPayload: KafkaMessageSentPayload = {
      type: 'MESSAGE_SENT',
      messageId,
      conversationId: data.conversationId,
      senderId,
      content: data.content,
      clientMessageId: data.clientMessageId,
      createdAt,
    };

    const published = await this.kafkaService.publish(kafkaPayload);

    // ── Step 2: ACK sender immediately after Kafka ACK (or direct mode) ────
    if (published) {
      // Kafka-first: Kafka ACK received → send single tick to sender
      this.registry.send(client, 'message_sent', {
        messageId,
        conversationId: data.conversationId,
        senderId,
        content: data.content,
        createdAt,
        clientMessageId: data.clientMessageId,
        status: 'sent',
      });
      this.logger.log(`✅ Kafka ACK received, message_sent dispatched for ${messageId}`);
    } else {
      // Fallback (no Kafka): persist directly and deliver inline
      this.logger.warn(`⚠️  Kafka unavailable — falling back to direct delivery for ${messageId}`);
      try {
        await this.directPersistAndDeliver(kafkaPayload, client);
      } catch (err) {
        // Direct-mode persist failure — surface it. (In Kafka mode persistence
        // is async in the consumer, so PERSIST_FAILED is direct-mode only.)
        this.logger.error(
          `💥 Direct persist failed for ${messageId}: ${(err as Error).message}`,
        );
        this.registry.send(client, 'error', {
          code: 'PERSIST_FAILED',
          message: 'Message could not be saved',
          clientMessageId: data.clientMessageId,
          conversationId: data.conversationId,
        });
      }
    }
  }

  // ─── WS: Client ACKs Delivery ──────────────────────────────────────────────

  @UseGuards(WsAuthGuard)
  @SubscribeMessage('message_delivered')
  async handleDeliveryAck(
    @ConnectedSocket() client: WebSocket,
    @MessageBody() data: MessageDeliveredDto,
  ): Promise<void> {
    const recipientId: string = (client as any).user.userId;

    // Membership check before recording anything (IDOR prevention)
    const member = await this.registry.requireMember(data.conversationId, recipientId, client);
    if (!member) {
      return;
    }

    const deliveredAt = new Date().toISOString();

    this.logger.log(`📬 Delivery ACK from ${recipientId} for message ${data.messageId}`);

    const kafkaPayload: KafkaMessageDeliveredPayload = {
      type: 'MESSAGE_DELIVERED',
      messageId: data.messageId,
      conversationId: data.conversationId,
      recipientId,
      deliveredAt,
    };

    const published = await this.kafkaService.publish(kafkaPayload);
    if (!published) {
      // Broker down → the consumer can't route the receipt; soft state, dropped.
      this.logger.warn(
        `⚠️  Kafka unavailable — delivery receipt for ${data.messageId} dropped`,
      );
    }
    // The MSS consumer (handleKafkaEvent) will route double grey tick to sender
  }

  // ─── WS: Mark Read (Blue Ticks) ────────────────────────────────────────────

  @UseGuards(WsAuthGuard)
  @SubscribeMessage('mark_read')
  async handleMarkRead(
    @ConnectedSocket() client: WebSocket,
    @MessageBody() data: MarkReadDto,
  ): Promise<void> {
    const readerId: string = (client as any).user.userId;

    // Membership check before recording anything (IDOR prevention)
    const member = await this.registry.requireMember(data.conversationId, readerId, client);
    if (!member) {
      return;
    }

    const readAt = new Date().toISOString();

    this.logger.log(`👁️  mark_read from ${readerId} in conversation ${data.conversationId} up to ${data.lastReadMessageId}`);

    const kafkaPayload: KafkaMessageReadPayload = {
      type: 'MESSAGE_READ',
      conversationId: data.conversationId,
      lastReadMessageId: data.lastReadMessageId,
      readerId,
      readAt,
    };

    // publish() never throws — it returns false when Kafka is unavailable — so
    // the failure handling is a warn log, consistent with handleDeliveryAck.
    const published = await this.kafkaService.publish(kafkaPayload);
    if (!published) {
      this.logger.warn(
        `⚠️  Kafka unavailable — read receipt for ${data.lastReadMessageId} dropped`,
      );
    }
    // MSS consumer will persist receipt and notify sender
  }

  // ─── WS: Fetch Message History ─────────────────────────────────────────────

  @UseGuards(WsAuthGuard)
  @SubscribeMessage('fetch_messages')
  async handleFetchMessages(
    @ConnectedSocket() client: WebSocket,
    @MessageBody() data: FetchMessagesDto,
  ): Promise<void> {
    const userId: string = (client as any).user.userId;
    // Limit is already capped by FetchMessagesDto (@Max(MAX_HISTORY_LIMIT))
    const limit = data.limit ?? 20;
    this.logger.log(`📜 Fetching last ${limit} messages for conversation ${data.conversationId}`);

    // Membership check before fetching history (IDOR prevention)
    const member = await this.registry.requireMember(data.conversationId, userId, client);
    if (!member) {
      return;
    }

    try {
      const messages = await this.cassandraService.getMessages(data.conversationId, limit);

      this.registry.send(client, 'messages_history', {
        conversationId: data.conversationId,
        messages,
      });
    } catch (err) {
      this.logger.error(
        `💥 Failed to fetch messages for ${data.conversationId}: ${(err as Error).message}`,
      );
      this.registry.send(client, 'error', {
        code: 'FETCH_FAILED',
        message: 'Failed to fetch messages',
        conversationId: data.conversationId,
      });
    }
  }

  // ─── Fallback: Direct Persist & Deliver (no Kafka) ─────────────────────────

  /**
   * Broker-down fallback for `message` events: persist to Cassandra and deliver
   * inline, bypassing the MSS consumer.
   *
   * NOTE the intentional `message_delivered` shape difference vs the Kafka path:
   * the fallback frame carries `clientMessageId` + `deliveredCount` (the sender's
   * own socket is the only place that learns delivery — nothing is persisted to
   * message_receipts, so the consumer-style recipientId/deliveredAt broadcast
   * cannot be produced), whereas the consumer path broadcasts
   * recipientId/deliveredAt to everyone but the acker and omits clientMessageId.
   * The FE handles both shapes (matching its own messages by messageId, or by
   * clientMessageId in direct-fallback mode).
   */
  private async directPersistAndDeliver(
    event: KafkaMessageSentPayload,
    senderSocket: WebSocket,
  ): Promise<void> {
    await this.cassandraService.saveMessage(
      event.conversationId,
      event.senderId,
      event.content,
      event.messageId,
      new Date(event.createdAt),
    );

    this.registry.send(senderSocket, 'message_sent', {
      messageId: event.messageId,
      conversationId: event.conversationId,
      senderId: event.senderId,
      content: event.content,
      createdAt: event.createdAt,
      clientMessageId: event.clientMessageId,
      status: 'sent',
    });

    // Fan out message_received to online recipients (offline → dropped, no queue)
    const deliveredCount = await this.registry.broadcastToParticipants(
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

    // Best-effort summary to the sender: how many recipients actually got it
    if (deliveredCount > 0) {
      this.registry.send(senderSocket, 'message_delivered', {
        messageId: event.messageId,
        conversationId: event.conversationId,
        clientMessageId: event.clientMessageId,
        status: 'delivered',
        deliveredCount,
      });
    }
  }
}
