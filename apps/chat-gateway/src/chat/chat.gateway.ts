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
import { UseGuards, UsePipes, ValidationPipe, Logger } from '@nestjs/common';
import { WebSocket, Server } from 'ws';
import { WsAuthGuard } from '../auth/ws-auth.guard';
import type {
  KafkaMessageSentPayload,
  KafkaMessageDeliveredPayload,
  KafkaMessageReadPayload,
} from '@chat/shared-types';
import { KafkaService } from '../kafka/kafka.service';
import { EmergencyPersistService } from '../messages/emergency-persist.service';
import { ConnectionRegistryService } from './connection-registry.service';
import { SendMessageDto } from './dto/send-message.dto';
import { MarkReadDto } from './dto/mark-read.dto';
import { MessageDeliveredDto } from './dto/message-delivered.dto';
import { types } from 'cassandra-driver';

/**
 * Canonical uuid form is xxxxxxxx-xxxx-Vxxx-... — this is the index of the
 * version nibble (V) in the 36-char string. The api's watermark CAS regex
 * mirrors this (the version nibble must be '1').
 */
const V1_VERSION_NIBBLE_INDEX = 14;

/**
 * Skew allowance, in ms, for accepting a mark_read watermark as "not future".
 * A crafted v1 timeuuid carries its own timestamp field; this tolerance lets a
 * slightly future-dated client clock through while still rejecting a forged
 * far-future timestamp that would permanently block later legitimate reads.
 */
const FUTURE_SKEW_MS = 60000;

/**
 * WS producer handlers. Connection lifecycle, heartbeat and fan-out live in
 * ConnectionRegistryService. The gateway is producer-only under the SRP split:
 * consumption, persistence and receipt routing (the MSS role) live in the
 * separate `mss` service, which consumes chat-events from Kafka. Incoming
 * WebSocket event DTOs are validated using NestJS ValidationPipe.
 */
@UsePipes(new ValidationPipe({ transform: true }))
@WebSocketGateway({ cors: true })
export class ChatGateway implements OnGatewayConnection, OnGatewayDisconnect, OnGatewayInit {
  @WebSocketServer()
  server: Server;

  private readonly logger = new Logger(ChatGateway.name);

  constructor(
    private readonly kafkaService: KafkaService,
    private readonly registry: ConnectionRegistryService,
    private readonly emergencyPersist: EmergencyPersistService,
  ) {}

  // ─── Heartbeat ─────────────────────────────────────────────────────────────

  afterInit() {
    // Heartbeat wiring lives in the registry — it owns the socket lifecycle
    this.registry.attachServer(this.server);
  }

  // ─── Connection lifecycle (delegated to the registry) ───────────────────────

  async handleConnection(client: any, request: any): Promise<void> {
    await this.registry.handleConnection(client, request);
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
    const isMember = await this.registry.requireMember(data.conversationId, senderId, client, {
      clientMessageId: data.clientMessageId,
    });
    if (!isMember) {
      return;
    }

    const messageId = types.TimeUuid.now().toString();
    const createdAt = new Date().toISOString();

    this.logger.log(`📨 Message from ${senderId} → conversation ${data.conversationId}`);

    // ── Step 1: Publish MESSAGE_SENT to Kafka ───────────────────────────────
    const kafkaPayload: KafkaMessageSentPayload = {
      type: 'MESSAGE_SENT',
      messageId,
      conversationId: data.conversationId,
      senderId,
      content: data.content,
      clientMessageId: data.clientMessageId,
      createdAt,
      senderSocketId: (client as any).socketId,
    };

    const isPublished = await this.kafkaService.publish(kafkaPayload);

    // ── Step 2: ACK sender after Kafka ACK ──────────────────────────────────
    if (isPublished) {
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
      // Degraded SRP exception — broker-down only: the gateway normally never
      // touches Cassandra (mss owns it), but with Kafka down nothing persists
      // the message, so we insert directly to keep sends working (NFR-3).
      try {
        await this.emergencyPersist.saveMessage(
          data.conversationId,
          senderId,
          data.content,
          messageId,
          new Date(createdAt),
        );
        // sender tick + local fan-out (recipients this node holds) + sender's other devices
        this.registry.send(client, 'message_sent', {
          messageId,
          conversationId: data.conversationId,
          senderId,
          content: data.content,
          createdAt,
          clientMessageId: data.clientMessageId,
          status: 'sent',
        });
        await this.registry.broadcastToParticipants(
          data.conversationId,
          senderId,
          'message_received',
          {
            messageId,
            conversationId: data.conversationId,
            senderId,
            content: data.content,
            createdAt,
          },
        );
        this.registry.sendToUserSockets(
          senderId,
          'message_received',
          {
            messageId,
            conversationId: data.conversationId,
            senderId,
            content: data.content,
            createdAt,
          },
          (client as any).socketId,
        );
      } catch (err) {
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
    const isMember = await this.registry.requireMember(data.conversationId, recipientId, client);
    if (!isMember) {
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

    const isPublished = await this.kafkaService.publish(kafkaPayload);
    if (!isPublished) {
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
    const isMember = await this.registry.requireMember(data.conversationId, readerId, client);
    if (!isMember) {
      return;
    }

    // Validate lastReadMessageId before it reaches Kafka / the api. Three gates:
    // (1) it must parse as a uuid, (2) it must be a v1 timeuuid — the driver's
    // TimeUuid.fromString (→ Uuid.fromString) validates length + hex only, NOT the
    // version nibble, so a crafted v4/v5 uuid with a past timestamp field would
    // otherwise pass and be written as the watermark, forging blue ticks — and
    // (3) it must not be future-dated (a forged future timestamp would advance the
    // watermark arbitrarily and permanently block later legitimate reads).
    // mark_read is fire-and-forget soft state, so every rejection is a silent
    // log + return: an INTERNAL error frame here has no clientMessageId, so the FE
    // could not know which row it referred to.
    let lastReadTimeUuid: types.TimeUuid;
    try {
      lastReadTimeUuid = types.TimeUuid.fromString(data.lastReadMessageId);
    } catch {
      this.logger.warn(
        `⚠️  Invalid lastReadMessageId from ${readerId} in conversation ${data.conversationId}`,
      );
      return;
    }
    // Canonical uuid is xxxxxxxx-xxxx-Vxxx-... — the version nibble sits at
    // V1_VERSION_NIBBLE_INDEX. The api's watermark CAS regex mirrors this
    // (version nibble must be '1').
    if (data.lastReadMessageId[V1_VERSION_NIBBLE_INDEX] !== '1') {
      this.logger.warn(
        `⚠️  Non-v1 lastReadMessageId from ${readerId} in conversation ${data.conversationId} — rejected (v1 required)`,
      );
      return;
    }
    if (lastReadTimeUuid.getDate().getTime() > Date.now() + FUTURE_SKEW_MS) {
      this.logger.warn(
        `⚠️  Future-dated lastReadMessageId from ${readerId} in conversation ${data.conversationId}`,
      );
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
    const isPublished = await this.kafkaService.publish(kafkaPayload);
    if (!isPublished) {
      this.logger.warn(
        `⚠️  Kafka unavailable — read receipt for ${data.lastReadMessageId} dropped`,
      );
    }
    // MSS consumer will persist receipt and notify sender
  }
}
