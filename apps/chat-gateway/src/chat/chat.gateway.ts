import {
  WebSocketGateway,
  WebSocketServer,
  SubscribeMessage,
  OnGatewayConnection,
  OnGatewayDisconnect,
  ConnectedSocket,
  MessageBody,
} from '@nestjs/websockets';
import { UseGuards, Logger } from '@nestjs/common';
import { WebSocket, Server } from 'ws';
import { JwtService } from '@nestjs/jwt';
import { WsAuthGuard } from '../auth/ws-auth.guard';
import type {
  SendMessagePayload,
  MarkReadPayload,
  MessageDeliveredPayload,
  KafkaMessageSentPayload,
  KafkaMessageDeliveredPayload,
  KafkaMessageReadPayload,
  KafkaChatEvent,
} from '@chat/shared-types';
import { CassandraService } from '../messages/cassandra.service';
import { ParticipantCacheService } from '../participants/participant-cache.service';
import { KafkaService } from '../kafka/kafka.service';
import { types } from 'cassandra-driver';

@WebSocketGateway({ cors: true })
export class ChatGateway implements OnGatewayConnection, OnGatewayDisconnect {
  @WebSocketServer()
  server: Server;

  private readonly logger = new Logger(ChatGateway.name);

  // In-memory connection registry: userId → WebSocket
  // Production: replace with Redis Connection Registry for multi-node routing
  private connectedUsers = new Map<string, WebSocket>();

  constructor(
    private readonly jwtService: JwtService,
    private readonly cassandraService: CassandraService,
    private readonly participantCache: ParticipantCacheService,
    private readonly kafkaService: KafkaService,
  ) {}

  // ─── Module Init ────────────────────────────────────────────────────────────

  onModuleInit() {
    // Register this gateway as a Kafka event consumer (Storage Consumer role)
    this.kafkaService.onEvent((event) => this.handleKafkaEvent(event));
  }

  // ─── Connection lifecycle ───────────────────────────────────────────────────

  handleConnection(client: any, request: any) {
    const url = new URL(request.url, 'http://localhost');
    const token = url.searchParams.get('token');

    if (!token) {
      this.logger.warn('❌ Rejected: no token');
      client.close(1008, 'Unauthorized');
      return;
    }

    try {
      const payload = this.jwtService.verify(token);
      client.user = payload; // { sub: userId, email }
      this.connectedUsers.set(payload.sub, client);
      this.logger.log(`✅ Connected: ${payload.sub}`);
    } catch {
      this.logger.warn('❌ Rejected: invalid token');
      client.close(1008, 'Unauthorized');
    }
  }

  handleDisconnect(client: WebSocket) {
    const userId = (client as any).user?.sub;
    if (userId) {
      this.connectedUsers.delete(userId);
      this.logger.log(`🔌 Disconnected: ${userId}`);
    }
  }

  // ─── WS: Send Message (Kafka-first) ────────────────────────────────────────

  @UseGuards(WsAuthGuard)
  @SubscribeMessage('message')
  async handleMessage(
    @ConnectedSocket() client: WebSocket,
    @MessageBody() data: SendMessagePayload,
  ): Promise<void> {
    const senderId: string = (client as any).user.sub;
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
    };

    const published = await this.kafkaService.publish(kafkaPayload);

    // ── Step 2: ACK sender immediately after Kafka ACK (or direct mode) ────
    if (published) {
      // Kafka-first: Kafka ACK received → send single tick to sender
      this.send(client, 'message_sent', {
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
      await this.directPersistAndDeliver({
        type: 'MESSAGE_SENT',
        messageId,
        conversationId: data.conversationId,
        senderId,
        content: data.content,
        clientMessageId: data.clientMessageId,
        createdAt,
      }, client);
    }
  }

  // ─── WS: Client ACKs Delivery ──────────────────────────────────────────────

  @UseGuards(WsAuthGuard)
  @SubscribeMessage('message_delivered')
  async handleDeliveryAck(
    @ConnectedSocket() client: WebSocket,
    @MessageBody() data: MessageDeliveredPayload,
  ): Promise<void> {
    const recipientId: string = (client as any).user.sub;
    const deliveredAt = new Date().toISOString();

    this.logger.log(`📬 Delivery ACK from ${recipientId} for message ${data.messageId}`);

    const kafkaPayload: KafkaMessageDeliveredPayload = {
      type: 'MESSAGE_DELIVERED',
      messageId: data.messageId,
      conversationId: data.conversationId,
      recipientId,
      deliveredAt,
    };

    await this.kafkaService.publish(kafkaPayload);
    // The Kafka consumer (handleKafkaEvent) will route double grey tick to sender
  }

  // ─── WS: Mark Read (Blue Ticks) ────────────────────────────────────────────

  @UseGuards(WsAuthGuard)
  @SubscribeMessage('mark_read')
  async handleMarkRead(
    @ConnectedSocket() client: WebSocket,
    @MessageBody() data: MarkReadPayload,
  ): Promise<void> {
    const readerId: string = (client as any).user.sub;
    const readAt = new Date().toISOString();

    this.logger.log(`👁️  mark_read from ${readerId} in conversation ${data.conversationId} up to ${data.lastReadMessageId}`);

    const kafkaPayload: KafkaMessageReadPayload = {
      type: 'MESSAGE_READ',
      conversationId: data.conversationId,
      lastReadMessageId: data.lastReadMessageId,
      readerId,
      readAt,
    };

    await this.kafkaService.publish(kafkaPayload);
    // Kafka consumer will persist receipt and notify sender
  }

  // ─── WS: Fetch Message History ─────────────────────────────────────────────

  @UseGuards(WsAuthGuard)
  @SubscribeMessage('fetch_messages')
  async handleFetchMessages(
    @ConnectedSocket() client: WebSocket,
    @MessageBody() data: { conversationId: string; limit?: number },
  ): Promise<void> {
    const limit = data.limit ?? 20;
    this.logger.log(`📜 Fetching last ${limit} messages for conversation ${data.conversationId}`);

    const messages = await this.cassandraService.getMessages(data.conversationId, limit);
    this.send(client, 'messages_history', { conversationId: data.conversationId, messages });
  }

  // ─── Kafka Consumer (MSS role in single-node dev) ──────────────────────────

  private async handleKafkaEvent(event: KafkaChatEvent): Promise<void> {
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
    }
  }

  /**
   * MSS: Persist message to Cassandra, route to online recipients, queue offline.
   */
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

    // 2. Route to online recipients
    const participantIds = await this.participantCache.getParticipants(event.conversationId);
    const recipientIds = participantIds.filter((id) => id !== event.senderId);

    for (const recipientId of recipientIds) {
      const recipientSocket = this.connectedUsers.get(recipientId);
      if (recipientSocket?.readyState === WebSocket.OPEN) {
        this.send(recipientSocket, 'message_received', {
          messageId: event.messageId,
          conversationId: event.conversationId,
          senderId: event.senderId,
          content: event.content,
          createdAt: event.createdAt,
        });
        this.logger.log(`📬 Delivered message ${event.messageId} to ${recipientId}`);
      } else {
        // TODO (Phase 4): Queue into Redis Inbox and trigger push notification
        this.logger.log(`📭 ${recipientId} offline — would queue in Redis Inbox`);
      }
    }
  }

  /**
   * MSS: Persist delivery receipt, route double grey tick to sender.
   */
  private async onKafkaMessageDelivered(event: KafkaMessageDeliveredPayload): Promise<void> {
    // Persist delivery receipt to Cassandra
    await this.cassandraService.upsertReceipt(
      event.conversationId,
      event.messageId,
      event.recipientId,
      'delivered',
    );

    // Find sender via message lookup and route status update
    // For now, we broadcast to all participants to update UI
    const participantIds = await this.participantCache.getParticipants(event.conversationId);
    for (const userId of participantIds) {
      if (userId === event.recipientId) continue;
      const socket = this.connectedUsers.get(userId);
      if (socket?.readyState === WebSocket.OPEN) {
        this.send(socket, 'message_delivered', {
          messageId: event.messageId,
          conversationId: event.conversationId,
          recipientId: event.recipientId,
          deliveredAt: event.deliveredAt,
          status: 'delivered',
        });
      }
    }
  }

  /**
   * MSS: Persist read receipt, route double blue tick to sender.
   */
  private async onKafkaMessageRead(event: KafkaMessageReadPayload): Promise<void> {
    // Persist read receipt to Cassandra
    await this.cassandraService.markConversationRead(
      event.conversationId,
      event.readerId,
      event.lastReadMessageId,
    );
    this.logger.log(`👁️  Read receipt stored — ${event.readerId} read up to ${event.lastReadMessageId}`);

    // Route blue tick to all OTHER participants (senders)
    const participantIds = await this.participantCache.getParticipants(event.conversationId);
    for (const userId of participantIds) {
      if (userId === event.readerId) continue;
      const socket = this.connectedUsers.get(userId);
      if (socket?.readyState === WebSocket.OPEN) {
        this.send(socket, 'message_read', {
          conversationId: event.conversationId,
          lastReadMessageId: event.lastReadMessageId,
          readerId: event.readerId,
          readAt: event.readAt,
          status: 'read',
        });
      }
    }
  }

  // ─── Fallback: Direct Persist & Deliver (no Kafka) ─────────────────────────

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

    this.send(senderSocket, 'message_sent', {
      messageId: event.messageId,
      conversationId: event.conversationId,
      senderId: event.senderId,
      content: event.content,
      createdAt: event.createdAt,
      clientMessageId: event.clientMessageId,
      status: 'sent',
    });

    const participantIds = await this.participantCache.getParticipants(event.conversationId);
    const recipientIds = participantIds.filter((id) => id !== event.senderId);

    let deliveredCount = 0;
    for (const recipientId of recipientIds) {
      const recipientSocket = this.connectedUsers.get(recipientId);
      if (recipientSocket?.readyState === WebSocket.OPEN) {
        this.send(recipientSocket, 'message_received', {
          messageId: event.messageId,
          conversationId: event.conversationId,
          senderId: event.senderId,
          content: event.content,
          createdAt: event.createdAt,
        });
        deliveredCount++;
      }
    }

    if (deliveredCount > 0) {
      this.send(senderSocket, 'message_delivered', {
        messageId: event.messageId,
        clientMessageId: event.clientMessageId,
        status: 'delivered',
        deliveredCount,
      });
    }
  }

  // ─── Helpers ────────────────────────────────────────────────────────────────

  private send(socket: WebSocket, event: string, data: unknown): void {
    if (socket.readyState === WebSocket.OPEN) {
      socket.send(JSON.stringify({ event, data }));
    }
  }
}
