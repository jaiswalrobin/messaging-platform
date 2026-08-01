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
import {
  UseGuards,
  UsePipes,
  ValidationPipe,
  Logger,
  OnModuleDestroy,
} from '@nestjs/common';
import { WebSocket, Server } from 'ws';
import { JwtService } from '@nestjs/jwt';
import { WsAuthGuard } from '../auth/ws-auth.guard';
import { MAX_HISTORY_LIMIT } from '@chat/shared-types';
import type {
  KafkaMessageSentPayload,
  KafkaMessageDeliveredPayload,
  KafkaMessageReadPayload,
  KafkaChatEvent,
} from '@chat/shared-types';
import { CassandraService } from '../messages/cassandra.service';
import { ParticipantCacheService } from '../participants/participant-cache.service';
import { KafkaService } from '../kafka/kafka.service';
import { ApiClientService } from '../internal/api-client.service';
import { SendMessageDto } from './dto/send-message.dto';
import { FetchMessagesDto } from './dto/fetch-messages.dto';
import { MarkReadDto } from './dto/mark-read.dto';
import { MessageDeliveredDto } from './dto/message-delivered.dto';
import { types } from 'cassandra-driver';

@WebSocketGateway({ cors: true })
export class ChatGateway
  implements OnGatewayConnection, OnGatewayDisconnect, OnGatewayInit, OnModuleDestroy
{
  @WebSocketServer()
  server: Server;

  private readonly logger = new Logger(ChatGateway.name);

  // In-memory connection registry: userId → Set of sockets (one per connected device)
  // Production: replace with Redis Connection Registry for multi-node routing
  private connectedUsers = new Map<string, Set<WebSocket>>();

  private heartbeat: NodeJS.Timeout;

  constructor(
    private readonly jwtService: JwtService,
    private readonly cassandraService: CassandraService,
    private readonly participantCache: ParticipantCacheService,
    private readonly kafkaService: KafkaService,
    private readonly apiClient: ApiClientService,
  ) {}

  // ─── Module Init ────────────────────────────────────────────────────────────

  onModuleInit() {
    // Register this gateway as a Kafka event consumer (Storage Consumer role)
    this.kafkaService.onEvent((event) => this.handleKafkaEvent(event));
  }

  // ─── Heartbeat ──────────────────────────────────────────────────────────────

  afterInit() {
    this.server.on('connection', (ws) => {
      (ws as any).isAlive = true;
      ws.on('pong', () => {
        (ws as any).isAlive = true;
      });
    });

    this.heartbeat = setInterval(() => {
      for (const ws of this.server.clients) {
        if ((ws as any).isAlive === false) {
          ws.terminate();
          continue;
        }
        (ws as any).isAlive = false;
        ws.ping();
      }
    }, 30000);
    this.heartbeat.unref();
  }

  onModuleDestroy() {
    clearInterval(this.heartbeat);
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
      const set = this.connectedUsers.get(payload.sub) ?? new Set<WebSocket>();
      set.add(client);
      this.connectedUsers.set(payload.sub, set);
      this.logger.log(`✅ Connected: ${payload.sub}`);
    } catch {
      this.logger.warn('❌ Rejected: invalid token');
      client.close(1008, 'Unauthorized');
    }
  }

  handleDisconnect(client: WebSocket) {
    const userId = (client as any).user?.sub;
    if (userId) {
      const set = this.connectedUsers.get(userId);
      if (set) {
        set.delete(client);
        // Only drop the map entry when the last socket for this user closes,
        // otherwise device A's disconnect would ghost device B's mapping.
        if (set.size === 0) {
          this.connectedUsers.delete(userId);
        }
      }
      this.logger.log(`🔌 Disconnected: ${userId}`);
    }
  }

  // ─── WS: Send Message (Kafka-first) ────────────────────────────────────────

  @UseGuards(WsAuthGuard)
  @UsePipes(new ValidationPipe({ whitelist: true, transform: true }))
  @SubscribeMessage('message')
  async handleMessage(
    @ConnectedSocket() client: WebSocket,
    @MessageBody() data: SendMessageDto,
  ): Promise<void> {
    const senderId: string = (client as any).user.sub;

    // ── Step 0: Membership check (IDOR prevention) ─────────────────────────
    const member = await this.requireMember(data.conversationId, senderId, client, {
      clientMessageId: data.clientMessageId,
    });
    if (!member) {
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
      try {
        await this.directPersistAndDeliver({
          type: 'MESSAGE_SENT',
          messageId,
          conversationId: data.conversationId,
          senderId,
          content: data.content,
          clientMessageId: data.clientMessageId,
          createdAt,
        }, client);
      } catch (err) {
        // Direct-mode persist failure — surface it. (In Kafka mode persistence
        // is async in the consumer, so PERSIST_FAILED is direct-mode only.)
        this.logger.error(
          `💥 Direct persist failed for ${messageId}: ${(err as Error).message}`,
        );
        this.send(client, 'error', {
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
  @UsePipes(new ValidationPipe({ whitelist: true, transform: true }))
  @SubscribeMessage('message_delivered')
  async handleDeliveryAck(
    @ConnectedSocket() client: WebSocket,
    @MessageBody() data: MessageDeliveredDto,
  ): Promise<void> {
    const recipientId: string = (client as any).user.sub;

    // Membership check before recording anything (IDOR prevention)
    const member = await this.requireMember(data.conversationId, recipientId, client);
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
    // The Kafka consumer (handleKafkaEvent) will route double grey tick to sender
  }

  // ─── WS: Mark Read (Blue Ticks) ────────────────────────────────────────────

  @UseGuards(WsAuthGuard)
  @UsePipes(new ValidationPipe({ whitelist: true, transform: true }))
  @SubscribeMessage('mark_read')
  async handleMarkRead(
    @ConnectedSocket() client: WebSocket,
    @MessageBody() data: MarkReadDto,
  ): Promise<void> {
    const readerId: string = (client as any).user.sub;

    // Membership check before recording anything (IDOR prevention)
    const member = await this.requireMember(data.conversationId, readerId, client);
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

    try {
      const published = await this.kafkaService.publish(kafkaPayload);
      if (!published) {
        this.logger.warn(
          `⚠️  Kafka unavailable — read receipt for ${data.lastReadMessageId} dropped`,
        );
      }
      // Kafka consumer will persist receipt and notify sender
    } catch (err) {
      this.logger.error(
        `💥 Failed to record read receipt for ${readerId} in ${data.conversationId}: ${(err as Error).message}`,
      );
      this.send(client, 'error', {
        code: 'INTERNAL',
        message: 'Failed to record read receipt',
        conversationId: data.conversationId,
      });
    }
  }

  // ─── WS: Fetch Message History ─────────────────────────────────────────────

  @UseGuards(WsAuthGuard)
  @UsePipes(new ValidationPipe({ whitelist: true, transform: true }))
  @SubscribeMessage('fetch_messages')
  async handleFetchMessages(
    @ConnectedSocket() client: WebSocket,
    @MessageBody() data: FetchMessagesDto,
  ): Promise<void> {
    const senderId: string = (client as any).user.sub;
    const limit = Math.min(data.limit ?? 20, MAX_HISTORY_LIMIT);
    this.logger.log(`📜 Fetching last ${limit} messages for conversation ${data.conversationId}`);

    // Membership check before fetching history (IDOR prevention)
    const member = await this.requireMember(data.conversationId, senderId, client);
    if (!member) {
      return;
    }

    try {
      const messages = await this.cassandraService.getMessages(data.conversationId, limit);

      this.send(client, 'messages_history', {
        conversationId: data.conversationId,
        messages,
      });
    } catch (err) {
      this.logger.error(
        `💥 Failed to fetch messages for ${data.conversationId}: ${(err as Error).message}`,
      );
      this.send(client, 'error', {
        code: 'FETCH_FAILED',
        message: 'Failed to fetch messages',
        conversationId: data.conversationId,
      });
    }
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

    // 2. Route to online recipients (all devices of each)
    await this.broadcastToParticipants(event.conversationId, event.senderId, 'message_received', {
      messageId: event.messageId,
      conversationId: event.conversationId,
      senderId: event.senderId,
      content: event.content,
      createdAt: event.createdAt,
    });
  }

  /**
   * MSS: Persist delivery receipt, route double grey tick to sender.
   */
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
    await this.broadcastToParticipants(
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

  /**
   * MSS: Persist read receipt, route double blue tick to sender.
   */
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
    await this.broadcastToParticipants(
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
      const recipientSockets = this.connectedUsers.get(recipientId);
      if (!recipientSockets || recipientSockets.size === 0) {
        this.logger.log(`📭 ${recipientId} offline — queued for offline delivery (stub)`);
        continue;
      }
      let delivered = false;
      for (const socket of recipientSockets) {
        if (socket.readyState === WebSocket.OPEN) {
          this.send(socket, 'message_received', {
            messageId: event.messageId,
            conversationId: event.conversationId,
            senderId: event.senderId,
            content: event.content,
            createdAt: event.createdAt,
          });
          delivered = true;
        }
      }
      if (delivered) deliveredCount++;
    }

    if (deliveredCount > 0) {
      this.send(senderSocket, 'message_delivered', {
        messageId: event.messageId,
        conversationId: event.conversationId,
        clientMessageId: event.clientMessageId,
        status: 'delivered',
        deliveredCount,
      });
    }
  }

  // ─── Helpers ────────────────────────────────────────────────────────────────

  /**
   * Membership check (IDOR prevention). Resolves true when the user is a
   * participant; otherwise sends the unified FORBIDDEN error frame (always
   * carrying conversationId, plus clientMessageId when provided) and resolves
   * false. Never throws — a failed membership lookup is logged and treated as
   * forbidden so the client always gets an error frame instead of a hang.
   */
  private async requireMember(
    conversationId: string,
    userId: string,
    client: WebSocket,
    extra?: { clientMessageId?: string },
  ): Promise<boolean> {
    try {
      const member = await this.participantCache.isMember(conversationId, userId);
      if (!member) {
        this.sendForbiddenFrame(client, conversationId, extra?.clientMessageId);
        return false;
      }
      return true;
    } catch (err) {
      this.logger.error(
        `❌ Membership check failed for ${userId} in ${conversationId}: ${(err as Error).message}`,
      );
      this.sendForbiddenFrame(client, conversationId, extra?.clientMessageId);
      return false;
    }
  }

  private sendForbiddenFrame(
    client: WebSocket,
    conversationId: string,
    clientMessageId?: string,
  ): void {
    const frame: Record<string, string> = {
      code: 'FORBIDDEN',
      message: 'You are not a member of this conversation',
      conversationId,
    };
    if (clientMessageId !== undefined) {
      frame.clientMessageId = clientMessageId;
    }
    this.send(client, 'error', frame);
  }

  private send(socket: WebSocket, event: string, data: unknown): void {
    if (socket.readyState === WebSocket.OPEN) {
      socket.send(JSON.stringify({ event, data }));
    }
  }

  /**
   * Send an event frame to all online sockets of every participant in a
   * conversation except `excludeUserId`. Returns the number of distinct
   * recipients reached (counted per user, not per device).
   */
  private async broadcastToParticipants(
    conversationId: string,
    excludeUserId: string,
    event: string,
    data: unknown,
  ): Promise<number> {
    const participantIds = await this.participantCache.getParticipants(conversationId);
    const recipientIds = participantIds.filter((id) => id !== excludeUserId);

    let deliveredCount = 0;

    await Promise.all(
      recipientIds.map(async (recipientId) => {
        const recipientSockets = this.connectedUsers.get(recipientId);

        if (!recipientSockets || recipientSockets.size === 0) {
          // Offline recipient → stub; BullMQ offline queue comes in Phase 4
          this.logger.log(`📭 ${recipientId} offline — queued for offline delivery (stub)`);
          return;
        }

        let delivered = false;
        for (const socket of recipientSockets) {
          if (socket.readyState === WebSocket.OPEN) {
            this.send(socket, event, data);
            delivered = true;
          }
        }

        // Count the recipient once per recipientId, not per device
        if (delivered) {
          deliveredCount++;
          this.logger.log(`📬 Delivered to ${recipientId}`);
        } else {
          this.logger.log(`📭 ${recipientId} offline — queued for offline delivery (stub)`);
        }
      }),
    );

    return deliveredCount;
  }
}
