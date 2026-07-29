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
import type { SendMessagePayload } from '@chat/shared-types';
import { CassandraService } from '../messages/cassandra.service';
import { ParticipantCacheService } from '../participants/participant-cache.service';

@WebSocketGateway({ cors: true })
export class ChatGateway implements OnGatewayConnection, OnGatewayDisconnect {
  @WebSocketServer()
  server: Server;

  private readonly logger = new Logger(ChatGateway.name);

  // In-memory connection registry: userId → WebSocket
  // Will be replaced with Redis when we scale to multiple instances (Phase 4)
  private connectedUsers = new Map<string, WebSocket>();

  constructor(
    private readonly jwtService: JwtService,
    private readonly cassandraService: CassandraService,
    private readonly participantCache: ParticipantCacheService,
  ) {}

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

  // ─── Message handler ────────────────────────────────────────────────────────

  @UseGuards(WsAuthGuard)
  @SubscribeMessage('message')
  async handleMessage(
    @ConnectedSocket() client: WebSocket,
    @MessageBody() data: SendMessagePayload,
  ): Promise<void> {
    const senderId: string = (client as any).user.sub;
    this.logger.log(`📨 Message from ${senderId} → conversation ${data.conversationId}`);

    // ── 1. Persist to Cassandra ─────────────────────────────────────────────
    const saved = await this.cassandraService.saveMessage(
      data.conversationId,
      senderId,
      data.content,
    );
    this.logger.log(`💾 Persisted message ${saved.id} to Cassandra`);

    // ── 2. Ack sender: message is saved ────────────────────────────────────
    this.send(client, 'message_sent', {
      messageId: saved.id,
      conversationId: saved.conversationId,
      senderId: saved.senderId,
      content: saved.content,
      createdAt: saved.createdAt,
      clientMessageId: data.clientMessageId,
      status: 'sent',
    });

    // ── 3. Resolve all recipients from Redis-cached participant list ─────────
    const participantIds = await this.participantCache.getParticipants(
      data.conversationId,
    );
    const recipientIds = participantIds.filter((id) => id !== senderId);

    if (recipientIds.length === 0) {
      this.logger.warn(`⚠️  No recipients in conversation ${data.conversationId}`);
      return;
    }

    // ── 4. Route to all online recipients concurrently (Group & Direct Fan-Out) ──
    let deliveredCount = 0;

    await Promise.all(
      recipientIds.map(async (recipientId) => {
        const recipientSocket = this.connectedUsers.get(recipientId);

        if (recipientSocket?.readyState === WebSocket.OPEN) {
          this.send(recipientSocket, 'message_received', {
            messageId: saved.id,
            conversationId: saved.conversationId,
            senderId: saved.senderId,
            content: saved.content,
            createdAt: saved.createdAt,
          });
          deliveredCount++;
          this.logger.log(`📬 Delivered to ${recipientId}`);
        } else {
          // Offline recipient → stub; BullMQ offline queue comes in Phase 4
          this.logger.log(`📭 ${recipientId} offline — queued for offline delivery (stub)`);
        }
      }),
    );

    // ── 5. Delivery receipt back to sender if at least one recipient received it ──
    if (deliveredCount > 0) {
      this.send(client, 'message_delivered', {
        messageId: saved.id,
        clientMessageId: data.clientMessageId,
        status: 'delivered',
        deliveredCount,
      });
    }
  }

  // ─── Fetch history handler ──────────────────────────────────────────────────

  @UseGuards(WsAuthGuard)
  @SubscribeMessage('fetch_messages')
  async handleFetchMessages(
    @ConnectedSocket() client: WebSocket,
    @MessageBody() data: { conversationId: string; limit?: number },
  ): Promise<void> {
    const limit = data.limit ?? 20;
    this.logger.log(`📜 Fetching last ${limit} messages for conversation ${data.conversationId}`);

    const messages = await this.cassandraService.getMessages(data.conversationId, limit);

    this.send(client, 'messages_history', {
      conversationId: data.conversationId,
      messages,
    });
  }

  // ─── Helpers ────────────────────────────────────────────────────────────────

  /** Send a typed WS event frame to a socket. */
  private send(socket: WebSocket, event: string, data: unknown): void {
    if (socket.readyState === WebSocket.OPEN) {
      socket.send(JSON.stringify({ event, data }));
    }
  }
}
