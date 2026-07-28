import {
  WebSocketGateway,
  WebSocketServer,
  SubscribeMessage,
  OnGatewayConnection,
  OnGatewayDisconnect,
  ConnectedSocket,
  MessageBody,
} from '@nestjs/websockets';
import { UseGuards } from '@nestjs/common';
import { WebSocket, Server } from 'ws'; // Correct import for raw ws library
import { WsAuthGuard } from '../auth/ws-auth.guard';
import type { SendMessagePayload } from '@chat/shared-types';
import { JwtService } from '@nestjs/jwt';

@WebSocketGateway(8080, { cors: true })
export class ChatGateway implements OnGatewayConnection, OnGatewayDisconnect {
  @WebSocketServer()
  server: Server;

  // Track connected users (In memory for now, we will move this to Redis later)
  private connectedUsers = new Map<string, WebSocket>();

  constructor(private jwtService: JwtService) { }

  handleConnection(client: any, request: any) {
    const url = new URL(request.url, 'http://localhost');
    const token = url.searchParams.get('token');

    if (!token) {
      console.log('❌ Rejected connection: no token provided');
      client.close(1008, 'Unauthorized');
      return;
    }

    try {
      const payload = this.jwtService.verify(token);
      client.user = payload;
      this.connectedUsers.set(payload.sub, client);
      console.log(`✅ User connected: ${payload.sub}`);
    } catch {
      console.log('❌ Rejected connection: invalid or missing token');
      client.close(1008, 'Unauthorized');
    }
  }

  handleDisconnect(client: WebSocket) {
    const userId = (client as any).user?.sub;
    if (userId) {
      console.log(`❌ User disconnected: ${userId}`);
      this.connectedUsers.delete(userId);
    }
  }

  @UseGuards(WsAuthGuard)
  @SubscribeMessage('message')
  handleMessage(
    @ConnectedSocket() client: WebSocket,
    @MessageBody() data: SendMessagePayload,
  ) {
    const senderId = (client as any).user.sub;
    console.log(`📨 Received message from ${senderId}:`, data);

    // Echo it back to the sender to prove it works
    return {
      event: 'message_sent',
      data: {
        ...data,
        senderId,
        status: 'sent',
      },
    };
  }
}
