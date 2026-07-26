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
import { WebSocket } from 'ws'; // Correct import for raw ws library
import { WsAuthGuard } from '../auth/ws-auth.guard';
import type { SendMessagePayload } from '@chat/shared-types';

@WebSocketGateway(8080, { cors: true })
export class ChatGateway implements OnGatewayConnection, OnGatewayDisconnect {
  @WebSocketServer()
  server: WebSocket.Server;

  // Track connected users (In memory for now, we will move this to Redis later)
  private connectedUsers = new Map<string, WebSocket>();

  handleConnection(client: WebSocket, ...args: any[]) {
    // The WsAuthGuard runs before this. If we are here, client.user is populated.
    const userId = (client as any).user?.sub;
    console.log(`✅ User connected: ${userId}`);

    this.connectedUsers.set(userId, client);
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
    @MessageBody() payload: { event: string; data: SendMessagePayload },
  ) {
    const senderId = (client as any).user.sub;
    console.log(`📨 Received message from ${senderId}:`, payload.data);

    // Echo it back to the sender to prove it works
    return {
      event: 'message_sent',
      data: {
        ...payload.data,
        senderId,
        status: 'sent',
      },
    };
  }
}
