import { Injectable, Logger, OnModuleDestroy } from '@nestjs/common';
import { JwtService } from '@nestjs/jwt';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository } from 'typeorm';
import { WebSocket, Server } from 'ws';
import { ParticipantCacheService } from '../participants/participant-cache.service';
import { User } from '../users/user.entity';

/**
 * WS close code used when a connection is rejected for an authentication /
 * policy violation (no token, invalid token, or the account no longer exists).
 * 1008 = policy violation per RFC 6455.
 */
export const WS_CLOSE_POLICY_VIOLATION = 1008;

/** WS close reason sent alongside WS_CLOSE_POLICY_VIOLATION on auth rejection. */
export const WS_CLOSE_UNAUTHORIZED_REASON = 'Unauthorized';

/**
 * Owns the connection registry (who is connected, on which sockets), the
 * heartbeat, connection lifecycle, and the broadcastToParticipants fan-out
 * helper used by both the producer handlers and the MSS consumer.
 */
@Injectable()
export class ConnectionRegistryService implements OnModuleDestroy {
  private readonly logger = new Logger(ConnectionRegistryService.name);

  // Heartbeat cadence: ping every 15s, reap a socket after 2 missed pongs (~30s
  // of silence) — 75% of the typical proxy idle timeout, so zombie sockets are
  // reaped before they'd be counted as "online" and falsely receive fan-out.
  private static readonly HEARTBEAT_INTERVAL_MS = 15000;
  private static readonly MAX_MISSED_PONGS = 2;

  // In-memory connection registry: userId → Set of sockets (one per connected device)
  // Production: replace with Redis Connection Registry for multi-node routing
  private connectedUsers = new Map<string, Set<WebSocket>>();

  private heartbeat: NodeJS.Timeout;

  constructor(
    private readonly jwtService: JwtService,
    private readonly participantCache: ParticipantCacheService,
    @InjectRepository(User)
    private readonly userRepo: Repository<User>,
  ) {}

  /**
   * Wire the 30s heartbeat. The heartbeat lives here — not in the gateway —
   * because the registry owns the socket lifecycle; the gateway hands over its
   * @WebSocketServer() instance from afterInit.
   */
  attachServer(server: Server): void {
    server.on('connection', (ws) => {
      (ws as any).missedPongs = 0;
      // Swallow socket-level errors (e.g. reset mid-close) — the readyState
      // guards in send()/broadcast keep dead sockets inert instead of crashing
      // the process on an unhandled 'error' event.
      ws.on('error', () => {
        /* ignore */
      });
      ws.on('pong', () => {
        (ws as any).missedPongs = 0;
      });
    });

    this.heartbeat = setInterval(() => {
      for (const ws of server.clients) {
        if ((ws as any).missedPongs >= ConnectionRegistryService.MAX_MISSED_PONGS) {
          ws.terminate();
          continue;
        }
        (ws as any).missedPongs++;
        try {
          ws.ping();
        } catch {
          /* socket closed mid-check — terminated next round */
        }
      }
    }, ConnectionRegistryService.HEARTBEAT_INTERVAL_MS);
    this.heartbeat.unref();
  }

  onModuleDestroy() {
    clearInterval(this.heartbeat);
  }

  // ─── Connection lifecycle ───────────────────────────────────────────────────

  /** Verify the ?token= query param and register the socket. Closes WS_CLOSE_POLICY_VIOLATION on failure. */
  async handleConnection(client: any, request: any): Promise<void> {
    const url = new URL(request.url, 'http://localhost');
    const token = url.searchParams.get('token');

    if (!token) {
      this.logger.warn('❌ Rejected: no token');
      client.close(WS_CLOSE_POLICY_VIOLATION, WS_CLOSE_UNAUTHORIZED_REASON);
      return;
    }

    try {
      const jwtPayload = this.jwtService.verify(token) as { sub: string; email?: string };

      // Re-validate the account still exists in Postgres
      const user = await this.userRepo.findOne({ where: { id: jwtPayload.sub } });
      if (!user) {
        this.logger.warn(`❌ Rejected: user ${jwtPayload.sub} no longer exists`);
        client.close(WS_CLOSE_POLICY_VIOLATION, WS_CLOSE_UNAUTHORIZED_REASON);
        return;
      }

      // The client may have disconnected while we awaited the DB lookup — a
      // closed socket must never be registered (handleDisconnect already ran
      // and had no userId to clean up, so this would leak the dead socket).
      if (client.readyState !== WebSocket.OPEN) {
        this.logger.log(`🔌 ${jwtPayload.sub} disconnected during handshake — not registered`);
        return;
      }

      // Unify identity shape with HTTP: JWT { sub, email } → { userId, email }
      client.user = { userId: jwtPayload.sub, email: jwtPayload.email };
      // Per-connection id so the gateway/consumer can target (or exclude) this
      // specific socket instead of the whole user (contract: (client as any).socketId)
      (client as any).socketId = crypto.randomUUID();
      const userSockets = this.connectedUsers.get(jwtPayload.sub) ?? new Set<WebSocket>();
      userSockets.add(client);
      this.connectedUsers.set(jwtPayload.sub, userSockets);
      this.logger.log(`✅ Connected: ${jwtPayload.sub}`);
    } catch {
      this.logger.warn('❌ Rejected: invalid token');
      client.close(WS_CLOSE_POLICY_VIOLATION, WS_CLOSE_UNAUTHORIZED_REASON);
    }
  }

  handleDisconnect(client: WebSocket): void {
    const userId = (client as any).user?.userId;
    if (userId) {
      const userSockets = this.connectedUsers.get(userId);
      if (userSockets) {
        userSockets.delete(client);
        // Only drop the map entry when the last socket for this user closes,
        // otherwise device A's disconnect would ghost device B's mapping.
        if (userSockets.size === 0) {
          this.connectedUsers.delete(userId);
        }
      }
      this.logger.log(`🔌 Disconnected: ${userId}`);
    }
  }

  // ─── Helpers ────────────────────────────────────────────────────────────────

  send(socket: WebSocket, event: string, data: unknown): void {
    if (socket.readyState !== WebSocket.OPEN) return;
    try {
      socket.send(JSON.stringify({ event, data }));
    } catch (err) {
      this.logger.warn(`⚠️ Socket write failed: ${(err as Error).message}`);
    }
  }

  /** Send an event frame to every online socket of `userId`, optionally excluding the socket with id `excludeSocketId`. Returns the number of sockets reached. */
  sendToUserSockets(userId: string, event: string, data: unknown, excludeSocketId?: string): number {
    const sockets = this.connectedUsers.get(userId);
    if (!sockets || sockets.size === 0) {
      return 0;
    }

    let reached = 0;
    for (const socket of sockets) {
      if (socket.readyState !== WebSocket.OPEN) {
        continue;
      }
      if ((socket as any).socketId === excludeSocketId) {
        continue;
      }
      this.send(socket, event, data);
      reached++;
    }
    return reached;
  }

  /**
   * Membership check (IDOR prevention). Resolves true when the user is a
   * participant; otherwise sends the unified FORBIDDEN error frame (always
   * carrying conversationId, plus clientMessageId when provided) and resolves
   * false. Never throws. A failed membership lookup (both Redis and Postgres
   * down at once) is logged loudly and treated as a member — fail-open — so the
   * message can still be persisted to Cassandra and delivered rather than
   * dropped. Tradeoff: a brief IDOR window for the duration of a total
   * cache+DB outage.
   */
  async requireMember(
    conversationId: string,
    userId: string,
    client: WebSocket,
    extra?: { clientMessageId?: string },
  ): Promise<boolean> {
    try {
      const isMember = await this.participantCache.isMember(conversationId, userId);
      if (!isMember) {
        this.sendForbiddenFrame(client, conversationId, extra?.clientMessageId);
        return false;
      }
      return true;
    } catch (err) {
      this.logger.error(
        `❌ Membership check failed for ${userId} in ${conversationId}: ${(err as Error).message} — failing OPEN, treating as member`,
      );
      return true;
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

  /**
   * Send an event frame to all online sockets of every participant in a
   * conversation except `excludeUserId`. Returns the number of distinct
   * recipients reached (counted per user, not per device).
   */
  async broadcastToParticipants(
    conversationId: string,
    excludeUserId: string,
    event: string,
    data: unknown,
  ): Promise<number> {
    const participantIds = await this.participantCache.getParticipants(conversationId);
    const recipientIds = participantIds.filter((id) => id !== excludeUserId);

    let deliveredCount = 0;

    // All per-recipient work is synchronous (registry lookup + socket.send) —
    // a plain loop avoids allocating a Promise per recipient.
    for (const recipientId of recipientIds) {
      const recipientSockets = this.connectedUsers.get(recipientId);

      if (!recipientSockets || recipientSockets.size === 0) {
        // Offline recipient → dropped; no offline queue yet (BullMQ comes in Phase 4)
        this.logger.log(`📭 ${recipientId} offline — dropped (no queue yet)`);
        continue;
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
        this.logger.log(`📭 ${recipientId} offline — dropped (no queue yet)`);
      }
    }

    return deliveredCount;
  }
}
