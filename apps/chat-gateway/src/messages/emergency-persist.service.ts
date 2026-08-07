import { Injectable, Logger, OnModuleDestroy } from '@nestjs/common';
import { Client, types } from 'cassandra-driver';

/**
 * DEGRADED SRP EXCEPTION — BROKER-DOWN ONLY.
 *
 * Under the SRP split the gateway is producer-only: consumption, persistence
 * and receipt routing (the MSS role) live in the separate `mss` service, which
 * owns Cassandra and consumes chat-events from Kafka. This service is the one
 * sanctioned exception to that split: when Kafka is unavailable, the gateway's
 * message handler calls saveMessage() so sends keep working (NFR-3 — the
 * platform must not lose user messages because the broker is down).
 *
 * It does NOT create schema. The keyspace and tables are mss's responsibility —
 * this service only connects and inserts. The INSERT deliberately omits
 * client_message_id because a pre-mss schema may not have that column; if the
 * schema is missing, the insert fails loudly and the caller surfaces
 * PERSIST_FAILED rather than inventing schema here.
 */
@Injectable()
export class EmergencyPersistService implements OnModuleDestroy {
  private readonly logger = new Logger(EmergencyPersistService.name);
  private client: Client | undefined;
  private keyspace = process.env.CASSANDRA_KEYSPACE ?? 'chat_ks';

  /**
   * Persist a message directly to Cassandra (broker-down emergency path only).
   * Logs loudly and rethrows on any failure so the caller can surface
   * PERSIST_FAILED — the caller wraps this in try/catch.
   */
  async saveMessage(
    conversationId: string,
    senderId: string,
    content: string,
    messageId: string,
    createdAt: Date,
  ): Promise<void> {
    const client = await this.getClient();

    const query = `
      INSERT INTO ${this.keyspace}.messages (conversation_id, created_at, id, sender_id, content)
      VALUES (?, ?, ?, ?, ?)
    `;

    try {
      await client.execute(
        query,
        [conversationId, createdAt, types.TimeUuid.fromString(messageId), senderId, content],
        { prepare: true },
      );
    } catch (err) {
      this.logger.error(
        `❌ Emergency persist failed for message ${messageId} in conversation ${conversationId}: ${(err as Error).message}`,
      );
      throw err;
    }
  }

  onModuleDestroy(): Promise<void> {
    if (!this.client) {
      return Promise.resolve();
    }
    return this.client.shutdown().catch((err) => {
      this.logger.warn(`⚠️ Emergency Cassandra shutdown failed: ${(err as Error).message}`);
    });
  }

  /**
   * Connect lazily on first use and cache the client — in normal Kafka-first
   * operation this service never touches Cassandra. Logs loudly and rethrows
   * so the caller can surface PERSIST_FAILED.
   */
  private async getClient(): Promise<Client> {
    if (this.client) {
      return this.client;
    }

    const contactPoints = (process.env.CASSANDRA_CONTACT_POINTS ?? 'localhost').split(',');
    const localDataCenter = process.env.CASSANDRA_LOCAL_DC ?? 'datacenter1';
    this.keyspace = process.env.CASSANDRA_KEYSPACE ?? 'chat_ks';

    // Guard against env-driven CQL injection via CASSANDRA_KEYSPACE — it is
    // interpolated into the INSERT above, so it must be a bare identifier.
    if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(this.keyspace)) {
      const err = new Error(
        `Invalid CASSANDRA_KEYSPACE '${this.keyspace}' — must match /^[A-Za-z_][A-Za-z0-9_]*$/`,
      );
      this.logger.error(`❌ ${err.message}`);
      throw err;
    }

    const client = new Client({ contactPoints, localDataCenter });
    try {
      await client.connect();
    } catch (err) {
      // Never cache a half-connected client — the next attempt retries fresh.
      await client.shutdown().catch(() => undefined);
      this.logger.error(
        `❌ Emergency Cassandra connect failed at ${contactPoints.join(',')}: ${(err as Error).message}`,
      );
      throw err;
    }

    this.client = client;
    this.logger.warn(
      `⚠️  Emergency persistence active — connected to Cassandra at ${contactPoints.join(',')} (broker-down fallback)`,
    );
    return client;
  }
}
