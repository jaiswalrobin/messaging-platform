import { Injectable, OnModuleInit, OnModuleDestroy, Logger } from '@nestjs/common';
import { Client, types } from 'cassandra-driver';

export interface MessageRecord {
  id: string;
  conversationId: string;
  senderId: string;
  content: string;
  createdAt: Date;
}

@Injectable()
export class CassandraService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(CassandraService.name);
  private client: Client;
  private keyspace: string;

  async onModuleInit() {
    const contactPoints = (process.env.CASSANDRA_CONTACT_POINTS ?? 'localhost').split(',');
    const localDataCenter = process.env.CASSANDRA_LOCAL_DC ?? 'datacenter1';
    this.keyspace = process.env.CASSANDRA_KEYSPACE ?? 'chat_ks';

    // Guard against env-driven CQL injection via CASSANDRA_KEYSPACE
    if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(this.keyspace)) {
      throw new Error(
        `Invalid CASSANDRA_KEYSPACE '${this.keyspace}' — must match /^[A-Za-z_][A-Za-z0-9_]*$/`,
      );
    }

    // 1. Connect WITHOUT keyspace first, so it doesn't crash if it doesn't exist yet
    this.client = new Client({
      contactPoints,
      localDataCenter,
    });

    this.logger.log(`Connecting to Cassandra at ${contactPoints.join(',')}`);

    // 2. Create keyspace and tables, retrying while Cassandra finishes booting.
    // Fail fast after 10 attempts — the gateway is useless without message storage.
    const maxAttempts = 10;
    for (let attempt = 1; attempt <= maxAttempts; attempt++) {
      try {
        await this.initSchema();
        break;
      } catch (err) {
        this.logger.error(
          `Cassandra init failed (attempt ${attempt}/${maxAttempts}): ${(err as Error).message}`,
        );
        if (attempt === maxAttempts) {
          throw err;
        }
        await new Promise((r) => setTimeout(r, 3000));
      }
    }

    this.logger.log(`✅ Cassandra keyspace '${this.keyspace}' and tables initialized`);
  }

  async onModuleDestroy() {
    await this.client.shutdown();
  }

  private async initSchema(): Promise<void> {
    // Create keyspace
    await this.client.execute(`
      CREATE KEYSPACE IF NOT EXISTS ${this.keyspace}
      WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
    `);

    // messages table: partitioned by conversation, ordered by time DESC
    await this.client.execute(`
      CREATE TABLE IF NOT EXISTS ${this.keyspace}.messages (
        conversation_id text,
        created_at timestamp,
        id timeuuid,
        sender_id text,
        content text,
        PRIMARY KEY (conversation_id, created_at, id)
      ) WITH CLUSTERING ORDER BY (created_at DESC, id DESC)
    `);

    // message_receipts table: tracks per-user delivery/read status
    // Partitioned by conversation_id for fast lookup.
    // Clustering by message_id and user_id.
    await this.client.execute(`
      CREATE TABLE IF NOT EXISTS ${this.keyspace}.message_receipts (
        conversation_id text,
        message_id text,
        user_id text,
        status text,
        updated_at timestamp,
        PRIMARY KEY (conversation_id, message_id, user_id)
      )
    `);
  }

  // ── Message Persistence ─────────────────────────────────────────────────────

  async saveMessage(
    conversationId: string,
    senderId: string,
    content: string,
    messageId?: string,
    createdAt?: Date,
  ): Promise<MessageRecord> {
    const id = messageId ? types.TimeUuid.fromString(messageId) : types.TimeUuid.now();
    const ts = createdAt ?? new Date();

    const query = `
      INSERT INTO ${this.keyspace}.messages (conversation_id, created_at, id, sender_id, content)
      VALUES (?, ?, ?, ?, ?)
    `;

    await this.client.execute(query, [conversationId, ts, id, senderId, content], { prepare: true });

    return {
      id: id.toString(),
      conversationId,
      senderId,
      content,
      createdAt: ts,
    };
  }

  async getMessages(conversationId: string, limit = 20): Promise<MessageRecord[]> {
    const query = `
      SELECT conversation_id, created_at, id, sender_id, content
      FROM ${this.keyspace}.messages
      WHERE conversation_id = ?
      LIMIT ?
    `;

    const result = await this.client.execute(query, [conversationId, limit], { prepare: true });

    return result.rows.map((row) => ({
      id: row.id.toString(),
      conversationId: row.conversation_id,
      senderId: row.sender_id,
      content: row.content,
      createdAt: row.created_at,
    }));
  }

  // ── Receipt Tracking ────────────────────────────────────────────────────────

  /**
   * Upsert a delivery or read receipt for a specific message and user.
   */
  async upsertReceipt(
    conversationId: string,
    messageId: string,
    userId: string,
    status: 'delivered' | 'read',
  ): Promise<void> {
    const query = `
      INSERT INTO ${this.keyspace}.message_receipts (conversation_id, message_id, user_id, status, updated_at)
      VALUES (?, ?, ?, ?, ?)
    `;
    await this.client.execute(
      query,
      [conversationId, messageId, userId, status, new Date()],
      { prepare: true },
    );
  }

  /**
   * Mark all messages in a conversation as read for a user up to a given message.
   */
  async markConversationRead(
    conversationId: string,
    userId: string,
    upToMessageId: string,
  ): Promise<void> {
    await this.upsertReceipt(conversationId, upToMessageId, userId, 'read');
  }

  /**
   * Get read receipts for a conversation (to show who read what).
   */
  async getReceipts(
    conversationId: string,
    messageId: string,
  ): Promise<{ userId: string; status: string; updatedAt: Date }[]> {
    const query = `
      SELECT user_id, status, updated_at
      FROM ${this.keyspace}.message_receipts
      WHERE conversation_id = ? AND message_id = ?
    `;
    const result = await this.client.execute(query, [conversationId, messageId], { prepare: true });

    return result.rows.map((row) => ({
      userId: row.user_id,
      status: row.status,
      updatedAt: row.updated_at,
    }));
  }
}
