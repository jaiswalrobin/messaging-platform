import { Injectable, OnModuleInit, OnModuleDestroy, Logger } from '@nestjs/common';
import { Client, types } from 'cassandra-driver';

export interface MessageRecord {
  id: string;
  conversationId: string;
  senderId: string;
  content: string;
  createdAt: Date;
  clientMessageId?: string;
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
    await this.client?.shutdown();
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
        client_message_id text,
        PRIMARY KEY (conversation_id, created_at, id)
      ) WITH CLUSTERING ORDER BY (created_at DESC, id DESC)
    `);

    // Idempotent migration for tables created before client_message_id existed.
    // Cassandra 4.1 has NO `ADD COLUMN IF NOT EXISTS`, so check system_schema
    // and ALTER only when the column is actually absent (bare ALTER throws if
    // the column already exists — e.g. on a re-run after a partial migration).
    const columnExists = await this.client.execute(
      `SELECT column_name FROM system_schema.columns
       WHERE keyspace_name = ? AND table_name = 'messages' AND column_name = 'client_message_id'`,
      [this.keyspace],
    );
    if (columnExists.rowLength === 0) {
      // NOTE: this Cassandra build rejects `ADD COLUMN ... text` (the COLUMN
      // keyword is not accepted here) — the working form is a bare `ADD`.
      await this.client.execute(
        `ALTER TABLE ${this.keyspace}.messages ADD client_message_id text`,
      );
    }

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

  // ── Liveness ───────────────────────────────────────────────────────────────

  /**
   * Liveness probe: can we execute a trivial query? Never throws.
   */
  async isHealthy(): Promise<boolean> {
    try {
      await Promise.race([
        this.client.execute('SELECT now() FROM system.local'),
        new Promise((_, reject) =>
          setTimeout(() => reject(new Error('Cassandra probe timed out')), 2000),
        ),
      ]);
      return true;
    } catch {
      return false;
    }
  }

  // ── Message Persistence ─────────────────────────────────────────────────────

  async saveMessage(
    conversationId: string,
    senderId: string,
    content: string,
    messageId?: string,
    createdAt?: Date,
    clientMessageId?: string,
  ): Promise<MessageRecord> {
    const id = messageId ? types.TimeUuid.fromString(messageId) : types.TimeUuid.now();
    const ts = createdAt ?? new Date();

    const query = `
      INSERT INTO ${this.keyspace}.messages (conversation_id, created_at, id, sender_id, content, client_message_id)
      VALUES (?, ?, ?, ?, ?, ?)
    `;

    await this.client.execute(
      query,
      [conversationId, ts, id, senderId, content, clientMessageId ?? null],
      { prepare: true },
    );

    return {
      id: id.toString(),
      conversationId,
      senderId,
      content,
      createdAt: ts,
      clientMessageId,
    };
  }

  async getMessages(conversationId: string, limit = 20): Promise<MessageRecord[]> {
    const query = `
      SELECT conversation_id, created_at, id, sender_id, content, client_message_id
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
      clientMessageId: row.client_message_id,
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
   * Write a SINGLE read-receipt watermark row: records that `userId` has read up
   * to (and including) `upToMessageId`. It does NOT enumerate or mark every
   * message as read — the FE infers earlier messages as read from this watermark.
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

  /**
   * Batch-fetch delivery/read status for many messages in a conversation.
   * Keyed by message_id; each value is the receipts for that message.
   * Handles the empty-array case (returns {}).
   */
  async getReceiptsForMessages(
    conversationId: string,
    messageIds: string[],
  ): Promise<Record<string, Array<{ userId: string; status: 'delivered' | 'read' }>>> {
    if (messageIds.length === 0) {
      return {};
    }

    const query = `
      SELECT message_id, user_id, status
      FROM ${this.keyspace}.message_receipts
      WHERE conversation_id = ? AND message_id IN (${messageIds.map(() => '?').join(', ')})
    `;

    const result = await this.client.execute(
      query,
      [conversationId, ...messageIds],
      { prepare: true },
    );

    const receipts: Record<string, Array<{ userId: string; status: 'delivered' | 'read' }>> = {};
    for (const row of result.rows) {
      const messageId = row.message_id;
      if (!receipts[messageId]) {
        receipts[messageId] = [];
      }
      receipts[messageId].push({ userId: row.user_id, status: row.status });
    }
    return receipts;
  }
}
