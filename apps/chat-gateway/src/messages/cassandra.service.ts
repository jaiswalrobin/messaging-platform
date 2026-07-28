import { Injectable, OnModuleInit, OnModuleDestroy, Logger } from '@nestjs/common';
import { Client, types } from 'cassandra-driver';

export interface MessageRecord {
  id: types.TimeUuid | string;
  conversationId: string;
  senderId: string;
  content: string;
  createdAt: Date;
}

@Injectable()
export class CassandraService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(CassandraService.name);
  private client: Client;

  onModuleInit() {
    const contactPoints = (process.env.CASSANDRA_CONTACT_POINTS ?? 'localhost').split(',');
    const localDataCenter = process.env.CASSANDRA_LOCAL_DC ?? 'datacenter1';
    const keyspace = process.env.CASSANDRA_KEYSPACE ?? 'chat_ks';

    this.client = new Client({
      contactPoints,
      localDataCenter,
      keyspace,
    });

    this.logger.log(`Connecting to Cassandra at ${contactPoints.join(',')} (Keyspace: ${keyspace})`);
  }

  async onModuleDestroy() {
    await this.client.shutdown();
  }

  /**
   * Initialize Keyspace and Messages table partitioned by conversation_id time-ordered by created_at.
   */
  async initSchema(): Promise<void> {
    const keyspace = process.env.CASSANDRA_KEYSPACE ?? 'chat_ks';
    
    // Create keyspace if not exists
    await this.client.execute(`
      CREATE KEYSPACE IF NOT EXISTS ${keyspace}
      WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
    `);

    // Create time-ordered messages table
    await this.client.execute(`
      CREATE TABLE IF NOT EXISTS messages (
        conversation_id text,
        created_at timestamp,
        id timeuuid,
        sender_id text,
        content text,
        PRIMARY KEY (conversation_id, created_at, id)
      ) WITH CLUSTERING ORDER BY (created_at DESC, id DESC);
    `);

    this.logger.log('✅ Cassandra keyspace and messages table initialized');
  }

  /**
   * Insert a new message into Cassandra.
   */
  async saveMessage(
    conversationId: string,
    senderId: string,
    content: string,
  ): Promise<MessageRecord> {
    const id = types.TimeUuid.now();
    const createdAt = new Date();

    const query = `
      INSERT INTO messages (conversation_id, created_at, id, sender_id, content)
      VALUES (?, ?, ?, ?, ?)
    `;

    await this.client.execute(
      query,
      [conversationId, createdAt, id, senderId, content],
      { prepare: true },
    );

    return {
      id: id.toString(),
      conversationId,
      senderId,
      content,
      createdAt,
    };
  }

  /**
   * Fetch messages for a conversation time-ordered with pagination limit.
   */
  async getMessages(conversationId: string, limit = 50): Promise<MessageRecord[]> {
    const query = `
      SELECT conversation_id, created_at, id, sender_id, content
      FROM messages
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
}
