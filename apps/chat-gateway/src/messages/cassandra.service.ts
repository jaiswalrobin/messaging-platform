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

    // 1. Connect WITHOUT keyspace first, so it doesn't crash if it doesn't exist yet
    this.client = new Client({
      contactPoints,
      localDataCenter,
    });

    this.logger.log(`Connecting to Cassandra at ${contactPoints.join(',')}`);

    // 2. Create keyspace and table
    await this.initSchema();
    
    this.logger.log(`✅ Cassandra keyspace '${this.keyspace}' and messages table initialized`);
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

    // Create table (prefix with keyspace name)
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
  }

  async saveMessage(
    conversationId: string,
    senderId: string,
    content: string,
  ): Promise<MessageRecord> {
    const id = types.TimeUuid.now();
    const createdAt = new Date();

    // Prefix table with keyspace
    const query = `
      INSERT INTO ${this.keyspace}.messages (conversation_id, created_at, id, sender_id, content)
      VALUES (?, ?, ?, ?, ?)
    `;

    await this.client.execute(query, [conversationId, createdAt, id, senderId, content], { prepare: true });

    return {
      id: id.toString(),
      conversationId,
      senderId,
      content,
      createdAt,
    };
  }

  async getMessages(conversationId: string, limit = 50): Promise<MessageRecord[]> {
    // Prefix table with keyspace
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
}
