import { Entity, PrimaryColumn } from 'typeorm';

// Minimal read-only mirror of the conversation_participants table owned by `api`.
// Only the columns needed by chat-gateway are mapped here.
// synchronize:true will NOT drop existing columns (role, joined_at) that are
// present in the DB but absent from this entity definition.
@Entity('conversation_participants')
export class ConversationParticipant {
  @PrimaryColumn({ name: 'conversation_id' })
  conversationId: string;

  @PrimaryColumn({ name: 'user_id' })
  userId: string;
}
