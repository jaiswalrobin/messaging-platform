import { Entity, PrimaryColumn, Column, CreateDateColumn } from 'typeorm';

@Entity('conversation_participants')
export class ConversationParticipant {
  @PrimaryColumn('uuid', { name: 'conversation_id' })
  conversationId: string;

  @PrimaryColumn('uuid', { name: 'user_id' })
  userId: string;

  @Column({ default: 'member' })
  role: string;

  @CreateDateColumn({ name: 'joined_at' })
  joinedAt: Date;
}
