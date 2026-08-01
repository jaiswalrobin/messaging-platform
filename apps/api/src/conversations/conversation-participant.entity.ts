import {
  Entity,
  Column,
  ManyToOne,
  JoinColumn,
  CreateDateColumn,
  PrimaryColumn,
} from 'typeorm';
import { Conversation } from './conversation.entity';
import { User } from '../users/user.entity';

/** Role of a participant within a conversation. */
export type ParticipantRole = 'admin' | 'member';

@Entity('conversation_participants')
export class ConversationParticipant {
  @PrimaryColumn('uuid', { name: 'conversation_id' })
  conversationId: string;

  @PrimaryColumn('uuid', { name: 'user_id' })
  userId: string;

  @Column({ default: 'member' })
  role: ParticipantRole;

  @CreateDateColumn({ name: 'joined_at' })
  joinedAt: Date;

  /** Read-receipt watermark: highest message id this participant has read (null = never read). */
  @Column({ name: 'last_read_message_id', type: 'uuid', nullable: true })
  lastReadMessageId: string | null;

  @ManyToOne(() => Conversation, (c) => c.participants)
  @JoinColumn({ name: 'conversation_id' })
  conversation: Conversation;

  @ManyToOne(() => User)
  @JoinColumn({ name: 'user_id' })
  user: User;
}
