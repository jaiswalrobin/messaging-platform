import {
  Entity,
  PrimaryGeneratedColumn,
  Column,
  CreateDateColumn,
  UpdateDateColumn,
  OneToMany,
} from 'typeorm';
import { ConversationParticipant } from './conversation-participant.entity';

@Entity('conversations')
export class Conversation {
  @PrimaryGeneratedColumn('uuid')
  id: string;

  @Column({ nullable: true })
  title: string;

  @Column({ default: 'direct' })
  type: string;

  @Column({ name: 'direct_key', type: 'varchar', nullable: true, unique: true })
  directKey: string | null;

  @CreateDateColumn({ name: 'created_at' })
  createdAt: Date;

  @UpdateDateColumn({ name: 'updated_at' })
  updatedAt: Date;

  @OneToMany(() => ConversationParticipant, (cp) => cp.conversation)
  participants: ConversationParticipant[];
}
