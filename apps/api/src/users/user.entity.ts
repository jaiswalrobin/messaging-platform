import {
  Entity,
  PrimaryGeneratedColumn,
  Column,
  CreateDateColumn,
} from 'typeorm';

@Entity('users')
export class User {
  @PrimaryGeneratedColumn('uuid')
  id: string;

  @Column({ unique: true })
  email: string;

  // Excluded from default selects so the bcrypt hash isn't pulled on every
  // relational fetch (latent leak). Only auth.login selects it explicitly.
  @Column({ select: false })
  password: string;

  @CreateDateColumn()
  createdAt: Date;
}
