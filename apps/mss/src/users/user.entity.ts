import { Entity, PrimaryColumn, Column } from 'typeorm';

/**
 * Minimal mirror of the api-owned `users` table (synchronize: false — the api
 * owns schema and migrations). Used only to re-validate that a JWT's subject
 * still has an account; deleted accounts' tokens must not keep working.
 */
@Entity('users')
export class User {
  @PrimaryColumn('uuid')
  id: string;

  @Column()
  email: string;
}
