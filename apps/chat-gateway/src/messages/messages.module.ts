import { Module } from '@nestjs/common';
import { CassandraService } from './cassandra.service';
import { MessagesController } from './messages.controller';
import { ParticipantsModule } from '../participants/participants.module';
import { UsersModule } from '../users/users.module';

@Module({
  imports: [
    // JwtService comes from the app.module global JwtModule.register (getJwtSecret() runs once).
    ParticipantsModule,
    // Provides the User mirror repository that HttpJwtGuard re-validates against
    UsersModule,
  ],
  controllers: [MessagesController],
  providers: [CassandraService],
  exports: [CassandraService],
})
export class MessagesModule {}
