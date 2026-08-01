import { Module } from '@nestjs/common';
import { JwtModule } from '@nestjs/jwt';
import { getJwtSecret, JWT_EXPIRES_IN } from '@chat/shared-types';
import { CassandraService } from './cassandra.service';
import { MessagesController } from './messages.controller';
import { ParticipantsModule } from '../participants/participants.module';

@Module({
  imports: [
    JwtModule.register({
      secret: getJwtSecret(),
      signOptions: { expiresIn: JWT_EXPIRES_IN },
    }),
    ParticipantsModule,
  ],
  controllers: [MessagesController],
  providers: [CassandraService],
  exports: [CassandraService],
})
export class MessagesModule {}
