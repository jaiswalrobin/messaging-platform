import { Module } from '@nestjs/common';
import { JwtModule } from '@nestjs/jwt';
import { TypeOrmModule } from '@nestjs/typeorm';
import { getJwtSecret, getTypeOrmConfig, JWT_EXPIRES_IN } from '@chat/shared-types';
import { ChatConsumerService } from './chat/chat-consumer.service';
import { DeliveryPublisherService } from './chat/delivery-publisher.service';
import { CassandraService } from './messages/cassandra.service';
import { ApiClientService } from './internal/api-client.service';
import { MssKafkaService } from './kafka/mss-kafka.service';
import { HealthController } from './health/health.controller';
import { MessagesController } from './messages/messages.controller';
import { ParticipantsModule } from './participants/participants.module';
import { UsersModule } from './users/users.module';

@Module({
  imports: [
    // synchronize: false — mss's minimal ConversationParticipant mirror entity
    // reads from the DB owned by `api` (same contract as chat-gateway). `api`
    // handles schema synchronization/migrations. MSS never writes Postgres —
    // cross-service writes (read watermarks) go through internal HTTP endpoints.
    TypeOrmModule.forRoot(getTypeOrmConfig({ synchronize: false })),
    // global:true makes JwtService injectable app-wide, so HttpJwtGuard needs no
    // per-module JwtModule.register (getJwtSecret() therefore runs exactly once).
    JwtModule.register({
      global: true,
      secret: getJwtSecret(),
      signOptions: { expiresIn: JWT_EXPIRES_IN },
    }),
    // Provides the User mirror repository that HttpJwtGuard re-validates against
    UsersModule,
    ParticipantsModule,
  ],
  controllers: [HealthController, MessagesController],
  providers: [
    MssKafkaService,
    ChatConsumerService,
    DeliveryPublisherService,
    CassandraService,
    ApiClientService,
  ],
})
export class AppModule {}
