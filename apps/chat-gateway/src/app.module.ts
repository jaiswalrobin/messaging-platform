import { Module } from '@nestjs/common';
import { JwtModule } from '@nestjs/jwt';
import { TypeOrmModule } from '@nestjs/typeorm';
import { getJwtSecret, getTypeOrmConfig, JWT_EXPIRES_IN } from '@chat/shared-types';
import { ChatGateway } from './chat/chat.gateway';
import { ChatConsumerService } from './chat/chat-consumer.service';
import { ConnectionRegistryService } from './chat/connection-registry.service';
import { HealthController } from './health/health.controller';
import { InvalidateController } from './internal/invalidate.controller';
import { ApiClientService } from './internal/api-client.service';
import { UsersModule } from './users/users.module';
import { MessagesModule } from './messages/messages.module';
import { ParticipantsModule } from './participants/participants.module';
import { KafkaModule } from './kafka/kafka.module';

@Module({
  imports: [
    // synchronize: false — chat-gateway minimal ConversationParticipant entity
    // reads from DB owned by `api`. `api` handles schema synchronization/migrations.
    TypeOrmModule.forRoot(getTypeOrmConfig({ synchronize: false })),
    // global:true makes JwtService injectable app-wide, so no child module needs
    // its own JwtModule.register (getJwtSecret() therefore runs exactly once).
    JwtModule.register({
      global: true,
      secret: getJwtSecret(),
      signOptions: { expiresIn: JWT_EXPIRES_IN },
    }),
    UsersModule,
    MessagesModule,
    ParticipantsModule,
    KafkaModule,
  ],
  controllers: [HealthController, InvalidateController],
  providers: [ChatGateway, ApiClientService, ConnectionRegistryService, ChatConsumerService],
  // AppController and AppService (dead boilerplate) removed
})
export class AppModule {}
