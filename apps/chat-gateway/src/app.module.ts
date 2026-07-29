import { Module } from '@nestjs/common';
import { JwtModule } from '@nestjs/jwt';
import { TypeOrmModule } from '@nestjs/typeorm';
import { ChatGateway } from './chat/chat.gateway';
import { MessagesModule } from './messages/messages.module';
import { ParticipantsModule } from './participants/participants.module';
import { KafkaModule } from './kafka/kafka.module';

@Module({
  imports: [
    TypeOrmModule.forRoot({
      type: 'postgres',
      host: process.env.DB_HOST ?? 'localhost',
      port: parseInt(process.env.DB_PORT ?? '5432'),
      username: process.env.DB_USERNAME ?? 'admin',
      password: process.env.DB_PASSWORD ?? 'admin',
      database: process.env.DB_NAME ?? 'chat_db',
      autoLoadEntities: true,
      // synchronize: false — chat-gateway minimal ConversationParticipant entity
      // reads from DB owned by `api`. `api` handles schema synchronization/migrations.
      synchronize: false,
    }),
    JwtModule.register({
      // TODO: move to process.env.JWT_SECRET via ConfigModule
      secret: 'super-secret-key-for-local-dev-only',
      signOptions: { expiresIn: '7d' },
    }),
    MessagesModule,
    ParticipantsModule,
    KafkaModule,
  ],
  providers: [ChatGateway],
  // AppController and AppService (dead boilerplate) removed
})
export class AppModule {}
