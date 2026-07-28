import { Module } from '@nestjs/common';
import { JwtModule } from '@nestjs/jwt';
import { TypeOrmModule } from '@nestjs/typeorm';
import { ChatGateway } from './chat/chat.gateway';
import { MessagesModule } from './messages/messages.module';
import { ParticipantsModule } from './participants/participants.module';

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
      // synchronize: true — chat-gateway minimal ConversationParticipant entity
      // only maps (conversation_id, user_id) as read-only.
      // Message persistence is handled by CassandraService.
      synchronize: true,
    }),
    JwtModule.register({
      // TODO: move to process.env.JWT_SECRET via ConfigModule
      secret: 'super-secret-key-for-local-dev-only',
      signOptions: { expiresIn: '7d' },
    }),
    MessagesModule,
    ParticipantsModule,
  ],
  providers: [ChatGateway],
  // AppController and AppService (dead boilerplate) removed
})
export class AppModule {}
