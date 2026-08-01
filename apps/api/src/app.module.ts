import { Module } from '@nestjs/common';
import { TypeOrmModule } from '@nestjs/typeorm';
import { getTypeOrmConfig } from '@chat/shared-types';
import { AppController } from './app.controller';
import { UsersModule } from './users/users.module';
import { AuthModule } from './auth/auth.module';
import { ConversationsModule } from './conversations/conversations.module';
import { HealthController } from './health/health.controller';

@Module({
  imports: [
    // Never auto-sync outside development — it can drop/alter columns destructively.
    TypeOrmModule.forRoot(
      getTypeOrmConfig({ synchronize: process.env.NODE_ENV !== 'production' }),
    ),
    UsersModule,
    AuthModule,
    ConversationsModule,
  ],
  controllers: [AppController, HealthController],
})
export class AppModule { }
