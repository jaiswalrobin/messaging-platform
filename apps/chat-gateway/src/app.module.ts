import { Module } from '@nestjs/common';
import { JwtModule } from '@nestjs/jwt';
import { ChatGateway } from './chat/chat.gateway';

@Module({
  imports: [
    JwtModule.register({
      // MUST match the API app secret
      secret: 'super-secret-key-for-local-dev-only', 
      signOptions: { expiresIn: '7d' },
    }),
  ],
  providers: [ChatGateway],
})
export class AppModule {}
