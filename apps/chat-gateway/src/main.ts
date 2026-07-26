import { NestFactory } from '@nestjs/core';
import { WsAdapter } from '@nestjs/platform-ws'; // Import the raw ws adapter
import { AppModule } from './app.module';

async function bootstrap() {
  const app = await NestFactory.create(AppModule);

  // Tell NestJS to use the raw 'ws' library instead of socket.io
  app.useWebSocketAdapter(new WsAdapter(app));

  await app.listen(8081);
  console.log('🚀 Chat Gateway is running on ws://localhost:8080');
}
bootstrap();
