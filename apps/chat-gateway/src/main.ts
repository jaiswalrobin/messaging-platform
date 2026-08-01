import { ValidationPipe } from '@nestjs/common';
import { NestFactory, HttpAdapterHost } from '@nestjs/core';
import { WsAdapter } from '@nestjs/platform-ws'; // Import the raw ws adapter
import { AppModule } from './app.module';
import { HttpExceptionFilter } from './common/http-exception.filter';

async function bootstrap() {
  const app = await NestFactory.create(AppModule);

  app.enableCors({
    origin: process.env.CORS_ORIGIN ? process.env.CORS_ORIGIN.split(',') : true,
    credentials: true,
  });

  app.useGlobalPipes(new ValidationPipe({ whitelist: true, transform: true }));
  app.useGlobalFilters(new HttpExceptionFilter(app.get(HttpAdapterHost)));

  // Tell NestJS to use the raw 'ws' library instead of socket.io
  app.useWebSocketAdapter(new WsAdapter(app));

  await app.listen(8080);
  console.log('🚀 Chat Gateway is running on ws://localhost:8080');
}
bootstrap();
