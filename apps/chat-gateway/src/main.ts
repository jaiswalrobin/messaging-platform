import { Logger, ValidationPipe } from '@nestjs/common';
import { NestFactory, HttpAdapterHost } from '@nestjs/core';
import { WsAdapter } from '@nestjs/platform-ws'; // Import the raw ws adapter
import { getCorsConfig } from '@chat/shared-types';
import { AppModule } from './app.module';
import { HttpExceptionFilter } from './common/http-exception.filter';

const logger = new Logger('Bootstrap');

async function bootstrap() {
  const app = await NestFactory.create(AppModule);

  app.enableCors(getCorsConfig());

  app.useGlobalPipes(new ValidationPipe({ whitelist: true, transform: true }));
  app.useGlobalFilters(new HttpExceptionFilter(app.get(HttpAdapterHost)));

  // Tell NestJS to use the raw 'ws' library instead of socket.io
  app.useWebSocketAdapter(new WsAdapter(app));

  const port = process.env.PORT ?? 8080;
  await app.listen(port);
  logger.log(`🚀 Chat Gateway is running on ws://localhost:${port}`);
}
bootstrap();
