import { NestFactory, HttpAdapterHost } from '@nestjs/core';
import { AppModule } from './app.module';
import { ValidationPipe } from '@nestjs/common';
import { HttpExceptionFilter } from './common/http-exception.filter';

async function bootstrap() {
  const app = await NestFactory.create(AppModule);

  app.enableCors({
    origin: process.env.CORS_ORIGIN ? process.env.CORS_ORIGIN.split(',') : true,
    credentials: true,
  });

  app.useGlobalFilters(new HttpExceptionFilter(app.get(HttpAdapterHost)));

  app.useGlobalPipes(new ValidationPipe({
    whitelist: true,      // strips any extra fields the client sends that aren't in the DTO
    transform: true,      // transforms payloads to DTO class instances
    // forbidNonWhitelisted: true,  // rejects the request entirely if extra fields are sent
  }));
  await app.listen(process.env.PORT ?? 3000);
}
bootstrap();
