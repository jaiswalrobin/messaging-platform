import { NestFactory } from '@nestjs/core';
import { AppModule } from './app.module';
import { ValidationPipe } from '@nestjs/common';

async function bootstrap() {
  const app = await NestFactory.create(AppModule);

  app.enableCors({
    origin: true,
    credentials: true,
  });

  app.useGlobalPipes(new ValidationPipe({
    whitelist: true,      // strips any extra fields the client sends that aren't in the DTO
    // forbidNonWhitelisted: true,  // rejects the request entirely if extra fields are sent
  }));
  await app.listen(process.env.PORT ?? 3000);
}
bootstrap();
