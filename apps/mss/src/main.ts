import { Logger } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import { AppModule } from './app.module';

const logger = new Logger('Bootstrap');

async function bootstrap() {
  const app = await NestFactory.create(AppModule);

  // Graceful shutdown: close the Cassandra client, Redis, and Kafka consumer
  // when the process receives SIGTERM/SIGINT.
  app.enableShutdownHooks();

  const port = process.env.MSS_PORT ?? 8081;
  await app.listen(port);
  logger.log(`🚀 MSS (Message Storage Service) is running on http://localhost:${port}`);
}
bootstrap().catch((err) => {
  // A boot failure (Cassandra init exhausted, Kafka never reachable) must be a
  // clean, logged exit — not an unhandled rejection.
  console.error('Failed to start mss', err);
  process.exit(1);
});
