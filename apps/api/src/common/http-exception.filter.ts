import {
  ArgumentsHost,
  Catch,
  ExceptionFilter,
  HttpException,
  HttpStatus,
  Logger,
} from '@nestjs/common';
import { HttpAdapterHost } from '@nestjs/core';
import type { Response } from 'express';

@Catch()
export class HttpExceptionFilter implements ExceptionFilter {
  private readonly logger = new Logger(HttpExceptionFilter.name);

  constructor(private readonly httpAdapterHost: HttpAdapterHost) {}

  catch(exception: unknown, host: ArgumentsHost) {
    const ctx = host.switchToHttp();
    const reply = ctx.getResponse<Response>();

    const status =
      exception instanceof HttpException
        ? exception.getStatus()
        : HttpStatus.INTERNAL_SERVER_ERROR;

    // Never leak the stack trace (or any internal detail) to the client.
    const message =
      exception instanceof HttpException
        ? (exception.getResponse() as any).message ?? exception.message
        : 'Internal server error';

    this.logger.error(
      `HTTP ${status} error: ${message}`,
      exception instanceof Error ? exception.stack : undefined,
    );

    reply.status(status).send({
      statusCode: status,
      message,
    });
  }
}
