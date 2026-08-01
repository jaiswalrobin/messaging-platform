import {
  ArgumentsHost,
  Catch,
  ExceptionFilter,
  HttpException,
  HttpStatus,
  Logger,
} from '@nestjs/common';
import { HttpAdapterHost } from '@nestjs/core';

@Catch()
export class HttpExceptionFilter implements ExceptionFilter {
  private readonly logger = new Logger(HttpExceptionFilter.name);

  // Constructor signature is owned by main.ts (which passes the HttpAdapterHost)
  constructor(private readonly httpAdapterHost: HttpAdapterHost) {}

  catch(exception: unknown, host: ArgumentsHost): void {
    const { httpAdapter } = this.httpAdapterHost;

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

    httpAdapter.reply(
      host.switchToHttp().getResponse(),
      { statusCode: status, message },
      status,
    );
  }
}
