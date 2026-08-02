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

  constructor(private readonly httpAdapterHost: HttpAdapterHost) {}

  catch(exception: unknown, host: ArgumentsHost) {
    const { httpAdapter } = this.httpAdapterHost;
    const ctx = host.switchToHttp();

    const status =
      exception instanceof HttpException
        ? exception.getStatus()
        : HttpStatus.INTERNAL_SERVER_ERROR;

    // Never leak the stack trace (or any internal detail) to the client.
    const message =
      exception instanceof HttpException
        ? ((exception.getResponse() as any).message ?? exception.message)
        : 'Internal server error';

    this.logger.error(
      `HTTP ${status} error: ${message}`,
      exception instanceof Error ? exception.stack : undefined,
    );

    // Framework-agnostic reply via the injected adapter — no express `Response`
    // casting needed.
    httpAdapter.reply(
      ctx.getResponse(),
      { statusCode: status, message },
      status,
    );
  }
}
