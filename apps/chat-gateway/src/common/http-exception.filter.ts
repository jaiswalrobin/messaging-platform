import { ArgumentsHost, Catch, ExceptionFilter, HttpException, Logger } from '@nestjs/common';
import { HttpAdapterHost } from '@nestjs/core';

@Catch()
export class HttpExceptionFilter implements ExceptionFilter {
  private readonly logger = new Logger(HttpExceptionFilter.name);

  constructor(private readonly httpAdapterHost: HttpAdapterHost) {}

  catch(exception: unknown, host: ArgumentsHost): void {
    const { httpAdapter } = this.httpAdapterHost;

    const status = exception instanceof HttpException ? exception.getStatus() : 500;
    const body = {
      statusCode: status,
      message:
        exception instanceof HttpException
          ? ((exception.getResponse() as any).message ?? exception.message)
          : 'Internal server error',
    };

    this.logger.error(`HTTP ${status}: ${body.message}`);

    httpAdapter.reply(host.switchToHttp().getResponse(), body, status);
  }
}
