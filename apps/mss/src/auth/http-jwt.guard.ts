import {
  CanActivate,
  ExecutionContext,
  Injectable,
  InternalServerErrorException,
  Logger,
  UnauthorizedException,
} from '@nestjs/common';
import { JwtService } from '@nestjs/jwt';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository } from 'typeorm';
import { User } from '../users/user.entity';

@Injectable()
export class HttpJwtGuard implements CanActivate {
  private readonly logger = new Logger(HttpJwtGuard.name);

  constructor(
    private readonly jwtService: JwtService,
    @InjectRepository(User)
    private readonly userRepo: Repository<User>,
  ) {}

  async canActivate(context: ExecutionContext): Promise<boolean> {
    const request = context.switchToHttp().getRequest();
    const authHeader: string | undefined = request.headers?.authorization;

    if (!authHeader?.startsWith('Bearer ')) {
      throw new UnauthorizedException('Missing or invalid Authorization header');
    }

    const token = authHeader.slice('Bearer '.length);

    let payload: { sub: string; email?: string };
    try {
      // Only JWT verification happens here: a malformed/expired token throws.
      payload = this.jwtService.verify(token) as { sub: string; email?: string };
    } catch {
      throw new UnauthorizedException('Invalid or expired token');
    }

    // Re-validate the account still exists in Postgres (the api owns the users
    // table — this is a read-only mirror). A deleted user's token must not keep
    // hitting protected endpoints. A DB outage is NOT an invalid token — it is a
    // server error, so it must not silently log the user out.
    let user: User | null;
    try {
      user = await this.userRepo.findOne({ where: { id: payload.sub } });
    } catch (err) {
      this.logger.error(`❌ userRepo lookup failed during auth: ${(err as Error).message}`);
      throw new InternalServerErrorException('Unable to validate user at this time');
    }
    if (!user) {
      throw new UnauthorizedException('User no longer exists');
    }

    request.user = { userId: payload.sub, email: payload.email };
    return true;
  }
}
