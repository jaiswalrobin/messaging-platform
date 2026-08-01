import { Injectable, UnauthorizedException } from '@nestjs/common';
import { PassportStrategy } from '@nestjs/passport';
import { ExtractJwt, Strategy } from 'passport-jwt';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository } from 'typeorm';
import { getJwtSecret } from '@chat/shared-types';
import { User } from '../users/user.entity';

@Injectable()
export class JwtStrategy extends PassportStrategy(Strategy) {
  constructor(
    @InjectRepository(User)
    private readonly userRepo: Repository<User>,
  ) {
    super({
      jwtFromRequest: ExtractJwt.fromAuthHeaderAsBearerToken(),
      ignoreExpiration: false,
      secretOrKey: getJwtSecret(),
    });
  }

  async validate(payload: any) {
    // Reject tokens whose subject no longer exists in the DB. A validly
    // signed token for a deleted account would otherwise pass auth and
    // explode on the first FK write (participant insert) with a 500.
    const user = await this.userRepo.findOne({
      where: { id: payload.sub },
      select: { id: true, email: true },
    });
    if (!user) {
      throw new UnauthorizedException('Account no longer exists');
    }
    return { userId: user.id, email: user.email };
  }
}
