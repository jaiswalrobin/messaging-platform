import {
  Injectable,
  UnauthorizedException,
  ConflictException,
} from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository } from 'typeorm';
import { JwtService } from '@nestjs/jwt';
import * as bcrypt from 'bcrypt';
import { UserAuthResponse } from '@chat/shared-types';
import { User } from '../users/user.entity';

@Injectable()
export class AuthService {
  constructor(
    @InjectRepository(User)
    private userRepository: Repository<User>,
    private jwtService: JwtService,
  ) {}

  async register(email: string, password: string): Promise<UserAuthResponse> {
    // Normalize to lowercase so `User@X.com` and `user@x.com` can't create
    // duplicate accounts or hijack a conversation lookup.
    const normalizedEmail = email.trim().toLowerCase();
    const existingUser = await this.userRepository.findOne({
      where: { email: normalizedEmail },
    });
    if (existingUser) {
      throw new ConflictException('User already exists');
    }

    // bcrypt auto-generates the salt when given rounds — one async step instead of two.
    const passwordHash = await bcrypt.hash(password, 10);

    const user = this.userRepository.create({
      email: normalizedEmail,
      password: passwordHash,
    });

    let savedUser: User;
    try {
      savedUser = await this.userRepository.save(user);
    } catch (err: any) {
      // Postgres unique-violation on email (23505) — race between the pre-check
      // above and the insert. Surface it as a 409 instead of a 500.
      if (err?.code === '23505') {
        throw new ConflictException('User already exists');
      }
      throw err;
    }

    const token = this.generateToken(savedUser);

    return { userId: savedUser.id, token };
  }

  async login(email: string, password: string): Promise<UserAuthResponse> {
    // Match the normalized form used at registration.
    const normalizedEmail = email.trim().toLowerCase();
    const user = await this.userRepository
      .createQueryBuilder('user')
      .addSelect('user.password') // password column is select:false — load it only for login verification
      .where('user.email = :email', { email: normalizedEmail })
      .getOne();
    if (!user) {
      throw new UnauthorizedException('Invalid credentials');
    }

    const isPasswordValid = await bcrypt.compare(
      password,
      user.password,
    );
    if (!isPasswordValid) {
      throw new UnauthorizedException('Invalid credentials');
    }

    const token = this.generateToken(user);
    return { userId: user.id, token };
  }

  private generateToken(user: User) {
    const payload = { sub: user.id, email: user.email };
    return this.jwtService.sign(payload);
  }
}
