import { Injectable } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository, ILike, Not } from 'typeorm';
import { User } from './user.entity';

@Injectable()
export class UsersService {
  constructor(
    @InjectRepository(User)
    private userRepository: Repository<User>,
  ) {}

  async searchUsers(query: string, currentUserId: string) {
    if (!query || query.length < 2) {
      return [];
    }

    const users = await this.userRepository.find({
      where: {
        email: ILike(`%${query}%`),
        id: Not(currentUserId),
      },
      select: { id: true, email: true },
      take: 10,
    });

    return users;
  }
}
