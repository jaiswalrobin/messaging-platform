import { Injectable } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository, ILike, Not } from 'typeorm';
import { User } from './user.entity';

/**
 * Minimum number of characters a search query must have before any lookup
 * runs. Shorter inputs return an empty result set immediately.
 */
const MIN_SEARCH_QUERY_LENGTH = 2;

/**
 * Upper bound on the number of users returned by a single search.
 */
const MAX_SEARCH_RESULTS = 10;

@Injectable()
export class UsersService {
  constructor(
    @InjectRepository(User)
    private userRepository: Repository<User>,
  ) {}

  async searchUsers(query: string, currentUserId: string) {
    if (!query || query.length < MIN_SEARCH_QUERY_LENGTH) {
      return [];
    }

    // Escape ILike wildcards so a search for `%` or `_` matches literally
    // instead of matching every row.
    const escaped = query.replace(/[%_]/g, (char) => `\\${char}`);

    const users = await this.userRepository.find({
      where: {
        email: ILike(`%${escaped}%`),
        id: Not(currentUserId),
      },
      select: { id: true, email: true },
      take: MAX_SEARCH_RESULTS,
    });

    return users;
  }
}
