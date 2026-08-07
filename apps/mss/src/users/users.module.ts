import { Module } from '@nestjs/common';
import { TypeOrmModule } from '@nestjs/typeorm';
import { User } from './user.entity';

@Module({
  imports: [TypeOrmModule.forFeature([User])],
  // Must export the repo registration: HttpJwtGuard (used by MessagesController,
  // which imports this module) injects Repository<User>.
  exports: [TypeOrmModule],
})
export class UsersModule {}
