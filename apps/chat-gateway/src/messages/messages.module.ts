import { Module } from '@nestjs/common';
import { CassandraService } from './cassandra.service';
import { MessagesController } from './messages.controller';

@Module({
  controllers: [MessagesController],
  providers: [CassandraService],
  exports: [CassandraService],
})
export class MessagesModule {}
