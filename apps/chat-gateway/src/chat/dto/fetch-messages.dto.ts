import { IsString, IsNotEmpty, IsOptional, IsInt, Min, Max } from 'class-validator';
import { Type } from 'class-transformer';
import { MAX_HISTORY_LIMIT } from '@chat/shared-types';

export class FetchMessagesDto {
  @IsString()
  @IsNotEmpty()
  conversationId: string;

  @IsOptional()
  @IsInt()
  @Min(1)
  @Max(MAX_HISTORY_LIMIT)
  @Type(() => Number)
  limit?: number;
}
