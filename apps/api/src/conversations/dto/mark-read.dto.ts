import { IsString, IsNotEmpty } from 'class-validator';

export class MarkReadDto {
  @IsString()
  @IsNotEmpty()
  userId: string;

  @IsString()
  @IsNotEmpty()
  lastReadMessageId: string;
}
