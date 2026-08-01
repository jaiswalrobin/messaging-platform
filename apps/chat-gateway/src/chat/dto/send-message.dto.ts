import { IsString, IsNotEmpty, MaxLength } from 'class-validator';
import { MAX_MESSAGE_LENGTH } from '@chat/shared-types';

export class SendMessageDto {
  @IsString()
  @IsNotEmpty()
  conversationId: string;

  @IsString()
  @IsNotEmpty()
  @MaxLength(MAX_MESSAGE_LENGTH)
  content: string;

  @IsString()
  @IsNotEmpty()
  clientMessageId: string;
}
