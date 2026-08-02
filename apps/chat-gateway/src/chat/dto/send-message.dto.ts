import { IsString, IsNotEmpty, MaxLength, Matches } from 'class-validator';
import { MAX_MESSAGE_LENGTH } from '@chat/shared-types';

export class SendMessageDto {
  @IsString()
  @IsNotEmpty()
  conversationId: string;

  @IsString()
  @IsNotEmpty()
  @Matches(/\S/, { message: 'content must contain a non-whitespace character' })
  @MaxLength(MAX_MESSAGE_LENGTH)
  content: string;

  @IsString()
  @IsNotEmpty()
  clientMessageId: string;
}
