import { IsString, IsNotEmpty } from 'class-validator';

export class MarkReadDto {
  @IsString()
  @IsNotEmpty()
  conversationId: string;

  /** Highest message id the reader has seen — the read watermark. */
  @IsString()
  @IsNotEmpty()
  lastReadMessageId: string;
}
