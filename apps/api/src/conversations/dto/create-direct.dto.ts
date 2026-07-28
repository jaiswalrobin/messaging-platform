import { IsString, IsNotEmpty } from 'class-validator';

export class CreateDirectDto {
  @IsString()
  @IsNotEmpty()
  targetUserId: string;
}
