import { IsString, IsNotEmpty } from 'class-validator';

export class UpdateGroupDto {
  @IsString()
  @IsNotEmpty()
  title: string;
}
