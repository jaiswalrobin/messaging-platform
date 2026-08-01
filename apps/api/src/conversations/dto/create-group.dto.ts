import { IsString, IsArray, ArrayMinSize, IsNotEmpty, ArrayUnique } from 'class-validator';

export class CreateGroupDto {
  @IsString()
  @IsNotEmpty()
  title: string;

  @IsArray()
  @ArrayMinSize(1)
  @IsString({ each: true })
  @ArrayUnique()
  participantIds: string[];
}
