import {
  IsString,
  IsArray,
  ArrayMinSize,
  ArrayMaxSize,
  IsNotEmpty,
  ArrayUnique,
} from 'class-validator';

export class CreateGroupDto {
  @IsString()
  @IsNotEmpty()
  title: string;

  @IsArray()
  @ArrayMinSize(1)
  @ArrayMaxSize(1000)
  @IsString({ each: true })
  @ArrayUnique()
  participantIds: string[];
}
