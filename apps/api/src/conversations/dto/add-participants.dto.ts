import {
  IsArray,
  ArrayMinSize,
  ArrayMaxSize,
  IsString,
  ArrayUnique,
} from 'class-validator';

export class AddParticipantsDto {
  @IsArray()
  @ArrayMinSize(1)
  @ArrayMaxSize(1000)
  @IsString({ each: true })
  @ArrayUnique()
  participantIds: string[];
}
