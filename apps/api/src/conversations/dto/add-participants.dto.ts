import { IsArray, ArrayMinSize, IsString, ArrayUnique } from 'class-validator';

export class AddParticipantsDto {
  @IsArray()
  @ArrayMinSize(1)
  @IsString({ each: true })
  @ArrayUnique()
  participantIds: string[];
}
