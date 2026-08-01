import { IsString, IsNotEmpty } from 'class-validator';

/**
 * Client→server delivery acknowledgment. The recipient's client sends this once
 * it has actually received AND processed a `message_received` frame — proving the
 * message reached the user's app, not merely an open socket. The gateway uses it
 * to route a `message_delivered` receipt back to the original sender, so the
 * sender's gray ✓✓ only appears on genuine end-to-end delivery (not when the
 * socket happens to be open but the browser is throttled/offline).
 */
export class MessageDeliveredDto {
  @IsString()
  @IsNotEmpty()
  conversationId: string;

  /** The server-assigned id of the message that was delivered. */
  @IsString()
  @IsNotEmpty()
  messageId: string;
}