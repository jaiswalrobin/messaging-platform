export interface SendMessagePayload {
  conversationId: string;
  content: string;
  clientMessageId: string;
}

export interface UserAuthResponse {
  userId: string;
  token: string;
}