import type { Request } from 'express';

/**
 * Shape of `req.user` as attached by JwtStrategy.validate() — the only identity
 * available to authenticated handlers.
 */
export interface RequestUser {
  userId: string;
  email: string;
}

/** Express request carrying the authenticated user. */
export interface AuthenticatedRequest extends Request {
  user: RequestUser;
}
