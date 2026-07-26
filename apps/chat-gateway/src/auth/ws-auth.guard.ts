import { CanActivate, ExecutionContext, Injectable } from '@nestjs/common';
import { JwtService } from '@nestjs/jwt';
import { WsException } from '@nestjs/websockets';
import { Observable } from 'rxjs';

@Injectable()
export class WsAuthGuard implements CanActivate {
  constructor(private jwtService: JwtService) {}

  canActivate(
    context: ExecutionContext,
  ): boolean | Promise<boolean> | Observable<boolean> {
    // Extract the client from the WebSocket context
    const client = context.switchToWs().getClient();
    
    // Get the token from the handshake auth object
    const token = client.handshake?.auth?.token;

    if (!token) {
      throw new WsException('No token provided');
    }

    try {
      // Verify the token using the EXACT SAME secret as the API app
      const payload = this.jwtService.verify(token, {
        secret: 'super-secret-key-for-local-dev-only',
      });
      
      // Attach the user payload to the client object so the gateway can use it
      client.user = payload;
      return true;
    } catch (err) {
      throw new WsException('Invalid token');
    }
  }
}
