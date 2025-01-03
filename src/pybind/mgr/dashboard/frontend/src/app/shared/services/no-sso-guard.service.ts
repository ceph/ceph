import { Injectable } from '@angular/core';
import { CanActivate, CanActivateChild } from '@angular/router';

import { DashboardUserDeniedError } from '~/app/core/error/error';
import { AuthStorageService } from './auth-storage.service';

/**
 * This service checks if a route can be activated if the user has not
 * been logged in via SSO.
 */
@Injectable({
  providedIn: 'root'
})
export class NoSsoGuardService implements CanActivate, CanActivateChild {
  constructor(private authStorageService: AuthStorageService) {}

  canActivate() {
    if (!this.authStorageService.isSSO()) {
      return true;
    }
    throw new DashboardUserDeniedError();
    return false;
  }

  canActivateChild(): boolean {
    return this.canActivate();
  }
}
