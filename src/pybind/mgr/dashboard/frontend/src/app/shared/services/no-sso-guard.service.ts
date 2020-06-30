import { Injectable } from '@angular/core';
import { CanActivate, CanActivateChild, Router } from '@angular/router';

import { AuthStorageService } from './auth-storage.service';

/**
 * This service checks if a route can be activated if the user has not
 * been logged in via SSO.
 */
@Injectable({
  providedIn: 'root'
})
export class NoSsoGuardService implements CanActivate, CanActivateChild {
  constructor(private authStorageService: AuthStorageService, private router: Router) {}

  canActivate() {
    if (!this.authStorageService.isSSO()) {
      return true;
    }
    this.router.navigate(['404']);
    return false;
  }

  canActivateChild(): boolean {
    return this.canActivate();
  }
}
