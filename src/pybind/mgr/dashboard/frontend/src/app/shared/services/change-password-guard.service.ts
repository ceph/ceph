import { Injectable } from '@angular/core';
import { CanActivate, CanActivateChild, Router } from '@angular/router';

import { AuthStorageService } from './auth-storage.service';

/**
 * This service guard checks if a user must be redirected to a special
 * page at '/login-change-password' to set a new password.
 */
@Injectable({
  providedIn: 'root'
})
export class ChangePasswordGuardService implements CanActivate, CanActivateChild {
  constructor(private router: Router, private authStorageService: AuthStorageService) {}

  canActivate() {
    // Redirect to '/login-change-password' when the following constraints
    // are fulfilled:
    // - The user must be logged in.
    // - SSO must be disabled.
    // - The flag 'User must change password at next logon' must be set.
    if (
      this.authStorageService.isLoggedIn() &&
      !this.authStorageService.isSSO() &&
      this.authStorageService.getPwdUpdateRequired()
    ) {
      this.router.navigate(['/login-change-password']);
      return false;
    }
    return true;
  }

  canActivateChild(): boolean {
    return this.canActivate();
  }
}
