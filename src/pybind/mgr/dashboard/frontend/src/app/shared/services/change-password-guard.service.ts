import { Injectable } from '@angular/core';
import { ActivatedRouteSnapshot, Router, RouterStateSnapshot } from '@angular/router';

import { AuthStorageService } from './auth-storage.service';

/**
 * This service guard checks if a user must be redirected to a special
 * page at '/login-change-password' to set a new password.
 */
@Injectable({
  providedIn: 'root'
})
export class ChangePasswordGuardService {
  constructor(private router: Router, private authStorageService: AuthStorageService) {}

  canActivate(_route: ActivatedRouteSnapshot, state: RouterStateSnapshot) {
    // Redirect to '/login-change-password' when the following constraints
    // are fulfilled:
    // - The user must be logged in.
    // - SSO must be disabled.
    // - The flag 'User must change password at next login' must be set.
    if (
      this.authStorageService.isLoggedIn() &&
      !this.authStorageService.isSSO() &&
      this.authStorageService.getPwdUpdateRequired()
    ) {
      this.router.navigate(['/login-change-password'], { queryParams: { returnUrl: state.url } });
      return false;
    }
    return true;
  }

  canActivateChild(childRoute: ActivatedRouteSnapshot, state: RouterStateSnapshot): boolean {
    return this.canActivate(childRoute, state);
  }
}
