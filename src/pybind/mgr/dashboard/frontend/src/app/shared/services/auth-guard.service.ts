import { Injectable } from '@angular/core';
import { CanActivate, CanActivateChild, Router } from '@angular/router';

import { AuthStorageService } from './auth-storage.service';

@Injectable({
  providedIn: 'root'
})
export class AuthGuardService implements CanActivate, CanActivateChild {
  constructor(private router: Router, private authStorageService: AuthStorageService) {}

  canActivate() {
    if (this.authStorageService.isLoggedIn()) {
      return true;
    }
    this.router.navigate(['/login']);
    return false;
  }

  canActivateChild(): boolean {
    return this.canActivate();
  }
}
