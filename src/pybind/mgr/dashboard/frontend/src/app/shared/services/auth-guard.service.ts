import { Injectable } from '@angular/core';
import {
  ActivatedRouteSnapshot,
  CanActivate,
  CanActivateChild,
  Router,
  RouterStateSnapshot
} from '@angular/router';

import { AuthStorageService } from './auth-storage.service';
import { ServicesModule } from './services.module';

@Injectable({
  providedIn: ServicesModule
})
export class AuthGuardService implements CanActivate, CanActivateChild {
  constructor(private router: Router, private authStorageService: AuthStorageService) {}

  canActivate(route: ActivatedRouteSnapshot, state: RouterStateSnapshot) {
    if (this.authStorageService.isLoggedIn()) {
      return true;
    }
    this.router.navigate(['/login']);
    return false;
  }

  canActivateChild(route: ActivatedRouteSnapshot, state: RouterStateSnapshot): boolean {
    return this.canActivate(route, state);
  }
}
