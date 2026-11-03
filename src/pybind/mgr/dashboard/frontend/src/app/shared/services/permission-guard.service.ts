import { Injectable } from '@angular/core';
import { ActivatedRouteSnapshot, Router } from '@angular/router';

import { AuthStorageService } from './auth-storage.service';

@Injectable({
  providedIn: 'root'
})
export class PermissionGuardService {
  constructor(
    private router: Router,
    private authStorageService: AuthStorageService
  ) {}

  canActivate(route: ActivatedRouteSnapshot): boolean {
    const permissions = this.authStorageService.getPermissions();
    const config = route.data?.['permissionGuardConfig'];
    if (config) {
      const scope = permissions[config.scope];
      if (!scope?.[config.action]) {
        this.router.navigate([config.redirectTo || '/overview']);
        return false;
      }
    }
    return true;
  }
}
