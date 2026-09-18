import { Injectable } from '@angular/core';
import { BehaviorSubject } from 'rxjs';
import { Permissions } from '../models/permissions';
import { LocalStorage } from '~/app/shared/enum/local-storage-enum';
@Injectable({
  providedIn: 'root'
})
export class AuthStorageService {
  isPwdDisplayedSource = new BehaviorSubject(false);
  isPwdDisplayed$ = this.isPwdDisplayedSource.asObservable();

  set(
    username: string,
    permissions = {},
    sso = false,
    pwdExpirationDate: number = null,
    pwdUpdateRequired: boolean = false
  ) {
    localStorage.setItem(LocalStorage.DASHBOARD_USERNAME, username);
    localStorage.setItem(LocalStorage.DASHBOARD_PERMISSIONS, JSON.stringify(new Permissions(permissions)));
    localStorage.setItem(LocalStorage.PWD_EXPIRATION_DATE, String(pwdExpirationDate));
    localStorage.setItem(LocalStorage.PWD_UPDATE_REQUIRED, String(pwdUpdateRequired));
    localStorage.setItem(LocalStorage.SSO, String(sso));
  }

  remove() {
    localStorage.removeItem(LocalStorage.DASHBOARD_USERNAME);
    localStorage.removeItem(LocalStorage.PWD_EXPIRATION_DATE);
    localStorage.removeItem(LocalStorage.PWD_UPDATE_REQUIRED);
  }

  isLoggedIn() {
    return localStorage.getItem(LocalStorage.DASHBOARD_USERNAME) !== null;
  }

  getUsername() {
    return localStorage.getItem(LocalStorage.DASHBOARD_USERNAME);
  }

  getPermissions(): Permissions {
    return JSON.parse(
      localStorage.getItem(LocalStorage.DASHBOARD_PERMISSIONS) || JSON.stringify(new Permissions({}))
    );
  }

  getPwdExpirationDate(): number {
    return Number(localStorage.getItem(LocalStorage.PWD_EXPIRATION_DATE));
  }

  getPwdUpdateRequired(): boolean {
    return localStorage.getItem(LocalStorage.PWD_UPDATE_REQUIRED) === 'true';
  }

  isSSO() {
    return localStorage.getItem(LocalStorage.SSO) === 'true';
  }
}
