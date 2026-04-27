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
    localStorage.setItem(LocalStorage.DASHBOARD_USRENAME, username);
    localStorage.setItem('dashboard_permissions', JSON.stringify(new Permissions(permissions)));
    localStorage.setItem('user_pwd_expiration_date', String(pwdExpirationDate));
    localStorage.setItem('user_pwd_update_required', String(pwdUpdateRequired));
    localStorage.setItem('sso', String(sso));
  }

  remove() {
    localStorage.removeItem(LocalStorage.DASHBOARD_USRENAME);
    localStorage.removeItem('user_pwd_expiration_data');
    localStorage.removeItem('user_pwd_update_required');
  }

  isLoggedIn() {
    return localStorage.getItem(LocalStorage.DASHBOARD_USRENAME) !== null;
  }

  getUsername() {
    return localStorage.getItem(LocalStorage.DASHBOARD_USRENAME);
  }

  getPermissions(): Permissions {
    return JSON.parse(
      localStorage.getItem('dashboard_permissions') || JSON.stringify(new Permissions({}))
    );
  }

  getPwdExpirationDate(): number {
    return Number(localStorage.getItem('user_pwd_expiration_date'));
  }

  getPwdUpdateRequired(): boolean {
    return localStorage.getItem('user_pwd_update_required') === 'true';
  }

  isSSO() {
    return localStorage.getItem('sso') === 'true';
  }
}
