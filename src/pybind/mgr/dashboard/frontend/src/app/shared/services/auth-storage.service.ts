import { Injectable } from '@angular/core';

import { BehaviorSubject } from 'rxjs';

import { Permissions } from '../models/permissions';

@Injectable({
  providedIn: 'root'
})
export class AuthStorageService {
  isPwdDisplayedSource = new BehaviorSubject(false);
  isPwdDisplayed$ = this.isPwdDisplayedSource.asObservable();

  set(
    username: string,
    token: string,
    permissions = {},
    sso = false,
    pwdExpirationDate: number = null,
    pwdUpdateRequired: boolean = false
  ) {
    localStorage.setItem('dashboard_username', username);
    localStorage.setItem('access_token', token);
    localStorage.setItem('dashboard_permissions', JSON.stringify(new Permissions(permissions)));
    localStorage.setItem('user_pwd_expiration_date', String(pwdExpirationDate));
    localStorage.setItem('user_pwd_update_required', String(pwdUpdateRequired));
    localStorage.setItem('sso', String(sso));
  }

  remove() {
    localStorage.removeItem('access_token');
    localStorage.removeItem('dashboard_username');
    localStorage.removeItem('user_pwd_expiration_data');
    localStorage.removeItem('user_pwd_update_required');
  }

  getToken(): string {
    return localStorage.getItem('access_token');
  }

  isLoggedIn() {
    return localStorage.getItem('dashboard_username') !== null;
  }

  getUsername() {
    return localStorage.getItem('dashboard_username');
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
