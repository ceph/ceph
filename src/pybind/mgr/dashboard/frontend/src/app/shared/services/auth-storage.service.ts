import { Injectable } from '@angular/core';

import { Permissions } from '../models/permissions';
import { Router } from '@angular/router';

@Injectable({
  providedIn: 'root'
})

export class AuthStorageService {
  constructor(    
    private router: Router
  ) {}

  set(username: string, token: string, permissions: object = {}, need_change_password: boolean) {
    localStorage.setItem('dashboard_username', username);
    localStorage.setItem('need_change_password', String(need_change_password));
    localStorage.setItem('access_token', token);
    localStorage.setItem('dashboard_permissions', JSON.stringify(new Permissions(permissions)));
  }

  remove() {
    localStorage.removeItem('access_token');
    localStorage.removeItem('dashboard_username');
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

  updateNeedChangePassword(need_change_password: boolean){
    localStorage.setItem('need_change_password', String(need_change_password));
  }
  
  ForceChangingPasswordAfterFirstLog() {
    if(localStorage.getItem('need_change_password') == 'false'){ 
        return;
      }
    return this.router.navigate(['/user-management/users/edit/'+ this.getUsername()]);;
  }

  getPermissions(): Permissions {
    this.ForceChangingPasswordAfterFirstLog();
    return JSON.parse(
      localStorage.getItem('dashboard_permissions') || JSON.stringify(new Permissions({}))
    );
  }
}
