import { Injectable } from '@angular/core';

import { Permissions } from '../models/permissions';

@Injectable({
  providedIn: 'root'
})
export class AuthStorageService {
  constructor() {}

  set(username: string, permissions: object = {}) {
    localStorage.setItem('dashboard_username', username);
    localStorage.setItem('dashboard_permissions', JSON.stringify(new Permissions(permissions)));
  }

  remove() {
    localStorage.removeItem('dashboard_username');
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
}
