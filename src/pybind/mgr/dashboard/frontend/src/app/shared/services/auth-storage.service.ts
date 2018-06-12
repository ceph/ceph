import { Injectable } from '@angular/core';

import { ServicesModule } from './services.module';

@Injectable({
  providedIn: ServicesModule
})
export class AuthStorageService {

  constructor() {
  }

  set(username: string) {
    localStorage.setItem('dashboard_username', username);
  }

  remove() {
    localStorage.removeItem('dashboard_username');
  }

  isLoggedIn() {
    return localStorage.getItem('dashboard_username') !== null;
  }

}
