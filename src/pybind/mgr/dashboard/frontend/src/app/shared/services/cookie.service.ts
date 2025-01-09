import { Injectable } from '@angular/core';
import { CookieService } from 'ngx-cookie-service';

@Injectable({
  providedIn: 'root'
})
export class CookiesService {
  constructor(private cookieService: CookieService) {}

  setToken(name: string, token: string) {
    this.cookieService.set(name, token, null, null, null, true, 'Strict');
  }

  getToken(name: string): string {
    return this.cookieService.get(name);
  }

  deleteToken(name: string) {
    this.cookieService.delete(name);
  }
}
