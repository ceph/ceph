import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';

import { Credentials } from '../models/credentials';
import { AuthStorageService } from './auth-storage.service';

@Injectable()
export class AuthService {

  constructor(private authStorageService: AuthStorageService,
              private http: HttpClient) {
  }

  login(credentials: Credentials) {
    return this.http.post('api/auth', credentials).toPromise().then((resp: Credentials) => {
      this.authStorageService.set(resp.username);
    });
  }

  logout() {
    return this.http.delete('api/auth').toPromise().then(() => {
      this.authStorageService.remove();
    });
  }
}
