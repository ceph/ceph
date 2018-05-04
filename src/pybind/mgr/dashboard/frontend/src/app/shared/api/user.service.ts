import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';

import { User } from '../models/user';

@Injectable()
export class UserService {

  constructor(private http: HttpClient) {}

  get(username: string) {
    return this.http.get(`api/user/${username}`);
  }

  set(user: User) {
    return this.http.put(`api/user/${user.username}`, user);
  }
}
