import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';

import { UserFormModel } from '../../core/auth/user-form/user-form.model';
import { ApiModule } from './api.module';

@Injectable({
  providedIn: ApiModule
})
export class UserService {
  constructor(private http: HttpClient) {}

  list() {
    return this.http.get('api/user');
  }

  delete(username: string) {
    return this.http.delete(`api/user/${username}`);
  }

  get(username: string) {
    return this.http.get(`api/user/${username}`);
  }

  create(user: UserFormModel) {
    return this.http.post(`api/user`, user);
  }

  update(user: UserFormModel) {
    return this.http.put(`api/user/${user.username}`, user);
  }

  changePassword(username, oldPassword, newPassword) {
    // Note, the specified user MUST be logged in to be able to change
    // the password. The backend ensures that the password of another
    // user can not be changed, otherwise an error will be thrown.
    return this.http.post(`api/user/${username}/change_password`, {
      old_password: oldPassword,
      new_password: newPassword
    });
  }
}
