import { HttpClient, HttpParams } from '@angular/common/http';
import { Injectable } from '@angular/core';

import { Observable, of as observableOf } from 'rxjs';
import { catchError, mapTo } from 'rxjs/operators';

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

  changePassword(username: string, oldPassword: string, newPassword: string) {
    // Note, the specified user MUST be logged in to be able to change
    // the password. The backend ensures that the password of another
    // user can not be changed, otherwise an error will be thrown.
    return this.http.post(`api/user/${username}/change_password`, {
      old_password: oldPassword,
      new_password: newPassword
    });
  }

  validateUserName(user_name: string): Observable<boolean> {
    return this.get(user_name).pipe(
      mapTo(true),
      catchError((error) => {
        error.preventDefault();
        return observableOf(false);
      })
    );
  }

  validatePassword(password: string, username: string = null, oldPassword: string = null) {
    let params = new HttpParams();
    params = params.append('password', password);
    if (username) {
      params = params.append('username', username);
    }
    if (oldPassword) {
      params = params.append('old_password', oldPassword);
    }
    return this.http.post('api/user/validate_password', null, { params });
  }
}
