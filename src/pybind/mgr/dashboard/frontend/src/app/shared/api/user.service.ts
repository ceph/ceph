import { HttpClient, HttpParams } from '@angular/common/http';
import { Injectable } from '@angular/core';

import * as _ from 'lodash';
import { of as observableOf } from 'rxjs';
import { mergeMap } from 'rxjs/operators';

import { UserFormModel } from '../../core/auth/user-form/user-form.model';
import { ApiModule } from './api.module';

@Injectable({
  providedIn: ApiModule
})
export class UserService {
  private url = 'api/user';

  constructor(private http: HttpClient) {}

  list() {
    return this.http.get(this.url);
  }

  delete(username: string) {
    return this.http.delete(`${this.url}/${username}`);
  }

  get(username: string) {
    return this.http.get(`${this.url}/${username}`);
  }

  create(user: UserFormModel) {
    return this.http.post(this.url, user);
  }

  clone(username: string, new_username: string) {
    let params = new HttpParams();
    params = params.append('new_username', new_username);
    return this.http.post(`${this.url}/${username}/clone`, null, { params: params });
  }

  update(user: UserFormModel) {
    return this.http.put(`${this.url}/${user.username}`, user);
  }

  /**
   * Check if the specified username exists.
   * @param {string} username The name of the user to check.
   * @return {Observable<boolean>}
   */
  exists(username: string) {
    return this.list().pipe(
      mergeMap((resp: object[]) => {
        const index = _.findIndex(resp, ['username', username]);
        return observableOf(-1 !== index);
      })
    );
  }
}
