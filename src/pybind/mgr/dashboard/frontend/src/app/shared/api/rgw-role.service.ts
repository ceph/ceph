import { HttpClient, HttpParams } from '@angular/common/http';
import { Injectable } from '@angular/core';
import { Observable } from 'rxjs';

@Injectable({
  providedIn: 'root'
})
export class RgwRoleService {
  private url = 'api/rgw/roles';

  constructor(private http: HttpClient) {}

  list(accountId?: string): Observable<any> {
    let params = new HttpParams();
    if (accountId) {
      params = params.append('account_id', accountId);
    }
    return this.http.get(this.url, { params });
  }

  get(roleName: string, accountId?: string): Observable<any> {
    let params = new HttpParams();
    if (accountId) {
      params = params.append('account_id', accountId);
    }
    return this.http.get(`${this.url}/${roleName}`, { params });
  }

  create(role: any): Observable<any> {
    return this.http.post(this.url, role);
  }

  update(_roleName: string, payload: any): Observable<any> {
    return this.http.put(this.url, payload);
  }

  delete(roleName: string, accountId?: string): Observable<any> {
    let params = new HttpParams();
    if (accountId) {
      params = params.append('account_id', accountId);
    }
    return this.http.delete(`${this.url}/${roleName}`, { params });
  }
}
