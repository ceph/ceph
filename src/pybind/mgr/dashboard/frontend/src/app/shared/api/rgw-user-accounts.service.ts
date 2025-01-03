import { HttpClient, HttpParams } from '@angular/common/http';
import { Injectable } from '@angular/core';
import { Observable } from 'rxjs';

@Injectable({
  providedIn: 'root'
})
export class RgwUserAccountsService {
  private url = 'api/rgw/accounts';

  constructor(private http: HttpClient) {}

  list(detailed?: boolean): Observable<any> {
    let params = new HttpParams();
    if (detailed) {
      params = params.append('detailed', detailed);
    }
    return this.http.get(this.url, { params });
  }

  get(account_id: string): Observable<any> {
    let params = new HttpParams();
    if (account_id) {
      params = params.append('account_id', account_id);
    }
    return this.http.get(this.url, { params });
  }

  create(payload: { account_id: string; account_name: string; email: string }): Observable<any> {
    return this.http.post(this.url, payload);
  }
}
