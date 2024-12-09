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
}
