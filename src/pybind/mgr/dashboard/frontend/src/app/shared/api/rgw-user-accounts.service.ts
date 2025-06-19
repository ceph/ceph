import { HttpClient, HttpParams } from '@angular/common/http';
import { Injectable } from '@angular/core';
import { Observable } from 'rxjs';
import { RgwDaemonService } from './rgw-daemon.service';
import { Account } from '~/app/ceph/rgw/models/rgw-user-accounts';

@Injectable({
  providedIn: 'root'
})
export class RgwUserAccountsService {
  private url = 'api/rgw/accounts';

  constructor(private http: HttpClient, private rgwDaemonService: RgwDaemonService) {}

  list(detailed?: boolean): Observable<any> {
    return this.rgwDaemonService.request((params: HttpParams) => {
      if (detailed) {
        params = params.append('detailed', detailed);
      }
      return this.http.get(this.url, { params });
    });
  }

  get(account_id: string): Observable<any> {
    return this.rgwDaemonService.request((params: HttpParams) => {
      if (account_id) {
        params = params.append('account_id', account_id);
      }
      return this.http.get(`${this.url}/get`, { params });
    });
  }

  create(payload: Partial<Account>): Observable<any> {
    return this.rgwDaemonService.request((params: HttpParams) => {
      return this.http.post(this.url, payload, { params: params });
    });
  }

  modify(payload: Partial<Account>): Observable<any> {
    return this.rgwDaemonService.request((params: HttpParams) => {
      return this.http.put(`${this.url}/set`, payload, { params: params });
    });
  }

  remove(accountId: string) {
    return this.rgwDaemonService.request((params: HttpParams) => {
      return this.http.delete(`${this.url}/${accountId}`, { params: params });
    });
  }

  setQuota(
    account_id: string,
    payload: { quota_type: string; max_size: string; max_objects: string; enabled: boolean }
  ) {
    return this.http.put(`${this.url}/${account_id}/quota`, payload);
  }
}
