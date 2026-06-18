import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';
import { Observable } from 'rxjs';
import {
  RgwRole,
  RgwRoleCreatePayload,
  RgwRoleUpdatePayload
} from '~/app/ceph/rgw/models/rgw-role';

@Injectable({
  providedIn: 'root'
})
export class RgwRoleService {
  constructor(private http: HttpClient) {}

  private getUrl(accountId: string): string {
    return `api/rgw/accounts/${accountId}/roles`;
  }

  list(accountId: string): Observable<RgwRole[]> {
    return this.http.get<RgwRole[]>(this.getUrl(accountId));
  }

  get(roleName: string, accountId: string): Observable<RgwRole> {
    return this.http.get<RgwRole>(`${this.getUrl(accountId)}/${roleName}`);
  }

  create(role: RgwRoleCreatePayload): Observable<RgwRole> {
    const accountId = role.account_id;
    return this.http.post<RgwRole>(this.getUrl(accountId), role);
  }

  update(_roleName: string, payload: RgwRoleUpdatePayload): Observable<RgwRole> {
    const accountId = payload.account_id;
    return this.http.put<RgwRole>(this.getUrl(accountId), payload);
  }

  delete(roleName: string, accountId: string): Observable<any> {
    return this.http.delete(`${this.getUrl(accountId)}/${roleName}`);
  }
}
