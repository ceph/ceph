import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';
import { Observable, of } from 'rxjs';
import { catchError, map } from 'rxjs/operators';
import isFunction from 'lodash/isFunction';
import {
  RgwPolicyDocument,
  RgwRole,
  RgwRoleCreatePayload,
  RgwRolePolicyResponse,
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

  exists(roleName: string, accountId: string): Observable<boolean> {
    return this.get(roleName, accountId).pipe(
      map(() => true),
      catchError((error: Event) => {
        if (isFunction(error?.preventDefault)) {
          error.preventDefault();
        }
        return of(false);
      })
    );
  }

  attachPolicy(
    roleName: string,
    policyName: string,
    policyDoc: string,
    accountId: string
  ): Observable<string> {
    return this.http.post<string>(`${this.getUrl(accountId)}/${roleName}/policy`, {
      role_name: roleName,
      policy_name: policyName,
      policy_doc: policyDoc
    });
  }

  listPolicies(roleName: string, accountId: string): Observable<string[]> {
    return this.http.get<string[]>(`${this.getUrl(accountId)}/${roleName}/policy`);
  }

  getPolicy(roleName: string, policyName: string, accountId: string): Observable<string> {
    return this.http
      .get<RgwRolePolicyResponse>(`${this.getUrl(accountId)}/${roleName}/policy/${policyName}`)
      .pipe(
        map((res) => this.formatPolicyDocument(res.PolicyDocument ?? res['Permission policy']))
      );
  }

  private formatPolicyDocument(policyDocument: string | RgwPolicyDocument): string {
    if (policyDocument && typeof policyDocument === 'object') {
      return JSON.stringify(policyDocument, null, 2);
    }
    if (typeof policyDocument !== 'string' || !policyDocument) {
      return '';
    }
    try {
      return JSON.stringify(JSON.parse(policyDocument), null, 2);
    } catch {
      return policyDocument;
    }
  }

  deletePolicy(roleName: string, policyName: string, accountId: string): Observable<string> {
    return this.http.delete<string>(`${this.getUrl(accountId)}/${roleName}/policy/${policyName}`);
  }
}
