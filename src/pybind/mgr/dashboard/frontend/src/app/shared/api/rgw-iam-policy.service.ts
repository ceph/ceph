import { HttpClient, HttpParams } from '@angular/common/http';
import { Injectable } from '@angular/core';
import { Observable } from 'rxjs';
import {
  IamPolicy,
  IamPolicyCreatePayload,
  IamPolicyTag,
  IamPolicyVersion
} from '~/app/ceph/rgw/models/rgw-iam-policy';

@Injectable({
  providedIn: 'root'
})
export class RgwIamPolicyService {
  constructor(private http: HttpClient) {}

  private getUrl(accountId: string): string {
    return `api/rgw/accounts/${accountId}/policies`;
  }

  list(accountId: string): Observable<IamPolicy[]> {
    return this.http.get<IamPolicy[]>(this.getUrl(accountId));
  }

  get(accountId: string, policyArn: string): Observable<IamPolicy> {
    const params = new HttpParams().append('policy_arn', policyArn);
    return this.http.get<IamPolicy>(`${this.getUrl(accountId)}/get`, { params });
  }

  create(accountId: string, payload: IamPolicyCreatePayload): Observable<IamPolicy> {
    return this.http.post<IamPolicy>(this.getUrl(accountId), payload);
  }

  delete(accountId: string, policyName: string): Observable<void> {
    return this.http.delete<void>(`${this.getUrl(accountId)}/${encodeURIComponent(policyName)}`);
  }

  listVersions(accountId: string, policyArn: string): Observable<IamPolicyVersion[]> {
    const params = new HttpParams().append('policy_arn', policyArn);
    return this.http.get<IamPolicyVersion[]>(`${this.getUrl(accountId)}/versions`, { params });
  }

  getVersion(
    accountId: string,
    policyArn: string,
    versionId: string
  ): Observable<IamPolicyVersion> {
    const params = new HttpParams().append('policy_arn', policyArn).append('version_id', versionId);
    return this.http.get<IamPolicyVersion>(`${this.getUrl(accountId)}/versions/get`, { params });
  }

  createVersion(
    accountId: string,
    policyArn: string,
    policyDoc: string,
    setAsDefault = false
  ): Observable<IamPolicyVersion> {
    return this.http.post<IamPolicyVersion>(`${this.getUrl(accountId)}/versions`, {
      policy_arn: policyArn,
      policy_doc: policyDoc,
      set_as_default: setAsDefault
    });
  }

  deleteVersion(accountId: string, policyArn: string, versionId: string): Observable<void> {
    const params = new HttpParams().append('policy_arn', policyArn).append('version_id', versionId);
    return this.http.delete<void>(`${this.getUrl(accountId)}/versions`, { params });
  }

  setDefaultVersion(accountId: string, policyArn: string, versionId: string): Observable<void> {
    const params = new HttpParams().append('policy_arn', policyArn).append('version_id', versionId);
    return this.http.put<void>(`${this.getUrl(accountId)}/versions/default`, null, { params });
  }

  listTags(accountId: string, policyArn: string): Observable<IamPolicyTag[]> {
    const params = new HttpParams().append('policy_arn', policyArn);
    return this.http.get<IamPolicyTag[]>(`${this.getUrl(accountId)}/tags`, { params });
  }

  addTags(accountId: string, policyArn: string, tags: IamPolicyTag[]): Observable<void> {
    return this.http.post<void>(`${this.getUrl(accountId)}/tags`, {
      policy_arn: policyArn,
      tags
    });
  }

  removeTags(accountId: string, policyArn: string, tagKeys: string[]): Observable<void> {
    const params = new HttpParams()
      .append('policy_arn', policyArn)
      .append('tag_keys', tagKeys.join(','));
    return this.http.delete<void>(`${this.getUrl(accountId)}/tags`, { params });
  }
}
