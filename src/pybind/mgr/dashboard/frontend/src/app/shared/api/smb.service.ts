import { HttpClient, HttpResponse } from '@angular/common/http';
import { Injectable } from '@angular/core';
import { Observable } from 'rxjs';

import { SMBCluster, SMBResult, SMBShare } from '~/app/ceph/smb/smb.model';

@Injectable({
  providedIn: 'root'
})
export class SmbService {
  baseURL = 'api/smb';

  constructor(private http: HttpClient) {}

  listClusters(): Observable<SMBCluster[]> {
    return this.http.get<SMBCluster[]>(`${this.baseURL}/cluster`);
  }

  listShares(clusterId: string): Observable<SMBShare[]> {
    return this.http.get<SMBShare[]>(`${this.baseURL}/share?cluster_id=${clusterId}`);
  }

  deleteShare(clusterId: string, shareId: string): Observable<HttpResponse<SMBResult>> {
    return this.http.delete<SMBResult>(`${this.baseURL}/share/${clusterId}/${shareId}`, {
      observe: 'response'
    });
  }
}
