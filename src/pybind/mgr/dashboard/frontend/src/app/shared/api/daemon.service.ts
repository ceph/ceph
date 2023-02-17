import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';

import { cdEncode } from '~/app/shared/decorators/cd-encode';

@cdEncode
@Injectable({
  providedIn: 'root'
})
export class DaemonService {
  private url = 'api/daemon';

  constructor(private http: HttpClient) {}

  action(daemonName: string, actionType: string) {
    return this.http.put(
      `${this.url}/${daemonName}`,
      {
        action: actionType,
        container_image: null
      },
      {
        headers: { Accept: 'application/vnd.ceph.api.v0.1+json' },
        observe: 'response'
      }
    );
  }
}
