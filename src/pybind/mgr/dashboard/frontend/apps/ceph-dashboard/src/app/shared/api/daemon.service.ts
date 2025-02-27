import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';
import { Observable } from 'rxjs';

import { cdEncode } from '~/app/shared/decorators/cd-encode';
import { Daemon } from '../models/daemon.interface';

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

  list(daemonTypes: string[]): Observable<Daemon[]> {
    return this.http.get<Daemon[]>(this.url, {
      params: { daemon_types: daemonTypes }
    });
  }
}
