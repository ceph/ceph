import { HttpClient, HttpParams } from '@angular/common/http';
import { Injectable } from '@angular/core';

import { Observable } from 'rxjs';
import { map, mergeMap } from 'rxjs/operators';

import { RgwDaemon } from '~/app/ceph/rgw/models/rgw-daemon';
import { RgwDaemonService } from '~/app/shared/api/rgw-daemon.service';
import { cdEncode } from '~/app/shared/decorators/cd-encode';

@cdEncode
@Injectable({
  providedIn: 'root'
})
export class RgwSiteService {
  private url = 'api/rgw/site';

  constructor(private http: HttpClient, private rgwDaemonService: RgwDaemonService) {}

  get(query?: string) {
    return this.rgwDaemonService.request((params: HttpParams) => {
      if (query) {
        params = params.append('query', query);
      }
      return this.http.get(this.url, { params: params });
    });
  }

  isDefaultRealm(): Observable<boolean> {
    return this.get('default-realm').pipe(
      mergeMap((defaultRealm: string) =>
        this.rgwDaemonService.selectedDaemon$.pipe(
          map((selectedDaemon: RgwDaemon) => selectedDaemon.realm_name === defaultRealm)
        )
      )
    );
  }
}
