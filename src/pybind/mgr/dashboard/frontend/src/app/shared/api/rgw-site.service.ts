import { HttpClient, HttpParams } from '@angular/common/http';
import { Injectable } from '@angular/core';

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
}
