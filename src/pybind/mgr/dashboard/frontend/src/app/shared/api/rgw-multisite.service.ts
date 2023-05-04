import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';
import { RgwDaemonService } from './rgw-daemon.service';

@Injectable({
  providedIn: 'root'
})
export class RgwMultisiteService {
  private url = 'ui-api/rgw/multisite';

  constructor(private http: HttpClient, private rgwDaemonService: RgwDaemonService) {}

  getMultisiteSyncStatus() {
    return this.rgwDaemonService.request(() => {
      return this.http.get(`${this.url}/sync_status`);
    });
  }
}
