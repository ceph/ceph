import { HttpClient } from '@angular/common/http';
import { Injectable, NgZone } from '@angular/core';

import { BehaviorSubject, Subscription } from 'rxjs';

import { cdEncode, cdEncodeNot } from '../decorators/cd-encode';
import { ApiModule } from './api.module';

@cdEncode
@Injectable({
  providedIn: ApiModule
})
export class RbdMirroringService {
  // Observable sources
  private summaryDataSource = new BehaviorSubject(null);

  // Observable streams
  summaryData$ = this.summaryDataSource.asObservable();

  constructor(private http: HttpClient, private ngZone: NgZone) {
    this.refreshAndSchedule();
  }

  refresh() {
    this.http.get('api/block/mirroring/summary').subscribe((data) => {
      this.summaryDataSource.next(data);
    });
  }

  refreshAndSchedule() {
    this.refresh();

    this.ngZone.runOutsideAngular(() => {
      setTimeout(() => {
        this.ngZone.run(() => {
          this.refreshAndSchedule();
        });
      }, 30000);
    });
  }

  /**
   * Returns the current value of summaryData
   */
  getCurrentSummary(): { [key: string]: any; executing_tasks: object[] } {
    return this.summaryDataSource.getValue();
  }

  /**
   * Subscribes to the summaryData,
   * which is updated once every 30 seconds or when a new task is created.
   */
  subscribeSummary(next: (summary: any) => void, error?: (error: any) => void): Subscription {
    return this.summaryData$.subscribe(next, error);
  }

  getPool(poolName: string) {
    return this.http.get(`api/block/mirroring/pool/${poolName}`);
  }

  updatePool(poolName: string, request: any) {
    return this.http.put(`api/block/mirroring/pool/${poolName}`, request, { observe: 'response' });
  }

  getSiteName() {
    return this.http.get(`api/block/mirroring/site_name`);
  }

  setSiteName(@cdEncodeNot siteName: string) {
    return this.http.put(
      `api/block/mirroring/site_name`,
      { site_name: siteName },
      { observe: 'response' }
    );
  }

  createBootstrapToken(poolName: string) {
    return this.http.post(`api/block/mirroring/pool/${poolName}/bootstrap/token`, {});
  }

  importBootstrapToken(
    poolName: string,
    @cdEncodeNot direction: string,
    @cdEncodeNot token: string
  ) {
    const request = {
      direction: direction,
      token: token
    };
    return this.http.post(`api/block/mirroring/pool/${poolName}/bootstrap/peer`, request, {
      observe: 'response'
    });
  }

  getPeer(poolName: string, peerUUID: string) {
    return this.http.get(`api/block/mirroring/pool/${poolName}/peer/${peerUUID}`);
  }

  addPeer(poolName: string, request: any) {
    return this.http.post(`api/block/mirroring/pool/${poolName}/peer`, request, {
      observe: 'response'
    });
  }

  updatePeer(poolName: string, peerUUID: string, request: any) {
    return this.http.put(`api/block/mirroring/pool/${poolName}/peer/${peerUUID}`, request, {
      observe: 'response'
    });
  }

  deletePeer(poolName: string, peerUUID: string) {
    return this.http.delete(`api/block/mirroring/pool/${poolName}/peer/${peerUUID}`, {
      observe: 'response'
    });
  }
}
