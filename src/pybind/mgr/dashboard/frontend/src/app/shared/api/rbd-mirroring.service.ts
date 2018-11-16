import { HttpClient } from '@angular/common/http';
import { Injectable, NgZone } from '@angular/core';

import { BehaviorSubject, Subscription } from 'rxjs';

import { ApiModule } from './api.module';

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

  getPool(poolName) {
    return this.http.get(`api/block/mirroring/pool/${poolName}`);
  }

  updatePool(poolName, request) {
    return this.http.put(`api/block/mirroring/pool/${poolName}`, request, { observe: 'response' });
  }

  getPeer(poolName, peerUUID) {
    return this.http.get(`api/block/mirroring/pool/${poolName}/peer/${peerUUID}`);
  }

  addPeer(poolName, request) {
    return this.http.post(`api/block/mirroring/pool/${poolName}/peer`, request, {
      observe: 'response'
    });
  }

  updatePeer(poolName, peerUUID, request) {
    return this.http.put(`api/block/mirroring/pool/${poolName}/peer/${peerUUID}`, request, {
      observe: 'response'
    });
  }

  deletePeer(poolName, peerUUID) {
    return this.http.delete(`api/block/mirroring/pool/${poolName}/peer/${peerUUID}`, {
      observe: 'response'
    });
  }
}
